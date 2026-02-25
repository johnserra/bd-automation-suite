"""Odoo CRM client using XML-RPC for BD Automation Suite."""

import os
import xmlrpc.client
from datetime import date
from typing import Optional

from thefuzz import fuzz

from shared.logger import get_logger

logger = get_logger("odoo_client")

# Fuzzy match threshold for deduplication (0-100, higher = stricter)
FUZZY_MATCH_THRESHOLD = 85


class OdooClient:
    """XML-RPC client for Odoo CRM.

    Authenticates lazily on first API call and re-authenticates on session expiry.
    All CRUD methods operate on crm.lead (the Lead/Opportunity model).
    """

    def __init__(self, url: str, db: str, user: str, api_key: str):
        self.url = url.rstrip("/")
        self.db = db
        self.user = user
        self.api_key = api_key
        self._uid: Optional[int] = None
        self._models: Optional[xmlrpc.client.ServerProxy] = None

    @classmethod
    def from_env(cls) -> "OdooClient":
        """Construct OdooClient from environment variables.

        Required env vars: ODOO_URL, ODOO_DB, ODOO_USER, ODOO_API_KEY
        """
        required = ["ODOO_URL", "ODOO_DB", "ODOO_USER", "ODOO_API_KEY"]
        missing = [v for v in required if not os.getenv(v)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return cls(
            url=os.environ["ODOO_URL"],
            db=os.environ["ODOO_DB"],
            user=os.environ["ODOO_USER"],
            api_key=os.environ["ODOO_API_KEY"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _authenticate(self) -> None:
        """Authenticate with Odoo and cache the user ID."""
        logger.debug("Authenticating with Odoo at %s", self.url)
        common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self._uid = common.authenticate(self.db, self.user, self.api_key, {})
        if not self._uid:
            raise PermissionError(
                f"Odoo authentication failed for user '{self.user}' on database '{self.db}'"
            )
        self._models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        logger.debug("Authenticated as uid=%s", self._uid)

    def _ensure_auth(self) -> None:
        if self._uid is None:
            self._authenticate()

    def _execute(self, model: str, method: str, *args, **kwargs):
        """Call an Odoo model method, re-authenticating on session expiry."""
        self._ensure_auth()
        try:
            return self._models.execute_kw(
                self.db, self._uid, self.api_key, model, method, list(args), kwargs
            )
        except xmlrpc.client.Fault as exc:
            if "session" in str(exc).lower() or "access" in str(exc).lower():
                logger.debug("Session may have expired, re-authenticating")
                self._uid = None
                self._authenticate()
                return self._models.execute_kw(
                    self.db, self._uid, self.api_key, model, method, list(args), kwargs
                )
            raise

    # ------------------------------------------------------------------
    # Lead / Opportunity methods
    # ------------------------------------------------------------------

    def search_leads(
        self,
        domain: list,
        fields: Optional[list] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Search crm.lead records matching domain.

        Args:
            domain: Odoo domain filter list, e.g. [['stage_id.name','=','Research']].
            fields: Field names to return. Defaults to a sensible default set.
            limit: Maximum records to return (None = no limit).

        Returns:
            List of dicts, one per matching lead.
        """
        if fields is None:
            fields = [
                "id", "name", "partner_name", "contact_name", "email_from",
                "phone", "city", "stage_id", "x_bd_stream", "x_lead_score",
                "x_enrichment_status", "x_last_personal_contact",
            ]
        kwargs = {"fields": fields}
        if limit is not None:
            kwargs["limit"] = limit
        logger.debug("search_leads domain=%s limit=%s", domain, limit)
        return self._execute("crm.lead", "search_read", domain, **kwargs)

    def get_lead(self, lead_id: int, fields: Optional[list] = None) -> dict:
        """Fetch a single lead by ID.

        Args:
            lead_id: Odoo record ID.
            fields: Field names to return. None = all fields.

        Returns:
            Dict of field values, or empty dict if not found.
        """
        kwargs = {}
        if fields:
            kwargs["fields"] = fields
        logger.debug("get_lead id=%s", lead_id)
        results = self._execute("crm.lead", "read", [lead_id], **kwargs)
        return results[0] if results else {}

    def create_lead(self, values: dict) -> int:
        """Create a new crm.lead record.

        Args:
            values: Dict of field name → value.

        Returns:
            Integer ID of the newly created record.
        """
        logger.debug("create_lead values=%s", list(values.keys()))
        lead_id = self._execute("crm.lead", "create", values)
        logger.debug("Created lead id=%s", lead_id)
        return lead_id

    def update_lead(self, lead_id: int, values: dict) -> bool:
        """Update fields on an existing crm.lead record.

        Args:
            lead_id: Odoo record ID.
            values: Dict of field name → new value.

        Returns:
            True on success.
        """
        logger.debug("update_lead id=%s fields=%s", lead_id, list(values.keys()))
        result = self._execute("crm.lead", "write", [lead_id], values)
        return bool(result)

    def create_activity(
        self,
        lead_id: int,
        summary: str,
        date_deadline: date,
        priority: str = "medium",
    ) -> int:
        """Create a mail.activity (todo/reminder) on a lead.

        Args:
            lead_id: crm.lead record ID.
            summary: Short description shown in the activity list.
            date_deadline: Due date for the activity.
            priority: 'low', 'medium', or 'high' (stored as 0/1/2 in Odoo).

        Returns:
            Integer ID of the created activity.
        """
        priority_map = {"low": "0", "medium": "1", "high": "2"}
        odoo_priority = priority_map.get(priority, "1")

        # Look up the generic "To-Do" activity type
        type_ids = self._execute(
            "mail.activity.type",
            "search",
            [["name", "ilike", "to-do"]],
        )
        activity_type_id = type_ids[0] if type_ids else False

        values = {
            "res_model_id": self._get_model_id("crm.lead"),
            "res_id": lead_id,
            "summary": summary,
            "date_deadline": date_deadline.isoformat()
            if isinstance(date_deadline, date)
            else date_deadline,
            "priority": odoo_priority,
        }
        if activity_type_id:
            values["activity_type_id"] = activity_type_id

        logger.debug("create_activity lead=%s summary='%s'", lead_id, summary)
        return self._execute("mail.activity", "create", values)

    def search_duplicate(
        self, partner_name: str, city: Optional[str] = None
    ) -> list[dict]:
        """Find existing leads that fuzzy-match the given company name.

        Uses Odoo's name_search for a coarse pre-filter, then thefuzz for
        precise matching. Returns leads above FUZZY_MATCH_THRESHOLD.

        Args:
            partner_name: Company name to check.
            city: Optional city to narrow the search.

        Returns:
            List of matching lead dicts (may be empty).
        """
        logger.debug(
            "search_duplicate partner_name='%s' city=%s", partner_name, city
        )
        # Coarse filter via Odoo name_search (substring match on partner_name)
        domain = [["partner_name", "ilike", partner_name[:5]]]
        if city:
            domain.append(["city", "ilike", city])

        candidates = self.search_leads(
            domain,
            fields=["id", "partner_name", "city", "x_bd_stream"],
            limit=50,
        )

        matches = []
        for candidate in candidates:
            ratio = fuzz.token_sort_ratio(
                partner_name.lower(),
                (candidate.get("partner_name") or "").lower(),
            )
            if ratio >= FUZZY_MATCH_THRESHOLD:
                candidate["_fuzzy_score"] = ratio
                matches.append(candidate)
                logger.debug(
                    "Fuzzy match: '%s' ↔ '%s' score=%s",
                    partner_name,
                    candidate["partner_name"],
                    ratio,
                )

        return matches

    def get_state_id(self, state_code: str, country_code: str = "US") -> Optional[int]:
        """Look up a res.country.state record ID by state abbreviation code.

        Args:
            state_code:   Two-letter abbreviation, e.g. "NY", "PA".
            country_code: ISO alpha-2 country code (default "US").

        Returns:
            Integer state ID, or None if not found.
        """
        logger.debug("get_state_id code='%s' country='%s'", state_code, country_code)
        ids = self._execute(
            "res.country.state",
            "search",
            [[
                ["code", "=", state_code.upper()],
                ["country_id.code", "=", country_code.upper()],
            ]],
        )
        return ids[0] if ids else None

    def get_open_activities(self, lead_id: int) -> list[dict]:
        """Return all open (incomplete) activities for a crm.lead record.

        mail.activity records are deleted in Odoo when completed, so all
        records returned here are open by definition.

        Args:
            lead_id: crm.lead record ID.

        Returns:
            List of activity dicts with keys: id, summary, date_deadline.
        """
        logger.debug("get_open_activities lead=%s", lead_id)
        return self._execute(
            "mail.activity",
            "search_read",
            [[
                ["res_model", "=", "crm.lead"],
                ["res_id", "=", lead_id],
            ]],
            fields=["id", "summary", "date_deadline", "activity_type_id"],
        )

    def get_stage_id(self, stage_name: str) -> Optional[int]:
        """Look up a crm.stage record ID by exact name.

        Args:
            stage_name: Stage name as it appears in Odoo (e.g. 'Research').

        Returns:
            Integer stage ID, or None if not found.
        """
        logger.debug("get_stage_id name='%s'", stage_name)
        ids = self._execute(
            "crm.stage", "search", [["name", "=", stage_name]]
        )
        return ids[0] if ids else None

    # ------------------------------------------------------------------
    # Internal utility
    # ------------------------------------------------------------------

    def _get_model_id(self, model_name: str) -> int:
        """Look up the ir.model integer ID for a model name."""
        ids = self._execute(
            "ir.model", "search", [["model", "=", model_name]]
        )
        return ids[0] if ids else False
