"""
Base Service Class

Provides common utilities and patterns for all service classes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID

from django.db import connection

if TYPE_CHECKING:
    pass


T = TypeVar('T')


@dataclass
class PaginationResult[T]:
    """Standard pagination result container."""
    items: list[T]
    total_count: int
    has_more: bool
    cursor_id: UUID | None = None
    cursor_created_at: str | None = None


class BaseService:
    """
    Base class for all service classes.

    Provides common utilities for:
    - User context management
    - Agency scoping
    - Permission checking
    - Query execution helpers
    """

    def __init__(self, user_id: UUID, agency_id: UUID | None = None):
        """
        Initialize service with user context.

        Args:
            user_id: The ID of the requesting user
            agency_id: Optional agency ID (will be fetched if not provided)
        """
        self.user_id = user_id
        self._agency_id = agency_id
        self._is_admin: bool | None = None
        self._user_cache: dict | None = None

    @property
    def agency_id(self) -> UUID:
        """Get the user's agency ID, fetching if necessary."""
        if self._agency_id is None:
            self._load_user_context()
        if self._agency_id is None:
            raise ValueError("Agency ID not available")
        return self._agency_id

    @property
    def is_admin(self) -> bool:
        """Check if the user is an admin."""
        if self._is_admin is None:
            self._load_user_context()
        if self._is_admin is None:
            return False
        return self._is_admin

    def _load_user_context(self) -> None:
        """Load user context from database."""
        # TODO: Implement Django ORM query
        # SELECT agency_id, is_admin, perm_level, role
        # FROM users WHERE id = self.user_id
        pass

    def _check_hierarchy_access(self, target_agent_id: UUID) -> bool:
        """
        Check if user has access to view/modify target agent.

        Admins can access anyone in their agency.
        Agents can access themselves and their downlines.

        Args:
            target_agent_id: The agent to check access for

        Returns:
            bool: True if access is allowed
        """
        if self.is_admin:
            # Admin can access anyone in their agency
            # TODO: Verify target is in same agency
            return True

        # Check if target is in user's downline
        # TODO: Implement downline check
        return target_agent_id == self.user_id

    def _execute_raw_sql(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        Execute raw SQL and return results as list of dicts.

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            List of row dictionaries
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
