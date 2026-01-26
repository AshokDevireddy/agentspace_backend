"""
Hierarchy QuerySet Mixin for recursive CTE operations.

Delegates to centralized hierarchy module for actual traversal logic.
"""
from typing import TYPE_CHECKING
from uuid import UUID

from apps.core.hierarchy import get_downline_ids, get_upline_ids

if TYPE_CHECKING:
    from django.db.models import Model


class HierarchyQuerySetMixin:
    """
    Mixin providing hierarchy traversal methods for QuerySets.

    Works with any model that has a self-referential foreign key (upline_id)
    and agency_id for multi-tenancy.

    Delegates to apps.core.hierarchy for the actual CTE operations.
    """

    def _get_downline_ids(
        self,
        user_id: UUID,
        agency_id: UUID,
        max_depth: int | None = None,
        include_self: bool = True
    ) -> list[UUID]:
        """
        Get downline IDs using centralized hierarchy module.

        Args:
            user_id: Root user ID
            agency_id: Agency ID for multi-tenancy
            max_depth: Maximum depth to traverse
            include_self: Whether to include the user themselves

        Returns:
            List of user IDs in the downline
        """
        return get_downline_ids(user_id, agency_id, max_depth, include_self)

    def _get_upline_ids(self, user_id: UUID, include_self: bool = False) -> list[UUID]:
        """
        Get upline chain IDs using centralized hierarchy module.

        Args:
            user_id: Starting user ID
            include_self: Whether to include the user themselves

        Returns:
            List of user IDs in the upline chain (ordered by proximity)
        """
        return get_upline_ids(user_id, include_self)

    def in_downline_of(
        self,
        user_id: UUID,
        agency_id: UUID,
        max_depth: int | None = None,
        include_self: bool = True
    ):
        """
        Filter queryset to records in user's downline.

        For User model: filters users who are in the downline.
        For Deal model: filters deals where agent_id is in the downline.

        Args:
            user_id: Root user ID
            agency_id: Agency ID for multi-tenancy
            max_depth: Maximum depth to traverse
            include_self: Whether to include the user themselves

        Returns:
            Filtered queryset
        """
        downline_ids = self._get_downline_ids(
            user_id, agency_id, max_depth, include_self
        )

        # Determine the field to filter on based on model
        model: Model = self.model  # type: ignore[attr-defined]
        if hasattr(model, 'agent_id'):
            # For Deal, Conversation, etc.
            return self.filter(agent_id__in=downline_ids)  # type: ignore[attr-defined]
        else:
            # For User model
            return self.filter(id__in=downline_ids)  # type: ignore[attr-defined]

    def in_upline_of(self, user_id: UUID, include_self: bool = False):
        """
        Filter to records in upline chain.

        Args:
            user_id: Starting user ID
            include_self: Whether to include the user

        Returns:
            Filtered queryset
        """
        upline_ids = self._get_upline_ids(user_id, include_self)

        model: Model = self.model  # type: ignore[attr-defined]
        if hasattr(model, 'agent_id'):
            return self.filter(agent_id__in=upline_ids)  # type: ignore[attr-defined]
        else:
            return self.filter(id__in=upline_ids)  # type: ignore[attr-defined]
