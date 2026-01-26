"""
Visibility QuerySet Mixin for view mode filtering.

Handles 'self', 'downlines', and 'all' view modes based on user role.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from apps.core.authentication import AuthenticatedUser


class ViewModeQuerySetMixin:
    """
    Mixin providing view mode filtering based on user role and hierarchy.

    Supports three view modes:
    - 'self': Only user's own records
    - 'downlines': User + their downline (default)
    - 'all': All agency records (admin only)
    """

    def for_view_mode(
        self,
        user: 'AuthenticatedUser',
        view_mode: str = 'downlines',
        agent_field: str = 'agent_id'
    ):
        """
        Filter by view mode based on user role.

        Args:
            user: The authenticated user
            view_mode: 'self', 'downlines', or 'all'
            agent_field: The field name to filter on (default: 'agent_id')

        Returns:
            Filtered queryset
        """
        is_admin = user.is_administrator

        if view_mode == 'self':
            # Only user's own records
            return self.filter(**{agent_field: user.id})

        elif view_mode == 'all' and is_admin:
            # Admin viewing all agency records
            return self.filter(agency_id=user.agency_id)

        else:
            # Default: user + downlines
            if hasattr(self, 'in_downline_of'):
                return self.in_downline_of(
                    user_id=user.id,
                    agency_id=user.agency_id,
                    include_self=True
                )
            else:
                # Fallback if HierarchyQuerySetMixin not available
                from apps.core.permissions import get_visible_agent_ids
                visible_ids = get_visible_agent_ids(
                    user, include_full_agency=is_admin
                )
                return self.filter(**{f'{agent_field}__in': visible_ids})

    def visible_to(self, user: 'AuthenticatedUser', agent_field: str = 'agent_id'):
        """
        Filter to records visible based on user role.

        Admins see all in agency, agents see self + downlines.

        Args:
            user: The authenticated user
            agent_field: The field name to filter on

        Returns:
            Filtered queryset
        """
        is_admin = user.is_administrator

        if is_admin:
            return self.filter(agency_id=user.agency_id)
        else:
            return self.for_view_mode(user, view_mode='downlines', agent_field=agent_field)

    def for_agent(self, agent_id, agent_field: str = 'agent_id'):
        """
        Filter to records for a specific agent.

        Args:
            agent_id: The agent's ID
            agent_field: The field name to filter on

        Returns:
            Filtered queryset
        """
        return self.filter(**{agent_field: agent_id})
