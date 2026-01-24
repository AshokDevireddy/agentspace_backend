"""
Hierarchy Service (P1-016)

Provides methods for navigating and validating the agent hierarchy.
Uses django-cte 2.0 for recursive CTEs.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db.models import F, Value, IntegerField
from django_cte import With

logger = logging.getLogger(__name__)


class HierarchyService:
    """
    Service for managing agent hierarchy relationships.

    All methods operate within the context of a single agency for multi-tenancy.
    """

    @staticmethod
    def get_downline(
        user_id: UUID,
        agency_id: UUID,
        max_depth: int | None = None,
        include_self: bool = False
    ) -> list[UUID]:
        """
        Get all agents in a user's downline (recursive).

        Uses django-cte for efficient hierarchy traversal.
        """
        from apps.core.models import User

        if max_depth is not None:
            max_depth = int(max_depth)
            if max_depth < 1:
                max_depth = None

        def make_cte(cte):
            base = (
                User.objects
                .filter(upline_id=user_id, agency_id=agency_id)
                .annotate(depth=Value(1, output_field=IntegerField()))
                .values('id', 'depth')
            )
            if max_depth is not None:
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .filter(agency_id=agency_id)
                    .filter(**{f'{cte.col.depth}__lt': max_depth})
                    .annotate(depth=cte.col.depth + 1)
                    .values('id', 'depth')
                )
            else:
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .filter(agency_id=agency_id)
                    .annotate(depth=cte.col.depth + 1)
                    .values('id', 'depth')
                )
            return base.union(recursive, all=True)

        cte = With.recursive(make_cte)
        result = list(
            cte.queryset()
            .with_cte(cte)
            .values_list('id', flat=True)
        )

        if include_self:
            result.insert(0, user_id)

        return result

    @staticmethod
    def get_upline_chain(user_id: UUID) -> list[UUID]:
        """
        Get the chain of uplines from a user to the root.

        Returns list of user IDs from direct upline to root (ordered by proximity).
        """
        from apps.core.models import User

        def make_cte(cte):
            base = (
                User.objects
                .filter(id=user_id)
                .annotate(depth=Value(1, output_field=IntegerField()))
                .values('id', 'upline_id', 'depth')
            )
            recursive = (
                cte.join(User, id=cte.col.upline_id)
                .annotate(depth=cte.col.depth + 1)
                .values('id', 'upline_id', 'depth')
            )
            return base.union(recursive, all=True)

        cte = With.recursive(make_cte)
        result = list(
            cte.queryset()
            .with_cte(cte)
            .filter(**{f'{cte.col.depth}__gt': 1})
            .order_by(cte.col.depth)
            .values_list('id', flat=True)
        )
        return result

    @staticmethod
    def get_visible_agent_ids(
        user_id: UUID,
        agency_id: UUID,
        is_admin: bool = False,
        include_full_agency: bool = False
    ) -> list[UUID]:
        """
        Get list of agent IDs visible to a user based on their role and hierarchy.
        """
        from apps.core.models import User

        if include_full_agency and is_admin:
            return list(
                User.objects
                .filter(agency_id=agency_id)
                .exclude(role='client')
                .values_list('id', flat=True)
            )

        def make_cte(cte):
            base = (
                User.objects
                .filter(id=user_id)
                .values('id')
            )
            recursive = (
                cte.join(User, upline_id=cte.col.id)
                .filter(agency_id=agency_id)
                .values('id')
            )
            return base.union(recursive, all=True)

        cte = With.recursive(make_cte)
        return list(
            cte.queryset()
            .with_cte(cte)
            .values_list('id', flat=True)
        )

    @staticmethod
    def is_in_hierarchy(
        user_id: UUID,
        target_id: UUID,
        agency_id: UUID,
        direction: str = 'downline'
    ) -> bool:
        """
        Check if a target user is in the user's hierarchy.
        """
        from apps.core.models import User

        if str(user_id) == str(target_id):
            return True

        if direction == 'downline':
            def make_cte(cte):
                base = User.objects.filter(id=user_id).values('id')
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .filter(agency_id=agency_id)
                    .values('id')
                )
                return base.union(recursive, all=True)

            cte = With.recursive(make_cte)
            return (
                cte.queryset()
                .with_cte(cte)
                .filter(**{f'{cte.col.id}': target_id})
                .exists()
            )
        else:
            def make_cte(cte):
                base = (
                    User.objects
                    .filter(id=user_id)
                    .values('id', 'upline_id')
                )
                recursive = (
                    cte.join(User, id=cte.col.upline_id)
                    .values('id', 'upline_id')
                )
                return base.union(recursive, all=True)

            cte = With.recursive(make_cte)
            return (
                cte.queryset()
                .with_cte(cte)
                .filter(**{f'{cte.col.id}': target_id})
                .exists()
            )

    @staticmethod
    def can_access_user(
        requesting_user_id: UUID,
        requesting_user_agency_id: UUID,
        requesting_user_is_admin: bool,
        target_user_id: UUID
    ) -> bool:
        """
        Check if a user can access another user's data.

        Rules:
        - Users can always access their own data
        - Admins can access anyone in their agency
        - Agents can access their downlines
        """
        from apps.core.models import User

        if str(requesting_user_id) == str(target_user_id):
            return True

        if requesting_user_is_admin:
            return User.objects.filter(
                id=target_user_id,
                agency_id=requesting_user_agency_id
            ).exists()

        return HierarchyService.is_in_hierarchy(
            requesting_user_id,
            target_user_id,
            requesting_user_agency_id,
            direction='downline'
        )

    @staticmethod
    def get_hierarchy_depth(user_id: UUID, agency_id: UUID) -> int:
        """
        Get the depth of a user in the hierarchy (distance from root).
        """
        from apps.core.models import User

        def make_cte(cte):
            base = (
                User.objects
                .filter(id=user_id, agency_id=agency_id)
                .annotate(depth=Value(0, output_field=IntegerField()))
                .values('id', 'upline_id', 'depth')
            )
            recursive = (
                cte.join(User, id=cte.col.upline_id)
                .filter(agency_id=agency_id)
                .annotate(depth=cte.col.depth + 1)
                .values('id', 'upline_id', 'depth')
            )
            return base.union(recursive, all=True)

        cte = With.recursive(make_cte)
        result = (
            cte.queryset()
            .with_cte(cte)
            .order_by(f'-{cte.col.depth}')
            .values_list('depth', flat=True)
            .first()
        )
        return result if result is not None else 0

    @staticmethod
    def validate_upline_assignment(
        agent_id: UUID,
        new_upline_id: UUID,
        agency_id: UUID
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that an upline assignment won't create a cycle.
        """
        from apps.core.models import User

        if str(agent_id) == str(new_upline_id):
            return False, "An agent cannot be their own upline"

        if HierarchyService.is_in_hierarchy(
            agent_id, new_upline_id, agency_id, direction='downline'
        ):
            return False, "Cannot assign upline that is in agent's downline (would create cycle)"

        if not User.objects.filter(id=new_upline_id, agency_id=agency_id).exists():
            return False, "New upline not found in agency"

        return True, None
