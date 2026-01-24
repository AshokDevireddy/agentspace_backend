"""
Hierarchy QuerySet Mixin for recursive CTE operations.

Uses django-cte 2.0 for recursive CTE support in Django ORM.
"""
from typing import Optional
from uuid import UUID

from django.db import models
from django.db.models import F, Value, IntegerField
from django_cte import With


class HierarchyQuerySetMixin:
    """
    Mixin providing hierarchy traversal methods using recursive CTEs.

    Works with any model that has a self-referential foreign key (upline_id)
    and agency_id for multi-tenancy.
    """

    def _get_downline_cte(
        self,
        user_id: UUID,
        agency_id: UUID,
        max_depth: Optional[int] = None,
    ):
        """
        Build a recursive CTE for downline traversal.

        Args:
            user_id: Root user ID to start traversal
            agency_id: Agency ID for multi-tenancy filtering
            max_depth: Maximum depth to traverse (None for unlimited)

        Returns:
            django_cte.With object representing the CTE
        """
        from apps.core.models import User

        # Base case: direct reports of the user
        def make_cte(cte):
            # Anchor: direct children of the user
            anchor = (
                User.objects.filter(
                    upline_id=user_id,
                    agency_id=agency_id
                )
                .annotate(depth=Value(1, output_field=IntegerField()))
                .values('id', 'depth')
            )

            # Recursive part: children of children
            if max_depth is not None:
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .filter(agency_id=agency_id)
                    .annotate(depth=cte.col.depth + 1)
                    .filter(depth__lt=max_depth + 1)
                    .values('id', 'depth')
                )
            else:
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .filter(agency_id=agency_id)
                    .annotate(depth=cte.col.depth + 1)
                    .values('id', 'depth')
                )

            return anchor.union(recursive, all=True)

        return With.recursive(make_cte)

    def _get_downline_ids(
        self,
        user_id: UUID,
        agency_id: UUID,
        max_depth: Optional[int] = None,
        include_self: bool = True
    ) -> list[UUID]:
        """
        Get downline IDs using recursive CTE.

        Args:
            user_id: Root user ID
            agency_id: Agency ID for multi-tenancy
            max_depth: Maximum depth to traverse
            include_self: Whether to include the user themselves

        Returns:
            List of user IDs in the downline
        """
        from apps.core.models import User

        cte = self._get_downline_cte(user_id, agency_id, max_depth)
        downline_ids = list(
            cte.queryset()
            .with_cte(cte)
            .values_list('id', flat=True)
        )

        if include_self:
            downline_ids.insert(0, user_id)

        return downline_ids

    def _get_upline_cte(self, user_id: UUID):
        """
        Build a recursive CTE for upline traversal.

        Args:
            user_id: Starting user ID

        Returns:
            django_cte.With object representing the CTE
        """
        from apps.core.models import User

        def make_cte(cte):
            # Anchor: the user themselves
            anchor = (
                User.objects.filter(id=user_id)
                .annotate(depth=Value(0, output_field=IntegerField()))
                .values('id', 'upline_id', 'depth')
            )

            # Recursive: follow upline chain
            recursive = (
                cte.join(User, id=cte.col.upline_id)
                .annotate(depth=cte.col.depth + 1)
                .values('id', 'upline_id', 'depth')
            )

            return anchor.union(recursive, all=True)

        return With.recursive(make_cte)

    def _get_upline_ids(self, user_id: UUID, include_self: bool = False) -> list[UUID]:
        """
        Get upline chain IDs using recursive CTE.

        Args:
            user_id: Starting user ID
            include_self: Whether to include the user themselves

        Returns:
            List of user IDs in the upline chain (ordered by proximity)
        """
        cte = self._get_upline_cte(user_id)
        query = cte.queryset().with_cte(cte).order_by('depth')

        if not include_self:
            query = query.exclude(id=user_id)

        return list(query.values_list('id', flat=True))

    def in_downline_of(
        self,
        user_id: UUID,
        agency_id: UUID,
        max_depth: Optional[int] = None,
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
        model = self.model
        if hasattr(model, 'agent_id'):
            # For Deal, Conversation, etc.
            return self.filter(agent_id__in=downline_ids)
        else:
            # For User model
            return self.filter(id__in=downline_ids)

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

        model = self.model
        if hasattr(model, 'agent_id'):
            return self.filter(agent_id__in=upline_ids)
        else:
            return self.filter(id__in=upline_ids)
