"""
User QuerySet and Manager for optimized user queries.
"""
from django.db import models
from django.db.models import Count, Prefetch, Q, Subquery, OuterRef

from apps.core.querysets import HierarchyQuerySetMixin, ViewModeQuerySetMixin


class UserQuerySet(HierarchyQuerySetMixin, ViewModeQuerySetMixin, models.QuerySet):
    """
    Custom QuerySet for User model with hierarchy and visibility support.
    """

    def active(self):
        """Filter to active users only."""
        return self.filter(is_active=True, status='active')

    def agents_only(self):
        """Filter to users with agent role (excludes clients)."""
        return self.exclude(role='client')

    def without_position(self):
        """Filter to users without a position assigned."""
        return self.filter(position__isnull=True)

    def with_position(self):
        """Filter to users with a position assigned."""
        return self.filter(position__isnull=False)

    def with_downline_count(self):
        """
        Annotate with count of direct downlines.

        Note: This only counts direct downlines (one level).
        For recursive count, use get_downline() method on individual users.
        """
        return self.annotate(
            direct_downline_count=Count('downlines', distinct=True)
        )

    def with_deal_count(self):
        """Annotate with count of deals."""
        return self.annotate(
            deal_count=Count('deals', distinct=True)
        )

    def with_upline(self):
        """Include upline with select_related."""
        return self.select_related('upline', 'upline__position')

    def with_position_details(self):
        """Include position with select_related."""
        return self.select_related('position')

    def with_agency(self):
        """Include agency with select_related."""
        return self.select_related('agency')

    def with_relations(self):
        """
        Include commonly needed relations.

        Optimized chain for agent listing views.
        """
        return self.select_related(
            'upline',
            'upline__position',
            'position',
            'agency'
        )

    def for_agency(self, agency_id):
        """Filter to users in a specific agency."""
        return self.filter(agency_id=agency_id)

    def search(self, query: str):
        """
        Search users by name or email.

        Args:
            query: Search string

        Returns:
            Filtered queryset
        """
        if not query:
            return self

        return self.filter(
            Q(first_name__icontains=query) |
            Q(last_name__icontains=query) |
            Q(email__icontains=query)
        )

    def with_production_stats(self):
        """
        Annotate with production statistics.

        Adds total_production, deal_count, positive_deal_count.
        """
        from django.db.models import Sum
        from django.db.models.functions import Coalesce

        return self.annotate(
            total_production=Coalesce(
                Sum('deals__annual_premium', filter=Q(deals__status_standardized='active')),
                0
            ),
            deal_count=Count('deals', distinct=True),
            positive_deal_count=Count(
                'deals',
                filter=Q(deals__status_standardized__in=['active', 'pending']),
                distinct=True
            )
        )


class UserManager(models.Manager):
    """
    Manager for User model with optimized queries.
    """

    def get_queryset(self):
        return UserQuerySet(self.model, using=self._db)

    def active(self):
        return self.get_queryset().active()

    def agents_only(self):
        return self.get_queryset().agents_only()

    def without_position(self):
        return self.get_queryset().without_position()

    def with_relations(self):
        return self.get_queryset().with_relations()

    def for_agency(self, agency_id):
        return self.get_queryset().for_agency(agency_id)

    def search(self, query: str):
        return self.get_queryset().search(query)

    def for_view_mode(self, user, view_mode='downlines'):
        return self.get_queryset().for_view_mode(user, view_mode, agent_field='id')

    def visible_to(self, user):
        return self.get_queryset().visible_to(user, agent_field='id')

    def in_downline_of(self, user_id, agency_id, max_depth=None, include_self=True):
        return self.get_queryset().in_downline_of(
            user_id, agency_id, max_depth, include_self
        )
