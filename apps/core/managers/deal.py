"""
Deal QuerySet and Manager for optimized deal queries.
"""
from datetime import date
from uuid import UUID

from django.db import models
from django.db.models import Count, Q, Sum
from django.db.models.functions import Coalesce

from apps.core.querysets import HierarchyQuerySetMixin, ViewModeQuerySetMixin


class DealQuerySet(HierarchyQuerySetMixin, ViewModeQuerySetMixin, models.QuerySet):
    """
    Custom QuerySet for Deal model with hierarchy and visibility support.
    """

    def active(self):
        """Filter to active deals only."""
        return self.filter(status_standardized='active')

    def pending(self):
        """Filter to pending deals."""
        return self.filter(status_standardized='pending')

    def positive_impact(self):
        """Filter to deals with positive or neutral status impact."""
        return self.filter(
            status_standardized__in=['active', 'pending']
        )

    def negative_impact(self):
        """Filter to deals with negative status impact (lapsed, cancelled, terminated)."""
        return self.filter(
            status_standardized__in=['lapsed', 'cancelled', 'terminated']
        )

    def in_date_range(self, date_from: date | None = None, date_to: date | None = None):
        """
        Filter by policy effective date range.

        Args:
            date_from: Start date (inclusive)
            date_to: End date (inclusive)

        Returns:
            Filtered queryset
        """
        qs = self
        if date_from:
            qs = qs.filter(policy_effective_date__gte=date_from)
        if date_to:
            qs = qs.filter(policy_effective_date__lte=date_to)
        return qs

    def for_carrier(self, carrier_id: UUID):
        """Filter by carrier."""
        return self.filter(carrier_id=carrier_id)

    def for_product(self, product_id: UUID):
        """Filter by product."""
        return self.filter(product_id=product_id)

    def for_client(self, client_id: UUID):
        """Filter by client."""
        return self.filter(client_id=client_id)

    def for_agency(self, agency_id: UUID):
        """Filter to deals in a specific agency."""
        return self.filter(agency_id=agency_id)

    def with_relations(self):
        """
        Include commonly needed relations.

        Optimized chain for deal listing views.
        """
        return self.select_related(
            'client',
            'carrier',
            'product',
            'agent',
            'agent__position',
            'agency'
        )

    def with_agent_details(self):
        """Include agent and their position."""
        return self.select_related('agent', 'agent__position', 'agent__upline')

    def with_client_details(self):
        """Include client details."""
        return self.select_related('client')

    def with_carrier_product(self):
        """Include carrier and product."""
        return self.select_related('carrier', 'product')

    def search(self, query: str):
        """
        Search deals by policy number or client name.

        Args:
            query: Search string

        Returns:
            Filtered queryset
        """
        if not query:
            return self

        return self.filter(
            Q(policy_number__icontains=query) |
            Q(client__first_name__icontains=query) |
            Q(client__last_name__icontains=query)
        )

    def with_status_impact(self):
        """
        Annotate with status impact (positive, negative, neutral).

        Note: This is a simplified version. For carrier-specific mapping,
        use the StatusMapping model.
        """
        from django.db.models import Case, CharField, Value, When

        return self.annotate(
            status_impact=Case(
                When(status_standardized__in=['active', 'pending'], then=Value('positive')),
                When(status_standardized__in=['lapsed', 'cancelled', 'terminated'], then=Value('negative')),
                default=Value('neutral'),
                output_field=CharField()
            )
        )

    def carrier_summary(self):
        """
        Get summary by carrier.

        Returns queryset grouped by carrier with aggregations.
        """
        return self.values('carrier_id', 'carrier__name').annotate(
            total_premium=Coalesce(Sum('annual_premium'), 0),
            deal_count=Count('id'),
            active_count=Count('id', filter=Q(status_standardized='active'))
        ).order_by('-total_premium')

    def agent_summary(self):
        """
        Get summary by agent.

        Returns queryset grouped by agent with aggregations.
        """
        return self.values(
            'agent_id',
            'agent__first_name',
            'agent__last_name',
            'agent__email'
        ).annotate(
            total_premium=Coalesce(Sum('annual_premium'), 0),
            deal_count=Count('id'),
            active_count=Count('id', filter=Q(status_standardized='active'))
        ).order_by('-total_premium')

    def keyset_paginate(
        self,
        cursor_date: date | None = None,
        cursor_id: UUID | None = None,
        limit: int = 50,
        order: str = 'desc'
    ):
        """
        Apply keyset pagination.

        Args:
            cursor_date: Last seen policy_effective_date
            cursor_id: Last seen deal id
            limit: Number of records to return
            order: 'asc' or 'desc'

        Returns:
            Paginated queryset
        """
        qs = self

        if order == 'asc':
            order_by = ['policy_effective_date', 'id']
            if cursor_date and cursor_id:
                qs = qs.filter(
                    Q(policy_effective_date__gt=cursor_date) |
                    Q(policy_effective_date=cursor_date, id__gt=cursor_id)
                )
        else:
            order_by = ['-policy_effective_date', '-id']
            if cursor_date and cursor_id:
                qs = qs.filter(
                    Q(policy_effective_date__lt=cursor_date) |
                    Q(policy_effective_date=cursor_date, id__lt=cursor_id)
                )

        return qs.order_by(*order_by)[:limit + 1]  # +1 to check has_more


class DealManager(models.Manager):
    """
    Manager for Deal model with optimized queries.
    """

    def get_queryset(self):
        return DealQuerySet(self.model, using=self._db)

    def active(self):
        return self.get_queryset().active()

    def positive_impact(self):
        return self.get_queryset().positive_impact()

    def negative_impact(self):
        return self.get_queryset().negative_impact()

    def in_date_range(self, date_from=None, date_to=None):
        return self.get_queryset().in_date_range(date_from, date_to)

    def with_relations(self):
        return self.get_queryset().with_relations()

    def for_agency(self, agency_id):
        return self.get_queryset().for_agency(agency_id)

    def search(self, query: str):
        return self.get_queryset().search(query)

    def for_view_mode(self, user, view_mode='downlines'):
        return self.get_queryset().for_view_mode(user, view_mode)

    def visible_to(self, user):
        return self.get_queryset().visible_to(user)

    def in_downline_of(self, user_id, agency_id, max_depth=None, include_self=True):
        return self.get_queryset().in_downline_of(
            user_id, agency_id, max_depth, include_self
        )

    def keyset_paginate(self, cursor_date=None, cursor_id=None, limit=50, order='desc'):
        return self.get_queryset().keyset_paginate(cursor_date, cursor_id, limit, order)
