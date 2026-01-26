"""
Analytics Service

Provides dashboard data, analytics, and reporting functionality.
Translated from Supabase RPC functions to Django ORM/raw SQL.

Priority: P1 - Analytics (P2-039)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.db import connection

from .base import BaseService
from .hierarchy_service import HierarchyService

logger = logging.getLogger(__name__)


# ============================================================================
# Data Transfer Objects (DTOs)
# ============================================================================

@dataclass
class CarrierActivePolicy:
    """Carrier breakdown in dashboard data."""
    carrier_id: UUID
    carrier: str
    active_policies: int


@dataclass
class DashboardYourDeals:
    """'Your Deals' section of dashboard."""
    active_policies: int
    monthly_commissions: Decimal
    new_policies: int
    total_clients: int
    carriers_active: list[CarrierActivePolicy]


@dataclass
class DashboardDownlineProduction:
    """'Downline Production' section of dashboard."""
    active_policies: int
    monthly_commissions: Decimal
    new_policies: int
    total_clients: int
    carriers_active: list[CarrierActivePolicy]


@dataclass
class DashboardData:
    """Complete dashboard data response."""
    your_deals: DashboardYourDeals
    downline_production: DashboardDownlineProduction


@dataclass
class DashboardDataWithRange:
    """Dashboard data with date range filtering."""
    your_deals: dict[str, Any]
    downline_production: dict[str, Any]
    leaderboard: list[dict[str, Any]]


@dataclass
class PersistencyData:
    """Persistency analytics data."""
    meta: dict[str, Any]
    series: list[dict[str, Any]]
    windows_by_carrier: dict[str, Any]
    totals: dict[str, Any]
    breakdowns_over_time: dict[str, Any]


@dataclass
class MonthlyStats:
    """Monthly statistics for book of business."""
    month: str
    deals_count: int
    total_premium: Decimal
    active_count: int
    inactive_count: int


# ============================================================================
# Analytics Service Implementation
# ============================================================================

class AnalyticsService(BaseService):
    """
    Service for analytics and dashboard operations.

    Handles:
    - Dashboard data retrieval
    - Production analytics
    - Persistency analysis
    - Leaderboard calculations
    """

    # ========================================================================
    # P1 - Dashboard Functions
    # ========================================================================

    def get_dashboard_data_with_agency_id(
        self,
        as_of_date: date | None = None
    ) -> DashboardData:
        """
        Get dashboard summary data for the current user.

        Args:
            as_of_date: Reference date for calculations (default: today)

        Returns:
            DashboardData: Complete dashboard summary
        """
        if as_of_date is None:
            as_of_date = date.today()

        one_week_ago = as_of_date - timedelta(days=7)

        # YOUR DEALS section - agent's personal production
        your_deals = self._get_your_deals_summary(as_of_date, one_week_ago)

        # DOWNLINE PRODUCTION section
        downline_production = self._get_downline_production_summary(as_of_date, one_week_ago)

        return DashboardData(
            your_deals=your_deals,
            downline_production=downline_production,
        )

    def _get_your_deals_summary(
        self,
        as_of_date: date,
        one_week_ago: date
    ) -> DashboardYourDeals:
        """Get summary for user's own deals."""
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH your_base AS (
                    SELECT
                        d.id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status,
                        d.policy_effective_date,
                        d.monthly_premium,
                        d.client_id,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m ON m.carrier_id = d.carrier_id
                        AND m.raw_status = d.status
                    WHERE d.agency_id = %s
                      AND d.agent_id = %s
                ),
                your_active AS (
                    SELECT * FROM your_base WHERE impact = 'positive'
                ),
                your_totals AS (
                    SELECT
                        COUNT(*)::int AS active_policies,
                        COALESCE(SUM(monthly_premium), 0) AS monthly_commissions,
                        COUNT(*) FILTER (WHERE policy_effective_date >= %s)::int AS new_policies,
                        COUNT(DISTINCT client_id)::int AS total_clients
                    FROM your_active
                ),
                carriers_breakdown AS (
                    SELECT
                        carrier_id,
                        carrier_name,
                        COUNT(*)::int as active_policies
                    FROM your_active
                    GROUP BY carrier_id, carrier_name
                    ORDER BY active_policies DESC
                )
                SELECT
                    (SELECT row_to_json(t) FROM your_totals t) as totals,
                    COALESCE(
                        (SELECT json_agg(row_to_json(c)) FROM carriers_breakdown c),
                        '[]'::json
                    ) as carriers
            """, [str(self.agency_id), str(self.user_id), one_week_ago])

            row = cursor.fetchone()
            if row and row[0]:
                totals = row[0]
                carriers_data = row[1] or []
            else:
                totals = {
                    'active_policies': 0,
                    'monthly_commissions': Decimal('0'),
                    'new_policies': 0,
                    'total_clients': 0,
                }
                carriers_data = []

        carriers_active = [
            CarrierActivePolicy(
                carrier_id=UUID(c['carrier_id']),
                carrier=c['carrier_name'],
                active_policies=c['active_policies']
            )
            for c in carriers_data
        ]

        return DashboardYourDeals(
            active_policies=totals.get('active_policies', 0),
            monthly_commissions=Decimal(str(totals.get('monthly_commissions', 0))),
            new_policies=totals.get('new_policies', 0),
            total_clients=totals.get('total_clients', 0),
            carriers_active=carriers_active,
        )

    def _get_downline_production_summary(
        self,
        as_of_date: date,
        one_week_ago: date
    ) -> DashboardDownlineProduction:
        """Get summary for downline production."""
        with connection.cursor() as cursor:
            # Check if user is admin
            cursor.execute("""
                SELECT is_admin, role, perm_level
                FROM users WHERE id = %s
            """, [str(self.user_id)])
            user_row = cursor.fetchone()
            is_admin = user_row and (
                user_row[0] or
                user_row[1] == 'admin' or
                user_row[2] == 'admin'
            )

            if is_admin:
                # Admin sees all agency deals except their own
                cursor.execute("""
                    WITH downline_base AS (
                        SELECT
                            d.id,
                            d.carrier_id,
                            c.name as carrier_name,
                            d.status,
                            d.policy_effective_date,
                            d.monthly_premium,
                            d.client_id,
                            COALESCE(m.impact, 'neutral') as impact
                        FROM deals d
                        JOIN carriers c ON c.id = d.carrier_id
                        LEFT JOIN status_mapping m ON m.carrier_id = d.carrier_id
                            AND m.raw_status = d.status
                        WHERE d.agency_id = %s
                          AND d.agent_id != %s
                    ),
                    downline_active AS (
                        SELECT * FROM downline_base WHERE impact = 'positive'
                    ),
                    downline_totals AS (
                        SELECT
                            COUNT(*)::int AS active_policies,
                            COALESCE(SUM(monthly_premium), 0) AS monthly_commissions,
                            COUNT(*) FILTER (WHERE policy_effective_date >= %s)::int AS new_policies,
                            COUNT(DISTINCT client_id)::int AS total_clients
                        FROM downline_active
                    ),
                    carriers_breakdown AS (
                        SELECT
                            carrier_id,
                            carrier_name,
                            COUNT(*)::int as active_policies
                        FROM downline_active
                        GROUP BY carrier_id, carrier_name
                        ORDER BY active_policies DESC
                    )
                    SELECT
                        (SELECT row_to_json(t) FROM downline_totals t) as totals,
                        COALESCE(
                            (SELECT json_agg(row_to_json(c)) FROM carriers_breakdown c),
                            '[]'::json
                        ) as carriers
                """, [str(self.agency_id), str(self.user_id), one_week_ago])
            else:
                # Agent sees deals from hierarchy snapshot (their downline)
                cursor.execute("""
                    WITH downline_agents AS (
                        SELECT DISTINCT dhs.agent_id
                        FROM deal_hierarchy_snapshots dhs
                        WHERE dhs.agent_id != %s
                          AND EXISTS (
                              SELECT 1 FROM deal_hierarchy_snapshots dhs2
                              WHERE dhs2.deal_id = dhs.deal_id
                                AND dhs2.agent_id = %s
                                AND dhs2.hierarchy_level < dhs.hierarchy_level
                          )
                    ),
                    downline_base AS (
                        SELECT
                            d.id,
                            d.carrier_id,
                            c.name as carrier_name,
                            d.status,
                            d.policy_effective_date,
                            d.monthly_premium,
                            d.client_id,
                            COALESCE(m.impact, 'neutral') as impact
                        FROM deals d
                        JOIN carriers c ON c.id = d.carrier_id
                        LEFT JOIN status_mapping m ON m.carrier_id = d.carrier_id
                            AND m.raw_status = d.status
                        WHERE d.agency_id = %s
                          AND d.agent_id IN (SELECT agent_id FROM downline_agents)
                    ),
                    downline_active AS (
                        SELECT * FROM downline_base WHERE impact = 'positive'
                    ),
                    downline_totals AS (
                        SELECT
                            COUNT(*)::int AS active_policies,
                            COALESCE(SUM(monthly_premium), 0) AS monthly_commissions,
                            COUNT(*) FILTER (WHERE policy_effective_date >= %s)::int AS new_policies,
                            COUNT(DISTINCT client_id)::int AS total_clients
                        FROM downline_active
                    ),
                    carriers_breakdown AS (
                        SELECT
                            carrier_id,
                            carrier_name,
                            COUNT(*)::int as active_policies
                        FROM downline_active
                        GROUP BY carrier_id, carrier_name
                        ORDER BY active_policies DESC
                    )
                    SELECT
                        (SELECT row_to_json(t) FROM downline_totals t) as totals,
                        COALESCE(
                            (SELECT json_agg(row_to_json(c)) FROM carriers_breakdown c),
                            '[]'::json
                        ) as carriers
                """, [str(self.user_id), str(self.user_id), str(self.agency_id), one_week_ago])

            row = cursor.fetchone()
            if row and row[0]:
                totals = row[0]
                carriers_data = row[1] or []
            else:
                totals = {
                    'active_policies': 0,
                    'monthly_commissions': Decimal('0'),
                    'new_policies': 0,
                    'total_clients': 0,
                }
                carriers_data = []

        carriers_active = [
            CarrierActivePolicy(
                carrier_id=UUID(c['carrier_id']),
                carrier=c['carrier_name'],
                active_policies=c['active_policies']
            )
            for c in carriers_data
        ]

        return DashboardDownlineProduction(
            active_policies=totals.get('active_policies', 0),
            monthly_commissions=Decimal(str(totals.get('monthly_commissions', 0))),
            new_policies=totals.get('new_policies', 0),
            total_clients=totals.get('total_clients', 0),
            carriers_active=carriers_active,
        )

    def get_dashboard_data_with_date_range(
        self,
        start_date: date,
        end_date: date,
        production_mode: str = 'submitted'
    ) -> DashboardDataWithRange:
        """
        Get dashboard data for a specific date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            production_mode: 'submitted' or 'issue_paid'

        Returns:
            DashboardDataWithRange: Dashboard data with range filtering
        """
        issue_paid_cutoff = date.today() - timedelta(days=7)

        with connection.cursor() as cursor:
            # Build the production filter
            if production_mode == 'issue_paid':
                production_filter = """
                    AND status_standardized = 'active'
                    AND policy_effective_date <= %s
                """
                filter_params = [issue_paid_cutoff]
            else:
                production_filter = ""
                filter_params = []

            # YOUR DEALS
            cursor.execute(f"""
                SELECT
                    COALESCE(SUM(annual_premium), 0) as production,
                    COUNT(*)::int as deals_count,
                    COUNT(*) FILTER (WHERE status_standardized = 'active')::int as active_policies,
                    COUNT(DISTINCT client_id)::int as total_clients
                FROM deals
                WHERE agent_id = %s
                  AND agency_id = %s
                  AND policy_effective_date BETWEEN %s AND %s
                  {production_filter}
            """, [
                str(self.user_id), str(self.agency_id),
                start_date, end_date, *filter_params
            ])
            your_row = cursor.fetchone()
            your_deals = {
                'production': float(your_row[0] or 0),
                'deals_count': your_row[1] or 0,
                'active_policies': your_row[2] or 0,
                'total_clients': your_row[3] or 0,
            }

            # AGENCY/DOWNLINE PRODUCTION
            cursor.execute(f"""
                SELECT
                    COALESCE(SUM(annual_premium), 0) as production,
                    COUNT(*)::int as deals_count,
                    COUNT(*) FILTER (WHERE status_standardized = 'active')::int as active_policies,
                    COUNT(DISTINCT client_id)::int as total_clients
                FROM deals
                WHERE agency_id = %s
                  AND policy_effective_date BETWEEN %s AND %s
                  {production_filter}
            """, [str(self.agency_id), start_date, end_date, *filter_params])
            agency_row = cursor.fetchone()
            downline_production = {
                'production': float(agency_row[0] or 0),
                'deals_count': agency_row[1] or 0,
                'active_policies': agency_row[2] or 0,
                'total_clients': agency_row[3] or 0,
            }

            # LEADERBOARD
            leaderboard = self.get_production_leaderboard(start_date, end_date)

        return DashboardDataWithRange(
            your_deals=your_deals,
            downline_production=downline_production,
            leaderboard=leaderboard,
        )

    # ========================================================================
    # P1 - Analytics Functions
    # ========================================================================

    def analyze_persistency_for_deals(
        self,
        as_of_date: date | None = None,
        carrier_id: UUID | None = None
    ) -> PersistencyData:
        """
        Analyze policy persistency (lapse rates) for deals.

        Args:
            as_of_date: Reference date (default: today)
            carrier_id: Optional carrier filter

        Returns:
            PersistencyData: Comprehensive persistency analysis
        """
        if as_of_date is None:
            as_of_date = date.today()

        carrier_filter = "AND d.carrier_id = %s" if carrier_id else ""
        carrier_params = [str(carrier_id)] if carrier_id else []

        with connection.cursor() as cursor:
            cursor.execute(f"""
                WITH base AS (
                    SELECT
                        d.id,
                        d.agency_id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status as raw_status,
                        d.policy_effective_date as issued_at,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m ON m.carrier_id = d.carrier_id
                        AND m.raw_status = d.status
                    WHERE d.agency_id = %s
                      AND d.policy_effective_date IS NOT NULL
                      {carrier_filter}
                ),
                buckets AS (
                    SELECT '3' as bucket, interval '3 months' as window
                    UNION ALL SELECT '6', interval '6 months'
                    UNION ALL SELECT '9', interval '9 months'
                    UNION ALL SELECT 'All', null::interval
                ),
                scoped AS (
                    SELECT
                        b.bucket,
                        base.carrier_id,
                        base.carrier_name,
                        base.raw_status,
                        base.impact
                    FROM base
                    CROSS JOIN buckets b
                    WHERE b.window IS NULL
                       OR base.issued_at >= (%s::date - b.window)
                ),
                per_carrier_impact AS (
                    SELECT
                        bucket,
                        carrier_id,
                        carrier_name,
                        COUNT(*) FILTER (WHERE impact = 'positive') as positive,
                        COUNT(*) FILTER (WHERE impact = 'negative') as negative,
                        COUNT(*) FILTER (WHERE impact = 'neutral') as neutral,
                        COUNT(*) as total
                    FROM scoped
                    GROUP BY bucket, carrier_id, carrier_name
                ),
                per_carrier_pct AS (
                    SELECT
                        bucket,
                        carrier_id,
                        carrier_name,
                        positive,
                        negative,
                        neutral,
                        total,
                        CASE WHEN (positive + negative) > 0
                            THEN ROUND(positive::numeric / (positive + negative) * 100, 2)
                            ELSE 0
                        END as positive_pct,
                        CASE WHEN (positive + negative) > 0
                            THEN ROUND(negative::numeric / (positive + negative) * 100, 2)
                            ELSE 0
                        END as negative_pct
                    FROM per_carrier_impact
                ),
                totals AS (
                    SELECT
                        bucket,
                        SUM(positive)::int as total_positive,
                        SUM(negative)::int as total_negative,
                        SUM(neutral)::int as total_neutral,
                        SUM(total)::int as total_policies
                    FROM per_carrier_impact
                    GROUP BY bucket
                )
                SELECT
                    json_build_object(
                        'as_of_date', %s,
                        'agency_id', %s,
                        'carrier_id', %s
                    ) as meta,
                    COALESCE(
                        (SELECT json_agg(row_to_json(p)) FROM per_carrier_pct p),
                        '[]'::json
                    ) as series,
                    (SELECT json_object_agg(bucket, json_build_object(
                        'positive', total_positive,
                        'negative', total_negative,
                        'neutral', total_neutral,
                        'total', total_policies
                    )) FROM totals) as totals
            """, [
                str(self.agency_id), *carrier_params,
                as_of_date, as_of_date, str(self.agency_id),
                str(carrier_id) if carrier_id else None
            ])

            row = cursor.fetchone()

        meta = row[0] if row else {}
        series = row[1] if row and row[1] else []
        totals = row[2] if row and row[2] else {}

        # Transform series into windows_by_carrier format
        windows_by_carrier = {}
        for item in series:
            cid = item.get('carrier_id')
            if cid not in windows_by_carrier:
                windows_by_carrier[cid] = {
                    'carrier_name': item.get('carrier_name'),
                    'buckets': {}
                }
            windows_by_carrier[cid]['buckets'][item['bucket']] = {
                'positive': item['positive'],
                'negative': item['negative'],
                'neutral': item['neutral'],
                'total': item['total'],
                'positive_pct': float(item['positive_pct']),
                'negative_pct': float(item['negative_pct']),
            }

        return PersistencyData(
            meta=meta,
            series=series,
            windows_by_carrier=windows_by_carrier,
            totals=totals,
            breakdowns_over_time={},  # Can be extended if needed
        )

    def get_book_of_business_monthly_stats(
        self,
        as_of_date: date | None = None
    ) -> dict[str, Any]:
        """
        Get monthly statistics for book of business.

        Args:
            as_of_date: Reference date (default: today)

        Returns:
            dict: Monthly statistics with series data
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get visible agent IDs based on hierarchy
        visible_agents = HierarchyService.get_visible_agent_ids(
            self.user_id,
            self.agency_id,
            is_admin=self.is_admin,
            include_full_agency=self.is_admin
        )

        with connection.cursor() as cursor:
            cursor.execute("""
                WITH monthly AS (
                    SELECT
                        DATE_TRUNC('month', policy_effective_date) as month,
                        COUNT(*)::int as deals_count,
                        COALESCE(SUM(annual_premium), 0) as total_premium,
                        COUNT(*) FILTER (WHERE status_standardized = 'active')::int as active_count,
                        COUNT(*) FILTER (WHERE status_standardized != 'active')::int as inactive_count
                    FROM deals
                    WHERE agency_id = %s
                      AND agent_id = ANY(%s)
                      AND policy_effective_date IS NOT NULL
                      AND policy_effective_date >= %s - interval '12 months'
                    GROUP BY DATE_TRUNC('month', policy_effective_date)
                    ORDER BY month
                )
                SELECT
                    COALESCE(json_agg(json_build_object(
                        'month', TO_CHAR(month, 'YYYY-MM'),
                        'deals_count', deals_count,
                        'total_premium', total_premium,
                        'active_count', active_count,
                        'inactive_count', inactive_count
                    )), '[]'::json) as series,
                    SUM(deals_count)::int as total_deals,
                    SUM(total_premium) as total_premium,
                    SUM(active_count)::int as total_active,
                    SUM(inactive_count)::int as total_inactive
                FROM monthly
            """, [
                str(self.agency_id),
                [str(a) for a in visible_agents],
                as_of_date
            ])

            row = cursor.fetchone()

        return {
            'series': row[0] if row else [],
            'totals': {
                'deals_count': row[1] or 0,
                'total_premium': float(row[2] or 0),
                'active_count': row[3] or 0,
                'inactive_count': row[4] or 0,
            }
        }

    # ========================================================================
    # Leaderboard Functions
    # ========================================================================

    def get_production_leaderboard(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10
    ) -> list[dict[str, Any]]:
        """
        Get production leaderboard for date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            limit: Number of top producers to return

        Returns:
            List[dict]: Ranked list of top producers
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH agent_production AS (
                    SELECT
                        d.agent_id,
                        u.first_name,
                        u.last_name,
                        u.email,
                        p.name as position_name,
                        COALESCE(SUM(d.annual_premium), 0) as production,
                        COUNT(*)::int as deals_count
                    FROM deals d
                    JOIN users u ON u.id = d.agent_id
                    LEFT JOIN positions p ON p.id = u.position_id
                    WHERE d.agency_id = %s
                      AND d.policy_effective_date BETWEEN %s AND %s
                    GROUP BY d.agent_id, u.first_name, u.last_name, u.email, p.name
                ),
                ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (ORDER BY production DESC) as rank
                    FROM agent_production
                )
                SELECT
                    rank,
                    agent_id,
                    CONCAT(first_name, ' ', last_name) as agent_name,
                    position_name,
                    production,
                    deals_count
                FROM ranked
                WHERE rank <= %s
                ORDER BY rank
            """, [str(self.agency_id), start_date, end_date, limit])

            columns = ['rank', 'agent_id', 'agent_name', 'position', 'production', 'deals_count']
            return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]

    def get_user_rank(
        self,
        start_date: date,
        end_date: date
    ) -> dict[str, Any]:
        """
        Get the current user's rank and production.

        Args:
            start_date: Start of date range
            end_date: End of date range

        Returns:
            dict: User's rank and production info
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH agent_production AS (
                    SELECT
                        agent_id,
                        COALESCE(SUM(annual_premium), 0) as production,
                        COUNT(*)::int as deals_count
                    FROM deals
                    WHERE agency_id = %s
                      AND policy_effective_date BETWEEN %s AND %s
                    GROUP BY agent_id
                ),
                ranked AS (
                    SELECT
                        agent_id,
                        production,
                        deals_count,
                        ROW_NUMBER() OVER (ORDER BY production DESC) as rank
                    FROM agent_production
                )
                SELECT rank, production, deals_count
                FROM ranked
                WHERE agent_id = %s
            """, [str(self.agency_id), start_date, end_date, str(self.user_id)])

            row = cursor.fetchone()

        if row:
            return {
                'rank': row[0],
                'production': float(row[1]),
                'deals_count': row[2],
            }
        return {
            'rank': None,
            'production': 0,
            'deals_count': 0,
        }


# ============================================================================
# Type Aliases for External Use
# ============================================================================

AnalyticsServiceResult = DashboardData | DashboardDataWithRange | PersistencyData
