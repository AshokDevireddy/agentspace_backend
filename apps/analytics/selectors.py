"""
Analytics Selectors

Complex analytics queries translated from Supabase RPC functions:
- get_analytics_split_view -> get_analytics_split_view()
- get_downline_production_distribution -> get_downline_production_distribution()
- get_analytics_from_deals_with_agency_id -> get_analytics_from_deals()
"""
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from django.db import connection
from django.db.models import Sum, Q, F, Value, Exists, OuterRef
from django_cte import With

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


def get_downline_production_distribution(
    agent_id: UUID,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> list[dict]:
    """
    Get production distribution for direct downlines.
    Uses django-cte for hierarchy traversal, ORM for aggregation.
    """
    from apps.core.models import User, Deal, DealHierarchySnapshot, StatusMapping

    def make_downline_cte(cte):
        base = User.objects.filter(upline_id=agent_id).values('id')
        recursive = cte.join(User, upline_id=cte.col.id).values('id')
        return base.union(recursive, all=True)

    cte = With.recursive(make_downline_cte)
    current_downline_ids = set(
        cte.queryset()
        .with_cte(cte)
        .values_list('id', flat=True)
    )

    snapshot_qs = DealHierarchySnapshot.objects.filter(upline_id=agent_id)

    deal_filter = Q()
    if start_date:
        deal_filter &= Q(deal__policy_effective_date__gte=start_date)
    if end_date:
        deal_filter &= Q(deal__policy_effective_date__lte=end_date)

    positive_status_subq = StatusMapping.objects.filter(
        carrier_id=OuterRef('deal__carrier_id'),
        raw_status=OuterRef('deal__status'),
        impact='positive'
    )

    snapshot_qs = (
        snapshot_qs
        .filter(deal_filter)
        .filter(Exists(positive_status_subq))
        .filter(deal__annual_premium__isnull=False, deal__annual_premium__gt=0)
    )

    production_by_agent = (
        snapshot_qs
        .values('agent_id')
        .annotate(total_production=Sum('deal__annual_premium'))
        .order_by('-total_production')
    )

    agent_ids = [p['agent_id'] for p in production_by_agent]
    agents = {
        u.id: u for u in
        User.objects.filter(id__in=agent_ids).select_related()
    }

    agents_with_downlines = set(
        User.objects.filter(upline_id__in=agent_ids).values_list('upline_id', flat=True).distinct()
    )

    result = []
    for prod in production_by_agent:
        agent = agents.get(prod['agent_id'])
        if not agent:
            continue
        result.append({
            'agent_id': str(prod['agent_id']),
            'agent_name': f"{agent.first_name or ''} {agent.last_name or ''}".strip(),
            'total_production': float(prod['total_production'] or 0),
            'is_clickable': prod['agent_id'] in current_downline_ids,
            'has_downlines': prod['agent_id'] in agents_with_downlines,
        })

    return result


def get_analytics_split_view(
    user: AuthenticatedUser,
    as_of: Optional[date] = None,
    all_time_months: int = 24,
    top_states: int = 5,
    carrier_ids: Optional[list[UUID]] = None,
) -> dict[str, Any]:
    """
    Get analytics split view data for the user.
    Translated from Supabase RPC: get_analytics_split_view

    This function returns comprehensive analytics data split into:
    - your_deals: Analytics for deals where user is the writing agent
    - downline: Analytics for deals from user's downlines

    Args:
        user: The authenticated user
        as_of: Reference date for calculations (default: today)
        all_time_months: Number of months for all-time window
        top_states: Number of top states to include in breakdown
        carrier_ids: Optional list of carrier IDs to filter by

    Returns:
        Dictionary with your_deals, downline, and metadata
    """
    if as_of is None:
        as_of = date.today()

    is_admin = user.is_admin or user.role == 'admin' or user.perm_level == 'admin'
    carrier_ids_array = [str(cid) for cid in carrier_ids] if carrier_ids else None

    with connection.cursor() as cursor:
        # Execute the complex analytics query
        # This is a simplified version - the full query is very complex
        cursor.execute("""
            WITH
            user_context AS (
                SELECT
                    u.id,
                    u.agency_id,
                    COALESCE(u.is_admin, false)
                        OR u.perm_level = 'admin'
                        OR u.role = 'admin' as is_admin
                FROM users u
                WHERE u.id = %s
                LIMIT 1
            ),
            as_of_month AS (
                SELECT make_date(
                    extract(year from %s::date)::int,
                    extract(month from %s::date)::int,
                    1
                ) as month_start
            ),
            -- YOUR DEALS (deals.agent_id = user_id)
            your_deals_base AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
                    d.annual_premium,
                    coalesce(nullif(btrim(d.state), ''), 'UNK') as state,
                    coalesce(d.age_band, 'UNK') as age_band,
                    nullif(btrim(d.status), '') as status_raw,
                    CASE
                        WHEN sm.impact = 'positive' THEN 'Active'
                        WHEN sm.impact = 'negative' THEN 'Inactive'
                        ELSE NULL
                    END as impact_class,
                    CASE
                        WHEN sm.placement = 'positive' THEN 'Placed'
                        WHEN sm.placement = 'negative' THEN 'Not Placed'
                        ELSE NULL
                    END as placement_class
                FROM deals d
                JOIN carriers c ON c.id = d.carrier_id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND sm.raw_status = d.status
                CROSS JOIN user_context uc
                CROSS JOIN as_of_month aom
                WHERE d.agency_id = uc.agency_id
                    AND d.agent_id = %s
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => %s - 1))
                            AND aom.month_start
                    AND (%s IS NULL OR d.carrier_id = ANY(%s::uuid[]))
            ),
            your_deals_summary AS (
                SELECT
                    count(*) as total_deals,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active_count,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive_count,
                    sum(CASE WHEN placement_class = 'Placed' THEN 1 ELSE 0 END) as placed_count,
                    sum(CASE WHEN placement_class = 'Not Placed' THEN 1 ELSE 0 END) as not_placed_count,
                    coalesce(sum(monthly_premium), 0) as total_premium,
                    coalesce(avg(monthly_premium), 0) as avg_premium
                FROM your_deals_base
            ),
            -- DOWNLINE DEALS (deals visible via deal_hierarchy_snapshot but NOT user's own)
            downline_deals_base AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
                    d.annual_premium,
                    coalesce(nullif(btrim(d.state), ''), 'UNK') as state,
                    coalesce(d.age_band, 'UNK') as age_band,
                    nullif(btrim(d.status), '') as status_raw,
                    CASE
                        WHEN sm.impact = 'positive' THEN 'Active'
                        WHEN sm.impact = 'negative' THEN 'Inactive'
                        ELSE NULL
                    END as impact_class,
                    CASE
                        WHEN sm.placement = 'positive' THEN 'Placed'
                        WHEN sm.placement = 'negative' THEN 'Not Placed'
                        ELSE NULL
                    END as placement_class
                FROM deals d
                JOIN carriers c ON c.id = d.carrier_id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND sm.raw_status = d.status
                CROSS JOIN user_context uc
                CROSS JOIN as_of_month aom
                WHERE d.agency_id = uc.agency_id
                    AND d.agent_id != %s
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => %s - 1))
                            AND aom.month_start
                    AND (%s IS NULL OR d.carrier_id = ANY(%s::uuid[]))
                    AND (
                        uc.is_admin
                        OR d.id IN (
                            SELECT deal_id
                            FROM deal_hierarchy_snapshot
                            WHERE agent_id = %s
                        )
                    )
            ),
            downline_summary AS (
                SELECT
                    count(*) as total_deals,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active_count,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive_count,
                    sum(CASE WHEN placement_class = 'Placed' THEN 1 ELSE 0 END) as placed_count,
                    sum(CASE WHEN placement_class = 'Not Placed' THEN 1 ELSE 0 END) as not_placed_count,
                    coalesce(sum(monthly_premium), 0) as total_premium,
                    coalesce(avg(monthly_premium), 0) as avg_premium
                FROM downline_deals_base
            ),
            -- Carrier breakdown for your deals
            your_carrier_breakdown AS (
                SELECT
                    carrier_id,
                    carrier_name,
                    count(*) as deal_count,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive,
                    coalesce(sum(monthly_premium), 0) as premium
                FROM your_deals_base
                GROUP BY carrier_id, carrier_name
                ORDER BY deal_count DESC
            ),
            -- Carrier breakdown for downline
            downline_carrier_breakdown AS (
                SELECT
                    carrier_id,
                    carrier_name,
                    count(*) as deal_count,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive,
                    coalesce(sum(monthly_premium), 0) as premium
                FROM downline_deals_base
                GROUP BY carrier_id, carrier_name
                ORDER BY deal_count DESC
            )
            SELECT
                -- Your deals summary
                (SELECT row_to_json(yds) FROM your_deals_summary yds) as your_deals_summary,
                -- Downline summary
                (SELECT row_to_json(ds) FROM downline_summary ds) as downline_summary,
                -- Your carrier breakdown
                (SELECT coalesce(json_agg(row_to_json(ycb)), '[]'::json) FROM your_carrier_breakdown ycb) as your_carrier_breakdown,
                -- Downline carrier breakdown
                (SELECT coalesce(json_agg(row_to_json(dcb)), '[]'::json) FROM downline_carrier_breakdown dcb) as downline_carrier_breakdown,
                -- Metadata
                (SELECT json_build_object(
                    'as_of', %s::date,
                    'all_time_months', %s,
                    'user_id', %s,
                    'is_admin', (SELECT is_admin FROM user_context)
                )) as metadata
        """, [
            str(user.id),
            as_of, as_of,
            str(user.id),
            all_time_months,
            carrier_ids_array, carrier_ids_array,
            str(user.id),
            all_time_months,
            carrier_ids_array, carrier_ids_array,
            str(user.id),
            as_of,
            all_time_months,
            str(user.id),
        ])

        row = cursor.fetchone()

    if not row:
        return {
            'your_deals': _empty_summary(),
            'downline': _empty_summary(),
            'metadata': {
                'as_of': as_of.isoformat(),
                'all_time_months': all_time_months,
            },
        }

    (
        your_summary_raw,
        downline_summary_raw,
        your_carriers,
        downline_carriers,
        metadata_raw,
    ) = row
    your_summary = your_summary_raw or {}
    downline_summary = downline_summary_raw or {}
    your_carriers = your_carriers or []
    downline_carriers = downline_carriers or []
    metadata = metadata_raw or {}

    # Calculate persistency rates
    your_total = (your_summary.get('active_count', 0) or 0) + (your_summary.get('inactive_count', 0) or 0)
    your_persistency = round(
        100 * (your_summary.get('active_count', 0) or 0) / your_total, 2
    ) if your_total > 0 else 0

    downline_total = (downline_summary.get('active_count', 0) or 0) + (downline_summary.get('inactive_count', 0) or 0)
    downline_persistency = round(
        100 * (downline_summary.get('active_count', 0) or 0) / downline_total, 2
    ) if downline_total > 0 else 0

    return {
        'your_deals': {
            'total_deals': your_summary.get('total_deals', 0) or 0,
            'active_count': your_summary.get('active_count', 0) or 0,
            'inactive_count': your_summary.get('inactive_count', 0) or 0,
            'placed_count': your_summary.get('placed_count', 0) or 0,
            'not_placed_count': your_summary.get('not_placed_count', 0) or 0,
            'total_premium': float(your_summary.get('total_premium', 0) or 0),
            'avg_premium': float(your_summary.get('avg_premium', 0) or 0),
            'persistency_rate': your_persistency,
            'carriers': your_carriers,
        },
        'downline': {
            'total_deals': downline_summary.get('total_deals', 0) or 0,
            'active_count': downline_summary.get('active_count', 0) or 0,
            'inactive_count': downline_summary.get('inactive_count', 0) or 0,
            'placed_count': downline_summary.get('placed_count', 0) or 0,
            'not_placed_count': downline_summary.get('not_placed_count', 0) or 0,
            'total_premium': float(downline_summary.get('total_premium', 0) or 0),
            'avg_premium': float(downline_summary.get('avg_premium', 0) or 0),
            'persistency_rate': downline_persistency,
            'carriers': downline_carriers,
        },
        'metadata': {
            'as_of': metadata.get('as_of', as_of.isoformat()),
            'all_time_months': metadata.get('all_time_months', all_time_months),
        },
    }


def get_analytics_from_deals(
    agency_id: UUID,
    as_of: Optional[date] = None,
    all_time_months: int = 24,
    carrier_ids: Optional[list[UUID]] = None,
) -> dict[str, Any]:
    """
    Get analytics from deals for an agency.
    Translated from Supabase RPC: get_analytics_from_deals_with_agency_id

    Args:
        agency_id: The agency ID to get analytics for
        as_of: Reference date for calculations (default: today)
        all_time_months: Number of months for all-time window
        carrier_ids: Optional list of carrier IDs to filter by

    Returns:
        Dictionary with comprehensive analytics data
    """
    if as_of is None:
        as_of = date.today()

    carrier_ids_array = [str(cid) for cid in carrier_ids] if carrier_ids else None

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH
            as_of_month AS (
                SELECT make_date(
                    extract(year from %s::date)::int,
                    extract(month from %s::date)::int,
                    1
                ) as month_start
            ),
            base AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
                    d.annual_premium,
                    coalesce(nullif(btrim(d.state), ''), 'UNK') as state,
                    coalesce(d.age_band, 'UNK') as age_band,
                    nullif(btrim(d.status), '') as status_raw,
                    CASE
                        WHEN sm.impact = 'positive' THEN 'Active'
                        WHEN sm.impact = 'negative' THEN 'Inactive'
                        ELSE NULL
                    END as impact_class
                FROM deals d
                JOIN carriers c ON c.id = d.carrier_id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND sm.raw_status = d.status
                CROSS JOIN as_of_month aom
                WHERE d.agency_id = %s
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => %s - 1))
                            AND aom.month_start
                    AND (%s IS NULL OR d.carrier_id = ANY(%s::uuid[]))
            ),
            summary AS (
                SELECT
                    count(*) as total_deals,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active_count,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive_count,
                    coalesce(sum(monthly_premium), 0) as total_premium,
                    coalesce(avg(monthly_premium), 0) as avg_premium
                FROM base
            ),
            carrier_breakdown AS (
                SELECT
                    carrier_id,
                    carrier_name,
                    count(*) as deal_count,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive,
                    coalesce(sum(monthly_premium), 0) as premium
                FROM base
                GROUP BY carrier_id, carrier_name
                ORDER BY deal_count DESC
            ),
            state_breakdown AS (
                SELECT
                    state,
                    count(*) as deal_count,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive
                FROM base
                GROUP BY state
                ORDER BY deal_count DESC
                LIMIT 10
            ),
            monthly_trend AS (
                SELECT
                    to_char(effective_month, 'YYYY-MM') as month,
                    count(*) as deal_count,
                    sum(CASE WHEN impact_class = 'Active' THEN 1 ELSE 0 END) as active,
                    sum(CASE WHEN impact_class = 'Inactive' THEN 1 ELSE 0 END) as inactive,
                    coalesce(sum(monthly_premium), 0) as premium
                FROM base
                GROUP BY effective_month
                ORDER BY effective_month
            )
            SELECT
                (SELECT row_to_json(s) FROM summary s) as summary,
                (SELECT coalesce(json_agg(row_to_json(cb)), '[]'::json) FROM carrier_breakdown cb) as carriers,
                (SELECT coalesce(json_agg(row_to_json(sb)), '[]'::json) FROM state_breakdown sb) as states,
                (SELECT coalesce(json_agg(row_to_json(mt)), '[]'::json) FROM monthly_trend mt) as monthly_trend
        """, [
            as_of, as_of,
            str(agency_id),
            all_time_months,
            carrier_ids_array, carrier_ids_array,
        ])

        row = cursor.fetchone()

    if not row:
        return {
            'summary': _empty_agency_summary(),
            'carriers': [],
            'states': [],
            'monthly_trend': [],
            'metadata': {
                'as_of': as_of.isoformat(),
                'all_time_months': all_time_months,
                'agency_id': str(agency_id),
            },
        }

    summary_raw, carriers, states, monthly_trend = row
    summary = summary_raw or {}
    carriers = carriers or []
    states = states or []
    monthly_trend = monthly_trend or []

    # Calculate persistency rate
    total = (summary.get('active_count', 0) or 0) + (summary.get('inactive_count', 0) or 0)
    persistency = round(
        100 * (summary.get('active_count', 0) or 0) / total, 2
    ) if total > 0 else 0

    return {
        'summary': {
            'total_deals': summary.get('total_deals', 0) or 0,
            'active_count': summary.get('active_count', 0) or 0,
            'inactive_count': summary.get('inactive_count', 0) or 0,
            'total_premium': float(summary.get('total_premium', 0) or 0),
            'avg_premium': float(summary.get('avg_premium', 0) or 0),
            'persistency_rate': persistency,
        },
        'carriers': carriers,
        'states': states,
        'monthly_trend': monthly_trend,
        'metadata': {
            'as_of': as_of.isoformat(),
            'all_time_months': all_time_months,
            'agency_id': str(agency_id),
        },
    }


def get_persistency_analytics(
    agency_id: UUID,
    as_of: Optional[date] = None,
    carrier_id: Optional[UUID] = None,
) -> dict[str, Any]:
    """
    Get persistency analytics for an agency.
    Wraps the existing analyze_persistency_for_deals RPC.

    Args:
        agency_id: The agency ID
        as_of: Reference date (default: today)
        carrier_id: Optional carrier ID to filter by

    Returns:
        Persistency analytics data
    """
    if as_of is None:
        as_of = date.today()

    with connection.cursor() as cursor:
        # Call the existing RPC function directly since it's complex
        cursor.execute("""
            SELECT analyze_persistency_for_deals(%s, %s, %s)
        """, [str(agency_id), as_of, str(carrier_id) if carrier_id else None])

        row = cursor.fetchone()

    if not row or not row[0]:
        return {
            'carriers': [],
            'overall_analytics': {},
            'carrier_comparison': {},
        }

    return row[0]


def _empty_summary() -> dict:
    """Return empty summary structure."""
    return {
        'total_deals': 0,
        'active_count': 0,
        'inactive_count': 0,
        'placed_count': 0,
        'not_placed_count': 0,
        'total_premium': 0,
        'avg_premium': 0,
        'persistency_rate': 0,
        'carriers': [],
    }


def _empty_agency_summary() -> dict:
    """Return empty agency summary structure."""
    return {
        'total_deals': 0,
        'active_count': 0,
        'inactive_count': 0,
        'total_premium': 0,
        'avg_premium': 0,
        'persistency_rate': 0,
    }
