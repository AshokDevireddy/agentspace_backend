"""
Analytics Selectors

Complex analytics queries translated from Supabase RPC functions:
- get_analytics_split_view -> get_analytics_split_view()
- get_downline_production_distribution -> get_downline_production_distribution()
- get_analytics_from_deals_with_agency_id -> get_analytics_from_deals()
"""
import logging
from datetime import date
from typing import Any
from uuid import UUID

from django.db import connection
from django.db.models import Exists, OuterRef, Q, Sum
from django_cte import With

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


def get_downline_production_distribution(
    agent_id: UUID,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """
    Get production distribution for direct downlines.
    Uses django-cte for hierarchy traversal, ORM for aggregation.
    """
    from apps.core.models import DealHierarchySnapshot, StatusMapping, User

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

    snapshot_qs = DealHierarchySnapshot.objects.filter(upline_id=agent_id)  # type: ignore[attr-defined]

    deal_filter = Q()
    if start_date:
        deal_filter &= Q(deal__policy_effective_date__gte=start_date)
    if end_date:
        deal_filter &= Q(deal__policy_effective_date__lte=end_date)

    positive_status_subq = StatusMapping.objects.filter(  # type: ignore[attr-defined]
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
        User.objects.filter(id__in=agent_ids).select_related()  # type: ignore[attr-defined]
    }

    agents_with_downlines = set(
        User.objects.filter(upline_id__in=agent_ids).values_list('upline_id', flat=True).distinct()  # type: ignore[attr-defined]
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
    as_of: date | None = None,
    all_time_months: int = 24,
    top_states: int = 50,
    carrier_ids: list[UUID] | None = None,
    include_series: bool = True,
    include_windows_by_carrier: bool = True,
    include_breakdowns_status: bool = True,
    include_breakdowns_state: bool = True,
    include_breakdowns_age: bool = True,
) -> dict[str, Any]:
    """
    Get analytics split view data for the user.
    Translated from Supabase RPC: get_analytics_split_view

    This function returns comprehensive analytics data split into:
    - your_deals: Analytics for deals where user is the writing agent
    - downline: Analytics for deals from user's downlines (or all agency if admin)

    Args:
        user: The authenticated user
        as_of: Reference date for calculations (default: today)
        all_time_months: Number of months for all-time window
        top_states: Number of top states to include in breakdown
        carrier_ids: Optional list of carrier IDs to filter by
        include_series: Include monthly time series data
        include_windows_by_carrier: Include window calculations per carrier
        include_breakdowns_status: Include status breakdown
        include_breakdowns_state: Include state breakdown
        include_breakdowns_age: Include age band breakdown

    Returns:
        Dictionary with your_deals, downline, and metadata matching RPC structure
    """
    if as_of is None:
        as_of = date.today()

    carrier_ids_array = [str(cid) for cid in carrier_ids] if carrier_ids else None

    # Call internal helper for your_deals
    your_deals_result = _get_analytics_for_scope(
        user=user,
        as_of=as_of,
        all_time_months=all_time_months,
        top_states=top_states,
        carrier_ids_array=carrier_ids_array,
        scope='your_deals',
        include_series=include_series,
        include_windows_by_carrier=include_windows_by_carrier,
        include_breakdowns_status=include_breakdowns_status,
        include_breakdowns_state=include_breakdowns_state,
        include_breakdowns_age=include_breakdowns_age,
    )

    # Call internal helper for downline
    downline_result = _get_analytics_for_scope(
        user=user,
        as_of=as_of,
        all_time_months=all_time_months,
        top_states=top_states,
        carrier_ids_array=carrier_ids_array,
        scope='downline',
        include_series=include_series,
        include_windows_by_carrier=include_windows_by_carrier,
        include_breakdowns_status=include_breakdowns_status,
        include_breakdowns_state=include_breakdowns_state,
        include_breakdowns_age=include_breakdowns_age,
    )

    return {
        'your_deals': your_deals_result,
        'downline': downline_result,
        'metadata': {
            'as_of': as_of.isoformat(),
            'all_time_months': all_time_months,
            'user_id': str(user.id),
        },
    }


def _get_analytics_for_scope(
    user: AuthenticatedUser,
    as_of: date,
    all_time_months: int,
    top_states: int,
    carrier_ids_array: list[str] | None,
    scope: str,  # 'your_deals' or 'downline'
    include_series: bool,
    include_windows_by_carrier: bool,
    include_breakdowns_status: bool,
    include_breakdowns_state: bool,
    include_breakdowns_age: bool,
) -> dict[str, Any]:
    """
    Internal helper to get analytics for a specific scope (your_deals or downline).
    Returns structure matching the RPC output.
    """
    # Build the scope-specific WHERE clause
    if scope == 'your_deals':
        scope_filter = "d.agent_id = %s"
        scope_params = [str(user.id)]
    else:  # downline
        scope_filter = """(
            (uc.is_admin = true)
            OR
            (uc.is_admin = false
             AND d.id IN (SELECT deal_id FROM deal_hierarchy_snapshot WHERE agent_id = %s)
             AND d.agent_id != %s
            )
        )"""
        scope_params = [str(user.id), str(user.id)]

    with connection.cursor() as cursor:
        cursor.execute(f"""
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
            base_deals AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
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
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => %s - 1))
                            AND aom.month_start
                    AND (%s IS NULL OR d.carrier_id = ANY(%s::uuid[]))
                    AND {scope_filter}
            ),
            -- Monthly series aggregation
            monthly_series AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    b.effective_month,
                    count(*)::int as submitted,
                    sum(b.monthly_premium)::numeric as premium_sum,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    sum((b.placement_class = 'Placed')::int)::int as placed,
                    sum((b.placement_class = 'Not Placed')::int)::int as not_placed
                FROM base_deals b
                GROUP BY b.carrier_id, b.carrier_name, b.effective_month
            ),
            -- Rolling 9m for persistency calculation
            rolling_9m AS (
                SELECT
                    ms_outer.carrier_id,
                    ms_outer.carrier_name,
                    ms_outer.effective_month,
                    sum(ms_inner.active)::int as active_9m,
                    sum(ms_inner.inactive)::int as inactive_9m,
                    sum(ms_inner.placed)::int as placed_9m,
                    sum(ms_inner.not_placed)::int as not_placed_9m
                FROM monthly_series ms_outer
                JOIN monthly_series ms_inner
                    ON ms_inner.carrier_id = ms_outer.carrier_id
                    AND ms_inner.effective_month BETWEEN
                        (ms_outer.effective_month - make_interval(months => 8))
                        AND ms_outer.effective_month
                GROUP BY ms_outer.carrier_id, ms_outer.carrier_name, ms_outer.effective_month
            ),
            -- Window definitions
            win_ranges AS (
                SELECT * FROM (VALUES
                    ('3m', 3),
                    ('6m', 6),
                    ('9m', 9),
                    ('all_time', %s)
                ) AS t(win_key, win_months)
                CROSS JOIN as_of_month aom
            ),
            -- Window series by carrier
            win_series AS (
                SELECT
                    ms.carrier_id,
                    ms.carrier_name,
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted,
                    sum(ms.placed)::int as placed,
                    sum(ms.not_placed)::int as not_placed
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY ms.carrier_id, ms.carrier_name, wr.win_key
            ),
            -- Window totals (all carriers combined)
            win_all AS (
                SELECT
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted,
                    sum(ms.placed)::int as placed,
                    sum(ms.not_placed)::int as not_placed
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY wr.win_key
            ),
            -- Status breakdown by carrier and window
            status_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    coalesce(b.status_raw, 'Unknown') as status_value,
                    count(*)::int as cnt
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, coalesce(b.status_raw, 'Unknown')
            ),
            -- State breakdown ranked
            state_ranked AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.state,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    row_number() over (
                        partition by b.carrier_id, wr.win_key
                        order by count(*) desc, b.state
                    ) as rn
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.state
            ),
            -- Age breakdown
            age_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.age_band,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.age_band
            )
            SELECT
                -- Meta
                (SELECT jsonb_build_object(
                    'window', 'all_time',
                    'grain', 'month',
                    'as_of', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM-DD'),
                    'carriers', coalesce(
                        (SELECT jsonb_agg(DISTINCT carrier_name ORDER BY carrier_name)
                         FROM base_deals), '[]'::jsonb),
                    'period_start', to_char(
                        (SELECT month_start - make_interval(months => %s - 1) FROM as_of_month),
                        'YYYY-MM'),
                    'period_end', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM')
                )) as meta,
                -- Series
                coalesce(
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'period', to_char(ms.effective_month, 'YYYY-MM'),
                            'carrier', ms.carrier_name,
                            'active', ms.active,
                            'inactive', ms.inactive,
                            'submitted', ms.submitted,
                            'avg_premium_submitted', ms.avg_premium_submitted,
                            'persistency', (rp.active_9m::numeric / nullif(rp.active_9m + rp.inactive_9m, 0)),
                            'placed', ms.placed,
                            'not_placed', ms.not_placed,
                            'placement', (rp.placed_9m::numeric / nullif(rp.placed_9m + rp.not_placed_9m, 0))
                        )
                        ORDER BY ms.carrier_name, ms.effective_month
                    )
                    FROM monthly_series ms
                    LEFT JOIN rolling_9m rp
                        ON rp.carrier_id = ms.carrier_id
                        AND rp.effective_month = ms.effective_month),
                    '[]'::jsonb
                ) as series,
                -- Windows by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        carrier_name,
                        win_payload
                        ORDER BY carrier_name
                    )
                    FROM (
                        SELECT
                            ws.carrier_name,
                            jsonb_object_agg(
                                ws.win_key,
                                jsonb_build_object(
                                    'active', ws.active,
                                    'inactive', ws.inactive,
                                    'submitted', ws.submitted,
                                    'avg_premium_submitted', ws.avg_premium_submitted,
                                    'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0)),
                                    'placed', ws.placed,
                                    'not_placed', ws.not_placed,
                                    'placement', (ws.placed::numeric / nullif(ws.placed + ws.not_placed, 0))
                                )
                                ORDER BY ws.win_key
                            ) as win_payload
                        FROM win_series ws
                        GROUP BY ws.carrier_name
                    ) t),
                    '{{}}'::jsonb
                ) as windows_by_carrier,
                -- Totals
                jsonb_build_object(
                    'by_carrier', coalesce(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'window', 'all_time',
                                'carrier', ws.carrier_name,
                                'active', ws.active,
                                'inactive', ws.inactive,
                                'submitted', ws.submitted,
                                'avg_premium_submitted', ws.avg_premium_submitted,
                                'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0)),
                                'placed', ws.placed,
                                'not_placed', ws.not_placed,
                                'placement', (ws.placed::numeric / nullif(ws.placed + ws.not_placed, 0))
                            )
                            ORDER BY ws.carrier_name
                        )
                        FROM win_series ws
                        WHERE ws.win_key = 'all_time'),
                        '[]'::jsonb
                    ),
                    'all', coalesce(
                        (SELECT jsonb_build_object(
                            'window', 'all_time',
                            'carrier', 'ALL',
                            'active', wa.active,
                            'inactive', wa.inactive,
                            'submitted', wa.submitted,
                            'avg_premium_submitted', wa.avg_premium_submitted,
                            'persistency', (wa.active::numeric / nullif(wa.active + wa.inactive, 0)),
                            'placed', wa.placed,
                            'not_placed', wa.not_placed,
                            'placement', (wa.placed::numeric / nullif(wa.placed + wa.not_placed, 0))
                        )
                        FROM win_all wa
                        WHERE wa.win_key = 'all_time'),
                        '{{}}'::jsonb
                    )
                ) as totals,
                -- Breakdowns by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        c.carrier_name,
                        jsonb_build_object(
                            'status', coalesce(
                                (SELECT jsonb_object_agg(
                                    sb.win_key,
                                    (SELECT jsonb_object_agg(sb2.status_value, sb2.cnt)
                                     FROM status_breakdown sb2
                                     WHERE sb2.carrier_id = c.carrier_id AND sb2.win_key = sb.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM status_breakdown WHERE carrier_id = c.carrier_id) sb),
                                '{{}}'::jsonb
                            ),
                            'state', coalesce(
                                (SELECT jsonb_object_agg(
                                    sr.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'state', sr2.state,
                                            'active', sr2.active,
                                            'inactive', sr2.inactive,
                                            'submitted', sr2.submitted,
                                            'avg_premium_submitted', sr2.avg_premium_submitted
                                        )
                                        ORDER BY sr2.submitted DESC, sr2.state
                                    )
                                    FROM state_ranked sr2
                                    WHERE sr2.carrier_id = c.carrier_id
                                        AND sr2.win_key = sr.win_key
                                        AND sr2.rn <= %s)
                                )
                                FROM (SELECT DISTINCT win_key FROM state_ranked WHERE carrier_id = c.carrier_id) sr),
                                '{{}}'::jsonb
                            ),
                            'age_band', coalesce(
                                (SELECT jsonb_object_agg(
                                    ab.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'age_band', ab2.age_band,
                                            'active', ab2.active,
                                            'inactive', ab2.inactive,
                                            'submitted', ab2.submitted,
                                            'avg_premium_submitted', ab2.avg_premium_submitted
                                        )
                                        ORDER BY
                                            CASE ab2.age_band
                                                WHEN '18-30' THEN 1
                                                WHEN '31-40' THEN 2
                                                WHEN '41-50' THEN 3
                                                WHEN '51-60' THEN 4
                                                WHEN '61-70' THEN 5
                                                WHEN '71+' THEN 6
                                                WHEN 'UNK' THEN 7
                                                ELSE 99
                                            END, ab2.age_band
                                    )
                                    FROM age_breakdown ab2
                                    WHERE ab2.carrier_id = c.carrier_id AND ab2.win_key = ab.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM age_breakdown WHERE carrier_id = c.carrier_id) ab),
                                '{{}}'::jsonb
                            )
                        )
                        ORDER BY c.carrier_name
                    )
                    FROM (SELECT DISTINCT carrier_id, carrier_name FROM base_deals) c),
                    '{{}}'::jsonb
                ) as breakdowns_by_carrier
        """, [
            str(user.id),
            as_of, as_of,
            all_time_months,
            carrier_ids_array, carrier_ids_array,
            *scope_params,
            all_time_months,
            all_time_months,
            top_states,
        ])

        row = cursor.fetchone()

    if not row:
        return _empty_analytics_scope()

    meta, series, windows_by_carrier, totals, breakdowns_by_carrier = row

    return {
        'meta': meta or {},
        'series': series if include_series else [],
        'windows_by_carrier': windows_by_carrier if include_windows_by_carrier else {},
        'totals': totals or {'by_carrier': [], 'all': {}},
        'breakdowns_over_time': {
            'by_carrier': breakdowns_by_carrier if (include_breakdowns_status or include_breakdowns_state or include_breakdowns_age) else {},
        },
    }


def _empty_analytics_scope() -> dict[str, Any]:
    """Return empty analytics scope structure."""
    return {
        'meta': {
            'window': 'all_time',
            'grain': 'month',
            'carriers': [],
        },
        'series': [],
        'windows_by_carrier': {},
        'totals': {
            'by_carrier': [],
            'all': {},
        },
        'breakdowns_over_time': {
            'by_carrier': {},
        },
    }


def get_analytics_from_deals(
    agency_id: UUID,
    as_of: date | None = None,
    all_time_months: int = 24,
    carrier_ids: list[UUID] | None = None,
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


def analyze_persistency_for_deals(
    agency_id: UUID,
    as_of: date | None = None,
    carrier_id: UUID | None = None,
) -> dict[str, Any]:
    """
    Full persistency analytics matching RPC analyze_persistency_for_deals.

    Returns complete structure with:
    - carriers: Array with timeRanges (3, 6, 9, All), statusBreakdowns
    - overall_analytics: activeCount, inactiveCount, overallPersistency, timeRanges
    - carrier_comparison: activeShareByCarrier, inactiveShareByCarrier

    Args:
        agency_id: The agency ID
        as_of: Reference date (default: today)
        carrier_id: Optional carrier ID to filter by

    Returns:
        Full persistency analytics matching RPC structure
    """
    if as_of is None:
        as_of = date.today()

    carrier_filter = "AND d.carrier_id = %s" if carrier_id else ""
    carrier_params = [str(carrier_id)] if carrier_id else []

    with connection.cursor() as cursor:
        cursor.execute(f"""
            WITH
            as_of_month AS (
                SELECT make_date(
                    extract(year from %s::date)::int,
                    extract(month from %s::date)::int,
                    1
                ) as month_start
            ),
            -- Time bucket definitions
            time_buckets AS (
                SELECT * FROM (VALUES
                    ('3', 3),
                    ('6', 6),
                    ('9', 9),
                    ('All', 24)
                ) AS t(bucket_key, bucket_months)
            ),
            base_deals AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    d.status as raw_status,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    CASE
                        WHEN sm.impact = 'positive' THEN 'Active'
                        WHEN sm.impact = 'negative' THEN 'Inactive'
                        ELSE 'Unknown'
                    END as status_class
                FROM deals d
                JOIN carriers c ON c.id = d.carrier_id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND sm.raw_status = d.status
                CROSS JOIN as_of_month aom
                WHERE d.agency_id = %s
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => 23))
                            AND aom.month_start
                    {carrier_filter}
            ),
            -- Carrier metrics by time bucket
            carrier_time_metrics AS (
                SELECT
                    bd.carrier_id,
                    bd.carrier_name,
                    tb.bucket_key,
                    count(*)::int as total_count,
                    sum(CASE WHEN bd.status_class = 'Active' THEN 1 ELSE 0 END)::int as positive_count,
                    sum(CASE WHEN bd.status_class = 'Inactive' THEN 1 ELSE 0 END)::int as negative_count
                FROM base_deals bd
                CROSS JOIN as_of_month aom
                JOIN time_buckets tb ON bd.effective_month >= (aom.month_start - make_interval(months => tb.bucket_months - 1))
                GROUP BY bd.carrier_id, bd.carrier_name, tb.bucket_key
            ),
            -- Status breakdown by carrier and time bucket (top 7 + Other)
            status_breakdown_raw AS (
                SELECT
                    bd.carrier_id,
                    bd.carrier_name,
                    tb.bucket_key,
                    COALESCE(bd.raw_status, 'Unknown') as status_value,
                    count(*)::int as cnt,
                    row_number() over (
                        partition by bd.carrier_id, tb.bucket_key
                        order by count(*) desc
                    ) as rn
                FROM base_deals bd
                CROSS JOIN as_of_month aom
                JOIN time_buckets tb ON bd.effective_month >= (aom.month_start - make_interval(months => tb.bucket_months - 1))
                GROUP BY bd.carrier_id, bd.carrier_name, tb.bucket_key, COALESCE(bd.raw_status, 'Unknown')
            ),
            -- Overall metrics by time bucket
            overall_time_metrics AS (
                SELECT
                    tb.bucket_key,
                    count(*)::int as total_count,
                    sum(CASE WHEN bd.status_class = 'Active' THEN 1 ELSE 0 END)::int as active_count,
                    sum(CASE WHEN bd.status_class = 'Inactive' THEN 1 ELSE 0 END)::int as inactive_count
                FROM base_deals bd
                CROSS JOIN as_of_month aom
                JOIN time_buckets tb ON bd.effective_month >= (aom.month_start - make_interval(months => tb.bucket_months - 1))
                GROUP BY tb.bucket_key
            ),
            -- Carrier comparison (share of active/inactive by carrier for 'All' bucket)
            carrier_shares AS (
                SELECT
                    bd.carrier_name,
                    sum(CASE WHEN bd.status_class = 'Active' THEN 1 ELSE 0 END)::float as active_cnt,
                    sum(CASE WHEN bd.status_class = 'Inactive' THEN 1 ELSE 0 END)::float as inactive_cnt,
                    (SELECT sum(CASE WHEN status_class = 'Active' THEN 1 ELSE 0 END) FROM base_deals)::float as total_active,
                    (SELECT sum(CASE WHEN status_class = 'Inactive' THEN 1 ELSE 0 END) FROM base_deals)::float as total_inactive
                FROM base_deals bd
                GROUP BY bd.carrier_name
            )
            SELECT
                -- Carriers array with timeRanges and statusBreakdowns
                coalesce(
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'carrier', c.carrier_name,
                            'timeRanges', (
                                SELECT jsonb_object_agg(
                                    ctm.bucket_key,
                                    jsonb_build_object(
                                        'positivePercentage', round((ctm.positive_count::numeric / nullif(ctm.total_count, 0)) * 100, 2),
                                        'positiveCount', ctm.positive_count,
                                        'negativePercentage', round((ctm.negative_count::numeric / nullif(ctm.total_count, 0)) * 100, 2),
                                        'negativeCount', ctm.negative_count
                                    )
                                )
                                FROM carrier_time_metrics ctm
                                WHERE ctm.carrier_id = c.carrier_id
                            ),
                            'statusBreakdowns', (
                                SELECT jsonb_object_agg(
                                    sbr.bucket_key,
                                    (
                                        SELECT jsonb_object_agg(
                                            CASE WHEN sbr2.rn <= 7 THEN sbr2.status_value ELSE 'Other' END,
                                            jsonb_build_object(
                                                'count', sbr2.cnt,
                                                'percentage', round((sbr2.cnt::numeric / nullif(
                                                    (SELECT sum(cnt) FROM status_breakdown_raw WHERE carrier_id = c.carrier_id AND bucket_key = sbr.bucket_key), 0
                                                )) * 100, 2)
                                            )
                                        )
                                        FROM status_breakdown_raw sbr2
                                        WHERE sbr2.carrier_id = c.carrier_id AND sbr2.bucket_key = sbr.bucket_key
                                    )
                                )
                                FROM (SELECT DISTINCT bucket_key FROM status_breakdown_raw WHERE carrier_id = c.carrier_id) sbr
                            ),
                            'totalPolicies', (SELECT sum(total_count) FROM carrier_time_metrics WHERE carrier_id = c.carrier_id AND bucket_key = 'All'),
                            'persistencyRate', (
                                SELECT round((positive_count::numeric / nullif(total_count, 0)) * 100, 2)
                                FROM carrier_time_metrics WHERE carrier_id = c.carrier_id AND bucket_key = 'All'
                            )
                        )
                        ORDER BY c.carrier_name
                    )
                    FROM (SELECT DISTINCT carrier_id, carrier_name FROM carrier_time_metrics) c),
                    '[]'::jsonb
                ) as carriers,
                -- Overall analytics
                jsonb_build_object(
                    'activeCount', (SELECT coalesce(sum(active_count), 0) FROM overall_time_metrics WHERE bucket_key = 'All'),
                    'inactiveCount', (SELECT coalesce(sum(inactive_count), 0) FROM overall_time_metrics WHERE bucket_key = 'All'),
                    'overallPersistency', (
                        SELECT round((active_count::numeric / nullif(active_count + inactive_count, 0)) * 100, 2)
                        FROM overall_time_metrics WHERE bucket_key = 'All'
                    ),
                    'timeRanges', (
                        SELECT jsonb_object_agg(
                            otm.bucket_key,
                            jsonb_build_object(
                                'activeCount', otm.active_count,
                                'inactiveCount', otm.inactive_count,
                                'activePercentage', round((otm.active_count::numeric / nullif(otm.total_count, 0)) * 100, 2)
                            )
                        )
                        FROM overall_time_metrics otm
                    )
                ) as overall_analytics,
                -- Carrier comparison
                jsonb_build_object(
                    'activeShareByCarrier', (
                        SELECT jsonb_object_agg(
                            cs.carrier_name,
                            round((cs.active_cnt / nullif(cs.total_active, 0)) * 100, 2)
                        )
                        FROM carrier_shares cs
                    ),
                    'inactiveShareByCarrier', (
                        SELECT jsonb_object_agg(
                            cs.carrier_name,
                            round((cs.inactive_cnt / nullif(cs.total_inactive, 0)) * 100, 2)
                        )
                        FROM carrier_shares cs
                    )
                ) as carrier_comparison
        """, [
            as_of, as_of,
            str(agency_id),
            *carrier_params,
        ])

        row = cursor.fetchone()

    if not row:
        return {
            'carriers': [],
            'overall_analytics': {
                'activeCount': 0,
                'inactiveCount': 0,
                'overallPersistency': 0,
                'timeRanges': {},
            },
            'carrier_comparison': {
                'activeShareByCarrier': {},
                'inactiveShareByCarrier': {},
            },
        }

    carriers, overall_analytics, carrier_comparison = row

    return {
        'carriers': carriers or [],
        'overall_analytics': overall_analytics or {},
        'carrier_comparison': carrier_comparison or {},
    }


# Alias for backward compatibility
def get_persistency_analytics(
    agency_id: UUID,
    as_of: date | None = None,
    carrier_id: UUID | None = None,
) -> dict[str, Any]:
    """
    Backward-compatible wrapper for analyze_persistency_for_deals.
    Returns simplified structure for existing callers.
    """
    result = analyze_persistency_for_deals(agency_id, as_of, carrier_id)
    overall = result.get('overall_analytics', {})
    return {
        'by_carrier': [
            {
                'carrier_name': c.get('carrier'),
                'total_policies': c.get('totalPolicies', 0),
                'persistency_rate': c.get('persistencyRate', 0),
            }
            for c in result.get('carriers', [])
        ],
        'by_agent': [],  # Not available in new structure
        'overall_persistency': overall.get('overallPersistency', 0),
        'total_policies': overall.get('activeCount', 0) + overall.get('inactiveCount', 0),
        'active_policies': overall.get('activeCount', 0),
    }


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


def get_analytics_for_agent(
    user: AuthenticatedUser,
    as_of: date | None = None,
    all_time_months: int = 12,
    carrier_ids: list[UUID] | None = None,
    include_series: bool = True,
    include_windows_by_carrier: bool = True,
    include_breakdowns_status: bool = True,
    include_breakdowns_state: bool = True,
    include_breakdowns_age: bool = True,
    top_states: int = 50,
) -> dict[str, Any]:
    """
    Get analytics for deals visible to a specific agent.
    Maps to Supabase RPC: get_analytics_from_deals_for_agent

    This returns analytics for ALL deals the agent can see (their own + downline),
    not split into separate views like get_analytics_split_view.

    Args:
        user: The authenticated user
        as_of: Reference date for calculations (default: today)
        all_time_months: Number of months for all-time window (default: 12)
        carrier_ids: Optional list of carrier IDs to filter by
        include_series: Include monthly time series data
        include_windows_by_carrier: Include window calculations per carrier
        include_breakdowns_status: Include status breakdown
        include_breakdowns_state: Include state breakdown
        include_breakdowns_age: Include age band breakdown
        top_states: Number of top states to include in breakdown

    Returns:
        Dictionary with meta, series, windows_by_carrier, totals, breakdowns_over_time
    """
    if as_of is None:
        as_of = date.today()

    carrier_ids_array = [str(cid) for cid in carrier_ids] if carrier_ids else None

    with connection.cursor() as cursor:
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
            base_deals AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
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
                    AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                    AND date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date
                        BETWEEN (aom.month_start - make_interval(months => %s - 1))
                            AND aom.month_start
                    AND (%s IS NULL OR d.carrier_id = ANY(%s::uuid[]))
                    -- For non-admins, only include deals from their hierarchy
                    AND (uc.is_admin OR d.id IN (
                        SELECT deal_id FROM deal_hierarchy_snapshot WHERE agent_id = %s
                    ))
            ),
            -- Monthly series aggregation
            monthly_series AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    b.effective_month,
                    count(*)::int as submitted,
                    sum(b.monthly_premium)::numeric as premium_sum,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    sum((b.placement_class = 'Placed')::int)::int as placed,
                    sum((b.placement_class = 'Not Placed')::int)::int as not_placed
                FROM base_deals b
                GROUP BY b.carrier_id, b.carrier_name, b.effective_month
            ),
            -- Rolling 9m for persistency calculation
            rolling_9m AS (
                SELECT
                    ms_outer.carrier_id,
                    ms_outer.carrier_name,
                    ms_outer.effective_month,
                    sum(ms_inner.active)::int as active_9m,
                    sum(ms_inner.inactive)::int as inactive_9m,
                    sum(ms_inner.placed)::int as placed_9m,
                    sum(ms_inner.not_placed)::int as not_placed_9m
                FROM monthly_series ms_outer
                JOIN monthly_series ms_inner
                    ON ms_inner.carrier_id = ms_outer.carrier_id
                    AND ms_inner.effective_month BETWEEN
                        (ms_outer.effective_month - make_interval(months => 8))
                        AND ms_outer.effective_month
                GROUP BY ms_outer.carrier_id, ms_outer.carrier_name, ms_outer.effective_month
            ),
            -- Window definitions
            win_ranges AS (
                SELECT * FROM (VALUES
                    ('3m', 3),
                    ('6m', 6),
                    ('9m', 9),
                    ('all_time', %s)
                ) AS t(win_key, win_months)
                CROSS JOIN as_of_month aom
            ),
            -- Window series by carrier
            win_series AS (
                SELECT
                    ms.carrier_id,
                    ms.carrier_name,
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted,
                    sum(ms.placed)::int as placed,
                    sum(ms.not_placed)::int as not_placed
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY ms.carrier_id, ms.carrier_name, wr.win_key
            ),
            -- Window totals (all carriers combined)
            win_all AS (
                SELECT
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted,
                    sum(ms.placed)::int as placed,
                    sum(ms.not_placed)::int as not_placed
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY wr.win_key
            ),
            -- Status breakdown by carrier and window
            status_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    coalesce(b.status_raw, 'Unknown') as status_value,
                    count(*)::int as cnt
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, coalesce(b.status_raw, 'Unknown')
            ),
            -- State breakdown ranked
            state_ranked AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.state,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    row_number() over (
                        partition by b.carrier_id, wr.win_key
                        order by count(*) desc, b.state
                    ) as rn
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.state
            ),
            -- Age breakdown
            age_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.age_band,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.age_band
            )
            SELECT
                -- Meta
                (SELECT jsonb_build_object(
                    'window', 'all_time',
                    'grain', 'month',
                    'as_of', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM-DD'),
                    'carriers', coalesce(
                        (SELECT jsonb_agg(DISTINCT carrier_name ORDER BY carrier_name)
                         FROM base_deals), '[]'::jsonb),
                    'definitions', jsonb_build_object(
                        'active_count_eom', 'Policies active at month end (via status_mapping.impact = positive)',
                        'inactive_count_eom', 'Policies lapsed/terminated by month end (impact = negative)',
                        'submitted_count', 'Policies effective in the calendar month',
                        'avg_premium_submitted', 'Average monthly premium of policies effective during the month (USD)',
                        'persistency_formula', 'active / (active + inactive)',
                        'placement_formula', 'placed / (placed + not_placed)'
                    ),
                    'period_start', to_char(
                        (SELECT month_start - make_interval(months => %s - 1) FROM as_of_month),
                        'YYYY-MM'),
                    'period_end', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM')
                )) as meta,
                -- Series
                coalesce(
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'period', to_char(ms.effective_month, 'YYYY-MM'),
                            'carrier', ms.carrier_name,
                            'active', ms.active,
                            'inactive', ms.inactive,
                            'submitted', ms.submitted,
                            'avg_premium_submitted', ms.avg_premium_submitted,
                            'persistency', (rp.active_9m::numeric / nullif(rp.active_9m + rp.inactive_9m, 0)),
                            'placed', ms.placed,
                            'not_placed', ms.not_placed,
                            'placement', (rp.placed_9m::numeric / nullif(rp.placed_9m + rp.not_placed_9m, 0))
                        )
                        ORDER BY ms.carrier_name, ms.effective_month
                    )
                    FROM monthly_series ms
                    LEFT JOIN rolling_9m rp
                        ON rp.carrier_id = ms.carrier_id
                        AND rp.effective_month = ms.effective_month),
                    '[]'::jsonb
                ) as series,
                -- Windows by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        carrier_name,
                        win_payload
                        ORDER BY carrier_name
                    )
                    FROM (
                        SELECT
                            ws.carrier_name,
                            jsonb_object_agg(
                                ws.win_key,
                                jsonb_build_object(
                                    'active', ws.active,
                                    'inactive', ws.inactive,
                                    'submitted', ws.submitted,
                                    'avg_premium_submitted', ws.avg_premium_submitted,
                                    'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0)),
                                    'placed', ws.placed,
                                    'not_placed', ws.not_placed,
                                    'placement', (ws.placed::numeric / nullif(ws.placed + ws.not_placed, 0))
                                )
                                ORDER BY ws.win_key
                            ) as win_payload
                        FROM win_series ws
                        GROUP BY ws.carrier_name
                    ) t),
                    '{}'::jsonb
                ) as windows_by_carrier,
                -- Totals
                jsonb_build_object(
                    'by_carrier', coalesce(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'window', 'all_time',
                                'carrier', ws.carrier_name,
                                'active', ws.active,
                                'inactive', ws.inactive,
                                'submitted', ws.submitted,
                                'avg_premium_submitted', ws.avg_premium_submitted,
                                'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0)),
                                'placed', ws.placed,
                                'not_placed', ws.not_placed,
                                'placement', (ws.placed::numeric / nullif(ws.placed + ws.not_placed, 0))
                            )
                            ORDER BY ws.carrier_name
                        )
                        FROM win_series ws
                        WHERE ws.win_key = 'all_time'),
                        '[]'::jsonb
                    ),
                    'all', coalesce(
                        (SELECT jsonb_build_object(
                            'window', 'all_time',
                            'carrier', 'ALL',
                            'active', wa.active,
                            'inactive', wa.inactive,
                            'submitted', wa.submitted,
                            'avg_premium_submitted', wa.avg_premium_submitted,
                            'persistency', (wa.active::numeric / nullif(wa.active + wa.inactive, 0)),
                            'placed', wa.placed,
                            'not_placed', wa.not_placed,
                            'placement', (wa.placed::numeric / nullif(wa.placed + wa.not_placed, 0))
                        )
                        FROM win_all wa
                        WHERE wa.win_key = 'all_time'),
                        '{}'::jsonb
                    )
                ) as totals,
                -- Breakdowns by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        c.carrier_name,
                        jsonb_build_object(
                            'status', coalesce(
                                (SELECT jsonb_object_agg(
                                    sb.win_key,
                                    (SELECT jsonb_object_agg(sb2.status_value, sb2.cnt)
                                     FROM status_breakdown sb2
                                     WHERE sb2.carrier_id = c.carrier_id AND sb2.win_key = sb.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM status_breakdown WHERE carrier_id = c.carrier_id) sb),
                                '{}'::jsonb
                            ),
                            'state', coalesce(
                                (SELECT jsonb_object_agg(
                                    sr.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'state', sr2.state,
                                            'active', sr2.active,
                                            'inactive', sr2.inactive,
                                            'submitted', sr2.submitted,
                                            'avg_premium_submitted', sr2.avg_premium_submitted
                                        )
                                        ORDER BY sr2.submitted DESC, sr2.state
                                    )
                                    FROM state_ranked sr2
                                    WHERE sr2.carrier_id = c.carrier_id
                                        AND sr2.win_key = sr.win_key
                                        AND sr2.rn <= %s)
                                )
                                FROM (SELECT DISTINCT win_key FROM state_ranked WHERE carrier_id = c.carrier_id) sr),
                                '{}'::jsonb
                            ),
                            'age_band', coalesce(
                                (SELECT jsonb_object_agg(
                                    ab.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'age_band', ab2.age_band,
                                            'active', ab2.active,
                                            'inactive', ab2.inactive,
                                            'submitted', ab2.submitted,
                                            'avg_premium_submitted', ab2.avg_premium_submitted
                                        )
                                        ORDER BY
                                            CASE ab2.age_band
                                                WHEN '18-30' THEN 1
                                                WHEN '31-40' THEN 2
                                                WHEN '41-50' THEN 3
                                                WHEN '51-60' THEN 4
                                                WHEN '61-70' THEN 5
                                                WHEN '71+' THEN 6
                                                WHEN 'UNK' THEN 7
                                                ELSE 99
                                            END, ab2.age_band
                                    )
                                    FROM age_breakdown ab2
                                    WHERE ab2.carrier_id = c.carrier_id AND ab2.win_key = ab.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM age_breakdown WHERE carrier_id = c.carrier_id) ab),
                                '{}'::jsonb
                            )
                        )
                        ORDER BY c.carrier_name
                    )
                    FROM (SELECT DISTINCT carrier_id, carrier_name FROM base_deals) c),
                    '{}'::jsonb
                ) as breakdowns_by_carrier
        """, [
            str(user.id),
            as_of, as_of,
            all_time_months,
            carrier_ids_array, carrier_ids_array,
            str(user.id),
            all_time_months,
            all_time_months,
            top_states,
        ])

        row = cursor.fetchone()

    if not row:
        return _empty_agent_analytics()

    meta, series, windows_by_carrier, totals, breakdowns_by_carrier = row

    return {
        'meta': meta or {},
        'series': series if include_series else [],
        'windows_by_carrier': windows_by_carrier if include_windows_by_carrier else {},
        'totals': totals or {'by_carrier': [], 'all': {}},
        'breakdowns_over_time': {
            'by_carrier': breakdowns_by_carrier if (include_breakdowns_status or include_breakdowns_state or include_breakdowns_age) else {},
        },
    }


def _empty_agent_analytics() -> dict[str, Any]:
    """Return empty agent analytics structure."""
    return {
        'meta': {
            'window': 'all_time',
            'grain': 'month',
            'carriers': [],
        },
        'series': [],
        'windows_by_carrier': {},
        'totals': {
            'by_carrier': [],
            'all': {},
        },
        'breakdowns_over_time': {
            'by_carrier': {},
        },
    }


def get_carrier_metrics(
    agency_id: UUID,
    as_of: date | None = None,
    all_time_months: int = 12,
    carrier_ids: list[UUID] | None = None,
    include_series: bool = True,
    include_windows_by_carrier: bool = True,
    include_breakdowns_status: bool = True,
    include_breakdowns_state: bool = True,
    include_breakdowns_age: bool = True,
    top_states: int = 7,
) -> dict[str, Any]:
    """
    Get carrier-level metrics for an agency.
    Maps to Supabase RPC: get_carrier_metrics_json

    Args:
        agency_id: The agency ID
        as_of: Reference date for calculations (default: today)
        all_time_months: Number of months for all-time window (default: 12)
        carrier_ids: Optional list of carrier IDs to filter by
        include_series: Include monthly time series data
        include_windows_by_carrier: Include window calculations per carrier
        include_breakdowns_status: Include status breakdown
        include_breakdowns_state: Include state breakdown
        include_breakdowns_age: Include age band breakdown
        top_states: Number of top states to include in breakdown (default: 7)

    Returns:
        Dictionary with meta, series, windows_by_carrier, totals, breakdowns_over_time
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
            base_deals AS (
                SELECT
                    d.id,
                    d.carrier_id,
                    c.name as carrier_name,
                    date_trunc('month', COALESCE(d.policy_effective_date, d.submission_date))::date as effective_month,
                    d.monthly_premium,
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
            -- Monthly series aggregation
            monthly_series AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    b.effective_month,
                    count(*)::int as submitted,
                    sum(b.monthly_premium)::numeric as premium_sum,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive
                FROM base_deals b
                GROUP BY b.carrier_id, b.carrier_name, b.effective_month
            ),
            -- Window definitions
            win_ranges AS (
                SELECT * FROM (VALUES
                    ('3m', 3),
                    ('6m', 6),
                    ('9m', 9),
                    ('all_time', %s)
                ) AS t(win_key, win_months)
                CROSS JOIN as_of_month aom
            ),
            -- Window series by carrier
            win_series AS (
                SELECT
                    ms.carrier_id,
                    ms.carrier_name,
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY ms.carrier_id, ms.carrier_name, wr.win_key
            ),
            -- Window totals (all carriers combined)
            win_all AS (
                SELECT
                    wr.win_key,
                    sum(ms.active)::int as active,
                    sum(ms.inactive)::int as inactive,
                    sum(ms.submitted)::int as submitted,
                    (sum(ms.premium_sum) / nullif(sum(ms.submitted), 0))::numeric(12,2) as avg_premium_submitted
                FROM monthly_series ms
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON ms.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY wr.win_key
            ),
            -- Status breakdown by carrier and window
            status_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    coalesce(b.status_raw, 'Unknown') as status_value,
                    count(*)::int as cnt
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, coalesce(b.status_raw, 'Unknown')
            ),
            -- State breakdown ranked
            state_ranked AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.state,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted,
                    row_number() over (
                        partition by b.carrier_id, wr.win_key
                        order by count(*) desc, b.state
                    ) as rn
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.state
            ),
            -- Age breakdown
            age_breakdown AS (
                SELECT
                    b.carrier_id,
                    b.carrier_name,
                    wr.win_key,
                    b.age_band,
                    count(*)::int as submitted,
                    sum((b.impact_class = 'Active')::int)::int as active,
                    sum((b.impact_class = 'Inactive')::int)::int as inactive,
                    (sum(b.monthly_premium) / nullif(count(*), 0))::numeric(12,2) as avg_premium_submitted
                FROM base_deals b
                CROSS JOIN as_of_month aom
                JOIN win_ranges wr ON b.effective_month BETWEEN
                    (aom.month_start - make_interval(months => wr.win_months - 1))
                    AND aom.month_start
                GROUP BY b.carrier_id, b.carrier_name, wr.win_key, b.age_band
            )
            SELECT
                -- Meta
                (SELECT jsonb_build_object(
                    'window', 'all_time',
                    'grain', 'month',
                    'as_of', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM-DD'),
                    'carriers', coalesce(
                        (SELECT jsonb_agg(DISTINCT carrier_name ORDER BY carrier_name)
                         FROM base_deals), '[]'::jsonb),
                    'definitions', jsonb_build_object(
                        'active_count_eom', 'Policies active at month end (via status_mapping.impact = positive)',
                        'inactive_count_eom', 'Policies lapsed/terminated by month end (impact = negative)',
                        'submitted_count', 'Policies effective in the calendar month',
                        'avg_premium_submitted', 'Average monthly premium of policies effective during the month (USD)',
                        'persistency_formula', 'active / (active + inactive)'
                    ),
                    'period_start', to_char(
                        (SELECT month_start - make_interval(months => %s - 1) FROM as_of_month),
                        'YYYY-MM'),
                    'period_end', to_char((SELECT month_start FROM as_of_month), 'YYYY-MM')
                )) as meta,
                -- Series
                coalesce(
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'period', to_char(ms.effective_month, 'YYYY-MM'),
                            'carrier', ms.carrier_name,
                            'active', ms.active,
                            'inactive', ms.inactive,
                            'submitted', ms.submitted,
                            'avg_premium_submitted', ms.avg_premium_submitted,
                            'persistency', (ms.active::numeric / nullif(ms.active + ms.inactive, 0))
                        )
                        ORDER BY ms.carrier_name, ms.effective_month
                    )
                    FROM monthly_series ms),
                    '[]'::jsonb
                ) as series,
                -- Windows by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        carrier_name,
                        win_payload
                        ORDER BY carrier_name
                    )
                    FROM (
                        SELECT
                            ws.carrier_name,
                            jsonb_object_agg(
                                ws.win_key,
                                jsonb_build_object(
                                    'active', ws.active,
                                    'inactive', ws.inactive,
                                    'submitted', ws.submitted,
                                    'avg_premium_submitted', ws.avg_premium_submitted,
                                    'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0))
                                )
                                ORDER BY ws.win_key
                            ) as win_payload
                        FROM win_series ws
                        GROUP BY ws.carrier_name
                    ) t),
                    '{}'::jsonb
                ) as windows_by_carrier,
                -- Totals
                jsonb_build_object(
                    'by_carrier', coalesce(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'window', 'all_time',
                                'carrier', ws.carrier_name,
                                'active', ws.active,
                                'inactive', ws.inactive,
                                'submitted', ws.submitted,
                                'avg_premium_submitted', ws.avg_premium_submitted,
                                'persistency', (ws.active::numeric / nullif(ws.active + ws.inactive, 0))
                            )
                            ORDER BY ws.carrier_name
                        )
                        FROM win_series ws
                        WHERE ws.win_key = 'all_time'),
                        '[]'::jsonb
                    ),
                    'all', coalesce(
                        (SELECT jsonb_build_object(
                            'window', 'all_time',
                            'carrier', 'ALL',
                            'active', wa.active,
                            'inactive', wa.inactive,
                            'submitted', wa.submitted,
                            'avg_premium_submitted', wa.avg_premium_submitted,
                            'persistency', (wa.active::numeric / nullif(wa.active + wa.inactive, 0))
                        )
                        FROM win_all wa
                        WHERE wa.win_key = 'all_time'),
                        '{}'::jsonb
                    )
                ) as totals,
                -- Breakdowns by carrier
                coalesce(
                    (SELECT jsonb_object_agg(
                        c.carrier_name,
                        jsonb_build_object(
                            'status', coalesce(
                                (SELECT jsonb_object_agg(
                                    sb.win_key,
                                    (SELECT jsonb_object_agg(sb2.status_value, sb2.cnt)
                                     FROM status_breakdown sb2
                                     WHERE sb2.carrier_id = c.carrier_id AND sb2.win_key = sb.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM status_breakdown WHERE carrier_id = c.carrier_id) sb),
                                '{}'::jsonb
                            ),
                            'state', coalesce(
                                (SELECT jsonb_object_agg(
                                    sr.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'state', sr2.state,
                                            'active', sr2.active,
                                            'inactive', sr2.inactive,
                                            'submitted', sr2.submitted,
                                            'avg_premium_submitted', sr2.avg_premium_submitted
                                        )
                                        ORDER BY sr2.submitted DESC, sr2.state
                                    )
                                    FROM state_ranked sr2
                                    WHERE sr2.carrier_id = c.carrier_id
                                        AND sr2.win_key = sr.win_key
                                        AND sr2.rn <= %s)
                                )
                                FROM (SELECT DISTINCT win_key FROM state_ranked WHERE carrier_id = c.carrier_id) sr),
                                '{}'::jsonb
                            ),
                            'age_band', coalesce(
                                (SELECT jsonb_object_agg(
                                    ab.win_key,
                                    (SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'age_band', ab2.age_band,
                                            'active', ab2.active,
                                            'inactive', ab2.inactive,
                                            'submitted', ab2.submitted,
                                            'avg_premium_submitted', ab2.avg_premium_submitted
                                        )
                                        ORDER BY
                                            CASE ab2.age_band
                                                WHEN '18-30' THEN 1
                                                WHEN '31-40' THEN 2
                                                WHEN '41-50' THEN 3
                                                WHEN '51-60' THEN 4
                                                WHEN '61-70' THEN 5
                                                WHEN '71+' THEN 6
                                                WHEN 'UNK' THEN 7
                                                ELSE 99
                                            END, ab2.age_band
                                    )
                                    FROM age_breakdown ab2
                                    WHERE ab2.carrier_id = c.carrier_id AND ab2.win_key = ab.win_key)
                                )
                                FROM (SELECT DISTINCT win_key FROM age_breakdown WHERE carrier_id = c.carrier_id) ab),
                                '{}'::jsonb
                            )
                        )
                        ORDER BY c.carrier_name
                    )
                    FROM (SELECT DISTINCT carrier_id, carrier_name FROM base_deals) c),
                    '{}'::jsonb
                ) as breakdowns_by_carrier
        """, [
            as_of, as_of,
            str(agency_id),
            all_time_months,
            carrier_ids_array, carrier_ids_array,
            all_time_months,
            all_time_months,
            top_states,
        ])

        row = cursor.fetchone()

    if not row:
        return _empty_carrier_metrics()

    meta, series, windows_by_carrier, totals, breakdowns_by_carrier = row

    return {
        'meta': meta or {},
        'series': series if include_series else [],
        'windows_by_carrier': windows_by_carrier if include_windows_by_carrier else {},
        'totals': totals or {'by_carrier': [], 'all': {}},
        'breakdowns_over_time': {
            'by_carrier': breakdowns_by_carrier if (include_breakdowns_status or include_breakdowns_state or include_breakdowns_age) else {},
        },
    }


def _empty_carrier_metrics() -> dict[str, Any]:
    """Return empty carrier metrics structure."""
    return {
        'meta': {
            'window': 'all_time',
            'grain': 'month',
            'carriers': [],
        },
        'series': [],
        'windows_by_carrier': {},
        'totals': {
            'by_carrier': [],
            'all': {},
        },
        'breakdowns_over_time': {
            'by_carrier': {},
        },
    }
