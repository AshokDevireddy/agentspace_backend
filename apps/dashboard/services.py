"""
Dashboard Services (P2-032, P2-033, P2-034, P2-035)

Contains the business logic for:
- Dashboard metrics (get_dashboard_data_with_agency_id, get_scoreboard_data)
- Widget management
- Report generation and scheduling
- Export (CSV/Excel/PDF)
"""
import csv
import io
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from uuid import UUID

from django.db import connection
from django.utils import timezone
from psycopg2.extras import Json

from apps.core.constants import EXPORT
from services.hierarchy_service import HierarchyService

logger = logging.getLogger(__name__)


def _ensure_list(value) -> list:
    """Ensure JSONB value is a Python list.

    PostgreSQL/psycopg2 sometimes returns JSONB as strings depending on
    database configuration. This helper safely parses them.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


@dataclass
class UserContext:
    """User context derived from authenticated request."""
    internal_user_id: UUID
    auth_user_id: UUID
    agency_id: UUID
    email: str
    is_admin: bool


def get_user_context_from_auth_id(auth_user_id: UUID) -> UserContext | None:
    """
    Get user context from auth_user_id.

    Uses Django ORM for the lookup.

    Args:
        auth_user_id: The Supabase auth user ID (from JWT sub claim)

    Returns:
        UserContext if found, None otherwise
    """
    from apps.core.models import User

    try:
        user = User.objects.filter(auth_user_id=auth_user_id).first()  # type: ignore[attr-defined]

        if not user:
            return None

        # Determine if user is admin (checking multiple fields for compatibility)
        is_admin = (
            (user.is_admin or False) or
            user.perm_level == 'admin' or
            user.role == 'admin'
        )

        return UserContext(
            internal_user_id=user.id,
            auth_user_id=user.auth_user_id,
            agency_id=user.agency_id,
            email=user.email or '',
            is_admin=is_admin,
        )
    except Exception as e:
        logger.error(f'Error getting user context: {e}')
        return None


def get_dashboard_summary(
    user_ctx: UserContext,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    production_mode: str = 'submitted',
) -> dict:
    """
    Get dashboard summary data.

    Mirrors: get_dashboard_data_with_agency_id and get_dashboard_data_with_date_range RPC functions.

    Args:
        user_ctx: User context
        as_of_date: Single date reference (legacy mode, used if start_date/end_date not provided)
        start_date: Start of date range for filtering
        end_date: End of date range for filtering
        production_mode: 'submitted' (use submission_date) or 'issue_paid' (use policy_effective_date with 7-day cutoff)

    Returns:
        {
            'your_deals': { active_policies, monthly_commissions, new_policies, total_clients, carriers_active },
            'downline_production': { ... same structure ... },
            'totals': { pending_positions }
        }
    """
    # Default to as_of_date mode if no date range provided
    if as_of_date is None and start_date is None:
        as_of_date = date.today()

    # If date range provided, use it; otherwise derive from as_of_date
    if start_date is not None and end_date is not None:
        use_date_range = True
    else:
        use_date_range = False
        if as_of_date is None:
            as_of_date = date.today()

    internal_id = str(user_ctx.internal_user_id)
    agency_id = str(user_ctx.agency_id)
    is_admin = user_ctx.is_admin

    # Determine date field and range filter based on production_mode
    if production_mode == 'issue_paid':
        date_field = "COALESCE(d.policy_effective_date, d.submission_date)"
        # For issue_paid mode, only count deals where effective date is at least 7 days ago
        cutoff_condition = f"AND ({date_field} <= CURRENT_DATE - INTERVAL '7 days')"
    else:  # 'submitted'
        date_field = "d.submission_date"
        cutoff_condition = ""

    # Build date range filter
    if use_date_range:
        date_filter = f"AND {date_field} BETWEEN %s AND %s"
        date_params = [start_date.isoformat(), end_date.isoformat()]  # type: ignore[union-attr]
        reference_date = end_date  # type: ignore[assignment]
    else:
        date_filter = ""
        date_params = []
        reference_date = as_of_date  # type: ignore[assignment]

    try:
        with connection.cursor() as cursor:
            # ==================== YOUR DEALS ====================
            your_query = f"""
                WITH
                your_base AS (
                    SELECT
                        d.id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status as raw_status,
                        {date_field} as relevant_date,
                        d.monthly_premium,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m
                        ON m.carrier_id = d.carrier_id
                        AND m.raw_status = d.status
                    WHERE d.agency_id = %s
                        AND d.agent_id = %s
                        {date_filter}
                        {cutoff_condition}
                ),
                your_active AS (
                    SELECT * FROM your_base WHERE impact = 'positive'
                ),
                your_totals AS (
                    SELECT
                        COUNT(*)::int AS active_policies,
                        COALESCE(SUM(monthly_premium), 0)::numeric(12,2) AS monthly_commissions,
                        COUNT(*) FILTER (
                            WHERE relevant_date >= (%s::date - interval '1 week')
                        )::int AS new_policies
                    FROM your_active
                ),
                your_clients AS (
                    SELECT COUNT(DISTINCT d.client_id)::int AS total_clients
                    FROM deals d
                    WHERE d.agency_id = %s
                        AND d.agent_id = %s
                        AND d.client_id IS NOT NULL
                        {date_filter}
                        {cutoff_condition}
                ),
                your_carriers_active AS (
                    SELECT
                        a.carrier_id,
                        a.carrier_name as carrier,
                        COUNT(*)::int AS active_policies
                    FROM your_active a
                    GROUP BY a.carrier_id, a.carrier_name
                    ORDER BY a.carrier_name
                )
                SELECT
                    COALESCE(t.active_policies, 0),
                    COALESCE(t.monthly_commissions, 0),
                    COALESCE(t.new_policies, 0),
                    COALESCE(c.total_clients, 0),
                    COALESCE(
                        (SELECT json_agg(row_to_json(ca)) FROM your_carriers_active ca),
                        '[]'::json
                    )
                FROM your_totals t
                CROSS JOIN your_clients c
            """
            # Build params list
            your_params = [agency_id, internal_id] + date_params + [reference_date.isoformat(), agency_id, internal_id] + date_params
            cursor.execute(your_query, your_params)
            your_row = cursor.fetchone()

            your_deals = {
                'active_policies': your_row[0] if your_row else 0,
                'monthly_commissions': float(your_row[1]) if your_row else 0,
                'new_policies': your_row[2] if your_row else 0,
                'total_clients': your_row[3] if your_row else 0,
                'carriers_active': your_row[4] if your_row else [],
            }

            # ==================== DOWNLINE PRODUCTION ====================
            if is_admin:
                # Admins see all deals in agency
                base_downline_filter = "d.agency_id = %s"
                base_downline_params = [agency_id]
            else:
                # Agents see only their downline deals (excluding their own)
                base_downline_filter = """
                    d.agency_id = %s
                    AND d.id IN (
                        SELECT deal_id
                        FROM deal_hierarchy_snapshot
                        WHERE agent_id = %s
                    )
                    AND d.agent_id != %s
                """
                base_downline_params = [agency_id, internal_id, internal_id]

            downline_query = f"""
                WITH
                downline_base AS (
                    SELECT
                        d.id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status as raw_status,
                        {date_field} as relevant_date,
                        d.monthly_premium,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m
                        ON m.carrier_id = d.carrier_id
                        AND m.raw_status = d.status
                    WHERE {base_downline_filter}
                        {date_filter}
                        {cutoff_condition}
                ),
                downline_active AS (
                    SELECT * FROM downline_base WHERE impact = 'positive'
                ),
                downline_totals AS (
                    SELECT
                        COUNT(*)::int AS active_policies,
                        COALESCE(SUM(monthly_premium), 0)::numeric(12,2) AS monthly_commissions,
                        COUNT(*) FILTER (
                            WHERE relevant_date >= (%s::date - interval '1 week')
                        )::int AS new_policies
                    FROM downline_active
                ),
                downline_clients AS (
                    SELECT COUNT(DISTINCT d.client_id)::int AS total_clients
                    FROM deals d
                    WHERE {base_downline_filter}
                        AND d.client_id IS NOT NULL
                        {date_filter}
                        {cutoff_condition}
                ),
                downline_carriers_active AS (
                    SELECT
                        a.carrier_id,
                        a.carrier_name as carrier,
                        COUNT(*)::int AS active_policies
                    FROM downline_active a
                    GROUP BY a.carrier_id, a.carrier_name
                    ORDER BY a.carrier_name
                )
                SELECT
                    COALESCE(t.active_policies, 0),
                    COALESCE(t.monthly_commissions, 0),
                    COALESCE(t.new_policies, 0),
                    COALESCE(c.total_clients, 0),
                    COALESCE(
                        (SELECT json_agg(row_to_json(ca)) FROM downline_carriers_active ca),
                        '[]'::json
                    )
                FROM downline_totals t
                CROSS JOIN downline_clients c
            """
            # Build params: base_filter + date_params (for downline_base) + reference_date + base_filter + date_params (for downline_clients)
            downline_params = base_downline_params + date_params + [reference_date.isoformat()] + base_downline_params + date_params
            cursor.execute(downline_query, downline_params)
            downline_row = cursor.fetchone()

            downline_production = {
                'active_policies': downline_row[0] if downline_row else 0,
                'monthly_commissions': float(downline_row[1]) if downline_row else 0,
                'new_policies': downline_row[2] if downline_row else 0,
                'total_clients': downline_row[3] if downline_row else 0,
                'carriers_active': downline_row[4] if downline_row else [],
            }

            # ==================== PENDING POSITIONS ====================
            if is_admin:
                cursor.execute("""
                    SELECT COUNT(*)::int
                    FROM users u
                    WHERE u.agency_id = %s
                        AND u.position_id IS NULL
                        AND u.role <> 'client'
                """, [agency_id])
                pending_row = cursor.fetchone()
                pending_positions = pending_row[0] if pending_row else 0
            else:
                # Use HierarchyService instead of Supabase RPC
                downline_ids = HierarchyService.get_downline(
                    user_id=user_ctx.internal_user_id,
                    agency_id=user_ctx.agency_id,
                    include_self=True
                )
                if downline_ids:
                    downline_ids_str = [str(uid) for uid in downline_ids]
                    cursor.execute("""
                        SELECT COUNT(*)::int
                        FROM users u
                        WHERE u.id = ANY(%s::uuid[])
                            AND u.position_id IS NULL
                            AND u.role <> 'client'
                    """, [downline_ids_str])
                    pending_row = cursor.fetchone()
                    pending_positions = pending_row[0] if pending_row else 0
                else:
                    pending_positions = 0

            return {
                'your_deals': your_deals,
                'downline_production': downline_production,
                'totals': {
                    'pending_positions': pending_positions,
                },
            }
    except Exception as e:
        logger.error(f'Error getting dashboard summary: {e}')
        raise


def get_scoreboard_data(
    user_ctx: UserContext,
    start_date: date,
    end_date: date
) -> dict:
    """
    Get scoreboard/leaderboard data.

    Mirrors: get_scoreboard_data RPC function.

    Returns:
        {
            'success': True,
            'data': {
                'leaderboard': [...],
                'stats': { totalProduction, totalDeals, activeAgents },
                'dateRange': { startDate, endDate }
            }
        }
    """
    agency_id = str(user_ctx.agency_id)

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH
                agency_agents AS (
                    SELECT
                        u.id AS agent_id,
                        CONCAT(u.first_name, ' ', u.last_name) AS name
                    FROM users u
                    WHERE u.agency_id = %s
                        AND u.role <> 'client'
                        AND u.is_active = true
                ),
                date_params AS (
                    SELECT
                        %s::date AS start_date,
                        %s::date AS end_date,
                        (%s::date - interval '1 year')::date AS lookback_date,
                        CURRENT_DATE AS today
                ),
                agency_deals AS (
                    SELECT
                        d.id AS deal_id,
                        d.agent_id,
                        d.annual_premium,
                        d.submission_date AS policy_start_date
                    FROM deals d
                    CROSS JOIN date_params dp
                    WHERE d.agency_id = %s
                        AND d.submission_date IS NOT NULL
                        AND d.submission_date >= dp.lookback_date
                        AND d.submission_date <= dp.end_date
                        AND d.annual_premium IS NOT NULL
                        AND d.annual_premium > 0
                ),
                agent_daily_breakdown AS (
                    SELECT
                        x.agent_id,
                        jsonb_object_agg(
                            x.policy_start_date::text,
                            ROUND(x.daily_total::numeric, 2)
                        ) AS daily_breakdown
                    FROM (
                        SELECT
                            ad.agent_id,
                            ad.policy_start_date,
                            SUM(ad.annual_premium) AS daily_total
                        FROM agency_deals ad
                        CROSS JOIN date_params dp
                        WHERE ad.policy_start_date BETWEEN dp.start_date AND dp.end_date
                            AND ad.policy_start_date <= dp.today
                        GROUP BY ad.agent_id, ad.policy_start_date
                    ) x
                    GROUP BY x.agent_id
                ),
                agent_totals AS (
                    SELECT
                        ad.agent_id,
                        SUM(ad.annual_premium) AS total_production,
                        COUNT(DISTINCT ad.deal_id) AS deal_count
                    FROM agency_deals ad
                    CROSS JOIN date_params dp
                    WHERE ad.policy_start_date BETWEEN dp.start_date AND dp.end_date
                        AND ad.policy_start_date <= dp.today
                    GROUP BY ad.agent_id
                ),
                new_business_agents AS (
                    SELECT DISTINCT ad.agent_id
                    FROM agency_deals ad
                    CROSS JOIN date_params dp
                    WHERE ad.policy_start_date BETWEEN dp.start_date AND dp.end_date
                        AND ad.policy_start_date <= dp.today
                ),
                leaderboard_data AS (
                    SELECT
                        aa.agent_id,
                        aa.name,
                        COALESCE(at.total_production, 0) AS total,
                        COALESCE(adb.daily_breakdown, '{}'::jsonb) AS daily_breakdown,
                        COALESCE(at.deal_count, 0) AS deal_count
                    FROM agency_agents aa
                    JOIN new_business_agents nba ON nba.agent_id = aa.agent_id
                    LEFT JOIN agent_totals at ON at.agent_id = aa.agent_id
                    LEFT JOIN agent_daily_breakdown adb ON adb.agent_id = aa.agent_id
                    WHERE COALESCE(at.deal_count, 0) > 0
                    ORDER BY total DESC
                ),
                ranked_leaderboard AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY total DESC) AS rank,
                        agent_id,
                        name,
                        total,
                        daily_breakdown,
                        deal_count
                    FROM leaderboard_data
                ),
                overall_stats AS (
                    SELECT
                        COALESCE(SUM(total), 0) AS total_production,
                        COALESCE(SUM(deal_count), 0) AS total_deals,
                        COUNT(*) AS active_agents
                    FROM leaderboard_data
                )
                SELECT
                    COALESCE(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'rank', rank,
                                'agent_id', agent_id,
                                'name', name,
                                'total', ROUND(total::numeric, 2),
                                'dailyBreakdown', daily_breakdown,
                                'dealCount', deal_count
                            )
                        ) FROM ranked_leaderboard),
                        '[]'::jsonb
                    ) AS leaderboard,
                    (SELECT jsonb_build_object(
                        'totalProduction', ROUND(total_production::numeric, 2),
                        'totalDeals', total_deals,
                        'activeAgents', active_agents
                    ) FROM overall_stats) AS stats
            """, [agency_id, start_date.isoformat(), end_date.isoformat(), start_date.isoformat(), agency_id])

            row = cursor.fetchone()
            leaderboard = _ensure_list(row[0]) if row else []
            stats = row[1] if row and row[1] else {
                'totalProduction': 0,
                'totalDeals': 0,
                'activeAgents': 0,
            }

            return {
                'success': True,
                'data': {
                    'leaderboard': leaderboard,
                    'stats': stats,
                    'dateRange': {
                        'startDate': start_date.isoformat(),
                        'endDate': end_date.isoformat(),
                    },
                },
            }
    except Exception as e:
        logger.error(f'Error getting scoreboard data: {e}')
        return {
            'success': False,
            'error': str(e),
        }


def get_scoreboard_lapsed_deals(
    user_ctx: UserContext,
    start_date: date,
    end_date: date,
    assumed_months_till_lapse: int = 0,
    scope: str = 'agency',
    submitted: bool = False,
) -> dict:
    """
    Get scoreboard data with updated lapsed deals calculation.

    Translated from Supabase RPC: get_scoreboard_data_updated_lapsed_deals

    Args:
        user_ctx: User context
        start_date: Start date for calculations
        end_date: End date for calculations
        assumed_months_till_lapse: Assumed months till lapse for lapsed deals
        scope: 'agency' for all agency or 'downline' for user's downline only
        submitted: If True, use submission_date; otherwise use policy_effective_date

    Returns:
        {
            'success': True,
            'data': {
                'leaderboard': [...],
                'stats': { totalProduction, totalDeals, activeAgents },
                'dateRange': { startDate, endDate }
            }
        }
    """
    internal_id = str(user_ctx.internal_user_id)
    agency_id = str(user_ctx.agency_id)

    try:
        with connection.cursor() as cursor:
            # Build scope filter - use CTE reference for downline scope
            if scope == 'agency':
                downline_cte = ""
                agent_scope_filter = ""
                deal_scope_filter = ""
            else:  # 'downline'
                downline_cte = """
                user_downline AS (
                    SELECT id FROM users WHERE id = %(internal_id)s::uuid
                    UNION ALL
                    SELECT u2.id FROM users u2 JOIN user_downline d ON u2.upline_id = d.id
                ),
                """
                agent_scope_filter = """
                    AND u.id IN (SELECT id FROM user_downline)
                """
                deal_scope_filter = """
                    AND d.agent_id IN (SELECT id FROM user_downline)
                """

            # Build date field selection based on submitted flag
            date_field = "d.submission_date" if submitted else "COALESCE(d.policy_effective_date, d.submission_date)"

            query = f"""
                WITH RECURSIVE
                {downline_cte}
                agency_agents AS (
                    SELECT
                        u.id AS agent_id,
                        CONCAT(u.first_name, ' ', u.last_name) AS name
                    FROM users u
                    WHERE u.agency_id = %(agency_id)s
                        AND u.role <> 'client'
                        AND u.is_active = true
                        {agent_scope_filter}
                ),

                agency_deals AS (
                    SELECT
                        d.id AS deal_id,
                        d.agent_id,
                        d.carrier_id,
                        d.status,
                        d.status_standardized,
                        d.annual_premium,
                        d.billing_cycle,
                        {date_field} AS policy_start_date
                    FROM deals d
                    JOIN users u ON u.id = d.agent_id AND u.is_active = true
                    WHERE d.agency_id = %(agency_id)s
                        AND {date_field} IS NOT NULL
                        AND {date_field} BETWEEN %(start_date)s AND %(end_date)s
                        AND d.annual_premium > 0
                        {deal_scope_filter}
                ),

                positive_deals AS (
                    SELECT
                        ad.deal_id,
                        ad.agent_id,
                        ad.annual_premium,
                        ad.billing_cycle,
                        ad.policy_start_date,
                        COALESCE(sm.status_standardized = 'Lapsed', false) AS is_lapsed
                    FROM agency_deals ad
                    LEFT JOIN status_mapping sm
                        ON sm.carrier_id = ad.carrier_id
                        AND sm.raw_status = ad.status
                    WHERE
                        %(submitted)s = true
                        OR (
                            COALESCE(sm.impact, 'neutral') = 'positive'
                            OR sm.status_standardized = 'Lapsed'
                        )
                ),

                agent_daily_breakdown AS (
                    SELECT
                        x.agent_id,
                        jsonb_object_agg(
                            x.policy_start_date::text,
                            ROUND(x.daily_total::numeric, 2)
                        ) AS daily_breakdown
                    FROM (
                        SELECT
                            src.agent_id,
                            src.policy_start_date,
                            SUM(src.annual_premium) AS daily_total
                        FROM (
                            SELECT pd.agent_id, pd.policy_start_date, pd.annual_premium
                            FROM positive_deals pd
                            WHERE %(submitted)s = false

                            UNION ALL

                            SELECT ad.agent_id, ad.policy_start_date, ad.annual_premium
                            FROM agency_deals ad
                            WHERE %(submitted)s = true
                        ) src
                        GROUP BY src.agent_id, src.policy_start_date
                    ) x
                    GROUP BY x.agent_id
                ),

                agent_totals AS (
                    SELECT
                        t.agent_id,
                        SUM(t.annual_premium) AS total_production,
                        COUNT(DISTINCT t.deal_id) AS deal_count
                    FROM (
                        SELECT pd.agent_id, pd.deal_id, pd.annual_premium
                        FROM positive_deals pd
                        WHERE %(submitted)s = false

                        UNION ALL

                        SELECT ad.agent_id, ad.deal_id, ad.annual_premium
                        FROM agency_deals ad
                        WHERE %(submitted)s = true
                    ) t
                    GROUP BY t.agent_id
                ),

                new_business_agents AS (
                    SELECT DISTINCT agent_id
                    FROM positive_deals pd
                    WHERE pd.policy_start_date BETWEEN %(start_date)s AND %(end_date)s
                ),

                leaderboard_data AS (
                    SELECT
                        aa.agent_id,
                        aa.name,
                        COALESCE(at.total_production, 0) AS total,
                        COALESCE(adb.daily_breakdown, '{{}}'::jsonb) AS daily_breakdown,
                        COALESCE(at.deal_count, 0) AS deal_count
                    FROM agency_agents aa
                    JOIN new_business_agents nba ON nba.agent_id = aa.agent_id
                    LEFT JOIN agent_totals at ON at.agent_id = aa.agent_id
                    LEFT JOIN agent_daily_breakdown adb ON adb.agent_id = aa.agent_id
                    WHERE COALESCE(at.deal_count, 0) > 0
                    ORDER BY total DESC
                ),

                ranked_leaderboard AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY total DESC) AS rank,
                        agent_id,
                        name,
                        total,
                        daily_breakdown,
                        deal_count
                    FROM leaderboard_data
                ),

                overall_stats AS (
                    SELECT
                        COALESCE(SUM(total), 0) AS total_production,
                        COALESCE(SUM(deal_count), 0) AS total_deals,
                        COUNT(*) AS active_agents
                    FROM leaderboard_data
                )

                SELECT
                    COALESCE(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'rank', rank,
                                'agent_id', agent_id,
                                'name', name,
                                'total', ROUND(total::numeric, 2),
                                'dailyBreakdown', daily_breakdown,
                                'dealCount', deal_count
                            )
                        ) FROM ranked_leaderboard),
                        '[]'::jsonb
                    ) AS leaderboard,
                    (SELECT jsonb_build_object(
                        'totalProduction', ROUND(total_production::numeric, 2),
                        'totalDeals', total_deals,
                        'activeAgents', active_agents
                    ) FROM overall_stats) AS stats
            """

            params = {
                'agency_id': agency_id,
                'internal_id': internal_id,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'submitted': submitted,
            }

            cursor.execute(query, params)  # type: ignore[arg-type]
            row = cursor.fetchone()

            leaderboard: list = _ensure_list(row[0]) if row else []
            stats: dict = row[1] if row and row[1] else {
                'totalProduction': 0,
                'totalDeals': 0,
                'activeAgents': 0,
            }

            return {
                'success': True,
                'data': {
                    'leaderboard': leaderboard,
                    'stats': stats,
                    'dateRange': {
                        'startDate': start_date.isoformat(),
                        'endDate': end_date.isoformat(),
                    },
                },
            }

    except Exception as e:
        logger.error(f'Error getting scoreboard lapsed deals data: {e}')
        return {
            'success': False,
            'error': str(e),
        }


def get_scoreboard_with_billing_cycle(
    user_ctx: UserContext,
    start_date: date,
    end_date: date,
    scope: str = 'agency',
) -> dict:
    """
    Get scoreboard data with billing cycle payment calculation.

    This function calculates recurring payments based on billing_cycle,
    mirroring the frontend logic in /api/scoreboard/route.ts.

    For each deal, payment dates are generated based on billing_cycle:
    - monthly: every 1 month from effective date
    - quarterly: every 3 months
    - semi-annually: every 6 months
    - annually: every 12 months

    Only payments falling within the date range (and not in the future) are counted.

    Args:
        user_ctx: User context
        start_date: Start date for calculations
        end_date: End date for calculations
        scope: 'agency' for all agency or 'downline' for user's downline only

    Returns:
        {
            'success': True,
            'data': {
                'leaderboard': [...],
                'stats': { totalProduction, totalDeals, activeAgents },
                'dateRange': { startDate, endDate }
            }
        }
    """
    internal_id = str(user_ctx.internal_user_id)
    agency_id = str(user_ctx.agency_id)

    try:
        with connection.cursor() as cursor:
            # Build scope filter - use CTE reference for downline scope
            if scope == 'agency':
                downline_cte = ""
                agent_scope_filter = ""
                deal_scope_filter = ""
            else:  # 'downline'
                downline_cte = """
                user_downline AS (
                    SELECT id FROM users WHERE id = %(internal_id)s::uuid
                    UNION ALL
                    SELECT u2.id FROM users u2 JOIN user_downline d ON u2.upline_id = d.id
                ),
                """
                agent_scope_filter = """
                    AND u.id IN (SELECT id FROM user_downline)
                """
                deal_scope_filter = """
                    AND d.agent_id IN (SELECT id FROM user_downline)
                """

            query = f"""
                WITH RECURSIVE
                {downline_cte}
                agency_agents AS (
                    SELECT
                        u.id AS agent_id,
                        CONCAT(u.first_name, ' ', u.last_name) AS name
                    FROM users u
                    WHERE u.agency_id = %(agency_id)s
                        AND u.role <> 'client'
                        AND u.is_active = true
                        {agent_scope_filter}
                ),

                -- Get deals going back 12 months to capture recurring payments
                lookback_deals AS (
                    SELECT
                        d.id AS deal_id,
                        d.agent_id,
                        d.carrier_id,
                        d.status,
                        d.annual_premium,
                        COALESCE(LOWER(d.billing_cycle), 'monthly') AS billing_cycle,
                        COALESCE(d.policy_effective_date, d.submission_date) AS effective_date
                    FROM deals d
                    JOIN users u ON u.id = d.agent_id AND u.is_active = true
                    WHERE d.agency_id = %(agency_id)s
                        AND COALESCE(d.policy_effective_date, d.submission_date) IS NOT NULL
                        AND COALESCE(d.policy_effective_date, d.submission_date) >= (%(start_date)s::date - INTERVAL '1 year')
                        AND COALESCE(d.policy_effective_date, d.submission_date) <= %(end_date)s::date
                        AND d.annual_premium IS NOT NULL
                        AND d.annual_premium > 0
                        {deal_scope_filter}
                ),

                -- Filter to positive impact only
                positive_deals AS (
                    SELECT
                        ld.deal_id,
                        ld.agent_id,
                        ld.annual_premium,
                        ld.billing_cycle,
                        ld.effective_date
                    FROM lookback_deals ld
                    LEFT JOIN status_mapping sm
                        ON sm.carrier_id = ld.carrier_id
                        AND sm.raw_status = ld.status
                    WHERE COALESCE(sm.impact, 'neutral') = 'positive'
                ),

                -- Calculate payment amount and interval based on billing cycle
                deals_with_payments AS (
                    SELECT
                        pd.deal_id,
                        pd.agent_id,
                        pd.annual_premium,
                        pd.effective_date,
                        CASE pd.billing_cycle
                            WHEN 'monthly' THEN pd.annual_premium / 12
                            WHEN 'quarterly' THEN pd.annual_premium / 4
                            WHEN 'semi-annually' THEN pd.annual_premium / 2
                            WHEN 'annually' THEN pd.annual_premium
                            ELSE pd.annual_premium / 12
                        END AS payment_amount,
                        CASE pd.billing_cycle
                            WHEN 'monthly' THEN 1
                            WHEN 'quarterly' THEN 3
                            WHEN 'semi-annually' THEN 6
                            WHEN 'annually' THEN 12
                            ELSE 1
                        END AS months_interval
                    FROM positive_deals pd
                ),

                -- Generate payment dates (up to 12 payments per deal)
                payment_dates AS (
                    SELECT
                        dwp.deal_id,
                        dwp.agent_id,
                        dwp.payment_amount,
                        (dwp.effective_date + (gs.n * dwp.months_interval * INTERVAL '1 month'))::date AS payment_date
                    FROM deals_with_payments dwp
                    CROSS JOIN generate_series(0, 11) AS gs(n)
                    WHERE (dwp.effective_date + (gs.n * dwp.months_interval * INTERVAL '1 month'))::date
                        BETWEEN %(start_date)s::date AND LEAST(%(end_date)s::date, CURRENT_DATE)
                ),

                -- Aggregate by agent and payment date for daily breakdown
                agent_daily_breakdown AS (
                    SELECT
                        pd.agent_id,
                        jsonb_object_agg(
                            pd.payment_date::text,
                            ROUND(pd.daily_total::numeric, 2)
                        ) AS daily_breakdown
                    FROM (
                        SELECT
                            agent_id,
                            payment_date,
                            SUM(payment_amount) AS daily_total
                        FROM payment_dates
                        GROUP BY agent_id, payment_date
                    ) pd
                    GROUP BY pd.agent_id
                ),

                -- Agent totals
                agent_totals AS (
                    SELECT
                        agent_id,
                        SUM(payment_amount) AS total_production,
                        COUNT(DISTINCT deal_id) AS deal_count
                    FROM payment_dates
                    GROUP BY agent_id
                ),

                -- Agents with payments in range
                active_agents_list AS (
                    SELECT DISTINCT agent_id FROM payment_dates
                ),

                leaderboard_data AS (
                    SELECT
                        aa.agent_id,
                        aa.name,
                        COALESCE(at.total_production, 0) AS total,
                        COALESCE(adb.daily_breakdown, '{{}}'::jsonb) AS daily_breakdown,
                        COALESCE(at.deal_count, 0) AS deal_count
                    FROM agency_agents aa
                    JOIN active_agents_list aal ON aal.agent_id = aa.agent_id
                    LEFT JOIN agent_totals at ON at.agent_id = aa.agent_id
                    LEFT JOIN agent_daily_breakdown adb ON adb.agent_id = aa.agent_id
                    WHERE COALESCE(at.deal_count, 0) > 0
                    ORDER BY total DESC
                ),

                ranked_leaderboard AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY total DESC) AS rank,
                        agent_id,
                        name,
                        total,
                        daily_breakdown,
                        deal_count
                    FROM leaderboard_data
                ),

                overall_stats AS (
                    SELECT
                        COALESCE(SUM(total), 0) AS total_production,
                        COALESCE(SUM(deal_count), 0) AS total_deals,
                        COUNT(*) AS active_agents
                    FROM leaderboard_data
                )

                SELECT
                    COALESCE(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'rank', rank,
                                'agent_id', agent_id,
                                'name', name,
                                'total', ROUND(total::numeric, 2),
                                'dailyBreakdown', daily_breakdown,
                                'dealCount', deal_count
                            )
                        ) FROM ranked_leaderboard),
                        '[]'::jsonb
                    ) AS leaderboard,
                    (SELECT jsonb_build_object(
                        'totalProduction', ROUND(total_production::numeric, 2),
                        'totalDeals', total_deals,
                        'activeAgents', active_agents
                    ) FROM overall_stats) AS stats
            """

            params = {
                'agency_id': agency_id,
                'internal_id': internal_id,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
            }

            cursor.execute(query, params)
            row = cursor.fetchone()

            leaderboard = _ensure_list(row[0]) if row else []
            stats = row[1] if row and row[1] else {
                'totalProduction': 0,
                'totalDeals': 0,
                'activeAgents': 0,
            }

            return {
                'success': True,
                'data': {
                    'leaderboard': leaderboard,
                    'stats': stats,
                    'dateRange': {
                        'startDate': start_date.isoformat(),
                        'endDate': end_date.isoformat(),
                    },
                },
            }

    except Exception as e:
        logger.error(f'Error getting scoreboard with billing cycle: {e}')
        return {
            'success': False,
            'error': str(e),
        }


def get_production_data(
    user_ctx: UserContext,
    agent_ids: list[str],
    start_date: date,
    end_date: date
) -> list[dict]:
    """
    Get production data for specified agents.

    Mirrors: get_agents_debt_production RPC function (simplified for production only).

    Returns list of:
        {
            'agent_id': uuid,
            'individual_production': number,
            'individual_production_count': int,
            'hierarchy_production': number,
            'hierarchy_production_count': int,
        }
    """
    if not agent_ids:
        return []

    agency_id = str(user_ctx.agency_id)

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE
                agent_tree AS (
                    SELECT
                        u.id as root_agent_id,
                        u.id as descendant_id,
                        0 as depth
                    FROM users u
                    WHERE u.id = ANY(%s::uuid[])
                        AND u.agency_id = %s

                    UNION ALL

                    SELECT
                        at.root_agent_id,
                        u.id as descendant_id,
                        at.depth + 1
                    FROM agent_tree at
                    JOIN users u ON u.upline_id = at.descendant_id
                    WHERE u.agency_id = %s
                ),
                individual_production AS (
                    SELECT
                        d.agent_id,
                        COALESCE(SUM(d.annual_premium), 0) AS production,
                        COUNT(*)::int AS production_count
                    FROM deals d
                    WHERE d.agent_id = ANY(%s::uuid[])
                        AND d.agency_id = %s
                        AND d.submission_date >= %s
                        AND d.submission_date <= %s
                        AND d.annual_premium IS NOT NULL
                        AND d.annual_premium > 0
                    GROUP BY d.agent_id
                ),
                hierarchy_production AS (
                    SELECT
                        at.root_agent_id AS agent_id,
                        COALESCE(SUM(d.annual_premium), 0) AS production,
                        COUNT(*)::int AS production_count
                    FROM agent_tree at
                    JOIN deals d ON d.agent_id = at.descendant_id
                    WHERE d.agency_id = %s
                        AND d.submission_date >= %s
                        AND d.submission_date <= %s
                        AND d.annual_premium IS NOT NULL
                        AND d.annual_premium > 0
                    GROUP BY at.root_agent_id
                )
                SELECT
                    a.id AS agent_id,
                    COALESCE(ip.production, 0) AS individual_production,
                    COALESCE(ip.production_count, 0) AS individual_production_count,
                    COALESCE(hp.production, 0) AS hierarchy_production,
                    COALESCE(hp.production_count, 0) AS hierarchy_production_count
                FROM unnest(%s::uuid[]) AS a(id)
                LEFT JOIN individual_production ip ON ip.agent_id = a.id
                LEFT JOIN hierarchy_production hp ON hp.agent_id = a.id
            """, [
                agent_ids, agency_id, agency_id,
                agent_ids, agency_id, start_date.isoformat(), end_date.isoformat(),
                agency_id, start_date.isoformat(), end_date.isoformat(),
                agent_ids
            ])

            results = []
            for row in cursor.fetchall():
                results.append({
                    'agent_id': str(row[0]) if row[0] else None,
                    'individual_production': float(row[1]) if row[1] else 0,
                    'individual_production_count': row[2] or 0,
                    'hierarchy_production': float(row[3]) if row[3] else 0,
                    'hierarchy_production_count': row[4] or 0,
                })

            return results
    except Exception as e:
        logger.error(f'Error getting production data: {e}')
        raise


# =============================================================================
# Dashboard Widgets (P2-032)
# =============================================================================

@dataclass
class WidgetInput:
    """Input for creating/updating a widget."""
    widget_type: str
    title: str
    position: int = 0
    config: dict | None = None
    is_visible: bool = True


def get_user_widgets(user_id: UUID) -> list[dict]:
    """Get all widgets for a user."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT id, widget_type, title, position, config, is_visible, created_at, updated_at
            FROM public.dashboard_widgets
            WHERE user_id = %s
            ORDER BY position, created_at
        """, [str(user_id)])
        rows = cursor.fetchall()

    return [
        {
            'id': str(row[0]),
            'widget_type': row[1],
            'title': row[2],
            'position': row[3],
            'config': row[4] or {},
            'is_visible': row[5],
            'created_at': row[6].isoformat() if row[6] else None,
            'updated_at': row[7].isoformat() if row[7] else None,
        }
        for row in rows
    ]


def create_widget(user_id: UUID, data: WidgetInput) -> dict:
    """Create a new dashboard widget."""
    widget_id = uuid.uuid4()

    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO public.dashboard_widgets (
                id, user_id, widget_type, title, position, config, is_visible, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id, created_at
        """, [
            str(widget_id),
            str(user_id),
            data.widget_type,
            data.title,
            data.position,
            Json(data.config or {}),  # type: ignore[arg-type]
            data.is_visible,
        ])

    return get_widget_by_id(widget_id, user_id)  # type: ignore[return-value]


def update_widget(widget_id: UUID, user_id: UUID, data: WidgetInput) -> dict | None:
    """Update an existing widget."""
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.dashboard_widgets
            SET widget_type = %s, title = %s, position = %s, config = %s, is_visible = %s, updated_at = NOW()
            WHERE id = %s AND user_id = %s
            RETURNING id
        """, [
            data.widget_type,
            data.title,
            data.position,
            Json(data.config or {}),  # type: ignore[arg-type]
            data.is_visible,
            str(widget_id),
            str(user_id),
        ])
        row = cursor.fetchone()  # type: ignore[assignment]

    if not row:
        return None

    return get_widget_by_id(widget_id, user_id)


def delete_widget(widget_id: UUID, user_id: UUID) -> bool:
    """Delete a widget."""
    with connection.cursor() as cursor:
        cursor.execute("""
            DELETE FROM public.dashboard_widgets
            WHERE id = %s AND user_id = %s
            RETURNING id
        """, [str(widget_id), str(user_id)])
        row = cursor.fetchone()

    return row is not None


def get_widget_by_id(widget_id: UUID, user_id: UUID) -> dict | None:
    """Get a single widget by ID."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT id, widget_type, title, position, config, is_visible, created_at, updated_at
            FROM public.dashboard_widgets
            WHERE id = %s AND user_id = %s
        """, [str(widget_id), str(user_id)])
        row = cursor.fetchone()

    if not row:
        return None

    return {
        'id': str(row[0]),
        'widget_type': row[1],
        'title': row[2],
        'position': row[3],
        'config': row[4] or {},
        'is_visible': row[5],
        'created_at': row[6].isoformat() if row[6] else None,
        'updated_at': row[7].isoformat() if row[7] else None,
    }


def reorder_widgets(user_id: UUID, widget_positions: list[dict]) -> list[dict]:
    """Reorder widgets by updating their positions."""
    with connection.cursor() as cursor:
        for item in widget_positions:
            cursor.execute("""
                UPDATE public.dashboard_widgets
                SET position = %s, updated_at = NOW()
                WHERE id = %s AND user_id = %s
            """, [item['position'], str(item['id']), str(user_id)])

    return get_user_widgets(user_id)


# =============================================================================
# Report Generation (P2-033)
# =============================================================================

@dataclass
class ReportInput:
    """Input for creating a report."""
    report_type: str
    title: str
    parameters: dict
    format: str = 'csv'


def create_report(user_ctx: UserContext, data: ReportInput) -> dict:
    """Create a new report request."""
    report_id = uuid.uuid4()

    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO public.reports (
                id, agency_id, user_id, report_type, title, parameters, format, status, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', NOW())
            RETURNING id, created_at
        """, [
            str(report_id),
            str(user_ctx.agency_id),
            str(user_ctx.internal_user_id),
            data.report_type,
            data.title,
            Json(data.parameters),  # type: ignore[arg-type]
            data.format,
        ])

    return get_report_by_id(report_id, user_ctx)  # type: ignore[return-value]


def get_report_by_id(report_id: UUID, user_ctx: UserContext) -> dict | None:
    """Get a report by ID."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                r.id, r.report_type, r.title, r.parameters, r.format, r.status,
                r.file_url, r.error_message, r.created_at, r.completed_at,
                u.first_name, u.last_name
            FROM public.reports r
            LEFT JOIN public.users u ON u.id = r.user_id
            WHERE r.id = %s AND r.agency_id = %s
        """, [str(report_id), str(user_ctx.agency_id)])
        row = cursor.fetchone()

    if not row:
        return None

    return {
        'id': str(row[0]),
        'report_type': row[1],
        'title': row[2],
        'parameters': row[3],
        'format': row[4],
        'status': row[5],
        'file_url': row[6],
        'error_message': row[7],
        'created_at': row[8].isoformat() if row[8] else None,
        'completed_at': row[9].isoformat() if row[9] else None,
        'created_by': f"{row[10] or ''} {row[11] or ''}".strip() or None,
    }


def list_reports(user_ctx: UserContext, limit: int = 50) -> list[dict]:
    """List reports for the agency."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                r.id, r.report_type, r.title, r.parameters, r.format, r.status,
                r.file_url, r.error_message, r.created_at, r.completed_at,
                u.first_name, u.last_name
            FROM public.reports r
            LEFT JOIN public.users u ON u.id = r.user_id
            WHERE r.agency_id = %s
            ORDER BY r.created_at DESC
            LIMIT %s
        """, [str(user_ctx.agency_id), limit])
        rows = cursor.fetchall()

    return [
        {
            'id': str(row[0]),
            'report_type': row[1],
            'title': row[2],
            'parameters': row[3],
            'format': row[4],
            'status': row[5],
            'file_url': row[6],
            'error_message': row[7],
            'created_at': row[8].isoformat() if row[8] else None,
            'completed_at': row[9].isoformat() if row[9] else None,
            'created_by': f"{row[10] or ''} {row[11] or ''}".strip() or None,
        }
        for row in rows
    ]


def generate_report(report_id: UUID, user_ctx: UserContext) -> dict | None:
    """
    Generate a report and return its data.

    This is a synchronous implementation. For async, use Django Tasks.
    """
    report = get_report_by_id(report_id, user_ctx)
    if not report:
        return None

    # Update status to generating
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.reports
            SET status = 'generating'
            WHERE id = %s
        """, [str(report_id)])

    try:
        report_type = report['report_type']
        params = report['parameters']
        report['format']

        # Generate report data based on type
        if report_type == 'production':
            data = _generate_production_report(user_ctx, params)
        elif report_type == 'pipeline':
            data = _generate_pipeline_report(user_ctx, params)
        elif report_type == 'team_performance':
            data = _generate_team_performance_report(user_ctx, params)
        elif report_type == 'revenue':
            data = _generate_revenue_report(user_ctx, params)
        elif report_type == 'commission':
            data = _generate_commission_report(user_ctx, params)
        else:
            raise ValueError(f"Unknown report type: {report_type}")

        # Update status to completed
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE public.reports
                SET status = 'completed', completed_at = NOW()
                WHERE id = %s
            """, [str(report_id)])

        return {
            'report': get_report_by_id(report_id, user_ctx),
            'data': data,
        }

    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE public.reports
                SET status = 'failed', error_message = %s
                WHERE id = %s
            """, [str(e), str(report_id)])
        return None


def _generate_production_report(user_ctx: UserContext, params: dict) -> list[dict]:
    """Generate production report data."""
    start_date = params.get('start_date', (date.today() - timedelta(days=30)).isoformat())
    end_date = params.get('end_date', date.today().isoformat())

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                d.id,
                d.policy_number,
                d.status,
                d.annual_premium,
                d.monthly_premium,
                d.policy_effective_date,
                d.submission_date,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                c.first_name as client_first_name,
                c.last_name as client_last_name,
                ca.name as carrier_name,
                p.name as product_name
            FROM public.deals d
            LEFT JOIN public.users u ON u.id = d.agent_id
            LEFT JOIN public.clients c ON c.id = d.client_id
            LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
            LEFT JOIN public.products p ON p.id = d.product_id
            WHERE d.agency_id = %s
                AND d.submission_date BETWEEN %s AND %s
            ORDER BY d.submission_date DESC
        """, [str(user_ctx.agency_id), start_date, end_date])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row, strict=False)) for row in rows]


def _generate_pipeline_report(user_ctx: UserContext, params: dict) -> list[dict]:
    """Generate pipeline report data."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                d.status_standardized,
                COUNT(*) as count,
                SUM(d.annual_premium) as total_premium
            FROM public.deals d
            WHERE d.agency_id = %s
                AND d.status_standardized IS NOT NULL
            GROUP BY d.status_standardized
            ORDER BY d.status_standardized
        """, [str(user_ctx.agency_id)])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row, strict=False)) for row in rows]


def _generate_team_performance_report(user_ctx: UserContext, params: dict) -> list[dict]:
    """Generate team performance report data."""
    start_date = params.get('start_date', (date.today() - timedelta(days=30)).isoformat())
    end_date = params.get('end_date', date.today().isoformat())

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                u.id as agent_id,
                u.first_name,
                u.last_name,
                u.email,
                COUNT(d.id) as deal_count,
                COALESCE(SUM(d.annual_premium), 0) as total_premium,
                COALESCE(AVG(d.annual_premium), 0) as avg_premium
            FROM public.users u
            LEFT JOIN public.deals d ON d.agent_id = u.id
                AND d.submission_date BETWEEN %s AND %s
            WHERE u.agency_id = %s
                AND u.role != 'client'
            GROUP BY u.id, u.first_name, u.last_name, u.email
            ORDER BY total_premium DESC
        """, [start_date, end_date, str(user_ctx.agency_id)])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row, strict=False)) for row in rows]


def _generate_revenue_report(user_ctx: UserContext, params: dict) -> list[dict]:
    """Generate revenue report data."""
    start_date = params.get('start_date', (date.today() - timedelta(days=365)).isoformat())
    end_date = params.get('end_date', date.today().isoformat())

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                date_trunc('month', d.policy_effective_date)::date as month,
                COUNT(*) as deal_count,
                SUM(d.annual_premium) as total_annual_premium,
                SUM(d.monthly_premium) as total_monthly_premium
            FROM public.deals d
            WHERE d.agency_id = %s
                AND d.policy_effective_date BETWEEN %s AND %s
            GROUP BY date_trunc('month', d.policy_effective_date)
            ORDER BY month
        """, [str(user_ctx.agency_id), start_date, end_date])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row, strict=False)) for row in rows]


def _generate_commission_report(user_ctx: UserContext, params: dict) -> list[dict]:
    """Generate commission report data.

    Uses the correct commission formula: annual_premium * 0.75 * (agent_% / hierarchy_total_%)
    This matches the payout calculation formula in /apps/payouts/selectors.py
    """
    start_date = params.get('start_date', (date.today() - timedelta(days=30)).isoformat())
    end_date = params.get('end_date', date.today().isoformat())

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH
            -- Get all deals in date range for the agency
            deals_in_range AS (
                SELECT
                    d.id as deal_id,
                    d.policy_number,
                    d.annual_premium,
                    d.policy_effective_date
                FROM public.deals d
                WHERE d.agency_id = %s
                    AND d.policy_effective_date BETWEEN %s AND %s
                    AND d.annual_premium IS NOT NULL
                    AND d.annual_premium > 0
            ),

            -- Calculate total commission percentage for each deal's hierarchy
            hierarchy_totals AS (
                SELECT
                    deal_id,
                    SUM(commission_percentage) as hierarchy_total_percentage
                FROM public.deal_hierarchy_snapshots
                WHERE deal_id IN (SELECT deal_id FROM deals_in_range)
                    AND commission_percentage IS NOT NULL
                GROUP BY deal_id
            )

            SELECT
                dhs.agent_id,
                u.first_name,
                u.last_name,
                dhs.hierarchy_level,
                dhs.commission_percentage,
                d.annual_premium,
                CASE
                    WHEN ht.hierarchy_total_percentage > 0
                         AND dhs.commission_percentage IS NOT NULL
                    THEN ROUND(
                        (d.annual_premium * 0.75 * (dhs.commission_percentage / ht.hierarchy_total_percentage))::numeric,
                        2
                    )
                    ELSE 0
                END as commission_amount,
                d.policy_number,
                d.policy_effective_date
            FROM deals_in_range d
            JOIN public.deal_hierarchy_snapshots dhs ON dhs.deal_id = d.deal_id
            LEFT JOIN hierarchy_totals ht ON ht.deal_id = d.deal_id
            LEFT JOIN public.users u ON u.id = dhs.agent_id
            WHERE dhs.commission_percentage IS NOT NULL
            ORDER BY d.policy_effective_date DESC, dhs.hierarchy_level
        """, [str(user_ctx.agency_id), start_date, end_date])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    return [dict(zip(columns, row, strict=False)) for row in rows]


# =============================================================================
# Export Functions (P2-035)
# =============================================================================

def export_to_csv(data: list[dict]) -> str:
    """Export data to CSV format."""
    if not data:
        return ""

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def export_to_excel(data: list[dict], sheet_name: str = 'Report') -> bytes:
    """Export data to Excel format."""
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    if not data:
        return io.BytesIO().getvalue()  # type: ignore[return-value]

    # Write header
    headers = list(data[0].keys())
    ws.append(headers)

    # Write data
    for row in data:
        ws.append([row.get(h) for h in headers])

    # Save to bytes
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


def export_to_pdf(data: list[dict], title: str = 'Report') -> bytes:
    """Export data to PDF format."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    output = io.BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(letter))
    elements = []

    # Add title
    styles = getSampleStyleSheet()
    elements.append(Paragraph(title, styles['Heading1']))
    elements.append(Spacer(1, 12))

    if not data:
        elements.append(Paragraph("No data available", styles['Normal']))
    else:
        # Create table data
        headers = list(data[0].keys())
        table_data = [headers]
        max_len = EXPORT["pdf_cell_max_length"]
        for row in data:
            table_data.append([str(row.get(h, ''))[:max_len] for h in headers])

        # Create table
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(table)

    doc.build(elements)
    output.seek(0)
    return output.getvalue()


# =============================================================================
# Scheduled Reports (P2-034)
# =============================================================================

@dataclass
class ScheduledReportInput:
    """Input for creating a scheduled report."""
    report_type: str
    title: str
    parameters: dict
    format: str
    frequency: str
    email_recipients: list[str]
    is_active: bool = True


def create_scheduled_report(user_ctx: UserContext, data: ScheduledReportInput) -> dict:
    """Create a new scheduled report."""
    report_id = uuid.uuid4()
    next_run = _calculate_next_run(data.frequency)

    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO public.scheduled_reports (
                id, agency_id, user_id, report_type, title, parameters, format,
                frequency, email_recipients, is_active, next_run_at, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id
        """, [
            str(report_id),
            str(user_ctx.agency_id),
            str(user_ctx.internal_user_id),
            data.report_type,
            data.title,
            Json(data.parameters),  # type: ignore[arg-type]
            data.format,
            data.frequency,
            data.email_recipients,
            data.is_active,
            next_run,
        ])

    return get_scheduled_report_by_id(report_id, user_ctx)  # type: ignore[return-value]


def get_scheduled_report_by_id(report_id: UUID, user_ctx: UserContext) -> dict | None:
    """Get a scheduled report by ID."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id, report_type, title, parameters, format, frequency,
                email_recipients, is_active, last_run_at, next_run_at, created_at, updated_at
            FROM public.scheduled_reports
            WHERE id = %s AND agency_id = %s
        """, [str(report_id), str(user_ctx.agency_id)])
        row = cursor.fetchone()

    if not row:
        return None

    return {
        'id': str(row[0]),
        'report_type': row[1],
        'title': row[2],
        'parameters': row[3],
        'format': row[4],
        'frequency': row[5],
        'email_recipients': row[6],
        'is_active': row[7],
        'last_run_at': row[8].isoformat() if row[8] else None,
        'next_run_at': row[9].isoformat() if row[9] else None,
        'created_at': row[10].isoformat() if row[10] else None,
        'updated_at': row[11].isoformat() if row[11] else None,
    }


def list_scheduled_reports(user_ctx: UserContext) -> list[dict]:
    """List scheduled reports for the agency."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id, report_type, title, parameters, format, frequency,
                email_recipients, is_active, last_run_at, next_run_at, created_at, updated_at
            FROM public.scheduled_reports
            WHERE agency_id = %s
            ORDER BY created_at DESC
        """, [str(user_ctx.agency_id)])
        rows = cursor.fetchall()

    return [
        {
            'id': str(row[0]),
            'report_type': row[1],
            'title': row[2],
            'parameters': row[3],
            'format': row[4],
            'frequency': row[5],
            'email_recipients': row[6],
            'is_active': row[7],
            'last_run_at': row[8].isoformat() if row[8] else None,
            'next_run_at': row[9].isoformat() if row[9] else None,
            'created_at': row[10].isoformat() if row[10] else None,
            'updated_at': row[11].isoformat() if row[11] else None,
        }
        for row in rows
    ]


def update_scheduled_report(
    report_id: UUID,
    user_ctx: UserContext,
    data: ScheduledReportInput
) -> dict | None:
    """Update a scheduled report."""
    next_run = _calculate_next_run(data.frequency)

    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.scheduled_reports
            SET report_type = %s, title = %s, parameters = %s, format = %s,
                frequency = %s, email_recipients = %s, is_active = %s,
                next_run_at = %s, updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [
            data.report_type,
            data.title,
            Json(data.parameters),  # type: ignore[arg-type]
            data.format,
            data.frequency,
            data.email_recipients,
            data.is_active,
            next_run,
            str(report_id),
            str(user_ctx.agency_id),
        ])
        row = cursor.fetchone()  # type: ignore[assignment]

    if not row:
        return None

    return get_scheduled_report_by_id(report_id, user_ctx)


def delete_scheduled_report(report_id: UUID, user_ctx: UserContext) -> bool:
    """Delete a scheduled report."""
    with connection.cursor() as cursor:
        cursor.execute("""
            DELETE FROM public.scheduled_reports
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(report_id), str(user_ctx.agency_id)])
        row = cursor.fetchone()

    return row is not None


def _calculate_next_run(frequency: str) -> datetime:
    """Calculate the next run time based on frequency."""
    now = timezone.now()

    if frequency == 'daily':
        return now + timedelta(days=1)
    elif frequency == 'weekly':
        return now + timedelta(weeks=1)
    elif frequency == 'monthly':
        return now + timedelta(days=30)
    elif frequency == 'quarterly':
        return now + timedelta(days=90)
    else:
        return now + timedelta(days=1)
