"""
Dashboard Services

Contains the business logic for retrieving dashboard metrics.
Mirrors the logic of Supabase RPC functions:
- get_dashboard_data_with_agency_id
- get_scoreboard_data
- get_agents_debt_production
"""
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)


@dataclass
class UserContext:
    """User context derived from authenticated request."""
    internal_user_id: UUID
    auth_user_id: UUID
    agency_id: UUID
    email: str
    is_admin: bool


def get_user_context_from_auth_id(auth_user_id: UUID) -> Optional[UserContext]:
    """
    Get user context from auth_user_id.

    Args:
        auth_user_id: The Supabase auth user ID (from JWT sub claim)

    Returns:
        UserContext if found, None otherwise
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    u.id,
                    u.auth_user_id,
                    u.agency_id,
                    u.email,
                    COALESCE(u.is_admin, false) OR u.perm_level = 'admin' OR u.role = 'admin'
                FROM users u
                WHERE u.auth_user_id = %s
                LIMIT 1
            """, [str(auth_user_id)])
            row = cursor.fetchone()

            if not row:
                return None

            return UserContext(
                internal_user_id=row[0],
                auth_user_id=row[1],
                agency_id=row[2],
                email=row[3] or '',
                is_admin=row[4] or False,
            )
    except Exception as e:
        logger.error(f'Error getting user context: {e}')
        return None


def get_dashboard_summary(user_ctx: UserContext, as_of_date: Optional[date] = None) -> dict:
    """
    Get dashboard summary data.

    Mirrors: get_dashboard_data_with_agency_id RPC function.

    Returns:
        {
            'your_deals': { active_policies, monthly_commissions, new_policies, total_clients, carriers_active },
            'downline_production': { ... same structure ... },
            'totals': { pending_positions }
        }
    """
    if as_of_date is None:
        as_of_date = date.today()

    internal_id = str(user_ctx.internal_user_id)
    agency_id = str(user_ctx.agency_id)
    is_admin = user_ctx.is_admin

    try:
        with connection.cursor() as cursor:
            # ==================== YOUR DEALS ====================
            cursor.execute("""
                WITH
                your_base AS (
                    SELECT
                        d.id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status as raw_status,
                        d.policy_effective_date,
                        d.monthly_premium,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m
                        ON m.carrier_id = d.carrier_id
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
                        COALESCE(SUM(monthly_premium), 0)::numeric(12,2) AS monthly_commissions,
                        COUNT(*) FILTER (
                            WHERE policy_effective_date >= (%s::date - interval '1 week')
                        )::int AS new_policies
                    FROM your_active
                ),
                your_clients AS (
                    SELECT COUNT(DISTINCT d.client_id)::int AS total_clients
                    FROM deals d
                    WHERE d.agency_id = %s
                        AND d.agent_id = %s
                        AND d.client_id IS NOT NULL
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
            """, [agency_id, internal_id, as_of_date.isoformat(), agency_id, internal_id])
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
                downline_filter = "d.agency_id = %s"
                downline_params = [agency_id, as_of_date.isoformat(), agency_id]
            else:
                # Agents see only their downline deals (excluding their own)
                downline_filter = """
                    d.agency_id = %s
                    AND d.id IN (
                        SELECT deal_id
                        FROM deal_hierarchy_snapshot
                        WHERE agent_id = %s
                    )
                    AND d.agent_id != %s
                """
                downline_params = [agency_id, internal_id, internal_id, as_of_date.isoformat(), agency_id, internal_id, internal_id]

            cursor.execute(f"""
                WITH
                downline_base AS (
                    SELECT
                        d.id,
                        d.carrier_id,
                        c.name as carrier_name,
                        d.status as raw_status,
                        d.policy_effective_date,
                        d.monthly_premium,
                        COALESCE(m.impact, 'neutral') as impact
                    FROM deals d
                    JOIN carriers c ON c.id = d.carrier_id
                    LEFT JOIN status_mapping m
                        ON m.carrier_id = d.carrier_id
                        AND m.raw_status = d.status
                    WHERE {downline_filter}
                ),
                downline_active AS (
                    SELECT * FROM downline_base WHERE impact = 'positive'
                ),
                downline_totals AS (
                    SELECT
                        COUNT(*)::int AS active_policies,
                        COALESCE(SUM(monthly_premium), 0)::numeric(12,2) AS monthly_commissions,
                        COUNT(*) FILTER (
                            WHERE policy_effective_date >= (%s::date - interval '1 week')
                        )::int AS new_policies
                    FROM downline_active
                ),
                downline_clients AS (
                    SELECT COUNT(DISTINCT d.client_id)::int AS total_clients
                    FROM deals d
                    WHERE {downline_filter.replace('%s', '%s', 1) if is_admin else downline_filter}
                        AND d.client_id IS NOT NULL
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
            """, downline_params)
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
            else:
                cursor.execute("""
                    WITH visible_users AS (
                        SELECT gd.id AS user_id
                        FROM public.get_agent_downline(%s) gd
                        UNION
                        SELECT %s AS user_id
                    )
                    SELECT COUNT(*)::int
                    FROM visible_users vu
                    JOIN users u ON u.id = vu.user_id
                    WHERE u.position_id IS NULL
                        AND u.role <> 'client'
                """, [internal_id, internal_id])

            pending_row = cursor.fetchone()
            pending_positions = pending_row[0] if pending_row else 0

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
            leaderboard = row[0] if row and row[0] else []
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
    assumed_months = max(assumed_months_till_lapse or 0, 0)

    try:
        with connection.cursor() as cursor:
            # Build scope filter for agents and deals
            if scope == 'agency':
                agent_scope_filter = ""
                deal_scope_filter = ""
            else:  # 'downline'
                agent_scope_filter = """
                    AND (
                        u.id = %(internal_id)s
                        OR u.id IN (
                            WITH RECURSIVE downline AS (
                                SELECT id FROM users WHERE id = %(internal_id)s::uuid
                                UNION ALL
                                SELECT u2.id FROM users u2 JOIN downline d ON u2.upline_id = d.id
                            )
                            SELECT id FROM downline WHERE id != %(internal_id)s::uuid
                        )
                    )
                """
                deal_scope_filter = """
                    AND (
                        d.agent_id = %(internal_id)s::uuid
                        OR d.agent_id IN (
                            WITH RECURSIVE downline AS (
                                SELECT id FROM users WHERE id = %(internal_id)s::uuid
                                UNION ALL
                                SELECT u2.id FROM users u2 JOIN downline d ON u2.upline_id = d.id
                            )
                            SELECT id FROM downline WHERE id != %(internal_id)s::uuid
                        )
                    )
                """

            # Build date field selection based on submitted flag
            date_field = "d.submission_date" if submitted else "COALESCE(d.policy_effective_date, d.submission_date)"

            query = f"""
                WITH
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

            cursor.execute(query, params)
            row = cursor.fetchone()

            leaderboard = row[0] if row and row[0] else []
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
        logger.error(f'Error getting scoreboard lapsed deals data: {e}')
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
