"""
Payout Selectors (P2-029)

Complex queries for commission payout calculations.
"""
import logging
from datetime import date
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser
from apps.core.permissions import get_visible_agent_ids

logger = logging.getLogger(__name__)



def get_expected_payouts(
    user: AuthenticatedUser,
    start_date: date | None = None,
    end_date: date | None = None,
    agent_id: UUID | None = None,
    carrier_id: UUID | None = None,
    include_full_agency: bool = False,
    production_type: str | None = None,  # 'personal', 'downline', or None for all
) -> dict:
    """
    Calculate expected commission payouts using historical hierarchy snapshots.

    Uses deal_hierarchy_snapshot for historical commission rates (not current position rates).
    Formula: annual_premium * 0.75 * (agent_commission_% / hierarchy_total_%)

    Args:
        user: The authenticated user
        start_date: Filter by policy effective date (from)
        end_date: Filter by policy effective date (to)
        agent_id: Filter by specific agent
        carrier_id: Filter by carrier
        include_full_agency: If True and user is admin, include all agency payouts
        production_type: 'personal' (hierarchy_level=0), 'downline' (level>0), or None (all)

    Returns:
        Dictionary with payouts list, totals, and summary breakdown
    """
    is_admin = user.is_admin or user.role == 'admin'

    # Build visible agent filter
    visible_ids = (
        [agent_id]
        if agent_id
        else get_visible_agent_ids(user, include_full_agency=include_full_agency and is_admin)
    )

    if not visible_ids:
        return {'payouts': [], 'total_expected': 0, 'total_premium': 0, 'deal_count': 0, 'summary': {}}

    # Convert visible_ids to list of strings for PostgreSQL array parameter (safe from SQL injection)
    visible_ids_list = [str(vid) for vid in visible_ids]

    # Build WHERE clauses for deals
    params = [str(user.agency_id)]
    where_clauses = ["d.agency_id = %s"]

    if start_date:
        where_clauses.append("d.policy_effective_date >= %s")
        params.append(start_date.isoformat())

    if end_date:
        where_clauses.append("d.policy_effective_date <= %s")
        params.append(end_date.isoformat())

    if carrier_id:
        where_clauses.append("d.carrier_id = %s")
        params.append(str(carrier_id))

    where_sql = " AND ".join(where_clauses)

    # Build production type filter for hierarchy level
    # Note: hierarchy_level is computed in the agent_hierarchy_levels CTE, aliased as ahl
    hierarchy_level_filter = ""
    if production_type == 'personal':
        hierarchy_level_filter = "AND COALESCE(ahl.hierarchy_level, 0) = 0"
    elif production_type == 'downline':
        hierarchy_level_filter = "AND COALESCE(ahl.hierarchy_level, 0) > 0"

    # Add visible_ids_list to params for the first use in query
    params.extend(visible_ids_list)  # type: ignore[arg-type]

    query = f"""
        WITH
        -- Get deals where any of our visible agents are in the hierarchy
        relevant_deals AS (
            SELECT DISTINCT
                d.id as deal_id,
                d.policy_number,
                d.annual_premium,
                d.policy_effective_date,
                d.status,
                d.status_standardized,
                d.carrier_id,
                d.client_id,
                d.client_name,
                d.product_id
            FROM public.deals d
            INNER JOIN public.deal_hierarchy_snapshot dhs ON dhs.deal_id = d.id
            WHERE {where_sql}
              AND dhs.agent_id = ANY(%s::uuid[])
              AND d.annual_premium IS NOT NULL
              AND d.annual_premium > 0
        ),

        -- Calculate hierarchy total percentage for each deal
        hierarchy_totals AS (
            SELECT
                deal_id,
                SUM(commission_percentage) as hierarchy_total_percentage
            FROM public.deal_hierarchy_snapshots
            WHERE deal_id IN (SELECT deal_id FROM relevant_deals)
              AND commission_percentage IS NOT NULL
            GROUP BY deal_id
        ),

        -- Filter by status impact (positive or neutral only)
        -- Uses INNER JOIN to match RPC behavior - deals without status mapping are excluded
        filtered_deals AS (
            SELECT
                rd.*,
                COALESCE(ht.hierarchy_total_percentage, 0) as hierarchy_total_percentage
            FROM relevant_deals rd
            LEFT JOIN hierarchy_totals ht ON ht.deal_id = rd.deal_id
            INNER JOIN public.status_mapping sm
                ON sm.carrier_id = rd.carrier_id
                AND LOWER(sm.raw_status) = LOWER(rd.status)
            WHERE sm.impact IN ('positive', 'neutral')
        ),

        -- Calculate hierarchy level (depth from writing agent) for each agent in each deal
        agent_hierarchy_levels AS (
            SELECT
                dhs.deal_id,
                dhs.agent_id,
                -- Count how many upline hops from writing agent (level 0)
                (
                    SELECT COUNT(*)
                    FROM public.deal_hierarchy_snapshot parent
                    WHERE parent.deal_id = dhs.deal_id
                      AND parent.agent_id != dhs.agent_id
                      -- Parent must be in the chain between this agent and deal
                      AND (
                          dhs.upline_id IS NOT NULL
                          AND (parent.agent_id = dhs.upline_id
                               OR EXISTS (
                                   SELECT 1 FROM public.deal_hierarchy_snapshot grandparent
                                   WHERE grandparent.deal_id = dhs.deal_id
                                     AND grandparent.agent_id = dhs.upline_id
                               ))
                      )
                )::int as hierarchy_level
            FROM public.deal_hierarchy_snapshot dhs
            WHERE dhs.deal_id IN (SELECT deal_id FROM filtered_deals)
        ),

        -- Get agent-specific payout info from hierarchy snapshot
        agent_payouts AS (
            SELECT
                fd.deal_id,
                fd.policy_number,
                fd.annual_premium,
                fd.policy_effective_date,
                fd.status,
                fd.status_standardized,
                fd.hierarchy_total_percentage,
                dhs.agent_id,
                COALESCE(ahl.hierarchy_level, 0) as hierarchy_level,
                dhs.commission_percentage as agent_commission_percentage,
                fd.client_name,
                cli.first_name as client_first_name,
                cli.last_name as client_last_name,
                ca.name as carrier_name,
                pr.name as product_name,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                po.name as position_name,
                CASE
                    WHEN fd.hierarchy_total_percentage > 0
                         AND dhs.commission_percentage IS NOT NULL
                    THEN ROUND(
                        (fd.annual_premium * 0.75 * (dhs.commission_percentage / fd.hierarchy_total_percentage))::numeric,
                        2
                    )
                    ELSE 0
                END as expected_payout
            FROM filtered_deals fd
            INNER JOIN public.deal_hierarchy_snapshot dhs ON dhs.deal_id = fd.deal_id
            LEFT JOIN agent_hierarchy_levels ahl ON ahl.deal_id = dhs.deal_id AND ahl.agent_id = dhs.agent_id
            LEFT JOIN public.users cli ON cli.id = fd.client_id AND cli.role = 'client'
            LEFT JOIN public.carriers ca ON ca.id = fd.carrier_id
            LEFT JOIN public.products pr ON pr.id = fd.product_id
            LEFT JOIN public.users u ON u.id = dhs.agent_id
            LEFT JOIN public.positions po ON po.id = u.position_id
            WHERE dhs.agent_id = ANY(%s::uuid[])
              AND dhs.commission_percentage IS NOT NULL
              {hierarchy_level_filter}
        )

        SELECT
            deal_id,
            policy_number,
            annual_premium,
            policy_effective_date,
            status,
            status_standardized,
            hierarchy_total_percentage,
            agent_id,
            hierarchy_level,
            agent_commission_percentage,
            client_name,
            client_first_name,
            client_last_name,
            carrier_name,
            product_name,
            agent_first_name,
            agent_last_name,
            position_name,
            expected_payout
        FROM agent_payouts
        ORDER BY policy_effective_date DESC NULLS LAST, deal_id DESC, hierarchy_level ASC
    """

    # Add visible_ids_list again for the second usage in agent_payouts CTE
    params.extend(visible_ids_list)  # type: ignore[arg-type]

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        payouts = []
        total_expected: float = 0.0
        total_premium: float = 0.0
        seen_deals = set()  # Track unique deals for premium calculation
        by_carrier = {}
        by_agent = {}
        personal_production: float = 0.0
        downline_production: float = 0.0

        for row in rows:
            payout = dict(zip(columns, row, strict=False))
            expected = float(payout['expected_payout']) if payout['expected_payout'] else 0.0
            premium = float(payout['annual_premium']) if payout['annual_premium'] else 0.0
            deal_id = str(payout['deal_id'])
            hierarchy_level = int(payout['hierarchy_level'] or 0)

            total_expected += expected

            # Check if this is a new deal BEFORE adding to seen_deals
            is_new_deal = deal_id not in seen_deals

            # Only count premium once per unique deal
            if is_new_deal:
                total_premium += premium
                seen_deals.add(deal_id)

            # Track personal vs downline production
            if hierarchy_level == 0:
                personal_production += expected
            else:
                downline_production += expected

            # Aggregate by carrier
            carrier = payout['carrier_name'] or 'Unknown'
            if carrier not in by_carrier:
                by_carrier[carrier] = {'premium': 0.0, 'payout': 0.0, 'count': 0}
            if is_new_deal:
                by_carrier[carrier]['premium'] += premium  # type: ignore[operator]
            by_carrier[carrier]['payout'] += expected  # type: ignore[operator]
            by_carrier[carrier]['count'] += 1  # type: ignore[operator]

            # Aggregate by agent
            agent_name = f"{payout['agent_first_name'] or ''} {payout['agent_last_name'] or ''}".strip() or 'Unknown'
            agent_key = str(payout['agent_id'])
            if agent_key not in by_agent:
                by_agent[agent_key] = {'name': agent_name, 'premium': 0.0, 'payout': 0.0, 'count': 0, 'personal': 0.0, 'downline': 0.0}
            by_agent[agent_key]['payout'] += expected  # type: ignore[operator]
            by_agent[agent_key]['count'] += 1  # type: ignore[operator]
            if hierarchy_level == 0:
                by_agent[agent_key]['personal'] += expected  # type: ignore[operator]
            else:
                by_agent[agent_key]['downline'] += expected  # type: ignore[operator]

            # Build client name - prefer from clients table join, fallback to deal's denormalized client_name
            client_name = f"{payout['client_first_name'] or ''} {payout['client_last_name'] or ''}".strip()
            if not client_name:
                client_name = payout.get('client_name') or ''

            payouts.append({
                'deal_id': deal_id,
                'policy_number': payout['policy_number'],
                'client_name': client_name,
                'carrier_name': payout['carrier_name'],
                'product_name': payout['product_name'],
                'agent_id': str(payout['agent_id']) if payout['agent_id'] else None,
                'agent_name': f"{payout['agent_first_name'] or ''} {payout['agent_last_name'] or ''}".strip(),
                'position_name': payout['position_name'],
                'premium': premium,
                'agent_commission_percentage': float(payout['agent_commission_percentage']) if payout['agent_commission_percentage'] else 0,
                'hierarchy_total_percentage': float(payout['hierarchy_total_percentage']) if payout['hierarchy_total_percentage'] else 0,
                'hierarchy_level': hierarchy_level,
                'is_personal': hierarchy_level == 0,
                'expected_payout': expected,
                'policy_effective_date': payout['policy_effective_date'].isoformat() if payout['policy_effective_date'] else None,
                'status': payout['status'],
                'status_standardized': payout['status_standardized'],
            })

        return {
            'payouts': payouts,
            'total_expected': round(total_expected, 2),
            'total_premium': round(total_premium, 2),
            'deal_count': len(seen_deals),
            'payout_entries': len(payouts),
            'personal_production': round(personal_production, 2),
            'downline_production': round(downline_production, 2),
            'summary': {
                'by_carrier': [
                    {  # type: ignore[arg-type,call-overload]
                        'carrier': k,
                        'premium': round(float(v['premium']), 2),
                        'payout': round(float(v['payout']), 2),
                        'count': int(v['count'])
                    }
                    for k, v in sorted(by_carrier.items(), key=lambda x: -float(x[1]['payout']))
                ],
                'by_agent': [
                    {
                        'agent_id': k,
                        'name': str(v['name']),
                        'payout': round(float(v['payout']), 2),  # type: ignore[arg-type]
                        'count': int(v['count']),  # type: ignore[call-overload]
                        'personal': round(float(v['personal']), 2),  # type: ignore[arg-type]
                        'downline': round(float(v['downline']), 2)  # type: ignore[arg-type]
                    }
                    for k, v in sorted(by_agent.items(), key=lambda x: -float(x[1]['payout']))  # type: ignore[arg-type]
                ],
            },
        }

    except Exception as e:
        logger.error(f'Error getting expected payouts: {e}')
        raise


def get_agent_debt(
    user: AuthenticatedUser,
    agent_id: UUID | None = None,
) -> dict:
    """
    Get agent debt (negative balance from chargebacks, lapses, etc.).

    Uses deal_hierarchy_snapshot for proper commission calculation.

    Debt Formula (from RPC get_agent_debt):
    1. Calculate original commission = annual_premium * 0.75 * (agent_commission_pct / hierarchy_total_pct)
    2. Early lapse (<=30 days): full commission is debt
    3. Late lapse (>30 days): prorated based on 9-month vesting
       debt = (commission / 9) * max(0, 9 - months_active)

    Args:
        user: The authenticated user
        agent_id: Filter by specific agent (defaults to user)

    Returns:
        Dictionary with debt information including total_debt, deal_count, and breakdown
    """
    target_agent_id = agent_id or user.id

    # Verify access
    if str(target_agent_id) != str(user.id):
        is_admin = user.is_admin or user.role == 'admin'
        visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
        if target_agent_id not in visible_ids:
            return {'debt': 0, 'deal_count': 0, 'deals': []}

    try:
        query = """
            WITH lapsed_deals AS (
                -- Get all deals where this agent is in the hierarchy AND status has negative impact
                SELECT
                    d.id AS deal_id,
                    d.annual_premium,
                    d.policy_effective_date,
                    d.updated_at AS lapse_date,
                    d.status,
                    d.carrier_id,
                    d.client_name,
                    d.policy_number,
                    ca.name AS carrier_name,
                    dhs.commission_percentage AS agent_commission_pct,
                    (
                        SELECT SUM(dhs2.commission_percentage)
                        FROM deal_hierarchy_snapshot dhs2
                        WHERE dhs2.deal_id = d.id
                        AND dhs2.commission_percentage IS NOT NULL
                    ) AS hierarchy_total_pct
                FROM deals d
                INNER JOIN deal_hierarchy_snapshot dhs ON dhs.deal_id = d.id
                INNER JOIN status_mapping sm ON sm.carrier_id = d.carrier_id
                    AND LOWER(sm.raw_status) = LOWER(d.status)
                    AND sm.impact = 'negative'
                LEFT JOIN carriers ca ON ca.id = d.carrier_id
                WHERE dhs.agent_id = %s
                    AND d.annual_premium IS NOT NULL
                    AND d.policy_effective_date IS NOT NULL
                    AND dhs.commission_percentage IS NOT NULL
            ),
            debt_calculations AS (
                SELECT
                    deal_id,
                    annual_premium,
                    policy_effective_date,
                    lapse_date,
                    client_name,
                    policy_number,
                    carrier_name,
                    status,
                    agent_commission_pct,
                    hierarchy_total_pct,
                    -- Calculate original commission: annual_premium * 0.75 * (agent_pct / total_pct)
                    (annual_premium * 0.75 * (agent_commission_pct / NULLIF(hierarchy_total_pct, 0))) AS original_commission,
                    -- Calculate days active
                    GREATEST(0, EXTRACT(EPOCH FROM (lapse_date - policy_effective_date)) / 86400)::INTEGER AS days_active,
                    -- Calculate months active (floor of days / 30)
                    GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (lapse_date - policy_effective_date)) / 86400 / 30))::INTEGER AS months_active
                FROM lapsed_deals
                WHERE hierarchy_total_pct > 0
            ),
            final_debt AS (
                SELECT
                    dc.deal_id,
                    dc.client_name,
                    dc.policy_number,
                    dc.carrier_name,
                    dc.status,
                    dc.annual_premium,
                    dc.policy_effective_date,
                    dc.original_commission,
                    dc.days_active,
                    dc.months_active,
                    -- Determine if early lapse (within 30 days)
                    (dc.days_active <= 30) AS is_early_lapse,
                    CASE
                        -- Early lapse (within 30 days): full commission is debt
                        WHEN dc.days_active <= 30 THEN dc.original_commission
                        -- Late lapse (after 30 days): prorate based on 9 months
                        -- Cap months_active at 9 to prevent negative debt
                        ELSE (dc.original_commission / 9) * GREATEST(0, 9 - LEAST(dc.months_active, 9))
                    END AS debt_amount
                FROM debt_calculations dc
            )
            SELECT
                deal_id,
                client_name,
                policy_number,
                carrier_name,
                status,
                annual_premium,
                policy_effective_date,
                ROUND(original_commission::numeric, 2) AS original_commission,
                ROUND(debt_amount::numeric, 2) AS debt_amount,
                days_active,
                months_active,
                is_early_lapse
            FROM final_debt
            ORDER BY policy_effective_date DESC NULLS LAST
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [str(target_agent_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        total_debt = 0.0
        deals = []

        for row in rows:
            row_dict = dict(zip(columns, row, strict=False))
            debt_amt = float(row_dict['debt_amount']) if row_dict['debt_amount'] else 0.0
            total_debt += debt_amt

            deals.append({
                'deal_id': str(row_dict['deal_id']),
                'policy_number': row_dict['policy_number'],
                'client_name': row_dict['client_name'] or '',
                'carrier_name': row_dict['carrier_name'],
                'premium': float(row_dict['annual_premium'] or 0),
                'original_commission': float(row_dict['original_commission'] or 0),
                'debt_amount': debt_amt,
                'status': row_dict['status'],
                'policy_effective_date': row_dict['policy_effective_date'].isoformat() if row_dict['policy_effective_date'] else None,
                'days_active': row_dict['days_active'],
                'months_active': row_dict['months_active'],
                'is_early_lapse': row_dict['is_early_lapse'],
            })

        return {
            'debt': round(total_debt, 2),
            'deal_count': len(deals),
            'deals': deals,
        }

    except Exception as e:
        logger.error(f'Error getting agent debt: {e}')
        raise
