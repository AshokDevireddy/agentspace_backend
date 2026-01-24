"""
Payout Selectors (P2-029)

Complex queries for commission payout calculations.
"""
import logging
from datetime import date
from typing import Optional
from uuid import UUID

from django.db import connection

from apps.core.permissions import get_visible_agent_ids
from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)

# Simplified chargeback rate for debt calculation
# Real implementation would use actual chargeback amounts from carrier data
DEFAULT_CHARGEBACK_RATE = 0.1


def get_expected_payouts(
    user: AuthenticatedUser,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    agent_id: Optional[UUID] = None,
    carrier_id: Optional[UUID] = None,
    include_full_agency: bool = False,
    production_type: Optional[str] = None,  # 'personal', 'downline', or None for all
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
    if agent_id:
        visible_ids = [agent_id]
    else:
        visible_ids = get_visible_agent_ids(user, include_full_agency=include_full_agency and is_admin)

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
    hierarchy_level_filter = ""
    if production_type == 'personal':
        hierarchy_level_filter = "AND dhs.hierarchy_level = 0"
    elif production_type == 'downline':
        hierarchy_level_filter = "AND dhs.hierarchy_level > 0"

    # Add visible_ids_list to params for the first use in query
    params.append(visible_ids_list)

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
                d.product_id
            FROM public.deals d
            INNER JOIN public.deal_hierarchy_snapshots dhs ON dhs.deal_id = d.id
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
        filtered_deals AS (
            SELECT
                rd.*,
                COALESCE(ht.hierarchy_total_percentage, 0) as hierarchy_total_percentage
            FROM relevant_deals rd
            LEFT JOIN hierarchy_totals ht ON ht.deal_id = rd.deal_id
            LEFT JOIN public.status_mapping sm
                ON sm.carrier_id = rd.carrier_id
                AND LOWER(sm.raw_status) = LOWER(rd.status)
            WHERE (sm.impact IS NULL OR sm.impact IN ('positive', 'neutral'))
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
                dhs.hierarchy_level,
                dhs.commission_percentage as agent_commission_percentage,
                cl.first_name as client_first_name,
                cl.last_name as client_last_name,
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
            INNER JOIN public.deal_hierarchy_snapshots dhs ON dhs.deal_id = fd.deal_id
            LEFT JOIN public.clients cl ON cl.id = fd.client_id
            LEFT JOIN public.carriers ca ON ca.id = fd.carrier_id
            LEFT JOIN public.products pr ON pr.id = fd.product_id
            LEFT JOIN public.users u ON u.id = dhs.agent_id
            LEFT JOIN public.positions po ON po.id = dhs.position_id
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
    params.append(visible_ids_list)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        payouts = []
        total_expected = 0
        total_premium = 0
        seen_deals = set()  # Track unique deals for premium calculation
        by_carrier = {}
        by_agent = {}
        personal_production = 0
        downline_production = 0

        for row in rows:
            payout = dict(zip(columns, row))
            expected = float(payout['expected_payout']) if payout['expected_payout'] else 0
            premium = float(payout['annual_premium']) if payout['annual_premium'] else 0
            deal_id = str(payout['deal_id'])
            hierarchy_level = payout['hierarchy_level'] or 0

            total_expected += expected

            # Only count premium once per unique deal
            if deal_id not in seen_deals:
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
                by_carrier[carrier] = {'premium': 0, 'payout': 0, 'count': 0}
            if deal_id not in seen_deals:
                by_carrier[carrier]['premium'] += premium
            by_carrier[carrier]['payout'] += expected
            by_carrier[carrier]['count'] += 1

            # Aggregate by agent
            agent_name = f"{payout['agent_first_name'] or ''} {payout['agent_last_name'] or ''}".strip() or 'Unknown'
            agent_key = str(payout['agent_id'])
            if agent_key not in by_agent:
                by_agent[agent_key] = {'name': agent_name, 'premium': 0, 'payout': 0, 'count': 0, 'personal': 0, 'downline': 0}
            by_agent[agent_key]['payout'] += expected
            by_agent[agent_key]['count'] += 1
            if hierarchy_level == 0:
                by_agent[agent_key]['personal'] += expected
            else:
                by_agent[agent_key]['downline'] += expected

            payouts.append({
                'deal_id': deal_id,
                'policy_number': payout['policy_number'],
                'client_name': f"{payout['client_first_name'] or ''} {payout['client_last_name'] or ''}".strip(),
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
                    {'carrier': k, 'premium': round(v['premium'], 2), 'payout': round(v['payout'], 2), 'count': v['count']}
                    for k, v in sorted(by_carrier.items(), key=lambda x: -x[1]['payout'])
                ],
                'by_agent': [
                    {'agent_id': k, 'name': v['name'], 'payout': round(v['payout'], 2), 'count': v['count'],
                     'personal': round(v['personal'], 2), 'downline': round(v['downline'], 2)}
                    for k, v in sorted(by_agent.items(), key=lambda x: -x[1]['payout'])
                ],
            },
        }

    except Exception as e:
        logger.error(f'Error getting expected payouts: {e}')
        raise


def get_agent_debt(
    user: AuthenticatedUser,
    agent_id: Optional[UUID] = None,
) -> dict:
    """
    Get agent debt (negative balance from chargebacks, lapses, etc.).

    Uses Django ORM with select_related for optimized queries.

    Args:
        user: The authenticated user
        agent_id: Filter by specific agent (defaults to user)

    Returns:
        Dictionary with debt information
    """
    from apps.core.models import Deal

    target_agent_id = agent_id or user.id

    # Verify access
    if str(target_agent_id) != str(user.id):
        is_admin = user.is_admin or user.role == 'admin'
        visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
        if target_agent_id not in visible_ids:
            return {'debt': 0, 'deals': []}

    try:
        # Use Django ORM with select_related to prevent N+1 queries
        deals_qs = (
            Deal.objects
            .filter(
                agent_id=target_agent_id,
                agency_id=user.agency_id,
                status_standardized__in=['lapsed', 'cancelled', 'terminated'],
                annual_premium__isnull=False
            )
            .select_related('client', 'carrier')
            .order_by('-policy_effective_date')
        )

        total_debt = 0
        deals = []

        for deal in deals_qs:
            premium = float(deal.annual_premium) if deal.annual_premium else 0
            debt_amount = premium * DEFAULT_CHARGEBACK_RATE

            total_debt += debt_amount

            # Build client name
            client_name = ''
            if deal.client:
                client_name = f"{deal.client.first_name or ''} {deal.client.last_name or ''}".strip()

            deals.append({
                'deal_id': str(deal.id),
                'policy_number': deal.policy_number,
                'client_name': client_name,
                'carrier_name': deal.carrier.name if deal.carrier else None,
                'premium': premium,
                'debt_amount': round(debt_amount, 2),
                'status': deal.status,
                'status_standardized': deal.status_standardized,
                'policy_effective_date': deal.policy_effective_date.isoformat() if deal.policy_effective_date else None,
            })

        return {
            'debt': round(total_debt, 2),
            'deal_count': len(deals),
            'deals': deals,
        }

    except Exception as e:
        logger.error(f'Error getting agent debt: {e}')
        raise
