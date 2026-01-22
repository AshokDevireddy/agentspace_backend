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


def get_expected_payouts(
    user: AuthenticatedUser,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    agent_id: Optional[UUID] = None,
    carrier_id: Optional[UUID] = None,
    include_full_agency: bool = False,
) -> dict:
    """
    Calculate expected commission payouts based on deals and commission rates.

    Joins deals with position_product_commissions to calculate expected payouts
    based on agent positions and product commission rates.

    Args:
        user: The authenticated user
        start_date: Filter by policy effective date (from)
        end_date: Filter by policy effective date (to)
        agent_id: Filter by specific agent
        carrier_id: Filter by carrier
        include_full_agency: If True and user is admin, include all agency payouts

    Returns:
        Dictionary with payouts list and total_expected
    """
    is_admin = user.is_admin or user.role == 'admin'

    # Build visible agent filter
    if agent_id:
        visible_ids = [agent_id]
    else:
        visible_ids = get_visible_agent_ids(user, include_full_agency=include_full_agency and is_admin)

    if not visible_ids:
        return {'payouts': [], 'total_expected': 0, 'summary': {}}

    visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)

    # Build WHERE clauses
    params = [str(user.agency_id)]
    where_clauses = [
        "d.agency_id = %s",
        f"d.agent_id IN ({visible_ids_str})",
        "d.annual_premium IS NOT NULL",
        "d.annual_premium > 0",
    ]

    if start_date:
        where_clauses.append("d.policy_effective_date >= %s")
        params.append(start_date.isoformat())

    if end_date:
        where_clauses.append("d.policy_effective_date <= %s")
        params.append(end_date.isoformat())

    if carrier_id:
        where_clauses.append("d.carrier_id = %s")
        params.append(str(carrier_id))

    # Only include positive statuses (active, pending)
    where_clauses.append("(d.status_standardized IN ('active', 'pending') OR d.status_standardized IS NULL)")

    where_sql = " AND ".join(where_clauses)

    query = f"""
        WITH deal_payouts AS (
            SELECT
                d.id as deal_id,
                d.policy_number,
                d.annual_premium,
                d.policy_effective_date,
                d.status,
                d.status_standardized,
                cl.first_name as client_first_name,
                cl.last_name as client_last_name,
                ca.name as carrier_name,
                pr.name as product_name,
                u.id as agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                po.name as position_name,
                COALESCE(ppc.commission_percentage, 0) as commission_percentage,
                CASE
                    WHEN ppc.commission_percentage IS NOT NULL
                    THEN ROUND((d.annual_premium * ppc.commission_percentage / 100)::numeric, 2)
                    ELSE 0
                END as expected_payout
            FROM public.deals d
            LEFT JOIN public.clients cl ON cl.id = d.client_id
            LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
            LEFT JOIN public.products pr ON pr.id = d.product_id
            LEFT JOIN public.users u ON u.id = d.agent_id
            LEFT JOIN public.positions po ON po.id = u.position_id
            LEFT JOIN public.position_product_commissions ppc
                ON ppc.position_id = u.position_id AND ppc.product_id = d.product_id
            WHERE {where_sql}
        )
        SELECT
            deal_id,
            policy_number,
            annual_premium,
            policy_effective_date,
            status,
            status_standardized,
            client_first_name,
            client_last_name,
            carrier_name,
            product_name,
            agent_id,
            agent_first_name,
            agent_last_name,
            position_name,
            commission_percentage,
            expected_payout
        FROM deal_payouts
        ORDER BY policy_effective_date DESC NULLS LAST, deal_id DESC
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        payouts = []
        total_expected = 0
        total_premium = 0
        by_carrier = {}
        by_agent = {}

        for row in rows:
            payout = dict(zip(columns, row))
            expected = float(payout['expected_payout']) if payout['expected_payout'] else 0
            premium = float(payout['annual_premium']) if payout['annual_premium'] else 0

            total_expected += expected
            total_premium += premium

            # Aggregate by carrier
            carrier = payout['carrier_name'] or 'Unknown'
            if carrier not in by_carrier:
                by_carrier[carrier] = {'premium': 0, 'payout': 0, 'count': 0}
            by_carrier[carrier]['premium'] += premium
            by_carrier[carrier]['payout'] += expected
            by_carrier[carrier]['count'] += 1

            # Aggregate by agent
            agent_name = f"{payout['agent_first_name'] or ''} {payout['agent_last_name'] or ''}".strip() or 'Unknown'
            agent_key = str(payout['agent_id'])
            if agent_key not in by_agent:
                by_agent[agent_key] = {'name': agent_name, 'premium': 0, 'payout': 0, 'count': 0}
            by_agent[agent_key]['premium'] += premium
            by_agent[agent_key]['payout'] += expected
            by_agent[agent_key]['count'] += 1

            payouts.append({
                'deal_id': str(payout['deal_id']),
                'policy_number': payout['policy_number'],
                'client_name': f"{payout['client_first_name'] or ''} {payout['client_last_name'] or ''}".strip(),
                'carrier_name': payout['carrier_name'],
                'product_name': payout['product_name'],
                'agent_id': str(payout['agent_id']) if payout['agent_id'] else None,
                'agent_name': f"{payout['agent_first_name'] or ''} {payout['agent_last_name'] or ''}".strip(),
                'position_name': payout['position_name'],
                'premium': premium,
                'commission_percentage': float(payout['commission_percentage']) if payout['commission_percentage'] else 0,
                'expected_payout': expected,
                'policy_effective_date': payout['policy_effective_date'].isoformat() if payout['policy_effective_date'] else None,
                'status': payout['status'],
                'status_standardized': payout['status_standardized'],
            })

        return {
            'payouts': payouts,
            'total_expected': round(total_expected, 2),
            'total_premium': round(total_premium, 2),
            'deal_count': len(payouts),
            'summary': {
                'by_carrier': [
                    {'carrier': k, **v}
                    for k, v in sorted(by_carrier.items(), key=lambda x: -x[1]['payout'])
                ],
                'by_agent': [
                    {'agent_id': k, **v}
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

    Args:
        user: The authenticated user
        agent_id: Filter by specific agent (defaults to user)

    Returns:
        Dictionary with debt information
    """
    target_agent_id = agent_id or user.id

    # Verify access
    if str(target_agent_id) != str(user.id):
        is_admin = user.is_admin or user.role == 'admin'
        visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
        if target_agent_id not in visible_ids:
            return {'debt': 0, 'deals': []}

    query = """
        SELECT
            d.id as deal_id,
            d.policy_number,
            d.annual_premium,
            d.status,
            d.status_standardized,
            cl.first_name as client_first_name,
            cl.last_name as client_last_name,
            ca.name as carrier_name,
            d.policy_effective_date
        FROM public.deals d
        LEFT JOIN public.clients cl ON cl.id = d.client_id
        LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
        WHERE d.agent_id = %s
          AND d.agency_id = %s
          AND d.status_standardized IN ('lapsed', 'cancelled', 'terminated')
          AND d.annual_premium IS NOT NULL
        ORDER BY d.policy_effective_date DESC
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, [str(target_agent_id), str(user.agency_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        total_debt = 0
        deals = []

        for row in rows:
            deal = dict(zip(columns, row))
            # Simplified debt calculation - in reality would use actual chargeback amounts
            premium = float(deal['annual_premium']) if deal['annual_premium'] else 0
            debt_amount = premium * 0.1  # Assume 10% chargeback for simplicity

            total_debt += debt_amount
            deals.append({
                'deal_id': str(deal['deal_id']),
                'policy_number': deal['policy_number'],
                'client_name': f"{deal['client_first_name'] or ''} {deal['client_last_name'] or ''}".strip(),
                'carrier_name': deal['carrier_name'],
                'premium': premium,
                'debt_amount': round(debt_amount, 2),
                'status': deal['status'],
                'status_standardized': deal['status_standardized'],
                'policy_effective_date': deal['policy_effective_date'].isoformat() if deal['policy_effective_date'] else None,
            })

        return {
            'debt': round(total_debt, 2),
            'deal_count': len(deals),
            'deals': deals,
        }

    except Exception as e:
        logger.error(f'Error getting agent debt: {e}')
        raise
