"""
Deal Selectors (P2-027, P2-028)

Complex queries for deal-related data retrieval.
"""
import logging
from datetime import date
from typing import Optional
from uuid import UUID

from django.db import connection

from apps.core.permissions import get_visible_agent_ids
from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


def get_book_of_business(
    user: AuthenticatedUser,
    limit: int = 50,
    cursor_policy_effective_date: Optional[date] = None,
    cursor_id: Optional[UUID] = None,
    carrier_id: Optional[UUID] = None,
    product_id: Optional[UUID] = None,
    agent_id: Optional[UUID] = None,
    client_id: Optional[UUID] = None,
    status: Optional[str] = None,
    status_standardized: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search_query: Optional[str] = None,
    policy_number: Optional[str] = None,
    billing_cycle: Optional[str] = None,
    lead_source: Optional[str] = None,
    view: Optional[str] = 'downlines',
    effective_date_sort: Optional[str] = None,
    include_full_agency: bool = False,
) -> dict:
    """
    Get paginated book of business (deals) with keyset pagination.

    Uses keyset pagination for performance on large datasets.
    Filters by user's visible agents based on hierarchy.

    Args:
        user: The authenticated user
        limit: Number of records to return
        cursor_policy_effective_date: Cursor for keyset pagination (date)
        cursor_id: Cursor for keyset pagination (id)
        carrier_id: Filter by carrier
        product_id: Filter by product
        agent_id: Filter by specific agent
        client_id: Filter by specific client (P2-027)
        status: Filter by raw status
        status_standardized: Filter by standardized status
        date_from: Filter by policy effective date (from)
        date_to: Filter by policy effective date (to)
        search_query: Search by client name or policy number
        policy_number: Filter by exact policy number (P2-027)
        billing_cycle: Filter by billing frequency (P2-027)
        lead_source: Filter by lead source (P2-027)
        view: Scope - 'self', 'downlines', 'all' (P2-027)
        effective_date_sort: Sort direction - 'oldest', 'newest' (P2-027)
        include_full_agency: If True and user is admin, include all agency deals

    Returns:
        Dictionary with deals, has_more, and next_cursor
    """
    is_admin = user.is_admin or user.role == 'admin'

    # Build visible agent filter based on view scope (P2-027)
    if agent_id:
        # Specific agent requested - verify access
        visible_ids = [agent_id]
    elif view == 'self':
        # Only user's own deals
        visible_ids = [user.id]
    elif view == 'all' and is_admin:
        # Admin viewing all agency deals
        visible_ids = get_visible_agent_ids(user, include_full_agency=True)
    else:
        # Default: user + downlines
        visible_ids = get_visible_agent_ids(user, include_full_agency=include_full_agency and is_admin)

    if not visible_ids:
        return {'deals': [], 'has_more': False, 'next_cursor': None}

    # Build query parameters
    params = [str(user.agency_id)]
    visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)

    # Build WHERE clauses
    where_clauses = [
        "d.agency_id = %s",
        f"d.agent_id IN ({visible_ids_str})",
    ]

    if carrier_id:
        where_clauses.append("d.carrier_id = %s")
        params.append(str(carrier_id))

    if product_id:
        where_clauses.append("d.product_id = %s")
        params.append(str(product_id))

    # New filter: client_id (P2-027)
    if client_id:
        where_clauses.append("d.client_id = %s")
        params.append(str(client_id))

    if status:
        where_clauses.append("d.status = %s")
        params.append(status)

    if status_standardized:
        where_clauses.append("d.status_standardized = %s")
        params.append(status_standardized)

    if date_from:
        where_clauses.append("d.policy_effective_date >= %s")
        params.append(date_from.isoformat())

    if date_to:
        where_clauses.append("d.policy_effective_date <= %s")
        params.append(date_to.isoformat())

    # New filter: policy_number exact match (P2-027)
    if policy_number:
        where_clauses.append("d.policy_number ILIKE %s")
        params.append(f"%{policy_number}%")

    # New filter: billing_cycle (P2-027)
    if billing_cycle:
        where_clauses.append("d.billing_cycle = %s")
        params.append(billing_cycle)

    # New filter: lead_source (P2-027)
    if lead_source:
        where_clauses.append("d.lead_source = %s")
        params.append(lead_source)

    if search_query:
        where_clauses.append("""
            (d.policy_number ILIKE %s
             OR cl.first_name ILIKE %s
             OR cl.last_name ILIKE %s
             OR CONCAT(cl.first_name, ' ', cl.last_name) ILIKE %s)
        """)
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])

    # Determine sort order (P2-027)
    if effective_date_sort == 'oldest':
        order_direction = 'ASC'
        cursor_comparison = '>'
    else:
        order_direction = 'DESC'
        cursor_comparison = '<'

    # Keyset pagination
    if cursor_policy_effective_date and cursor_id:
        where_clauses.append(f"""
            (d.policy_effective_date, d.id) {cursor_comparison} (%s, %s)
        """)
        params.extend([cursor_policy_effective_date.isoformat(), str(cursor_id)])

    where_sql = " AND ".join(where_clauses)

    # Fetch limit + 1 to determine if there are more records
    fetch_limit = limit + 1
    params.append(fetch_limit)

    query = f"""
        SELECT
            d.id,
            d.policy_number,
            d.status,
            d.status_standardized,
            d.annual_premium,
            d.monthly_premium,
            d.policy_effective_date,
            d.submission_date,
            d.billing_cycle,
            d.lead_source,
            d.created_at,
            d.updated_at,
            cl.id as client_id,
            cl.first_name as client_first_name,
            cl.last_name as client_last_name,
            cl.email as client_email,
            cl.phone as client_phone,
            ca.id as carrier_id,
            ca.name as carrier_name,
            p.id as product_id,
            p.name as product_name,
            u.id as agent_id,
            u.first_name as agent_first_name,
            u.last_name as agent_last_name,
            u.email as agent_email
        FROM public.deals d
        LEFT JOIN public.clients cl ON cl.id = d.client_id
        LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
        LEFT JOIN public.products p ON p.id = d.product_id
        LEFT JOIN public.users u ON u.id = d.agent_id
        WHERE {where_sql}
        ORDER BY d.policy_effective_date {order_direction} NULLS LAST, d.id {order_direction}
        LIMIT %s
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        # Check if there are more records
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        # Format results
        deals = []
        for row in rows:
            deal = dict(zip(columns, row))
            deals.append({
                'id': str(deal['id']),
                'policy_number': deal['policy_number'],
                'status': deal['status'],
                'status_standardized': deal['status_standardized'],
                'annual_premium': float(deal['annual_premium']) if deal['annual_premium'] else None,
                'monthly_premium': float(deal['monthly_premium']) if deal['monthly_premium'] else None,
                'policy_effective_date': deal['policy_effective_date'].isoformat() if deal['policy_effective_date'] else None,
                'submission_date': deal['submission_date'].isoformat() if deal['submission_date'] else None,
                'billing_cycle': deal.get('billing_cycle'),  # P2-027
                'lead_source': deal.get('lead_source'),  # P2-027
                'created_at': deal['created_at'].isoformat() if deal['created_at'] else None,
                'client': {
                    'id': str(deal['client_id']) if deal['client_id'] else None,
                    'first_name': deal['client_first_name'],
                    'last_name': deal['client_last_name'],
                    'email': deal['client_email'],
                    'phone': deal['client_phone'],
                    'name': f"{deal['client_first_name'] or ''} {deal['client_last_name'] or ''}".strip(),
                } if deal['client_id'] else None,
                'carrier': {
                    'id': str(deal['carrier_id']) if deal['carrier_id'] else None,
                    'name': deal['carrier_name'],
                } if deal['carrier_id'] else None,
                'product': {
                    'id': str(deal['product_id']) if deal['product_id'] else None,
                    'name': deal['product_name'],
                } if deal['product_id'] else None,
                'agent': {
                    'id': str(deal['agent_id']) if deal['agent_id'] else None,
                    'first_name': deal['agent_first_name'],
                    'last_name': deal['agent_last_name'],
                    'email': deal['agent_email'],
                    'name': f"{deal['agent_first_name'] or ''} {deal['agent_last_name'] or ''}".strip(),
                } if deal['agent_id'] else None,
            })

        # Build next cursor
        next_cursor = None
        if has_more and deals:
            last_deal = deals[-1]
            next_cursor = {
                'policy_effective_date': last_deal['policy_effective_date'],
                'id': last_deal['id'],
            }

        return {
            'deals': deals,
            'has_more': has_more,
            'next_cursor': next_cursor,
        }

    except Exception as e:
        logger.error(f'Error getting book of business: {e}')
        raise


def get_static_filter_options(user: AuthenticatedUser) -> dict:
    """
    Get static filter options for deals (P2-028).

    Returns carriers, products, statuses, and agents available for filtering.

    Args:
        user: The authenticated user

    Returns:
        Dictionary with filter options
    """
    is_admin = user.is_admin or user.role == 'admin'

    try:
        with connection.cursor() as cursor:
            # Get carriers for agency
            cursor.execute("""
                SELECT DISTINCT c.id, c.name
                FROM public.carriers c
                JOIN public.deals d ON d.carrier_id = c.id
                WHERE d.agency_id = %s
                ORDER BY c.name
            """, [str(user.agency_id)])
            carriers = [{'id': str(row[0]), 'name': row[1]} for row in cursor.fetchall()]

            # Get products for agency
            cursor.execute("""
                SELECT DISTINCT p.id, p.name, c.name as carrier_name
                FROM public.products p
                JOIN public.carriers c ON c.id = p.carrier_id
                JOIN public.deals d ON d.product_id = p.id
                WHERE d.agency_id = %s
                ORDER BY p.name
            """, [str(user.agency_id)])
            products = [
                {'id': str(row[0]), 'name': row[1], 'carrier_name': row[2]}
                for row in cursor.fetchall()
            ]

            # Get distinct statuses
            cursor.execute("""
                SELECT DISTINCT status
                FROM public.deals
                WHERE agency_id = %s AND status IS NOT NULL
                ORDER BY status
            """, [str(user.agency_id)])
            statuses = [row[0] for row in cursor.fetchall()]

            # Get distinct standardized statuses
            cursor.execute("""
                SELECT DISTINCT status_standardized
                FROM public.deals
                WHERE agency_id = %s AND status_standardized IS NOT NULL
                ORDER BY status_standardized
            """, [str(user.agency_id)])
            statuses_standardized = [row[0] for row in cursor.fetchall()]

            # Get agents (visible based on hierarchy)
            visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
            if visible_ids:
                visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)
                cursor.execute(f"""
                    SELECT DISTINCT u.id, u.first_name, u.last_name, u.email
                    FROM public.users u
                    JOIN public.deals d ON d.agent_id = u.id
                    WHERE d.agency_id = %s AND u.id IN ({visible_ids_str})
                    ORDER BY u.first_name, u.last_name
                """, [str(user.agency_id)])
                agents = [
                    {
                        'id': str(row[0]),
                        'name': f"{row[1] or ''} {row[2] or ''}".strip() or row[3],
                        'email': row[3],
                    }
                    for row in cursor.fetchall()
                ]
            else:
                agents = []

            return {
                'carriers': carriers,
                'products': products,
                'statuses': statuses,
                'statuses_standardized': statuses_standardized,
                'agents': agents,
            }

    except Exception as e:
        logger.error(f'Error getting filter options: {e}')
        raise
