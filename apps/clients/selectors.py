"""
Client Selectors (P2-037)

Complex queries for client data retrieval.
"""
import logging
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser
from apps.core.permissions import get_visible_agent_ids

logger = logging.getLogger(__name__)


def get_clients_list(
    user: AuthenticatedUser,
    page: int = 1,
    limit: int = 20,
    search_query: str | None = None,
    agent_id: UUID | None = None,
    include_full_agency: bool = False,
) -> dict:
    """
    Get paginated list of clients.

    Args:
        user: The authenticated user
        page: Page number (1-based)
        limit: Page size
        search_query: Search by client name, email, or phone
        agent_id: Filter by specific agent's clients
        include_full_agency: If True and user is admin, include all agency clients

    Returns:
        Dictionary with clients and pagination
    """
    is_admin = user.is_admin or user.role == 'admin'
    offset = (page - 1) * limit

    # Build agent filter for deals (to get clients associated with visible agents)
    visible_ids = (
        [agent_id]
        if agent_id
        else get_visible_agent_ids(user, include_full_agency=include_full_agency and is_admin)
    )

    if not visible_ids:
        return {'clients': [], 'pagination': _empty_pagination(page, limit)}

    visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)

    # Build search filter
    search_filter = ""
    search_params = []
    if search_query:
        search_filter = """
            AND (c.first_name ILIKE %s
                 OR c.last_name ILIKE %s
                 OR c.email ILIKE %s
                 OR c.phone ILIKE %s
                 OR CONCAT(c.first_name, ' ', c.last_name) ILIKE %s)
        """
        search_pattern = f"%{search_query}%"
        search_params = [search_pattern] * 5

    # Count query - clients who have deals with visible agents
    count_query = f"""
        SELECT COUNT(DISTINCT c.id)
        FROM public.clients c
        JOIN public.deals d ON d.client_id = c.id
        WHERE c.agency_id = %s
          AND d.agent_id IN ({visible_ids_str})
          {search_filter}
    """

    # Main query with deal counts
    main_query = f"""
        SELECT
            c.id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.created_at,
            COUNT(DISTINCT d.id) as deal_count,
            SUM(CASE WHEN d.status_standardized = 'active' THEN 1 ELSE 0 END) as active_deals,
            SUM(COALESCE(d.annual_premium, 0)) as total_premium,
            MAX(d.policy_effective_date) as latest_policy_date
        FROM public.clients c
        JOIN public.deals d ON d.client_id = c.id
        WHERE c.agency_id = %s
          AND d.agent_id IN ({visible_ids_str})
          {search_filter}
        GROUP BY c.id, c.first_name, c.last_name, c.email, c.phone, c.created_at
        ORDER BY c.last_name, c.first_name
        LIMIT %s OFFSET %s
    """

    try:
        params = [str(user.agency_id)] + search_params

        with connection.cursor() as cursor:
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]

            cursor.execute(main_query, params + [limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        clients = []
        for row in rows:
            client = dict(zip(columns, row, strict=False))
            clients.append({
                'id': str(client['id']),
                'first_name': client['first_name'],
                'last_name': client['last_name'],
                'name': f"{client['first_name'] or ''} {client['last_name'] or ''}".strip(),
                'email': client['email'],
                'phone': client['phone'],
                'created_at': client['created_at'].isoformat() if client['created_at'] else None,
                'deal_count': client['deal_count'] or 0,
                'active_deals': client['active_deals'] or 0,
                'total_premium': float(client['total_premium']) if client['total_premium'] else 0,
                'latest_policy_date': (
                    client['latest_policy_date'].isoformat()
                    if client['latest_policy_date']
                    else None
                ),
            })

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            'clients': clients,
            'pagination': {
                'currentPage': page,
                'totalPages': total_pages,
                'totalCount': total_count,
                'limit': limit,
                'hasNextPage': page < total_pages,
                'hasPrevPage': page > 1,
            },
        }

    except Exception as e:
        logger.error(f'Error getting clients list: {e}')
        raise


def get_client_detail(
    user: AuthenticatedUser,
    client_id: UUID,
) -> dict | None:
    """
    Get detailed information about a client.

    Args:
        user: The authenticated user
        client_id: The client ID

    Returns:
        Client details or None if not found/accessible
    """
    is_admin = user.is_admin or user.role == 'admin'
    visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)

    if not visible_ids:
        return None

    visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)

    query = f"""
        SELECT
            c.id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.created_at,
            c.updated_at
        FROM public.clients c
        JOIN public.deals d ON d.client_id = c.id
        WHERE c.id = %s
          AND c.agency_id = %s
          AND d.agent_id IN ({visible_ids_str})
        LIMIT 1
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, [str(client_id), str(user.agency_id)])
            row = cursor.fetchone()

        if not row:
            return None

        columns = ['id', 'first_name', 'last_name', 'email', 'phone', 'created_at', 'updated_at']
        client = dict(zip(columns, row, strict=False))

        # Get client's deals
        deals_query = f"""
            SELECT
                d.id,
                d.policy_number,
                d.status,
                d.status_standardized,
                d.annual_premium,
                d.policy_effective_date,
                ca.name as carrier_name,
                pr.name as product_name,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name
            FROM public.deals d
            LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
            LEFT JOIN public.products pr ON pr.id = d.product_id
            LEFT JOIN public.users u ON u.id = d.agent_id
            WHERE d.client_id = %s
              AND d.agent_id IN ({visible_ids_str})
            ORDER BY d.policy_effective_date DESC
        """

        with connection.cursor() as cursor:
            cursor.execute(deals_query, [str(client_id)])
            deal_columns = [col[0] for col in cursor.description]
            deal_rows = cursor.fetchall()

        deals = []
        for row in deal_rows:
            deal = dict(zip(deal_columns, row, strict=False))
            deals.append({
                'id': str(deal['id']),
                'policy_number': deal['policy_number'],
                'status': deal['status'],
                'status_standardized': deal['status_standardized'],
                'annual_premium': float(deal['annual_premium']) if deal['annual_premium'] else None,
                'policy_effective_date': deal['policy_effective_date'].isoformat() if deal['policy_effective_date'] else None,
                'carrier_name': deal['carrier_name'],
                'product_name': deal['product_name'],
                'agent_name': f"{deal['agent_first_name'] or ''} {deal['agent_last_name'] or ''}".strip(),
            })

        return {
            'id': str(client['id']),
            'first_name': client['first_name'],
            'last_name': client['last_name'],
            'name': f"{client['first_name'] or ''} {client['last_name'] or ''}".strip(),
            'email': client['email'],
            'phone': client['phone'],
            'created_at': client['created_at'].isoformat() if client['created_at'] else None,
            'updated_at': client['updated_at'].isoformat() if client['updated_at'] else None,
            'deals': deals,
            'deal_count': len(deals),
            'active_deals': sum(1 for d in deals if d['status_standardized'] == 'active'),
            'total_premium': sum(d['annual_premium'] or 0 for d in deals),
        }

    except Exception as e:
        logger.error(f'Error getting client detail: {e}')
        raise


def _empty_pagination(page: int, limit: int) -> dict:
    """Return empty pagination structure."""
    return {
        'currentPage': page,
        'totalPages': 0,
        'totalCount': 0,
        'limit': limit,
        'hasNextPage': False,
        'hasPrevPage': False,
    }
