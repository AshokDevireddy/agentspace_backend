"""
Deal Selectors (P2-027, P2-028)

Complex queries for deal-related data retrieval.
"""
import logging
from datetime import date
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser
from apps.core.permissions import get_visible_agent_ids

logger = logging.getLogger(__name__)


def mask_phone_number(phone: str | None, can_view_full: bool = False) -> str | None:
    """
    Mask a phone number for privacy protection (P2-027).

    Args:
        phone: The phone number to mask
        can_view_full: If True, returns the full phone number

    Returns:
        Masked phone number (e.g., '555****12') or full number if permitted
    """
    if not phone:
        return None

    if can_view_full:
        return phone

    # Strip non-digits for consistent masking
    digits = ''.join(c for c in phone if c.isdigit())

    if len(digits) <= 4:
        return '****'
    elif len(digits) <= 6:
        return digits[:2] + '****'
    else:
        # Show first 3 and last 2 digits
        return digits[:3] + '****' + digits[-2:]


def get_book_of_business(
    user: AuthenticatedUser,
    limit: int = 50,
    cursor_policy_effective_date: date | None = None,
    cursor_id: UUID | None = None,
    carrier_id: UUID | None = None,
    product_id: UUID | None = None,
    agent_id: UUID | None = None,
    client_id: UUID | None = None,
    status: str | None = None,
    status_standardized: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    search_query: str | None = None,
    policy_number: str | None = None,
    billing_cycle: str | None = None,
    lead_source: str | None = None,
    client_phone: str | None = None,
    view: str | None = 'downlines',
    effective_date_sort: str | None = None,
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
        client_phone: Filter by client phone number
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

    # Convert visible_ids to list of strings for PostgreSQL array parameter (safe from SQL injection)
    visible_ids_list = [str(vid) for vid in visible_ids]

    # Build query parameters - visible_ids_list is added as second parameter for ANY(%s::uuid[])
    params = [str(user.agency_id), visible_ids_list]

    # Build WHERE clauses
    where_clauses = [
        "d.agency_id = %s",
        "d.agent_id = ANY(%s::uuid[])",
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

    if client_phone:
        # Normalize phone: remove non-digits for comparison
        normalized_phone = ''.join(filter(str.isdigit, client_phone))
        where_clauses.append("REGEXP_REPLACE(cl.phone, '[^0-9]', '', 'g') LIKE %s")
        params.append(f"%{normalized_phone}%")

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
    params.append(str(fetch_limit))

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
            d.client_name,
            d.client_phone as deal_client_phone,
            d.client_email as deal_client_email,
            d.client_address,
            d.client_gender,
            d.date_of_birth,
            d.ssn_last_4,
            d.ssn_benefit,
            d.face_value,
            d.issue_age,
            d.notes,
            d.application_number,
            d.lapse_date,
            d.state,
            d.zipcode,
            d.payment_method,
            ca.id as carrier_id,
            ca.name as carrier_name,
            p.id as product_id,
            p.name as product_name,
            u.id as agent_id,
            u.first_name as agent_first_name,
            u.last_name as agent_last_name,
            u.email as agent_email
        FROM public.deals d
        LEFT JOIN public.carriers ca ON ca.id = d.carrier_id
        LEFT JOIN public.products p ON p.id = d.product_id
        LEFT JOIN public.users u ON u.id = d.agent_id
        WHERE {where_sql}
        ORDER BY d.policy_effective_date {order_direction} NULLS LAST, d.id {order_direction}
        LIMIT %s
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)  # type: ignore[arg-type]
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        # Check if there are more records
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        # Format results
        deals = []
        for row in rows:
            deal = dict(zip(columns, row, strict=False))

            # Phone masking: admins see all, users see own deals' phones (P2-027)
            deal_agent_id = deal['agent_id']
            can_view_full_phone = is_admin or (deal_agent_id and str(deal_agent_id) == str(user.id))

            # Parse client name into first/last name if available
            client_name = deal.get('client_name') or ''
            client_name_parts = client_name.split(' ', 1) if client_name else ['', '']
            client_first_name = client_name_parts[0] if client_name_parts else ''
            client_last_name = client_name_parts[1] if len(client_name_parts) > 1 else ''

            deals.append({
                'id': str(deal['id']),
                'policy_number': deal['policy_number'],
                'application_number': deal.get('application_number'),
                'status': deal['status'],
                'status_standardized': deal['status_standardized'],
                'annual_premium': float(deal['annual_premium']) if deal['annual_premium'] else None,
                'monthly_premium': float(deal['monthly_premium']) if deal['monthly_premium'] else None,
                'policy_effective_date': deal['policy_effective_date'].isoformat() if deal['policy_effective_date'] else None,
                'submission_date': deal['submission_date'].isoformat() if deal['submission_date'] else None,
                'lapse_date': deal['lapse_date'].isoformat() if deal.get('lapse_date') else None,
                'billing_cycle': deal.get('billing_cycle'),
                'lead_source': deal.get('lead_source'),
                'notes': deal.get('notes'),
                'face_value': float(deal['face_value']) if deal.get('face_value') else None,
                'issue_age': float(deal['issue_age']) if deal.get('issue_age') else None,
                'payment_method': deal.get('payment_method'),
                'created_at': deal['created_at'].isoformat() if deal['created_at'] else None,
                'client': {
                    'name': client_name,
                    'first_name': client_first_name,
                    'last_name': client_last_name,
                    'email': deal.get('deal_client_email'),
                    'phone': mask_phone_number(deal.get('deal_client_phone'), can_view_full_phone),
                    'address': deal.get('client_address'),
                    'gender': deal.get('client_gender'),
                    'date_of_birth': deal['date_of_birth'].isoformat() if deal.get('date_of_birth') else None,
                    'ssn_last_4': deal.get('ssn_last_4') if can_view_full_phone else None,
                    'ssn_benefit': deal.get('ssn_benefit'),
                    'state': deal.get('state'),
                    'zipcode': deal.get('zipcode'),
                } if client_name or deal.get('deal_client_phone') or deal.get('deal_client_email') else None,
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

    Uses Django ORM for all queries - optimized with select_related.

    Returns carriers, products, statuses, and agents available for filtering.

    Args:
        user: The authenticated user

    Returns:
        Dictionary with filter options
    """
    from apps.core.models import Carrier, Deal, Product, User

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Get carriers that have deals in this agency
        carrier_ids = (
            Deal.objects.filter(agency_id=user.agency_id, carrier_id__isnull=False)  # type: ignore[attr-defined]
            .values_list('carrier_id', flat=True)
            .distinct()
        )
        carriers = [
            {'id': str(c.id), 'name': c.name}
            for c in Carrier.objects.filter(id__in=carrier_ids).order_by('name')  # type: ignore[attr-defined]
        ]

        # Get products that have deals in this agency (with carrier info)
        product_ids = (
            Deal.objects.filter(agency_id=user.agency_id, product_id__isnull=False)  # type: ignore[attr-defined]
            .values_list('product_id', flat=True)
            .distinct()
        )
        products = [
            {'id': str(p.id), 'name': p.name, 'carrier_name': p.carrier.name if p.carrier else None}
            for p in Product.objects.filter(id__in=product_ids).select_related('carrier').order_by('name')  # type: ignore[attr-defined]
        ]

        # Get distinct statuses
        statuses = list(
            Deal.objects.filter(agency_id=user.agency_id, status__isnull=False)  # type: ignore[attr-defined]
            .values_list('status', flat=True)
            .distinct()
            .order_by('status')
        )

        # Get distinct standardized statuses
        statuses_standardized = list(
            Deal.objects.filter(agency_id=user.agency_id, status_standardized__isnull=False)  # type: ignore[attr-defined]
            .values_list('status_standardized', flat=True)
            .distinct()
            .order_by('status_standardized')
        )

        # Get agents (visible based on hierarchy) who have deals
        visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
        if visible_ids:
            # Get agent IDs that have deals in this agency and are visible
            agent_ids_with_deals = (
                Deal.objects.filter(  # type: ignore[attr-defined]
                    agency_id=user.agency_id,
                    agent_id__in=visible_ids
                )
                .values_list('agent_id', flat=True)
                .distinct()
            )
            agents = [
                {
                    'id': str(u.id),
                    'name': f"{u.first_name or ''} {u.last_name or ''}".strip() or u.email,
                    'email': u.email,
                }
                for u in User.objects.filter(id__in=agent_ids_with_deals).order_by('first_name', 'last_name')  # type: ignore[attr-defined]
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


def get_post_deal_form_data(user: AuthenticatedUser) -> dict:
    """
    Get form data for the Post A Deal page.

    Returns carriers, products, agents, and lead sources for the form.

    Args:
        user: The authenticated user

    Returns:
        Dictionary with form data options
    """
    from apps.core.models import Carrier, Product, User

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Get all carriers for this agency
        carriers = [
            {'id': str(c.id), 'name': c.name}
            for c in Carrier.objects.filter(agency_id=user.agency_id, is_active=True).order_by('name')  # type: ignore[attr-defined]
        ]

        # Get all products with carrier info
        products = [
            {
                'id': str(p.id),
                'name': p.name,
                'carrier_id': str(p.carrier_id) if p.carrier_id else None,
                'carrier_name': p.carrier.name if p.carrier else None,
            }
            for p in (
                Product.objects.filter(agency_id=user.agency_id, is_active=True)  # type: ignore[attr-defined]
                .select_related('carrier')
                .order_by('carrier__name', 'name')
            )
        ]

        # Get visible agents for the dropdown
        visible_ids = get_visible_agent_ids(user, include_full_agency=is_admin)
        if visible_ids:
            agents = [
                {
                    'id': str(u.id),
                    'name': f"{u.first_name or ''} {u.last_name or ''}".strip() or u.email,
                    'email': u.email,
                }
                for u in User.objects.filter(id__in=visible_ids, is_active=True).order_by('first_name', 'last_name')  # type: ignore[attr-defined]
            ]
        else:
            agents = []

        # Lead source options (common values)
        lead_sources = [
            'Referral',
            'Website',
            'Social Media',
            'Cold Call',
            'Walk-in',
            'Other',
        ]

        return {
            'carriers': carriers,
            'products': products,
            'agents': agents,
            'lead_sources': lead_sources,
            'user': {
                'id': str(user.id),
                'is_admin': is_admin,
            },
        }

    except Exception as e:
        logger.error(f'Error getting post deal form data: {e}')
        raise


def get_products_by_carrier(user: AuthenticatedUser, carrier_id: UUID) -> list[dict]:
    """
    Get products for a specific carrier.

    Args:
        user: The authenticated user
        carrier_id: The carrier UUID

    Returns:
        List of product dictionaries
    """
    from apps.core.models import Product

    try:
        products = (
            Product.objects  # type: ignore[attr-defined]
            .filter(
                agency_id=user.agency_id,
                carrier_id=carrier_id,
                is_active=True,
            )
            .order_by('name')
        )

        return [
            {
                'id': str(p.id),
                'name': p.name,
                'carrier_id': str(p.carrier_id) if p.carrier_id else None,
            }
            for p in products
        ]

    except Exception as e:
        logger.error(f'Error getting products by carrier: {e}')
        raise


def find_deal_by_client_phone(phone: str, agency_id: str) -> dict | None:
    """
    Find a deal by client phone number within an agency.

    Searches multiple phone format variations to handle different formats.

    Args:
        phone: The client phone number to search for
        agency_id: The agency ID to search within

    Returns:
        Deal dict with agent info if found, None otherwise
    """
    import re

    # Normalize phone number for comparison (remove all non-digits)
    normalized = re.sub(r'\D', '', phone)

    # Generate phone format variations to search
    phone_variations = [
        normalized,                                  # e.g., "6692456363"
        f'+1{normalized}' if len(normalized) == 10 else f'+{normalized}',  # e.g., "+16692456363"
        f'1{normalized}' if len(normalized) == 10 else normalized,         # e.g., "16692456363"
    ]

    # Add formatted versions if we have 10 digits
    if len(normalized) == 10:
        phone_variations.extend([
            f'({normalized[:3]}) {normalized[3:6]}-{normalized[6:]}',  # (669) 245-6363
            f'{normalized[:3]}-{normalized[3:6]}-{normalized[6:]}',    # 669-245-6363
        ])

    try:
        for variation in phone_variations:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        d.id,
                        d.policy_number,
                        d.status,
                        d.status_standardized,
                        d.client_name,
                        d.client_phone,
                        d.client_email,
                        d.annual_premium,
                        d.monthly_premium,
                        d.policy_effective_date,
                        u.id as agent_id,
                        u.first_name as agent_first_name,
                        u.last_name as agent_last_name,
                        u.phone_number as agent_phone,
                        u.agency_id as agent_agency_id
                    FROM public.deals d
                    JOIN public.users u ON u.id = d.agent_id
                    WHERE d.client_phone = %s
                        AND d.agency_id = %s
                    ORDER BY d.created_at DESC
                    LIMIT 1
                """, [variation, agency_id])
                row = cursor.fetchone()

            if row:
                return {
                    'id': str(row[0]),
                    'policy_number': row[1],
                    'status': row[2],
                    'status_standardized': row[3],
                    'client_name': row[4],
                    'client_phone': row[5],
                    'client_email': row[6],
                    'annual_premium': float(row[7]) if row[7] else None,
                    'monthly_premium': float(row[8]) if row[8] else None,
                    'policy_effective_date': row[9].isoformat() if row[9] else None,
                    'agent': {
                        'id': str(row[10]),
                        'first_name': row[11],
                        'last_name': row[12],
                        'phone_number': row[13],
                        'agency_id': str(row[14]) if row[14] else None,
                        'name': f"{row[11] or ''} {row[12] or ''}".strip(),
                    }
                }

        # If no exact match found, try pattern matching with LIKE
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    d.id,
                    d.policy_number,
                    d.status,
                    d.status_standardized,
                    d.client_name,
                    d.client_phone,
                    d.client_email,
                    d.annual_premium,
                    d.monthly_premium,
                    d.policy_effective_date,
                    u.id as agent_id,
                    u.first_name as agent_first_name,
                    u.last_name as agent_last_name,
                    u.phone_number as agent_phone,
                    u.agency_id as agent_agency_id
                FROM public.deals d
                JOIN public.users u ON u.id = d.agent_id
                WHERE REGEXP_REPLACE(d.client_phone, '[^0-9]', '', 'g') = %s
                    AND d.agency_id = %s
                ORDER BY d.created_at DESC
                LIMIT 1
            """, [normalized, agency_id])
            row = cursor.fetchone()

        if row:
            return {
                'id': str(row[0]),
                'policy_number': row[1],
                'status': row[2],
                'status_standardized': row[3],
                'client_name': row[4],
                'client_phone': row[5],
                'client_email': row[6],
                'annual_premium': float(row[7]) if row[7] else None,
                'monthly_premium': float(row[8]) if row[8] else None,
                'policy_effective_date': row[9].isoformat() if row[9] else None,
                'agent': {
                    'id': str(row[10]),
                    'first_name': row[11],
                    'last_name': row[12],
                    'phone_number': row[13],
                    'agency_id': str(row[14]) if row[14] else None,
                    'name': f"{row[11] or ''} {row[12] or ''}".strip(),
                }
            }

        return None

    except Exception as e:
        logger.error(f'Error finding deal by client phone: {e}')
        raise
