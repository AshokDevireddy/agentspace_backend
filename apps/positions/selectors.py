"""
Position Selectors

Query functions for position data following the selector pattern.
Replaces Supabase RPC: get_positions_for_agency

Uses Django ORM with annotations and prefetch for efficient queries.
"""
from uuid import UUID

from django.db.models import Count, Q

from apps.core.models import Position, PositionProductCommission, User


def get_positions_for_agency(user_id: UUID, include_agent_count: bool = False) -> list[dict]:
    """
    Get all positions for a user's agency.
    Translated from Supabase RPC: get_positions_for_agency

    Uses Django ORM with optional annotation for agent count.

    Args:
        user_id: The user UUID (to determine agency)
        include_agent_count: If True, include agent_count (optional, not in RPC)

    Returns:
        List of position dictionaries matching RPC structure
        - position_id (not 'id') to match RPC
        - Ordered by level DESC (not ASC) to match RPC
    """
    # Get user's agency first
    user = User.objects.filter(id=user_id).values('agency_id').first()  # type: ignore[attr-defined]
    if not user or not user['agency_id']:
        return []

    agency_id = user['agency_id']

    # Query positions - order by level DESC to match RPC
    positions = (
        Position.objects  # type: ignore[attr-defined]
        .filter(agency_id=agency_id)
        .order_by('-level', 'name')  # Changed to DESC to match RPC
    )

    # Only annotate agent_count if requested (not in RPC, but kept for backward compatibility)
    if include_agent_count:
        positions = positions.annotate(
            agent_count=Count(
                'users',
                filter=Q(users__agency_id=agency_id)
            )
        )

    result = []
    for p in positions:
        item = {
            'position_id': p.id,  # Changed from 'id' to 'position_id' to match RPC
            'name': p.name,
            'level': p.level,
            'description': p.description,
            'is_active': p.is_active,
            'created_at': p.created_at,
            'updated_at': p.updated_at,
        }
        # Only include agent_count if requested
        if include_agent_count:
            item['agent_count'] = p.agent_count
        result.append(item)

    return result


def get_position_by_id(position_id: UUID, agency_id: UUID) -> dict | None:
    """
    Get a single position by ID (agency-scoped).

    Uses Django ORM with annotation for agent count.

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check

    Returns:
        Position dictionary or None if not found
    """
    position = (
        Position.objects  # type: ignore[attr-defined]
        .filter(id=position_id, agency_id=agency_id)
        .annotate(
            agent_count=Count(
                'users',
                filter=Q(users__agency_id=agency_id)
            )
        )
        .first()
    )

    if not position:
        return None

    return {
        'id': position.id,
        'name': position.name,
        'level': position.level,
        'description': position.description,
        'is_active': position.is_active,
        'created_at': position.created_at,
        'updated_at': position.updated_at,
        'agent_count': position.agent_count,
    }


def get_position_product_commissions(position_id: UUID, agency_id: UUID) -> list[dict]:
    """
    Get product commissions for a position.

    Uses Django ORM with select_related to prevent N+1 queries.

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check

    Returns:
        List of commission dictionaries with product info
    """
    commissions = (
        PositionProductCommission.objects  # type: ignore[attr-defined]
        .filter(
            position_id=position_id,
            position__agency_id=agency_id,
            product__agency_id=agency_id,
            product__is_active=True,
        )
        .select_related('product', 'product__carrier')
        .order_by('product__carrier__name', 'product__name')
    )

    return [
        {
            'id': c.id,
            'position_id': c.position_id,
            'product_id': c.product_id,
            'commission_percentage': c.commission_percentage,
            'product_name': c.product.name,
            'product_code': getattr(c.product, 'product_code', None),
            'carrier_name': c.product.carrier.name if c.product.carrier else None,
            'carrier_display_name': getattr(c.product.carrier, 'display_name', None) if c.product.carrier else None,
        }
        for c in commissions
    ]


def get_all_position_product_commissions(user_id: UUID, carrier_id: UUID | None = None) -> list[dict]:
    """
    Get all product commissions for all positions in user's agency.
    Translated from Supabase RPC: get_position_product_commissions

    Uses Django ORM with select_related to prevent N+1 queries.

    Args:
        user_id: The user UUID (to determine agency)
        carrier_id: Optional carrier UUID to filter by

    Returns:
        List of commission dictionaries with position and product info
    """
    # Get user's agency first
    user = User.objects.filter(id=user_id).values('agency_id').first()  # type: ignore[attr-defined]
    if not user or not user['agency_id']:
        return []

    agency_id = user['agency_id']

    # Build query with filters
    queryset = (
        PositionProductCommission.objects  # type: ignore[attr-defined]
        .filter(
            position__agency_id=agency_id,
        )
        .select_related('position', 'product', 'product__carrier')
    )

    # Apply carrier filter if provided
    if carrier_id:
        queryset = queryset.filter(product__carrier_id=carrier_id)

    # Order by position level desc, carrier, product name (matching RPC)
    queryset = queryset.order_by('-position__level', 'product__carrier_id', 'product__name')

    return [
        {
            'commission_id': c.id,
            'position_id': c.position_id,
            'position_name': c.position.name,
            'position_level': c.position.level,
            'product_id': c.product_id,
            'product_name': c.product.name,
            'carrier_id': c.product.carrier_id,
            'carrier_name': c.product.carrier.display_name if c.product.carrier else None,
            'commission_percentage': c.commission_percentage,
        }
        for c in queryset
    ]
