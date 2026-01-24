"""
Position Selectors

Query functions for position data following the selector pattern.
Replaces Supabase RPC: get_positions_for_agency

Uses Django ORM with annotations and prefetch for efficient queries.
"""
from typing import List, Optional
from uuid import UUID

from django.db.models import Count, Q

from apps.core.models import Position, PositionProductCommission, User


def get_positions_for_agency(user_id: UUID) -> List[dict]:
    """
    Get all positions for a user's agency.
    Translated from Supabase RPC: get_positions_for_agency

    Uses Django ORM with annotation for agent count.

    Args:
        user_id: The user UUID (to determine agency)

    Returns:
        List of position dictionaries with counts
    """
    # Get user's agency first
    user = User.objects.filter(id=user_id).values('agency_id').first()
    if not user or not user['agency_id']:
        return []

    agency_id = user['agency_id']

    # Query positions with agent count annotation
    positions = (
        Position.objects
        .filter(agency_id=agency_id)
        .annotate(
            agent_count=Count(
                'users',
                filter=Q(users__agency_id=agency_id)
            )
        )
        .order_by('level', 'name')
    )

    return [
        {
            'id': p.id,
            'name': p.name,
            'level': p.level,
            'description': p.description,
            'is_active': p.is_active,
            'created_at': p.created_at,
            'updated_at': p.updated_at,
            'agent_count': p.agent_count,
        }
        for p in positions
    ]


def get_position_by_id(position_id: UUID, agency_id: UUID) -> Optional[dict]:
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
        Position.objects
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


def get_position_product_commissions(position_id: UUID, agency_id: UUID) -> List[dict]:
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
        PositionProductCommission.objects
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
