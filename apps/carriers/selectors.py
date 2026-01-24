"""
Carrier Selectors

Query functions for carrier data following the selector pattern.

Uses Django ORM with select_related and prefetch_related for efficient queries.
"""
from typing import List, Optional
from uuid import UUID

from django.db.models import Prefetch

from apps.core.models import Carrier, Product, StatusMapping


def get_active_carriers() -> List[dict]:
    """
    Get all active carriers ordered by display_name.

    Uses Django ORM.

    Returns:
        List of carrier dictionaries with id, name, display_name, is_active, created_at
    """
    carriers = (
        Carrier.objects
        .filter(is_active=True)
        .order_by('name')  # Fallback since display_name may not exist
    )

    return [
        {
            'id': c.id,
            'name': c.name,
            'display_name': getattr(c, 'display_name', None),
            'is_active': c.is_active,
            'created_at': c.created_at,
        }
        for c in carriers
    ]


def get_carriers_with_products_for_agency(agency_id: UUID) -> List[dict]:
    """
    Get carriers that have products associated with the given agency.

    Uses Django ORM with Prefetch to prevent N+1 queries.

    Args:
        agency_id: The agency UUID

    Returns:
        List of carrier dictionaries with nested products
    """
    # Prefetch active products for this agency
    products_prefetch = Prefetch(
        'products',
        queryset=Product.objects.filter(
            agency_id=agency_id,
            is_active=True
        ).order_by('name'),
        to_attr='agency_products'
    )

    # Get carriers with active products for this agency
    carriers = (
        Carrier.objects
        .filter(
            is_active=True,
            products__agency_id=agency_id,
            products__is_active=True,
        )
        .distinct()
        .prefetch_related(products_prefetch)
        .order_by('name')
    )

    return [
        {
            'id': c.id,
            'name': c.name,
            'display_name': getattr(c, 'display_name', None),
            'is_active': c.is_active,
            'products': [
                {
                    'id': p.id,
                    'name': p.name,
                    'product_code': getattr(p, 'product_code', None),
                    'is_active': p.is_active,
                    'created_at': p.created_at,
                }
                for p in c.agency_products
            ],
        }
        for c in carriers
    ]


def get_carrier_names() -> List[dict]:
    """
    Get carrier names for dropdowns (lightweight query).

    Uses Django ORM with only() to minimize data transfer.

    Returns:
        List of carrier dictionaries with id and name only
    """
    carriers = (
        Carrier.objects
        .filter(is_active=True)
        .only('id', 'name')
        .order_by('name')
    )

    return [
        {
            'id': c.id,
            'name': getattr(c, 'display_name', None) or c.name,
        }
        for c in carriers
    ]


def get_carriers_for_agency(agency_id: UUID) -> List[dict]:
    """
    Get carriers associated with an agency (P2-030).

    Returns carriers that have products or deals associated with the agency.
    This is a lightweight list for agency-specific carrier selection.

    Args:
        agency_id: The agency UUID

    Returns:
        List of carrier dictionaries with id, name, display_name, is_active
    """
    # Get carriers that have products for this agency
    carriers = (
        Carrier.objects
        .filter(
            is_active=True,
            products__agency_id=agency_id,
        )
        .distinct()
        .order_by('name')
    )

    return [
        {
            'id': str(c.id),
            'name': c.name,
            'display_name': c.display_name,
            'is_active': c.is_active,
        }
        for c in carriers
    ]


def get_carrier_by_id(carrier_id: UUID) -> Optional[dict]:
    """
    Get a single carrier by ID.

    Uses Django ORM.

    Args:
        carrier_id: The carrier UUID

    Returns:
        Carrier dictionary or None if not found
    """
    carrier = Carrier.objects.filter(id=carrier_id).first()

    if not carrier:
        return None

    return {
        'id': carrier.id,
        'name': carrier.name,
        'display_name': getattr(carrier, 'display_name', None),
        'code': getattr(carrier, 'code', None),
        'is_active': carrier.is_active,
        'created_at': carrier.created_at,
        'updated_at': carrier.updated_at,
    }


# =============================================================================
# Status Mappings (P1-020)
# =============================================================================

def get_status_mappings(carrier_id: Optional[UUID] = None) -> List[dict]:
    """
    Get status mappings (P1-020).

    Returns carrier-specific status codes mapped to standardized statuses.

    Args:
        carrier_id: Optional carrier UUID to filter by

    Returns:
        List of status mapping dictionaries
    """
    qs = StatusMapping.objects.select_related('carrier').order_by('carrier__name', 'raw_status')

    if carrier_id:
        qs = qs.filter(carrier_id=carrier_id)

    return [
        {
            'id': str(sm.id),
            'carrier_id': str(sm.carrier_id),
            'carrier_name': sm.carrier.name if sm.carrier else None,
            'raw_status': sm.raw_status,
            'standardized_status': sm.standardized_status,
            'impact': sm.impact,
            'created_at': sm.created_at.isoformat() if sm.created_at else None,
            'updated_at': sm.updated_at.isoformat() if sm.updated_at else None,
        }
        for sm in qs
    ]


def get_standardized_statuses() -> List[dict]:
    """
    Get list of standardized status values (P1-020).

    Returns static list of valid standardized statuses with their metadata.
    """
    from apps.core.constants import STANDARDIZED_STATUSES
    return STANDARDIZED_STATUSES
