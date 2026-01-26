"""
Product Selectors

Query functions for product data following the selector pattern.

Uses Django ORM with select_related for efficient queries.
"""
from uuid import UUID

from apps.core.models import Product


def get_products_for_carrier(carrier_id: UUID, agency_id: UUID) -> list[dict]:
    """
    Get all active products for a specific carrier and agency.

    Uses Django ORM.

    Args:
        carrier_id: The carrier UUID
        agency_id: The agency UUID

    Returns:
        List of product dictionaries
    """
    products = (
        Product.objects  # type: ignore[attr-defined]
        .filter(
            carrier_id=carrier_id,
            agency_id=agency_id,
            is_active=True,
        )
        .order_by('name')
    )

    return [
        {
            'id': p.id,
            'carrier_id': p.carrier_id,
            'name': p.name,
            'product_code': getattr(p, 'product_code', None),
            'is_active': p.is_active,
            'created_at': p.created_at,
        }
        for p in products
    ]


def get_all_products_for_agency(agency_id: UUID) -> list[dict]:
    """
    Get all active products for an agency with carrier information.

    Uses Django ORM with select_related to prevent N+1 queries.

    Args:
        agency_id: The agency UUID

    Returns:
        List of product dictionaries with carrier name
    """
    products = (
        Product.objects  # type: ignore[attr-defined]
        .filter(
            agency_id=agency_id,
            is_active=True,
        )
        .select_related('carrier')
        .order_by('carrier__name', 'name')
    )

    return [
        {
            'id': p.id,
            'carrier_id': p.carrier_id,
            'name': p.name,
            'product_code': getattr(p, 'product_code', None),
            'is_active': p.is_active,
            'created_at': p.created_at,
            'carrier_name': p.carrier.name if p.carrier else None,
            'carrier_display_name': getattr(p.carrier, 'display_name', None) if p.carrier else None,
        }
        for p in products
    ]


def get_product_by_id(product_id: UUID, agency_id: UUID) -> dict | None:
    """
    Get a single product by ID (agency-scoped).

    Uses Django ORM with select_related.

    Args:
        product_id: The product UUID
        agency_id: The agency UUID for security check

    Returns:
        Product dictionary or None if not found
    """
    product = (
        Product.objects  # type: ignore[attr-defined]
        .filter(id=product_id, agency_id=agency_id)
        .select_related('carrier')
        .first()
    )

    if not product:
        return None

    return {
        'id': product.id,
        'carrier_id': product.carrier_id,
        'name': product.name,
        'product_code': getattr(product, 'product_code', None),
        'is_active': product.is_active,
        'created_at': product.created_at,
        'carrier_name': product.carrier.name if product.carrier else None,
        'carrier_display_name': getattr(product.carrier, 'display_name', None) if product.carrier else None,
    }


def get_products_for_dropdown(agency_id: UUID, carrier_id: UUID | None = None) -> list[dict]:
    """
    Get products for dropdown selection (lightweight query).

    Uses Django ORM with select_related.

    Args:
        agency_id: The agency UUID
        carrier_id: Optional carrier UUID to filter by

    Returns:
        List of product dictionaries with id, name, and carrier info
    """
    queryset = (
        Product.objects  # type: ignore[attr-defined]
        .filter(agency_id=agency_id, is_active=True)
        .select_related('carrier')
    )

    if carrier_id:
        queryset = queryset.filter(carrier_id=carrier_id)

    products = queryset.order_by('carrier__name', 'name')

    return [
        {
            'id': p.id,
            'name': p.name,
            'carrier_name': p.carrier.name if p.carrier else None,
        }
        for p in products
    ]
