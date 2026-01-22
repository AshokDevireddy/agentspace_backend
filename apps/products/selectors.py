"""
Product Selectors

Query functions for product data following the selector pattern.
"""
from typing import List, Optional
from uuid import UUID

from django.db import connection


def get_products_for_carrier(carrier_id: UUID, agency_id: UUID) -> List[dict]:
    """
    Get all active products for a specific carrier and agency.

    Args:
        carrier_id: The carrier UUID
        agency_id: The agency UUID

    Returns:
        List of product dictionaries
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id,
                carrier_id,
                name,
                product_code,
                is_active,
                created_at
            FROM products
            WHERE carrier_id = %s
                AND agency_id = %s
                AND is_active = true
            ORDER BY name ASC
        """, [str(carrier_id), str(agency_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_all_products_for_agency(agency_id: UUID) -> List[dict]:
    """
    Get all active products for an agency with carrier information.

    Args:
        agency_id: The agency UUID

    Returns:
        List of product dictionaries with carrier name
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                p.id,
                p.carrier_id,
                p.name,
                p.product_code,
                p.is_active,
                p.created_at,
                c.name as carrier_name,
                c.display_name as carrier_display_name
            FROM products p
            LEFT JOIN carriers c ON c.id = p.carrier_id
            WHERE p.agency_id = %s
                AND p.is_active = true
            ORDER BY COALESCE(c.display_name, c.name) ASC, p.name ASC
        """, [str(agency_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_product_by_id(product_id: UUID, agency_id: UUID) -> Optional[dict]:
    """
    Get a single product by ID (agency-scoped).

    Args:
        product_id: The product UUID
        agency_id: The agency UUID for security check

    Returns:
        Product dictionary or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                p.id,
                p.carrier_id,
                p.name,
                p.product_code,
                p.is_active,
                p.created_at,
                c.name as carrier_name,
                c.display_name as carrier_display_name
            FROM products p
            LEFT JOIN carriers c ON c.id = p.carrier_id
            WHERE p.id = %s
                AND p.agency_id = %s
        """, [str(product_id), str(agency_id)])
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None
