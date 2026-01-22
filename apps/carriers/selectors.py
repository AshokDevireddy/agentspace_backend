"""
Carrier Selectors

Query functions for carrier data following the selector pattern.
"""
from typing import List, Optional
from uuid import UUID

from django.db import connection

from apps.core.models import Carrier


def get_active_carriers() -> List[dict]:
    """
    Get all active carriers ordered by display_name.

    Returns:
        List of carrier dictionaries with id, name, display_name, is_active, created_at
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id,
                name,
                display_name,
                is_active,
                created_at
            FROM carriers
            WHERE is_active = true
            ORDER BY COALESCE(display_name, name) ASC
        """)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_carriers_with_products_for_agency(agency_id: UUID) -> List[dict]:
    """
    Get carriers that have products associated with the given agency.

    Args:
        agency_id: The agency UUID

    Returns:
        List of carrier dictionaries with nested products
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT
                c.id,
                c.name,
                c.display_name,
                c.is_active
            FROM carriers c
            INNER JOIN products p ON p.carrier_id = c.id
            WHERE p.agency_id = %s
                AND c.is_active = true
                AND p.is_active = true
            ORDER BY COALESCE(c.display_name, c.name) ASC
        """, [str(agency_id)])
        columns = [col[0] for col in cursor.description]
        carriers = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Fetch products for each carrier
    for carrier in carriers:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    name,
                    product_code,
                    is_active,
                    created_at
                FROM products
                WHERE carrier_id = %s
                    AND agency_id = %s
                    AND is_active = true
                ORDER BY name ASC
            """, [str(carrier['id']), str(agency_id)])
            columns = [col[0] for col in cursor.description]
            carrier['products'] = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return carriers


def get_carrier_names() -> List[dict]:
    """
    Get carrier names for dropdowns (lightweight query).

    Returns:
        List of carrier dictionaries with id and name only
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id,
                COALESCE(display_name, name) as name
            FROM carriers
            WHERE is_active = true
            ORDER BY COALESCE(display_name, name) ASC
        """)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
