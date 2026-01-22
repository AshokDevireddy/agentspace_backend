"""
Product Services

Business logic for product operations.
"""
from typing import Optional
from uuid import UUID

from django.db import connection, transaction


@transaction.atomic
def create_product(
    *,
    carrier_id: UUID,
    agency_id: UUID,
    name: str,
    product_code: Optional[str] = None,
    is_active: bool = True,
) -> dict:
    """
    Create a new product and auto-generate commission entries for all positions.

    Args:
        carrier_id: The carrier UUID
        agency_id: The agency UUID
        name: Product name
        product_code: Optional product code
        is_active: Whether the product is active

    Returns:
        Created product dictionary
    """
    with connection.cursor() as cursor:
        # Check if positions exist for this agency
        cursor.execute("""
            SELECT COUNT(*) FROM positions
            WHERE agency_id = %s AND is_active = true
        """, [str(agency_id)])
        position_count = cursor.fetchone()[0]

        if position_count == 0:
            raise ValueError(
                'No positions found. Please create positions before adding products. '
                'All products must have commission percentages set for each position.'
            )

        # Insert the product
        cursor.execute("""
            INSERT INTO products (carrier_id, agency_id, name, product_code, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, carrier_id, agency_id, name, product_code, is_active, created_at
        """, [str(carrier_id), str(agency_id), name, product_code, is_active])
        columns = [col[0] for col in cursor.description]
        product = dict(zip(columns, cursor.fetchone()))

        # Auto-create commission entries for all positions (set to 0%)
        cursor.execute("""
            INSERT INTO position_product_commissions (position_id, product_id, commission_percentage)
            SELECT id, %s, 0
            FROM positions
            WHERE agency_id = %s AND is_active = true
        """, [str(product['id']), str(agency_id)])

        return product


@transaction.atomic
def update_product(
    *,
    product_id: UUID,
    agency_id: UUID,
    name: Optional[str] = None,
    product_code: Optional[str] = None,
    is_active: Optional[bool] = None,
) -> Optional[dict]:
    """
    Update an existing product.

    Args:
        product_id: The product UUID
        agency_id: The agency UUID for security check
        name: Optional new name
        product_code: Optional new product code
        is_active: Optional new active status

    Returns:
        Updated product dictionary or None if not found
    """
    # Build update fields dynamically
    updates = []
    params = []

    if name is not None:
        updates.append('name = %s')
        params.append(name)

    if product_code is not None:
        updates.append('product_code = %s')
        params.append(product_code)

    if is_active is not None:
        updates.append('is_active = %s')
        params.append(is_active)

    if not updates:
        return None

    updates.append('updated_at = NOW()')
    params.extend([str(product_id), str(agency_id)])

    with connection.cursor() as cursor:
        cursor.execute(f"""
            UPDATE products
            SET {', '.join(updates)}
            WHERE id = %s AND agency_id = %s
            RETURNING id, carrier_id, name, product_code, is_active, created_at, updated_at
        """, params)
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None
