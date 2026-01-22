"""
Position Services

Business logic for position operations.
"""
from typing import Optional
from uuid import UUID

from django.db import connection, transaction


@transaction.atomic
def create_position(
    *,
    agency_id: UUID,
    name: str,
    level: int,
    description: Optional[str] = None,
    is_active: bool = True,
) -> dict:
    """
    Create a new position.

    Args:
        agency_id: The agency UUID
        name: Position name
        level: Position level in hierarchy
        description: Optional description
        is_active: Whether the position is active

    Returns:
        Created position dictionary
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO positions (agency_id, name, level, description, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, agency_id, name, level, description, is_active, created_at
        """, [str(agency_id), name, level, description, is_active])
        columns = [col[0] for col in cursor.description]
        return dict(zip(columns, cursor.fetchone()))


@transaction.atomic
def update_position(
    *,
    position_id: UUID,
    agency_id: UUID,
    name: Optional[str] = None,
    level: Optional[int] = None,
    description: Optional[str] = None,
    is_active: Optional[bool] = None,
) -> Optional[dict]:
    """
    Update an existing position.

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check
        name: Optional new name
        level: Optional new level
        description: Optional new description
        is_active: Optional new active status

    Returns:
        Updated position dictionary or None if not found
    """
    updates = []
    params = []

    if name is not None:
        updates.append('name = %s')
        params.append(name)

    if level is not None:
        updates.append('level = %s')
        params.append(level)

    if description is not None:
        updates.append('description = %s')
        params.append(description)

    if is_active is not None:
        updates.append('is_active = %s')
        params.append(is_active)

    if not updates:
        return None

    updates.append('updated_at = NOW()')
    params.extend([str(position_id), str(agency_id)])

    with connection.cursor() as cursor:
        cursor.execute(f"""
            UPDATE positions
            SET {', '.join(updates)}
            WHERE id = %s AND agency_id = %s
            RETURNING id, name, level, description, is_active, created_at, updated_at
        """, params)
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


@transaction.atomic
def update_position_commission(
    *,
    commission_id: UUID,
    agency_id: UUID,
    commission_percentage: float,
) -> Optional[dict]:
    """
    Update a position-product commission percentage.

    Args:
        commission_id: The commission record UUID
        agency_id: The agency UUID for security check
        commission_percentage: New commission percentage

    Returns:
        Updated commission dictionary or None if not found
    """
    with connection.cursor() as cursor:
        # Verify the commission belongs to the agency
        cursor.execute("""
            UPDATE position_product_commissions ppc
            SET commission_percentage = %s
            FROM positions pos
            WHERE ppc.id = %s
                AND ppc.position_id = pos.id
                AND pos.agency_id = %s
            RETURNING ppc.id, ppc.position_id, ppc.product_id, ppc.commission_percentage
        """, [commission_percentage, str(commission_id), str(agency_id)])
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


@transaction.atomic
def sync_position_commissions(agency_id: UUID) -> int:
    """
    Sync position commissions - create missing entries for all position-product combinations.

    Args:
        agency_id: The agency UUID

    Returns:
        Number of commission entries created
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO position_product_commissions (position_id, product_id, commission_percentage)
            SELECT pos.id, p.id, 0
            FROM positions pos
            CROSS JOIN products p
            WHERE pos.agency_id = %s
                AND p.agency_id = %s
                AND pos.is_active = true
                AND p.is_active = true
                AND NOT EXISTS (
                    SELECT 1 FROM position_product_commissions ppc
                    WHERE ppc.position_id = pos.id AND ppc.product_id = p.id
                )
            RETURNING id
        """, [str(agency_id), str(agency_id)])
        return cursor.rowcount
