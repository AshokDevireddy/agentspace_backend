"""
Position Services

Business logic for position operations.
"""
from typing import Any
from uuid import UUID

from django.db import connection, transaction


@transaction.atomic
def create_position(
    *,
    agency_id: UUID,
    name: str,
    level: int,
    description: str | None = None,
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
        return dict(zip(columns, cursor.fetchone(), strict=False))


@transaction.atomic
def update_position(
    *,
    position_id: UUID,
    agency_id: UUID,
    name: str | None = None,
    level: int | None = None,
    description: str | None = None,
    is_active: bool | None = None,
) -> dict | None:
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
    params: list[Any] = []

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
            return dict(zip(columns, row, strict=False))
        return None


@transaction.atomic
def delete_position(
    *,
    position_id: UUID,
    agency_id: UUID,
) -> bool:
    """
    Delete a position if no agents are assigned to it.

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check

    Returns:
        True if deleted, False if not found

    Raises:
        ValueError: If agents are assigned to this position
    """
    with connection.cursor() as cursor:
        # Check if any agents are assigned to this position
        cursor.execute("""
            SELECT COUNT(*) FROM users
            WHERE position_id = %s AND agency_id = %s
        """, [str(position_id), str(agency_id)])
        agent_count = cursor.fetchone()[0]

        if agent_count > 0:
            raise ValueError(
                'This position is currently assigned to one or more agents. '
                'Please reassign them first.'
            )

        # Delete the position
        cursor.execute("""
            DELETE FROM positions
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(position_id), str(agency_id)])
        return cursor.fetchone() is not None


@transaction.atomic
def update_position_commission(
    *,
    commission_id: UUID,
    agency_id: UUID,
    commission_percentage: float,
) -> dict | None:
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
            return dict(zip(columns, row, strict=False))
        return None


@transaction.atomic
def delete_position_commission(
    *,
    commission_id: UUID,
    agency_id: UUID,
) -> bool:
    """
    Delete a position-product commission entry.

    Args:
        commission_id: The commission record UUID
        agency_id: The agency UUID for security check

    Returns:
        True if deleted, False if not found
    """
    with connection.cursor() as cursor:
        # Delete commission only if it belongs to a position in the agency
        cursor.execute("""
            DELETE FROM position_product_commissions ppc
            USING positions pos
            WHERE ppc.id = %s
                AND ppc.position_id = pos.id
                AND pos.agency_id = %s
            RETURNING ppc.id
        """, [str(commission_id), str(agency_id)])
        return cursor.fetchone() is not None


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


@transaction.atomic
def upsert_position_commissions(
    agency_id: UUID,
    commissions: list[dict],
) -> list[dict]:
    """
    Batch upsert position-product commissions.

    Creates new entries or updates existing ones based on position_id + product_id combination.

    Args:
        agency_id: The agency UUID for security validation
        commissions: List of dicts with position_id, product_id, commission_percentage

    Returns:
        List of upserted commission dictionaries
    """
    if not commissions:
        return []

    results = []

    with connection.cursor() as cursor:
        for comm in commissions:
            position_id = str(comm['position_id'])
            product_id = str(comm['product_id'])
            commission_percentage = float(comm['commission_percentage'])

            # Verify position belongs to agency
            cursor.execute("""
                SELECT id FROM positions
                WHERE id = %s AND agency_id = %s
            """, [position_id, str(agency_id)])

            if not cursor.fetchone():
                # Skip if position doesn't belong to agency
                continue

            # Upsert the commission
            cursor.execute("""
                INSERT INTO position_product_commissions (position_id, product_id, commission_percentage)
                VALUES (%s, %s, %s)
                ON CONFLICT (position_id, product_id)
                DO UPDATE SET commission_percentage = EXCLUDED.commission_percentage
                RETURNING id, position_id, product_id, commission_percentage
            """, [position_id, product_id, commission_percentage])

            row = cursor.fetchone()
            if row:
                results.append({
                    'id': str(row[0]),
                    'position_id': str(row[1]),
                    'product_id': str(row[2]),
                    'commission_percentage': float(row[3]),
                })

    return results
