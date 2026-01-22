"""
Position Selectors

Query functions for position data following the selector pattern.
Replaces Supabase RPC: get_positions_for_agency
"""
from typing import List, Optional
from uuid import UUID

from django.db import connection


def get_positions_for_agency(user_id: UUID) -> List[dict]:
    """
    Get all positions for a user's agency.
    Translated from Supabase RPC: get_positions_for_agency

    Args:
        user_id: The user UUID (to determine agency)

    Returns:
        List of position dictionaries with counts
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH user_agency AS (
                SELECT agency_id FROM users WHERE id = %s LIMIT 1
            )
            SELECT
                p.id,
                p.name,
                p.level,
                p.description,
                p.is_active,
                p.created_at,
                p.updated_at,
                COUNT(u.id) FILTER (WHERE u.position_id = p.id) as agent_count
            FROM positions p
            CROSS JOIN user_agency ua
            LEFT JOIN users u ON u.position_id = p.id AND u.agency_id = ua.agency_id
            WHERE p.agency_id = ua.agency_id
            GROUP BY p.id, p.name, p.level, p.description, p.is_active, p.created_at, p.updated_at
            ORDER BY p.level ASC, p.name ASC
        """, [str(user_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_position_by_id(position_id: UUID, agency_id: UUID) -> Optional[dict]:
    """
    Get a single position by ID (agency-scoped).

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check

    Returns:
        Position dictionary or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                p.id,
                p.name,
                p.level,
                p.description,
                p.is_active,
                p.created_at,
                p.updated_at,
                COUNT(u.id) FILTER (WHERE u.position_id = p.id) as agent_count
            FROM positions p
            LEFT JOIN users u ON u.position_id = p.id AND u.agency_id = %s
            WHERE p.id = %s AND p.agency_id = %s
            GROUP BY p.id, p.name, p.level, p.description, p.is_active, p.created_at, p.updated_at
        """, [str(agency_id), str(position_id), str(agency_id)])
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


def get_position_product_commissions(position_id: UUID, agency_id: UUID) -> List[dict]:
    """
    Get product commissions for a position.

    Args:
        position_id: The position UUID
        agency_id: The agency UUID for security check

    Returns:
        List of commission dictionaries with product info
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                ppc.id,
                ppc.position_id,
                ppc.product_id,
                ppc.commission_percentage,
                p.name as product_name,
                p.product_code,
                c.name as carrier_name,
                c.display_name as carrier_display_name
            FROM position_product_commissions ppc
            INNER JOIN products p ON p.id = ppc.product_id
            INNER JOIN carriers c ON c.id = p.carrier_id
            INNER JOIN positions pos ON pos.id = ppc.position_id
            WHERE ppc.position_id = %s
                AND pos.agency_id = %s
                AND p.agency_id = %s
                AND p.is_active = true
            ORDER BY c.name ASC, p.name ASC
        """, [str(position_id), str(agency_id), str(agency_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
