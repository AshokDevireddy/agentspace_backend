"""
Agent Services

Business logic for agent operations.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db import connection, transaction

logger = logging.getLogger(__name__)


@transaction.atomic
def update_agent_position(
    *,
    user_id: UUID,
    agent_id: UUID,
    position_id: UUID,
) -> dict:
    """
    Update an agent's position with permission checks.
    Translated from Supabase RPC: update_agent_position

    Args:
        user_id: The requesting user's ID
        agent_id: The agent to update
        position_id: The new position ID

    Returns:
        Dictionary with success status and optional error message
    """
    with connection.cursor() as cursor:
        # Get current user context
        cursor.execute("""
            SELECT
                u.agency_id,
                COALESCE(u.is_admin, false) OR u.perm_level = 'admin' OR u.role = 'admin' as is_admin
            FROM users u
            WHERE u.id = %s
            LIMIT 1
        """, [str(user_id)])

        user_row = cursor.fetchone()
        if not user_row:
            return {'success': False, 'error': 'User not found'}

        agency_id, is_admin = user_row

        # Check if position belongs to the same agency
        cursor.execute("""
            SELECT agency_id
            FROM positions
            WHERE id = %s
            LIMIT 1
        """, [str(position_id)])

        position_row = cursor.fetchone()
        if not position_row or position_row[0] != agency_id:
            return {'success': False, 'error': 'Invalid position for this agency'}

        # Check permissions: admin can update anyone, agents can update their downlines
        has_permission = is_admin

        if not is_admin:
            # Check if agent_id is in the downline of user_id
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT u.id
                    FROM users u
                    WHERE u.id = %s
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE d.id <> u.id  -- Prevent cycles
                )
                SELECT EXISTS (
                    SELECT 1 FROM downline WHERE id = %s
                )
            """, [str(user_id), str(agent_id)])

            has_permission = cursor.fetchone()[0]

        if not has_permission:
            return {'success': False, 'error': 'You do not have permission to update this agent'}

        # Update the agent's position
        cursor.execute("""
            UPDATE users
            SET
                position_id = %s,
                updated_at = NOW()
            WHERE id = %s
                AND agency_id = %s
            RETURNING id
        """, [str(position_id), str(agent_id), str(agency_id)])

        updated = cursor.fetchone() is not None

        if not updated:
            return {'success': False, 'error': 'Agent not found or does not belong to your agency'}

        return {'success': True}


@transaction.atomic
def assign_position_to_agent(
    *,
    agent_id: UUID,
    position_id: Optional[UUID],
    agency_id: UUID,
) -> Optional[dict]:
    """
    Assign a position to an agent.

    Args:
        agent_id: The agent UUID
        position_id: The position UUID (or None to clear)
        agency_id: The agency UUID for security check

    Returns:
        Updated agent dictionary or None if not found
    """
    with connection.cursor() as cursor:
        if position_id:
            # Verify position belongs to agency
            cursor.execute("""
                SELECT id FROM positions
                WHERE id = %s AND agency_id = %s
            """, [str(position_id), str(agency_id)])
            if not cursor.fetchone():
                raise ValueError('Position not found or does not belong to your agency')

        cursor.execute("""
            UPDATE users
            SET position_id = %s, updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id, first_name, last_name, email, position_id, status
        """, [str(position_id) if position_id else None, str(agent_id), str(agency_id)])

        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None
