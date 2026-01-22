"""
Agent Services

Business logic for agent operations.
"""
from typing import Optional
from uuid import UUID

from django.db import connection, transaction


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
