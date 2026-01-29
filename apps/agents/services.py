"""
Agent Services

Business logic for agent operations.
"""
import logging
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
    position_id: UUID | None,
    agency_id: UUID,
) -> dict | None:
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
            return dict(zip(columns, row, strict=False))
        return None


@transaction.atomic
def invite_agent(
    *,
    inviter_id: UUID,
    agency_id: UUID,
    email: str,
    first_name: str,
    last_name: str,
    phone_number: str | None = None,
    position_id: UUID | None = None,
) -> dict:
    """
    Invite a new agent to the agency.
    Creates a user record with status='invited' and sets upline to inviter.

    Args:
        inviter_id: The inviting user's ID (becomes upline)
        agency_id: The agency UUID
        email: Invited agent's email
        first_name: Invited agent's first name
        last_name: Invited agent's last name
        phone_number: Optional phone number
        position_id: Optional position ID

    Returns:
        Dictionary with invited user details or error
    """
    import uuid

    email = email.strip().lower()

    with connection.cursor() as cursor:
        # Check if user already exists in this agency
        cursor.execute("""
            SELECT id, status FROM users
            WHERE email = %s AND agency_id = %s
            LIMIT 1
        """, [email, str(agency_id)])

        existing = cursor.fetchone()
        if existing:
            existing_id, existing_status = existing
            if existing_status == 'active':
                return {'success': False, 'error': 'User with this email already exists'}
            if existing_status == 'invited':
                return {
                    'success': True,
                    'user_id': str(existing_id),
                    'message': 'User already invited - resend invite email'
                }

        # Validate position if provided
        if position_id:
            cursor.execute("""
                SELECT id FROM positions
                WHERE id = %s AND agency_id = %s
            """, [str(position_id), str(agency_id)])
            if not cursor.fetchone():
                return {'success': False, 'error': 'Invalid position'}

        # Create invited user
        new_user_id = uuid.uuid4()
        cursor.execute("""
            INSERT INTO users (
                id, email, first_name, last_name, phone_number,
                agency_id, upline_id, position_id, role, status,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'agent', 'invited', NOW(), NOW())
            RETURNING id, email, first_name, last_name, status
        """, [
            str(new_user_id),
            email,
            first_name.strip(),
            last_name.strip(),
            phone_number.strip() if phone_number else None,
            str(agency_id),
            str(inviter_id),
            str(position_id) if position_id else None,
        ])

        row = cursor.fetchone()
        if not row:
            return {'success': False, 'error': 'Failed to create user'}

        return {
            'success': True,
            'user_id': str(row[0]),
            'email': row[1],
            'first_name': row[2],
            'last_name': row[3],
            'status': row[4],
        }
