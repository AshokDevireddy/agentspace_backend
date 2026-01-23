"""
Onboarding Services

Business logic for onboarding operations.
Uses @transaction.atomic for database consistency.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db import connection, transaction

logger = logging.getLogger(__name__)


@transaction.atomic
def create_onboarding_progress(user_id: UUID) -> dict:
    """
    Create a new onboarding progress record for a user.
    Called after account setup during invite flow.

    Args:
        user_id: The user's UUID

    Returns:
        The created progress record
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO onboarding_progress (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO UPDATE SET updated_at = NOW()
            RETURNING id, user_id, current_step, nipr_status, started_at
        """, [str(user_id)])

        row = cursor.fetchone()

    return {
        'id': str(row[0]),
        'user_id': str(row[1]),
        'current_step': row[2],
        'nipr_status': row[3],
        'started_at': row[4].isoformat() if row[4] else None,
    }


@transaction.atomic
def update_onboarding_step(user_id: UUID, step: str) -> bool:
    """
    Update the current onboarding step.

    Args:
        user_id: The user's UUID
        step: The new step (nipr_verification, team_invitation, completed)

    Returns:
        True if updated successfully
    """
    valid_steps = ('nipr_verification', 'team_invitation', 'completed')
    if step not in valid_steps:
        raise ValueError(f"Invalid step: {step}. Must be one of {valid_steps}")

    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE onboarding_progress
            SET
                current_step = %s,
                completed_at = CASE WHEN %s = 'completed' THEN NOW() ELSE completed_at END
            WHERE user_id = %s
            RETURNING id
        """, [step, step, str(user_id)])

        row = cursor.fetchone()

    return row is not None


@transaction.atomic
def update_nipr_status(
    user_id: UUID,
    status: str,
    job_id: Optional[UUID] = None,
    carriers: Optional[list[str]] = None,
    licensed_states: Optional[dict] = None,
) -> bool:
    """
    Update NIPR verification status.

    Args:
        user_id: The user's UUID
        status: The NIPR status (pending, running, completed, failed, skipped)
        job_id: Optional NIPR job UUID
        carriers: Optional list of carriers found
        licensed_states: Optional dict of licensed states

    Returns:
        True if updated successfully
    """
    valid_statuses = ('pending', 'running', 'completed', 'failed', 'skipped')
    if status not in valid_statuses:
        raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")

    update_parts = ['nipr_status = %s']
    params = [status]

    if job_id is not None:
        update_parts.append('nipr_job_id = %s')
        params.append(str(job_id))

    if carriers is not None:
        update_parts.append('nipr_carriers = %s')
        params.append(carriers)

    if licensed_states is not None:
        update_parts.append('nipr_licensed_states = %s')
        params.append(licensed_states)

    params.append(str(user_id))

    with connection.cursor() as cursor:
        cursor.execute(f"""
            UPDATE onboarding_progress
            SET {', '.join(update_parts)}
            WHERE user_id = %s
            RETURNING id
        """, params)

        row = cursor.fetchone()

    return row is not None


@transaction.atomic
def add_pending_invitation(user_id: UUID, invitation: dict) -> bool:
    """
    Add a pending invitation to the list.

    Args:
        user_id: The user's UUID
        invitation: Invitation dict with firstName, lastName, email, etc.

    Returns:
        True if added successfully
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE onboarding_progress
            SET pending_invitations = pending_invitations || %s::jsonb
            WHERE user_id = %s
            RETURNING id
        """, [[invitation], str(user_id)])

        row = cursor.fetchone()

    return row is not None


@transaction.atomic
def remove_pending_invitation(user_id: UUID, index: int) -> bool:
    """
    Remove a pending invitation by index.

    Args:
        user_id: The user's UUID
        index: Index of the invitation to remove

    Returns:
        True if removed successfully
    """
    with connection.cursor() as cursor:
        # First get current invitations
        cursor.execute("""
            SELECT pending_invitations
            FROM onboarding_progress
            WHERE user_id = %s
        """, [str(user_id)])

        row = cursor.fetchone()
        if not row:
            return False

        invitations = row[0] or []
        if index < 0 or index >= len(invitations):
            return False

        # Remove the invitation at index
        invitations.pop(index)

        cursor.execute("""
            UPDATE onboarding_progress
            SET pending_invitations = %s
            WHERE user_id = %s
            RETURNING id
        """, [invitations, str(user_id)])

        row = cursor.fetchone()

    return row is not None


@transaction.atomic
def clear_pending_invitations(user_id: UUID) -> list:
    """
    Clear all pending invitations and return them.

    Args:
        user_id: The user's UUID

    Returns:
        List of cleared invitations
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE onboarding_progress
            SET pending_invitations = '[]'::jsonb
            WHERE user_id = %s
            RETURNING (
                SELECT pending_invitations
                FROM onboarding_progress
                WHERE user_id = %s
            )
        """, [str(user_id), str(user_id)])

        row = cursor.fetchone()

    return row[0] if row and row[0] else []


@transaction.atomic
def complete_onboarding(user_id: UUID) -> bool:
    """
    Mark onboarding as complete and update user status to active.

    Args:
        user_id: The user's UUID

    Returns:
        True if completed successfully
    """
    with connection.cursor() as cursor:
        # Update onboarding progress
        cursor.execute("""
            UPDATE onboarding_progress
            SET
                current_step = 'completed',
                completed_at = NOW()
            WHERE user_id = %s
            RETURNING id
        """, [str(user_id)])

        if not cursor.fetchone():
            return False

        # Update user status to active
        cursor.execute("""
            UPDATE users
            SET status = 'active'
            WHERE id = %s AND status = 'onboarding'
            RETURNING id
        """, [str(user_id)])

        row = cursor.fetchone()

    return row is not None


@transaction.atomic
def store_nipr_carriers(user_id: UUID, carriers: list[str]) -> bool:
    """
    Store NIPR carriers in both onboarding_progress and users table.

    Args:
        user_id: The user's UUID
        carriers: List of carrier names

    Returns:
        True if stored successfully
    """
    # Filter and clean carriers
    valid_carriers = [
        c.strip() for c in carriers
        if c and isinstance(c, str) and c.strip()
    ]

    with connection.cursor() as cursor:
        # Update onboarding progress
        cursor.execute("""
            UPDATE onboarding_progress
            SET nipr_carriers = %s
            WHERE user_id = %s
        """, [valid_carriers, str(user_id)])

        # Update users table
        cursor.execute("""
            UPDATE users
            SET unique_carriers = %s
            WHERE id = %s
            RETURNING id
        """, [valid_carriers, str(user_id)])

        row = cursor.fetchone()

    return row is not None
