"""
Onboarding Selectors

Query functions for onboarding progress retrieval.
Follows the selector pattern used in other apps.
"""
import logging
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)


def get_onboarding_progress(user_id: UUID) -> dict | None:
    """
    Get onboarding progress for a user.

    Args:
        user_id: The user's UUID

    Returns:
        Onboarding progress dict or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id,
                user_id,
                current_step,
                nipr_status,
                nipr_job_id,
                nipr_carriers,
                nipr_licensed_states,
                pending_invitations,
                started_at,
                completed_at,
                updated_at
            FROM onboarding_progress
            WHERE user_id = %s
        """, [str(user_id)])

        row = cursor.fetchone()

    if not row:
        return None

    return {
        'id': str(row[0]),
        'user_id': str(row[1]),
        'current_step': row[2],
        'nipr_status': row[3],
        'nipr_job_id': str(row[4]) if row[4] else None,
        'nipr_carriers': row[5] or [],
        'nipr_licensed_states': row[6] or {'resident': [], 'nonResident': []},
        'pending_invitations': row[7] or [],
        'started_at': row[8].isoformat() if row[8] else None,
        'completed_at': row[9].isoformat() if row[9] else None,
        'updated_at': row[10].isoformat() if row[10] else None,
    }


def get_nipr_job_with_progress(job_id: UUID) -> dict | None:
    """
    Get NIPR job status with progress information.

    Args:
        job_id: The NIPR job UUID

    Returns:
        Job details with progress or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                nj.id,
                nj.user_id,
                nj.status,
                nj.progress,
                nj.progress_message,
                nj.created_at,
                nj.started_at,
                nj.completed_at,
                nj.result_files,
                nj.result_carriers,
                nj.error_message,
                (
                    SELECT COUNT(*) + 1
                    FROM nipr_jobs nj2
                    WHERE nj2.status = 'pending'
                    AND nj2.created_at < nj.created_at
                ) as queue_position
            FROM nipr_jobs nj
            WHERE nj.id = %s
        """, [str(job_id)])

        row = cursor.fetchone()

    if not row:
        return None

    return {
        'job_id': str(row[0]),
        'user_id': str(row[1]) if row[1] else None,
        'status': row[2],
        'progress': row[3] or 0,
        'progress_message': row[4] or '',
        'created_at': row[5].isoformat() if row[5] else None,
        'started_at': row[6].isoformat() if row[6] else None,
        'completed_at': row[7].isoformat() if row[7] else None,
        'result_files': row[8] or [],
        'result_carriers': row[9] or [],
        'error_message': row[10],
        'queue_position': row[11] if row[2] == 'pending' else None,
    }


def check_nipr_already_completed(user_id: UUID) -> dict:
    """
    Check if a user has already completed NIPR verification.

    Args:
        user_id: The user's UUID

    Returns:
        Dict with completed status and carriers
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT unique_carriers
            FROM users
            WHERE id = %s
        """, [str(user_id)])

        row = cursor.fetchone()

    if not row or not row[0]:
        return {'completed': False, 'carriers': []}

    carriers = row[0]
    return {
        'completed': len(carriers) > 0,
        'carriers': carriers
    }
