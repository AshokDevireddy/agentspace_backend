"""
NIPR Services

Job management functions for NIPR processing translated from Supabase RPC functions:
- acquire_nipr_job -> acquire_job()
- complete_nipr_job -> complete_job()
- update_nipr_job_progress -> update_job_progress()
- release_stale_nipr_locks -> release_stale_locks()
"""
import logging
from uuid import UUID

from django.db import connection, transaction

logger = logging.getLogger(__name__)


def acquire_job() -> dict | None:
    """
    Acquire the next pending NIPR job for processing.
    Translated from Supabase RPC: acquire_nipr_job

    Uses row-level locking to prevent race conditions.
    Only one job can be processing at a time (global lock).

    Returns:
        Job details if acquired, None if no jobs available
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            -- Check if any job is currently being processed (global lock)
            WITH check_processing AS (
                SELECT 1 FROM nipr_jobs
                WHERE status = 'processing'
                AND (locked_until IS NULL OR locked_until > NOW())
                LIMIT 1
            ),
            acquired AS (
                UPDATE nipr_jobs
                SET
                    status = 'processing',
                    started_at = NOW(),
                    locked_until = NOW() + INTERVAL '10 minutes'
                WHERE NOT EXISTS (SELECT 1 FROM check_processing)
                AND id = (
                    SELECT id FROM nipr_jobs
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, user_id, last_name, npn, ssn_last4, dob
            )
            SELECT * FROM acquired
        """)

        row = cursor.fetchone()

    if not row:
        return None

    return {
        'job_id': str(row[0]),
        'user_id': str(row[1]) if row[1] else None,
        'last_name': row[2],
        'npn': row[3],
        'ssn_last4': row[4],
        'dob': row[5],
    }


@transaction.atomic
def complete_job(
    job_id: UUID,
    success: bool,
    files: list[str] | None = None,
    carriers: list[str] | None = None,
    error: str | None = None,
) -> None:
    """
    Mark a NIPR job as completed or failed.
    Translated from Supabase RPC: complete_nipr_job

    Args:
        job_id: The job ID to complete
        success: Whether the job succeeded
        files: List of result file paths
        carriers: List of carriers found
        error: Error message if failed

    Returns:
        None (matches RPC void return type)
    """
    files = files or []
    carriers = carriers or []

    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE nipr_jobs
            SET
                status = CASE WHEN %s THEN 'completed' ELSE 'failed' END,
                completed_at = NOW(),
                locked_until = NULL,
                progress = 100,
                progress_message = CASE WHEN %s THEN 'Complete!' ELSE 'Failed' END,
                result_files = %s,
                result_carriers = %s,
                error_message = %s
            WHERE id = %s
        """, [success, success, files, carriers, error, str(job_id)])


def update_job_progress(
    job_id: UUID,
    progress: int,
    message: str | None = None,
) -> None:
    """
    Update the progress of a running NIPR job.
    Translated from Supabase RPC: update_nipr_job_progress

    Args:
        job_id: The job ID to update
        progress: Progress percentage (0-100)
        message: Optional progress message

    Returns:
        None (matches RPC void return type)
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE nipr_jobs
            SET
                progress = %s,
                progress_message = COALESCE(%s, progress_message)
            WHERE id = %s
        """, [progress, message, str(job_id)])


def release_stale_locks() -> int:
    """
    Release stale locks on NIPR jobs.
    Translated from Supabase RPC: release_stale_nipr_locks

    Jobs that have been processing for too long (past locked_until)
    are reset to pending status.

    Returns:
        Number of jobs released
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE nipr_jobs
            SET
                status = 'pending',
                started_at = NULL,
                locked_until = NULL
            WHERE status = 'processing'
            AND locked_until < NOW()
        """)

        return cursor.rowcount


def create_job(
    user_id: str,
    last_name: str,
    npn: str,
    ssn_last4: str,
    dob: str,
) -> dict | None:
    """
    Create a new NIPR verification job.
    If a pending/processing job already exists for the user, returns that job's ID.

    Args:
        user_id: User's UUID string
        last_name: User's last name
        npn: National Producer Number
        ssn_last4: Last 4 digits of SSN
        dob: Date of birth (MM/DD/YYYY format)

    Returns:
        Dict with job_id and whether it was newly created, or None on failure
    """
    try:
        with connection.cursor() as cursor:
            # Check for existing pending/processing job for this user
            cursor.execute("""
                SELECT id FROM nipr_jobs
                WHERE user_id = %s
                AND status IN ('pending', 'processing')
                LIMIT 1
            """, [user_id])

            existing = cursor.fetchone()
            if existing:
                return {
                    'job_id': str(existing[0]),
                    'created': False,
                    'message': 'Existing job found'
                }

            # Create new job
            cursor.execute("""
                INSERT INTO nipr_jobs (
                    user_id, last_name, npn, ssn_last4, dob, status
                )
                VALUES (%s, %s, %s, %s, %s, 'pending')
                RETURNING id
            """, [user_id, last_name, npn, ssn_last4, dob])

            row = cursor.fetchone()
            if row:
                return {
                    'job_id': str(row[0]),
                    'created': True,
                    'message': 'Job created'
                }
            return None

    except Exception as e:
        logger.error(f'Failed to create NIPR job: {e}')
        return None


def check_user_nipr_completed(user_id: str) -> dict:
    """
    Check if a user has already completed NIPR verification (has carriers).

    Args:
        user_id: User's UUID string

    Returns:
        Dict with 'completed' boolean and 'carriers' list if completed
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT unique_carriers
            FROM users
            WHERE id = %s
        """, [user_id])

        row = cursor.fetchone()

    if row and row[0]:
        carriers = row[0]
        if isinstance(carriers, list) and len(carriers) > 0:
            return {
                'completed': True,
                'carriers': carriers
            }

    return {
        'completed': False,
        'carriers': []
    }


def has_pending_jobs() -> bool:
    """
    Check if there are any pending NIPR jobs in the queue.

    Returns:
        True if there are pending jobs, False otherwise
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM nipr_jobs
                WHERE status = 'pending'
                LIMIT 1
            )
        """)
        row = cursor.fetchone()

    return row[0] if row else False


def get_job_status(job_id: UUID) -> dict | None:
    """
    Get the current status of a NIPR job.

    Args:
        job_id: The job ID to check

    Returns:
        Job status details or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                id,
                user_id,
                status,
                progress,
                progress_message,
                created_at,
                started_at,
                completed_at,
                result_files,
                result_carriers,
                error_message
            FROM nipr_jobs
            WHERE id = %s
        """, [str(job_id)])

        row = cursor.fetchone()

    if not row:
        return None

    return {
        'job_id': str(row[0]),
        'user_id': str(row[1]) if row[1] else None,
        'status': row[2],
        'progress': row[3],
        'progress_message': row[4],
        'created_at': row[5].isoformat() if row[5] else None,
        'started_at': row[6].isoformat() if row[6] else None,
        'completed_at': row[7].isoformat() if row[7] else None,
        'result_files': row[8] or [],
        'result_carriers': row[9] or [],
        'error_message': row[10],
    }
