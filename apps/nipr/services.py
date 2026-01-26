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
) -> bool:
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
        True if job was updated
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
            RETURNING id
        """, [success, success, files, carriers, error, str(job_id)])

        row = cursor.fetchone()

    return row is not None


def update_job_progress(
    job_id: UUID,
    progress: int,
    message: str | None = None,
) -> bool:
    """
    Update the progress of a running NIPR job.
    Translated from Supabase RPC: update_nipr_job_progress

    Args:
        job_id: The job ID to update
        progress: Progress percentage (0-100)
        message: Optional progress message

    Returns:
        True if job was updated
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE nipr_jobs
            SET
                progress = %s,
                progress_message = COALESCE(%s, progress_message)
            WHERE id = %s
            RETURNING id
        """, [progress, message, str(job_id)])

        row = cursor.fetchone()

    return row is not None


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
