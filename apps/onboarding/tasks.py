"""
Onboarding Background Tasks

Uses Django 6.0 built-in Tasks framework for background job processing.
No Celery required for simple task patterns.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db import connection, transaction

logger = logging.getLogger(__name__)


# Note: Django 6.0 Tasks framework decorator
# For actual implementation, ensure django_tasks is configured in settings
# from django.tasks import task


def start_nipr_verification(
    user_id: str,
    last_name: str,
    npn: str,
    ssn_last4: str,
    dob: str,
) -> Optional[str]:
    """
    Create a NIPR verification job.

    This creates a job entry that will be picked up by the NIPR automation
    worker (HyperBrowser-based external process).

    Args:
        user_id: User's UUID string
        last_name: User's last name
        npn: National Producer Number
        ssn_last4: Last 4 digits of SSN
        dob: Date of birth (YYYY-MM-DD format)

    Returns:
        Job ID string if created, None on failure
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
                return str(existing[0])

            # Create new job
            cursor.execute("""
                INSERT INTO nipr_jobs (
                    user_id, last_name, npn, ssn_last4, dob, status
                )
                VALUES (%s, %s, %s, %s, %s, 'pending')
                RETURNING id
            """, [user_id, last_name, npn, ssn_last4, dob])

            row = cursor.fetchone()
            return str(row[0]) if row else None

    except Exception as e:
        logger.error(f'Failed to create NIPR job: {e}')
        return None


def get_nipr_queue_position(job_id: str) -> int:
    """
    Get the queue position for a pending NIPR job.

    Args:
        job_id: Job UUID string

    Returns:
        Queue position (1-based) or 0 if not in queue
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                COUNT(*) + 1 as position
            FROM nipr_jobs nj
            WHERE nj.status = 'pending'
            AND nj.created_at < (
                SELECT created_at FROM nipr_jobs WHERE id = %s
            )
        """, [job_id])

        row = cursor.fetchone()
        return row[0] if row else 0


@transaction.atomic
def handle_nipr_upload(
    user_id: str,
    file_path: str,
    carriers: list[str],
    licensed_states: Optional[dict] = None,
) -> bool:
    """
    Process a NIPR document upload.

    Updates both the onboarding progress and user tables with
    the extracted carrier information.

    Args:
        user_id: User's UUID string
        file_path: Path to uploaded file
        carriers: List of carriers extracted from document
        licensed_states: Dict of licensed states {resident: [], nonResident: []}

    Returns:
        True if processed successfully
    """
    licensed_states = licensed_states or {'resident': [], 'nonResident': []}

    try:
        with connection.cursor() as cursor:
            # Update onboarding progress
            cursor.execute("""
                UPDATE onboarding_progress
                SET
                    nipr_status = 'completed',
                    nipr_carriers = %s,
                    nipr_licensed_states = %s
                WHERE user_id = %s
            """, [carriers, licensed_states, user_id])

            # Update users table
            cursor.execute("""
                UPDATE users
                SET unique_carriers = %s
                WHERE id = %s
                RETURNING id
            """, [carriers, user_id])

            row = cursor.fetchone()
            return row is not None

    except Exception as e:
        logger.error(f'Failed to process NIPR upload: {e}')
        return False


# ============================================================================
# Django 6.0 Task Decorators (when ready to use built-in tasks)
# ============================================================================
#
# @task
# def process_nipr_verification_async(job_id: str, user_id: str, nipr_data: dict):
#     """
#     Async NIPR verification task.
#
#     Note: The actual NIPR automation uses HyperBrowser which is
#     an external process. This task is for when we want to move
#     the automation into Django background tasks.
#
#     For now, NIPR automation is handled by the external worker
#     that polls /api/nipr/acquire-job and calls /api/nipr/complete-job.
#     """
#     pass
#
#
# @task
# def analyze_nipr_document_async(file_path: str, user_id: str):
#     """
#     Parse NIPR PDF and extract carriers/states.
#
#     This could be a background task if document analysis
#     is time-consuming.
#     """
#     pass
