"""
NIPR API Views

Provides endpoints for NIPR job management:
- POST /api/nipr/acquire-job - Acquire next pending job
- POST /api/nipr/complete-job - Mark job as complete/failed
- PATCH /api/nipr/job-progress - Update job progress
- POST /api/nipr/release-locks - Release stale locks
- GET /api/nipr/job/{job_id} - Get job status
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .services import (
    acquire_job,
    complete_job,
    get_job_status,
    release_stale_locks,
    update_job_progress,
)

logger = logging.getLogger(__name__)


class AcquireJobView(APIView):
    """
    POST /api/nipr/acquire-job

    Acquire the next pending NIPR job for processing.
    Only one job can be processing at a time (global lock).
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            job = acquire_job()

            if job:
                return Response({
                    'acquired': True,
                    'job': job,
                })
            else:
                return Response({
                    'acquired': False,
                    'message': 'No jobs available or another job is processing',
                })

        except Exception as e:
            logger.error(f'Acquire job failed: {e}')
            return Response(
                {'error': 'Failed to acquire job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CompleteJobView(APIView):
    """
    POST /api/nipr/complete-job

    Mark a NIPR job as completed or failed.

    Request body:
        job_id: UUID of the job
        success: boolean indicating success/failure
        files: optional list of result file paths
        carriers: optional list of carriers found
        error: optional error message if failed
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        job_id_str = request.data.get('job_id')
        if not job_id_str:
            return Response(
                {'error': 'job_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            job_id = UUID(job_id_str)
        except ValueError:
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        success = request.data.get('success', False)
        files = request.data.get('files', [])
        carriers = request.data.get('carriers', [])
        error = request.data.get('error')

        try:
            updated = complete_job(
                job_id=job_id,
                success=success,
                files=files,
                carriers=carriers,
                error=error,
            )

            if updated:
                return Response({
                    'completed': True,
                    'job_id': str(job_id),
                })
            else:
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            logger.error(f'Complete job failed: {e}')
            return Response(
                {'error': 'Failed to complete job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UpdateProgressView(APIView):
    """
    PATCH /api/nipr/job-progress

    Update the progress of a running NIPR job.

    Request body:
        job_id: UUID of the job
        progress: integer 0-100
        message: optional progress message
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        job_id_str = request.data.get('job_id')
        if not job_id_str:
            return Response(
                {'error': 'job_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            job_id = UUID(job_id_str)
        except ValueError:
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        progress = request.data.get('progress')
        if progress is None:
            return Response(
                {'error': 'progress is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            progress = int(progress)
            progress = max(0, min(100, progress))  # Clamp to 0-100
        except (ValueError, TypeError):
            return Response(
                {'error': 'progress must be an integer'},
                status=status.HTTP_400_BAD_REQUEST
            )

        message = request.data.get('message')

        try:
            updated = update_job_progress(
                job_id=job_id,
                progress=progress,
                message=message,
            )

            if updated:
                return Response({
                    'updated': True,
                    'job_id': str(job_id),
                    'progress': progress,
                })
            else:
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            logger.error(f'Update progress failed: {e}')
            return Response(
                {'error': 'Failed to update progress', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ReleaseLocksView(APIView):
    """
    POST /api/nipr/release-locks

    Release stale locks on NIPR jobs.
    Admin only endpoint for maintenance.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            released_count = release_stale_locks()

            return Response({
                'released': True,
                'count': released_count,
            })

        except Exception as e:
            logger.error(f'Release locks failed: {e}')
            return Response(
                {'error': 'Failed to release locks', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class JobStatusView(APIView):
    """
    GET /api/nipr/job/{job_id}

    Get the current status of a NIPR job.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            job_uuid = UUID(job_id)
        except ValueError:
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            job = get_job_status(job_uuid)

            if job:
                return Response(job)
            else:
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            logger.error(f'Get job status failed: {e}')
            return Response(
                {'error': 'Failed to get job status', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
