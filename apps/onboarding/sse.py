"""
Server-Sent Events (SSE) for NIPR Progress

Provides real-time updates for NIPR verification progress.
Replaces 30s polling with efficient SSE streaming.
"""
import json
import logging
import time
from uuid import UUID

from django.db import connection
from django.http import StreamingHttpResponse
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

logger = logging.getLogger(__name__)

# SSE polling interval in seconds
SSE_POLL_INTERVAL = 2

# Maximum time to keep connection open (5 minutes)
SSE_MAX_DURATION = 300


class NiprProgressSSEView(APIView):
    """
    GET /api/onboarding/nipr/sse?job_id={job_id}

    Server-Sent Events stream for NIPR job progress.
    Polls database every 2 seconds and pushes updates to client.
    Connection closes automatically when job completes/fails or after 5 minutes.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return StreamingHttpResponse(
                self._error_stream('Unauthorized'),
                content_type='text/event-stream',
                status=401
            )

        job_id_str = request.GET.get('job_id')
        if not job_id_str:
            return StreamingHttpResponse(
                self._error_stream('job_id parameter required'),
                content_type='text/event-stream',
                status=400
            )

        try:
            job_id = UUID(job_id_str)
        except ValueError:
            return StreamingHttpResponse(
                self._error_stream('Invalid job_id format'),
                content_type='text/event-stream',
                status=400
            )

        response = StreamingHttpResponse(
            self._event_stream(job_id, user.id),
            content_type='text/event-stream'
        )
        # Disable buffering for real-time streaming
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'  # For nginx
        return response

    def _error_stream(self, message: str):
        """Generate an error event and close stream."""
        yield f"event: error\ndata: {json.dumps({'error': message})}\n\n"

    def _event_stream(self, job_id: UUID, user_id: UUID):
        """
        Generate SSE events for NIPR job progress.

        Events:
        - progress: Job progress update (status, progress %, message)
        - completed: Job completed successfully with results
        - failed: Job failed with error message
        - timeout: Stream timeout (client should reconnect or handle)
        """
        start_time = time.time()

        while True:
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > SSE_MAX_DURATION:
                yield f"event: timeout\ndata: {json.dumps({'message': 'Stream timeout'})}\n\n"
                break

            # Fetch job status from database
            job = self._get_job_status(job_id)

            if not job:
                yield f"event: error\ndata: {json.dumps({'error': 'Job not found'})}\n\n"
                break

            # Build event data
            event_data = {
                'job_id': str(job_id),
                'status': job['status'],
                'progress': job['progress'],
                'progress_message': job['progress_message'],
                'queue_position': job['queue_position'],
            }

            # Handle terminal states
            if job['status'] == 'completed':
                event_data.update({
                    'result_files': job['result_files'],
                    'result_carriers': job['result_carriers'],
                    'completed_at': job['completed_at'],
                })
                yield f"event: completed\ndata: {json.dumps(event_data)}\n\n"
                break

            elif job['status'] == 'failed':
                event_data['error_message'] = job['error_message']
                yield f"event: failed\ndata: {json.dumps(event_data)}\n\n"
                break

            # Progress update
            yield f"event: progress\ndata: {json.dumps(event_data)}\n\n"

            # Wait before next poll
            time.sleep(SSE_POLL_INTERVAL)

    def _get_job_status(self, job_id: UUID) -> dict | None:
        """Fetch NIPR job status from database."""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    nj.id,
                    nj.status,
                    COALESCE(nj.progress, 0) as progress,
                    COALESCE(nj.progress_message, '') as progress_message,
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
            'status': row[1],
            'progress': row[2],
            'progress_message': row[3],
            'completed_at': row[4].isoformat() if row[4] else None,
            'result_files': row[5] or [],
            'result_carriers': row[6] or [],
            'error_message': row[7],
            'queue_position': row[8] if row[1] == 'pending' else None,
        }
