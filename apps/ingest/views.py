"""
Ingest API Views

Provides endpoints for policy report processing:
- POST /api/ingest/enqueue-job - Enqueue a policy report parse job
- POST /api/ingest/orchestrate - Run full policy report ingest
- POST /api/ingest/sync-staging - Sync staging to deals
- GET /api/ingest/staging-summary - Get staging summary
- POST /api/ingest/presign - Generate S3 presigned URLs for file uploads
"""
import logging
import os
import re
import uuid as uuid_module

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import CronSecretAuthentication, SupabaseJWTAuthentication, get_user_context

from .services import (
    create_clients_from_deals,
    create_clients_from_policy_staging,
    create_products_from_staging,
    create_users_from_staging,
    create_writing_agent_numbers,
    dedupe_staging,
    enqueue_policy_report_parse_job,
    fill_agent_carrier_numbers_from_staging,
    fill_agent_carrier_numbers_with_audit,
    get_staging_summary,
    link_staged_agent_numbers,
    orchestrate_policy_report_ingest,
    sync_agent_carrier_numbers_from_staging,
    sync_policy_report_staging_to_deals,
    upsert_products_from_staging,
)

logger = logging.getLogger(__name__)


class EnqueueJobView(APIView):
    """
    POST /api/ingest/enqueue-job

    Enqueue a policy report parse job.
    Translated from Supabase RPC: enqueue_policy_report_parse_job

    Request body:
        bucket: Storage bucket name
        path: File path in bucket
        carrier: Carrier name
        priority: Job priority (optional, default 0)
        delay_sec: Delay in seconds (optional, default 0)
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Admin only
        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        bucket = request.data.get('bucket')
        path = request.data.get('path')
        carrier = request.data.get('carrier')

        if not bucket or not path or not carrier:
            return Response(
                {'error': 'bucket, path, and carrier are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            priority = int(request.data.get('priority', 0))
            delay_sec = int(request.data.get('delay_sec', 0))
        except (ValueError, TypeError):
            priority = 0
            delay_sec = 0

        try:
            msg_id = enqueue_policy_report_parse_job(
                bucket=bucket,
                path=path,
                carrier=carrier,
                agency_id=user.agency_id,
                priority=priority,
                delay_sec=delay_sec,
            )

            return Response({
                'success': True,
                'message_id': msg_id,
            })

        except Exception as e:
            logger.error(f'Enqueue job failed: {e}')
            return Response(
                {'error': 'Failed to enqueue job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OrchestrateIngestView(APIView):
    """
    POST /api/ingest/orchestrate

    Run full policy report ingest orchestration.
    Translated from Supabase RPC: orchestrate_policy_report_ingest_with_agency_id

    This runs the complete ingest pipeline:
    1. Dedupe staging rows
    2. Normalize staging data
    3. Create users from staging
    4. Create writing agent numbers
    5. Sync staging to deals
    6. Link/create clients
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Admin only
        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            result = orchestrate_policy_report_ingest(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Orchestrate ingest failed: {e}')
            return Response(
                {'ok': False, 'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SyncStagingView(APIView):
    """
    POST /api/ingest/sync-staging

    Sync policy report staging data to deals table.
    Translated from Supabase RPC: sync_policy_report_staging_to_deals_with_agency_id
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Admin only
        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            result = sync_policy_report_staging_to_deals(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Sync staging failed: {e}')
            return Response(
                {'ok': False, 'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class StagingSummaryView(APIView):
    """
    GET /api/ingest/staging-summary

    Get summary of policy report staging data.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            summary = get_staging_summary(user.agency_id)
            return Response(summary)

        except Exception as e:
            logger.error(f'Get staging summary failed: {e}')
            return Response(
                {'error': 'Failed to get staging summary', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CreateClientsFromDealsView(APIView):
    """
    POST /api/ingest/create-clients-from-deals

    Create client users from deal data.
    Translated from Supabase RPC: create_clients_from_deals_with_agency_id
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
            result = create_clients_from_deals(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Create clients from deals failed: {e}')
            return Response(
                {'error': 'Failed to create clients from deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CreateClientsFromStagingView(APIView):
    """
    POST /api/ingest/create-clients-from-staging

    Create client users from policy staging data.
    Translated from Supabase RPC: create_clients_from_policy_staging
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
            result = create_clients_from_policy_staging(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Create clients from staging failed: {e}')
            return Response(
                {'error': 'Failed to create clients from staging', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CreateUsersFromStagingView(APIView):
    """
    POST /api/ingest/create-users-from-staging

    Create agent users from policy report staging data.
    Translated from Supabase RPC: create_users_from_policy_report_staging_with_agency_id
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
            result = create_users_from_staging(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Create users from staging failed: {e}')
            return Response(
                {'error': 'Failed to create users from staging', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CreateProductsFromStagingView(APIView):
    """
    POST /api/ingest/create-products-from-staging

    Create products from policy report staging data.
    Translated from Supabase RPC: create_products_from_policy_report_staging_with_agency_id
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
            result = create_products_from_staging(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Create products from staging failed: {e}')
            return Response(
                {'error': 'Failed to create products from staging', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CreateWritingAgentNumbersView(APIView):
    """
    POST /api/ingest/create-writing-agent-numbers

    Create writing agent numbers from policy report staging.
    Translated from Supabase RPC: create_writing_agent_numbers_from_policy_report_staging_with_agency_id
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
            result = create_writing_agent_numbers(user.agency_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Create writing agent numbers failed: {e}')
            return Response(
                {'error': 'Failed to create writing agent numbers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DedupeStagingView(APIView):
    """
    POST /api/ingest/dedupe-staging

    Deduplicate policy report staging rows.
    Translated from Supabase RPC: dedupe_policy_report_staging_with_agency_id

    Request body:
        dry_run: If true, just report what would be deleted (default: false)
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

        dry_run = request.data.get('dry_run', False)

        try:
            result = dedupe_staging(user.agency_id, dry_run=dry_run)
            return Response(result)

        except Exception as e:
            logger.error(f'Dedupe staging failed: {e}')
            return Response(
                {'error': 'Failed to dedupe staging', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UpsertProductsView(APIView):
    """
    POST /api/ingest/upsert-products

    Upsert products from staging with logging.
    Translated from Supabase RPC: upsert_products_from_staging_logged

    Note: This processes all agencies, not agency-scoped.
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
            result = upsert_products_from_staging()
            return Response(result)

        except Exception as e:
            logger.error(f'Upsert products failed: {e}')
            return Response(
                {'error': 'Failed to upsert products', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class FillAgentCarrierNumbersView(APIView):
    """
    POST /api/ingest/fill-agent-carrier-numbers

    Fill agent carrier numbers from staging.
    Translated from Supabase RPC: fill_agent_carrier_numbers_from_staging
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
            result = fill_agent_carrier_numbers_from_staging()
            return Response(result)

        except Exception as e:
            logger.error(f'Fill agent carrier numbers failed: {e}')
            return Response(
                {'error': 'Failed to fill agent carrier numbers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class FillAgentCarrierNumbersWithAuditView(APIView):
    """
    POST /api/ingest/fill-agent-carrier-numbers-with-audit

    Fill agent carrier numbers with detailed audit trail.
    Translated from Supabase RPC: fill_agent_carrier_numbers_from_staging_with_audit
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
            result = fill_agent_carrier_numbers_with_audit()
            return Response(result)

        except Exception as e:
            logger.error(f'Fill agent carrier numbers with audit failed: {e}')
            return Response(
                {'error': 'Failed to fill agent carrier numbers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class LinkStagedAgentNumbersView(APIView):
    """
    POST /api/ingest/link-staged-agent-numbers

    Link staged agent numbers to users.
    Translated from Supabase RPC: link_staged_agent_numbers
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
            result = link_staged_agent_numbers()
            return Response(result)

        except Exception as e:
            logger.error(f'Link staged agent numbers failed: {e}')
            return Response(
                {'error': 'Failed to link staged agent numbers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SyncAgentCarrierNumbersView(APIView):
    """
    POST /api/ingest/sync-agent-carrier-numbers

    Sync agent carrier numbers from staging.
    Translated from Supabase RPC: sync_agent_carrier_numbers_from_staging
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
            result = sync_agent_carrier_numbers_from_staging()
            return Response(result)

        except Exception as e:
            logger.error(f'Sync agent carrier numbers failed: {e}')
            return Response(
                {'error': 'Failed to sync agent carrier numbers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# Ingest Job CRUD Views
# =============================================================================


class IngestJobsView(APIView):
    """
    GET /api/ingest/jobs - List ingest jobs for the user's agency
    POST /api/ingest/jobs - Create a new ingest job

    Query params (GET):
        days: Number of days to look back (default 30)
        limit: Maximum number of jobs (default 50)

    Request body (POST):
        expected_files: Number of expected files (required)
        client_job_id: Optional client-provided job ID for idempotency
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            days = int(request.query_params.get('days', 30))
            limit = int(request.query_params.get('limit', 50))
        except (ValueError, TypeError):
            days = 30
            limit = 50

        try:
            from .services import list_ingest_jobs, list_ingest_job_files

            jobs = list_ingest_jobs(
                agency_id=user.agency_id,
                days=days,
                limit=limit,
            )

            # Also fetch files for these jobs
            job_ids = [j['job_id'] for j in jobs]
            files = list_ingest_job_files(job_ids) if job_ids else []

            # Group files by job
            files_by_job = {}
            for f in files:
                job_id = f['job_id']
                if job_id not in files_by_job:
                    files_by_job[job_id] = []
                files_by_job[job_id].append(f)

            # Format response to match frontend expectation
            formatted_files = []
            for job in jobs:
                job_files = files_by_job.get(job['job_id'], [])
                for f in job_files:
                    formatted_files.append({
                        'id': f['file_id'],
                        'name': f['file_name'],
                        'job_id': f['job_id'],
                        'status': f['status'],
                        'parsed_rows': f['parsed_rows'],
                        'error_message': f['error_message'],
                        'created_at': f['created_at'],
                        'job_status': job['status'],
                        'job_expected_files': job['expected_files'],
                        'job_parsed_files': job['parsed_files'],
                        'job_created_at': job['created_at'],
                    })

            return Response({
                'files': formatted_files,
                'jobs': jobs,
            })

        except Exception as e:
            logger.error(f'List ingest jobs failed: {e}')
            return Response(
                {'error': 'Failed to list ingest jobs', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        expected_files = request.data.get('expected_files')
        client_job_id = request.data.get('client_job_id')

        if expected_files is None:
            return Response(
                {'error': 'expected_files is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            expected_files = int(expected_files)
            if expected_files < 0:
                raise ValueError('expected_files must be non-negative')
        except (ValueError, TypeError) as e:
            return Response(
                {'error': f'Invalid expected_files: {e}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            from .services import create_ingest_job

            job = create_ingest_job(
                agency_id=user.agency_id,
                expected_files=expected_files,
                client_job_id=client_job_id,
            )

            # Return 200 for existing job, 201 for new job
            is_existing = job.get('existing', False)
            status_code = status.HTTP_200_OK if is_existing else status.HTTP_201_CREATED

            response_data = {
                'job': {
                    'jobId': job['job_id'],
                    'agencyId': job['agency_id'],
                    'expectedFiles': job['expected_files'],
                    'parsedFiles': job['parsed_files'],
                    'status': job['status'],
                    'createdAt': job['created_at'],
                    'updatedAt': job['updated_at'],
                    'clientJobId': job['client_job_id'],
                }
            }
            if is_existing:
                response_data['existing'] = True

            return Response(response_data, status=status_code)

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_409_CONFLICT
            )
        except Exception as e:
            logger.error(f'Create ingest job failed: {e}')
            return Response(
                {'error': 'Failed to create ingest job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class IngestJobDetailView(APIView):
    """
    GET /api/ingest/jobs/{job_id} - Get a specific ingest job

    Returns job details including files.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            from uuid import UUID
            job_uuid = UUID(job_id)
        except (ValueError, TypeError):
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            from .services import get_ingest_job, list_ingest_job_files

            job = get_ingest_job(job_id=job_uuid, agency_id=user.agency_id)

            if not job:
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Also fetch files for this job
            files = list_ingest_job_files([job_uuid])

            return Response({
                'job': job,
                'files': files,
            })

        except Exception as e:
            logger.error(f'Get ingest job failed: {e}')
            return Response(
                {'error': 'Failed to get ingest job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# Policy Report Staging Views
# =============================================================================


class StagingBulkInsertView(APIView):
    """
    POST /api/ingest/staging/bulk - Bulk insert staging records

    Request body:
        records: Array of staging records to insert
            Each record should have:
            - client_name, policy_number, writing_agent_number, agent_name
            - status, policy_effective_date, carrier_name
            - Optional: product, date_of_birth, issue_age, face_value, etc.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        records = request.data.get('records', [])

        if not records:
            return Response(
                {'error': 'No records provided'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(records, list):
            return Response(
                {'error': 'records must be an array'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate each record has minimum required fields
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                return Response(
                    {'error': f'Record {i} is not an object'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not record.get('carrier_name'):
                return Response(
                    {'error': f'Record {i} missing carrier_name'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        try:
            from .services import bulk_insert_staging_records

            result = bulk_insert_staging_records(
                agency_id=user.agency_id,
                records=records,
            )

            if result['success']:
                return Response({
                    'success': True,
                    'insertedCount': result['inserted_count'],
                })
            else:
                return Response(
                    {'error': result.get('error', 'Insert failed')},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f'Bulk insert staging failed: {e}')
            return Response(
                {'error': 'Failed to insert staging records', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UpsertJobFileView(APIView):
    """
    POST /api/ingest/jobs/{job_id}/files - Upsert a file record for an ingest job

    Request body:
        file_id: UUID for the file
        file_name: Original file name
        status: File status (default 'received')
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, job_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            from uuid import UUID
            job_uuid = UUID(job_id)
        except (ValueError, TypeError):
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        file_id = request.data.get('file_id')
        file_name = request.data.get('file_name')
        file_status = request.data.get('status', 'received')

        if not file_id or not file_name:
            return Response(
                {'error': 'file_id and file_name are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            from .services import verify_job_exists, upsert_ingest_job_file

            # Verify job exists and belongs to user's agency
            if not verify_job_exists(job_uuid, user.agency_id):
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Upsert the file record
            result = upsert_ingest_job_file(
                job_id=job_uuid,
                file_id=file_id,
                file_name=file_name,
                status=file_status,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Upsert job file failed: {e}')
            return Response(
                {'error': 'Failed to upsert file', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class VerifyJobExistsView(APIView):
    """
    GET /api/ingest/jobs/{job_id}/verify - Verify a job exists

    Returns 200 if job exists, 404 if not.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            from uuid import UUID
            job_uuid = UUID(job_id)
        except (ValueError, TypeError):
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            from .services import verify_job_exists

            if verify_job_exists(job_uuid, user.agency_id):
                return Response({'exists': True})
            else:
                return Response(
                    {'error': 'Job not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            logger.error(f'Verify job exists failed: {e}')
            return Response(
                {'error': 'Failed to verify job', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class StagingRecordsView(APIView):
    """
    GET /api/ingest/staging - Get staging records

    Query params:
        carrier: Filter by carrier name (optional)
        limit: Maximum records to return (default 100)
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        carrier = request.query_params.get('carrier')
        try:
            limit = int(request.query_params.get('limit', 100))
        except (ValueError, TypeError):
            limit = 100

        try:
            from .services import get_staging_records

            records = get_staging_records(
                agency_id=user.agency_id,
                carrier=carrier,
                limit=limit,
            )

            return Response({
                'success': True,
                'agencyId': str(user.agency_id),
                'carrier': carrier or 'all',
                'records': records,
                'count': len(records),
            })

        except Exception as e:
            logger.error(f'Get staging records failed: {e}')
            return Response(
                {'error': 'Failed to fetch records', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# =============================================================================
# S3 Presigned URL Views (migrated from Next.js)
# =============================================================================


def _sanitize_filename(filename: str) -> str:
    """Sanitize filename to be safe for S3 keys."""
    return re.sub(r'[^a-zA-Z0-9._-]', '_', filename)


class S3PresignView(APIView):
    """
    POST /api/ingest/presign

    Generate S3 presigned URLs for file uploads.
    Migrated from frontend/src/app/api/upload-policy-reports/sign/route.ts

    Request body:
        {
            "jobId": "uuid",
            "files": [
                {
                    "fileName": "report.csv",
                    "contentType": "text/csv",
                    "size": 12345
                }
            ]
        }

    Response (200):
        {
            "jobId": "uuid",
            "files": [
                {
                    "fileId": "uuid",
                    "fileName": "report.csv",
                    "objectKey": "policy-reports/{agency}/{job}/{file}/report.csv",
                    "presignedUrl": "https://...",
                    "contentType": "text/csv",
                    "size": 12345,
                    "expiresInSeconds": 60
                }
            ]
        }
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    # Allowed content types for upload
    ALLOWED_TYPES = [
        'text/csv',
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ]
    MAX_FILE_SIZE = 25 * 1024 * 1024  # 25MB per file

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if not user.agency_id:
            return Response(
                {'error': 'No agency for user'},
                status=status.HTTP_400_BAD_REQUEST
            )

        job_id = request.data.get('jobId')
        files = request.data.get('files', [])

        if not job_id or not files:
            return Response(
                {'error': 'jobId and files are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate job_id format
        try:
            from uuid import UUID
            job_uuid = UUID(job_id)
        except (ValueError, TypeError):
            return Response(
                {'error': 'Invalid jobId format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Verify job exists and belongs to user's agency
        try:
            from .services import verify_job_exists
            if not verify_job_exists(job_uuid, user.agency_id):
                return Response(
                    {'error': 'Invalid jobId'},
                    status=status.HTTP_400_BAD_REQUEST
                )
        except Exception as e:
            logger.error(f'Failed to verify job: {e}')
            return Response(
                {'error': 'Failed to verify job'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Get S3 bucket from environment
        bucket = os.getenv('AWS_S3_BUCKET_NAME')
        if not bucket:
            return Response(
                {'error': 'AWS_S3_BUCKET_NAME not configured'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Initialize boto3 S3 client
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                region_name=os.getenv('AWS_REGION', 'us-east-1'),
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            )
        except Exception as e:
            logger.error(f'Failed to initialize S3 client: {e}')
            return Response(
                {'error': 'S3 service unavailable'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        results = []

        for file_info in files:
            file_name = str(file_info.get('fileName', ''))
            content_type = str(file_info.get('contentType', ''))
            size = file_info.get('size', 0)

            if not file_name or not content_type:
                return Response(
                    {'error': 'Each file requires fileName and contentType'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            try:
                size = int(size)
            except (ValueError, TypeError):
                size = 0

            # Validate content type
            if content_type not in self.ALLOWED_TYPES:
                return Response(
                    {'error': f'Invalid file type: {content_type}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate file size
            if size > self.MAX_FILE_SIZE:
                return Response(
                    {'error': f'File too large: {file_name}'},
                    status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
                )

            # Generate file ID and S3 key
            file_id = str(uuid_module.uuid4())
            safe_name = _sanitize_filename(file_name)
            object_key = f'policy-reports/{user.agency_id}/{job_id}/{file_id}/{safe_name}'

            # Upsert ingest_job_file record
            try:
                from .services import upsert_ingest_job_file
                upsert_ingest_job_file(
                    job_id=job_uuid,
                    file_id=file_id,
                    file_name=file_name,
                    status='received',
                )
            except Exception as e:
                logger.error(f'Failed to register file: {e}')
                return Response(
                    {'error': 'Failed to register file'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # Generate presigned URL
            try:
                presigned_url = s3_client.generate_presigned_url(
                    'put_object',
                    Params={
                        'Bucket': bucket,
                        'Key': object_key,
                        'ContentType': content_type,
                    },
                    ExpiresIn=60,  # 60 seconds
                )
            except Exception as e:
                logger.error(f'Failed to generate presigned URL: {e}')
                return Response(
                    {'error': 'Failed to generate upload URL'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            results.append({
                'fileId': file_id,
                'fileName': file_name,
                'objectKey': object_key,
                'presignedUrl': presigned_url,
                'contentType': content_type,
                'size': size,
                'expiresInSeconds': 60,
            })

        return Response({
            'jobId': job_id,
            'files': results,
        })


# =============================================================================
# Supabase Storage Views (Policy Reports by Carrier)
# =============================================================================


class PolicyReportUploadView(APIView):
    """
    POST /api/ingest/policy-report-upload

    Upload policy report files organized by carrier.
    This replaces the frontend direct Supabase Storage access.

    Expects multipart/form-data with files keyed as "carrier_{CarrierName}".
    Each carrier's existing files are replaced with the new upload.

    Response (200):
        {
            "success": true,
            "message": "Successfully uploaded N file(s)",
            "agencyId": "uuid",
            "totalFilesReplaced": 0,
            "results": [...],
            "errors": []
        }
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User authentication failed'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if not user.agency_id:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User is not associated with an agency'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        from .storage_service import replace_carrier_files, validate_file

        # Parse files from request
        uploads = []
        for key, file in request.FILES.items():
            # Extract carrier name from key (format: "carrier_{CarrierName}")
            if key.startswith('carrier_'):
                carrier_name = key[8:]  # Remove "carrier_" prefix
                uploads.append({
                    'carrier': carrier_name,
                    'file': file,
                })

        if not uploads:
            return Response(
                {'error': 'No files uploaded', 'detail': 'Please upload at least one policy report file'},
                status=status.HTTP_400_BAD_REQUEST
            )

        results = []
        errors = []
        total_files_replaced = 0

        for upload in uploads:
            carrier = upload['carrier']
            file = upload['file']

            # Validate file
            validation_error = validate_file(
                content_type=file.content_type,
                size=file.size,
                file_name=file.name,
            )
            if validation_error:
                errors.append(f'{carrier}: {validation_error}')
                continue

            # Read file content
            try:
                file_content = file.read()
            except Exception as e:
                errors.append(f'{carrier}: Failed to read file - {e}')
                continue

            # Upload (replacing existing files)
            result = replace_carrier_files(
                agency_id=user.agency_id,
                carrier_name=carrier,
                file_content=file_content,
                file_name=file.name,
                content_type=file.content_type,
            )

            if result.success:
                results.append({
                    'carrier': carrier,
                    'fileName': result.file_name,
                    'storagePath': result.storage_path,
                    'size': result.size,
                    'type': result.content_type,
                })
            else:
                errors.append(f'{carrier}: {result.error}')

        success = len(errors) == 0
        response_data = {
            'success': success,
            'message': (
                f"Successfully uploaded {len(results)} file(s)"
                if success else 'Some files failed to upload'
            ),
            'agencyId': str(user.agency_id),
            'totalFilesReplaced': total_files_replaced,
            'results': results,
            'errors': errors,
        }

        return Response(
            response_data,
            status=status.HTTP_200_OK if success else status.HTTP_207_MULTI_STATUS
        )


class PolicyReportFilesView(APIView):
    """
    GET /api/ingest/policy-report-files - List policy report files
    DELETE /api/ingest/policy-report-files - Delete policy report files

    Query params (GET):
        carrier: Optional carrier name to filter by

    Request body (DELETE):
        paths: List of storage paths to delete
        OR
        carrier: Carrier name to delete all files for

    Response (GET 200):
        {
            "success": true,
            "agencyId": "uuid",
            "files": [...]
        }
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User authentication failed'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if not user.agency_id:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User is not associated with an agency'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        from .storage_service import list_agency_files, list_carrier_files

        carrier = request.query_params.get('carrier')

        if carrier:
            result = list_carrier_files(
                agency_id=user.agency_id,
                carrier_name=carrier,
            )
        else:
            result = list_agency_files(agency_id=user.agency_id)

        if not result.success:
            return Response(
                {'error': 'Failed to list files', 'detail': result.error},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response({
            'success': True,
            'agencyId': str(user.agency_id),
            'files': result.files,
        })

    def delete(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User authentication failed'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if not user.agency_id:
            return Response(
                {'error': 'Unauthorized', 'detail': 'User is not associated with an agency'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        from .storage_service import delete_file, delete_carrier_folder

        paths = request.data.get('paths', [])
        carrier = request.data.get('carrier')

        if carrier:
            # Delete all files for carrier
            result = delete_carrier_folder(
                agency_id=user.agency_id,
                carrier_name=carrier,
            )

            if not result.success:
                return Response(
                    {'error': 'Failed to delete files', 'detail': result.error},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            return Response({
                'success': True,
                'deletedCount': result.deleted_count,
            })

        elif paths:
            # Delete specific files
            deleted_count = 0
            errors = []

            for path in paths:
                result = delete_file(path)
                if result.success:
                    deleted_count += 1
                else:
                    errors.append(f'{path}: {result.error}')

            return Response({
                'success': len(errors) == 0,
                'deletedCount': deleted_count,
                'errors': errors,
            })

        else:
            return Response(
                {'error': 'Either paths or carrier is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
