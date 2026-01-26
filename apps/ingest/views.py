"""
Ingest API Views

Provides endpoints for policy report processing:
- POST /api/ingest/enqueue-job - Enqueue a policy report parse job
- POST /api/ingest/orchestrate - Run full policy report ingest
- POST /api/ingest/sync-staging - Sync staging to deals
- GET /api/ingest/staging-summary - Get staging summary
"""
import logging

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

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
