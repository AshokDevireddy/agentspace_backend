"""
Analytics API Views

Provides analytics-related endpoints:
- GET /api/analytics/split-view - Get analytics split by your deals vs downline
- GET /api/analytics/downline-distribution - Get production distribution for downlines
- GET /api/analytics/deals - Get agency-wide deal analytics
- GET /api/analytics/persistency - Get persistency analytics
"""
import logging
from datetime import datetime
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import (
    get_analytics_from_deals,
    get_analytics_split_view,
    get_downline_production_distribution,
    get_persistency_analytics,
)

logger = logging.getLogger(__name__)


class AnalyticsSplitView(APIView):
    """
    GET /api/analytics/split-view

    Get analytics data split between user's own deals and downline deals.

    Query params:
        as_of: Reference date (default: today, format: YYYY-MM-DD)
        all_time_months: Number of months for all-time window (default: 24)
        carrier_ids: Comma-separated carrier UUIDs to filter by
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
            # Parse as_of date
            as_of = None
            as_of_str = request.query_params.get('as_of')
            if as_of_str:
                try:
                    as_of = datetime.strptime(as_of_str, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid as_of format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Parse all_time_months
            all_time_months = int(request.query_params.get('all_time_months', 24))
            all_time_months = max(1, min(all_time_months, 120))  # Limit to 1-120 months

            # Parse carrier_ids
            carrier_ids = None
            carrier_ids_str = request.query_params.get('carrier_ids')
            if carrier_ids_str:
                try:
                    carrier_ids = [UUID(cid.strip()) for cid in carrier_ids_str.split(',') if cid.strip()]
                except ValueError:
                    return Response(
                        {'error': 'Invalid carrier_ids format. Use comma-separated UUIDs.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            result = get_analytics_split_view(
                user=user,
                as_of=as_of,
                all_time_months=all_time_months,
                carrier_ids=carrier_ids,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Analytics split view failed: {e}')
            return Response(
                {'error': 'Failed to fetch analytics', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DownlineDistributionView(APIView):
    """
    GET /api/analytics/downline-distribution

    Get production distribution for direct downlines.

    Query params:
        agent_id: Agent UUID to get downlines for (default: current user)
        start_date: Start date filter (format: YYYY-MM-DD)
        end_date: End date filter (format: YYYY-MM-DD)
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
            # Parse agent_id (default to current user)
            agent_id_str = request.query_params.get('agent_id')
            if agent_id_str:
                try:
                    agent_id = UUID(agent_id_str)
                except ValueError:
                    return Response(
                        {'error': 'Invalid agent_id format'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            else:
                agent_id = user.id

            # Parse start_date
            start_date = None
            start_date_str = request.query_params.get('start_date')
            if start_date_str:
                try:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid start_date format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Parse end_date
            end_date = None
            end_date_str = request.query_params.get('end_date')
            if end_date_str:
                try:
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid end_date format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            result = get_downline_production_distribution(
                agent_id=agent_id,
                start_date=start_date,
                end_date=end_date,
            )

            return Response({
                'distribution': result,
                'agent_id': str(agent_id),
            })

        except Exception as e:
            logger.error(f'Downline distribution failed: {e}')
            return Response(
                {'error': 'Failed to fetch distribution', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DealsAnalyticsView(APIView):
    """
    GET /api/analytics/deals

    Get agency-wide deal analytics.

    Query params:
        as_of: Reference date (default: today, format: YYYY-MM-DD)
        all_time_months: Number of months for all-time window (default: 24)
        carrier_ids: Comma-separated carrier UUIDs to filter by
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
            # Parse as_of date
            as_of = None
            as_of_str = request.query_params.get('as_of')
            if as_of_str:
                try:
                    as_of = datetime.strptime(as_of_str, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid as_of format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Parse all_time_months
            all_time_months = int(request.query_params.get('all_time_months', 24))
            all_time_months = max(1, min(all_time_months, 120))

            # Parse carrier_ids
            carrier_ids = None
            carrier_ids_str = request.query_params.get('carrier_ids')
            if carrier_ids_str:
                try:
                    carrier_ids = [UUID(cid.strip()) for cid in carrier_ids_str.split(',') if cid.strip()]
                except ValueError:
                    return Response(
                        {'error': 'Invalid carrier_ids format. Use comma-separated UUIDs.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            result = get_analytics_from_deals(
                agency_id=user.agency_id,
                as_of=as_of,
                all_time_months=all_time_months,
                carrier_ids=carrier_ids,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Deals analytics failed: {e}')
            return Response(
                {'error': 'Failed to fetch analytics', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PersistencyAnalyticsView(APIView):
    """
    GET /api/analytics/persistency

    Get persistency analytics for the agency.

    Query params:
        as_of: Reference date (default: today, format: YYYY-MM-DD)
        carrier_id: Optional carrier UUID to filter by
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
            # Parse as_of date
            as_of = None
            as_of_str = request.query_params.get('as_of')
            if as_of_str:
                try:
                    as_of = datetime.strptime(as_of_str, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid as_of format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Parse carrier_id
            carrier_id = None
            carrier_id_str = request.query_params.get('carrier_id')
            if carrier_id_str:
                try:
                    carrier_id = UUID(carrier_id_str)
                except ValueError:
                    return Response(
                        {'error': 'Invalid carrier_id format'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            result = get_persistency_analytics(
                agency_id=user.agency_id,
                as_of=as_of,
                carrier_id=carrier_id,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Persistency analytics failed: {e}')
            return Response(
                {'error': 'Failed to fetch persistency data', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
