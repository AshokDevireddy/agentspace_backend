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
    get_analytics_for_agent,
    get_analytics_from_deals,
    get_analytics_split_view,
    get_carrier_metrics,
    get_downline_production_distribution,
    get_persistency_analytics,
)

logger = logging.getLogger(__name__)


class AnalyticsSplitView(APIView):
    """
    GET /api/analytics/split-view

    Get analytics data split between user's own deals and downline deals.

    Query params:
        agent_id: Optional agent UUID to get analytics for (default: current user)
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
            # Parse agent_id (default to current user)
            agent_id_str = request.query_params.get('agent_id')
            target_user = user
            if agent_id_str:
                try:
                    target_agent_id = UUID(agent_id_str)
                    # Create a mock user context with the target agent ID
                    # This allows admins or managers to view analytics for their downlines
                    from dataclasses import replace
                    target_user = replace(user, id=target_agent_id)
                except ValueError:
                    return Response(
                        {'error': 'Invalid agent_id format'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

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
                user=target_user,
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

            # Calculate totals for frontend contract
            total_production = sum(entry.get('total_production', 0) for entry in result)
            total_deals = len(result)  # Each entry represents an agent's production

            return Response({
                'entries': result,
                'total_production': total_production,
                'total_deals': total_deals,
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


class AgentDealsAnalyticsView(APIView):
    """
    GET /api/analytics/agent-deals

    Get analytics for deals visible to a specific agent.
    Maps to Supabase RPC: get_analytics_from_deals_for_agent

    Query params:
        agent_id: Optional agent UUID (default: current user)
        as_of: Reference date (default: today, format: YYYY-MM-DD)
        all_time_months: Number of months for all-time window (default: 12)
        carrier_ids: Comma-separated carrier UUIDs to filter by
        include_series: Include monthly time series (default: true)
        include_windows_by_carrier: Include window calculations per carrier (default: true)
        include_breakdowns_status: Include status breakdown (default: true)
        include_breakdowns_state: Include state breakdown (default: true)
        include_breakdowns_age: Include age band breakdown (default: true)
        top_states: Number of top states to include (default: 50)
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
            target_user = user
            if agent_id_str:
                try:
                    target_agent_id = UUID(agent_id_str)
                    from dataclasses import replace
                    target_user = replace(user, id=target_agent_id)
                except ValueError:
                    return Response(
                        {'error': 'Invalid agent_id format'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

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
            all_time_months = int(request.query_params.get('all_time_months', 12))
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

            # Parse boolean flags
            def parse_bool(param_name: str, default: bool = True) -> bool:
                val = request.query_params.get(param_name)
                if val is None:
                    return default
                return val.lower() in ('true', '1', 'yes')

            include_series = parse_bool('include_series')
            include_windows_by_carrier = parse_bool('include_windows_by_carrier')
            include_breakdowns_status = parse_bool('include_breakdowns_status')
            include_breakdowns_state = parse_bool('include_breakdowns_state')
            include_breakdowns_age = parse_bool('include_breakdowns_age')

            # Parse top_states
            top_states = int(request.query_params.get('top_states', 50))
            top_states = max(1, min(top_states, 100))

            result = get_analytics_for_agent(
                user=target_user,
                as_of=as_of,
                all_time_months=all_time_months,
                carrier_ids=carrier_ids,
                include_series=include_series,
                include_windows_by_carrier=include_windows_by_carrier,
                include_breakdowns_status=include_breakdowns_status,
                include_breakdowns_state=include_breakdowns_state,
                include_breakdowns_age=include_breakdowns_age,
                top_states=top_states,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Agent deals analytics failed: {e}')
            return Response(
                {'error': 'Failed to fetch analytics', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CarrierMetricsView(APIView):
    """
    GET /api/analytics/carrier-metrics

    Get carrier-level metrics for the agency.
    Maps to Supabase RPC: get_carrier_metrics_json

    Query params:
        as_of: Reference date (default: today, format: YYYY-MM-DD)
        all_time_months: Number of months for all-time window (default: 12)
        carrier_ids: Comma-separated carrier UUIDs to filter by
        include_series: Include monthly time series (default: true)
        include_windows_by_carrier: Include window calculations per carrier (default: true)
        include_breakdowns_status: Include status breakdown (default: true)
        include_breakdowns_state: Include state breakdown (default: true)
        include_breakdowns_age: Include age band breakdown (default: true)
        top_states: Number of top states to include (default: 7)
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
            all_time_months = int(request.query_params.get('all_time_months', 12))
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

            # Parse boolean flags
            def parse_bool(param_name: str, default: bool = True) -> bool:
                val = request.query_params.get(param_name)
                if val is None:
                    return default
                return val.lower() in ('true', '1', 'yes')

            include_series = parse_bool('include_series')
            include_windows_by_carrier = parse_bool('include_windows_by_carrier')
            include_breakdowns_status = parse_bool('include_breakdowns_status')
            include_breakdowns_state = parse_bool('include_breakdowns_state')
            include_breakdowns_age = parse_bool('include_breakdowns_age')

            # Parse top_states
            top_states = int(request.query_params.get('top_states', 7))
            top_states = max(1, min(top_states, 100))

            result = get_carrier_metrics(
                agency_id=user.agency_id,
                as_of=as_of,
                all_time_months=all_time_months,
                carrier_ids=carrier_ids,
                include_series=include_series,
                include_windows_by_carrier=include_windows_by_carrier,
                include_breakdowns_status=include_breakdowns_status,
                include_breakdowns_state=include_breakdowns_state,
                include_breakdowns_age=include_breakdowns_age,
                top_states=top_states,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Carrier metrics failed: {e}')
            return Response(
                {'error': 'Failed to fetch carrier metrics', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
