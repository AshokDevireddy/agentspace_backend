"""
Dashboard API Views

Provides dashboard endpoints that mirror Supabase RPC functions:
- /api/dashboard/summary -> get_dashboard_data_with_agency_id
- /api/dashboard/scoreboard -> get_scoreboard_data
- /api/dashboard/production -> get_agents_debt_production
"""
import logging
from datetime import date, datetime

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .services import (
    get_user_context_from_auth_id,
    get_dashboard_summary,
    get_scoreboard_data,
    get_scoreboard_lapsed_deals,
    get_scoreboard_with_billing_cycle,
    get_production_data,
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date | None:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return None


class DashboardSummaryView(APIView):
    """
    GET /api/dashboard/summary

    Get dashboard summary data (active policies, clients, carriers breakdown).

    Query params:
        as_of_date: Optional date (YYYY-MM-DD) for calculations (default: today)

    Response (200):
        {
            "your_deals": {
                "active_policies": 42,
                "monthly_commissions": 1234.56,
                "new_policies": 5,
                "total_clients": 30,
                "carriers_active": [
                    { "carrier_id": "uuid", "carrier": "Carrier Name", "active_policies": 10 }
                ]
            },
            "downline_production": { ... same structure ... },
            "totals": {
                "pending_positions": 3
            }
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get user context from auth_user_id
        user_ctx = get_user_context_from_auth_id(user.auth_user_id)
        if not user_ctx:
            return Response(
                {'error': 'NotFound', 'message': 'User not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Parse optional as_of_date parameter
        as_of_date_str = request.query_params.get('as_of_date')
        as_of_date = parse_date(as_of_date_str) if as_of_date_str else None

        try:
            data = get_dashboard_summary(user_ctx, as_of_date)
            return Response(data)
        except Exception as e:
            logger.error(f'Dashboard summary failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get dashboard data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ScoreboardView(APIView):
    """
    GET /api/dashboard/scoreboard

    Get scoreboard/leaderboard data for the agency.

    Query params:
        start_date: Required date (YYYY-MM-DD)
        end_date: Required date (YYYY-MM-DD)

    Response (200):
        {
            "success": true,
            "data": {
                "leaderboard": [
                    {
                        "rank": 1,
                        "agent_id": "uuid",
                        "name": "John Doe",
                        "total": 12345.67,
                        "dailyBreakdown": { "2024-01-15": 500.00 },
                        "dealCount": 5
                    }
                ],
                "stats": {
                    "totalProduction": 50000.00,
                    "totalDeals": 25,
                    "activeAgents": 10
                },
                "dateRange": {
                    "startDate": "2024-01-01",
                    "endDate": "2024-01-31"
                }
            }
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get user context from auth_user_id
        user_ctx = get_user_context_from_auth_id(user.auth_user_id)
        if not user_ctx:
            return Response(
                {'error': 'NotFound', 'message': 'User not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Parse required date parameters
        start_date_str = request.query_params.get('start_date')
        end_date_str = request.query_params.get('end_date')

        if not start_date_str or not end_date_str:
            return Response(
                {'error': 'ValidationError', 'message': 'start_date and end_date are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {'error': 'ValidationError', 'message': 'Invalid date format. Use YYYY-MM-DD'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            data = get_scoreboard_data(user_ctx, start_date, end_date)
            return Response(data)
        except Exception as e:
            logger.error(f'Scoreboard failed: {e}')
            return Response(
                {'success': False, 'error': 'Failed to get scoreboard data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ProductionView(APIView):
    """
    GET /api/dashboard/production

    Get production metrics for specified agents.

    Query params:
        agent_ids: Required comma-separated list of agent UUIDs
        start_date: Required date (YYYY-MM-DD)
        end_date: Required date (YYYY-MM-DD)

    Response (200):
        [
            {
                "agent_id": "uuid",
                "individual_production": 5000.00,
                "individual_production_count": 3,
                "hierarchy_production": 15000.00,
                "hierarchy_production_count": 10
            }
        ]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get user context from auth_user_id
        user_ctx = get_user_context_from_auth_id(user.auth_user_id)
        if not user_ctx:
            return Response(
                {'error': 'NotFound', 'message': 'User not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Parse required parameters
        agent_ids_str = request.query_params.get('agent_ids', '')
        start_date_str = request.query_params.get('start_date')
        end_date_str = request.query_params.get('end_date')

        if not agent_ids_str:
            return Response(
                {'error': 'ValidationError', 'message': 'agent_ids is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not start_date_str or not end_date_str:
            return Response(
                {'error': 'ValidationError', 'message': 'start_date and end_date are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {'error': 'ValidationError', 'message': 'Invalid date format. Use YYYY-MM-DD'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse agent IDs
        agent_ids = [aid.strip() for aid in agent_ids_str.split(',') if aid.strip()]
        if not agent_ids:
            return Response(
                {'error': 'ValidationError', 'message': 'No valid agent_ids provided'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            data = get_production_data(user_ctx, agent_ids, start_date, end_date)
            return Response(data)
        except Exception as e:
            logger.error(f'Production data failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get production data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ScoreboardLapsedView(APIView):
    """
    GET /api/dashboard/scoreboard-lapsed

    Get scoreboard data with updated lapsed deals calculation.
    Translated from Supabase RPC: get_scoreboard_data_updated_lapsed_deals

    Query params:
        start_date: Required date (YYYY-MM-DD)
        end_date: Required date (YYYY-MM-DD)
        assumed_months_till_lapse: Optional int (default: 0)
        scope: Optional 'agency' or 'downline' (default: 'agency')
        submitted: Optional boolean (default: false)

    Response (200):
        {
            "success": true,
            "data": {
                "leaderboard": [...],
                "stats": { "totalProduction": ..., "totalDeals": ..., "activeAgents": ... },
                "dateRange": { "startDate": "...", "endDate": "..." }
            }
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get user context from auth_user_id
        user_ctx = get_user_context_from_auth_id(user.auth_user_id)
        if not user_ctx:
            return Response(
                {'success': False, 'error': 'User not associated with an agency'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Parse required date parameters
        start_date_str = request.query_params.get('start_date')
        end_date_str = request.query_params.get('end_date')

        if not start_date_str or not end_date_str:
            return Response(
                {'success': False, 'error': 'start_date and end_date are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse optional parameters
        try:
            assumed_months = int(request.query_params.get('assumed_months_till_lapse', 0))
        except (ValueError, TypeError):
            assumed_months = 0

        scope = request.query_params.get('scope', 'agency')
        if scope not in ('agency', 'downline'):
            scope = 'agency'

        submitted_str = request.query_params.get('submitted', 'false').lower()
        submitted = submitted_str in ('true', '1', 'yes')

        try:
            data = get_scoreboard_lapsed_deals(
                user_ctx,
                start_date,
                end_date,
                assumed_months_till_lapse=assumed_months,
                scope=scope,
                submitted=submitted,
            )
            return Response(data)
        except Exception as e:
            logger.error(f'Scoreboard lapsed failed: {e}')
            return Response(
                {'success': False, 'error': 'Failed to get scoreboard data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ScoreboardBillingCycleView(APIView):
    """
    GET /api/dashboard/scoreboard-billing-cycle

    Get scoreboard data with billing cycle payment calculation.

    This endpoint mirrors the frontend /api/scoreboard logic, calculating
    recurring payments based on billing_cycle (monthly, quarterly, etc.)
    and aggregating by payment date rather than deal date.

    Query params:
        start_date: Required date (YYYY-MM-DD)
        end_date: Required date (YYYY-MM-DD)
        scope: Optional 'agency' or 'downline' (default: 'agency')

    Response (200):
        {
            "success": true,
            "data": {
                "leaderboard": [
                    {
                        "rank": 1,
                        "agent_id": "uuid",
                        "name": "John Doe",
                        "total": 12345.67,
                        "dailyBreakdown": { "2024-01-15": 500.00 },
                        "dealCount": 5
                    }
                ],
                "stats": {
                    "totalProduction": 50000.00,
                    "totalDeals": 25,
                    "activeAgents": 10
                },
                "dateRange": {
                    "startDate": "2024-01-01",
                    "endDate": "2024-01-31"
                }
            }
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get user context from auth_user_id
        user_ctx = get_user_context_from_auth_id(user.auth_user_id)
        if not user_ctx:
            return Response(
                {'success': False, 'error': 'User not associated with an agency'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Parse required date parameters
        start_date_str = request.query_params.get('start_date')
        end_date_str = request.query_params.get('end_date')

        if not start_date_str or not end_date_str:
            return Response(
                {'success': False, 'error': 'start_date and end_date are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Parse optional scope parameter
        scope = request.query_params.get('scope', 'agency')
        if scope not in ('agency', 'downline'):
            scope = 'agency'

        try:
            data = get_scoreboard_with_billing_cycle(
                user_ctx,
                start_date,
                end_date,
                scope=scope,
            )
            return Response(data)
        except Exception as e:
            logger.error(f'Scoreboard billing cycle failed: {e}')
            return Response(
                {'success': False, 'error': 'Failed to get scoreboard data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
