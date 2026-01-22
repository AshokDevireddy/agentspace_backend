"""
Payouts API Views (P2-029)

Provides payout-related endpoints:
- GET /api/expected-payouts - Get expected commission payouts
- GET /api/expected-payouts/debt - Get agent debt
"""
import logging
from datetime import datetime
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import get_expected_payouts, get_agent_debt

logger = logging.getLogger(__name__)


class ExpectedPayoutsView(APIView):
    """
    GET /api/expected-payouts

    Get expected commission payouts based on deals and commission rates.

    Query params:
        start_date: Filter by policy effective date (from, YYYY-MM-DD)
        end_date: Filter by policy effective date (to, YYYY-MM-DD)
        agent_id: Filter by specific agent
        carrier_id: Filter by carrier
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
            # Parse query params
            start_date = request.query_params.get('start_date')
            if start_date:
                try:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid start_date format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            end_date = request.query_params.get('end_date')
            if end_date:
                try:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid end_date format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            agent_id = request.query_params.get('agent_id')
            if agent_id:
                try:
                    agent_id = UUID(agent_id)
                except ValueError:
                    agent_id = None

            carrier_id = request.query_params.get('carrier_id')
            if carrier_id:
                try:
                    carrier_id = UUID(carrier_id)
                except ValueError:
                    carrier_id = None

            is_admin = user.is_admin or user.role == 'admin'

            result = get_expected_payouts(
                user=user,
                start_date=start_date,
                end_date=end_date,
                agent_id=agent_id,
                carrier_id=carrier_id,
                include_full_agency=is_admin,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Expected payouts failed: {e}')
            return Response(
                {'error': 'Failed to calculate payouts', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AgentDebtView(APIView):
    """
    GET /api/expected-payouts/debt

    Get agent debt from chargebacks, lapses, etc.

    Query params:
        agent_id: Filter by specific agent (defaults to current user)
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
            agent_id = request.query_params.get('agent_id')
            if agent_id:
                try:
                    agent_id = UUID(agent_id)
                except ValueError:
                    agent_id = None

            result = get_agent_debt(user=user, agent_id=agent_id)

            return Response(result)

        except Exception as e:
            logger.error(f'Agent debt failed: {e}')
            return Response(
                {'error': 'Failed to get debt', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
