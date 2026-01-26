"""
Payouts API Views (P2-029)

Provides payout-related endpoints:
- GET /api/expected-payouts - Get expected commission payouts
- GET /api/expected-payouts/debt - Get agent debt
"""
import logging
from datetime import date, datetime
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import get_agent_debt, get_expected_payouts

logger = logging.getLogger(__name__)


def parse_date_param(value: str | None) -> date | None:
    """Parse a YYYY-MM-DD date string, returning None if invalid."""
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return None


def parse_uuid_param(value: str | None) -> UUID | None:
    """Parse a UUID string, returning None if invalid."""
    if not value:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


class ExpectedPayoutsView(APIView):
    """
    GET /api/expected-payouts

    Get expected commission payouts using historical hierarchy snapshots.
    Formula: annual_premium * 0.75 * (agent_commission_% / hierarchy_total_%)

    Query params:
        start_date: Filter by policy effective date (from, YYYY-MM-DD)
        end_date: Filter by policy effective date (to, YYYY-MM-DD)
        agent_id: Filter by specific agent
        carrier_id: Filter by carrier
        production_type: 'personal' (own deals), 'downline' (override commissions), or omit for all
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
            start_date = parse_date_param(request.query_params.get('start_date'))
            end_date = parse_date_param(request.query_params.get('end_date'))
            agent_id = parse_uuid_param(request.query_params.get('agent_id'))
            carrier_id = parse_uuid_param(request.query_params.get('carrier_id'))

            production_type = request.query_params.get('production_type')
            if production_type and production_type not in ('personal', 'downline'):
                production_type = None

            is_admin = user.is_admin or user.role == 'admin'

            result = get_expected_payouts(
                user=user,
                start_date=start_date,
                end_date=end_date,
                agent_id=agent_id,
                carrier_id=carrier_id,
                include_full_agency=is_admin,
                production_type=production_type,
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
            agent_id = parse_uuid_param(request.query_params.get('agent_id'))
            result = get_agent_debt(user=user, agent_id=agent_id)
            return Response(result)

        except Exception as e:
            logger.error(f'Agent debt failed: {e}')
            return Response(
                {'error': 'Failed to get debt', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
