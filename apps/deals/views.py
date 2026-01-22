"""
Deals API Views (P2-027, P2-028)

Provides deal-related endpoints:
- GET /api/deals/book-of-business - Get paginated deals
- GET /api/deals/filter-options - Get filter options
"""
import logging
from datetime import datetime
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import get_book_of_business, get_static_filter_options

logger = logging.getLogger(__name__)


class BookOfBusinessView(APIView):
    """
    GET /api/deals/book-of-business

    Get paginated book of business (deals) with keyset pagination.

    Query params:
        limit: Page size (default: 50)
        cursor_policy_effective_date: Cursor date for pagination
        cursor_id: Cursor ID for pagination
        carrier_id: Filter by carrier
        product_id: Filter by product
        agent_id: Filter by agent
        status: Filter by raw status
        status_standardized: Filter by standardized status
        date_from: Filter by policy effective date (from)
        date_to: Filter by policy effective date (to)
        search: Search by client name or policy number
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
            limit = int(request.query_params.get('limit', 50))
            limit = min(limit, 100)  # Cap at 100

            cursor_date = request.query_params.get('cursor_policy_effective_date')
            cursor_id = request.query_params.get('cursor_id')

            cursor_policy_effective_date = None
            if cursor_date:
                try:
                    cursor_policy_effective_date = datetime.strptime(cursor_date, '%Y-%m-%d').date()
                except ValueError:
                    return Response(
                        {'error': 'Invalid cursor_policy_effective_date format. Use YYYY-MM-DD.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            cursor_uuid = None
            if cursor_id:
                try:
                    cursor_uuid = UUID(cursor_id)
                except ValueError:
                    return Response(
                        {'error': 'Invalid cursor_id format.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Parse filter params
            carrier_id = request.query_params.get('carrier_id')
            if carrier_id:
                try:
                    carrier_id = UUID(carrier_id)
                except ValueError:
                    carrier_id = None

            product_id = request.query_params.get('product_id')
            if product_id:
                try:
                    product_id = UUID(product_id)
                except ValueError:
                    product_id = None

            agent_id = request.query_params.get('agent_id')
            if agent_id:
                try:
                    agent_id = UUID(agent_id)
                except ValueError:
                    agent_id = None

            status_filter = request.query_params.get('status')
            status_standardized = request.query_params.get('status_standardized')

            date_from = request.query_params.get('date_from')
            if date_from:
                try:
                    date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
                except ValueError:
                    date_from = None

            date_to = request.query_params.get('date_to')
            if date_to:
                try:
                    date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
                except ValueError:
                    date_to = None

            search_query = request.query_params.get('search', '').strip() or None

            is_admin = user.is_admin or user.role == 'admin'

            # Get book of business
            result = get_book_of_business(
                user=user,
                limit=limit,
                cursor_policy_effective_date=cursor_policy_effective_date,
                cursor_id=cursor_uuid,
                carrier_id=carrier_id,
                product_id=product_id,
                agent_id=agent_id,
                status=status_filter,
                status_standardized=status_standardized,
                date_from=date_from,
                date_to=date_to,
                search_query=search_query,
                include_full_agency=is_admin,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Book of business failed: {e}')
            return Response(
                {'error': 'Failed to fetch deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class FilterOptionsView(APIView):
    """
    GET /api/deals/filter-options

    Get filter options for deals (carriers, products, statuses, agents).
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
            options = get_static_filter_options(user)
            return Response(options)

        except Exception as e:
            logger.error(f'Filter options failed: {e}')
            return Response(
                {'error': 'Failed to fetch filter options', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
