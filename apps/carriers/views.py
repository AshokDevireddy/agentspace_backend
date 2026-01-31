"""
Carriers API Views

Provides carrier-related endpoints:
- GET /api/carriers - List all active carriers
- GET /api/carriers/names - Get carrier names for dropdowns
- GET /api/carriers/with-products - Get carriers with their products (agency-scoped)
- POST /api/carriers/logins - Create/update carrier portal credentials
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import (
    get_active_carriers,
    get_carrier_by_id,
    get_carrier_names,
    get_carriers_for_agency,
    get_carriers_with_products_for_agency,
    get_contracts_paginated,
    get_standardized_statuses,
    get_status_mappings,
)
from .services import create_or_update_carrier_login

logger = logging.getLogger(__name__)


class CarriersListView(APIView):
    """
    GET /api/carriers

    Get all active carriers ordered by display name.

    Response (200):
        [
            {
                "id": "uuid",
                "name": "Carrier Name",
                "display_name": "Display Name",
                "is_active": true,
                "created_at": "2024-01-01T00:00:00Z"
            }
        ]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            carriers = get_active_carriers()
            return Response(carriers)
        except Exception as e:
            logger.error(f'Carriers list failed: {e}')
            return Response(
                {'error': 'Failed to fetch carriers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CarrierDetailView(APIView):
    """
    GET /api/carriers/{id}

    Get a single carrier by ID.

    Response (200):
        {
            "id": "uuid",
            "name": "Carrier Name",
            "display_name": "Display Name",
            "code": "CODE",
            "is_active": true,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, carrier_id):
        try:
            carrier_uuid = UUID(carrier_id)
        except ValueError:
            return Response(
                {'error': 'Invalid carrier_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            carrier = get_carrier_by_id(carrier_uuid)

            if not carrier:
                return Response(
                    {'error': 'Carrier not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response(carrier)
        except Exception as e:
            logger.error(f'Carrier detail failed: {e}')
            return Response(
                {'error': 'Failed to fetch carrier', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CarrierNamesView(APIView):
    """
    GET /api/carriers/names

    Get carrier names for dropdown/select components.

    Response (200):
        [
            {"id": "uuid", "name": "Carrier Name"}
        ]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            carriers = get_carrier_names()
            return Response(carriers)
        except Exception as e:
            logger.error(f'Carrier names failed: {e}')
            return Response(
                {'error': 'Failed to fetch carrier names', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AgencyCarriersView(APIView):
    """
    GET /api/carriers/agency (P2-030)

    Get carriers associated with the user's agency.
    Returns a lightweight list of carriers that have products for the agency.

    Response (200):
        [
            {
                "id": "uuid",
                "name": "Carrier Name",
                "display_name": "Display Name",
                "is_active": true
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

        try:
            carriers = get_carriers_for_agency(user.agency_id)
            return Response(carriers)
        except Exception as e:
            logger.error(f'Agency carriers failed: {e}')
            return Response(
                {'error': 'Failed to fetch agency carriers', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CarriersWithProductsView(APIView):
    """
    GET /api/carriers/with-products

    Get carriers that have products for the user's agency.

    Response (200):
        [
            {
                "id": "uuid",
                "name": "Carrier Name",
                "display_name": "Display Name",
                "is_active": true,
                "products": [
                    {
                        "id": "uuid",
                        "name": "Product Name",
                        "product_code": "CODE",
                        "is_active": true,
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ]
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

        try:
            carriers = get_carriers_with_products_for_agency(user.agency_id)
            return Response(carriers)
        except Exception as e:
            logger.error(f'Carriers with products failed: {e}')
            return Response(
                {'error': 'Failed to fetch carriers with products', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class StatusMappingsView(APIView):
    """
    GET /api/carriers/statuses (P1-020)

    Get status mappings for all carriers or a specific carrier.

    Query params:
        carrier_id: Optional carrier UUID to filter by

    Response (200):
        {
            "statuses": [
                {
                    "id": "uuid",
                    "carrier_id": "uuid",
                    "carrier_name": "Carrier Name",
                    "raw_status": "ACTIVE",
                    "standardized_status": "active",
                    "impact": "positive"
                }
            ]
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from uuid import UUID

        carrier_id = request.query_params.get('carrier_id')
        carrier_uuid = None

        if carrier_id:
            try:
                carrier_uuid = UUID(carrier_id)
            except ValueError:
                return Response(
                    {'error': 'Invalid carrier_id format'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        try:
            statuses = get_status_mappings(carrier_uuid)
            return Response({'statuses': statuses})
        except Exception as e:
            logger.error(f'Status mappings failed: {e}')
            return Response(
                {'error': 'Failed to fetch status mappings', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class StandardizedStatusesView(APIView):
    """
    GET /api/carriers/standardized-statuses (P1-020)

    Get list of standardized status values.

    Response (200):
        {
            "statuses": [
                {
                    "value": "active",
                    "label": "Active",
                    "impact": "positive",
                    "description": "Policy is active and in force"
                }
            ]
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            statuses = get_standardized_statuses()
            return Response({'statuses': statuses})
        except Exception as e:
            logger.error(f'Standardized statuses failed: {e}')
            return Response(
                {'error': 'Failed to fetch standardized statuses', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CarrierLoginsView(APIView):
    """
    POST /api/carriers/logins

    Create or update carrier portal login credentials.
    Admin-only endpoint for managing parsing_info records.

    Request body:
        {
            "carrier_name": "Carrier Name",
            "login": "username@email.com",
            "password": "secret_password"
        }

    Response (201):
        {
            "success": true,
            "data": {
                "id": "uuid",
                "created_at": "2024-01-01T00:00:00Z"
            }
        }
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Verify user is admin
        if not user.is_admin:
            return Response(
                {'error': 'Forbidden', 'detail': 'Admin access required to save carrier logins'},
                status=status.HTTP_403_FORBIDDEN
            )

        data = request.data
        carrier_name = data.get('carrier_name')
        login = data.get('login')
        password = data.get('password')

        # Validate required fields
        if not carrier_name:
            return Response(
                {'error': 'Missing required fields', 'detail': 'carrier_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not login:
            return Response(
                {'error': 'Missing required fields', 'detail': 'login is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not password:
            return Response(
                {'error': 'Missing required fields', 'detail': 'password is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = create_or_update_carrier_login(
                user_id=user.id,
                agency_id=user.agency_id,
                carrier_name=carrier_name,
                login=login,
                password=password,
            )

            if not result.get('success'):
                return Response(
                    {'error': result.get('error', 'Failed to save carrier login')},
                    status=status.HTTP_400_BAD_REQUEST
                )

            return Response(
                {'success': True, 'data': result.get('data')},
                status=status.HTTP_201_CREATED
            )

        except Exception as e:
            logger.error(f'Carrier login save failed: {e}')
            return Response(
                {'error': 'Failed to save carrier login', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ContractsListView(APIView):
    """
    GET /api/contracts

    List agent carrier numbers (contracts) with pagination.
    Returns contracts for the user's agency.

    Query params:
        page: Page number (default: 1)
        limit: Items per page (default: 20, max: 100)

    Response (200):
        {
            "contracts": [
                {
                    "id": "uuid",
                    "carrier": "Carrier Name",
                    "agent": "Agent Name",
                    "loa": "LOA info",
                    "status": "Active",
                    "startDate": "Jan 1, 2024",
                    "agentNumber": "12345"
                }
            ],
            "pagination": {
                "currentPage": 1,
                "totalPages": 5,
                "totalCount": 100,
                "limit": 20,
                "hasNextPage": true,
                "hasPrevPage": false
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

        # Parse pagination params
        try:
            page = max(1, int(request.query_params.get('page', 1)))
        except ValueError:
            page = 1

        try:
            limit = min(100, max(1, int(request.query_params.get('limit', 20))))
        except ValueError:
            limit = 20

        try:
            result = get_contracts_paginated(
                agency_id=user.agency_id,
                page=page,
                limit=limit,
            )
            return Response(result)
        except Exception as e:
            logger.error(f'Contracts list failed: {e}')
            return Response(
                {'error': 'Failed to fetch contracts', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
