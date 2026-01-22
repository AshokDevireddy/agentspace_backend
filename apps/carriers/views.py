"""
Carriers API Views

Provides carrier-related endpoints:
- GET /api/carriers - List all active carriers
- GET /api/carriers/names - Get carrier names for dropdowns
- GET /api/carriers/with-products - Get carriers with their products (agency-scoped)
"""
import logging

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    get_active_carriers,
    get_carrier_names,
    get_carriers_with_products_for_agency,
)

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
