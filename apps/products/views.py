"""
Products API Views

Provides product-related endpoints:
- GET /api/products - List products for a carrier
- GET /api/products/all - List all products for user's agency
- GET /api/products/{id} - Get single product
- POST /api/products - Create a new product
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    get_products_for_carrier,
    get_all_products_for_agency,
    get_product_by_id,
)
from .services import create_product, update_product

logger = logging.getLogger(__name__)


class ProductsListView(APIView):
    """
    GET /api/products?carrier_id={uuid}

    Get products for a specific carrier filtered by user's agency.

    Query params:
        carrier_id: Required carrier UUID

    Response (200):
        [
            {
                "id": "uuid",
                "carrier_id": "uuid",
                "name": "Product Name",
                "product_code": "CODE",
                "is_active": true,
                "created_at": "2024-01-01T00:00:00Z"
            }
        ]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        carrier_id = request.query_params.get('carrier_id')
        if not carrier_id:
            return Response(
                {'error': 'Missing carrier_id parameter', 'detail': 'carrier_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            carrier_uuid = UUID(carrier_id)
        except ValueError:
            return Response(
                {'error': 'Invalid carrier_id', 'detail': 'carrier_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            products = get_products_for_carrier(carrier_uuid, user.agency_id)
            return Response(products)
        except Exception as e:
            logger.error(f'Products list failed: {e}')
            return Response(
                {'error': 'Failed to fetch products', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        """
        POST /api/products

        Create a new product.

        Request body:
            {
                "carrier_id": "uuid",
                "name": "Product Name",
                "product_code": "CODE",  // optional
                "is_active": true        // optional, defaults to true
            }
        """
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        data = request.data
        carrier_id = data.get('carrier_id')
        name = data.get('name')

        if not carrier_id or not name:
            return Response(
                {'error': 'Missing required fields', 'detail': 'carrier_id and name are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            carrier_uuid = UUID(carrier_id)
        except ValueError:
            return Response(
                {'error': 'Invalid carrier_id', 'detail': 'carrier_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            product = create_product(
                carrier_id=carrier_uuid,
                agency_id=user.agency_id,
                name=name,
                product_code=data.get('product_code'),
                is_active=data.get('is_active', True),
            )
            return Response({
                'product': product,
                'message': 'Product created successfully. Please set commission percentages for all positions.'
            }, status=status.HTTP_201_CREATED)
        except ValueError as e:
            return Response(
                {'error': str(e), 'detail': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f'Product creation failed: {e}')
            # Check for unique constraint violation
            if 'duplicate key' in str(e).lower() or '23505' in str(e):
                return Response(
                    {'error': 'A product with this name already exists for this carrier.'},
                    status=status.HTTP_409_CONFLICT
                )
            return Response(
                {'error': 'Failed to create product', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AllProductsView(APIView):
    """
    GET /api/products/all

    Get all products for user's agency with carrier information.

    Response (200):
        [
            {
                "id": "uuid",
                "carrier_id": "uuid",
                "name": "Product Name",
                "product_code": "CODE",
                "is_active": true,
                "created_at": "2024-01-01T00:00:00Z",
                "carrier_name": "Carrier Name",
                "carrier_display_name": "Display Name"
            }
        ]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            products = get_all_products_for_agency(user.agency_id)
            return Response(products)
        except Exception as e:
            logger.error(f'All products list failed: {e}')
            return Response(
                {'error': 'Failed to fetch products', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ProductDetailView(APIView):
    """
    GET /api/products/{id}

    Get a single product by ID.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, product_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            product_uuid = UUID(product_id)
        except ValueError:
            return Response(
                {'error': 'Invalid product_id', 'detail': 'product_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            product = get_product_by_id(product_uuid, user.agency_id)
            if not product:
                return Response(
                    {'error': 'Product not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response(product)
        except Exception as e:
            logger.error(f'Product detail failed: {e}')
            return Response(
                {'error': 'Failed to fetch product', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def patch(self, request, product_id):
        """
        PATCH /api/products/{id}

        Update a product.
        """
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            product_uuid = UUID(product_id)
        except ValueError:
            return Response(
                {'error': 'Invalid product_id', 'detail': 'product_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            data = request.data
            product = update_product(
                product_id=product_uuid,
                agency_id=user.agency_id,
                name=data.get('name'),
                product_code=data.get('product_code'),
                is_active=data.get('is_active'),
            )
            if not product:
                return Response(
                    {'error': 'Product not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response(product)
        except Exception as e:
            logger.error(f'Product update failed: {e}')
            return Response(
                {'error': 'Failed to update product', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
