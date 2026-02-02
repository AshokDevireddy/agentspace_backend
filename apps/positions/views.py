"""
Positions API Views

Provides position-related endpoints:
- GET /api/positions - List positions for user's agency
- GET /api/positions/{id} - Get single position
- POST /api/positions - Create a new position
- PATCH /api/positions/{id} - Update a position
- GET /api/positions/product-commissions - Get commissions for a position
- PATCH /api/positions/product-commissions/{id} - Update a commission
- POST /api/positions/product-commissions/sync - Sync missing commissions
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import (
    get_all_position_product_commissions,
    get_position_by_id,
    get_position_product_commissions,
    get_positions_for_agency,
)
from .services import (
    create_position,
    delete_position,
    delete_position_commission,
    sync_position_commissions,
    update_position,
    update_position_commission,
    upsert_position_commissions,
)

logger = logging.getLogger(__name__)


class PositionsListView(APIView):
    """
    GET /api/positions

    Get all positions for user's agency.
    Mirrors Supabase RPC: get_positions_for_agency

    Response (200):
        [
            {
                "id": "uuid",
                "name": "Position Name",
                "level": 1,
                "description": "Description",
                "is_active": true,
                "created_at": "2024-01-01T00:00:00Z",
                "agent_count": 5
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
            positions = get_positions_for_agency(user.id)
            return Response(positions)
        except Exception as e:
            logger.error(f'Positions list failed: {e}')
            return Response(
                {'error': 'Failed to fetch positions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        """
        POST /api/positions

        Create a new position.

        Request body:
            {
                "name": "Position Name",
                "level": 1,
                "description": "Description",  // optional
                "is_active": true              // optional, defaults to true
            }
        """
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        data = request.data
        name = data.get('name')
        level = data.get('level')

        if not name or level is None:
            return Response(
                {'error': 'Missing required fields', 'detail': 'name and level are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(level, int) or level < 0:
            return Response(
                {'error': 'Invalid level', 'detail': 'level must be a positive integer'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            position = create_position(
                agency_id=user.agency_id,
                name=name,
                level=level,
                description=data.get('description'),
                is_active=data.get('is_active', True),
            )
            return Response({'position': position}, status=status.HTTP_201_CREATED)
        except Exception as e:
            logger.error(f'Position creation failed: {e}')
            if 'duplicate key' in str(e).lower() or '23505' in str(e):
                return Response(
                    {'error': 'A position with this name already exists in your agency.'},
                    status=status.HTTP_409_CONFLICT
                )
            return Response(
                {'error': 'Failed to create position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PositionDetailView(APIView):
    """
    GET /api/positions/{id}
    PATCH /api/positions/{id}

    Get or update a single position by ID.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, position_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            position_uuid = UUID(position_id)
        except ValueError:
            return Response(
                {'error': 'Invalid position_id', 'detail': 'position_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            position = get_position_by_id(position_uuid, user.agency_id)
            if not position:
                return Response(
                    {'error': 'Position not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response(position)
        except Exception as e:
            logger.error(f'Position detail failed: {e}')
            return Response(
                {'error': 'Failed to fetch position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def patch(self, request, position_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            position_uuid = UUID(position_id)
        except ValueError:
            return Response(
                {'error': 'Invalid position_id', 'detail': 'position_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            data = request.data
            position = update_position(
                position_id=position_uuid,
                agency_id=user.agency_id,
                name=data.get('name'),
                level=data.get('level'),
                description=data.get('description'),
                is_active=data.get('is_active'),
            )
            if not position:
                return Response(
                    {'error': 'Position not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response(position)
        except Exception as e:
            logger.error(f'Position update failed: {e}')
            return Response(
                {'error': 'Failed to update position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def delete(self, request, position_id):
        """
        DELETE /api/positions/{id}

        Delete a position if no agents are assigned to it.
        """
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            position_uuid = UUID(position_id)
        except ValueError:
            return Response(
                {'error': 'Invalid position_id', 'detail': 'position_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            deleted = delete_position(
                position_id=position_uuid,
                agency_id=user.agency_id,
            )
            if not deleted:
                return Response(
                    {'error': 'Position not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response({'success': True})
        except ValueError as e:
            return Response(
                {'error': 'Cannot delete position', 'detail': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f'Position delete failed: {e}')
            return Response(
                {'error': 'Failed to delete position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PositionCommissionsView(APIView):
    """
    GET /api/positions/product-commissions?position_id={uuid}
    POST /api/positions/product-commissions (batch upsert)

    Get product commissions for a position or batch upsert commissions.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        position_id = request.query_params.get('position_id')
        if not position_id:
            return Response(
                {'error': 'Missing position_id parameter', 'detail': 'position_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            position_uuid = UUID(position_id)
        except ValueError:
            return Response(
                {'error': 'Invalid position_id', 'detail': 'position_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            commissions = get_position_product_commissions(position_uuid, user.agency_id)
            return Response(commissions)
        except Exception as e:
            logger.error(f'Position commissions failed: {e}')
            return Response(
                {'error': 'Failed to fetch commissions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        """
        Batch upsert position-product commissions.

        Request body:
            {
                "commissions": [
                    {"position_id": "uuid", "product_id": "uuid", "commission_percentage": 50.0},
                    ...
                ]
            }
        """
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        commissions = request.data.get('commissions', [])

        if not isinstance(commissions, list) or not commissions:
            return Response(
                {'error': 'Missing required fields', 'detail': 'commissions array is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate each commission entry
        for comm in commissions:
            if not isinstance(comm, dict):
                return Response(
                    {'error': 'Invalid commission data', 'detail': 'Each commission must be an object'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            position_id = comm.get('position_id')
            product_id = comm.get('product_id')
            commission_percentage = comm.get('commission_percentage')

            if not position_id or not product_id or commission_percentage is None:
                return Response(
                    {'error': 'Invalid commission data', 'detail': 'Each commission must have position_id, product_id, and commission_percentage'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate UUIDs
            try:
                UUID(position_id)
                UUID(product_id)
            except ValueError:
                return Response(
                    {'error': 'Invalid UUID', 'detail': 'position_id and product_id must be valid UUIDs'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate commission_percentage range
            try:
                pct = float(commission_percentage)
                if pct < 0 or pct > 999.99:
                    return Response(
                        {'error': 'Invalid commission percentage', 'detail': 'commission_percentage must be between 0 and 999.99'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            except (TypeError, ValueError):
                return Response(
                    {'error': 'Invalid commission percentage', 'detail': 'commission_percentage must be a number'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        try:
            result = upsert_position_commissions(user.agency_id, commissions)
            return Response({'commissions': result}, status=status.HTTP_201_CREATED)
        except Exception as e:
            logger.error(f'Commission upsert failed: {e}')
            return Response(
                {'error': 'Failed to create/update commissions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PositionCommissionDetailView(APIView):
    """
    PATCH /api/positions/product-commissions/{id}
    DELETE /api/positions/product-commissions/{id}

    Update or delete a commission entry.
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, commission_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            commission_uuid = UUID(commission_id)
        except ValueError:
            return Response(
                {'error': 'Invalid commission_id', 'detail': 'commission_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        commission_percentage = request.data.get('commission_percentage')
        if commission_percentage is None:
            return Response(
                {'error': 'Missing commission_percentage', 'detail': 'commission_percentage is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            commission = update_position_commission(
                commission_id=commission_uuid,
                agency_id=user.agency_id,
                commission_percentage=float(commission_percentage),
            )
            if not commission:
                return Response(
                    {'error': 'Commission not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response(commission)
        except Exception as e:
            logger.error(f'Commission update failed: {e}')
            return Response(
                {'error': 'Failed to update commission', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def delete(self, request, commission_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            commission_uuid = UUID(commission_id)
        except ValueError:
            return Response(
                {'error': 'Invalid commission_id', 'detail': 'commission_id must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            deleted = delete_position_commission(
                commission_id=commission_uuid,
                agency_id=user.agency_id,
            )
            if not deleted:
                return Response(
                    {'error': 'Commission not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response({'success': True})
        except Exception as e:
            logger.error(f'Commission delete failed: {e}')
            return Response(
                {'error': 'Failed to delete commission', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SyncCommissionsView(APIView):
    """
    POST /api/positions/product-commissions/sync

    Sync missing commission entries for all position-product combinations.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            created_count = sync_position_commissions(user.agency_id)
            return Response({
                'success': True,
                'created_count': created_count,
                'message': f'Created {created_count} missing commission entries.'
            })
        except Exception as e:
            logger.error(f'Commission sync failed: {e}')
            return Response(
                {'error': 'Failed to sync commissions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AllPositionCommissionsView(APIView):
    """
    GET /api/positions/all-commissions

    Get all product commissions for all positions in user's agency.
    Replaces Supabase RPC: get_position_product_commissions

    Query params:
        carrier_id: Optional carrier UUID to filter by

    Response (200):
        [
            {
                "commission_id": "uuid",
                "position_id": "uuid",
                "position_name": "Agent",
                "position_level": 1,
                "product_id": "uuid",
                "product_name": "Term Life 20",
                "carrier_id": "uuid",
                "carrier_name": "Carrier Name",
                "commission_percentage": 50.00
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

        carrier_id_str = request.query_params.get('carrier_id')
        carrier_id = None

        if carrier_id_str:
            try:
                carrier_id = UUID(carrier_id_str)
            except ValueError:
                return Response(
                    {'error': 'Invalid carrier_id', 'detail': 'carrier_id must be a valid UUID'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        try:
            commissions = get_all_position_product_commissions(user.id, carrier_id)
            return Response(commissions)
        except Exception as e:
            logger.error(f'All position commissions failed: {e}')
            return Response(
                {'error': 'Failed to fetch commissions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
