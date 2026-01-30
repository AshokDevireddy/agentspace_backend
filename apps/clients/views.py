"""
Clients API Views (P2-037)

Provides client-related endpoints:
- GET /api/clients - Get paginated client list
- GET /api/clients/{id} - Get client detail
- POST /api/clients/invite - Invite a new client
- POST /api/clients/resend-invite - Resend client invitation
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import (
    get_client_dashboard_data,
    get_client_detail,
    get_client_own_deals,
    get_clients_list,
)
from .services import invite_client, resend_client_invite

logger = logging.getLogger(__name__)


class ClientsListView(APIView):
    """
    GET /api/clients
    GET /api/clients/overview

    Get paginated list of clients.
    Translated from Supabase RPC: get_clients_overview

    Query params:
        page: Page number (default: 1)
        limit: Page size (default: 20)
        search: Search by client name, email, or phone
        agent_id: Filter by specific agent's clients
        view: Visibility scope - 'self' (own clients), 'downlines' (default), 'all' (admin only)
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
            page = int(request.query_params.get('page', 1))
            limit = int(request.query_params.get('limit', 20))
            limit = min(limit, 100)

            search_query = request.query_params.get('search', '').strip() or None

            # Support view parameter from RPC (self, downlines, all)
            view = request.query_params.get('view', 'downlines').lower()

            agent_id = request.query_params.get('agent_id')
            if agent_id:
                try:
                    agent_id = UUID(agent_id)
                except ValueError:
                    agent_id = None

            is_admin = user.is_admin or user.role == 'admin'

            # Handle view parameter - normalize to flags
            include_full_agency = is_admin and view == 'all'
            if view == 'self':
                agent_id = user.id

            result = get_clients_list(
                user=user,
                page=page,
                limit=limit,
                search_query=search_query,
                agent_id=agent_id,
                include_full_agency=include_full_agency,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'Clients list failed: {e}')
            return Response(
                {'error': 'Failed to fetch clients', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClientDetailView(APIView):
    """
    GET /api/clients/{id}

    Get detailed information about a client including their deals.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, client_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            client_uuid = UUID(client_id)
        except ValueError:
            return Response(
                {'error': 'Invalid client_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = get_client_detail(user=user, client_id=client_uuid)

            if not result:
                return Response(
                    {'error': 'Client not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response(result)

        except Exception as e:
            logger.error(f'Client detail failed: {e}')
            return Response(
                {'error': 'Failed to fetch client', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClientDashboardView(APIView):
    """
    GET /api/client/dashboard

    Get dashboard data for a client user (their own profile and agency branding).
    This endpoint is for clients viewing their own data.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Verify user is a client
        if user.role != 'client':
            return Response(
                {'error': 'Forbidden', 'message': 'This endpoint is for client users only'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            result = get_client_dashboard_data(
                user_id=user.id,
                auth_user_id=str(user.auth_user_id),
            )

            if not result:
                return Response(
                    {'error': 'NotFound', 'message': 'Client profile not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response(result)

        except Exception as e:
            logger.error(f'Client dashboard failed: {e}')
            return Response(
                {'error': 'Failed to fetch dashboard', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClientDealsView(APIView):
    """
    GET /api/client/deals

    Get deals for a client user (their own policies).
    This endpoint is for clients viewing their own policies.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Verify user is a client
        if user.role != 'client':
            return Response(
                {'error': 'Forbidden', 'message': 'This endpoint is for client users only'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_client_own_deals(user_id=user.id)
            return Response({'deals': deals})

        except Exception as e:
            logger.error(f'Client deals failed: {e}')
            return Response(
                {'error': 'Failed to fetch deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClientInviteView(APIView):
    """
    POST /api/clients/invite

    Invite a new client to the agency.
    Creates Supabase auth user, sends invite email, and creates DB user record.

    Request body:
        {
            "email": "client@example.com",
            "firstName": "Jane",
            "lastName": "Smith",
            "phoneNumber": "+1234567890"  // optional
        }

    Response (200):
        {
            "success": true,
            "userId": "uuid",
            "message": "Client invited successfully",
            "alreadyExists": false
        }
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        data = request.data
        # Support both camelCase (frontend) and snake_case (backend) params
        email = data.get('email', '').strip().lower()
        first_name = data.get('firstName') or data.get('first_name', '')
        last_name = data.get('lastName') or data.get('last_name', '')
        phone_number = data.get('phoneNumber') or data.get('phone_number') or data.get('phone')

        # Validate required fields
        if not email:
            return Response(
                {'error': 'email is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = invite_client(
                inviter_id=user.id,
                agency_id=user.agency_id,
                email=email,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
            )

            if not result.get('success'):
                return Response(
                    {'error': result.get('error', 'Failed to invite client')},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Return with camelCase keys for frontend compatibility
            return Response({
                'success': True,
                'userId': result.get('user_id'),
                'message': result.get('message', 'Client invited successfully'),
                'alreadyExists': result.get('already_exists', False),
                'status': result.get('status'),
            })

        except Exception as e:
            logger.error(f'Client invite failed: {e}')
            return Response(
                {'error': 'Failed to invite client', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClientResendInviteView(APIView):
    """
    POST /api/clients/resend-invite

    Resend an invitation to a client who has not yet completed onboarding.
    Unlinks old auth account, deletes it, creates new invite, and updates user record.

    Request body:
        {
            "clientId": "uuid"
        }

    Response (200):
        {
            "success": true,
            "message": "Invitation resent successfully to Jane Smith"
        }
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        data = request.data
        # Support both camelCase and snake_case
        client_id_str = data.get('clientId') or data.get('client_id')

        if not client_id_str:
            return Response(
                {'error': 'Client ID is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            client_uuid = UUID(client_id_str)
        except ValueError:
            return Response(
                {'error': 'Invalid clientId format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = resend_client_invite(
                requester_id=user.id,
                agency_id=user.agency_id,
                client_id=client_uuid,
            )

            if not result.get('success'):
                error_msg = result.get('error', 'Failed to resend invite')
                # Determine appropriate status code
                if 'not found' in error_msg.lower():
                    return Response({'error': error_msg}, status=status.HTTP_404_NOT_FOUND)
                if 'other agencies' in error_msg.lower():
                    return Response({'error': error_msg}, status=status.HTTP_403_FORBIDDEN)
                return Response({'error': error_msg}, status=status.HTTP_400_BAD_REQUEST)

            return Response({
                'success': True,
                'message': result.get('message', 'Invitation resent successfully'),
            })

        except Exception as e:
            logger.error(f'Client resend invite failed: {e}')
            return Response(
                {'error': 'Failed to resend invite', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
