"""
Clients API Views (P2-037)

Provides client-related endpoints:
- GET /api/clients - Get paginated client list
- GET /api/clients/{id} - Get client detail
- POST /api/clients/invite - Invite a new client
"""
import logging
import uuid as uuid_module
from uuid import UUID

from django.db import connection

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

    Invite a new client. Creates a client record with an optional user record
    for client portal access.

    Request body:
        {
            "email": "client@example.com",
            "first_name": "Jane",
            "last_name": "Smith",
            "phone": "+1234567890",      // optional
            "create_portal_access": true  // optional - create user for client portal
        }

    Response (201):
        {
            "success": true,
            "client_id": "uuid",
            "user_id": "uuid",  // if portal access created
            ...
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
        email = data.get('email', '').strip().lower()
        first_name = data.get('first_name', '').strip()
        last_name = data.get('last_name', '').strip()
        phone = data.get('phone', '').strip() if data.get('phone') else None
        create_portal_access = data.get('create_portal_access', False)

        # Validate required fields
        if not email:
            return Response(
                {'error': 'email is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not first_name:
            return Response(
                {'error': 'first_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not last_name:
            return Response(
                {'error': 'last_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with connection.cursor() as cursor:
                # Check if client already exists
                cursor.execute("""
                    SELECT id, email FROM clients
                    WHERE email = %s AND agency_id = %s
                    LIMIT 1
                """, [email, str(user.agency_id)])

                existing = cursor.fetchone()
                if existing:
                    return Response({
                        'success': True,
                        'client_id': str(existing[0]),
                        'message': 'Client already exists',
                        'existing': True
                    })

                # Create client record
                client_id = uuid_module.uuid4()
                cursor.execute("""
                    INSERT INTO clients (
                        id, agency_id, first_name, last_name, email, phone,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    RETURNING id, email, first_name, last_name
                """, [
                    str(client_id),
                    str(user.agency_id),
                    first_name,
                    last_name,
                    email,
                    phone,
                ])

                client_row = cursor.fetchone()
                if not client_row:
                    return Response(
                        {'error': 'Failed to create client'},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                result = {
                    'success': True,
                    'client_id': str(client_row[0]),
                    'email': client_row[1],
                    'first_name': client_row[2],
                    'last_name': client_row[3],
                }

                # Optionally create user record for portal access
                if create_portal_access:
                    user_id = uuid_module.uuid4()
                    cursor.execute("""
                        INSERT INTO users (
                            id, email, first_name, last_name, phone_number,
                            agency_id, role, status, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, 'client', 'invited', NOW(), NOW())
                        ON CONFLICT (email, agency_id) DO NOTHING
                        RETURNING id
                    """, [
                        str(user_id),
                        email,
                        first_name,
                        last_name,
                        phone,
                        str(user.agency_id),
                    ])

                    user_row = cursor.fetchone()
                    if user_row:
                        result['user_id'] = str(user_row[0])
                        result['portal_access'] = True

                return Response(result, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f'Client invite failed: {e}')
            return Response(
                {'error': 'Failed to invite client', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
