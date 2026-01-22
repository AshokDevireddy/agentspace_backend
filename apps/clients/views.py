"""
Clients API Views (P2-037)

Provides client-related endpoints:
- GET /api/clients - Get paginated client list
- GET /api/clients/{id} - Get client detail
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import get_clients_list, get_client_detail

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
