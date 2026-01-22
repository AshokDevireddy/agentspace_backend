"""
SMS API Views (P2-033 to P2-035)

Provides SMS-related endpoints:
- GET /api/sms/conversations - Get SMS conversations
- GET /api/sms/messages - Get messages for conversation
- GET /api/sms/drafts - Get draft messages
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    get_sms_conversations,
    get_sms_messages,
    get_draft_messages,
    get_unread_message_count,
)

logger = logging.getLogger(__name__)


class ConversationsView(APIView):
    """
    GET /api/sms/conversations

    Get SMS conversations based on view mode.

    Query params:
        view_mode: 'all' (admin), 'self', or 'downlines' (default: 'self')
        page: Page number (default: 1)
        limit: Page size (default: 20)
        search: Search by client name or phone number
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
            view_mode = request.query_params.get('view_mode', 'self')
            if view_mode not in ('all', 'self', 'downlines'):
                view_mode = 'self'

            page = int(request.query_params.get('page', 1))
            limit = int(request.query_params.get('limit', 20))
            limit = min(limit, 100)

            search_query = request.query_params.get('search', '').strip() or None

            result = get_sms_conversations(
                user=user,
                view_mode=view_mode,
                page=page,
                limit=limit,
                search_query=search_query,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'SMS conversations failed: {e}')
            return Response(
                {'error': 'Failed to fetch conversations', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class MessagesView(APIView):
    """
    GET /api/sms/messages

    Get messages for a conversation.

    Query params:
        conversation_id: The conversation UUID (required)
        page: Page number (default: 1)
        limit: Page size (default: 50)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        conversation_id = request.query_params.get('conversation_id')
        if not conversation_id:
            return Response(
                {'error': 'conversation_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            conversation_uuid = UUID(conversation_id)
        except ValueError:
            return Response(
                {'error': 'Invalid conversation_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            page = int(request.query_params.get('page', 1))
            limit = int(request.query_params.get('limit', 50))
            limit = min(limit, 100)

            result = get_sms_messages(
                user=user,
                conversation_id=conversation_uuid,
                page=page,
                limit=limit,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'SMS messages failed: {e}')
            return Response(
                {'error': 'Failed to fetch messages', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class DraftsView(APIView):
    """
    GET /api/sms/drafts

    Get draft messages pending approval.

    Query params:
        view_mode: 'all' (admin), 'self', or 'downlines' (default: 'self')
        page: Page number (default: 1)
        limit: Page size (default: 20)
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
            view_mode = request.query_params.get('view_mode', 'self')
            if view_mode not in ('all', 'self', 'downlines'):
                view_mode = 'self'

            page = int(request.query_params.get('page', 1))
            limit = int(request.query_params.get('limit', 20))
            limit = min(limit, 100)

            result = get_draft_messages(
                user=user,
                view_mode=view_mode,
                page=page,
                limit=limit,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'SMS drafts failed: {e}')
            return Response(
                {'error': 'Failed to fetch drafts', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UnreadCountView(APIView):
    """
    GET /api/sms/unread-count

    Get count of unread inbound messages.
    Translated from Supabase RPC: get_unread_message_count

    Query params:
        view_mode: 'all' (admin), 'self', or 'downlines' (default: 'self')
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
            view_mode = request.query_params.get('view_mode', 'self')
            if view_mode not in ('all', 'self', 'downlines'):
                view_mode = 'self'

            count = get_unread_message_count(user=user, view_mode=view_mode)

            return Response({'count': count})

        except Exception as e:
            logger.error(f'SMS unread count failed: {e}')
            return Response(
                {'error': 'Failed to get unread count', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
