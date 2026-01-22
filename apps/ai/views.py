"""
AI API Views (P1-015)

Provides AI conversation and message management endpoints:
- GET /api/ai/conversations - List user's AI conversations
- POST /api/ai/conversations - Create new conversation
- GET /api/ai/conversations/{id} - Get conversation with messages
- DELETE /api/ai/conversations/{id} - Archive/delete conversation
- GET /api/ai/conversations/{id}/messages - Get messages (paginated)
- POST /api/ai/conversations/{id}/messages - Add message
"""
import logging
import uuid
from uuid import UUID

from django.db import connection
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    get_ai_conversations,
    get_ai_conversation_detail,
    get_ai_messages,
)

logger = logging.getLogger(__name__)


class AIConversationsView(APIView):
    """
    GET /api/ai/conversations
    POST /api/ai/conversations

    List or create AI conversations.

    GET Query params:
        page: Page number (default: 1)
        limit: Page size (default: 20)

    POST Body:
        title: Optional conversation title
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

            result = get_ai_conversations(
                user=user,
                page=page,
                limit=limit,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'AI conversations list failed: {e}')
            return Response(
                {'error': 'Failed to fetch conversations', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            title = request.data.get('title', None)
            conversation_id = uuid.uuid4()

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO public.ai_conversations (id, user_id, agency_id, title, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id, title, is_active, created_at, updated_at
                """, [
                    str(conversation_id),
                    str(user.id),
                    str(user.agency_id),
                    title,
                    True
                ])
                row = cursor.fetchone()

            if not row:
                return Response(
                    {'error': 'Failed to create conversation'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            return Response({
                'id': str(row[0]),
                'title': row[1],
                'is_active': row[2],
                'created_at': row[3].isoformat() if row[3] else None,
                'updated_at': row[4].isoformat() if row[4] else None,
                'messages': [],
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f'AI conversation create failed: {e}')
            return Response(
                {'error': 'Failed to create conversation', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AIConversationDetailView(APIView):
    """
    GET /api/ai/conversations/{id}
    DELETE /api/ai/conversations/{id}

    Get or delete a specific AI conversation.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, conversation_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            conversation_uuid = UUID(str(conversation_id))
        except ValueError:
            return Response(
                {'error': 'Invalid conversation_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = get_ai_conversation_detail(
                user=user,
                conversation_id=conversation_uuid,
            )

            if not result:
                return Response(
                    {'error': 'Conversation not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response(result)

        except Exception as e:
            logger.error(f'AI conversation detail failed: {e}')
            return Response(
                {'error': 'Failed to fetch conversation', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def delete(self, request, conversation_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            conversation_uuid = UUID(str(conversation_id))
        except ValueError:
            return Response(
                {'error': 'Invalid conversation_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with connection.cursor() as cursor:
                # Verify ownership and archive (soft delete)
                cursor.execute("""
                    UPDATE public.ai_conversations
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE id = %s AND user_id = %s AND agency_id = %s
                    RETURNING id
                """, [str(conversation_uuid), str(user.id), str(user.agency_id)])
                row = cursor.fetchone()

            if not row:
                return Response(
                    {'error': 'Conversation not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response({'success': True}, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f'AI conversation delete failed: {e}')
            return Response(
                {'error': 'Failed to delete conversation', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AIMessagesView(APIView):
    """
    GET /api/ai/conversations/{id}/messages
    POST /api/ai/conversations/{id}/messages

    List or create messages in an AI conversation.

    GET Query params:
        page: Page number (default: 1)
        limit: Page size (default: 50)

    POST Body:
        role: 'user', 'assistant', or 'system'
        content: Message content
        tool_calls: Optional JSON for tool calls
        tool_results: Optional JSON for tool results
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, conversation_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            conversation_uuid = UUID(str(conversation_id))
        except ValueError:
            return Response(
                {'error': 'Invalid conversation_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            page = int(request.query_params.get('page', 1))
            limit = int(request.query_params.get('limit', 50))
            limit = min(limit, 100)

            result = get_ai_messages(
                user=user,
                conversation_id=conversation_uuid,
                page=page,
                limit=limit,
            )

            return Response(result)

        except Exception as e:
            logger.error(f'AI messages list failed: {e}')
            return Response(
                {'error': 'Failed to fetch messages', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request, conversation_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            conversation_uuid = UUID(str(conversation_id))
        except ValueError:
            return Response(
                {'error': 'Invalid conversation_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate required fields
        role = request.data.get('role')
        content = request.data.get('content')

        if not role or role not in ('user', 'assistant', 'system'):
            return Response(
                {'error': 'role is required and must be user, assistant, or system'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if content is None:
            return Response(
                {'error': 'content is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        tool_calls = request.data.get('tool_calls')
        tool_results = request.data.get('tool_results')

        try:
            # Verify conversation ownership
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM public.ai_conversations
                    WHERE id = %s AND user_id = %s AND agency_id = %s AND is_active = TRUE
                    LIMIT 1
                """, [str(conversation_uuid), str(user.id), str(user.agency_id)])
                row = cursor.fetchone()

            if not row:
                return Response(
                    {'error': 'Conversation not found or inactive'},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Create message
            message_id = uuid.uuid4()

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO public.ai_messages (id, conversation_id, role, content, tool_calls, tool_results)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id, role, content, tool_calls, tool_results, created_at
                """, [
                    str(message_id),
                    str(conversation_uuid),
                    role,
                    content,
                    tool_calls,
                    tool_results
                ])
                msg_row = cursor.fetchone()

                # Update conversation updated_at
                cursor.execute("""
                    UPDATE public.ai_conversations
                    SET updated_at = NOW()
                    WHERE id = %s
                """, [str(conversation_uuid)])

            if not msg_row:
                return Response(
                    {'error': 'Failed to create message'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            return Response({
                'id': str(msg_row[0]),
                'role': msg_row[1],
                'content': msg_row[2],
                'tool_calls': msg_row[3],
                'tool_results': msg_row[4],
                'created_at': msg_row[5].isoformat() if msg_row[5] else None,
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f'AI message create failed: {e}')
            return Response(
                {'error': 'Failed to create message', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
