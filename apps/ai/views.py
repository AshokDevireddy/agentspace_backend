"""
AI API Views (P1-015, P2-036, P2-037, P2-038)

Endpoints:
- GET/POST /api/ai/conversations - List/create conversations
- GET/DELETE /api/ai/conversations/{id} - Get/delete conversation
- GET/POST /api/ai/conversations/{id}/messages - List/send messages
- POST /api/ai/chat - Quick chat without conversation
- GET /api/ai/suggestions - AI-powered suggestions (P2-037)
- GET /api/ai/analytics/insights - AI analytics insights (P2-038)
"""
import logging
import uuid

from django.db import connection
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.constants import PAGINATION
from apps.core.mixins import AuthenticatedAPIView
from apps.core.permissions import SubscriptionTierPermission

from .selectors import (
    get_ai_conversation_detail,
    get_ai_conversations,
    get_ai_messages,
)
from .services import (
    generate_analytics_insights,
    generate_chat_response,
    generate_suggestions,
    save_ai_message,
)
from .services import (
    get_user_context as get_ai_user_context,
)

logger = logging.getLogger(__name__)


class AIConversationsView(AuthenticatedAPIView, APIView):
    """GET/POST /api/ai/conversations - List or create AI conversations."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['ai_chat_enabled']

    def get(self, request):
        user = self.get_user(request)

        page = int(request.query_params.get('page', 1))
        limit = min(
            int(request.query_params.get('limit', PAGINATION["default_limit"])),
            PAGINATION["max_limit"],
        )

        result = get_ai_conversations(user=user, page=page, limit=limit)
        return Response(result)

    def post(self, request):
        user = self.get_user(request)

        title = request.data.get('title')
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


class AIConversationDetailView(AuthenticatedAPIView, APIView):
    """GET/DELETE /api/ai/conversations/{id} - Get or delete conversation."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['ai_chat_enabled']

    def get(self, request, conversation_id):
        user = self.get_user(request)
        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        result = get_ai_conversation_detail(user=user, conversation_id=conversation_uuid)
        if not result:
            return Response(
                {'error': 'Conversation not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        return Response(result)

    def delete(self, request, conversation_id):
        user = self.get_user(request)
        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        with connection.cursor() as cursor:
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

        return Response({'success': True})


class AIMessagesView(AuthenticatedAPIView, APIView):
    """GET/POST /api/ai/conversations/{id}/messages - List or send messages."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['ai_chat_enabled']

    def get(self, request, conversation_id):
        user = self.get_user(request)
        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        page = int(request.query_params.get('page', 1))
        limit = min(
            int(request.query_params.get('limit', 50)),
            PAGINATION["max_limit"],
        )

        result = get_ai_messages(
            user=user,
            conversation_id=conversation_uuid,
            page=page,
            limit=limit,
        )
        return Response(result)

    def post(self, request, conversation_id):
        """Send a user message and get AI response."""
        user = self.get_user(request)
        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        content = request.data.get('content')
        if not content or not content.strip():
            return Response(
                {'error': 'content is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        include_context = request.data.get('include_context', True)

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

        # Save user message
        user_message = save_ai_message(
            conversation_id=conversation_uuid,
            role='user',
            content=content.strip(),
        )

        if not user_message:
            return Response(
                {'error': 'Failed to save message'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Get context for AI if requested
        context = get_ai_user_context(user) if include_context else None

        # Generate AI response
        ai_response = generate_chat_response(
            user=user,
            conversation_id=conversation_uuid,
            user_message=content.strip(),
            context=context,
        )

        # Save AI response
        assistant_message = save_ai_message(
            conversation_id=conversation_uuid,
            role='assistant',
            content=ai_response.content,
            input_tokens=ai_response.input_tokens,
            output_tokens=ai_response.output_tokens,
            tokens_used=ai_response.total_tokens,
        )

        return Response({
            'user_message': user_message,
            'assistant_message': assistant_message,
            'tokens': {
                'input': ai_response.input_tokens,
                'output': ai_response.output_tokens,
                'total': ai_response.total_tokens,
            },
            'error': ai_response.error,
        }, status=status.HTTP_201_CREATED)


class AIQuickChatView(AuthenticatedAPIView, APIView):
    """POST /api/ai/chat - Quick chat without creating a conversation."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['ai_chat_enabled']

    def post(self, request):
        user = self.get_user(request)

        content = request.data.get('content')
        if not content or not content.strip():
            return Response(
                {'error': 'content is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        include_context = request.data.get('include_context', True)
        context = get_ai_user_context(user) if include_context else None

        ai_response = generate_chat_response(
            user=user,
            conversation_id=uuid.uuid4(),
            user_message=content.strip(),
            context=context,
        )

        return Response({
            'response': ai_response.content,
            'tokens': {
                'input': ai_response.input_tokens,
                'output': ai_response.output_tokens,
                'total': ai_response.total_tokens,
            },
            'error': ai_response.error,
        })


class AISuggestionsView(AuthenticatedAPIView, APIView):
    """GET /api/ai/suggestions (P2-037) - AI-powered suggestions."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['ai_chat_enabled']

    def get(self, request):
        user = self.get_user(request)
        suggestion_type = request.query_params.get('type', 'general')

        context = get_ai_user_context(user)
        suggestions = generate_suggestions(
            user=user,
            suggestion_type=suggestion_type,
            context=context,
        )

        return Response({
            'suggestions': suggestions,
            'type': suggestion_type,
        })


class AIAnalyticsInsightsView(AuthenticatedAPIView, APIView):
    """GET /api/ai/analytics/insights (P2-038) - AI analytics insights."""

    permission_classes = [IsAuthenticated, SubscriptionTierPermission]
    required_features = ['advanced_analytics', 'ai_chat_enabled']

    def get(self, request):
        user = self.get_user(request)
        insight_type = request.query_params.get('type', 'general')
        period = request.query_params.get('period', 'month')

        analytics_data = self._get_analytics_data(user, insight_type, period)
        insights = generate_analytics_insights(
            user=user,
            analytics_data=analytics_data,
            insight_type=insight_type,
        )

        return Response({
            'insights': insights,
            'type': insight_type,
            'period': period,
        })

    def _get_analytics_data(self, user, insight_type: str, period: str) -> dict:
        """Fetch relevant analytics data for AI analysis."""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_deals,
                    COUNT(*) FILTER (WHERE status_standardized = 'active') as active_deals,
                    COUNT(*) FILTER (WHERE status_standardized = 'pending') as pending_deals,
                    COUNT(*) FILTER (WHERE status_standardized IN ('cancelled', 'lapsed')) as lost_deals,
                    COALESCE(SUM(annual_premium) FILTER (WHERE status_standardized = 'active'), 0) as total_premium,
                    COALESCE(AVG(annual_premium) FILTER (WHERE status_standardized = 'active'), 0) as avg_premium
                FROM public.deals
                WHERE agent_id = %s
            """, [str(user.id)])
            row = cursor.fetchone()

        return {
            'total_deals': row[0] if row else 0,
            'active_deals': row[1] if row else 0,
            'pending_deals': row[2] if row else 0,
            'lost_deals': row[3] if row else 0,
            'total_premium': float(row[4]) if row else 0.0,
            'avg_premium': float(row[5]) if row else 0.0,
            'period': period,
            'insight_type': insight_type,
        }
