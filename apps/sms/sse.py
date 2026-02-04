"""
Server-Sent Events (SSE) for SMS Real-time Updates

Provides real-time updates for SMS conversations and messages.
Replaces Supabase realtime subscriptions with efficient SSE streaming.
"""
import json
import logging
import time
from datetime import datetime
from typing import Optional
from uuid import UUID

from django.db import connection
from django.http import StreamingHttpResponse
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

logger = logging.getLogger(__name__)

# SSE polling interval in seconds
SSE_POLL_INTERVAL = 2

# Maximum time to keep connection open (5 minutes)
SSE_MAX_DURATION = 300


class ConversationMessagesSSEView(APIView):
    """
    GET /api/sms/sse/messages?conversation_id={id}

    Server-Sent Events stream for conversation messages.
    Polls database every 2 seconds and pushes updates to client.
    Connection closes automatically after 5 minutes.

    Events:
    - new_message: New message inserted
    - message_updated: Message updated (e.g., marked as read)
    - conversation_updated: Conversation metadata changed
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return StreamingHttpResponse(
                self._error_stream('Unauthorized'),
                content_type='text/event-stream',
                status=401
            )

        conversation_id_str = request.GET.get('conversation_id')
        if not conversation_id_str:
            return StreamingHttpResponse(
                self._error_stream('conversation_id parameter required'),
                content_type='text/event-stream',
                status=400
            )

        try:
            conversation_id = UUID(conversation_id_str)
        except ValueError:
            return StreamingHttpResponse(
                self._error_stream('Invalid conversation_id format'),
                content_type='text/event-stream',
                status=400
            )

        response = StreamingHttpResponse(
            self._event_stream(conversation_id, user.id),
            content_type='text/event-stream'
        )
        # Disable buffering for real-time streaming
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'  # For nginx
        return response

    def _error_stream(self, message: str):
        """Generate an error event and close stream."""
        yield f"event: error\ndata: {json.dumps({'error': message})}\n\n"

    def _event_stream(self, conversation_id: UUID, user_id: UUID):
        """
        Generate SSE events for conversation updates.
        """
        start_time = time.time()
        last_message_check = None
        last_conversation_check = None

        while True:
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > SSE_MAX_DURATION:
                yield f"event: timeout\ndata: {json.dumps({'message': 'Stream timeout'})}\n\n"
                break

            # Check for new/updated messages
            messages_result = self._get_messages_since(
                conversation_id, last_message_check
            )

            for msg in messages_result.get('new_messages', []):
                yield f"event: new_message\ndata: {json.dumps(msg)}\n\n"

            for msg in messages_result.get('updated_messages', []):
                yield f"event: message_updated\ndata: {json.dumps(msg)}\n\n"

            if messages_result.get('latest_check'):
                last_message_check = messages_result['latest_check']

            # Check for conversation updates
            conv_result = self._get_conversation_updates(
                conversation_id, last_conversation_check
            )

            if conv_result.get('updated'):
                yield f"event: conversation_updated\ndata: {json.dumps(conv_result['data'])}\n\n"

            if conv_result.get('latest_check'):
                last_conversation_check = conv_result['latest_check']

            # Wait before next poll
            time.sleep(SSE_POLL_INTERVAL)

    def _get_messages_since(
        self, conversation_id: UUID, since: Optional[str]
    ) -> dict:
        """Fetch new and updated messages since the last check."""
        result = {
            'new_messages': [],
            'updated_messages': [],
            'latest_check': None,
        }

        with connection.cursor() as cursor:
            now = datetime.utcnow().isoformat()

            if since:
                # Check for new messages
                cursor.execute("""
                    SELECT id, conversation_id, sender_id, receiver_id, body,
                           direction, status, sent_at, read_at, metadata,
                           created_at, updated_at
                    FROM messages
                    WHERE conversation_id = %s
                      AND created_at > %s
                    ORDER BY created_at ASC
                """, [str(conversation_id), since])

                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    msg = dict(zip(columns, row))
                    # Convert datetime objects to ISO strings
                    for key in ['sent_at', 'read_at', 'created_at', 'updated_at']:
                        if msg.get(key):
                            msg[key] = msg[key].isoformat()
                    # Convert UUID to string
                    for key in ['id', 'conversation_id', 'sender_id', 'receiver_id']:
                        if msg.get(key):
                            msg[key] = str(msg[key])
                    result['new_messages'].append(msg)

                # Check for updated messages (e.g., marked as read)
                cursor.execute("""
                    SELECT id, conversation_id, sender_id, receiver_id, body,
                           direction, status, sent_at, read_at, metadata,
                           created_at, updated_at
                    FROM messages
                    WHERE conversation_id = %s
                      AND updated_at > %s
                      AND created_at <= %s
                    ORDER BY updated_at ASC
                """, [str(conversation_id), since, since])

                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    msg = dict(zip(columns, row))
                    for key in ['sent_at', 'read_at', 'created_at', 'updated_at']:
                        if msg.get(key):
                            msg[key] = msg[key].isoformat()
                    for key in ['id', 'conversation_id', 'sender_id', 'receiver_id']:
                        if msg.get(key):
                            msg[key] = str(msg[key])
                    result['updated_messages'].append(msg)

            result['latest_check'] = now

        return result

    def _get_conversation_updates(
        self, conversation_id: UUID, since: Optional[str]
    ) -> dict:
        """Check if conversation metadata was updated."""
        result = {
            'updated': False,
            'data': None,
            'latest_check': None,
        }

        with connection.cursor() as cursor:
            now = datetime.utcnow().isoformat()

            if since:
                cursor.execute("""
                    SELECT id, sms_opt_in_status, opted_in_at, opted_out_at,
                           updated_at
                    FROM conversations
                    WHERE id = %s AND updated_at > %s
                """, [str(conversation_id), since])

                row = cursor.fetchone()
                if row:
                    columns = [col[0] for col in cursor.description]
                    data = dict(zip(columns, row))
                    data['id'] = str(data['id'])
                    for key in ['opted_in_at', 'opted_out_at', 'updated_at']:
                        if data.get(key):
                            data[key] = data[key].isoformat()
                    result['updated'] = True
                    result['data'] = data

            result['latest_check'] = now

        return result


class UnreadCountSSEView(APIView):
    """
    GET /api/sms/sse/unread-count

    Server-Sent Events stream for unread message count.
    Polls database every 2 seconds and pushes updates when count changes.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return StreamingHttpResponse(
                self._error_stream('Unauthorized'),
                content_type='text/event-stream',
                status=401
            )

        response = StreamingHttpResponse(
            self._event_stream(user.id),
            content_type='text/event-stream'
        )
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'
        return response

    def _error_stream(self, message: str):
        """Generate an error event and close stream."""
        yield f"event: error\ndata: {json.dumps({'error': message})}\n\n"

    def _event_stream(self, user_id: UUID):
        """Generate SSE events for unread count changes."""
        start_time = time.time()
        last_count = None

        while True:
            elapsed = time.time() - start_time
            if elapsed > SSE_MAX_DURATION:
                yield f"event: timeout\ndata: {json.dumps({'message': 'Stream timeout'})}\n\n"
                break

            current_count = self._get_unread_count(user_id)

            # Only send event if count changed
            if last_count is None or current_count != last_count:
                yield f"event: count_update\ndata: {json.dumps({'unread_count': current_count})}\n\n"
                last_count = current_count

            time.sleep(SSE_POLL_INTERVAL)

    def _get_unread_count(self, user_id: UUID) -> int:
        """Get total unread message count for user."""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM messages m
                JOIN conversations c ON m.conversation_id = c.id
                WHERE c.agent_id = %s
                  AND m.direction = 'inbound'
                  AND m.read_at IS NULL
            """, [str(user_id)])

            row = cursor.fetchone()
            return row[0] if row else 0


class ConversationsSSEView(APIView):
    """
    GET /api/sms/sse/conversations

    Server-Sent Events stream for conversation list updates.
    Notifies when any conversation has new inbound messages.
    Used for list invalidation.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return StreamingHttpResponse(
                self._error_stream('Unauthorized'),
                content_type='text/event-stream',
                status=401
            )

        # Get view mode (self or all for admins)
        view = request.GET.get('view', 'self')

        response = StreamingHttpResponse(
            self._event_stream(user.id, user.agency_id, user.is_admin, view),
            content_type='text/event-stream'
        )
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'
        return response

    def _error_stream(self, message: str):
        """Generate an error event and close stream."""
        yield f"event: error\ndata: {json.dumps({'error': message})}\n\n"

    def _event_stream(
        self, user_id: UUID, agency_id: UUID, is_admin: bool, view: str
    ):
        """Generate SSE events for conversation list changes."""
        start_time = time.time()
        last_check = None

        while True:
            elapsed = time.time() - start_time
            if elapsed > SSE_MAX_DURATION:
                yield f"event: timeout\ndata: {json.dumps({'message': 'Stream timeout'})}\n\n"
                break

            result = self._check_conversation_updates(
                user_id, agency_id, is_admin, view, last_check
            )

            if result.get('has_updates'):
                yield f"event: conversation_update\ndata: {json.dumps({'updated_conversations': result['conversation_ids']})}\n\n"

            if result.get('latest_check'):
                last_check = result['latest_check']

            time.sleep(SSE_POLL_INTERVAL)

    def _check_conversation_updates(
        self,
        user_id: UUID,
        agency_id: UUID,
        is_admin: bool,
        view: str,
        since: Optional[str]
    ) -> dict:
        """Check for conversations with new inbound messages."""
        result = {
            'has_updates': False,
            'conversation_ids': [],
            'latest_check': None,
        }

        with connection.cursor() as cursor:
            now = datetime.utcnow().isoformat()

            if since:
                # Build base query based on view mode
                if view == 'all' and is_admin:
                    # Admin sees all agency conversations
                    cursor.execute("""
                        SELECT DISTINCT c.id
                        FROM messages m
                        JOIN conversations c ON m.conversation_id = c.id
                        WHERE c.agency_id = %s
                          AND m.direction = 'inbound'
                          AND m.created_at > %s
                    """, [str(agency_id), since])
                else:
                    # User sees only their conversations
                    cursor.execute("""
                        SELECT DISTINCT c.id
                        FROM messages m
                        JOIN conversations c ON m.conversation_id = c.id
                        WHERE c.agent_id = %s
                          AND m.direction = 'inbound'
                          AND m.created_at > %s
                    """, [str(user_id), since])

                conversation_ids = [str(row[0]) for row in cursor.fetchall()]

                if conversation_ids:
                    result['has_updates'] = True
                    result['conversation_ids'] = conversation_ids

            result['latest_check'] = now

        return result
