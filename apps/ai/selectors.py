"""
AI Selectors (P1-015)

Query functions for AI conversations and messages.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


def get_ai_conversations(
    user: AuthenticatedUser,
    page: int = 1,
    limit: int = 20,
) -> dict:
    """
    Get AI conversations for a user.

    Args:
        user: The authenticated user
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with conversations and pagination
    """
    offset = (page - 1) * limit

    # Count query
    count_query = """
        SELECT COUNT(*)
        FROM public.ai_conversations
        WHERE user_id = %s AND agency_id = %s
    """

    # Main query
    main_query = """
        SELECT
            c.id,
            c.title,
            c.is_active,
            c.created_at,
            c.updated_at,
            (
                SELECT COUNT(*)
                FROM public.ai_messages m
                WHERE m.conversation_id = c.id
            ) as message_count
        FROM public.ai_conversations c
        WHERE c.user_id = %s AND c.agency_id = %s
        ORDER BY c.updated_at DESC, c.id DESC
        LIMIT %s OFFSET %s
    """

    try:
        params = [str(user.id), str(user.agency_id)]

        with connection.cursor() as cursor:
            # Get total count
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]

            # Get conversations
            cursor.execute(main_query, params + [limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        conversations = []
        for row in rows:
            conv = dict(zip(columns, row))
            conversations.append({
                'id': str(conv['id']),
                'title': conv['title'],
                'is_active': conv['is_active'],
                'message_count': conv['message_count'] or 0,
                'created_at': conv['created_at'].isoformat() if conv['created_at'] else None,
                'updated_at': conv['updated_at'].isoformat() if conv['updated_at'] else None,
            })

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            'conversations': conversations,
            'pagination': {
                'currentPage': page,
                'totalPages': total_pages,
                'totalCount': total_count,
                'limit': limit,
                'hasNextPage': page < total_pages,
                'hasPrevPage': page > 1,
            },
        }

    except Exception as e:
        logger.error(f'Error getting AI conversations: {e}')
        raise


def get_ai_conversation_detail(
    user: AuthenticatedUser,
    conversation_id: UUID,
) -> Optional[dict]:
    """
    Get a single AI conversation with its messages.

    Args:
        user: The authenticated user
        conversation_id: The conversation ID

    Returns:
        Dictionary with conversation details and messages, or None if not found
    """
    # Get conversation
    conversation_query = """
        SELECT
            id,
            title,
            user_id,
            agency_id,
            is_active,
            created_at,
            updated_at
        FROM public.ai_conversations
        WHERE id = %s AND user_id = %s AND agency_id = %s
        LIMIT 1
    """

    # Get messages
    messages_query = """
        SELECT
            id,
            role,
            content,
            tool_calls,
            tool_results,
            created_at
        FROM public.ai_messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
    """

    try:
        with connection.cursor() as cursor:
            # Get conversation
            cursor.execute(conversation_query, [
                str(conversation_id),
                str(user.id),
                str(user.agency_id)
            ])
            row = cursor.fetchone()

            if not row:
                return None

            columns = [col[0] for col in cursor.description]
            conv = dict(zip(columns, row))

            # Get messages
            cursor.execute(messages_query, [str(conversation_id)])
            msg_columns = [col[0] for col in cursor.description]
            msg_rows = cursor.fetchall()

        messages = []
        for msg_row in msg_rows:
            msg = dict(zip(msg_columns, msg_row))
            messages.append({
                'id': str(msg['id']),
                'role': msg['role'],
                'content': msg['content'],
                'tool_calls': msg['tool_calls'],
                'tool_results': msg['tool_results'],
                'created_at': msg['created_at'].isoformat() if msg['created_at'] else None,
            })

        return {
            'id': str(conv['id']),
            'title': conv['title'],
            'user_id': str(conv['user_id']),
            'agency_id': str(conv['agency_id']),
            'is_active': conv['is_active'],
            'messages': messages,
            'created_at': conv['created_at'].isoformat() if conv['created_at'] else None,
            'updated_at': conv['updated_at'].isoformat() if conv['updated_at'] else None,
        }

    except Exception as e:
        logger.error(f'Error getting AI conversation detail: {e}')
        raise


def get_ai_messages(
    user: AuthenticatedUser,
    conversation_id: UUID,
    page: int = 1,
    limit: int = 50,
) -> dict:
    """
    Get paginated messages for an AI conversation.

    Args:
        user: The authenticated user
        conversation_id: The conversation ID
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with messages and pagination
    """
    offset = (page - 1) * limit

    # Verify user owns the conversation
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT user_id, agency_id
            FROM public.ai_conversations
            WHERE id = %s
        """, [str(conversation_id)])
        row = cursor.fetchone()

    if not row:
        return {'messages': [], 'pagination': _empty_pagination(page, limit)}

    owner_user_id, owner_agency_id = row

    # Check access
    if str(owner_user_id) != str(user.id) or str(owner_agency_id) != str(user.agency_id):
        return {'messages': [], 'pagination': _empty_pagination(page, limit)}

    # Count query
    count_query = """
        SELECT COUNT(*)
        FROM public.ai_messages
        WHERE conversation_id = %s
    """

    # Main query
    main_query = """
        SELECT
            id,
            role,
            content,
            tool_calls,
            tool_results,
            created_at
        FROM public.ai_messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
        LIMIT %s OFFSET %s
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(count_query, [str(conversation_id)])
            total_count = cursor.fetchone()[0]

            cursor.execute(main_query, [str(conversation_id), limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        messages = []
        for row in rows:
            msg = dict(zip(columns, row))
            messages.append({
                'id': str(msg['id']),
                'role': msg['role'],
                'content': msg['content'],
                'tool_calls': msg['tool_calls'],
                'tool_results': msg['tool_results'],
                'created_at': msg['created_at'].isoformat() if msg['created_at'] else None,
            })

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            'messages': messages,
            'pagination': {
                'currentPage': page,
                'totalPages': total_pages,
                'totalCount': total_count,
                'limit': limit,
                'hasNextPage': page < total_pages,
                'hasPrevPage': page > 1,
            },
        }

    except Exception as e:
        logger.error(f'Error getting AI messages: {e}')
        raise


def _empty_pagination(page: int, limit: int) -> dict:
    """Return empty pagination structure."""
    return {
        'currentPage': page,
        'totalPages': 0,
        'totalCount': 0,
        'limit': limit,
        'hasNextPage': False,
        'hasPrevPage': False,
    }
