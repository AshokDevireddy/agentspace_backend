"""
SMS Selectors (P2-033 to P2-035)

Complex queries for SMS conversations, messages, and drafts.
"""
import logging
from typing import Optional, Literal
from uuid import UUID

from django.db import connection

from apps.core.permissions import get_visible_agent_ids
from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)

ViewMode = Literal['all', 'self', 'downlines']


def get_sms_conversations(
    user: AuthenticatedUser,
    view_mode: ViewMode = 'self',
    page: int = 1,
    limit: int = 20,
    search_query: Optional[str] = None,
) -> dict:
    """
    Get SMS conversations based on view mode.

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'
        page: Page number (1-based)
        limit: Page size
        search_query: Search by client name or phone number

    Returns:
        Dictionary with conversations and pagination
    """
    is_admin = user.is_admin or user.role == 'admin'
    offset = (page - 1) * limit

    # Build agent filter based on view mode
    if view_mode == 'all' and is_admin:
        # Admin sees all agency conversations
        agent_filter = "c.agency_id = %s"
        agent_params = [str(user.agency_id)]
    elif view_mode == 'downlines':
        # User sees downline conversations
        visible_ids = get_visible_agent_ids(user, include_full_agency=False)
        # Exclude self for 'downlines' mode
        visible_ids = [vid for vid in visible_ids if str(vid) != str(user.id)]
        if not visible_ids:
            return {'conversations': [], 'pagination': _empty_pagination(page, limit)}
        visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)
        agent_filter = f"c.agency_id = %s AND c.agent_id IN ({visible_ids_str})"
        agent_params = [str(user.agency_id)]
    else:  # 'self'
        # User sees only their own conversations
        agent_filter = "c.agency_id = %s AND c.agent_id = %s"
        agent_params = [str(user.agency_id), str(user.id)]

    # Build search filter
    search_filter = ""
    search_params = []
    if search_query:
        search_filter = """
            AND (c.phone_number ILIKE %s
                 OR cl.first_name ILIKE %s
                 OR cl.last_name ILIKE %s
                 OR CONCAT(cl.first_name, ' ', cl.last_name) ILIKE %s)
        """
        search_pattern = f"%{search_query}%"
        search_params = [search_pattern] * 4

    # Count query
    count_query = f"""
        SELECT COUNT(*)
        FROM public.conversations c
        LEFT JOIN public.clients cl ON cl.id = c.client_id
        WHERE {agent_filter} {search_filter}
    """

    # Main query
    main_query = f"""
        SELECT
            c.id,
            c.phone_number,
            c.last_message_at,
            c.unread_count,
            c.is_archived,
            c.created_at,
            cl.id as client_id,
            cl.first_name as client_first_name,
            cl.last_name as client_last_name,
            cl.email as client_email,
            u.id as agent_id,
            u.first_name as agent_first_name,
            u.last_name as agent_last_name,
            (
                SELECT m.content
                FROM public.messages m
                WHERE m.conversation_id = c.id
                ORDER BY m.created_at DESC
                LIMIT 1
            ) as last_message
        FROM public.conversations c
        LEFT JOIN public.clients cl ON cl.id = c.client_id
        LEFT JOIN public.users u ON u.id = c.agent_id
        WHERE {agent_filter} {search_filter}
        ORDER BY c.last_message_at DESC NULLS LAST, c.id DESC
        LIMIT %s OFFSET %s
    """

    try:
        params = agent_params + search_params

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
                'phone_number': conv['phone_number'],
                'last_message_at': conv['last_message_at'].isoformat() if conv['last_message_at'] else None,
                'unread_count': conv['unread_count'] or 0,
                'is_archived': conv['is_archived'] or False,
                'created_at': conv['created_at'].isoformat() if conv['created_at'] else None,
                'last_message': conv['last_message'],
                'client': {
                    'id': str(conv['client_id']) if conv['client_id'] else None,
                    'first_name': conv['client_first_name'],
                    'last_name': conv['client_last_name'],
                    'email': conv['client_email'],
                    'name': f"{conv['client_first_name'] or ''} {conv['client_last_name'] or ''}".strip(),
                } if conv['client_id'] else None,
                'agent': {
                    'id': str(conv['agent_id']) if conv['agent_id'] else None,
                    'first_name': conv['agent_first_name'],
                    'last_name': conv['agent_last_name'],
                    'name': f"{conv['agent_first_name'] or ''} {conv['agent_last_name'] or ''}".strip(),
                } if conv['agent_id'] else None,
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
        logger.error(f'Error getting SMS conversations: {e}')
        raise


def get_sms_messages(
    user: AuthenticatedUser,
    conversation_id: UUID,
    page: int = 1,
    limit: int = 50,
) -> dict:
    """
    Get messages for a conversation.

    Args:
        user: The authenticated user
        conversation_id: The conversation ID
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with messages and pagination
    """
    offset = (page - 1) * limit

    # Verify access to conversation
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT agent_id, agency_id
            FROM public.conversations
            WHERE id = %s
        """, [str(conversation_id)])
        row = cursor.fetchone()

    if not row:
        return {'messages': [], 'pagination': _empty_pagination(page, limit)}

    agent_id, agency_id = row

    # Check access
    if str(agency_id) != str(user.agency_id):
        return {'messages': [], 'pagination': _empty_pagination(page, limit)}

    is_admin = user.is_admin or user.role == 'admin'
    if not is_admin and agent_id:
        visible_ids = get_visible_agent_ids(user, include_full_agency=False)
        if agent_id not in visible_ids:
            return {'messages': [], 'pagination': _empty_pagination(page, limit)}

    # Get messages
    count_query = """
        SELECT COUNT(*)
        FROM public.messages
        WHERE conversation_id = %s
    """

    main_query = """
        SELECT
            m.id,
            m.content,
            m.direction,
            m.status,
            m.external_id,
            m.is_read,
            m.created_at,
            u.id as sent_by_id,
            u.first_name as sent_by_first_name,
            u.last_name as sent_by_last_name
        FROM public.messages m
        LEFT JOIN public.users u ON u.id = m.sent_by
        WHERE m.conversation_id = %s
        ORDER BY m.created_at ASC
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
                'content': msg['content'],
                'direction': msg['direction'],
                'status': msg['status'],
                'is_read': msg['is_read'] or False,
                'created_at': msg['created_at'].isoformat() if msg['created_at'] else None,
                'sent_by': {
                    'id': str(msg['sent_by_id']) if msg['sent_by_id'] else None,
                    'name': f"{msg['sent_by_first_name'] or ''} {msg['sent_by_last_name'] or ''}".strip(),
                } if msg['sent_by_id'] else None,
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
        logger.error(f'Error getting SMS messages: {e}')
        raise


def get_draft_messages(
    user: AuthenticatedUser,
    view_mode: ViewMode = 'self',
    page: int = 1,
    limit: int = 20,
) -> dict:
    """
    Get draft messages based on view mode.

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with drafts and pagination
    """
    is_admin = user.is_admin or user.role == 'admin'
    offset = (page - 1) * limit

    # Build agent filter based on view mode
    if view_mode == 'all' and is_admin:
        agent_filter = "d.agency_id = %s"
        agent_params = [str(user.agency_id)]
    elif view_mode == 'downlines':
        visible_ids = get_visible_agent_ids(user, include_full_agency=False)
        visible_ids = [vid for vid in visible_ids if str(vid) != str(user.id)]
        if not visible_ids:
            return {'drafts': [], 'pagination': _empty_pagination(page, limit)}
        visible_ids_str = ','.join(f"'{str(vid)}'" for vid in visible_ids)
        agent_filter = f"d.agency_id = %s AND d.agent_id IN ({visible_ids_str})"
        agent_params = [str(user.agency_id)]
    else:  # 'self'
        agent_filter = "d.agency_id = %s AND d.agent_id = %s"
        agent_params = [str(user.agency_id), str(user.id)]

    # Count query
    count_query = f"""
        SELECT COUNT(*)
        FROM public.draft_messages d
        WHERE {agent_filter} AND d.status = 'pending'
    """

    # Main query
    main_query = f"""
        SELECT
            d.id,
            d.content,
            d.status,
            d.created_at,
            d.rejection_reason,
            u.id as agent_id,
            u.first_name as agent_first_name,
            u.last_name as agent_last_name,
            c.id as conversation_id,
            c.phone_number,
            cl.first_name as client_first_name,
            cl.last_name as client_last_name
        FROM public.draft_messages d
        LEFT JOIN public.users u ON u.id = d.agent_id
        LEFT JOIN public.conversations c ON c.id = d.conversation_id
        LEFT JOIN public.clients cl ON cl.id = c.client_id
        WHERE {agent_filter} AND d.status = 'pending'
        ORDER BY d.created_at DESC
        LIMIT %s OFFSET %s
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(count_query, agent_params)
            total_count = cursor.fetchone()[0]

            cursor.execute(main_query, agent_params + [limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        drafts = []
        for row in rows:
            draft = dict(zip(columns, row))
            client_name = f"{draft['client_first_name'] or ''} {draft['client_last_name'] or ''}".strip()
            drafts.append({
                'id': str(draft['id']),
                'content': draft['content'],
                'status': draft['status'],
                'created_at': draft['created_at'].isoformat() if draft['created_at'] else None,
                'recipient_name': client_name or draft['phone_number'],
                'agent': {
                    'id': str(draft['agent_id']) if draft['agent_id'] else None,
                    'name': f"{draft['agent_first_name'] or ''} {draft['agent_last_name'] or ''}".strip(),
                } if draft['agent_id'] else None,
                'conversation_id': str(draft['conversation_id']) if draft['conversation_id'] else None,
            })

        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        return {
            'drafts': drafts,
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
        logger.error(f'Error getting draft messages: {e}')
        raise


def get_unread_message_count(
    user: AuthenticatedUser,
    view_mode: ViewMode = 'self',
) -> int:
    """
    Get count of unread inbound messages.
    Translated from Supabase RPC: get_unread_message_count

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'

    Returns:
        Count of unread messages
    """
    is_admin = user.is_admin or user.role == 'admin'

    with connection.cursor() as cursor:
        if view_mode == 'all' and is_admin:
            # Admin: count all unread messages in agency
            cursor.execute("""
                SELECT COUNT(*)::INTEGER
                FROM messages m
                JOIN conversations c ON m.conversation_id = c.id
                JOIN users u ON c.agent_id = u.id
                WHERE u.agency_id = %s
                    AND m.direction = 'inbound'
                    AND m.read_at IS NULL
            """, [str(user.agency_id)])

        elif view_mode == 'self':
            # Self: count unread messages for user's own conversations
            cursor.execute("""
                SELECT COUNT(*)::INTEGER
                FROM messages m
                JOIN conversations c ON m.conversation_id = c.id
                WHERE c.agent_id = %s
                    AND m.direction = 'inbound'
                    AND m.read_at IS NULL
            """, [str(user.id)])

        else:  # 'downlines'
            # Downlines: count unread messages for user and their downlines
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT id FROM users WHERE id = %s
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE d.id <> u.id  -- Prevent cycles
                )
                SELECT COUNT(*)::INTEGER
                FROM messages m
                JOIN conversations c ON m.conversation_id = c.id
                WHERE c.agent_id IN (SELECT id FROM downline)
                    AND m.direction = 'inbound'
                    AND m.read_at IS NULL
            """, [str(user.id)])

        count = cursor.fetchone()[0]

    return count or 0


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
