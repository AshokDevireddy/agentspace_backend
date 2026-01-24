"""
SMS Selectors (P2-033 to P2-035)

Complex queries for SMS conversations, messages, and drafts.
Converted to Django ORM for improved maintainability and N+1 prevention.
"""
import logging
from typing import Optional, Literal
from uuid import UUID

from django.db import connection
from django.db.models import Q, Subquery, OuterRef, Count
from django.core.paginator import Paginator

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

    Uses Django ORM with select_related for N+1 prevention.

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'
        page: Page number (1-based)
        limit: Page size
        search_query: Search by client name or phone number

    Returns:
        Dictionary with conversations and pagination
    """
    from apps.core.models import Conversation, Message

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Start with base queryset
        qs = Conversation.objects.filter(agency_id=user.agency_id)

        # Apply view mode filter
        if view_mode == 'all' and is_admin:
            # Admin sees all agency conversations
            pass
        elif view_mode == 'downlines':
            # User sees downline conversations (excluding self)
            visible_ids = get_visible_agent_ids(user, include_full_agency=False)
            visible_ids = [vid for vid in visible_ids if str(vid) != str(user.id)]
            if not visible_ids:
                return {'conversations': [], 'pagination': _empty_pagination(page, limit)}
            qs = qs.filter(agent_id__in=visible_ids)
        else:  # 'self'
            # User sees only their own conversations
            qs = qs.filter(agent_id=user.id)

        # Apply search filter
        if search_query:
            qs = qs.filter(
                Q(phone_number__icontains=search_query) |
                Q(client__first_name__icontains=search_query) |
                Q(client__last_name__icontains=search_query)
            )

        # Add last message content via subquery
        last_message_subquery = (
            Message.objects.filter(conversation_id=OuterRef('id'))
            .order_by('-created_at')
            .values('content')[:1]
        )

        # Optimize with select_related and annotate
        qs = (
            qs.select_related('client', 'agent')
            .annotate(last_message_content=Subquery(last_message_subquery))
            .order_by('-last_message_at', '-id')
        )

        # Get total count before pagination
        total_count = qs.count()

        # Apply pagination
        paginator = Paginator(qs, limit)
        page_obj = paginator.get_page(page)

        # Format results
        conversations = []
        for conv in page_obj:
            conversations.append({
                'id': str(conv.id),
                'phone_number': conv.phone_number,
                'last_message_at': conv.last_message_at.isoformat() if conv.last_message_at else None,
                'unread_count': conv.unread_count or 0,
                'is_archived': conv.is_archived or False,
                'created_at': conv.created_at.isoformat() if conv.created_at else None,
                'last_message': conv.last_message_content,
                'client': {
                    'id': str(conv.client.id) if conv.client else None,
                    'first_name': conv.client.first_name if conv.client else None,
                    'last_name': conv.client.last_name if conv.client else None,
                    'email': conv.client.email if conv.client else None,
                    'name': f"{conv.client.first_name or ''} {conv.client.last_name or ''}".strip() if conv.client else '',
                } if conv.client else None,
                'agent': {
                    'id': str(conv.agent.id) if conv.agent else None,
                    'first_name': conv.agent.first_name if conv.agent else None,
                    'last_name': conv.agent.last_name if conv.agent else None,
                    'name': f"{conv.agent.first_name or ''} {conv.agent.last_name or ''}".strip() if conv.agent else '',
                } if conv.agent else None,
            })

        total_pages = paginator.num_pages

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

    Uses Django ORM with select_related for N+1 prevention.

    Args:
        user: The authenticated user
        conversation_id: The conversation ID
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with messages and pagination
    """
    from apps.core.models import Conversation, Message

    try:
        # Verify access to conversation
        try:
            conversation = Conversation.objects.get(id=conversation_id)
        except Conversation.DoesNotExist:
            return {'messages': [], 'pagination': _empty_pagination(page, limit)}

        # Check agency access
        if str(conversation.agency_id) != str(user.agency_id):
            return {'messages': [], 'pagination': _empty_pagination(page, limit)}

        # Check hierarchy access
        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin and conversation.agent_id:
            visible_ids = get_visible_agent_ids(user, include_full_agency=False)
            if conversation.agent_id not in visible_ids:
                return {'messages': [], 'pagination': _empty_pagination(page, limit)}

        # Get messages with optimized query
        qs = (
            Message.objects.filter(conversation_id=conversation_id)
            .select_related('sent_by')
            .order_by('created_at')
        )

        # Get total count
        total_count = qs.count()

        # Apply pagination
        paginator = Paginator(qs, limit)
        page_obj = paginator.get_page(page)

        # Format results
        messages = []
        for msg in page_obj:
            messages.append({
                'id': str(msg.id),
                'content': msg.content,
                'direction': msg.direction,
                'status': msg.status,
                'is_read': msg.is_read or False,
                'created_at': msg.created_at.isoformat() if msg.created_at else None,
                'sent_by': {
                    'id': str(msg.sent_by.id) if msg.sent_by else None,
                    'name': f"{msg.sent_by.first_name or ''} {msg.sent_by.last_name or ''}".strip() if msg.sent_by else '',
                } if msg.sent_by else None,
            })

        total_pages = paginator.num_pages

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

    Uses Django ORM with select_related for N+1 prevention.

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with drafts and pagination
    """
    from apps.core.models import DraftMessage

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Start with base queryset - only pending drafts
        qs = DraftMessage.objects.filter(
            agency_id=user.agency_id,
            status='pending'
        )

        # Apply view mode filter
        if view_mode == 'all' and is_admin:
            # Admin sees all agency drafts
            pass
        elif view_mode == 'downlines':
            # User sees downline drafts (excluding self)
            visible_ids = get_visible_agent_ids(user, include_full_agency=False)
            visible_ids = [vid for vid in visible_ids if str(vid) != str(user.id)]
            if not visible_ids:
                return {'drafts': [], 'pagination': _empty_pagination(page, limit)}
            qs = qs.filter(agent_id__in=visible_ids)
        else:  # 'self'
            # User sees only their own drafts
            qs = qs.filter(agent_id=user.id)

        # Optimize with select_related
        qs = (
            qs.select_related('agent', 'conversation', 'conversation__client')
            .order_by('-created_at')
        )

        # Get total count
        total_count = qs.count()

        # Apply pagination
        paginator = Paginator(qs, limit)
        page_obj = paginator.get_page(page)

        # Format results
        drafts = []
        for draft in page_obj:
            # Build recipient name from conversation's client
            client_name = ''
            phone_number = None
            if draft.conversation:
                phone_number = draft.conversation.phone_number
                if draft.conversation.client:
                    client = draft.conversation.client
                    client_name = f"{client.first_name or ''} {client.last_name or ''}".strip()

            drafts.append({
                'id': str(draft.id),
                'content': draft.content,
                'status': draft.status,
                'created_at': draft.created_at.isoformat() if draft.created_at else None,
                'recipient_name': client_name or phone_number or '',
                'agent': {
                    'id': str(draft.agent.id) if draft.agent else None,
                    'name': f"{draft.agent.first_name or ''} {draft.agent.last_name or ''}".strip() if draft.agent else '',
                } if draft.agent else None,
                'conversation_id': str(draft.conversation.id) if draft.conversation else None,
            })

        total_pages = paginator.num_pages

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

    Uses Django ORM with django-cte for downlines.
    """
    from apps.core.models import Message, User
    from django.db.models import Value, IntegerField
    from django_cte import With

    is_admin = user.is_admin or user.role == 'admin'

    try:
        if view_mode == 'all' and is_admin:
            return (
                Message.objects.filter(
                    conversation__agent__agency_id=user.agency_id,
                    direction='inbound',
                    is_read=False
                )
                .count()
            )

        elif view_mode == 'self':
            return (
                Message.objects.filter(
                    conversation__agent_id=user.id,
                    direction='inbound',
                    is_read=False
                )
                .count()
            )

        else:
            def make_cte(cte):
                base = User.objects.filter(id=user.id).values('id')
                recursive = (
                    cte.join(User, upline_id=cte.col.id)
                    .values('id')
                )
                return base.union(recursive, all=True)

            cte = With.recursive(make_cte)
            downline_ids = list(
                cte.queryset()
                .with_cte(cte)
                .values_list('id', flat=True)
            )

            return (
                Message.objects.filter(
                    conversation__agent_id__in=downline_ids,
                    direction='inbound',
                    is_read=False
                )
                .count()
            )

    except Exception as e:
        logger.error(f'Error getting unread message count: {e}')
        return 0


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
