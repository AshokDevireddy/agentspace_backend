"""
SMS Selectors (P2-033 to P2-035)

Complex queries for SMS conversations, messages, and drafts.
Converted to Django ORM for improved maintainability and N+1 prevention.
"""
import logging
from typing import Literal
from uuid import UUID

from django.core.paginator import Paginator
from django.db.models import Count, OuterRef, Q, Subquery

from apps.core.authentication import AuthenticatedUser
from apps.core.permissions import get_visible_agent_ids

logger = logging.getLogger(__name__)

ViewMode = Literal['all', 'self', 'downlines']


def get_sms_conversations(
    user: AuthenticatedUser,
    view_mode: ViewMode = 'self',
    page: int = 1,
    limit: int = 20,
    search_query: str | None = None,
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
        # Start with base queryset - filter for SMS type only
        qs = Conversation.objects.filter(agency_id=user.agency_id, type='sms', is_active=True)  # type: ignore[attr-defined]

        # Apply view mode filter
        if view_mode == 'all' and is_admin:
            # Admin sees all agency conversations
            pass
        elif view_mode == 'downlines':
            # User sees conversations for deals where they appear in deal_hierarchy_snapshot
            # This uses deal-level visibility (not org hierarchy) to match RPC behavior
            from apps.core.models import DealHierarchySnapshot

            # Get deal IDs where this user appears in the hierarchy
            # Note: Does NOT exclude user's own deals to match RPC behavior
            visible_deal_ids = (
                DealHierarchySnapshot.objects.filter(agent_id=user.id)  # type: ignore[attr-defined]
                .values_list('deal_id', flat=True)
            )
            if not visible_deal_ids:
                return {'conversations': [], 'pagination': _empty_pagination(page, limit)}
            qs = qs.filter(deal_id__in=visible_deal_ids)
        else:  # 'self'
            # User sees only their own conversations
            qs = qs.filter(agent_id=user.id)

        # Apply search filter
        if search_query:
            qs = qs.filter(
                Q(client_phone__icontains=search_query) |
                Q(client__first_name__icontains=search_query) |
                Q(client__last_name__icontains=search_query)
            )

        # Add last message content via subquery
        last_message_subquery = (
            Message.objects.filter(conversation_id=OuterRef('id'))  # type: ignore[attr-defined]
            .order_by('-created_at')
            .values('content')[:1]
        )

        # Add unread count via subquery (inbound messages without read_at)
        unread_count_subquery = (
            Message.objects.filter(  # type: ignore[attr-defined]
                conversation_id=OuterRef('id'),
                direction='inbound',
                read_at__isnull=True
            )
            .values('conversation_id')
            .annotate(cnt=Count('id'))
            .values('cnt')[:1]
        )

        # Optimize with select_related and annotate
        # Note: Conversation has no client FK, client info comes from deal
        qs = (
            qs.select_related('agent', 'deal')
            .annotate(
                last_message_content=Subquery(last_message_subquery),
                unread_count=Subquery(unread_count_subquery),
            )
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
            # Use client_phone field (model uses client_phone, not phone_number)
            phone = getattr(conv, 'client_phone', None) or getattr(conv, 'phone_number', None)
            # unread_count comes from annotation, is_archived may not exist
            unread = getattr(conv, 'unread_count', 0) or 0
            is_archived = getattr(conv, 'is_archived', False) or False

            # Client info comes from deal (conversations don't have direct client FK)
            client_name = conv.deal.client_name if conv.deal else None
            client_email = conv.deal.client_email if conv.deal else None
            client_first = client_name.split(' ', 1)[0] if client_name else None
            client_last = client_name.split(' ', 1)[1] if client_name and ' ' in client_name else None

            conversations.append({
                'id': str(conv.id),
                'phone_number': phone,
                'deal_id': str(conv.deal_id) if conv.deal_id else None,
                'last_message_at': conv.last_message_at.isoformat() if conv.last_message_at else None,
                'unread_count': unread,
                'is_archived': is_archived,
                'created_at': conv.created_at.isoformat() if conv.created_at else None,
                'last_message': conv.last_message_content,
                'sms_opt_in_status': conv.sms_opt_in_status,
                'opted_in_at': conv.opted_in_at.isoformat() if conv.opted_in_at else None,
                'opted_out_at': conv.opted_out_at.isoformat() if conv.opted_out_at else None,
                'status_standardized': conv.deal.status_standardized if conv.deal else None,
                'client': {
                    'name': client_name or '',
                    'first_name': client_first,
                    'last_name': client_last,
                    'email': client_email,
                } if client_name or client_email else None,
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
            conversation = Conversation.objects.get(id=conversation_id)  # type: ignore[attr-defined]
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

        # Mark inbound messages as read (matches RPC side effect)
        from django.db import connection as db_connection
        with db_connection.cursor() as cursor:
            cursor.execute("""
                UPDATE public.messages m
                SET read_at = NOW()
                FROM public.conversations c
                WHERE m.conversation_id = %s
                    AND m.conversation_id = c.id
                    AND c.agency_id = %s
                    AND m.direction = 'inbound'
                    AND m.read_at IS NULL
            """, [str(conversation_id), str(user.agency_id)])

        # Get messages with optimized query
        qs = (
            Message.objects.filter(conversation_id=conversation_id)  # type: ignore[attr-defined]
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
                'is_read': msg.read_at is not None,
                'created_at': msg.created_at.isoformat() if msg.created_at else None,
                'sent_at': msg.sent_at.isoformat() if msg.sent_at else None,
                'external_id': msg.external_id,
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

    Drafts are messages in the messages table with status='draft'.

    Args:
        user: The authenticated user
        view_mode: 'all' (admin), 'self', or 'downlines'
        page: Page number (1-based)
        limit: Page size

    Returns:
        Dictionary with drafts and pagination
    """
    from apps.core.models import Message

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Start with base queryset - draft messages in the agency (SMS only, active conversations)
        qs = Message.objects.filter(  # type: ignore[attr-defined]
            conversation__agency_id=user.agency_id,
            conversation__type='sms',
            conversation__is_active=True,
            status='draft'
        )

        # Apply view mode filter
        if view_mode == 'all' and is_admin:
            # Admin sees all agency drafts
            pass
        elif view_mode == 'downlines':
            # User sees drafts for conversations linked to deals in their hierarchy
            # Note: Does NOT exclude user's own deals to match RPC behavior
            from apps.core.models import DealHierarchySnapshot

            visible_deal_ids = (
                DealHierarchySnapshot.objects.filter(agent_id=user.id)  # type: ignore[attr-defined]
                .values_list('deal_id', flat=True)
            )
            if not visible_deal_ids:
                return {'drafts': [], 'pagination': _empty_pagination(page, limit)}
            qs = qs.filter(conversation__deal_id__in=visible_deal_ids)
        else:  # 'self'
            # User sees only their own drafts
            qs = qs.filter(conversation__agent_id=user.id)

        # Optimize with select_related
        qs = (
            qs.select_related('conversation', 'conversation__client', 'conversation__agent', 'sent_by')
            .order_by('-created_at')
        )

        # Get total count before pagination
        total_count = qs.count()

        # Apply pagination
        paginator = Paginator(qs, limit)
        page_obj = paginator.get_page(page)

        # Format results
        drafts = []
        for msg in page_obj:
            conv = msg.conversation
            drafts.append({
                'id': str(msg.id),
                'content': msg.content,
                'created_at': msg.created_at.isoformat() if msg.created_at else None,
                'updated_at': msg.updated_at.isoformat() if msg.updated_at else None,
                'conversation_id': str(conv.id) if conv else None,
                'phone_number': conv.phone_number if conv else None,
                'client': {
                    'id': str(conv.client.id) if conv and conv.client else None,
                    'first_name': conv.client.first_name if conv and conv.client else None,
                    'last_name': conv.client.last_name if conv and conv.client else None,
                    'name': f"{conv.client.first_name or ''} {conv.client.last_name or ''}".strip() if conv and conv.client else '',
                } if conv and conv.client else None,
                'agent': {
                    'id': str(conv.agent.id) if conv and conv.agent else None,
                    'first_name': conv.agent.first_name if conv and conv.agent else None,
                    'last_name': conv.agent.last_name if conv and conv.agent else None,
                    'name': f"{conv.agent.first_name or ''} {conv.agent.last_name or ''}".strip() if conv and conv.agent else '',
                } if conv and conv.agent else None,
                'sent_by': {
                    'id': str(msg.sent_by.id) if msg.sent_by else None,
                    'name': f"{msg.sent_by.first_name or ''} {msg.sent_by.last_name or ''}".strip() if msg.sent_by else '',
                } if msg.sent_by else None,
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
    Filters by conversation type='sms' and is_active=True to match RPC behavior.
    """
    from django_cte import With

    from apps.core.models import Message, User

    is_admin = user.is_admin or user.role == 'admin'

    try:
        # Base filter matching RPC: type='sms' and is_active=True
        base_filter = {
            'conversation__type': 'sms',
            'conversation__is_active': True,
            'direction': 'inbound',
            'read_at__isnull': True,
        }

        if view_mode == 'all' and is_admin:
            return (
                Message.objects.filter(  # type: ignore[attr-defined]
                    conversation__agent__agency_id=user.agency_id,
                    **base_filter
                )
                .count()
            )

        elif view_mode == 'self':
            return (
                Message.objects.filter(  # type: ignore[attr-defined]
                    conversation__agent_id=user.id,
                    **base_filter
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
                Message.objects.filter(  # type: ignore[attr-defined]
                    conversation__agent_id__in=downline_ids,
                    **base_filter
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
