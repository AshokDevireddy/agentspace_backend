"""
SMS Service

Translated from Supabase RPC functions related to SMS conversations
and messaging.

Priority: P1 - SMS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from django.db import connection
from django.utils import timezone

from .base import BaseService

# ============================================================================
# Data Transfer Objects (DTOs)
# ============================================================================

@dataclass
class SMSConversation:
    """Result from get_sms_conversations_* functions."""
    conversation_id: UUID
    deal_id: UUID
    agent_id: UUID
    client_name: str
    client_phone: str
    last_message: str
    last_message_at: datetime | None
    unread_count: int
    sms_opt_in_status: str | None
    opted_in_at: datetime | None
    opted_out_at: datetime | None
    status_standardized: str | None


@dataclass
class ConversationMessage:
    """Result from get_conversation_messages."""
    id: UUID
    conversation_id: UUID
    sender_id: UUID | None
    receiver_id: UUID | None
    body: str
    direction: str  # 'inbound' or 'outbound'
    message_type: str
    sent_at: datetime | None
    status: str
    metadata: dict[str, Any] | None
    read_at: datetime | None


# ============================================================================
# SMS Service Implementation
# ============================================================================

class SMSService(BaseService):
    """
    Service for SMS conversation operations.

    Handles:
    - Retrieving SMS conversations (self, downlines, all)
    - Reading conversation messages
    - Marking messages as read
    """

    # ========================================================================
    # P1 - SMS Conversation Functions
    # ========================================================================

    def get_sms_conversations_self(self) -> list[SMSConversation]:
        """
        Translated from Supabase RPC: get_sms_conversations_self

        Get SMS conversations where user is the assigned agent.

        Original SQL Logic:
        - Filters conversations where agent_id = user AND type = 'sms' AND is_active
        - Gets last message (most recent by sent_at per conversation)
        - Counts unread messages (direction = 'inbound' AND read_at IS NULL)
        - Joins with deals for client info and status
        - Orders by last_message_at DESC

        Returns:
            List[SMSConversation]: User's personal SMS conversations
        """
        query = """
            WITH last_messages AS (
                SELECT DISTINCT ON (m.conversation_id)
                    m.conversation_id,
                    m.body,
                    m.sent_at
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                WHERE c.agent_id = %s
                    AND c.type = 'sms'
                    AND c.is_active = true
                ORDER BY m.conversation_id, m.sent_at DESC
            ),
            unread_counts AS (
                SELECT m.conversation_id, COUNT(*) as unread_count
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                WHERE c.agent_id = %s
                    AND c.type = 'sms'
                    AND c.is_active = true
                    AND m.direction = 'inbound'
                    AND m.read_at IS NULL
                GROUP BY m.conversation_id
            )
            SELECT
                c.id as conversation_id,
                c.deal_id,
                c.agent_id,
                d.client_name,
                COALESCE(d.client_phone, '') as client_phone,
                COALESCE(lm.body, '') as last_message,
                c.last_message_at,
                COALESCE(uc.unread_count, 0) as unread_count,
                c.sms_opt_in_status,
                c.opted_in_at,
                c.opted_out_at,
                d.status_standardized
            FROM conversations c
            INNER JOIN deals d ON d.id = c.deal_id
            LEFT JOIN last_messages lm ON lm.conversation_id = c.id
            LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
            WHERE c.agent_id = %s
                AND c.type = 'sms'
                AND c.is_active = true
            ORDER BY c.last_message_at DESC NULLS LAST
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [str(self.user_id), str(self.user_id), str(self.user_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return [
            SMSConversation(
                conversation_id=row[columns.index('conversation_id')],
                deal_id=row[columns.index('deal_id')],
                agent_id=row[columns.index('agent_id')],
                client_name=row[columns.index('client_name')] or '',
                client_phone=row[columns.index('client_phone')],
                last_message=row[columns.index('last_message')],
                last_message_at=row[columns.index('last_message_at')],
                unread_count=row[columns.index('unread_count')],
                sms_opt_in_status=row[columns.index('sms_opt_in_status')],
                opted_in_at=row[columns.index('opted_in_at')],
                opted_out_at=row[columns.index('opted_out_at')],
                status_standardized=row[columns.index('status_standardized')],
            )
            for row in rows
        ]

    def get_sms_conversations_downlines(self) -> list[SMSConversation]:
        """
        Translated from Supabase RPC: get_sms_conversations_downlines

        Get SMS conversations for user's downline hierarchy.

        Original SQL Logic:
        - Uses deal_hierarchy_snapshot to get visible deals
        - Gets conversations for those deals where type = 'sms' AND is_active
        - Same last_message and unread_count logic as self view
        - Joins with deals for client info
        - Orders by last_message_at DESC

        Returns:
            List[SMSConversation]: Downline SMS conversations
        """
        query = """
            WITH visible_deals AS (
                SELECT DISTINCT dhs.deal_id
                FROM deal_hierarchy_snapshot dhs
                WHERE dhs.agent_id = %s
            ),
            last_messages AS (
                SELECT DISTINCT ON (m.conversation_id)
                    m.conversation_id,
                    m.body,
                    m.sent_at
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
                WHERE c.type = 'sms' AND c.is_active = true
                ORDER BY m.conversation_id, m.sent_at DESC
            ),
            unread_counts AS (
                SELECT m.conversation_id, COUNT(*) AS unread_count
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
                WHERE c.type = 'sms' AND c.is_active = true
                    AND m.direction = 'inbound' AND m.read_at IS NULL
                GROUP BY m.conversation_id
            )
            SELECT
                c.id AS conversation_id,
                c.deal_id,
                c.agent_id,
                d.client_name,
                COALESCE(d.client_phone, '') as client_phone,
                COALESCE(lm.body, '') as last_message,
                c.last_message_at,
                COALESCE(uc.unread_count, 0) as unread_count,
                c.sms_opt_in_status,
                c.opted_in_at,
                c.opted_out_at,
                d.status_standardized
            FROM conversations c
            INNER JOIN deals d ON d.id = c.deal_id
            INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
            LEFT JOIN last_messages lm ON lm.conversation_id = c.id
            LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
            WHERE c.type = 'sms' AND c.is_active = true
            ORDER BY c.last_message_at DESC NULLS LAST
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [str(self.user_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return [
            SMSConversation(
                conversation_id=row[columns.index('conversation_id')],
                deal_id=row[columns.index('deal_id')],
                agent_id=row[columns.index('agent_id')],
                client_name=row[columns.index('client_name')] or '',
                client_phone=row[columns.index('client_phone')],
                last_message=row[columns.index('last_message')],
                last_message_at=row[columns.index('last_message_at')],
                unread_count=row[columns.index('unread_count')],
                sms_opt_in_status=row[columns.index('sms_opt_in_status')],
                opted_in_at=row[columns.index('opted_in_at')],
                opted_out_at=row[columns.index('opted_out_at')],
                status_standardized=row[columns.index('status_standardized')],
            )
            for row in rows
        ]

    def get_sms_conversations_all(self) -> list[SMSConversation]:
        """
        Translated from Supabase RPC: get_sms_conversations_all

        Get all SMS conversations in the agency (admin only).

        Original SQL Logic:
        - Only admins can use this function (raises exception otherwise)
        - Gets all conversations in user's agency where type = 'sms' AND is_active
        - Same last_message and unread_count logic
        - Orders by last_message_at DESC

        Returns:
            List[SMSConversation]: All agency SMS conversations

        Raises:
            PermissionError: If user is not an admin
        """
        if not self.is_admin:
            raise PermissionError("Unauthorized: Only admins can view all conversations")

        query = """
            WITH last_messages AS (
                SELECT DISTINCT ON (m.conversation_id)
                    m.conversation_id,
                    m.body,
                    m.sent_at
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                WHERE c.agency_id = %s
                    AND c.type = 'sms' AND c.is_active = true
                ORDER BY m.conversation_id, m.sent_at DESC
            ),
            unread_counts AS (
                SELECT m.conversation_id, COUNT(*) as unread_count
                FROM messages m
                INNER JOIN conversations c ON c.id = m.conversation_id
                WHERE c.agency_id = %s
                    AND c.type = 'sms' AND c.is_active = true
                    AND m.direction = 'inbound' AND m.read_at IS NULL
                GROUP BY m.conversation_id
            )
            SELECT
                c.id as conversation_id,
                c.deal_id,
                c.agent_id,
                d.client_name,
                COALESCE(d.client_phone, '') as client_phone,
                COALESCE(lm.body, '') as last_message,
                c.last_message_at,
                COALESCE(uc.unread_count, 0) as unread_count,
                c.sms_opt_in_status,
                c.opted_in_at,
                c.opted_out_at,
                d.status_standardized
            FROM conversations c
            INNER JOIN deals d ON d.id = c.deal_id
            LEFT JOIN last_messages lm ON lm.conversation_id = c.id
            LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
            WHERE c.agency_id = %s
                AND c.type = 'sms' AND c.is_active = true
            ORDER BY c.last_message_at DESC NULLS LAST
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [str(self.agency_id), str(self.agency_id), str(self.agency_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return [
            SMSConversation(
                conversation_id=row[columns.index('conversation_id')],
                deal_id=row[columns.index('deal_id')],
                agent_id=row[columns.index('agent_id')],
                client_name=row[columns.index('client_name')] or '',
                client_phone=row[columns.index('client_phone')],
                last_message=row[columns.index('last_message')],
                last_message_at=row[columns.index('last_message_at')],
                unread_count=row[columns.index('unread_count')],
                sms_opt_in_status=row[columns.index('sms_opt_in_status')],
                opted_in_at=row[columns.index('opted_in_at')],
                opted_out_at=row[columns.index('opted_out_at')],
                status_standardized=row[columns.index('status_standardized')],
            )
            for row in rows
        ]

    # ========================================================================
    # P1 - Message Functions
    # ========================================================================

    def get_conversation_messages(
        self,
        conversation_id: UUID,
        view: str = 'downlines'
    ) -> list[ConversationMessage]:
        """
        Translated from Supabase RPC: get_conversation_messages

        Get all messages in a conversation with permission checking.

        Original SQL Logic:
        - Permission check based on view mode:
          - 'self': conversation.agent_id must equal user
          - 'all': Admin must be in same agency as conversation
          - 'downlines': Conversation's deal must be in user's hierarchy
        - Marks inbound messages as read (UPDATE read_at = NOW())
        - Returns messages sorted by sent_at ASC
          - Drafts (sent_at = NULL) appear at bottom

        Args:
            conversation_id: The conversation to get messages for
            view: View mode for permission checking ('self', 'downlines', 'all')

        Returns:
            List[ConversationMessage]: Conversation messages

        Raises:
            PermissionError: If user doesn't have access to conversation
            ValueError: If conversation not found
        """
        # Normalize view mode
        normalized_view = view.lower() if view else 'downlines'
        if normalized_view not in ('self', 'downlines', 'all'):
            normalized_view = 'downlines'

        # First, check if conversation exists and get its details
        conversation_query = """
            SELECT c.id, c.agent_id, c.agency_id, c.deal_id
            FROM conversations c
            WHERE c.id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(conversation_query, [str(conversation_id)])
            conv_row = cursor.fetchone()

        if not conv_row:
            raise ValueError(f"Conversation not found: {conversation_id}")

        conv_agent_id = conv_row[1]
        conv_agency_id = conv_row[2]
        conv_deal_id = conv_row[3]

        # Permission check based on view mode
        can_view = False

        if normalized_view == 'self':
            can_view = str(conv_agent_id) == str(self.user_id)
        elif normalized_view == 'all':
            can_view = self.is_admin and str(conv_agency_id) == str(self.agency_id)
        else:  # 'downlines'
            # Check if conversation's deal is in user's hierarchy
            hierarchy_check = """
                SELECT 1 FROM deal_hierarchy_snapshot dhs
                WHERE dhs.agent_id = %s AND dhs.deal_id = %s
                LIMIT 1
            """
            with connection.cursor() as cursor:
                cursor.execute(hierarchy_check, [str(self.user_id), str(conv_deal_id)])
                can_view = cursor.fetchone() is not None

        if not can_view:
            raise PermissionError("You do not have permission to view this conversation")

        # Mark inbound messages as read
        mark_read_query = """
            UPDATE messages
            SET read_at = %s
            WHERE conversation_id = %s
                AND direction = 'inbound'
                AND read_at IS NULL
        """
        with connection.cursor() as cursor:
            cursor.execute(mark_read_query, [timezone.now(), str(conversation_id)])

        # Get all messages sorted by sent_at (drafts at bottom)
        messages_query = """
            SELECT
                m.id,
                m.conversation_id,
                m.sender_id,
                m.receiver_id,
                m.body,
                m.direction,
                m.message_type,
                m.sent_at,
                m.status,
                m.metadata,
                m.read_at
            FROM messages m
            WHERE m.conversation_id = %s
            ORDER BY
                CASE WHEN m.sent_at IS NULL THEN 1 ELSE 0 END,
                m.sent_at ASC,
                m.id
        """

        with connection.cursor() as cursor:
            cursor.execute(messages_query, [str(conversation_id)])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return [
            ConversationMessage(
                id=row[columns.index('id')],
                conversation_id=row[columns.index('conversation_id')],
                sender_id=row[columns.index('sender_id')],
                receiver_id=row[columns.index('receiver_id')],
                body=row[columns.index('body')] or '',
                direction=row[columns.index('direction')],
                message_type=row[columns.index('message_type')] or 'sms',
                sent_at=row[columns.index('sent_at')],
                status=row[columns.index('status')] or 'delivered',
                metadata=row[columns.index('metadata')],
                read_at=row[columns.index('read_at')],
            )
            for row in rows
        ]

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _check_conversation_access(
        self,
        conversation_id: UUID,
        view: str
    ) -> bool:
        """
        Check if user has access to a conversation based on view mode.

        Args:
            conversation_id: The conversation to check
            view: View mode ('self', 'downlines', 'all')

        Returns:
            bool: True if user has access
        """
        normalized_view = view.lower() if view else 'downlines'

        # Get conversation details
        conversation_query = """
            SELECT c.id, c.agent_id, c.agency_id, c.deal_id
            FROM conversations c
            WHERE c.id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(conversation_query, [str(conversation_id)])
            conv_row = cursor.fetchone()

        if not conv_row:
            return False

        conv_agent_id = conv_row[1]
        conv_agency_id = conv_row[2]
        conv_deal_id = conv_row[3]

        if normalized_view == 'self':
            return str(conv_agent_id) == str(self.user_id)
        elif normalized_view == 'all':
            return self.is_admin and str(conv_agency_id) == str(self.agency_id)
        else:  # 'downlines'
            hierarchy_check = """
                SELECT 1 FROM deal_hierarchy_snapshot dhs
                WHERE dhs.agent_id = %s AND dhs.deal_id = %s
                LIMIT 1
            """
            with connection.cursor() as cursor:
                cursor.execute(hierarchy_check, [str(self.user_id), str(conv_deal_id)])
                return cursor.fetchone() is not None


# ============================================================================
# Type Aliases for External Use
# ============================================================================

SMSServiceResult = SMSConversation | ConversationMessage
