"""
SMS Service

Translated from Supabase RPC functions related to SMS conversations
and messaging.

Priority: P1 - SMS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

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
    last_message_at: Optional[datetime]
    unread_count: int
    sms_opt_in_status: Optional[str]
    opted_in_at: Optional[datetime]
    opted_out_at: Optional[datetime]
    status_standardized: Optional[str]


@dataclass
class ConversationMessage:
    """Result from get_conversation_messages."""
    id: UUID
    conversation_id: UUID
    sender_id: Optional[UUID]
    receiver_id: Optional[UUID]
    body: str
    direction: str  # 'inbound' or 'outbound'
    message_type: str
    sent_at: Optional[datetime]
    status: str
    metadata: Optional[Dict[str, Any]]
    read_at: Optional[datetime]


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

    def get_sms_conversations_self(self) -> List[SMSConversation]:
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
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH last_messages AS (
        #   SELECT DISTINCT ON (m.conversation_id)
        #     m.conversation_id, m.body, m.sent_at
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   WHERE c.agent_id = p_user_id
        #     AND c.type = 'sms'
        #     AND c.is_active = true
        #   ORDER BY m.conversation_id, m.sent_at DESC
        # ),
        # unread_counts AS (
        #   SELECT m.conversation_id, COUNT(*) as unread_count
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   WHERE c.agent_id = p_user_id
        #     AND c.type = 'sms'
        #     AND c.is_active = true
        #     AND m.direction = 'inbound'
        #     AND m.read_at IS NULL
        #   GROUP BY m.conversation_id
        # )
        # SELECT c.id as conversation_id, c.deal_id, c.agent_id,
        #        d.client_name, COALESCE(d.client_phone, ''),
        #        COALESCE(lm.body, ''), c.last_message_at,
        #        COALESCE(uc.unread_count, 0), c.sms_opt_in_status,
        #        c.opted_in_at, c.opted_out_at, d.status_standardized
        # FROM conversations c
        # INNER JOIN deals d ON d.id = c.deal_id
        # LEFT JOIN last_messages lm ON lm.conversation_id = c.id
        # LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
        # WHERE c.agent_id = p_user_id
        #   AND c.type = 'sms'
        #   AND c.is_active = true
        # ORDER BY c.last_message_at DESC;
        pass

    def get_sms_conversations_downlines(self) -> List[SMSConversation]:
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
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH visible_deals AS (
        #   SELECT DISTINCT dhs.deal_id
        #   FROM deal_hierarchy_snapshot dhs
        #   WHERE dhs.agent_id = p_user_id
        # ),
        # last_messages AS (
        #   SELECT DISTINCT ON (m.conversation_id)
        #     m.conversation_id, m.body, m.sent_at
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
        #   WHERE c.type = 'sms' AND c.is_active = true
        #   ORDER BY m.conversation_id, m.sent_at DESC
        # ),
        # unread_counts AS (
        #   SELECT m.conversation_id, COUNT(*) AS unread_count
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
        #   WHERE c.type = 'sms' AND c.is_active = true
        #     AND m.direction = 'inbound' AND m.read_at IS NULL
        #   GROUP BY m.conversation_id
        # )
        # SELECT c.id AS conversation_id, c.deal_id, c.agent_id,
        #        d.client_name, COALESCE(d.client_phone, ''),
        #        COALESCE(lm.body, ''), c.last_message_at,
        #        COALESCE(uc.unread_count, 0), c.sms_opt_in_status,
        #        c.opted_in_at, c.opted_out_at, d.status_standardized
        # FROM conversations c
        # INNER JOIN deals d ON d.id = c.deal_id
        # INNER JOIN visible_deals vd ON vd.deal_id = c.deal_id
        # LEFT JOIN last_messages lm ON lm.conversation_id = c.id
        # LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
        # WHERE c.type = 'sms' AND c.is_active = true
        # ORDER BY c.last_message_at DESC;
        pass

    def get_sms_conversations_all(self) -> List[SMSConversation]:
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
        # TODO: Implement Django ORM equivalent
        if not self.is_admin:
            raise PermissionError("Unauthorized: Only admins can view all conversations")

        # Original SQL:
        # WITH last_messages AS (
        #   SELECT DISTINCT ON (m.conversation_id)
        #     m.conversation_id, m.body, m.sent_at
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   WHERE c.agency_id = v_agency_id
        #     AND c.type = 'sms' AND c.is_active = true
        #   ORDER BY m.conversation_id, m.sent_at DESC
        # ),
        # unread_counts AS (
        #   SELECT m.conversation_id, COUNT(*) as unread_count
        #   FROM messages m
        #   INNER JOIN conversations c ON c.id = m.conversation_id
        #   WHERE c.agency_id = v_agency_id
        #     AND c.type = 'sms' AND c.is_active = true
        #     AND m.direction = 'inbound' AND m.read_at IS NULL
        #   GROUP BY m.conversation_id
        # )
        # SELECT c.id as conversation_id, c.deal_id, c.agent_id,
        #        d.client_name, COALESCE(d.client_phone, ''),
        #        COALESCE(lm.body, ''), c.last_message_at,
        #        COALESCE(uc.unread_count, 0), c.sms_opt_in_status,
        #        c.opted_in_at, c.opted_out_at, d.status_standardized
        # FROM conversations c
        # INNER JOIN deals d ON d.id = c.deal_id
        # LEFT JOIN last_messages lm ON lm.conversation_id = c.id
        # LEFT JOIN unread_counts uc ON uc.conversation_id = c.id
        # WHERE c.agency_id = v_agency_id
        #   AND c.type = 'sms' AND c.is_active = true
        # ORDER BY c.last_message_at DESC;
        pass

    # ========================================================================
    # P1 - Message Functions
    # ========================================================================

    def get_conversation_messages(
        self,
        conversation_id: UUID,
        view: str = 'downlines'
    ) -> List[ConversationMessage]:
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
        # TODO: Implement Django ORM equivalent
        # Original SQL permission logic:
        # IF v_normalized_view = 'self' THEN
        #   IF v_conversation.agent_id = v_user.id THEN
        #     v_can_view := true;
        #   END IF;
        # ELSIF v_normalized_view = 'all' THEN
        #   IF v_user.is_admin AND v_user.agency_id = v_conversation.agency_id THEN
        #     v_can_view := true;
        #   END IF;
        # ELSE
        #   IF EXISTS (
        #     SELECT 1 FROM deal_hierarchy_snapshot dhs
        #     WHERE dhs.agent_id = v_user.id
        #       AND dhs.deal_id = v_conversation.deal_id
        #   ) THEN
        #     v_can_view := true;
        #   END IF;
        # END IF;
        #
        # -- Mark inbound messages as read
        # UPDATE messages
        # SET read_at = NOW()
        # WHERE messages.conversation_id = p_conversation_id
        #   AND messages.direction = 'inbound'
        #   AND messages.read_at IS NULL;
        #
        # -- Return messages
        # SELECT m.id, m.conversation_id, m.sender_id, m.receiver_id,
        #        m.body, m.direction, m.message_type, m.sent_at,
        #        m.status, m.metadata, m.read_at
        # FROM messages m
        # WHERE m.conversation_id = p_conversation_id
        # ORDER BY
        #   CASE WHEN m.sent_at IS NULL THEN 1 ELSE 0 END,
        #   m.sent_at ASC, m.id;
        pass

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
        # TODO: Implement permission check
        pass


# ============================================================================
# Type Aliases for External Use
# ============================================================================

SMSServiceResult = SMSConversation | ConversationMessage
