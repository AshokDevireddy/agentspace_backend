"""
SMS API Views (P1-016, P2-029, P2-030, P2-031)

Endpoints:
- GET /api/sms/conversations - Get SMS conversations
- GET/POST /api/sms/messages - Get/send messages
- POST /api/sms/bulk - Send bulk SMS
- CRUD /api/sms/templates - SMS template management
- GET/PUT /api/sms/opt-out - Opt-out management
"""
import logging

from django.utils.decorators import method_decorator
from django_ratelimit.decorators import ratelimit
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import CronSecretAuthentication, SupabaseJWTAuthentication
from apps.core.constants import PAGINATION, RATE_LIMITS
from apps.core.mixins import AuthenticatedAPIView

from .selectors import (
    get_draft_messages,
    get_sms_conversations,
    get_sms_messages,
    get_unread_message_count,
)
from .services import (
    BulkSendInput,
    GetOrCreateConversationInput,
    LogMessageInput,
    SendMessageInput,
    StartConversationInput,
    TemplateInput,
    approve_drafts,
    create_template,
    delete_template,
    find_conversation,
    get_opted_out_numbers,
    get_or_create_conversation,
    get_template_by_id,
    list_templates,
    log_message,
    mark_message_as_read,
    reject_drafts,
    send_bulk_messages,
    send_message,
    start_conversation,
    update_draft_body,
    update_opt_status,
    update_template,
)

logger = logging.getLogger(__name__)


class ConversationsView(AuthenticatedAPIView, APIView):
    """GET /api/sms/conversations - Get SMS conversations."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        view_mode = request.query_params.get("view_mode", "self")
        if view_mode not in ("all", "self", "downlines"):
            view_mode = "self"

        page = int(request.query_params.get("page", 1))
        limit = min(
            int(request.query_params.get("limit", PAGINATION["default_limit"])),
            PAGINATION["max_limit"],
        )
        search_query = request.query_params.get("search", "").strip() or None

        result = get_sms_conversations(
            user=user,
            view_mode=view_mode,
            page=page,
            limit=limit,
            search_query=search_query,
        )
        return Response(result)


class MessagesView(AuthenticatedAPIView, APIView):
    """
    GET /api/sms/messages - Get messages for a conversation
    POST /api/sms/messages - Send a message (P1-016)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        conversation_id = request.query_params.get("conversation_id")
        if not conversation_id:
            return Response(
                {"error": "conversation_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        page = int(request.query_params.get("page", 1))
        limit = min(
            int(request.query_params.get("limit", 50)), PAGINATION["max_limit"]
        )

        result = get_sms_messages(
            user=user,
            conversation_id=conversation_uuid,
            page=page,
            limit=limit,
        )
        return Response(result)

    @method_decorator(ratelimit(key="user", rate=RATE_LIMITS["sms_send"], method="POST"))
    def post(self, request):
        """Send an SMS message (P1-016)."""
        user = self.get_user(request)

        data = request.data
        conversation_id = data.get("conversation_id")
        content = data.get("content", "").strip()

        if not conversation_id:
            return Response(
                {"error": "conversation_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not content:
            return Response(
                {"error": "content is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        result = send_message(
            user=user,
            data=SendMessageInput(conversation_id=conversation_uuid, content=content),
        )

        if result.success:
            return Response(
                {
                    "success": True,
                    "message_id": str(result.message_id),
                    "external_id": result.external_id,
                },
                status=status.HTTP_201_CREATED,
            )
        return Response({"error": result.error}, status=status.HTTP_400_BAD_REQUEST)


class DraftsView(AuthenticatedAPIView, APIView):
    """GET /api/sms/drafts - Get draft messages pending approval."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        view_mode = request.query_params.get("view_mode", "self")
        if view_mode not in ("all", "self", "downlines"):
            view_mode = "self"

        page = int(request.query_params.get("page", 1))
        limit = min(
            int(request.query_params.get("limit", PAGINATION["default_limit"])),
            PAGINATION["max_limit"],
        )

        result = get_draft_messages(
            user=user, view_mode=view_mode, page=page, limit=limit
        )
        return Response(result)


class DraftsApproveView(AuthenticatedAPIView, APIView):
    """POST /api/sms/drafts/approve - Approve and send draft SMS messages."""

    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        message_ids = request.data.get('messageIds', [])
        if not message_ids or not isinstance(message_ids, list):
            return Response(
                {'error': 'Message IDs are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        result = approve_drafts(user=user, message_ids=message_ids)

        return Response({
            'success': result.success,
            'approved': result.approved,
            'failed': result.failed,
            'results': result.results or [],
            'errors': result.errors,
        })


class DraftsRejectView(AuthenticatedAPIView, APIView):
    """POST /api/sms/drafts/reject - Reject (delete) draft SMS messages."""

    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        message_ids = request.data.get('messageIds', [])
        if not message_ids or not isinstance(message_ids, list):
            return Response(
                {'error': 'Message IDs are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        result = reject_drafts(user=user, message_ids=message_ids)

        return Response({
            'success': result.success,
            'rejected': result.rejected,
        })


class DraftsEditView(AuthenticatedAPIView, APIView):
    """PATCH /api/sms/drafts/{message_id} - Update a draft message body."""

    permission_classes = [IsAuthenticated]

    def patch(self, request, message_id: str):
        user = self.get_user(request)

        message_uuid = self.parse_uuid(message_id, "message_id")

        new_body = request.data.get('body', '').strip()
        if not new_body:
            return Response(
                {'error': 'Message body is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        result = update_draft_body(user=user, message_id=message_uuid, new_body=new_body)

        if not result.success:
            return Response(
                {'error': result.error or 'Draft message not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        return Response({
            'success': True,
            'message': result.message,
        })


class UnreadCountView(AuthenticatedAPIView, APIView):
    """GET /api/sms/unread-count - Get count of unread inbound messages."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        view_mode = request.query_params.get("view_mode", "self")
        if view_mode not in ("all", "self", "downlines"):
            view_mode = "self"

        count = get_unread_message_count(user=user, view_mode=view_mode)
        return Response({"count": count})


class MarkMessageReadView(AuthenticatedAPIView, APIView):
    """POST /api/sms/messages/{message_id}/read - Mark a message as read."""

    permission_classes = [IsAuthenticated]

    def post(self, request, message_id: str):
        user = self.get_user(request)

        message_uuid = self.parse_uuid(message_id, "message_id")

        success = mark_message_as_read(user=user, message_id=message_uuid)

        if success:
            return Response({"success": True, "message": "Message marked as read"})
        return Response(
            {"success": False, "message": "Message not found or already read"},
            status=status.HTTP_404_NOT_FOUND
        )


class BulkSmsView(AuthenticatedAPIView, APIView):
    """POST /api/sms/bulk - Send bulk SMS messages (P2-030)."""

    permission_classes = [IsAuthenticated]

    @method_decorator(ratelimit(key="user", rate=RATE_LIMITS["sms_bulk"], method="POST"))
    def post(self, request):
        user = self.get_user(request)

        data = request.data
        template_id = data.get("template_id")
        content = data.get("content")
        recipient_ids = data.get("recipient_ids", [])
        recipient_type = data.get("recipient_type", "client")

        if not recipient_ids:
            return Response(
                {"error": "recipient_ids is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not template_id and not content:
            return Response(
                {"error": "Either template_id or content is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        template_uuid = self.parse_uuid(template_id, "template_id") if template_id else None
        recipient_uuids = [self.parse_uuid(rid, "recipient_id") for rid in recipient_ids]

        result = send_bulk_messages(
            user=user,
            data=BulkSendInput(
                template_id=template_uuid,
                content=content,
                recipient_ids=recipient_uuids,
                recipient_type=recipient_type,
            ),
        )

        response_status = (
            status.HTTP_200_OK if result.success else status.HTTP_207_MULTI_STATUS
        )
        return Response(
            {
                "success": result.success,
                "total": result.total,
                "sent": result.sent,
                "failed": result.failed,
                "errors": result.errors,
            },
            status=response_status,
        )


class TemplatesListView(AuthenticatedAPIView, APIView):
    """
    GET /api/sms/templates - List all templates
    POST /api/sms/templates - Create a new template (P2-029)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        template_type = request.query_params.get("template_type")
        templates = list_templates(user, template_type)
        return Response({"templates": templates})

    def post(self, request):
        user = self.get_user(request)

        data = request.data
        name = data.get("name", "").strip()
        template_type = data.get("template_type", "custom")
        content = data.get("content", "").strip()
        is_active = data.get("is_active", True)

        if not name:
            return Response(
                {"error": "name is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        if not content:
            return Response(
                {"error": "content is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        template = create_template(
            user=user,
            data=TemplateInput(
                name=name,
                template_type=template_type,
                content=content,
                is_active=is_active,
            ),
        )
        return Response(template, status=status.HTTP_201_CREATED)


class TemplateDetailView(AuthenticatedAPIView, APIView):
    """
    GET /api/sms/templates/{id} - Get template details
    PUT /api/sms/templates/{id} - Update template
    DELETE /api/sms/templates/{id} - Delete template (P2-029)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request, template_id):
        user = self.get_user(request)
        template_uuid = self.parse_uuid(template_id, "template_id")

        template = get_template_by_id(template_uuid, user)
        if not template:
            return Response(
                {"error": "Template not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(template)

    def put(self, request, template_id):
        user = self.get_user(request)
        template_uuid = self.parse_uuid(template_id, "template_id")

        data = request.data
        name = data.get("name", "").strip()
        template_type = data.get("template_type", "custom")
        content = data.get("content", "").strip()
        is_active = data.get("is_active", True)

        if not name or not content:
            return Response(
                {"error": "name and content are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        template = update_template(
            template_uuid,
            user=user,
            data=TemplateInput(
                name=name,
                template_type=template_type,
                content=content,
                is_active=is_active,
            ),
        )
        if not template:
            return Response(
                {"error": "Template not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(template)

    def delete(self, request, template_id):
        user = self.get_user(request)
        template_uuid = self.parse_uuid(template_id, "template_id")

        deleted = delete_template(template_uuid, user)
        if not deleted:
            return Response(
                {"error": "Template not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response({"success": True})


class OptOutView(AuthenticatedAPIView, APIView):
    """
    GET /api/sms/opt-out - List opted-out numbers
    PUT /api/sms/opt-out - Update opt status (P2-031)

    Supports CronSecretAuthentication for server-to-server calls (webhooks).
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        opted_out = get_opted_out_numbers(user)
        return Response({"opted_out": opted_out})

    def put(self, request):
        user = self.get_user(request)

        data = request.data
        conversation_id = data.get("conversation_id")
        opt_status = data.get("status")

        if not conversation_id:
            return Response(
                {"error": "conversation_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not opt_status:
            return Response(
                {"error": "status is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        try:
            result = update_opt_status(user, conversation_uuid, opt_status)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        if not result:
            return Response(
                {"error": "Conversation not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(result)


class ConversationFindView(AuthenticatedAPIView, APIView):
    """
    GET /api/sms/conversations/find - Find an existing conversation

    Query params:
    - agent_id: Filter by agent ID
    - deal_id: Filter by deal ID
    - phone: Filter by phone number

    Supports CronSecretAuthentication for server-to-server calls.
    """

    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        agent_id = request.query_params.get("agent_id")
        deal_id = request.query_params.get("deal_id")
        phone = request.query_params.get("phone")

        if not any([agent_id, deal_id, phone]):
            return Response(
                {"error": "At least one of agent_id, deal_id, or phone is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        agent_uuid = self.parse_uuid(agent_id, "agent_id") if agent_id else None
        deal_uuid = self.parse_uuid(deal_id, "deal_id") if deal_id else None

        conversation = find_conversation(
            user=user,
            agent_id=agent_uuid,
            deal_id=deal_uuid,
            phone=phone,
        )

        if not conversation:
            return Response(
                {"found": False, "conversation": None},
                status=status.HTTP_200_OK
            )

        return Response({"found": True, "conversation": conversation})


class ConversationGetOrCreateView(AuthenticatedAPIView, APIView):
    """
    POST /api/sms/conversations/get-or-create - Get or create a conversation

    Body:
    - agent_id: Required - The agent ID
    - deal_id: Optional - The deal ID
    - client_id: Optional - The client ID
    - phone_number: Optional - The phone number (required if no deal_id)

    Supports CronSecretAuthentication for server-to-server calls.
    """

    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        data = request.data
        agent_id = data.get("agent_id")
        deal_id = data.get("deal_id")
        client_id = data.get("client_id")
        phone_number = data.get("phone_number")

        if not agent_id:
            return Response(
                {"error": "agent_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        agent_uuid = self.parse_uuid(agent_id, "agent_id")
        deal_uuid = self.parse_uuid(deal_id, "deal_id") if deal_id else None
        client_uuid = self.parse_uuid(client_id, "client_id") if client_id else None

        result = get_or_create_conversation(
            user=user,
            data=GetOrCreateConversationInput(
                agent_id=agent_uuid,
                deal_id=deal_uuid,
                client_id=client_uuid,
                phone_number=phone_number,
            ),
        )

        if not result.success:
            return Response(
                {"success": False, "error": result.error},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({
            "success": True,
            "conversation": result.conversation,
            "created": result.created,
        }, status=status.HTTP_201_CREATED if result.created else status.HTTP_200_OK)


class StartConversationView(AuthenticatedAPIView, APIView):
    """
    POST /api/sms/conversations/start - Start a new SMS conversation from a deal

    This endpoint creates a new conversation, sends a welcome message, and logs it.

    Body:
    - dealId or deal_id: Required - The deal ID to start conversation from

    Response (200):
    - conversation: The created conversation object
    - message: Success message

    Response (409 Conflict):
    - error: Message indicating conversation already exists
    - existingConversation: The existing conversation object
    """

    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        data = request.data
        # Support both camelCase and snake_case
        deal_id = data.get("dealId") or data.get("deal_id")

        if not deal_id:
            return Response(
                {"error": "Deal ID is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        deal_uuid = self.parse_uuid(deal_id, "dealId")

        result = start_conversation(
            user=user,
            data=StartConversationInput(deal_id=deal_uuid),
        )

        if not result.success:
            # Check if it's a conflict (existing conversation)
            if result.existing_conversation:
                return Response({
                    "error": result.error,
                    "existingConversation": result.existing_conversation,
                }, status=status.HTTP_409_CONFLICT)

            return Response(
                {"error": result.error},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({
            "conversation": result.conversation,
            "message": result.message,
        })


class MessageLogView(AuthenticatedAPIView, APIView):
    """
    POST /api/sms/messages/log - Log a message without sending

    Used for:
    - Recording inbound messages from webhooks
    - Creating draft messages for approval
    - Logging automated messages

    Body:
    - conversation_id: Required - The conversation ID
    - content: Required - The message content
    - direction: Required - 'inbound' or 'outbound'
    - status: Optional - Message status ('delivered', 'draft', 'pending'). Defaults to 'delivered'
    - metadata: Optional - Additional metadata as JSON object

    Supports CronSecretAuthentication for server-to-server calls.
    """

    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        data = request.data
        conversation_id = data.get("conversation_id")
        content = data.get("content", "").strip()
        direction = data.get("direction", "outbound")
        msg_status = data.get("status", "delivered")
        metadata = data.get("metadata")

        if not conversation_id:
            return Response(
                {"error": "conversation_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not content:
            return Response(
                {"error": "content is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if direction not in ("inbound", "outbound"):
            return Response(
                {"error": "direction must be 'inbound' or 'outbound'"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        conversation_uuid = self.parse_uuid(conversation_id, "conversation_id")

        result = log_message(
            user=user,
            data=LogMessageInput(
                conversation_id=conversation_uuid,
                content=content,
                direction=direction,
                status=msg_status,
                metadata=metadata,
            ),
        )

        if result.success:
            return Response(
                {
                    "success": True,
                    "message_id": str(result.message_id),
                },
                status=status.HTTP_201_CREATED,
            )
        return Response({"error": result.error}, status=status.HTTP_400_BAD_REQUEST)


class TelnyxWebhookView(APIView):
    """
    POST /api/sms/webhooks/telnyx - Handle Telnyx inbound SMS webhooks

    This endpoint receives webhooks from Telnyx when SMS messages are received.
    No authentication required (public webhook endpoint).
    """

    # Disable authentication for webhook
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Handle inbound SMS webhook from Telnyx."""
        try:
            body = request.data

            # Telnyx sends event_type in data.event_type
            event_type = body.get("data", {}).get("event_type")

            if event_type != "message.received":
                return Response({
                    "success": True,
                    "message": "Event received"
                })

            payload = body.get("data", {}).get("payload", {})
            from_number = payload.get("from", {}).get("phone_number")
            to_numbers = payload.get("to", [])
            to_number = to_numbers[0].get("phone_number") if to_numbers else None
            message_text = payload.get("text", "")
            telnyx_message_id = payload.get("id")

            if not from_number or not to_number:
                return Response({
                    "success": False,
                    "message": "Missing phone numbers"
                }, status=status.HTTP_400_BAD_REQUEST)

            logger.info(f"Received inbound SMS from {from_number} to {to_number}: {message_text[:50]}...")

            # Find agency by receiving phone number
            from apps.sms.services import normalize_phone_number
            from apps.deals.selectors import find_deal_by_client_phone

            # Get agency by phone number
            agency = self._find_agency_by_phone(to_number)
            if not agency:
                logger.warning(f"No agency found for phone number: {to_number}")
                return Response({
                    "success": False,
                    "message": "Agency not found"
                })

            agency_id = agency["id"]
            logger.info(f"Message received for agency: {agency['name']} ({agency_id})")

            # Normalize client phone for storage
            normalized_client_phone = normalize_phone_number(from_number)
            if normalized_client_phone.startswith("+1"):
                normalized_client_phone = normalized_client_phone[2:]

            # Find deal by client phone
            deal = find_deal_by_client_phone(normalized_client_phone, agency_id)
            if not deal:
                logger.warning(f"No deal found for client phone: {normalized_client_phone} in agency: {agency_id}")
                return Response({
                    "success": False,
                    "message": "Client not found"
                })

            agent_info = deal.get("agent", {})
            agent_id = agent_info.get("id")
            if not agent_id:
                logger.warning(f"No agent found for deal: {deal.get('id')}")
                return Response({
                    "success": False,
                    "message": "Agent not found"
                })

            # Get or create conversation
            from apps.sms.services import (
                get_or_create_conversation,
                GetOrCreateConversationInput,
                handle_stop_keyword,
                handle_start_keyword,
            )
            from apps.core.authentication import AuthenticatedUser
            from uuid import UUID

            # Create a minimal user object for the service call
            user = AuthenticatedUser(
                id=UUID(agent_id),
                email="webhook@system.internal",
                agency_id=UUID(agency_id),
                is_admin=False,
                role="agent",
            )

            conv_result = get_or_create_conversation(
                user=user,
                data=GetOrCreateConversationInput(
                    agent_id=UUID(agent_id),
                    deal_id=UUID(deal["id"]) if deal.get("id") else None,
                    phone_number=normalized_client_phone,
                ),
            )

            if not conv_result.success or not conv_result.conversation:
                logger.error(f"Failed to get/create conversation: {conv_result.error}")
                return Response({
                    "success": False,
                    "message": "Failed to create conversation"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            conversation = conv_result.conversation
            conversation_id = conversation["id"]

            # Check for compliance keywords
            keyword = self._get_compliance_keyword(message_text)

            if keyword:
                logger.info(f"Received compliance keyword: {keyword}")

                # Log the incoming keyword message
                self._log_message(
                    conversation_id=conversation_id,
                    content=message_text,
                    direction="inbound",
                    status="received",
                    metadata={
                        "client_phone": normalized_client_phone,
                        "telnyx_message_id": telnyx_message_id,
                        "compliance_keyword": keyword,
                    },
                    user=user,
                )

                # Handle the compliance keyword
                if keyword == "STOP":
                    handle_stop_keyword(normalized_client_phone, UUID(agency_id))
                    # Note: Response SMS is still sent by Next.js webhook handler
                elif keyword == "START":
                    handle_start_keyword(normalized_client_phone, UUID(agency_id))
                    # Note: Response SMS is still sent by Next.js webhook handler

                return Response({
                    "success": True,
                    "message": "Compliance keyword processed",
                    "keyword": keyword,
                    "conversation_id": conversation_id,
                })

            # Log regular inbound message
            self._log_message(
                conversation_id=conversation_id,
                content=message_text,
                direction="inbound",
                status="received",
                metadata={
                    "client_phone": normalized_client_phone,
                    "telnyx_message_id": telnyx_message_id,
                },
                user=user,
            )

            return Response({
                "success": True,
                "message": "Message processed",
                "conversation_id": conversation_id,
                "deal_id": deal.get("id"),
                "agent_id": agent_id,
            })

        except Exception as e:
            logger.error(f"Telnyx webhook error: {e}", exc_info=True)
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get(self, request):
        """Health check for webhook endpoint."""
        return Response({
            "status": "ok",
            "message": "Telnyx webhook endpoint is active"
        })

    def _get_compliance_keyword(self, message_text: str) -> str | None:
        """Check if message is an opt-out/help/opt-in keyword."""
        text = message_text.strip().upper()
        if text in ("STOP", "UNSUBSCRIBE"):
            return "STOP"
        if text in ("START", "UNSTOP", "SUBSCRIBE"):
            return "START"
        if text in ("HELP", "INFO"):
            return "HELP"
        return None

    def _find_agency_by_phone(self, phone_number: str) -> dict | None:
        """Find agency by phone number."""
        import re
        from django.db import connection

        # Normalize phone number
        normalized = re.sub(r"\D", "", phone_number)
        phone_variations = [
            phone_number,
            normalized,
            f"+1{normalized}" if len(normalized) == 10 else f"+{normalized}",
        ]

        for variation in phone_variations:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, phone_number
                    FROM public.agencies
                    WHERE phone_number = %s
                """, [variation])
                row = cursor.fetchone()

            if row:
                return {
                    "id": str(row[0]),
                    "name": row[1],
                    "phone_number": row[2],
                }

        return None

    def _log_message(
        self,
        conversation_id: str,
        content: str,
        direction: str,
        status: str,
        metadata: dict,
        user,
    ) -> None:
        """Log a message to the database."""
        from apps.sms.services import log_message, LogMessageInput
        from uuid import UUID

        log_message(
            user=user,
            data=LogMessageInput(
                conversation_id=UUID(conversation_id),
                content=content,
                direction=direction,
                status=status,
                metadata=metadata,
            ),
        )
