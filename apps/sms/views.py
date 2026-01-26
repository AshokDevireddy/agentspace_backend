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
    SendMessageInput,
    TemplateInput,
    approve_drafts,
    create_template,
    delete_template,
    get_opted_out_numbers,
    get_template_by_id,
    list_templates,
    reject_drafts,
    send_bulk_messages,
    send_message,
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
    """

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
