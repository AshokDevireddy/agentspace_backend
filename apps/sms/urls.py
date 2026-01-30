"""
SMS URL Configuration (P1-016, P2-029, P2-030, P2-031)
"""
from django.urls import path

from .views import (
    BulkSmsView,
    ConversationFindView,
    ConversationGetOrCreateView,
    ConversationsView,
    DraftsApproveView,
    DraftsEditView,
    DraftsRejectView,
    DraftsView,
    MarkMessageReadView,
    MessageLogView,
    MessagesView,
    OptOutView,
    StartConversationView,
    TemplateDetailView,
    TemplatesListView,
    TelnyxWebhookView,
    UnreadCountView,
)

urlpatterns = [
    # Core SMS endpoints
    path('conversations/', ConversationsView.as_view(), name='conversations'),
    path('conversations/find', ConversationFindView.as_view(), name='conversation_find'),
    path('conversations/get-or-create', ConversationGetOrCreateView.as_view(), name='conversation_get_or_create'),
    path('conversations/start', StartConversationView.as_view(), name='conversation_start'),
    path('messages/', MessagesView.as_view(), name='messages'),
    path('messages/log', MessageLogView.as_view(), name='message_log'),
    path('messages/<str:message_id>/read/', MarkMessageReadView.as_view(), name='mark_message_read'),
    path('drafts/', DraftsView.as_view(), name='drafts'),
    path('drafts/approve/', DraftsApproveView.as_view(), name='drafts_approve'),
    path('drafts/reject/', DraftsRejectView.as_view(), name='drafts_reject'),
    path('drafts/<str:message_id>/', DraftsEditView.as_view(), name='drafts_edit'),
    path('unread-count/', UnreadCountView.as_view(), name='unread_count'),

    # Bulk SMS (P2-030)
    path('bulk/', BulkSmsView.as_view(), name='bulk_sms'),

    # SMS Templates (P2-029)
    path('templates/', TemplatesListView.as_view(), name='templates_list'),
    path('templates/<str:template_id>/', TemplateDetailView.as_view(), name='template_detail'),

    # Opt-out Management (P2-031)
    path('opt-out/', OptOutView.as_view(), name='opt_out'),

    # Webhooks
    path('webhooks/telnyx', TelnyxWebhookView.as_view(), name='telnyx_webhook'),
]
