"""
SMS URL Configuration (P1-016, P2-029, P2-030, P2-031)
"""
from django.urls import path

from .views import (
    BulkSmsView,
    ConversationsView,
    DraftsApproveView,
    DraftsRejectView,
    DraftsView,
    MessagesView,
    OptOutView,
    TemplateDetailView,
    TemplatesListView,
    UnreadCountView,
)

urlpatterns = [
    # Core SMS endpoints
    path('conversations/', ConversationsView.as_view(), name='conversations'),
    path('messages/', MessagesView.as_view(), name='messages'),
    path('drafts/', DraftsView.as_view(), name='drafts'),
    path('drafts/approve/', DraftsApproveView.as_view(), name='drafts_approve'),
    path('drafts/reject/', DraftsRejectView.as_view(), name='drafts_reject'),
    path('unread-count/', UnreadCountView.as_view(), name='unread_count'),

    # Bulk SMS (P2-030)
    path('bulk/', BulkSmsView.as_view(), name='bulk_sms'),

    # SMS Templates (P2-029)
    path('templates/', TemplatesListView.as_view(), name='templates_list'),
    path('templates/<str:template_id>/', TemplateDetailView.as_view(), name='template_detail'),

    # Opt-out Management (P2-031)
    path('opt-out/', OptOutView.as_view(), name='opt_out'),
]
