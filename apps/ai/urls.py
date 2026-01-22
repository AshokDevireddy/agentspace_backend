"""
AI URL Configuration (P1-015)
"""
from django.urls import path

from .views import AIConversationsView, AIConversationDetailView, AIMessagesView

urlpatterns = [
    path('conversations', AIConversationsView.as_view(), name='ai_conversations'),
    path('conversations/<uuid:conversation_id>', AIConversationDetailView.as_view(), name='ai_conversation_detail'),
    path('conversations/<uuid:conversation_id>/messages', AIMessagesView.as_view(), name='ai_messages'),
]
