"""
AI URL Configuration (P1-015, P2-036, P2-037, P2-038)
"""
from django.urls import path

from .views import (
    AIConversationsView,
    AIConversationDetailView,
    AIMessagesView,
    AIQuickChatView,
    AISuggestionsView,
    AIAnalyticsInsightsView,
)

urlpatterns = [
    # Conversation management
    path('conversations', AIConversationsView.as_view(), name='ai_conversations'),
    path('conversations/<uuid:conversation_id>', AIConversationDetailView.as_view(), name='ai_conversation_detail'),
    path('conversations/<uuid:conversation_id>/messages', AIMessagesView.as_view(), name='ai_messages'),

    # Quick chat (P2-036)
    path('chat', AIQuickChatView.as_view(), name='ai_quick_chat'),

    # AI Suggestions (P2-037)
    path('suggestions', AISuggestionsView.as_view(), name='ai_suggestions'),

    # AI Analytics Insights (P2-038)
    path('analytics/insights', AIAnalyticsInsightsView.as_view(), name='ai_analytics_insights'),
]
