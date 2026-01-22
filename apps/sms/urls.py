"""
SMS URL Configuration
"""
from django.urls import path

from .views import ConversationsView, MessagesView, DraftsView, UnreadCountView

urlpatterns = [
    path('conversations', ConversationsView.as_view(), name='conversations'),
    path('messages', MessagesView.as_view(), name='messages'),
    path('drafts', DraftsView.as_view(), name='drafts'),
    path('unread-count', UnreadCountView.as_view(), name='unread_count'),
]
