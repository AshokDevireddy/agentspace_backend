"""
AI App Tests (P1-015)

Unit tests for AI conversations and messages.
"""
import uuid
from unittest.mock import MagicMock, patch

from django.test import TestCase
from rest_framework.test import APIRequestFactory

from apps.core.authentication import AuthenticatedUser
from apps.core.models import AIConversation, AIMessage


class AIConversationModelTests(TestCase):
    """Tests for AIConversation model."""

    def test_str_representation_with_title(self):
        """Test string representation with title."""
        conversation = AIConversation(
            id=uuid.uuid4(),
            title='Test Conversation'
        )
        conversation.user = MagicMock(email='test@example.com')
        self.assertIn('Test Conversation', str(conversation))

    def test_str_representation_without_title(self):
        """Test string representation without title."""
        conversation = AIConversation(
            id=uuid.uuid4(),
            title=None
        )
        conversation.user = MagicMock(email='test@example.com')
        self.assertIn('Untitled', str(conversation))


class AIMessageModelTests(TestCase):
    """Tests for AIMessage model."""

    def test_str_representation(self):
        """Test string representation."""
        message = AIMessage(
            id=uuid.uuid4(),
            role='user',
            content='Hello, this is a test message'
        )
        self.assertIn('user', str(message))
        self.assertIn('Hello', str(message))

    def test_role_choices(self):
        """Test role choices are valid."""
        valid_roles = ['user', 'assistant', 'system']
        for role, _ in AIMessage.ROLE_CHOICES:
            self.assertIn(role, valid_roles)


class AIConversationSelectorsTests(TestCase):
    """Tests for AI conversation selectors."""

    def setUp(self):
        """Set up test fixtures."""
        self.user = AuthenticatedUser(
            id=uuid.uuid4(),
            auth_user_id=uuid.uuid4(),
            email='test@example.com',
            agency_id=uuid.uuid4(),
            role='agent',
            is_admin=False,
            status='active',
            perm_level=None,
            subscription_tier='pro'
        )

    @patch('apps.ai.selectors.connection')
    def test_get_ai_conversations_empty(self, mock_connection):
        """Test getting conversations returns empty list when none exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        from apps.ai.selectors import get_ai_conversations
        result = get_ai_conversations(self.user, page=1, limit=20)

        self.assertEqual(result['conversations'], [])
        self.assertEqual(result['pagination']['totalCount'], 0)
        self.assertEqual(result['pagination']['currentPage'], 1)

    @patch('apps.ai.selectors.connection')
    def test_get_ai_conversation_detail_not_found(self, mock_connection):
        """Test getting non-existent conversation returns None."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        from apps.ai.selectors import get_ai_conversation_detail
        result = get_ai_conversation_detail(self.user, conversation_id=uuid.uuid4())

        self.assertIsNone(result)

    @patch('apps.ai.selectors.connection')
    def test_get_ai_messages_unauthorized(self, mock_connection):
        """Test getting messages from another user's conversation returns empty."""
        # First call: get conversation ownership (different user)
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (uuid.uuid4(), uuid.uuid4())  # Different user/agency
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        from apps.ai.selectors import get_ai_messages
        result = get_ai_messages(self.user, conversation_id=uuid.uuid4())

        self.assertEqual(result['messages'], [])


class AIConversationViewTests(TestCase):
    """Tests for AI conversation views."""

    def setUp(self):
        """Set up test fixtures."""
        self.factory = APIRequestFactory()
        self.user = AuthenticatedUser(
            id=uuid.uuid4(),
            auth_user_id=uuid.uuid4(),
            email='test@example.com',
            agency_id=uuid.uuid4(),
            role='agent',
            is_admin=False,
            status='active',
            perm_level=None,
            subscription_tier='pro'
        )

    @patch('apps.ai.views.get_user_context')
    @patch('apps.ai.views.get_ai_conversations')
    def test_list_conversations(self, mock_get_conversations, mock_get_user):
        """Test listing conversations."""
        mock_get_user.return_value = self.user
        mock_get_conversations.return_value = {
            'conversations': [],
            'pagination': {
                'currentPage': 1,
                'totalPages': 0,
                'totalCount': 0,
                'limit': 20,
                'hasNextPage': False,
                'hasPrevPage': False
            }
        }

        from apps.ai.views import AIConversationsView
        view = AIConversationsView.as_view()
        request = self.factory.get('/api/ai/conversations')
        request.user = self.user

        response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertIn('conversations', response.data)
        self.assertIn('pagination', response.data)

    @patch('apps.ai.views.get_user_context')
    @patch('apps.ai.views.connection')
    def test_create_conversation(self, mock_connection, mock_get_user):
        """Test creating a conversation."""
        mock_get_user.return_value = self.user

        # Mock the cursor
        mock_cursor = MagicMock()
        conv_id = uuid.uuid4()
        from datetime import datetime
        mock_cursor.fetchone.return_value = (
            conv_id,
            'Test Title',
            True,
            datetime.now(),
            datetime.now()
        )
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        from apps.ai.views import AIConversationsView
        view = AIConversationsView.as_view()
        request = self.factory.post('/api/ai/conversations', {'title': 'Test Title'}, format='json')
        request.user = self.user

        response = view(request)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data['title'], 'Test Title')
        self.assertTrue(response.data['is_active'])

    @patch('apps.ai.views.get_user_context')
    def test_unauthorized_request(self, mock_get_user):
        """Test unauthorized request returns 401."""
        mock_get_user.return_value = None

        from apps.ai.views import AIConversationsView
        view = AIConversationsView.as_view()
        request = self.factory.get('/api/ai/conversations')

        response = view(request)

        self.assertEqual(response.status_code, 401)


class AIMessageViewTests(TestCase):
    """Tests for AI message views."""

    def setUp(self):
        """Set up test fixtures."""
        self.factory = APIRequestFactory()
        self.user = AuthenticatedUser(
            id=uuid.uuid4(),
            auth_user_id=uuid.uuid4(),
            email='test@example.com',
            agency_id=uuid.uuid4(),
            role='agent',
            is_admin=False,
            status='active',
            perm_level=None,
            subscription_tier='pro'
        )
        self.conversation_id = uuid.uuid4()

    @patch('apps.ai.views.get_user_context')
    @patch('apps.ai.views.get_ai_messages')
    def test_list_messages(self, mock_get_messages, mock_get_user):
        """Test listing messages."""
        mock_get_user.return_value = self.user
        mock_get_messages.return_value = {
            'messages': [],
            'pagination': {
                'currentPage': 1,
                'totalPages': 0,
                'totalCount': 0,
                'limit': 50,
                'hasNextPage': False,
                'hasPrevPage': False
            }
        }

        from apps.ai.views import AIMessagesView
        view = AIMessagesView.as_view()
        request = self.factory.get(f'/api/ai/conversations/{self.conversation_id}/messages')
        request.user = self.user

        response = view(request, conversation_id=self.conversation_id)

        self.assertEqual(response.status_code, 200)
        self.assertIn('messages', response.data)
        self.assertIn('pagination', response.data)

    @patch('apps.ai.views.get_user_context')
    def test_create_message_invalid_role(self, mock_get_user):
        """Test creating a message with invalid role fails."""
        mock_get_user.return_value = self.user

        from apps.ai.views import AIMessagesView
        view = AIMessagesView.as_view()
        request = self.factory.post(
            f'/api/ai/conversations/{self.conversation_id}/messages',
            {'role': 'invalid', 'content': 'Test'},
            format='json'
        )
        request.user = self.user

        response = view(request, conversation_id=self.conversation_id)

        self.assertEqual(response.status_code, 400)
        self.assertIn('role', response.data['error'].lower())

    @patch('apps.ai.views.get_user_context')
    def test_create_message_missing_content(self, mock_get_user):
        """Test creating a message without content fails."""
        mock_get_user.return_value = self.user

        from apps.ai.views import AIMessagesView
        view = AIMessagesView.as_view()
        request = self.factory.post(
            f'/api/ai/conversations/{self.conversation_id}/messages',
            {'role': 'user'},
            format='json'
        )
        request.user = self.user

        response = view(request, conversation_id=self.conversation_id)

        self.assertEqual(response.status_code, 400)
        self.assertIn('content', response.data['error'].lower())

    @patch('apps.ai.views.get_user_context')
    def test_invalid_conversation_id(self, mock_get_user):
        """Test invalid conversation ID returns 400."""
        mock_get_user.return_value = self.user

        from apps.ai.views import AIMessagesView
        view = AIMessagesView.as_view()
        request = self.factory.get('/api/ai/conversations/invalid-uuid/messages')
        request.user = self.user

        response = view(request, conversation_id='invalid-uuid')

        self.assertEqual(response.status_code, 400)
