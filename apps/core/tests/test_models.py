"""
Model Unit Tests (P1-020)

Tests for core Django models including hierarchy methods.
"""
import uuid
from unittest.mock import patch, MagicMock

from django.test import TestCase

from apps.core.models import (
    Agency, User, Position, Carrier, Product, Client, Deal,
    PositionProductCommission, StatusMapping, Conversation, Message,
    DraftMessage, AIConversation, AIMessage, FeatureFlag,
)


class AgencyModelTests(TestCase):
    """Tests for the Agency model."""

    def test_agency_str_with_display_name(self):
        """Agency string representation uses display_name when available."""
        agency = Agency(
            id=uuid.uuid4(),
            name='test_agency',
            display_name='Test Agency Inc.'
        )
        self.assertEqual(str(agency), 'Test Agency Inc.')

    def test_agency_str_without_display_name(self):
        """Agency string representation falls back to name."""
        agency = Agency(
            id=uuid.uuid4(),
            name='test_agency',
            display_name=None
        )
        self.assertEqual(str(agency), 'test_agency')


class UserModelTests(TestCase):
    """Tests for the User model."""

    def test_user_str_representation(self):
        """User string representation includes name and email."""
        user = User(
            id=uuid.uuid4(),
            first_name='John',
            last_name='Doe',
            email='john@example.com'
        )
        self.assertEqual(str(user), 'John Doe (john@example.com)')

    def test_user_str_without_name(self):
        """User string representation handles missing name."""
        user = User(
            id=uuid.uuid4(),
            first_name=None,
            last_name=None,
            email='john@example.com'
        )
        self.assertIn('john@example.com', str(user))

    def test_full_name_property(self):
        """Full name property combines first and last name."""
        user = User(
            id=uuid.uuid4(),
            first_name='John',
            last_name='Doe',
            email='john@example.com'
        )
        self.assertEqual(user.full_name, 'John Doe')

    def test_full_name_falls_back_to_email(self):
        """Full name falls back to email when name is empty."""
        user = User(
            id=uuid.uuid4(),
            first_name='',
            last_name='',
            email='john@example.com'
        )
        self.assertEqual(user.full_name, 'john@example.com')

    @patch('apps.core.models.connection')
    def test_get_downline_calls_database(self, mock_connection):
        """get_downline method executes correct SQL."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (uuid.uuid4(),),
            (uuid.uuid4(),),
        ]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = User(
            id=uuid.uuid4(),
            agency_id=agency_id
        )

        result = user.get_downline()

        self.assertEqual(len(result), 2)
        mock_cursor.execute.assert_called_once()
        # Verify recursive CTE is used
        call_args = mock_cursor.execute.call_args[0][0]
        self.assertIn('WITH RECURSIVE', call_args)
        self.assertIn('downline', call_args)

    @patch('apps.core.models.connection')
    def test_get_upline_chain_calls_database(self, mock_connection):
        """get_upline_chain method executes correct SQL."""
        mock_cursor = MagicMock()
        upline_id_1 = uuid.uuid4()
        upline_id_2 = uuid.uuid4()
        mock_cursor.fetchall.return_value = [
            (upline_id_1,),
            (upline_id_2,),
        ]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        user = User(id=uuid.uuid4())

        result = user.get_upline_chain()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], upline_id_1)
        self.assertEqual(result[1], upline_id_2)

    @patch('apps.core.models.connection')
    def test_is_in_downline_returns_true(self, mock_connection):
        """is_in_downline returns True when user is in downline."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = User(id=uuid.uuid4(), agency_id=agency_id)

        result = user.is_in_downline(uuid.uuid4())

        self.assertTrue(result)

    @patch('apps.core.models.connection')
    def test_is_in_downline_returns_false(self, mock_connection):
        """is_in_downline returns False when user is not in downline."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = User(id=uuid.uuid4(), agency_id=agency_id)

        result = user.is_in_downline(uuid.uuid4())

        self.assertFalse(result)


class PositionModelTests(TestCase):
    """Tests for the Position model."""

    def test_position_str_representation(self):
        """Position string representation includes name and level."""
        position = Position(
            id=uuid.uuid4(),
            name='Senior Agent',
            level=3
        )
        self.assertEqual(str(position), 'Senior Agent (Level 3)')


class CarrierModelTests(TestCase):
    """Tests for the Carrier model."""

    def test_carrier_str_representation(self):
        """Carrier string representation is the name."""
        carrier = Carrier(
            id=uuid.uuid4(),
            name='ABC Insurance'
        )
        self.assertEqual(str(carrier), 'ABC Insurance')


class ProductModelTests(TestCase):
    """Tests for the Product model."""

    def test_product_str_representation(self):
        """Product string representation includes carrier."""
        carrier = Carrier(id=uuid.uuid4(), name='ABC Insurance')
        product = Product(
            id=uuid.uuid4(),
            name='Life Plan',
            carrier=carrier
        )
        self.assertEqual(str(product), 'Life Plan (ABC Insurance)')

    def test_product_str_without_carrier(self):
        """Product string representation handles missing carrier."""
        product = Product(
            id=uuid.uuid4(),
            name='Life Plan',
            carrier=None
        )
        self.assertEqual(str(product), 'Life Plan (No carrier)')


class DealModelTests(TestCase):
    """Tests for the Deal model."""

    def test_deal_str_representation(self):
        """Deal string representation includes policy number and client."""
        client = Client(
            id=uuid.uuid4(),
            first_name='Jane',
            last_name='Smith'
        )
        deal = Deal(
            id=uuid.uuid4(),
            policy_number='POL123',
            client=client
        )
        self.assertEqual(str(deal), 'POL123 - Jane Smith')

    def test_deal_str_without_client(self):
        """Deal string representation handles missing client."""
        deal = Deal(
            id=uuid.uuid4(),
            policy_number='POL123',
            client=None
        )
        self.assertEqual(str(deal), 'POL123 - No client')

    def test_deal_client_name_property(self):
        """Deal client_name property returns formatted name."""
        client = Client(
            id=uuid.uuid4(),
            first_name='Jane',
            last_name='Smith'
        )
        deal = Deal(id=uuid.uuid4(), client=client)
        self.assertEqual(deal.client_name, 'Jane Smith')

    def test_deal_client_name_without_client(self):
        """Deal client_name property returns empty string without client."""
        deal = Deal(id=uuid.uuid4(), client=None)
        self.assertEqual(deal.client_name, '')


class StatusMappingModelTests(TestCase):
    """Tests for the StatusMapping model."""

    def test_status_mapping_str_representation(self):
        """StatusMapping string shows carrier mapping."""
        carrier = Carrier(id=uuid.uuid4(), name='ABC Insurance')
        mapping = StatusMapping(
            id=uuid.uuid4(),
            carrier=carrier,
            raw_status='ACTIVE_POLICY',
            standardized_status='active'
        )
        self.assertEqual(str(mapping), 'ABC Insurance: ACTIVE_POLICY -> active')


class ConversationModelTests(TestCase):
    """Tests for the Conversation model."""

    def test_conversation_str_with_client(self):
        """Conversation string includes client name."""
        client = Client(
            id=uuid.uuid4(),
            first_name='Jane',
            last_name='Smith'
        )
        conversation = Conversation(
            id=uuid.uuid4(),
            client=client,
            phone_number='+1234567890'
        )
        self.assertEqual(str(conversation), 'Conversation with Jane Smith')

    def test_conversation_str_without_client(self):
        """Conversation string uses phone number without client."""
        conversation = Conversation(
            id=uuid.uuid4(),
            client=None,
            phone_number='+1234567890'
        )
        self.assertEqual(str(conversation), 'Conversation with +1234567890')


class MessageModelTests(TestCase):
    """Tests for the Message model."""

    def test_message_str_representation(self):
        """Message string shows direction and truncated content."""
        conversation = Conversation(id=uuid.uuid4(), phone_number='+1234567890')
        message = Message(
            id=uuid.uuid4(),
            conversation=conversation,
            direction='outbound',
            content='Hello, this is a test message that is longer than fifty characters.'
        )
        result = str(message)
        self.assertIn('outbound', result)
        self.assertIn('Hello', result)


class FeatureFlagModelTests(TestCase):
    """Tests for the FeatureFlag model."""

    def test_feature_flag_str_global(self):
        """Global feature flag string representation."""
        flag = FeatureFlag(
            id=uuid.uuid4(),
            name='use_django_auth',
            is_enabled=True,
            agency=None
        )
        result = str(flag)
        self.assertIn('use_django_auth', result)
        self.assertIn('Global', result)
        self.assertIn('Enabled', result)

    def test_feature_flag_str_agency_specific(self):
        """Agency-specific feature flag string representation."""
        agency = Agency(id=uuid.uuid4(), name='test_agency', display_name='Test Agency')
        flag = FeatureFlag(
            id=uuid.uuid4(),
            name='use_django_auth',
            is_enabled=False,
            agency=agency
        )
        result = str(flag)
        self.assertIn('use_django_auth', result)
        self.assertIn('Test Agency', result)
        self.assertIn('Disabled', result)
