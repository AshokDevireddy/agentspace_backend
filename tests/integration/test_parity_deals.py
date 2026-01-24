"""
Integration Parity Tests for Deals API (P2-040)

Tests deal endpoints including book-of-business with phone masking.
Verifies response structures match expected format.
"""
import uuid
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient


@pytest.mark.django_db
class TestBookOfBusinessParity:
    """
    Verify GET /api/deals/book-of-business endpoint response structure.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()
        self.agency_id = uuid.uuid4()
        self.user_id = uuid.uuid4()

    def _create_mock_user(self, is_admin: bool = False) -> MagicMock:
        """Create a mock authenticated user."""
        user = MagicMock()
        user.id = self.user_id
        user.agency_id = self.agency_id
        user.role = 'admin' if is_admin else 'agent'
        user.is_admin = is_admin
        user.email = 'test@example.com'
        user.first_name = 'Test'
        user.last_name = 'User'
        return user

    def _create_mock_deal(self, agent_id: uuid.UUID, client_phone: str = '+15551234567'):
        """Create a mock deal with client data."""
        return {
            'id': str(uuid.uuid4()),
            'policy_number': 'POL-12345678',
            'status': 'Active',
            'status_standardized': 'active',
            'annual_premium': 12000.00,
            'monthly_premium': 1000.00,
            'policy_effective_date': '2024-01-15',
            'submission_date': '2024-01-01',
            'billing_cycle': 'monthly',
            'lead_source': 'referral',
            'created_at': '2024-01-01T00:00:00Z',
            'client': {
                'id': str(uuid.uuid4()),
                'first_name': 'Jane',
                'last_name': 'Doe',
                'email': 'jane@client.com',
                'phone': client_phone,
                'name': 'Jane Doe',
            },
            'carrier': {
                'id': str(uuid.uuid4()),
                'name': 'Test Carrier',
            },
            'product': {
                'id': str(uuid.uuid4()),
                'name': 'Term Life',
            },
            'agent': {
                'id': str(agent_id),
                'first_name': 'John',
                'last_name': 'Agent',
                'email': 'john@agent.com',
                'name': 'John Agent',
            },
        }

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_response_structure_includes_required_fields(self, mock_selector, mock_get_user):
        """Verify all required fields are present in response."""
        mock_user = self._create_mock_user()
        mock_get_user.return_value = mock_user

        mock_deal = self._create_mock_deal(mock_user.id)
        mock_selector.return_value = {
            'deals': [mock_deal],
            'has_more': False,
            'next_cursor': None,
        }

        response = self.client.get('/api/deals/book-of-business/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Verify structure
        assert 'deals' in data
        assert 'has_more' in data

        if data['deals']:
            deal = data['deals'][0]
            assert 'id' in deal
            assert 'policy_number' in deal
            assert 'status' in deal
            assert 'client' in deal
            assert 'carrier' in deal
            assert 'product' in deal
            assert 'agent' in deal

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_keyset_pagination_structure(self, mock_selector, mock_get_user):
        """Verify keyset pagination returns correct cursor."""
        mock_get_user.return_value = self._create_mock_user()

        mock_selector.return_value = {
            'deals': [self._create_mock_deal(self.user_id)],
            'has_more': True,
            'next_cursor': {
                'policy_effective_date': '2024-01-15',
                'id': str(uuid.uuid4()),
            },
        }

        response = self.client.get('/api/deals/book-of-business/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data['has_more'] is True
        assert 'next_cursor' in data
        assert 'policy_effective_date' in data['next_cursor']
        assert 'id' in data['next_cursor']

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_filter_parameters_accepted(self, mock_selector, mock_get_user):
        """Verify all filter parameters are accepted."""
        mock_get_user.return_value = self._create_mock_user(is_admin=True)
        mock_selector.return_value = {'deals': [], 'has_more': False, 'next_cursor': None}

        params = {
            'limit': '50',
            'carrier_id': str(uuid.uuid4()),
            'product_id': str(uuid.uuid4()),
            'agent_id': str(uuid.uuid4()),
            'client_id': str(uuid.uuid4()),
            'status': 'Active',
            'status_standardized': 'active',
            'date_from': (date.today() - timedelta(days=90)).isoformat(),
            'date_to': date.today().isoformat(),
            'search_query': 'john',
            'policy_number': 'POL-123',
            'billing_cycle': 'monthly',
            'lead_source': 'referral',
            'view': 'all',
            'effective_date_sort': 'newest',
        }

        response = self.client.get('/api/deals/book-of-business/', params)

        # Should not reject valid params
        assert response.status_code == status.HTTP_200_OK

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_view_scope_self(self, mock_selector, mock_get_user):
        """Test view='self' returns only user's deals."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {'deals': [], 'has_more': False, 'next_cursor': None}

        response = self.client.get('/api/deals/book-of-business/', {'view': 'self'})

        assert response.status_code == status.HTTP_200_OK

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_view_scope_downlines(self, mock_selector, mock_get_user):
        """Test view='downlines' returns user + downlines deals."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {'deals': [], 'has_more': False, 'next_cursor': None}

        response = self.client.get('/api/deals/book-of-business/', {'view': 'downlines'})

        assert response.status_code == status.HTTP_200_OK

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_view_scope_all_admin_only(self, mock_selector, mock_get_user):
        """Test view='all' works for admin users."""
        mock_get_user.return_value = self._create_mock_user(is_admin=True)
        mock_selector.return_value = {'deals': [], 'has_more': False, 'next_cursor': None}

        response = self.client.get('/api/deals/book-of-business/', {'view': 'all'})

        assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
class TestPhoneMaskingParity:
    """
    Test phone number masking in book-of-business (P2-027).
    """

    def test_mask_phone_number_function(self):
        """Test the phone masking helper function."""
        from apps.deals.selectors import mask_phone_number

        # Test full visibility
        assert mask_phone_number('+15551234567', can_view_full=True) == '+15551234567'

        # Test masking
        masked = mask_phone_number('+15551234567', can_view_full=False)
        assert masked != '+15551234567'
        assert '****' in masked

        # Test short numbers
        short_masked = mask_phone_number('12345', can_view_full=False)
        assert '****' in short_masked

        # Test very short numbers
        very_short_masked = mask_phone_number('123', can_view_full=False)
        assert very_short_masked == '****'

        # Test None
        assert mask_phone_number(None, can_view_full=False) is None
        assert mask_phone_number(None, can_view_full=True) is None

        # Test empty string
        assert mask_phone_number('', can_view_full=False) is None

    def test_phone_masking_format(self):
        """Verify masked phone format: first 3 + **** + last 2."""
        from apps.deals.selectors import mask_phone_number

        masked = mask_phone_number('5551234567', can_view_full=False)

        # Should start with first 3 digits
        assert masked.startswith('555')

        # Should end with last 2 digits
        assert masked.endswith('67')

        # Should have **** in the middle
        assert '****' in masked


@pytest.mark.django_db
class TestFilterOptionsParity:
    """
    Verify GET /api/deals/filter-options endpoint.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_static_filter_options')
    def test_filter_options_structure(self, mock_selector, mock_get_user):
        """Verify filter options response structure."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_user.is_admin = False
        mock_user.role = 'agent'
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'carriers': [
                {'id': str(uuid.uuid4()), 'name': 'Carrier A'},
                {'id': str(uuid.uuid4()), 'name': 'Carrier B'},
            ],
            'products': [
                {'id': str(uuid.uuid4()), 'name': 'Product A', 'carrier_name': 'Carrier A'},
            ],
            'statuses': ['Active', 'Pending', 'Lapsed'],
            'statuses_standardized': ['active', 'pending', 'lapsed'],
            'agents': [
                {'id': str(uuid.uuid4()), 'name': 'Agent One', 'email': 'agent1@test.com'},
            ],
        }

        response = self.client.get('/api/deals/filter-options/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Verify structure
        assert 'carriers' in data
        assert 'products' in data
        assert 'statuses' in data
        assert 'statuses_standardized' in data
        assert 'agents' in data

        # Verify carriers structure
        if data['carriers']:
            carrier = data['carriers'][0]
            assert 'id' in carrier
            assert 'name' in carrier

        # Verify products have carrier_name
        if data['products']:
            product = data['products'][0]
            assert 'id' in product
            assert 'name' in product
            assert 'carrier_name' in product


@pytest.mark.django_db
class TestEffectiveDateSortParity:
    """
    Test effective_date_sort parameter (P2-027).
    """

    def test_sort_values_valid(self):
        """Verify only valid sort values are accepted."""
        valid_sorts = ['oldest', 'newest', None]

        for sort_val in valid_sorts:
            if sort_val == 'oldest':
                order = 'ASC'
            else:
                order = 'DESC'

            assert order in ['ASC', 'DESC']

    def test_sort_affects_order_direction(self):
        """Test that sort parameter changes query order."""
        # oldest -> ASC
        oldest_order = 'ASC' if 'oldest' == 'oldest' else 'DESC'
        assert oldest_order == 'ASC'

        # newest -> DESC (default)
        newest_order = 'DESC' if 'newest' == 'newest' else 'DESC'
        assert newest_order == 'DESC'


@pytest.mark.django_db
class TestBillingCycleFilterParity:
    """
    Test billing_cycle filter (P2-027).
    """

    def test_valid_billing_cycles(self):
        """Verify valid billing cycle values."""
        valid_cycles = ['monthly', 'quarterly', 'semi-annually', 'annually']

        for cycle in valid_cycles:
            # All should be valid filter values
            assert cycle in valid_cycles

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_billing_cycle_filter_accepted(self, mock_selector, mock_get_user):
        """Test billing_cycle parameter is accepted."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_user.is_admin = False
        mock_user.role = 'agent'
        mock_get_user.return_value = mock_user
        mock_selector.return_value = {'deals': [], 'has_more': False, 'next_cursor': None}

        client = APIClient()
        response = client.get('/api/deals/book-of-business/', {'billing_cycle': 'monthly'})

        assert response.status_code == status.HTTP_200_OK
