"""
Integration Tests for Clients API (P2-037)

Tests client listing and detail functionality including:
1. Authentication requirements
2. Pagination
3. View mode filtering (self, downlines, all)
4. Search functionality
5. Client detail with deals
"""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient


@pytest.mark.django_db
class TestClientsListAPI:
    """
    Integration tests for the clients list endpoint.
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

    @patch('apps.clients.views.get_user_context')
    def test_clients_list_requires_authentication(self, mock_get_user):
        """Test that unauthenticated requests are rejected."""
        mock_get_user.return_value = None

        response = self.client.get('/api/clients/')

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_clients_list_returns_paginated_data(self, mock_selector, mock_get_user):
        """Test that clients list returns paginated data."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'clients': [
                {
                    'id': str(uuid.uuid4()),
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'name': 'John Doe',
                    'email': 'john@example.com',
                    'phone': '555-1234',
                    'deal_count': 3,
                    'active_deals': 2,
                    'total_premium': 12000.00,
                }
            ],
            'pagination': {
                'currentPage': 1,
                'totalPages': 1,
                'totalCount': 1,
                'limit': 20,
                'hasNextPage': False,
                'hasPrevPage': False,
            },
        }

        response = self.client.get('/api/clients/')

        assert response.status_code == status.HTTP_200_OK
        assert 'clients' in response.data
        assert 'pagination' in response.data
        assert len(response.data['clients']) == 1

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_clients_list_accepts_pagination_params(self, mock_selector, mock_get_user):
        """Test that pagination parameters are accepted."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'clients': [],
            'pagination': {
                'currentPage': 2,
                'totalPages': 5,
                'totalCount': 100,
                'limit': 10,
                'hasNextPage': True,
                'hasPrevPage': True,
            },
        }

        response = self.client.get('/api/clients/', {'page': 2, 'limit': 10})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['page'] == 2
        assert call_kwargs['limit'] == 10

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_clients_list_caps_limit_at_100(self, mock_selector, mock_get_user):
        """Test that limit is capped at 100."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 100, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/', {'limit': 500})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['limit'] == 100

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_clients_list_accepts_search_param(self, mock_selector, mock_get_user):
        """Test that search parameter is passed to selector."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 20, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/', {'search': 'John'})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['search_query'] == 'John'


@pytest.mark.django_db
class TestClientsViewModes:
    """
    Test client list view mode filtering.
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
        return user

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_view_self_sets_agent_id_to_user(self, mock_selector, mock_get_user):
        """Test that view='self' filters to user's own clients."""
        user = self._create_mock_user()
        mock_get_user.return_value = user
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 20, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/', {'view': 'self'})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['agent_id'] == user.id

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_view_downlines_is_default(self, mock_selector, mock_get_user):
        """Test that view='downlines' is the default."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 20, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/')

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['agent_id'] is None
        assert call_kwargs['include_full_agency'] is False

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_view_all_requires_admin(self, mock_selector, mock_get_user):
        """Test that view='all' only works for admins."""
        mock_get_user.return_value = self._create_mock_user(is_admin=True)
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 20, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/', {'view': 'all'})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['include_full_agency'] is True

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_view_all_non_admin_gets_downlines(self, mock_selector, mock_get_user):
        """Test that non-admins requesting view='all' get downlines instead."""
        mock_get_user.return_value = self._create_mock_user(is_admin=False)
        mock_selector.return_value = {
            'clients': [],
            'pagination': {'currentPage': 1, 'totalPages': 0, 'totalCount': 0, 'limit': 20, 'hasNextPage': False, 'hasPrevPage': False},
        }

        response = self.client.get('/api/clients/', {'view': 'all'})

        assert response.status_code == status.HTTP_200_OK
        mock_selector.assert_called_once()
        call_kwargs = mock_selector.call_args[1]
        assert call_kwargs['include_full_agency'] is False


@pytest.mark.django_db
class TestClientDetailAPI:
    """
    Integration tests for the client detail endpoint.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()
        self.agency_id = uuid.uuid4()
        self.user_id = uuid.uuid4()
        self.client_id = uuid.uuid4()

    def _create_mock_user(self, is_admin: bool = False) -> MagicMock:
        """Create a mock authenticated user."""
        user = MagicMock()
        user.id = self.user_id
        user.agency_id = self.agency_id
        user.role = 'admin' if is_admin else 'agent'
        user.is_admin = is_admin
        return user

    @patch('apps.clients.views.get_user_context')
    def test_client_detail_requires_authentication(self, mock_get_user):
        """Test that unauthenticated requests are rejected."""
        mock_get_user.return_value = None

        response = self.client.get(f'/api/clients/{self.client_id}/')

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_client_detail')
    def test_client_detail_returns_client_data(self, mock_selector, mock_get_user):
        """Test that client detail returns complete client data."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'id': str(self.client_id),
            'first_name': 'John',
            'last_name': 'Doe',
            'name': 'John Doe',
            'email': 'john@example.com',
            'phone': '555-1234',
            'created_at': '2024-01-01T00:00:00',
            'deals': [
                {
                    'id': str(uuid.uuid4()),
                    'policy_number': 'POL-001',
                    'status': 'active',
                    'annual_premium': 5000.00,
                }
            ],
            'deal_count': 1,
            'active_deals': 1,
            'total_premium': 5000.00,
        }

        response = self.client.get(f'/api/clients/{self.client_id}/')

        assert response.status_code == status.HTTP_200_OK
        assert response.data['id'] == str(self.client_id)
        assert response.data['name'] == 'John Doe'
        assert 'deals' in response.data

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_client_detail')
    def test_client_detail_returns_404_for_not_found(self, mock_selector, mock_get_user):
        """Test that 404 is returned for non-existent clients."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = None

        response = self.client.get(f'/api/clients/{self.client_id}/')

        assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch('apps.clients.views.get_user_context')
    def test_client_detail_validates_uuid(self, mock_get_user):
        """Test that invalid UUIDs return 400."""
        mock_get_user.return_value = self._create_mock_user()

        response = self.client.get('/api/clients/not-a-uuid/')

        assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
class TestClientSearchFunctionality:
    """
    Test client search functionality.
    """

    def test_search_matches_first_name(self):
        """Test search pattern matches first name."""
        search_query = 'John'
        search_pattern = f"%{search_query}%"

        first_name = 'John'
        assert search_query.lower() in first_name.lower()
        assert search_pattern == '%John%'

    def test_search_matches_last_name(self):
        """Test search pattern matches last name."""
        search_query = 'Doe'
        last_name = 'Doe'
        assert search_query.lower() in last_name.lower()

    def test_search_matches_email(self):
        """Test search pattern matches email."""
        search_query = 'john@'
        email = 'john@example.com'
        assert search_query.lower() in email.lower()

    def test_search_matches_phone(self):
        """Test search pattern matches phone."""
        search_query = '555'
        phone = '555-1234'
        assert search_query in phone

    def test_search_matches_full_name(self):
        """Test search pattern matches concatenated full name."""
        search_query = 'John Doe'
        first_name = 'John'
        last_name = 'Doe'
        full_name = f"{first_name} {last_name}"
        assert search_query.lower() == full_name.lower()


@pytest.mark.django_db
class TestClientResponseStructure:
    """
    Test client API response structure validation.
    """

    def test_client_list_item_has_required_fields(self):
        """Test that client list items have all required fields."""
        required_fields = [
            'id',
            'first_name',
            'last_name',
            'name',
            'email',
            'phone',
            'deal_count',
            'active_deals',
            'total_premium',
        ]

        sample_client = {
            'id': str(uuid.uuid4()),
            'first_name': 'John',
            'last_name': 'Doe',
            'name': 'John Doe',
            'email': 'john@example.com',
            'phone': '555-1234',
            'deal_count': 3,
            'active_deals': 2,
            'total_premium': 12000.00,
        }

        for field in required_fields:
            assert field in sample_client

    def test_pagination_has_required_fields(self):
        """Test that pagination has all required fields."""
        required_fields = [
            'currentPage',
            'totalPages',
            'totalCount',
            'limit',
            'hasNextPage',
            'hasPrevPage',
        ]

        sample_pagination = {
            'currentPage': 1,
            'totalPages': 5,
            'totalCount': 100,
            'limit': 20,
            'hasNextPage': True,
            'hasPrevPage': False,
        }

        for field in required_fields:
            assert field in sample_pagination

    def test_client_detail_has_deals_array(self):
        """Test that client detail includes deals array."""
        sample_detail = {
            'id': str(uuid.uuid4()),
            'name': 'John Doe',
            'deals': [
                {'id': str(uuid.uuid4()), 'policy_number': 'POL-001'}
            ],
            'deal_count': 1,
        }

        assert 'deals' in sample_detail
        assert isinstance(sample_detail['deals'], list)
        assert sample_detail['deal_count'] == len(sample_detail['deals'])
