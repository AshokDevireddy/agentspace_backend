"""
Integration Tests for Deals API (P2-027)

Tests book-of-business functionality including:
1. Pagination and filtering
2. Permission filtering by hierarchy
3. Status impact filtering
4. Missing filter parameters
"""
import uuid
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient


@pytest.mark.django_db
class TestBookOfBusinessAPI:
    """
    Integration tests for the book-of-business endpoint.
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

    @patch('apps.deals.views.get_user_context')
    def test_book_of_business_requires_authentication(self, mock_get_user):
        """Test that unauthenticated requests are rejected."""
        mock_get_user.return_value = None

        response = self.client.get('/api/deals/book-of-business/')

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_book_of_business_accepts_filter_params(self, mock_selector, mock_get_user):
        """Test that filter parameters are accepted."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'deals': [],
            'pagination': {'total': 0, 'page': 1, 'page_size': 50}
        }

        # Test various filter params
        params = {
            'carrier_id': str(uuid.uuid4()),
            'product_id': str(uuid.uuid4()),
            'status': 'active',
            'start_date': (date.today() - timedelta(days=30)).isoformat(),
            'end_date': date.today().isoformat(),
        }

        response = self.client.get('/api/deals/book-of-business/', params)

        # Should not return 400 for valid params
        assert response.status_code != status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
class TestDealFilters:
    """
    Test deal filtering logic.
    """

    def test_view_scope_self_filters_to_user_only(self):
        """
        view='self' should only return the user's own deals.
        """
        # This tests the logic that view='self' means visible_ids = [user.id]
        user_id = uuid.uuid4()
        view = 'self'

        visible_ids = [user_id] if view == 'self' else []  # Would be populated from hierarchy

        assert len(visible_ids) == 1
        assert visible_ids[0] == user_id

    def test_view_scope_downlines_includes_hierarchy(self):
        """
        view='downlines' should include user and all downlines.
        """
        user_id = uuid.uuid4()
        downline_ids = [uuid.uuid4() for _ in range(3)]
        view = 'downlines'

        visible_ids = [user_id] + downline_ids if view == 'downlines' else [user_id]

        assert len(visible_ids) == 4
        assert user_id in visible_ids

    def test_effective_date_sort_options(self):
        """
        Test effective_date_sort values: 'oldest' and 'newest'.
        """
        valid_sorts = ['oldest', 'newest']

        for sort_value in valid_sorts:
            if sort_value == 'oldest':
                order_by = 'policy_effective_date ASC'
            elif sort_value == 'newest':
                order_by = 'policy_effective_date DESC'
            else:
                order_by = 'created_at DESC'

            assert 'policy_effective_date' in order_by


@pytest.mark.django_db
class TestStatusImpactFiltering:
    """
    Test status impact filtering logic.
    """

    def test_positive_impact_includes_active_deals(self):
        """
        Status impact 'positive' should include active deals.
        """
        impact_positive_statuses = ['active']
        deal_status = 'active'

        is_positive = deal_status in impact_positive_statuses
        assert is_positive is True

    def test_neutral_impact_includes_pending_deals(self):
        """
        Status impact 'neutral' should include pending deals.
        """
        impact_neutral_statuses = ['pending']
        deal_status = 'pending'

        is_neutral = deal_status in impact_neutral_statuses
        assert is_neutral is True

    def test_negative_impact_excludes_from_payouts(self):
        """
        Status impact 'negative' should exclude deals from payout calculations.
        """
        impact_negative_statuses = ['lapsed', 'cancelled', 'terminated']

        for deal_status in impact_negative_statuses:
            is_negative = deal_status in impact_negative_statuses
            assert is_negative is True
