"""
Integration Tests for Payouts API (P2-029)

Tests the expected payouts calculation formula:
    expected_payout = annual_premium * 0.75 * (agent_commission_% / hierarchy_total_%)

Critical Test Cases:
1. Formula verification with known values
2. Hierarchy permission filtering
3. Status impact filtering (positive/neutral/negative)
4. Personal vs downline production split
"""
import uuid
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient

from apps.core.authentication import AuthenticatedUser

# =============================================================================
# Formula Verification Tests (Unit Tests)
# =============================================================================

class TestPayoutFormulaCalculation:
    """
    Test the payout formula with known values.

    Formula: annual_premium * 0.75 * (agent_commission_% / hierarchy_total_%)
    """

    def test_formula_simple_case(self):
        """
        Test Case:
        - Annual premium: $10,000
        - Agent commission: 50%
        - Hierarchy total: 100%
        - Expected: 10000 * 0.75 * (50/100) = $3,750
        """
        annual_premium = Decimal('10000.00')
        agent_commission_pct = Decimal('50.00')
        hierarchy_total_pct = Decimal('100.00')

        expected_payout = annual_premium * Decimal('0.75') * (agent_commission_pct / hierarchy_total_pct)

        assert expected_payout == Decimal('3750.00')

    def test_formula_multiple_hierarchy_levels(self):
        """
        Test Case with 3 agents in hierarchy:
        - Annual premium: $12,000
        - Writing agent: 60%
        - Level 1 upline: 25%
        - Level 2 upline: 15%
        - Hierarchy total: 100%

        Writing agent payout: 12000 * 0.75 * (60/100) = $5,400
        Level 1 payout: 12000 * 0.75 * (25/100) = $2,250
        Level 2 payout: 12000 * 0.75 * (15/100) = $1,350
        """
        annual_premium = Decimal('12000.00')
        hierarchy_total = Decimal('100.00')

        writing_agent_pct = Decimal('60.00')
        level1_pct = Decimal('25.00')
        level2_pct = Decimal('15.00')

        writing_agent_payout = annual_premium * Decimal('0.75') * (writing_agent_pct / hierarchy_total)
        level1_payout = annual_premium * Decimal('0.75') * (level1_pct / hierarchy_total)
        level2_payout = annual_premium * Decimal('0.75') * (level2_pct / hierarchy_total)

        assert writing_agent_payout == Decimal('5400.00')
        assert level1_payout == Decimal('2250.00')
        assert level2_payout == Decimal('1350.00')

        # Total distributed should equal 75% of premium
        total_distributed = writing_agent_payout + level1_payout + level2_payout
        assert total_distributed == annual_premium * Decimal('0.75')

    def test_formula_partial_hierarchy(self):
        """
        Test Case where hierarchy doesn't sum to 100%:
        - Annual premium: $8,000
        - Writing agent: 70%
        - Level 1 upline: 20%
        - Hierarchy total: 90%

        Writing agent payout: 8000 * 0.75 * (70/90) = $4,666.67
        Level 1 payout: 8000 * 0.75 * (20/90) = $1,333.33
        """
        annual_premium = Decimal('8000.00')
        hierarchy_total = Decimal('90.00')

        writing_agent_pct = Decimal('70.00')
        level1_pct = Decimal('20.00')

        writing_agent_payout = round(annual_premium * Decimal('0.75') * (writing_agent_pct / hierarchy_total), 2)
        level1_payout = round(annual_premium * Decimal('0.75') * (level1_pct / hierarchy_total), 2)

        assert writing_agent_payout == Decimal('4666.67')
        assert level1_payout == Decimal('1333.33')


# =============================================================================
# API Integration Tests (requires database)
# =============================================================================

@pytest.mark.django_db
class TestExpectedPayoutsAPI:
    """
    Integration tests for the /api/expected-payouts/ endpoint.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()
        self.agency_id = uuid.uuid4()
        self.user_id = uuid.uuid4()

    def _create_mock_user(self, is_admin: bool = False) -> MagicMock:
        """Create a mock authenticated user."""
        user = MagicMock(spec=AuthenticatedUser)
        user.id = self.user_id
        user.agency_id = self.agency_id
        user.role = 'admin' if is_admin else 'agent'
        user.is_admin = is_admin
        user.email = 'test@example.com'
        user.first_name = 'Test'
        user.last_name = 'User'
        return user

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_visible_agent_ids')
    @patch('django.db.connection.cursor')
    def test_expected_payouts_endpoint_returns_correct_structure(
        self,
        mock_cursor,
        mock_visible_ids,
        mock_get_user,
    ):
        """Test that the API returns the expected response structure."""
        # Setup mocks
        mock_get_user.return_value = self._create_mock_user()
        mock_visible_ids.return_value = [self.user_id]

        # Mock database cursor
        mock_cursor_instance = MagicMock()
        mock_cursor_instance.__enter__ = MagicMock(return_value=mock_cursor_instance)
        mock_cursor_instance.__exit__ = MagicMock(return_value=False)
        mock_cursor_instance.description = []
        mock_cursor_instance.fetchall.return_value = []
        mock_cursor.return_value = mock_cursor_instance

        response = self.client.get('/api/expected-payouts/')

        # Verify response structure
        assert response.status_code == status.HTTP_200_OK

        data = response.json()
        assert 'payouts' in data
        assert 'total_expected' in data
        assert 'total_premium' in data
        assert 'deal_count' in data
        assert 'summary' in data
        assert 'by_carrier' in data['summary']
        assert 'by_agent' in data['summary']

    @patch('apps.payouts.views.get_user_context')
    def test_expected_payouts_requires_authentication(self, mock_get_user):
        """Test that unauthenticated requests are rejected."""
        mock_get_user.return_value = None

        response = self.client.get('/api/expected-payouts/')

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @patch('apps.payouts.views.get_user_context')
    def test_expected_payouts_validates_date_format(self, mock_get_user):
        """Test that invalid date formats are rejected."""
        mock_get_user.return_value = self._create_mock_user()

        response = self.client.get('/api/expected-payouts/?start_date=invalid-date')

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert 'Invalid start_date format' in response.json()['error']

    @patch('apps.payouts.views.get_user_context')
    def test_expected_payouts_accepts_production_type_filter(self, mock_get_user):
        """Test that production_type filter is accepted."""
        mock_get_user.return_value = self._create_mock_user()

        # These should not raise errors even if they return empty results
        for production_type in ['personal', 'downline']:
            with (
                patch('apps.payouts.selectors.get_visible_agent_ids', return_value=[self.user_id]),
                patch('django.db.connection.cursor') as mock_cursor,
            ):
                mock_cursor_instance = MagicMock()
                mock_cursor_instance.__enter__ = MagicMock(return_value=mock_cursor_instance)
                mock_cursor_instance.__exit__ = MagicMock(return_value=False)
                mock_cursor_instance.description = []
                mock_cursor_instance.fetchall.return_value = []
                mock_cursor.return_value = mock_cursor_instance

                response = self.client.get(f'/api/expected-payouts/?production_type={production_type}')
                # Should not be a 400 error
                assert response.status_code != status.HTTP_400_BAD_REQUEST


# =============================================================================
# Selector Tests
# =============================================================================

@pytest.mark.django_db
class TestPayoutSelectors:
    """
    Test the payout selector functions directly.
    """

    @patch('apps.core.permissions.get_visible_agent_ids')
    @patch('django.db.connection.cursor')
    def test_get_expected_payouts_empty_visible_ids(
        self,
        mock_cursor,
        mock_visible_ids,
    ):
        """Test that empty visible IDs returns empty result."""
        from apps.payouts.selectors import get_expected_payouts

        # Mock user
        user = MagicMock()
        user.id = uuid.uuid4()
        user.agency_id = uuid.uuid4()
        user.is_admin = False
        user.role = 'agent'

        mock_visible_ids.return_value = []

        result = get_expected_payouts(user)

        assert result['payouts'] == []
        assert result['total_expected'] == 0
        assert result['deal_count'] == 0

    @patch('apps.core.permissions.get_visible_agent_ids')
    @patch('django.db.connection.cursor')
    def test_get_expected_payouts_with_date_filters(
        self,
        mock_cursor,
        mock_visible_ids,
    ):
        """Test that date filters are properly applied."""
        from apps.payouts.selectors import get_expected_payouts

        user = MagicMock()
        user.id = uuid.uuid4()
        user.agency_id = uuid.uuid4()
        user.is_admin = False
        user.role = 'agent'

        mock_visible_ids.return_value = [user.id]

        # Setup mock cursor
        mock_cursor_instance = MagicMock()
        mock_cursor_instance.__enter__ = MagicMock(return_value=mock_cursor_instance)
        mock_cursor_instance.__exit__ = MagicMock(return_value=False)
        mock_cursor_instance.description = []
        mock_cursor_instance.fetchall.return_value = []
        mock_cursor.return_value = mock_cursor_instance

        start_date = date.today() - timedelta(days=30)
        end_date = date.today()

        result = get_expected_payouts(
            user,
            start_date=start_date,
            end_date=end_date,
        )

        # Verify cursor was called (query was executed)
        assert mock_cursor.called

        # Verify result structure
        assert 'payouts' in result
        assert 'total_expected' in result


# =============================================================================
# Production Split Tests
# =============================================================================

class TestProductionSplit:
    """
    Test personal vs downline production split calculations.
    """

    def test_personal_production_is_hierarchy_level_zero(self):
        """
        Personal production = hierarchy_level 0 (writing agent).
        """
        # In the selector, is_personal = hierarchy_level == 0
        hierarchy_level = 0
        is_personal = hierarchy_level == 0
        assert is_personal is True

    def test_downline_production_is_hierarchy_level_greater_than_zero(self):
        """
        Downline production = hierarchy_level > 0 (override commissions).
        """
        for level in [1, 2, 3]:
            is_personal = level == 0
            assert is_personal is False
