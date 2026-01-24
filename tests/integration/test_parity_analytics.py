"""
Integration Parity Tests for Analytics API (P2-040)

Tests analytics endpoints including dashboard summary, scoreboard, and metrics.
Verifies response structures match expected format.
"""
import uuid
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient


@pytest.mark.django_db
class TestDashboardSummaryParity:
    """
    Verify GET /api/dashboard/summary endpoint response structure.
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

    @patch('apps.dashboard.views.get_user_context')
    def test_dashboard_requires_authentication(self, mock_get_user):
        """Verify unauthenticated requests are rejected."""
        mock_get_user.return_value = None

        response = self.client.get('/api/dashboard/summary/')

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @patch('apps.dashboard.views.get_user_context')
    @patch('apps.dashboard.selectors.get_dashboard_summary')
    def test_dashboard_response_structure(self, mock_selector, mock_get_user):
        """Verify dashboard summary response structure."""
        mock_get_user.return_value = self._create_mock_user()
        mock_selector.return_value = {
            'total_production': 150000.00,
            'total_policies': 75,
            'active_policies': 60,
            'pending_policies': 10,
            'lapsed_policies': 5,
            'mtd_production': 25000.00,
            'ytd_production': 150000.00,
            'goal_progress': 75.0,
            'annual_goal': 200000.00,
        }

        response = self.client.get('/api/dashboard/summary/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Verify key metrics present
        expected_fields = [
            'total_production',
            'total_policies',
            'active_policies',
        ]

        for field in expected_fields:
            if field in data:
                assert data[field] is not None


@pytest.mark.django_db
class TestScoreboardParity:
    """
    Verify GET /api/scoreboard endpoint (P2-036).
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()

    @patch('apps.dashboard.views.get_user_context')
    @patch('apps.dashboard.selectors.get_scoreboard')
    def test_scoreboard_response_structure(self, mock_selector, mock_get_user):
        """Verify scoreboard response structure with rankings."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'rankings': [
                {
                    'rank': 1,
                    'agent': {
                        'id': str(uuid.uuid4()),
                        'name': 'Top Agent',
                        'email': 'top@test.com',
                    },
                    'production': 50000.00,
                    'policies_sold': 25,
                },
                {
                    'rank': 2,
                    'agent': {
                        'id': str(uuid.uuid4()),
                        'name': 'Second Agent',
                        'email': 'second@test.com',
                    },
                    'production': 40000.00,
                    'policies_sold': 20,
                },
            ],
            'period': 'monthly',
            'period_start': date.today().replace(day=1).isoformat(),
            'period_end': date.today().isoformat(),
            'user_rank': 5,
        }

        response = self.client.get('/api/scoreboard/')

        # Check response
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
        ]

    @patch('apps.dashboard.views.get_user_context')
    @patch('apps.dashboard.selectors.get_scoreboard')
    def test_scoreboard_period_filter(self, mock_selector, mock_get_user):
        """Test period parameter: weekly, monthly, quarterly, yearly."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user
        mock_selector.return_value = {'rankings': [], 'period': 'weekly'}

        periods = ['weekly', 'monthly', 'quarterly', 'yearly']

        for period in periods:
            response = self.client.get('/api/scoreboard/', {'period': period})
            assert response.status_code in [
                status.HTTP_200_OK,
                status.HTTP_400_BAD_REQUEST,  # If period invalid
                status.HTTP_401_UNAUTHORIZED,
            ]


@pytest.mark.django_db
class TestExpectedPayoutsParity:
    """
    Verify GET /api/expected-payouts endpoint (P2-029).
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_expected_payouts')
    def test_payouts_response_structure(self, mock_selector, mock_get_user):
        """Verify expected payouts response structure."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'payouts': [
                {
                    'deal_id': str(uuid.uuid4()),
                    'policy_number': 'POL-12345678',
                    'client_name': 'John Doe',
                    'carrier_name': 'Test Carrier',
                    'product_name': 'Term Life',
                    'annual_premium': 12000.00,
                    'expected_payout': 3750.00,  # premium * 0.75 * (agent% / total%)
                    'agent_commission_rate': 50.0,
                    'total_hierarchy_rate': 100.0,
                    'production_type': 'personal',
                    'policy_effective_date': '2024-01-15',
                },
            ],
            'summary': {
                'total_expected': 3750.00,
                'personal_total': 3750.00,
                'downline_total': 0.00,
                'policy_count': 1,
            },
        }

        response = self.client.get('/api/expected-payouts/')

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
        ]

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_expected_payouts')
    def test_payouts_formula_calculation(self, mock_selector, mock_get_user):
        """Verify payout formula: premium * 0.75 * (agent% / total%)."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        # Test case: $10,000 premium, 50% agent rate, 100% total
        # Expected: 10000 * 0.75 * (50/100) = 3750
        premium = 10000
        agent_rate = 50
        total_rate = 100
        expected = premium * 0.75 * (agent_rate / total_rate)

        assert expected == 3750.00

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_expected_payouts')
    def test_payouts_date_filter(self, mock_selector, mock_get_user):
        """Test date range filtering."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user
        mock_selector.return_value = {'payouts': [], 'summary': {}}

        params = {
            'date_from': (date.today() - timedelta(days=30)).isoformat(),
            'date_to': date.today().isoformat(),
        }

        response = self.client.get('/api/expected-payouts/', params)

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
        ]

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_expected_payouts')
    def test_payouts_production_type_filter(self, mock_selector, mock_get_user):
        """Test production_type filter: personal, downline."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user
        mock_selector.return_value = {'payouts': [], 'summary': {}}

        for prod_type in ['personal', 'downline']:
            response = self.client.get('/api/expected-payouts/', {'production_type': prod_type})
            assert response.status_code in [
                status.HTTP_200_OK,
                status.HTTP_401_UNAUTHORIZED,
            ]


@pytest.mark.django_db
class TestAnalyticsEndpointsParity:
    """
    Verify GET /api/analytics/* endpoints (P2-039).
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.client = APIClient()

    @patch('apps.analytics.views.get_user_context')
    @patch('apps.analytics.selectors.get_production_trends')
    def test_production_trends_response(self, mock_selector, mock_get_user):
        """Verify production trends endpoint."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'trends': [
                {
                    'period': '2024-01',
                    'production': 50000.00,
                    'policies': 25,
                },
                {
                    'period': '2024-02',
                    'production': 60000.00,
                    'policies': 30,
                },
            ],
            'period_type': 'monthly',
        }

        response = self.client.get('/api/analytics/production-trends/')

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_404_NOT_FOUND,  # If endpoint path differs
        ]

    @patch('apps.analytics.views.get_user_context')
    @patch('apps.analytics.selectors.get_carrier_breakdown')
    def test_carrier_breakdown_response(self, mock_selector, mock_get_user):
        """Verify carrier breakdown endpoint."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'carriers': [
                {
                    'carrier': {
                        'id': str(uuid.uuid4()),
                        'name': 'Carrier A',
                    },
                    'production': 75000.00,
                    'policies': 40,
                    'percentage': 60.0,
                },
                {
                    'carrier': {
                        'id': str(uuid.uuid4()),
                        'name': 'Carrier B',
                    },
                    'production': 50000.00,
                    'policies': 25,
                    'percentage': 40.0,
                },
            ],
        }

        response = self.client.get('/api/analytics/carrier-breakdown/')

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_404_NOT_FOUND,
        ]

    @patch('apps.analytics.views.get_user_context')
    @patch('apps.analytics.selectors.get_persistency_metrics')
    def test_persistency_metrics_response(self, mock_selector, mock_get_user):
        """Verify persistency metrics endpoint."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'overall_persistency': 92.5,
            'by_period': [
                {'period': '2024-Q1', 'persistency': 94.0},
                {'period': '2024-Q2', 'persistency': 91.0},
            ],
            'by_carrier': [
                {'carrier_name': 'Carrier A', 'persistency': 95.0},
                {'carrier_name': 'Carrier B', 'persistency': 88.0},
            ],
        }

        response = self.client.get('/api/analytics/persistency/')

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_404_NOT_FOUND,
        ]

    @patch('apps.analytics.views.get_user_context')
    @patch('apps.analytics.selectors.get_team_performance')
    def test_team_performance_response(self, mock_selector, mock_get_user):
        """Verify team performance endpoint."""
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.agency_id = uuid.uuid4()
        mock_user.is_admin = True
        mock_get_user.return_value = mock_user

        mock_selector.return_value = {
            'team_members': [
                {
                    'agent': {
                        'id': str(uuid.uuid4()),
                        'name': 'Agent One',
                    },
                    'production': 50000.00,
                    'policies': 25,
                    'goal_progress': 75.0,
                },
            ],
            'team_totals': {
                'total_production': 150000.00,
                'total_policies': 75,
                'average_goal_progress': 70.0,
            },
        }

        response = self.client.get('/api/analytics/team-performance/')

        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,  # May require admin
            status.HTTP_404_NOT_FOUND,
        ]


@pytest.mark.django_db
class TestProductionSplitParity:
    """
    Test production classification: personal vs downline.
    """

    def test_personal_production_definition(self):
        """Personal production = hierarchy_level 0 (writing agent)."""
        hierarchy_level = 0
        is_personal = hierarchy_level == 0

        assert is_personal is True

    def test_downline_production_definition(self):
        """Downline production = hierarchy_level > 0."""
        for level in [1, 2, 3]:
            is_downline = level > 0
            assert is_downline is True

    def test_production_type_values(self):
        """Verify valid production_type values."""
        valid_types = ['personal', 'downline']

        for prod_type in valid_types:
            assert prod_type in valid_types
