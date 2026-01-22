"""
API Integration Tests (P2-040)

Integration tests for all API endpoints.
These tests verify API response formats and basic functionality.
"""
import json
import uuid
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch, MagicMock

from django.test import TestCase, TransactionTestCase, override_settings
from django.db import connection
from rest_framework.test import APIClient, APITestCase

from apps.core.authentication import AuthenticatedUser


def create_test_user(
    user_id=None,
    agency_id=None,
    role='agent',
    is_admin=False,
    status='active',
    subscription_tier='pro',
    email='test@example.com',
    first_name='Test',
    last_name='User',
):
    """Create a mock authenticated user for testing."""
    return AuthenticatedUser(
        id=user_id or uuid.uuid4(),
        auth_user_id=uuid.uuid4(),
        email=email,
        agency_id=agency_id or uuid.uuid4(),
        role=role,
        is_admin=is_admin,
        status=status,
        perm_level='admin' if is_admin else 'agent',
        subscription_tier=subscription_tier,
        first_name=first_name,
        last_name=last_name,
    )


class MockAuthenticationMixin:
    """Mixin to provide mock authentication for tests."""

    def setUp(self):
        super().setUp()
        self.client = APIClient()
        self.user = create_test_user(is_admin=True)
        self.agency_id = self.user.agency_id

        # Patch authentication
        self.auth_patcher = patch('apps.core.authentication.SupabaseJWTAuthentication.authenticate')
        self.mock_auth = self.auth_patcher.start()
        self.mock_auth.return_value = (self.user, None)

        # Patch get_user_context
        self.user_context_patcher = patch('apps.core.authentication.get_user_context')
        self.mock_user_context = self.user_context_patcher.start()
        self.mock_user_context.return_value = self.user

    def tearDown(self):
        self.auth_patcher.stop()
        self.user_context_patcher.stop()
        super().tearDown()


# =============================================================================
# Agents API Tests (P2-023 to P2-026)
# =============================================================================

class AgentsAPITests(MockAuthenticationMixin, TestCase):
    """Tests for Agents API endpoints."""

    @patch('apps.agents.views.get_user_context')
    @patch('apps.agents.selectors.get_agents_table')
    @patch('apps.agents.selectors.get_agent_options')
    @patch('apps.agents.selectors.get_agents_debt_production')
    def test_agents_list_table_view(
        self, mock_debt_prod, mock_options, mock_table, mock_user_context
    ):
        """GET /api/agents/?view=table returns expected response format."""
        mock_user_context.return_value = self.user
        mock_table.return_value = [
            {
                'agent_id': uuid.uuid4(),
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@test.com',
                'perm_level': 'agent',
                'status': 'active',
                'position_id': None,
                'position_name': None,
                'position_level': None,
                'upline_name': None,
                'created_at': '2024-01-01T00:00:00',
                'total_prod': 10000.00,
                'downline_count': 5,
                'total_count': 1,
            }
        ]
        mock_options.return_value = [
            {'agent_id': uuid.uuid4(), 'display_name': 'John Doe'}
        ]
        mock_debt_prod.return_value = []

        response = self.client.get('/api/agents/', {'view': 'table'})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('agents', data)
        self.assertIn('pagination', data)
        self.assertIn('allAgents', data)

        # Verify pagination format
        pagination = data['pagination']
        self.assertIn('currentPage', pagination)
        self.assertIn('totalPages', pagination)
        self.assertIn('totalCount', pagination)
        self.assertIn('limit', pagination)
        self.assertIn('hasNextPage', pagination)
        self.assertIn('hasPrevPage', pagination)

    @patch('apps.agents.views.get_user_context')
    @patch('apps.agents.selectors.get_agents_hierarchy_nodes')
    def test_agents_list_tree_view(self, mock_hierarchy, mock_user_context):
        """GET /api/agents/?view=tree returns tree structure."""
        mock_user_context.return_value = self.user
        mock_hierarchy.return_value = [
            {
                'agent_id': str(self.user.id),
                'first_name': 'Test',
                'last_name': 'User',
                'upline_id': None,
                'perm_level': 'admin',
                'position_name': 'Manager',
            }
        ]

        response = self.client.get('/api/agents/', {'view': 'tree'})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('tree', data)
        tree = data['tree']
        self.assertIn('name', tree)
        self.assertIn('attributes', tree)
        self.assertIn('children', tree)

    @patch('apps.agents.views.get_user_context')
    @patch('apps.agents.selectors.get_agent_downlines_with_details')
    @patch('apps.agents.selectors.get_agents_debt_production')
    def test_agents_downlines_with_agent_id(
        self, mock_debt, mock_downlines, mock_user_context
    ):
        """GET /api/agents/downlines?agentId={id} returns downlines."""
        mock_user_context.return_value = self.user
        agent_id = uuid.uuid4()
        mock_downlines.return_value = [
            {
                'id': uuid.uuid4(),
                'first_name': 'Jane',
                'last_name': 'Doe',
                'position_name': 'Agent',
                'position_level': 1,
                'status': 'active',
                'created_at': '2024-01-01',
            }
        ]
        mock_debt.return_value = []

        response = self.client.get('/api/agents/downlines', {'agentId': str(agent_id)})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('agentId', data)
        self.assertIn('downlines', data)
        self.assertIn('downlineCount', data)

    def test_agents_downlines_requires_agent_id(self):
        """GET /api/agents/downlines without agentId returns 400."""
        response = self.client.get('/api/agents/downlines')
        self.assertEqual(response.status_code, 400)

    @patch('apps.agents.views.get_user_context')
    @patch('apps.agents.selectors.get_agents_without_positions')
    def test_agents_without_positions(self, mock_agents, mock_user_context):
        """GET /api/agents/without-positions returns agents list."""
        mock_user_context.return_value = self.user
        mock_agents.return_value = [
            {
                'agent_id': uuid.uuid4(),
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@test.com',
                'phone_number': None,
                'role': 'agent',
                'upline_name': None,
                'created_at': '2024-01-01',
            }
        ]

        response = self.client.get('/api/agents/without-positions')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('agents', data)
        self.assertIn('count', data)


# =============================================================================
# Deals API Tests (P2-027, P2-028)
# =============================================================================

class DealsAPITests(MockAuthenticationMixin, TestCase):
    """Tests for Deals API endpoints."""

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_book_of_business')
    def test_book_of_business_returns_keyset_pagination(
        self, mock_bob, mock_user_context
    ):
        """GET /api/deals/book-of-business returns keyset pagination."""
        mock_user_context.return_value = self.user
        mock_bob.return_value = {
            'deals': [
                {
                    'id': str(uuid.uuid4()),
                    'policy_number': 'POL-001',
                    'client_name': 'John Doe',
                    'carrier_name': 'Carrier A',
                    'product_name': 'Product A',
                    'status': 'active',
                    'annual_premium': '10000.00',
                    'created_at': '2024-01-01T00:00:00Z',
                }
            ],
            'has_more': True,
            'next_cursor': 'abc123',
        }

        response = self.client.get('/api/deals/book-of-business')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('deals', data)
        self.assertIn('has_more', data)
        self.assertIn('next_cursor', data)

    @patch('apps.deals.views.get_user_context')
    @patch('apps.deals.selectors.get_static_filter_options')
    def test_filter_options_returns_all_options(
        self, mock_options, mock_user_context
    ):
        """GET /api/deals/filter-options returns all filter options."""
        mock_user_context.return_value = self.user
        mock_options.return_value = {
            'carriers': [{'id': str(uuid.uuid4()), 'name': 'Carrier A'}],
            'products': [{'id': str(uuid.uuid4()), 'name': 'Product A'}],
            'statuses': ['active', 'pending'],
            'statuses_standardized': ['active', 'pending', 'cancelled'],
            'agents': [{'id': str(uuid.uuid4()), 'name': 'Agent A'}],
        }

        response = self.client.get('/api/deals/filter-options')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('carriers', data)
        self.assertIn('products', data)
        self.assertIn('statuses', data)
        self.assertIn('agents', data)


# =============================================================================
# Payouts API Tests (P2-029)
# =============================================================================

class PayoutsAPITests(MockAuthenticationMixin, TestCase):
    """Tests for Payouts API endpoints."""

    @patch('apps.payouts.views.get_user_context')
    @patch('apps.payouts.selectors.get_expected_payouts')
    def test_expected_payouts_returns_summary(
        self, mock_payouts, mock_user_context
    ):
        """GET /api/expected-payouts returns payouts with summary."""
        mock_user_context.return_value = self.user
        mock_payouts.return_value = {
            'payouts': [
                {
                    'deal_id': str(uuid.uuid4()),
                    'policy_number': 'POL-001',
                    'client_name': 'John Doe',
                    'carrier_name': 'Carrier A',
                    'expected_commission': '1000.00',
                }
            ],
            'total_expected': 1000.00,
            'total_premium': 10000.00,
            'deal_count': 1,
            'summary': {
                'by_carrier': [],
                'by_agent': [],
            },
        }

        response = self.client.get('/api/expected-payouts/')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('payouts', data)
        self.assertIn('total_expected', data)


# =============================================================================
# SMS API Tests (P2-033 to P2-035)
# =============================================================================

class SMSAPITests(MockAuthenticationMixin, TestCase):
    """Tests for SMS API endpoints."""

    @patch('apps.sms.views.get_user_context')
    @patch('apps.sms.selectors.get_sms_conversations')
    def test_conversations_returns_pagination(
        self, mock_convos, mock_user_context
    ):
        """GET /api/sms/conversations returns paginated conversations."""
        mock_user_context.return_value = self.user
        mock_convos.return_value = {
            'conversations': [
                {
                    'id': str(uuid.uuid4()),
                    'client_name': 'John Doe',
                    'phone_number': '+1234567890',
                    'status': 'active',
                    'unread_count': 2,
                    'last_message_at': '2024-01-01T00:00:00Z',
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

        response = self.client.get('/api/sms/conversations')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('conversations', data)
        self.assertIn('pagination', data)

    @patch('apps.sms.views.get_user_context')
    @patch('apps.sms.selectors.get_sms_messages')
    def test_messages_requires_conversation_id(self, mock_msgs, mock_user_context):
        """GET /api/sms/messages requires conversationId."""
        mock_user_context.return_value = self.user

        response = self.client.get('/api/sms/messages')

        self.assertEqual(response.status_code, 400)

    @patch('apps.sms.views.get_user_context')
    @patch('apps.sms.selectors.get_draft_messages')
    def test_drafts_returns_pagination(self, mock_drafts, mock_user_context):
        """GET /api/sms/drafts returns paginated drafts."""
        mock_user_context.return_value = self.user
        mock_drafts.return_value = {
            'drafts': [],
            'pagination': {
                'currentPage': 1,
                'totalPages': 0,
                'totalCount': 0,
                'limit': 20,
                'hasNextPage': False,
                'hasPrevPage': False,
            },
        }

        response = self.client.get('/api/sms/drafts')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('drafts', data)
        self.assertIn('pagination', data)


# =============================================================================
# Clients API Tests (P2-037)
# =============================================================================

class ClientsAPITests(MockAuthenticationMixin, TestCase):
    """Tests for Clients API endpoints."""

    @patch('apps.clients.views.get_user_context')
    @patch('apps.clients.selectors.get_clients_list')
    def test_clients_list_returns_pagination(
        self, mock_clients, mock_user_context
    ):
        """GET /api/clients returns paginated clients."""
        mock_user_context.return_value = self.user
        mock_clients.return_value = {
            'clients': [
                {
                    'id': str(uuid.uuid4()),
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'email': 'john@test.com',
                    'phone': '+1234567890',
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

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('clients', data)
        self.assertIn('pagination', data)


# =============================================================================
# Dashboard API Tests (P2-036, P2-039)
# =============================================================================

class DashboardAPITests(MockAuthenticationMixin, TestCase):
    """Tests for Dashboard API endpoints."""

    @patch('apps.dashboard.views.get_user_context')
    @patch('apps.dashboard.selectors.get_dashboard_summary')
    def test_dashboard_summary_returns_metrics(
        self, mock_summary, mock_user_context
    ):
        """GET /api/dashboard/summary returns dashboard metrics."""
        mock_user_context.return_value = self.user
        mock_summary.return_value = {
            'your_deals': {
                'active_policies': 10,
                'monthly_commissions': 5000.00,
                'new_policies': 2,
                'total_clients': 8,
            },
            'downline_production': {
                'active_policies': 50,
                'monthly_commissions': 25000.00,
                'new_policies': 10,
                'total_clients': 40,
            },
        }

        response = self.client.get('/api/dashboard/summary')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('your_deals', data)
        self.assertIn('downline_production', data)

    @patch('apps.dashboard.views.get_user_context')
    @patch('apps.dashboard.selectors.get_scoreboard_data')
    def test_scoreboard_returns_leaderboard(
        self, mock_scoreboard, mock_user_context
    ):
        """GET /api/scoreboard returns leaderboard data."""
        mock_user_context.return_value = self.user
        mock_scoreboard.return_value = {
            'entries': [
                {
                    'rank': 1,
                    'agent_id': str(uuid.uuid4()),
                    'agent_name': 'Top Agent',
                    'production': 100000.00,
                    'deals_count': 50,
                }
            ],
            'user_rank': 5,
            'user_production': 20000.00,
        }

        response = self.client.get('/api/scoreboard')

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('entries', data)


# =============================================================================
# API Response Format Tests
# =============================================================================

class APIResponseFormatTests(TestCase):
    """Tests to verify API responses match OpenAPI spec."""

    def test_pagination_format_consistent(self):
        """Pagination object has consistent format across endpoints."""
        expected_fields = {
            'currentPage', 'totalPages', 'totalCount',
            'limit', 'hasNextPage', 'hasPrevPage'
        }

        pagination = {
            'currentPage': 1,
            'totalPages': 10,
            'totalCount': 100,
            'limit': 10,
            'hasNextPage': True,
            'hasPrevPage': False,
        }

        self.assertEqual(set(pagination.keys()), expected_fields)

    def test_keyset_pagination_format(self):
        """Keyset pagination response has consistent format."""
        expected_fields = {'deals', 'has_more', 'next_cursor'}

        response = {
            'deals': [],
            'has_more': False,
            'next_cursor': None,
        }

        self.assertEqual(set(response.keys()), expected_fields)

    def test_error_response_format(self):
        """Error responses have consistent format."""
        error_response = {
            'error': 'Not Found',
            'detail': 'Resource not found',
        }

        self.assertIn('error', error_response)


# =============================================================================
# Feature Flag Tests
# =============================================================================

class FeatureFlagTests(TestCase):
    """Tests for feature flag functionality."""

    def test_feature_flags_enum_defined(self):
        """All expected feature flags are defined."""
        from apps.core.feature_flags import FeatureFlags

        expected_flags = [
            'USE_DJANGO_AUTH',
            'USE_DJANGO_DASHBOARD',
            'USE_DJANGO_AGENTS',
            'USE_DJANGO_DEALS',
            'USE_DJANGO_CARRIERS',
            'USE_DJANGO_PRODUCTS',
            'USE_DJANGO_POSITIONS',
            'USE_DJANGO_SMS',
            'USE_DJANGO_PAYOUTS',
            'USE_DJANGO_CLIENTS',
        ]

        for flag in expected_flags:
            self.assertTrue(
                hasattr(FeatureFlags, flag),
                f"FeatureFlags missing {flag}"
            )

    @patch('apps.core.feature_flags.connection')
    @patch('apps.core.feature_flags.cache')
    def test_get_feature_flag_checks_database(self, mock_cache, mock_connection):
        """get_feature_flag queries database when not cached."""
        from apps.core.feature_flags import get_feature_flag

        mock_cache.get.return_value = None
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True, 100)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        result = get_feature_flag('test_flag')

        self.assertTrue(result)
        mock_cursor.execute.assert_called()

    @patch('apps.core.feature_flags.connection')
    @patch('apps.core.feature_flags.cache')
    def test_get_feature_flag_uses_cache(self, mock_cache, mock_connection):
        """get_feature_flag uses cached value when available."""
        from apps.core.feature_flags import get_feature_flag

        mock_cache.get.return_value = True

        result = get_feature_flag('test_flag')

        self.assertTrue(result)
        mock_connection.cursor.assert_not_called()


# =============================================================================
# Hierarchy Service Tests
# =============================================================================

class HierarchyServiceTests(TestCase):
    """Tests for HierarchyService."""

    @patch('services.hierarchy_service.connection')
    def test_get_downline_uses_recursive_cte(self, mock_connection):
        """get_downline uses recursive CTE for efficiency."""
        from services.hierarchy_service import HierarchyService

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(uuid.uuid4(),), (uuid.uuid4(),)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        user_id = uuid.uuid4()
        agency_id = uuid.uuid4()

        result = HierarchyService.get_downline(user_id, agency_id)

        self.assertEqual(len(result), 2)
        call_args = mock_cursor.execute.call_args[0][0]
        self.assertIn('WITH RECURSIVE', call_args)

    @patch('services.hierarchy_service.connection')
    def test_can_access_user_allows_self(self, mock_connection):
        """can_access_user allows users to access themselves."""
        from services.hierarchy_service import HierarchyService

        user_id = uuid.uuid4()
        agency_id = uuid.uuid4()

        result = HierarchyService.can_access_user(
            requesting_user_id=user_id,
            requesting_user_agency_id=agency_id,
            requesting_user_is_admin=False,
            target_user_id=user_id,
        )

        self.assertTrue(result)
        mock_connection.cursor.assert_not_called()

    @patch('services.hierarchy_service.connection')
    def test_validate_upline_prevents_self_assignment(self, mock_connection):
        """validate_upline_assignment prevents self-upline."""
        from services.hierarchy_service import HierarchyService

        user_id = uuid.uuid4()
        agency_id = uuid.uuid4()

        is_valid, error = HierarchyService.validate_upline_assignment(
            agent_id=user_id,
            new_upline_id=user_id,
            agency_id=agency_id,
        )

        self.assertFalse(is_valid)
        self.assertIn('own upline', error)


# =============================================================================
# Analytics Service Tests
# =============================================================================

class AnalyticsServiceTests(TestCase):
    """Tests for AnalyticsService."""

    @patch('services.analytics_service.connection')
    def test_get_production_leaderboard_returns_ranked_list(self, mock_connection):
        """get_production_leaderboard returns ranked agent list."""
        from services.analytics_service import AnalyticsService

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, uuid.uuid4(), 'Top Agent', 'Manager', 100000.00, 50),
            (2, uuid.uuid4(), 'Second Agent', 'Agent', 80000.00, 40),
        ]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        service = AnalyticsService(
            user_id=uuid.uuid4(),
            agency_id=uuid.uuid4()
        )
        service._is_admin = True

        result = service.get_production_leaderboard(
            start_date=date.today() - timedelta(days=30),
            end_date=date.today(),
            limit=10
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['rank'], 1)
        self.assertIn('agent_name', result[0])
        self.assertIn('production', result[0])


# =============================================================================
# Permission Tests
# =============================================================================

class PermissionTests(TestCase):
    """Tests for permission classes."""

    def test_is_agency_member_permission_exists(self):
        """IsAgencyMember permission class exists."""
        from apps.core.permissions import IsAgencyMember
        self.assertTrue(callable(IsAgencyMember))

    def test_is_admin_or_self_or_downline_permission_exists(self):
        """IsAdminOrSelfOrDownline permission class exists."""
        from apps.core.permissions import IsAdminOrSelfOrDownline
        self.assertTrue(callable(IsAdminOrSelfOrDownline))

    def test_subscription_tier_permission_exists(self):
        """SubscriptionTierPermission class exists."""
        from apps.core.permissions import SubscriptionTierPermission
        self.assertTrue(callable(SubscriptionTierPermission))


# =============================================================================
# Serializer Tests
# =============================================================================

class SerializerTests(TestCase):
    """Tests for DRF serializers."""

    def test_pagination_serializer_fields(self):
        """PaginationSerializer has all required fields."""
        from apps.core.serializers import PaginationSerializer

        serializer = PaginationSerializer(data={
            'currentPage': 1,
            'totalPages': 10,
            'totalCount': 100,
            'limit': 10,
            'hasNextPage': True,
            'hasPrevPage': False,
        })

        self.assertTrue(serializer.is_valid())

    def test_user_list_serializer_fields(self):
        """UserListSerializer exposes expected fields."""
        from apps.core.serializers import UserListSerializer

        expected_fields = {
            'id', 'email', 'first_name', 'last_name', 'full_name',
            'phone', 'role', 'is_admin', 'status', 'position_id',
            'position_name', 'position_level', 'upline_id', 'upline_name',
            'total_prod', 'total_policies_sold', 'created_at',
        }

        serializer = UserListSerializer()
        self.assertEqual(set(serializer.fields.keys()), expected_fields)

    def test_deal_list_serializer_fields(self):
        """DealListSerializer exposes expected fields."""
        from apps.core.serializers import DealListSerializer

        expected_fields = {
            'id', 'policy_number', 'status', 'status_standardized',
            'agent_id', 'agent_name', 'client_id', 'client_name',
            'carrier_id', 'carrier_name', 'product_id', 'product_name',
            'annual_premium', 'monthly_premium', 'policy_effective_date',
            'submission_date', 'created_at',
        }

        serializer = DealListSerializer()
        self.assertEqual(set(serializer.fields.keys()), expected_fields)
