"""
Permission Unit Tests (P1-021)

Tests for permission classes and access control.
"""
import uuid
from unittest.mock import patch, MagicMock

from django.test import TestCase, RequestFactory
from rest_framework.views import APIView

from apps.core.authentication import AuthenticatedUser
from apps.core.permissions import (
    IsAuthenticated, IsActiveUser, IsAdmin, IsAdminOrSelf,
    HasSubscriptionTier, IsSameAgency, IsAgencyMember,
    IsAdminOrSelfOrDownline, SubscriptionTierPermission,
    CanAccessConversation, HasUnlimitedSMS,
    check_hierarchy_access, get_visible_agent_ids,
    get_tier_limits, check_feature_access, TIER_LIMITS,
)


class MockView(APIView):
    """Mock view for testing permissions."""
    pass


class MockViewWithTiers(APIView):
    """Mock view with tier requirements."""
    required_tiers = ['pro', 'expert']


class MockViewWithFeatures(APIView):
    """Mock view with feature requirements."""
    required_features = ['ai_chat_enabled']


class MockViewWithMinTier(APIView):
    """Mock view with minimum tier requirement."""
    required_tier = 'pro'


def create_auth_user(
    user_id=None,
    agency_id=None,
    role='agent',
    is_admin=False,
    status='active',
    subscription_tier='free'
):
    """Helper to create AuthenticatedUser for tests."""
    return AuthenticatedUser(
        id=user_id or uuid.uuid4(),
        auth_user_id=uuid.uuid4(),
        email='test@example.com',
        agency_id=agency_id or uuid.uuid4(),
        role=role,
        is_admin=is_admin,
        status=status,
        perm_level=None,
        subscription_tier=subscription_tier,
    )


class IsAuthenticatedTests(TestCase):
    """Tests for IsAuthenticated permission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsAuthenticated()
        self.view = MockView()

    def test_authenticated_user_allowed(self):
        """Authenticated user passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user()

        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_anonymous_user_denied(self):
        """Anonymous user fails permission check."""
        request = self.factory.get('/')
        request.user = None

        self.assertFalse(self.permission.has_permission(request, self.view))

    def test_non_authenticated_user_type_denied(self):
        """Non-AuthenticatedUser type fails permission check."""
        request = self.factory.get('/')
        request.user = {'id': 'fake'}

        self.assertFalse(self.permission.has_permission(request, self.view))


class IsActiveUserTests(TestCase):
    """Tests for IsActiveUser permission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsActiveUser()
        self.view = MockView()

    def test_active_user_allowed(self):
        """Active user passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(status='active')

        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_inactive_user_denied(self):
        """Inactive user fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(status='inactive')

        self.assertFalse(self.permission.has_permission(request, self.view))

    def test_invited_user_denied(self):
        """Invited user fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(status='invited')

        self.assertFalse(self.permission.has_permission(request, self.view))


class IsAdminTests(TestCase):
    """Tests for IsAdmin permission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsAdmin()
        self.view = MockView()

    def test_admin_user_allowed(self):
        """Admin user passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(is_admin=True)

        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_admin_role_allowed(self):
        """User with admin role passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(role='admin')

        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_regular_user_denied(self):
        """Regular user fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(role='agent', is_admin=False)

        self.assertFalse(self.permission.has_permission(request, self.view))


class IsAgencyMemberTests(TestCase):
    """Tests for IsAgencyMember permission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsAgencyMember()
        self.view = MockView()

    def test_user_with_agency_allowed(self):
        """User with agency ID passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=uuid.uuid4())

        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_user_without_agency_denied(self):
        """User without agency ID fails permission check."""
        request = self.factory.get('/')
        user = create_auth_user()
        user.agency_id = None
        request.user = user

        self.assertFalse(self.permission.has_permission(request, self.view))


class HasSubscriptionTierTests(TestCase):
    """Tests for HasSubscriptionTier permission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = HasSubscriptionTier()

    def test_user_with_required_tier_allowed(self):
        """User with required tier passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='pro')
        view = MockViewWithTiers()

        self.assertTrue(self.permission.has_permission(request, view))

    def test_user_without_required_tier_denied(self):
        """User without required tier fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='free')
        view = MockViewWithTiers()

        self.assertFalse(self.permission.has_permission(request, view))

    def test_view_without_required_tiers_allowed(self):
        """View without tier requirements allows all users."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='free')
        view = MockView()

        self.assertTrue(self.permission.has_permission(request, view))


class SubscriptionTierPermissionTests(TestCase):
    """Tests for SubscriptionTierPermission."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = SubscriptionTierPermission()

    def test_feature_access_allowed(self):
        """User with feature access passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='pro')
        view = MockViewWithFeatures()

        self.assertTrue(self.permission.has_permission(request, view))

    def test_feature_access_denied(self):
        """User without feature access fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='free')
        view = MockViewWithFeatures()

        self.assertFalse(self.permission.has_permission(request, view))

    def test_minimum_tier_allowed(self):
        """User meeting minimum tier passes permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='expert')
        view = MockViewWithMinTier()

        self.assertTrue(self.permission.has_permission(request, view))

    def test_minimum_tier_denied(self):
        """User below minimum tier fails permission check."""
        request = self.factory.get('/')
        request.user = create_auth_user(subscription_tier='free')
        view = MockViewWithMinTier()

        self.assertFalse(self.permission.has_permission(request, view))


class CheckHierarchyAccessTests(TestCase):
    """Tests for check_hierarchy_access function."""

    def test_user_can_access_self(self):
        """User can always access their own data."""
        user_id = uuid.uuid4()
        user = create_auth_user(user_id=user_id)

        result = check_hierarchy_access(user, user_id)

        self.assertTrue(result)

    @patch('apps.core.permissions.connection')
    def test_admin_can_access_agency_member(self, mock_connection):
        """Admin can access any user in their agency."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=True)
        target_id = uuid.uuid4()

        result = check_hierarchy_access(user, target_id)

        self.assertTrue(result)

    @patch('apps.core.permissions.connection')
    def test_agent_can_access_downline(self, mock_connection):
        """Agent can access users in their downline."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=False)
        target_id = uuid.uuid4()

        result = check_hierarchy_access(user, target_id)

        self.assertTrue(result)

    @patch('apps.core.permissions.connection')
    def test_agent_cannot_access_non_downline(self, mock_connection):
        """Agent cannot access users not in their downline."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=False)
        target_id = uuid.uuid4()

        result = check_hierarchy_access(user, target_id)

        self.assertFalse(result)


class GetVisibleAgentIdsTests(TestCase):
    """Tests for get_visible_agent_ids function."""

    @patch('apps.core.permissions.connection')
    def test_admin_with_full_agency_gets_all_agents(self, mock_connection):
        """Admin requesting full agency gets all non-client users."""
        mock_cursor = MagicMock()
        agent_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        mock_cursor.fetchall.return_value = [(aid,) for aid in agent_ids]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=True)

        result = get_visible_agent_ids(user, include_full_agency=True)

        self.assertEqual(len(result), 3)
        # Verify query excludes clients
        call_args = mock_cursor.execute.call_args[0][0]
        self.assertIn("role != 'client'", call_args)

    @patch('apps.core.permissions.connection')
    def test_regular_user_gets_self_and_downline(self, mock_connection):
        """Regular user gets themselves and their downline."""
        mock_cursor = MagicMock()
        agent_ids = [uuid.uuid4(), uuid.uuid4()]
        mock_cursor.fetchall.return_value = [(aid,) for aid in agent_ids]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=False)

        result = get_visible_agent_ids(user, include_full_agency=False)

        self.assertEqual(len(result), 2)
        # Verify recursive CTE is used
        call_args = mock_cursor.execute.call_args[0][0]
        self.assertIn('WITH RECURSIVE', call_args)


class TierLimitsTests(TestCase):
    """Tests for tier limits configuration."""

    def test_free_tier_limits(self):
        """Free tier has expected limits."""
        limits = TIER_LIMITS['free']
        self.assertEqual(limits['max_agents'], 5)
        self.assertEqual(limits['max_deals_per_month'], 50)
        self.assertFalse(limits['ai_chat_enabled'])

    def test_pro_tier_limits(self):
        """Pro tier has expected limits."""
        limits = TIER_LIMITS['pro']
        self.assertEqual(limits['max_agents'], 25)
        self.assertTrue(limits['ai_chat_enabled'])
        self.assertTrue(limits['advanced_analytics'])

    def test_expert_tier_unlimited(self):
        """Expert tier has unlimited resources."""
        limits = TIER_LIMITS['expert']
        self.assertIsNone(limits['max_agents'])
        self.assertIsNone(limits['max_deals_per_month'])
        self.assertTrue(limits['custom_branding'])


class GetTierLimitsTests(TestCase):
    """Tests for get_tier_limits function."""

    def test_get_known_tier(self):
        """Getting limits for known tier returns correct config."""
        limits = get_tier_limits('pro')
        self.assertEqual(limits['max_agents'], 25)

    def test_get_unknown_tier_returns_free(self):
        """Getting limits for unknown tier returns free tier."""
        limits = get_tier_limits('unknown_tier')
        self.assertEqual(limits['max_agents'], 5)  # Free tier limit


class CheckFeatureAccessTests(TestCase):
    """Tests for check_feature_access function."""

    def test_pro_has_ai_chat(self):
        """Pro tier has AI chat access."""
        user = create_auth_user(subscription_tier='pro')
        self.assertTrue(check_feature_access(user, 'ai_chat_enabled'))

    def test_free_no_ai_chat(self):
        """Free tier does not have AI chat access."""
        user = create_auth_user(subscription_tier='free')
        self.assertFalse(check_feature_access(user, 'ai_chat_enabled'))

    def test_expert_has_custom_branding(self):
        """Expert tier has custom branding."""
        user = create_auth_user(subscription_tier='expert')
        self.assertTrue(check_feature_access(user, 'custom_branding'))
