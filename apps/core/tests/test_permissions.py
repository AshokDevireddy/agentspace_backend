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

    def test_basic_tier_limits(self):
        """Basic tier has expected limits (between free and pro)."""
        limits = TIER_LIMITS['basic']
        self.assertEqual(limits['max_agents'], 10)
        self.assertEqual(limits['max_deals_per_month'], 100)
        self.assertEqual(limits['max_sms_per_month'], 50)
        self.assertFalse(limits['ai_chat_enabled'])
        self.assertFalse(limits['advanced_analytics'])
        self.assertFalse(limits['custom_branding'])

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

    def test_tier_order_is_correct(self):
        """Tier order is free < basic < pro < expert."""
        tier_order = ['free', 'basic', 'pro', 'expert']
        for tier in tier_order:
            self.assertIn(tier, TIER_LIMITS)


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

    def test_basic_no_ai_chat(self):
        """Basic tier does not have AI chat access."""
        user = create_auth_user(subscription_tier='basic')
        self.assertFalse(check_feature_access(user, 'ai_chat_enabled'))

    def test_basic_no_advanced_analytics(self):
        """Basic tier does not have advanced analytics."""
        user = create_auth_user(subscription_tier='basic')
        self.assertFalse(check_feature_access(user, 'advanced_analytics'))


# =============================================================================
# IsAdminOrSelfOrDownline Tests (P1-018)
# =============================================================================

class IsAdminOrSelfOrDownlineTests(TestCase):
    """Tests for IsAdminOrSelfOrDownline permission (P1-018)."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsAdminOrSelfOrDownline()
        self.view = MockView()

    def test_has_permission_allows_authenticated_user(self):
        """has_permission allows any authenticated user."""
        request = self.factory.get('/')
        request.user = create_auth_user()
        self.assertTrue(self.permission.has_permission(request, self.view))

    def test_has_permission_denies_unauthenticated(self):
        """has_permission denies unauthenticated users."""
        request = self.factory.get('/')
        request.user = None
        self.assertFalse(self.permission.has_permission(request, self.view))

    def test_has_object_permission_allows_object_without_user_context(self):
        """has_object_permission allows objects without user reference."""
        request = self.factory.get('/')
        request.user = create_auth_user()

        class MockObjectNoUser:
            pass

        obj = MockObjectNoUser()
        self.assertTrue(self.permission.has_object_permission(request, self.view, obj))

    @patch('apps.core.permissions.check_hierarchy_access')
    def test_has_object_permission_checks_hierarchy_for_agent_id(self, mock_check):
        """has_object_permission uses agent_id attribute."""
        mock_check.return_value = True
        request = self.factory.get('/')
        request.user = create_auth_user()

        class MockObjectWithAgentId:
            agent_id = uuid.uuid4()

        obj = MockObjectWithAgentId()
        result = self.permission.has_object_permission(request, self.view, obj)

        self.assertTrue(result)
        mock_check.assert_called_once()

    @patch('apps.core.permissions.check_hierarchy_access')
    def test_has_object_permission_checks_hierarchy_for_user_id(self, mock_check):
        """has_object_permission uses user_id attribute."""
        mock_check.return_value = True
        request = self.factory.get('/')
        request.user = create_auth_user()

        class MockObjectWithUserId:
            user_id = uuid.uuid4()

        obj = MockObjectWithUserId()
        result = self.permission.has_object_permission(request, self.view, obj)

        self.assertTrue(result)
        mock_check.assert_called_once()

    @patch('apps.core.permissions.check_hierarchy_access')
    def test_has_object_permission_denies_when_not_in_hierarchy(self, mock_check):
        """has_object_permission denies when target not in hierarchy."""
        mock_check.return_value = False
        request = self.factory.get('/')
        request.user = create_auth_user()

        class MockObjectWithAgentId:
            agent_id = uuid.uuid4()

        obj = MockObjectWithAgentId()
        result = self.permission.has_object_permission(request, self.view, obj)

        self.assertFalse(result)

    def test_has_object_permission_for_user_object(self):
        """has_object_permission identifies User-like objects by email attribute."""
        request = self.factory.get('/')
        user = create_auth_user()
        request.user = user

        class MockUserObject:
            def __init__(self, user_id):
                self.id = user_id
                self.email = 'test@example.com'

        # When obj.id matches user.id, should allow
        obj = MockUserObject(user.id)

        with patch('apps.core.permissions.check_hierarchy_access') as mock_check:
            mock_check.return_value = True
            result = self.permission.has_object_permission(request, self.view, obj)
            self.assertTrue(result)


# =============================================================================
# CanAccessConversation Tests (P1-021)
# =============================================================================

class CanAccessConversationTests(TestCase):
    """Tests for CanAccessConversation permission (P1-021)."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = CanAccessConversation()
        self.view = MockView()
        self.agency_id = uuid.uuid4()

    def test_denies_non_authenticated_user(self):
        """Denies access for non-authenticated users."""
        request = self.factory.get('/')
        request.user = {'not': 'authenticated'}

        class MockConversation:
            agency_id = uuid.uuid4()
            agent_id = uuid.uuid4()

        obj = MockConversation()
        self.assertFalse(self.permission.has_object_permission(request, self.view, obj))

    def test_admin_can_access_same_agency_conversation(self):
        """Admin can access any conversation in their agency."""
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=self.agency_id, is_admin=True)

        class MockConversation:
            agency_id = None
            agent_id = uuid.uuid4()

        obj = MockConversation()
        obj.agency_id = self.agency_id
        self.assertTrue(self.permission.has_object_permission(request, self.view, obj))

    def test_admin_cannot_access_different_agency_conversation(self):
        """Admin cannot access conversation from different agency."""
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=self.agency_id, is_admin=True)

        class MockConversation:
            agency_id = uuid.uuid4()  # Different agency
            agent_id = uuid.uuid4()

        obj = MockConversation()
        self.assertFalse(self.permission.has_object_permission(request, self.view, obj))

    def test_user_can_access_own_conversation(self):
        """User can access their own conversation."""
        user_id = uuid.uuid4()
        request = self.factory.get('/')
        request.user = create_auth_user(user_id=user_id, agency_id=self.agency_id)

        class MockConversation:
            agency_id = None
            agent_id = None

        obj = MockConversation()
        obj.agency_id = self.agency_id
        obj.agent_id = user_id
        self.assertTrue(self.permission.has_object_permission(request, self.view, obj))

    @patch('apps.core.permissions.check_hierarchy_access')
    def test_user_can_access_downline_conversation(self, mock_check):
        """User can access conversation of agent in their downline."""
        mock_check.return_value = True
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=self.agency_id)

        class MockConversation:
            agency_id = None
            agent_id = uuid.uuid4()  # Different agent, but in downline

        obj = MockConversation()
        obj.agency_id = self.agency_id
        result = self.permission.has_object_permission(request, self.view, obj)

        self.assertTrue(result)
        mock_check.assert_called_once()

    @patch('apps.core.permissions.check_hierarchy_access')
    def test_user_cannot_access_non_downline_conversation(self, mock_check):
        """User cannot access conversation of agent not in their downline."""
        mock_check.return_value = False
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=self.agency_id)

        class MockConversation:
            agency_id = None
            agent_id = uuid.uuid4()

        obj = MockConversation()
        obj.agency_id = self.agency_id
        result = self.permission.has_object_permission(request, self.view, obj)

        self.assertFalse(result)

    def test_denies_when_conversation_has_no_agent(self):
        """Denies access when conversation has no agent (for non-admin)."""
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=self.agency_id)

        class MockConversation:
            agency_id = None
            agent_id = None

        obj = MockConversation()
        obj.agency_id = self.agency_id
        self.assertFalse(self.permission.has_object_permission(request, self.view, obj))


# =============================================================================
# Cross-Agency Denial Tests (P1-021)
# =============================================================================

class IsSameAgencyTests(TestCase):
    """Tests for IsSameAgency permission - cross-agency denial (P1-021)."""

    def setUp(self):
        self.factory = RequestFactory()
        self.permission = IsSameAgency()
        self.view = MockView()

    def test_allows_object_without_agency_id(self):
        """Allows access to objects without agency_id."""
        request = self.factory.get('/')
        request.user = create_auth_user()

        class MockObjectNoAgency:
            pass

        obj = MockObjectNoAgency()
        self.assertTrue(self.permission.has_object_permission(request, self.view, obj))

    def test_allows_same_agency(self):
        """Allows access when user and object are same agency."""
        agency_id = uuid.uuid4()
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=agency_id)

        class MockObject:
            pass

        obj = MockObject()
        obj.agency_id = agency_id
        self.assertTrue(self.permission.has_object_permission(request, self.view, obj))

    def test_denies_different_agency(self):
        """Denies access when user and object are different agencies."""
        request = self.factory.get('/')
        request.user = create_auth_user(agency_id=uuid.uuid4())

        class MockObject:
            agency_id = uuid.uuid4()  # Different agency

        obj = MockObject()
        self.assertFalse(self.permission.has_object_permission(request, self.view, obj))

    def test_denies_unauthenticated(self):
        """Denies access for unauthenticated users."""
        request = self.factory.get('/')
        request.user = {'not': 'authenticated'}

        class MockObject:
            agency_id = uuid.uuid4()

        obj = MockObject()
        self.assertFalse(self.permission.has_object_permission(request, self.view, obj))


# =============================================================================
# Edge Case Tests (P1-018)
# =============================================================================

class HierarchyEdgeCaseTests(TestCase):
    """Tests for hierarchy edge cases - no upline, no position (P1-018)."""

    def test_check_hierarchy_access_user_can_always_access_self(self):
        """User can always access their own data regardless of hierarchy."""
        user_id = uuid.uuid4()
        user = create_auth_user(user_id=user_id)

        # User accessing self should always return True without DB query
        result = check_hierarchy_access(user, user_id)
        self.assertTrue(result)

    @patch('apps.core.permissions.connection')
    def test_admin_role_grants_agency_access(self, mock_connection):
        """User with role='admin' has admin access even without is_admin flag."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, role='admin', is_admin=False)
        target_id = uuid.uuid4()

        result = check_hierarchy_access(user, target_id)
        self.assertTrue(result)

    @patch('apps.core.permissions.connection')
    def test_admin_cannot_access_different_agency_member(self, mock_connection):
        """Admin cannot access users from different agency."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Target not in same agency
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        agency_id = uuid.uuid4()
        user = create_auth_user(agency_id=agency_id, is_admin=True)
        target_id = uuid.uuid4()

        result = check_hierarchy_access(user, target_id)
        self.assertFalse(result)
