"""
Permission Classes for AgentSpace Backend

Provides multi-tenancy and role-based access control.
"""
import logging
from typing import Optional
from uuid import UUID

from rest_framework import permissions

from .authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


# =============================================================================
# Subscription Tier Configuration
# =============================================================================

TIER_LIMITS = {
    'free': {
        'max_agents': 5,
        'max_deals_per_month': 50,
        'max_sms_per_month': 100,
        'ai_chat_enabled': False,
        'advanced_analytics': False,
        'custom_branding': False,
    },
    'pro': {
        'max_agents': 25,
        'max_deals_per_month': 500,
        'max_sms_per_month': 1000,
        'ai_chat_enabled': True,
        'advanced_analytics': True,
        'custom_branding': False,
    },
    'expert': {
        'max_agents': None,  # Unlimited
        'max_deals_per_month': None,  # Unlimited
        'max_sms_per_month': None,  # Unlimited
        'ai_chat_enabled': True,
        'advanced_analytics': True,
        'custom_branding': True,
    },
}


class IsAuthenticated(permissions.BasePermission):
    """
    Allows access only to authenticated users with valid status.
    """
    message = 'Authentication required'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        # Allow any authenticated user (status checking is separate)
        return True


class IsActiveUser(permissions.BasePermission):
    """
    Allows access only to users with 'active' status.
    """
    message = 'Account is not active'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        return user.status == 'active'


class IsAdmin(permissions.BasePermission):
    """
    Allows access only to admin users.
    """
    message = 'Admin access required'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        return user.is_admin or user.role == 'admin'


class IsAdminOrSelf(permissions.BasePermission):
    """
    Allows admins full access, or users to access their own data.

    Use with views that have a user_id URL parameter or request body field.
    """
    message = 'You can only access your own data'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        return True

    def has_object_permission(self, request, view, obj):
        user = request.user
        if user.is_admin or user.role == 'admin':
            return True

        # Check if object belongs to user
        obj_user_id = getattr(obj, 'user_id', None) or getattr(obj, 'id', None)
        return str(obj_user_id) == str(user.id)


class HasSubscriptionTier(permissions.BasePermission):
    """
    Allows access only to users with specific subscription tiers.

    Configure required tiers on the view:
        class MyView(APIView):
            permission_classes = [HasSubscriptionTier]
            required_tiers = ['pro', 'expert']
    """
    message = 'Subscription upgrade required'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False

        required_tiers = getattr(view, 'required_tiers', [])
        if not required_tiers:
            return True

        user_tier = user.subscription_tier or 'free'
        return user_tier in required_tiers


class IsSameAgency(permissions.BasePermission):
    """
    Ensures the requested resource belongs to the user's agency.

    SECURITY: This is a critical permission for multi-tenancy.
    It prevents cross-tenant data access.
    """
    message = 'Access denied - resource belongs to different agency'

    def has_object_permission(self, request, view, obj):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False

        obj_agency_id = getattr(obj, 'agency_id', None)
        if obj_agency_id is None:
            # Object doesn't have agency_id - allow (not agency-scoped)
            return True

        return str(obj_agency_id) == str(user.agency_id)


def check_hierarchy_access(user: AuthenticatedUser, target_agent_id: UUID) -> bool:
    """
    Check if user has access to view/modify a target agent.

    Admins can access anyone in their agency.
    Agents can access themselves and their downlines.

    Args:
        user: The authenticated user
        target_agent_id: The agent to check access for

    Returns:
        bool: True if access is allowed
    """
    from django.db import connection

    # User can always access themselves
    if str(user.id) == str(target_agent_id):
        return True

    # Admins can access anyone in their agency
    if user.is_admin or user.role == 'admin':
        # Verify target is in same agency
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM public.users
                WHERE id = %s AND agency_id = %s
                LIMIT 1
            """, [str(target_agent_id), str(user.agency_id)])
            return cursor.fetchone() is not None

    # Check if target is in user's downline using recursive CTE
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE downline AS (
                SELECT id FROM public.users WHERE id = %s
                UNION ALL
                SELECT u.id FROM public.users u
                JOIN downline d ON u.upline_id = d.id
            )
            SELECT 1 FROM downline WHERE id = %s LIMIT 1
        """, [str(user.id), str(target_agent_id)])
        return cursor.fetchone() is not None


def get_visible_agent_ids(user: AuthenticatedUser, include_full_agency: bool = False) -> list[UUID]:
    """
    Get list of agent IDs visible to a user.

    Args:
        user: The authenticated user
        include_full_agency: If True, return all agents in agency (admin only)

    Returns:
        List of agent UUIDs the user can access
    """
    from django.db import connection

    if include_full_agency and (user.is_admin or user.role == 'admin'):
        # Admin sees all agents in agency
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM public.users
                WHERE agency_id = %s AND role != 'client'
            """, [str(user.agency_id)])
            return [row[0] for row in cursor.fetchall()]

    # Regular user sees themselves and their downline
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE downline AS (
                SELECT id FROM public.users WHERE id = %s
                UNION ALL
                SELECT u.id FROM public.users u
                JOIN downline d ON u.upline_id = d.id
                WHERE u.agency_id = %s
            )
            SELECT id FROM downline
        """, [str(user.id), str(user.agency_id)])
        return [row[0] for row in cursor.fetchall()]


# =============================================================================
# Additional Permission Classes (P1-017, P1-018, P1-019)
# =============================================================================

class IsAgencyMember(permissions.BasePermission):
    """
    Ensures user belongs to an agency (P1-017).

    SECURITY: Critical for multi-tenancy - prevents access without agency context.
    """
    message = 'Agency membership required'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        return user.agency_id is not None


class IsAdminOrSelfOrDownline(permissions.BasePermission):
    """
    Allows access based on hierarchy (P1-018).

    - Admins can access any user in their agency
    - Users can access themselves
    - Users can access their downlines

    Use with views that have a target user (via URL param, query param, or object).
    """
    message = 'Access denied - not in your hierarchy'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False
        return True

    def has_object_permission(self, request, view, obj):
        user = request.user

        # Get target user ID from object
        target_user_id = None
        if hasattr(obj, 'agent_id'):
            target_user_id = obj.agent_id
        elif hasattr(obj, 'user_id'):
            target_user_id = obj.user_id
        elif hasattr(obj, 'id') and hasattr(obj, 'email'):  # Looks like a User
            target_user_id = obj.id

        if target_user_id is None:
            return True  # No user context on object, allow

        return check_hierarchy_access(user, target_user_id)


class SubscriptionTierPermission(permissions.BasePermission):
    """
    Feature gating based on subscription tier (P1-019).

    Configure required features on the view:
        class MyView(APIView):
            permission_classes = [SubscriptionTierPermission]
            required_features = ['ai_chat_enabled', 'advanced_analytics']
            # OR
            required_tier = 'pro'  # Minimum tier required
    """
    message = 'Subscription upgrade required for this feature'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False

        user_tier = user.subscription_tier or 'free'
        tier_config = TIER_LIMITS.get(user_tier, TIER_LIMITS['free'])

        # Check required features
        required_features = getattr(view, 'required_features', [])
        for feature in required_features:
            if not tier_config.get(feature, False):
                self.message = f'Upgrade to access {feature.replace("_", " ")}'
                return False

        # Check minimum tier
        required_tier = getattr(view, 'required_tier', None)
        if required_tier:
            tier_order = ['free', 'pro', 'expert']
            user_tier_index = tier_order.index(user_tier) if user_tier in tier_order else 0
            required_tier_index = tier_order.index(required_tier) if required_tier in tier_order else 0
            if user_tier_index < required_tier_index:
                self.message = f'Upgrade to {required_tier} tier to access this feature'
                return False

        return True


class CanAccessConversation(permissions.BasePermission):
    """
    Check if user can access an SMS conversation.

    Rules:
    - User owns the conversation (is the agent)
    - User is in the upline of the conversation's agent
    - User is admin
    """
    message = 'Access denied to this conversation'

    def has_object_permission(self, request, view, obj):
        user = request.user
        if not isinstance(user, AuthenticatedUser):
            return False

        # Admin can access any conversation in their agency
        if user.is_admin or user.role == 'admin':
            return str(obj.agency_id) == str(user.agency_id)

        # Check if user is the conversation's agent
        if obj.agent_id and str(obj.agent_id) == str(user.id):
            return True

        # Check if conversation's agent is in user's downline
        if obj.agent_id:
            return check_hierarchy_access(user, obj.agent_id)

        return False


class HasUnlimitedSMS(permissions.BasePermission):
    """
    Check if user has unlimited SMS based on subscription tier.
    """
    message = 'SMS limit reached. Upgrade for unlimited messaging.'

    def has_permission(self, request, view):
        user = getattr(request, 'user', None)
        if not isinstance(user, AuthenticatedUser):
            return False

        user_tier = user.subscription_tier or 'free'
        tier_config = TIER_LIMITS.get(user_tier, TIER_LIMITS['free'])

        # Expert tier has unlimited
        if tier_config.get('max_sms_per_month') is None:
            return True

        # For limited tiers, would need to check usage count
        # This is a simplified check - real implementation would query usage
        return True  # Allow for now, actual limit check in service layer


def get_tier_limits(tier: str) -> dict:
    """
    Get the limits for a subscription tier.

    Args:
        tier: The subscription tier name

    Returns:
        Dictionary of tier limits
    """
    return TIER_LIMITS.get(tier, TIER_LIMITS['free']).copy()


def check_feature_access(user: AuthenticatedUser, feature: str) -> bool:
    """
    Check if a user has access to a specific feature.

    Args:
        user: The authenticated user
        feature: The feature name to check

    Returns:
        True if user has access to the feature
    """
    user_tier = user.subscription_tier or 'free'
    tier_config = TIER_LIMITS.get(user_tier, TIER_LIMITS['free'])
    return tier_config.get(feature, False)
