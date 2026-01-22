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
