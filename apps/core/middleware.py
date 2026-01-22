"""
Authentication Middleware for AgentSpace Backend

Handles JWT authentication and attaches user context to requests.
"""
import logging
import re
from typing import Callable, List

from django.http import JsonResponse

from .authentication import SupabaseJWTAuthentication

logger = logging.getLogger(__name__)


class SupabaseAuthMiddleware:
    """
    Middleware that authenticates requests using Supabase JWTs.

    This middleware:
    1. Skips authentication for public routes
    2. Validates JWT for protected routes
    3. Attaches AuthenticatedUser to request.user
    4. Returns 401 for unauthenticated requests to protected routes
    """

    # Routes that don't require authentication
    PUBLIC_ROUTES: List[str] = [
        r'^/api/health$',
        r'^/api/auth/login$',
        r'^/api/auth/register$',
        r'^/api/auth/verify-invite$',
        r'^/api/auth/forgot-password$',
        r'^/api/auth/reset-password$',
        r'^/api/cron/',
        r'^/api/webhooks/',
    ]

    def __init__(self, get_response: Callable):
        self.get_response = get_response
        self.authenticator = SupabaseJWTAuthentication()
        # Compile regex patterns for efficiency
        self._public_patterns = [re.compile(pattern) for pattern in self.PUBLIC_ROUTES]

    def __call__(self, request):
        # Check if route is public
        if self._is_public_route(request.path):
            request.user = None
            return self.get_response(request)

        # Attempt authentication
        try:
            auth_result = self.authenticator.authenticate(request)

            if auth_result is None:
                # No credentials provided
                return JsonResponse(
                    {
                        'error': 'Unauthorized',
                        'message': 'Authentication required'
                    },
                    status=401
                )

            user, token = auth_result
            request.user = user
            request.auth_token = token

            # Log authentication for audit
            logger.debug(
                f'Authenticated user {user.id} (agency: {user.agency_id}) '
                f'accessing {request.path}'
            )

        except Exception as e:
            logger.warning(f'Authentication failed: {e}')
            return JsonResponse(
                {
                    'error': 'Unauthorized',
                    'message': str(e)
                },
                status=401
            )

        return self.get_response(request)

    def _is_public_route(self, path: str) -> bool:
        """Check if the given path matches any public route pattern."""
        for pattern in self._public_patterns:
            if pattern.match(path):
                return True
        return False


class AgencyContextMiddleware:
    """
    Middleware that ensures agency context is properly set.

    SECURITY: Agency ID is ALWAYS derived from the authenticated user,
    NEVER from request headers or body. This prevents cross-tenant attacks.
    """

    def __init__(self, get_response: Callable):
        self.get_response = get_response

    def __call__(self, request):
        user = getattr(request, 'user', None)

        if user and hasattr(user, 'agency_id'):
            # Set agency context from authenticated user
            request.agency_id = user.agency_id
        else:
            request.agency_id = None

        return self.get_response(request)


class RateLimitMiddleware:
    """
    Simple rate limiting middleware using sliding window.

    For production, consider using Redis-based rate limiting
    or a dedicated service like Cloudflare Rate Limiting.
    """

    def __init__(self, get_response: Callable):
        self.get_response = get_response
        # In-memory store - use Redis in production
        self._requests: dict = {}

    def __call__(self, request):
        # Skip rate limiting for now - implement with Redis in production
        return self.get_response(request)
