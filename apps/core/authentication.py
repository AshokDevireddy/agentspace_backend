"""
Supabase JWT Authentication for Django REST Framework

Validates JWTs issued by Supabase Auth and attaches user context to requests.
"""
import hmac
import logging
from dataclasses import dataclass
from uuid import UUID

import jwt
from django.conf import settings
from django.db import connection
from rest_framework import authentication, exceptions

logger = logging.getLogger(__name__)


@dataclass
class AuthenticatedUser:
    """
    Represents an authenticated user from Supabase.

    This is NOT a Django User model - it's a lightweight container
    for user context derived from the JWT and users table.
    """
    id: UUID                      # users.id (public.users primary key)
    auth_user_id: UUID            # auth.users.id (Supabase auth user ID)
    email: str
    agency_id: UUID
    role: str                     # 'admin', 'agent', 'client'
    is_admin: bool
    status: str                   # 'pre-invite', 'invited', 'onboarding', 'active', 'inactive'
    perm_level: str | None     # Permission level within agency
    subscription_tier: str | None  # 'free', 'pro', 'expert'
    first_name: str | None = None  # First name from users table
    last_name: str | None = None   # Last name from users table

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def is_active(self) -> bool:
        return self.status == 'active'

    @property
    def is_administrator(self) -> bool:
        """Check if user has administrator privileges."""
        return self.is_admin or self.role == 'admin'


class SupabaseJWTAuthentication(authentication.BaseAuthentication):
    """
    Authenticates requests using Supabase JWTs.

    Flow:
    1. Extract Bearer token from Authorization header
    2. Decode and validate JWT using Supabase JWT secret
    3. Look up user in public.users by auth_user_id (sub claim)
    4. Return AuthenticatedUser with full context
    """

    def authenticate(self, request):
        """
        Authenticate the request and return (user, token) or None.

        Returns:
            tuple: (AuthenticatedUser, token) if authenticated
            None: If no authentication credentials provided

        Raises:
            AuthenticationFailed: If credentials are invalid
        """
        auth_header = request.META.get('HTTP_AUTHORIZATION', '')

        if not auth_header:
            return None

        if not auth_header.startswith('Bearer '):
            return None

        token = auth_header[7:]  # Remove 'Bearer ' prefix

        if not token:
            return None

        # Decode and validate JWT
        payload = self._decode_jwt(token)
        if not payload:
            raise exceptions.AuthenticationFailed('Invalid or expired token')

        # Get user from database
        user = self._get_user_from_payload(payload)
        if not user:
            raise exceptions.AuthenticationFailed('User not found')

        return (user, token)

    def authenticate_header(self, request):
        """
        Return the WWW-Authenticate header value for 401 responses.
        """
        return 'Bearer realm="api"'

    def _decode_jwt(self, token: str) -> dict | None:
        """
        Decode and validate a Supabase JWT.

        Args:
            token: The JWT string

        Returns:
            dict: The decoded payload if valid
            None: If token is invalid or expired
        """
        jwt_secret = getattr(settings, 'AUTH_JWT_SECRET', None)

        if not jwt_secret:
            logger.error('AUTH_JWT_SECRET not configured')
            return None

        try:
            # Build the expected issuer URL from Supabase URL
            supabase_url = getattr(settings, 'SUPABASE_URL', '')
            expected_issuer = f'{supabase_url}/auth/v1' if supabase_url else None

            # Supabase JWTs use HS256 algorithm
            decode_options = {
                'verify_exp': True,
                'verify_aud': True,
                'verify_iss': bool(expected_issuer),
            }

            decode_kwargs = {
                'jwt': token,
                'key': jwt_secret,
                'algorithms': ['HS256'],
                'audience': 'authenticated',
                'options': decode_options,
            }

            # Add issuer validation if we have the Supabase URL configured
            if expected_issuer:
                decode_kwargs['issuer'] = expected_issuer

            payload = jwt.decode(**decode_kwargs)
            return payload
        except jwt.ExpiredSignatureError:
            logger.debug('JWT has expired')
            return None
        except jwt.InvalidAudienceError:
            logger.debug('JWT has invalid audience')
            return None
        except jwt.InvalidTokenError as e:
            logger.debug(f'JWT validation failed: {e}')
            return None

    def _get_user_from_payload(self, payload: dict) -> AuthenticatedUser | None:
        """
        Look up user in public.users by auth_user_id from JWT sub claim.

        Args:
            payload: Decoded JWT payload

        Returns:
            AuthenticatedUser if found, None otherwise
        """
        auth_user_id = payload.get('sub')
        if not auth_user_id:
            logger.warning('JWT missing sub claim')
            return None

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id,
                        auth_user_id,
                        email,
                        agency_id,
                        role,
                        is_admin,
                        status,
                        perm_level,
                        subscription_tier,
                        first_name,
                        last_name
                    FROM public.users
                    WHERE auth_user_id = %s
                    LIMIT 1
                """, [auth_user_id])

                row = cursor.fetchone()

                if not row:
                    logger.warning(f'No user found for auth_user_id: {auth_user_id}')
                    return None

                return AuthenticatedUser(
                    id=row[0],
                    auth_user_id=row[1],
                    email=row[2] or '',
                    agency_id=row[3],
                    role=row[4] or 'agent',
                    is_admin=row[5] or False,
                    status=row[6] or 'active',
                    perm_level=row[7],
                    subscription_tier=row[8],
                    first_name=row[9],
                    last_name=row[10],
                )
        except Exception as e:
            logger.error(f'Database error looking up user: {e}')
            return None


def get_user_context(request) -> AuthenticatedUser | None:
    """
    Utility function to get authenticated user from request.

    Use this in views that need user context.

    Args:
        request: Django request object

    Returns:
        AuthenticatedUser if authenticated, None otherwise
    """
    user = getattr(request, 'user', None)
    if isinstance(user, AuthenticatedUser):
        return user
    return None


class CronSecretAuthentication(authentication.BaseAuthentication):
    """
    Authenticates requests using a shared CRON_SECRET.

    Used by cron jobs and internal services that don't have user context.
    Returns a system-level AuthenticatedUser with admin privileges.

    The secret is passed via the X-Cron-Secret header.
    """

    def authenticate(self, request):
        """
        Authenticate the request using the CRON_SECRET header.

        Returns:
            tuple: (AuthenticatedUser, None) if authenticated
            None: If no cron secret header provided

        Raises:
            AuthenticationFailed: If secret is invalid
        """
        cron_secret = request.META.get('HTTP_X_CRON_SECRET', '')

        if not cron_secret:
            return None

        expected_secret = getattr(settings, 'CRON_SECRET', None)

        if not expected_secret:
            logger.error('CRON_SECRET not configured in settings')
            return None

        # Use constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(cron_secret, expected_secret):
            raise exceptions.AuthenticationFailed('Invalid cron secret')

        # Return a system-level user for cron jobs
        # This user has admin privileges but isn't tied to a real user
        system_user = AuthenticatedUser(
            id=UUID('00000000-0000-0000-0000-000000000000'),  # System user ID
            auth_user_id=UUID('00000000-0000-0000-0000-000000000000'),
            email='system@internal',
            agency_id=UUID('00000000-0000-0000-0000-000000000000'),  # Will be overridden per-agency
            role='admin',
            is_admin=True,
            status='active',
            perm_level='admin',
            subscription_tier='expert',
            first_name='System',
            last_name='Cron',
        )

        return (system_user, None)
