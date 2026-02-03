"""
Authentication API Views

Implements auth endpoints that work with Supabase Auth.
"""
import logging

import httpx
from django.conf import settings
from django.db import connection
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from apps.core.throttles import AuthRateThrottle
from apps.onboarding.services import create_onboarding_progress

logger = logging.getLogger(__name__)


class LoginView(APIView):
    """
    POST /api/auth/login

    Authenticate user with email/password via Supabase.

    Request Body:
        {
            "email": "user@example.com",
            "password": "password123"
        }

    Response (200):
        {
            "access_token": "...",
            "refresh_token": "...",
            "user": {
                "id": "uuid",
                "email": "user@example.com",
                "agency_id": "uuid",
                "role": "admin",
                "status": "active"
            }
        }

    Errors:
        400: Invalid request body
        401: Invalid credentials
        403: Account inactive/invited
    """
    permission_classes = [AllowAny]
    # SECURITY FIX: Rate limit login attempts to prevent brute-force attacks
    throttle_classes = [AuthRateThrottle]

    def post(self, request):
        email = request.data.get('email', '').strip().lower()
        password = request.data.get('password', '')

        if not email or not password:
            return Response(
                {'error': 'ValidationError', 'message': 'Email and password required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Authenticate via Supabase
        try:
            supabase_url = settings.SUPABASE_URL
            supabase_anon_key = settings.SUPABASE_ANON_KEY

            with httpx.Client() as client:
                response = client.post(
                    f'{supabase_url}/auth/v1/token?grant_type=password',
                    json={'email': email, 'password': password},
                    headers={
                        'apikey': supabase_anon_key,
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                if response.status_code != 200:
                    error_data = response.json() if response.content else {}
                    error_msg = error_data.get('error_description', 'Invalid credentials')
                    return Response(
                        {'error': 'AuthenticationError', 'message': error_msg},
                        status=status.HTTP_401_UNAUTHORIZED
                    )

                auth_data = response.json()

        except httpx.RequestError as e:
            logger.error(f'Supabase auth request failed: {e}')
            return Response(
                {'error': 'ServiceError', 'message': 'Authentication service unavailable'},
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )

        # Get user from database
        auth_user_id = auth_data.get('user', {}).get('id')
        if not auth_user_id:
            return Response(
                {'error': 'AuthenticationError', 'message': 'Invalid auth response'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        user_data = self._get_user_data(auth_user_id)
        if not user_data:
            return Response(
                {'error': 'NotFound', 'message': 'User not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Check user status
        if user_data['status'] == 'inactive':
            return Response(
                {'error': 'Forbidden', 'message': 'Account is deactivated'},
                status=status.HTTP_403_FORBIDDEN
            )

        if user_data['status'] == 'invited':
            return Response(
                {'error': 'Forbidden', 'message': 'Please check your email for invite link'},
                status=status.HTTP_403_FORBIDDEN
            )

        return Response({
            'access_token': auth_data.get('access_token'),
            'refresh_token': auth_data.get('refresh_token'),
            'expires_in': auth_data.get('expires_in'),
            'token_type': 'bearer',
            'user': user_data
        })

    def _get_user_data(self, auth_user_id: str) -> dict | None:
        """Fetch user data from database."""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, email, first_name, last_name,
                        agency_id, role, is_admin, status,
                        subscription_tier
                    FROM public.users
                    WHERE auth_user_id = %s
                    LIMIT 1
                """, [auth_user_id])
                row = cursor.fetchone()

                if not row:
                    return None

                return {
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'agency_id': str(row[4]) if row[4] else None,
                    'role': row[5],
                    'is_admin': row[6],
                    'status': row[7],
                    'subscription_tier': row[8],
                }
        except Exception as e:
            logger.error(f'Error fetching user data: {e}')
            return None


class LogoutView(APIView):
    """
    POST /api/auth/logout

    Sign out user from Supabase.

    Headers:
        Authorization: Bearer <access_token>

    Response (200):
        {"message": "Logged out successfully"}
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            token = getattr(request, 'auth_token', None)
            if token:
                supabase_url = settings.SUPABASE_URL
                supabase_anon_key = settings.SUPABASE_ANON_KEY

                with httpx.Client() as client:
                    # Call Supabase logout endpoint
                    client.post(
                        f'{supabase_url}/auth/v1/logout',
                        headers={
                            'apikey': supabase_anon_key,
                            'Authorization': f'Bearer {token}',
                        },
                        timeout=5.0
                    )
        except Exception as e:
            logger.warning(f'Supabase logout request failed: {e}')
            # Continue even if Supabase logout fails

        return Response({'message': 'Logged out successfully'})


class RegisterView(APIView):
    """
    POST /api/auth/register

    Register a new admin user and agency.

    Request Body:
        {
            "email": "admin@agency.com",
            "password": "password123",
            "first_name": "John",
            "last_name": "Doe",
            "agency_name": "My Insurance Agency"
        }

    Response (201):
        {
            "message": "Registration successful",
            "user_id": "uuid"
        }
    """
    permission_classes = [AllowAny]
    # SECURITY FIX: Rate limit registration to prevent abuse
    throttle_classes = [AuthRateThrottle]

    def post(self, request):
        email = request.data.get('email', '').strip().lower()
        password = request.data.get('password', '')
        first_name = request.data.get('first_name', '').strip()
        last_name = request.data.get('last_name', '').strip()
        agency_name = request.data.get('agency_name', '').strip()

        # Validate required fields
        errors = []
        if not email:
            errors.append('Email is required')
        if not password or len(password) < 8:
            errors.append('Password must be at least 8 characters')
        if not first_name:
            errors.append('First name is required')
        if not last_name:
            errors.append('Last name is required')
        if not agency_name:
            errors.append('Agency name is required')

        if errors:
            return Response(
                {'error': 'ValidationError', 'message': '; '.join(errors)},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Create user in Supabase Auth
            supabase_url = settings.SUPABASE_URL
            service_role_key = settings.SUPABASE_SERVICE_ROLE_KEY

            with httpx.Client() as client:
                # Create auth user
                response = client.post(
                    f'{supabase_url}/auth/v1/admin/users',
                    json={
                        'email': email,
                        'password': password,
                        'email_confirm': True,  # Auto-confirm for admin registration
                    },
                    headers={
                        'apikey': service_role_key,
                        'Authorization': f'Bearer {service_role_key}',
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                if response.status_code not in (200, 201):
                    error_data = response.json() if response.content else {}
                    error_msg = error_data.get('msg', 'Registration failed')
                    return Response(
                        {'error': 'RegistrationError', 'message': error_msg},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                auth_user = response.json()
                auth_user_id = auth_user.get('id')

            # Create agency and user in database
            with connection.cursor() as cursor:
                # Create agency
                cursor.execute("""
                    INSERT INTO public.agencies (name)
                    VALUES (%s)
                    RETURNING id
                """, [agency_name])
                agency_id = cursor.fetchone()[0]

                # Create user with onboarding status
                cursor.execute("""
                    INSERT INTO public.users (
                        auth_user_id, email, first_name, last_name,
                        agency_id, role, is_admin, status
                    )
                    VALUES (%s, %s, %s, %s, %s, 'admin', true, 'onboarding')
                    RETURNING id
                """, [auth_user_id, email, first_name, last_name, agency_id])
                user_id = cursor.fetchone()[0]

            # Initialize onboarding progress for new admin
            try:
                create_onboarding_progress(user_id)
            except Exception as e:
                logger.warning(f'Failed to create onboarding progress for new admin: {e}')

            return Response(
                {
                    'message': 'Registration successful',
                    'user_id': str(user_id),
                    'agency_id': str(agency_id),
                },
                status=status.HTTP_201_CREATED
            )

        except Exception as e:
            logger.error(f'Registration failed: {e}')
            return Response(
                {'error': 'RegistrationError', 'message': 'Registration failed'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class VerifyInviteView(APIView):
    """
    POST /api/auth/verify-invite

    Verify an invite OTP token.

    Request Body:
        {
            "email": "user@example.com",
            "token": "123456"
        }

    Response (200):
        {
            "valid": true,
            "access_token": "...",
            "refresh_token": "...",
            "user": {...}
        }
    """
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get('email', '').strip().lower()
        token = request.data.get('token', '').strip()

        if not email or not token:
            return Response(
                {'error': 'ValidationError', 'message': 'Email and token required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            supabase_url = settings.SUPABASE_URL
            supabase_anon_key = settings.SUPABASE_ANON_KEY

            with httpx.Client() as client:
                # Verify OTP with Supabase
                response = client.post(
                    f'{supabase_url}/auth/v1/verify',
                    json={
                        'email': email,
                        'token': token,
                        'type': 'invite',
                    },
                    headers={
                        'apikey': supabase_anon_key,
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                if response.status_code != 200:
                    return Response(
                        {'error': 'InvalidToken', 'message': 'Invalid or expired invite token'},
                        status=status.HTTP_401_UNAUTHORIZED
                    )

                auth_data = response.json()

            # Update user status to onboarding and create onboarding progress
            auth_user_id = auth_data.get('user', {}).get('id')
            user_id = None
            if auth_user_id:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.users
                        SET status = 'onboarding'
                        WHERE auth_user_id = %s AND status = 'invited'
                        RETURNING id
                    """, [auth_user_id])
                    row = cursor.fetchone()
                    if row:
                        user_id = row[0]

                # Initialize onboarding progress for the user
                if user_id:
                    try:
                        create_onboarding_progress(user_id)
                    except Exception as e:
                        logger.warning(f'Failed to create onboarding progress: {e}')

            return Response({
                'valid': True,
                'access_token': auth_data.get('access_token'),
                'refresh_token': auth_data.get('refresh_token'),
            })

        except Exception as e:
            logger.error(f'Verify invite failed: {e}')
            return Response(
                {'error': 'VerificationError', 'message': 'Verification failed'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SetupAccountView(APIView):
    """
    POST /api/auth/setup-account

    Set password and update profile during onboarding.

    Headers:
        Authorization: Bearer <access_token>

    Request Body:
        {
            "password": "newpassword123",
            "first_name": "John",
            "last_name": "Doe"
        }

    Response (200):
        {"message": "Account setup complete"}
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        password = request.data.get('password', '')
        first_name = request.data.get('first_name', '').strip()
        last_name = request.data.get('last_name', '').strip()

        if password and len(password) < 8:
            return Response(
                {'error': 'ValidationError', 'message': 'Password must be at least 8 characters'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Update password in Supabase if provided
            if password:
                supabase_url = settings.SUPABASE_URL
                service_role_key = settings.SUPABASE_SERVICE_ROLE_KEY

                with httpx.Client() as client:
                    response = client.put(
                        f'{supabase_url}/auth/v1/admin/users/{user.auth_user_id}',
                        json={'password': password},
                        headers={
                            'apikey': service_role_key,
                            'Authorization': f'Bearer {service_role_key}',
                            'Content-Type': 'application/json',
                        },
                        timeout=10.0
                    )

                    if response.status_code not in (200, 201):
                        return Response(
                            {'error': 'UpdateError', 'message': 'Failed to update password'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR
                        )

            # Update user profile in database
            updates = []
            params = []

            if first_name:
                updates.append('first_name = %s')
                params.append(first_name)
            if last_name:
                updates.append('last_name = %s')
                params.append(last_name)

            if updates:
                params.append(str(user.id))
                with connection.cursor() as cursor:
                    cursor.execute(f"""
                        UPDATE public.users
                        SET {', '.join(updates)}
                        WHERE id = %s
                    """, params)

            return Response({'message': 'Account setup complete'})

        except Exception as e:
            logger.error(f'Setup account failed: {e}')
            return Response(
                {'error': 'SetupError', 'message': 'Account setup failed'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ForgotPasswordView(APIView):
    """
    POST /api/auth/forgot-password

    Send password reset email.

    Request Body:
        {"email": "user@example.com"}

    Response (200):
        {"message": "Password reset email sent"}
    """
    permission_classes = [AllowAny]
    # SECURITY FIX: Rate limit password reset to prevent abuse/enumeration
    throttle_classes = [AuthRateThrottle]

    def post(self, request):
        email = request.data.get('email', '').strip().lower()

        if not email:
            return Response(
                {'error': 'ValidationError', 'message': 'Email required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            supabase_url = settings.SUPABASE_URL
            supabase_anon_key = settings.SUPABASE_ANON_KEY
            app_url = settings.APP_URL

            with httpx.Client() as client:
                _response = client.post(
                    f'{supabase_url}/auth/v1/recover',
                    json={
                        'email': email,
                        'redirect_to': f'{app_url}/reset-password',
                    },
                    headers={
                        'apikey': supabase_anon_key,
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                # Always return success to prevent email enumeration
                return Response({'message': 'If an account exists, a password reset email has been sent'})

        except Exception as e:
            logger.error(f'Forgot password failed: {e}')
            return Response({'message': 'If an account exists, a password reset email has been sent'})


class ResetPasswordView(APIView):
    """
    POST /api/auth/reset-password

    Reset password with token.

    Request Body:
        {
            "access_token": "...",
            "password": "newpassword123"
        }

    Response (200):
        {"message": "Password reset successful"}
    """
    permission_classes = [AllowAny]
    # SECURITY FIX: Rate limit password reset to prevent abuse
    throttle_classes = [AuthRateThrottle]

    def post(self, request):
        access_token = request.data.get('access_token', '')
        password = request.data.get('password', '')

        if not access_token or not password:
            return Response(
                {'error': 'ValidationError', 'message': 'Access token and password required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if len(password) < 8:
            return Response(
                {'error': 'ValidationError', 'message': 'Password must be at least 8 characters'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            supabase_url = settings.SUPABASE_URL
            supabase_anon_key = settings.SUPABASE_ANON_KEY

            with httpx.Client() as client:
                response = client.put(
                    f'{supabase_url}/auth/v1/user',
                    json={'password': password},
                    headers={
                        'apikey': supabase_anon_key,
                        'Authorization': f'Bearer {access_token}',
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                if response.status_code != 200:
                    return Response(
                        {'error': 'ResetError', 'message': 'Password reset failed'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            return Response({'message': 'Password reset successful'})

        except Exception as e:
            logger.error(f'Reset password failed: {e}')
            return Response(
                {'error': 'ResetError', 'message': 'Password reset failed'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class RefreshTokenView(APIView):
    """
    POST /api/auth/refresh

    Refresh access token.

    Request Body:
        {"refresh_token": "..."}

    Response (200):
        {
            "access_token": "...",
            "refresh_token": "...",
            "expires_in": 3600
        }
    """
    permission_classes = [AllowAny]

    def post(self, request):
        refresh_token = request.data.get('refresh_token', '')

        if not refresh_token:
            return Response(
                {'error': 'ValidationError', 'message': 'Refresh token required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            supabase_url = settings.SUPABASE_URL
            supabase_anon_key = settings.SUPABASE_ANON_KEY

            with httpx.Client() as client:
                response = client.post(
                    f'{supabase_url}/auth/v1/token?grant_type=refresh_token',
                    json={'refresh_token': refresh_token},
                    headers={
                        'apikey': supabase_anon_key,
                        'Content-Type': 'application/json',
                    },
                    timeout=10.0
                )

                if response.status_code != 200:
                    return Response(
                        {'error': 'RefreshError', 'message': 'Token refresh failed'},
                        status=status.HTTP_401_UNAUTHORIZED
                    )

                auth_data = response.json()

            return Response({
                'access_token': auth_data.get('access_token'),
                'refresh_token': auth_data.get('refresh_token'),
                'expires_in': auth_data.get('expires_in'),
                'token_type': 'bearer',
            })

        except Exception as e:
            logger.error(f'Refresh token failed: {e}')
            return Response(
                {'error': 'RefreshError', 'message': 'Token refresh failed'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SessionView(APIView):
    """
    GET /api/auth/session

    Get current session info.

    Response (200):
        {
            "authenticated": true,
            "user": {...}
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response({'authenticated': False})

        return Response({
            'authenticated': True,
            'user': {
                'id': str(user.id),
                'email': user.email,
                'agency_id': str(user.agency_id),
                'role': user.role,
                'is_admin': user.is_admin,
                'status': user.status,
                'subscription_tier': user.subscription_tier,
            }
        })


class UserProfileView(APIView):
    """
    GET /api/user/profile - Get current user's full profile.
    PUT /api/user/profile - Update current user's profile.

    Response (200):
        {
            "id": "uuid",
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "agency_id": "uuid",
            "agency_name": "My Agency",
            ...
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        u.id, u.email, u.first_name, u.last_name,
                        u.agency_id, a.name as agency_name,
                        u.role, u.is_admin, u.status, u.perm_level,
                        u.subscription_tier, u.phone_number, u.start_date,
                        u.annual_goal, u.total_prod, u.total_policies_sold,
                        u.created_at, u.position_id, p.name as position_name, p.level as position_level
                    FROM public.users u
                    LEFT JOIN public.agencies a ON a.id = u.agency_id
                    LEFT JOIN public.positions p ON p.id = u.position_id
                    WHERE u.id = %s
                    LIMIT 1
                """, [str(user.id)])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'agency_id': str(row[4]) if row[4] else None,
                    'agency_name': row[5],
                    'role': row[6],
                    'is_admin': row[7],
                    'status': row[8],
                    'perm_level': row[9],
                    'subscription_tier': row[10],
                    'phone': row[11],
                    'start_date': str(row[12]) if row[12] else None,
                    'annual_goal': float(row[13]) if row[13] else None,
                    'total_prod': float(row[14]) if row[14] else 0,
                    'total_policies_sold': row[15] or 0,
                    'created_at': str(row[16]) if row[16] else None,
                    'position_id': str(row[17]) if row[17] else None,
                    'position_name': row[18],
                    'position_level': row[19],
                })

        except Exception as e:
            logger.error(f'Get profile failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get profile'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def put(self, request):
        """Update current user's profile."""
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Allowed fields for update
        allowed_fields = [
            'first_name', 'last_name', 'phone_number', 'status',
            'annual_goal', 'start_date',
        ]

        # Initialize onboarding on status change if needed
        status_changed_to_onboarding = False

        updates = []
        params = []

        for field in allowed_fields:
            if field in request.data:
                value = request.data[field]
                # Handle status transitions
                if field == 'status':
                    # Valid transitions:
                    # - invited -> onboarding (invite confirmation flow)
                    # - onboarding -> active (clients skipping wizard)
                    if value == 'onboarding' and user.status == 'invited':
                        status_changed_to_onboarding = True
                        updates.append(f'{field} = %s')
                        params.append(value)
                    elif value == 'active' and user.status == 'onboarding':
                        updates.append(f'{field} = %s')
                        params.append(value)
                    # Skip other status changes
                    continue
                updates.append(f'{field} = %s')
                params.append(value)

        if not updates:
            return Response(
                {'error': 'ValidationError', 'message': 'No valid fields to update'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            params.append(str(user.id))
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE public.users
                    SET {', '.join(updates)}, updated_at = NOW()
                    WHERE id = %s
                """, params)

            # Initialize onboarding progress if transitioning to onboarding
            if status_changed_to_onboarding:
                try:
                    create_onboarding_progress(user.id)
                except Exception as e:
                    logger.warning(f'Failed to create onboarding progress: {e}')

            return Response({
                'success': True,
                'message': 'Profile updated successfully'
            })

        except Exception as e:
            logger.error(f'Update profile failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update profile'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CompleteOnboardingView(APIView):
    """
    POST /api/user/complete-onboarding

    Mark user as having completed onboarding.

    Response (200):
        {"message": "Onboarding completed", "status": "active"}
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if user.status != 'onboarding':
            return Response(
                {'error': 'InvalidState', 'message': 'User is not in onboarding state'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET status = 'active'
                    WHERE id = %s AND status = 'onboarding'
                    RETURNING status
                """, [str(user.id)])
                result = cursor.fetchone()

                if not result:
                    return Response(
                        {'error': 'UpdateError', 'message': 'Failed to update status'},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

            return Response({
                'message': 'Onboarding completed',
                'status': 'active'
            })

        except Exception as e:
            logger.error(f'Complete onboarding failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to complete onboarding'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserByIdView(APIView):
    """
    GET /api/user/{user_id}

    Get a user by their ID. Only users in the same agency can be accessed.

    Response (200):
        {
            "id": "uuid",
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "is_admin": true,
            ...
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, user_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        u.id, u.email, u.first_name, u.last_name,
                        u.agency_id, u.role, u.is_admin, u.status, u.perm_level,
                        u.subscription_tier, u.phone_number, u.position_id,
                        p.name as position_name, p.level as position_level,
                        u.created_at
                    FROM public.users u
                    LEFT JOIN public.positions p ON p.id = u.position_id
                    WHERE u.id = %s AND u.agency_id = %s
                    LIMIT 1
                """, [user_id, str(user.agency_id)])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'agency_id': str(row[4]) if row[4] else None,
                    'role': row[5],
                    'is_admin': row[6],
                    'status': row[7],
                    'perm_level': row[8],
                    'subscription_tier': row[9],
                    'phone': row[10],
                    'position_id': str(row[11]) if row[11] else None,
                    'position_name': row[12],
                    'position_level': row[13],
                    'created_at': str(row[14]) if row[14] else None,
                })

        except Exception as e:
            logger.error(f'Get user by ID failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get user'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserCarriersView(APIView):
    """
    PATCH /api/user/{user_id}/carriers

    Update a user's unique_carriers list (for NIPR).
    Only the user themselves or an admin can update this.

    Request Body:
        {
            "unique_carriers": ["carrier1", "carrier2"]
        }

    Response (200):
        {"success": true, "unique_carriers": ["carrier1", "carrier2"]}
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, user_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Check permissions: must be self or admin
        is_self = str(user.id) == user_id
        if not is_self and not user.is_admin:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only update your own carriers'},
                status=status.HTTP_403_FORBIDDEN
            )

        unique_carriers = request.data.get('unique_carriers', [])
        if not isinstance(unique_carriers, list):
            return Response(
                {'error': 'ValidationError', 'message': 'unique_carriers must be an array'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            import json
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET unique_carriers = %s::jsonb, updated_at = NOW()
                    WHERE id = %s AND agency_id = %s
                    RETURNING unique_carriers
                """, [json.dumps(unique_carriers), user_id, str(user.agency_id)])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'success': True,
                    'unique_carriers': row[0] if row[0] else []
                })

        except Exception as e:
            logger.error(f'Update user carriers failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update carriers'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserNIPRDataView(APIView):
    """
    PATCH /api/user/{user_id}/nipr-data

    Update a user's NIPR data (unique_carriers and licensed_states).
    Used by NIPR cron job to save processed data.
    Only the user themselves, an admin, or cron jobs can update this.

    Request Body:
        {
            "unique_carriers": ["carrier1", "carrier2"],
            "licensed_states": ["CA", "TX", "NY"]
        }

    Response (200):
        {
            "success": true,
            "unique_carriers": ["carrier1", "carrier2"],
            "licensed_states": ["CA", "TX", "NY"]
        }
    """
    from apps.core.authentication import CronSecretAuthentication
    from rest_framework.authentication import SessionAuthentication

    authentication_classes = [CronSecretAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def patch(self, request, user_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Check permissions: must be self or admin (cron jobs auth as admin)
        is_self = str(user.id) == user_id
        if not is_self and not user.is_admin:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only update your own NIPR data'},
                status=status.HTTP_403_FORBIDDEN
            )

        unique_carriers = request.data.get('unique_carriers', [])
        licensed_states = request.data.get('licensed_states', [])

        if not isinstance(unique_carriers, list):
            return Response(
                {'error': 'ValidationError', 'message': 'unique_carriers must be an array'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(licensed_states, list):
            return Response(
                {'error': 'ValidationError', 'message': 'licensed_states must be an array'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            import json
            with connection.cursor() as cursor:
                # Check if this is a system cron user (has all-zeros UUID)
                is_cron_system_user = str(user.id) == '00000000-0000-0000-0000-000000000000'

                # SECURITY FIX: For cron system users, allow cross-agency access
                # For regular admins, still enforce agency boundary
                if is_cron_system_user:
                    cursor.execute("""
                        UPDATE public.users
                        SET
                            unique_carriers = %s::jsonb,
                            licensed_states = %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING unique_carriers, licensed_states
                    """, [
                        json.dumps(unique_carriers),
                        json.dumps(licensed_states),
                        user_id,
                    ])
                else:
                    # Regular users (including admins) are scoped to their agency
                    cursor.execute("""
                        UPDATE public.users
                        SET
                            unique_carriers = %s::jsonb,
                            licensed_states = %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s AND agency_id = %s
                        RETURNING unique_carriers, licensed_states
                    """, [
                        json.dumps(unique_carriers),
                        json.dumps(licensed_states),
                        user_id,
                        str(user.agency_id)
                    ])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'success': True,
                    'unique_carriers': row[0] if row[0] else [],
                    'licensed_states': row[1] if row[1] else []
                })

        except Exception as e:
            logger.error(f'Update user NIPR data failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update NIPR data'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserSmsUsageView(APIView):
    """
    GET /api/user/sms-usage - Get current user's SMS usage and subscription info
    POST /api/user/sms-usage/increment - Increment message sent count
    POST /api/user/sms-usage/reset - Reset message counter (for billing cycle)

    Used by SMS send route to check limits and update usage.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get user SMS usage and subscription info."""
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id,
                        agency_id,
                        first_name,
                        last_name,
                        subscription_tier,
                        subscription_status,
                        messages_sent_count,
                        messages_reset_date,
                        messages_topup_credits,
                        stripe_subscription_id,
                        billing_cycle_start,
                        billing_cycle_end
                    FROM public.users
                    WHERE id = %s
                """, [str(user.id)])
                row = cursor.fetchone()

            if not row:
                return Response(
                    {'error': 'NotFound', 'message': 'User not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response({
                'id': str(row[0]),
                'agency_id': str(row[1]) if row[1] else None,
                'first_name': row[2],
                'last_name': row[3],
                'subscription_tier': row[4] or 'free',
                'subscription_status': row[5] or 'free',
                'messages_sent_count': row[6] or 0,
                'messages_reset_date': row[7].isoformat() if row[7] else None,
                'messages_topup_credits': row[8] or 0,
                'stripe_subscription_id': row[9],
                'billing_cycle_start': row[10].isoformat() if row[10] else None,
                'billing_cycle_end': row[11].isoformat() if row[11] else None,
            })

        except Exception as e:
            logger.error(f'Get user SMS usage failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get SMS usage'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        """Update user message count (increment or reset)."""
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        action = request.data.get('action')

        if action == 'increment':
            # Increment message count by 1
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.users
                        SET messages_sent_count = COALESCE(messages_sent_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING messages_sent_count
                    """, [str(user.id)])
                    row = cursor.fetchone()

                return Response({
                    'success': True,
                    'messages_sent_count': row[0] if row else 0,
                })

            except Exception as e:
                logger.error(f'Increment message count failed: {e}')
                return Response(
                    {'error': 'ServerError', 'message': 'Failed to increment message count'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        elif action == 'reset':
            # Reset message counter (new billing cycle)
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.users
                        SET messages_sent_count = 0,
                            messages_reset_date = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING messages_sent_count, messages_reset_date
                    """, [str(user.id)])
                    row = cursor.fetchone()

                return Response({
                    'success': True,
                    'messages_sent_count': 0,
                    'messages_reset_date': row[1].isoformat() if row and row[1] else None,
                })

            except Exception as e:
                logger.error(f'Reset message count failed: {e}')
                return Response(
                    {'error': 'ServerError', 'message': 'Failed to reset message count'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        else:
            return Response(
                {'error': 'ValidationError', 'message': 'action must be "increment" or "reset"'},
                status=status.HTTP_400_BAD_REQUEST
            )


class UserByAuthIdView(APIView):
    """
    GET /api/users/by-auth-id/:auth_user_id

    Get user by their Supabase auth_user_id. This is used during onboarding
    when the user has an auth token but we need to look up their user record.

    This endpoint requires authentication via the access token.

    Response (200):
        {
            "id": "uuid",
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "phone_number": "1234567890",
            "role": "agent",
            "perm_level": "agent",
            "is_admin": false,
            "status": "onboarding",
            "agency_id": "uuid"
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, auth_user_id: str):
        # The auth_user_id in the URL must match the authenticated user's auth_user_id
        # This prevents users from looking up other users' data
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if str(user.auth_user_id) != auth_user_id:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only access your own user data'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, email, first_name, last_name, phone_number,
                        role, perm_level, is_admin, status, agency_id
                    FROM public.users
                    WHERE auth_user_id = %s
                    LIMIT 1
                """, [auth_user_id])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'phone_number': row[4],
                    'role': row[5],
                    'perm_level': row[6],
                    'is_admin': row[7],
                    'status': row[8],
                    'agency_id': str(row[9]) if row[9] else None,
                })

        except Exception as e:
            logger.error(f'Get user by auth ID failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get user'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserByAuthIdOnboardingView(APIView):
    """
    GET /api/users/by-auth-id/:auth_user_id/onboarding

    Get user by auth_user_id for onboarding flow. Only returns data if
    user status is 'onboarding'. Used by setup-account page.

    This is a more restricted version that validates the status.

    Response (200):
        {
            "id": "uuid",
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "phone_number": "1234567890",
            "role": "agent",
            "perm_level": "agent",
            "is_admin": false,
            "status": "onboarding",
            "agency_id": "uuid"
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, auth_user_id: str):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if str(user.auth_user_id) != auth_user_id:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only access your own user data'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, email, first_name, last_name, phone_number,
                        role, perm_level, is_admin, status, agency_id
                    FROM public.users
                    WHERE auth_user_id = %s AND status = 'onboarding'
                    LIMIT 1
                """, [auth_user_id])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found or already completed onboarding'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'phone_number': row[4],
                    'role': row[5],
                    'perm_level': row[6],
                    'is_admin': row[7],
                    'status': row[8],
                    'agency_id': str(row[9]) if row[9] else None,
                })

        except Exception as e:
            logger.error(f'Get user by auth ID for onboarding failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get user'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserStripeProfileView(APIView):
    """
    GET /api/user/stripe-profile

    Get current user's Stripe-related data for payment operations.

    Response (200):
        {
            "id": "uuid",
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "stripe_customer_id": "cus_xxx",
            "stripe_subscription_id": "sub_xxx",
            "subscription_tier": "pro",
            "subscription_status": "active",
            "billing_cycle_end": "2025-02-01T00:00:00",
            "scheduled_tier_change": null,
            "scheduled_tier_change_date": null,
            "is_admin": true,
            "role": "admin"
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, email, first_name, last_name,
                        stripe_customer_id, stripe_subscription_id,
                        subscription_tier, subscription_status,
                        billing_cycle_end, billing_cycle_start,
                        scheduled_tier_change, scheduled_tier_change_date,
                        is_admin, role
                    FROM public.users
                    WHERE id = %s
                    LIMIT 1
                """, [str(user.id)])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'id': str(row[0]),
                    'email': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'stripe_customer_id': row[4],
                    'stripe_subscription_id': row[5],
                    'subscription_tier': row[6] or 'free',
                    'subscription_status': row[7],
                    'billing_cycle_end': row[8].isoformat() if row[8] else None,
                    'billing_cycle_start': row[9].isoformat() if row[9] else None,
                    'scheduled_tier_change': row[10],
                    'scheduled_tier_change_date': row[11].isoformat() if row[11] else None,
                    'is_admin': row[12],
                    'role': row[13],
                })

        except Exception as e:
            logger.error(f'Get Stripe profile failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get Stripe profile'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserStripeCustomerIdView(APIView):
    """
    PATCH /api/user/stripe-customer-id

    Update user's Stripe customer ID. Used when creating a new Stripe customer.

    Request Body:
        {"stripe_customer_id": "cus_xxx"}

    Response (200):
        {"success": true, "stripe_customer_id": "cus_xxx"}
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Support both camelCase and snake_case
        stripe_customer_id = request.data.get('stripe_customer_id') or request.data.get('stripeCustomerId')

        if not stripe_customer_id:
            return Response(
                {'error': 'ValidationError', 'message': 'stripe_customer_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET stripe_customer_id = %s, updated_at = NOW()
                    WHERE id = %s
                    RETURNING stripe_customer_id
                """, [stripe_customer_id, str(user.id)])
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'success': True,
                    'stripe_customer_id': row[0],
                })

        except Exception as e:
            logger.error(f'Update Stripe customer ID failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update Stripe customer ID'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UserSubscriptionTierView(APIView):
    """
    PATCH /api/user/subscription-tier

    Update user's subscription tier and related fields.
    Used after Stripe subscription changes.

    Request Body:
        {
            "subscription_tier": "pro",
            "scheduled_tier_change": null,
            "scheduled_tier_change_date": null
        }

    Response (200):
        {"success": true, "subscription_tier": "pro"}
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'message': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Allowed fields for update
        allowed_fields = {
            'subscription_tier': 'subscription_tier',
            'subscriptionTier': 'subscription_tier',
            'scheduled_tier_change': 'scheduled_tier_change',
            'scheduledTierChange': 'scheduled_tier_change',
            'scheduled_tier_change_date': 'scheduled_tier_change_date',
            'scheduledTierChangeDate': 'scheduled_tier_change_date',
        }

        updates = []
        params = []

        for request_key, db_field in allowed_fields.items():
            if request_key in request.data:
                value = request.data[request_key]
                # Skip duplicate mappings
                if any(f'{db_field} = %s' in u for u in updates):
                    continue
                updates.append(f'{db_field} = %s')
                params.append(value)

        if not updates:
            return Response(
                {'error': 'ValidationError', 'message': 'No valid fields to update'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            params.append(str(user.id))
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE public.users
                    SET {', '.join(updates)}, updated_at = NOW()
                    WHERE id = %s
                    RETURNING subscription_tier, scheduled_tier_change, scheduled_tier_change_date
                """, params)
                row = cursor.fetchone()

                if not row:
                    return Response(
                        {'error': 'NotFound', 'message': 'User not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'success': True,
                    'subscription_tier': row[0],
                    'scheduled_tier_change': row[1],
                    'scheduled_tier_change_date': row[2].isoformat() if row[2] else None,
                })

        except Exception as e:
            logger.error(f'Update subscription tier failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update subscription tier'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
