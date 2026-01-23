"""
Authentication API Views

Implements auth endpoints that work with Supabase Auth.
"""
import logging
from typing import Optional
from uuid import UUID

import httpx
from django.conf import settings
from django.db import connection
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import AuthenticatedUser, get_user_context
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

    def _get_user_data(self, auth_user_id: str) -> Optional[dict]:
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
                response = client.post(
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
    GET /api/user/profile

    Get current user's full profile.

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
                        u.subscription_tier, u.phone, u.start_date,
                        u.annual_goal, u.total_prod, u.total_policies_sold,
                        u.created_at
                    FROM public.users u
                    LEFT JOIN public.agencies a ON a.id = u.agency_id
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
                })

        except Exception as e:
            logger.error(f'Get profile failed: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get profile'},
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
