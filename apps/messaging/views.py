"""
Messaging API Views

Provides endpoints for cron-triggered automated messaging:
- GET /api/messaging/billing-reminders - Deals for billing reminders
- GET /api/messaging/birthdays - Deals for birthday messages
- GET /api/messaging/holidays - Deals for holiday messages
- GET /api/messaging/lapse-reminders - Deals for lapse reminders
- GET /api/messaging/needs-info - Deals needing more info
- GET /api/messaging/policy-checkups - Deals for policy packet checkup
- GET /api/messaging/quarterly-checkins - Deals for quarterly check-in
"""
import logging

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import CronSecretAuthentication, SupabaseJWTAuthentication, get_user_context

from .selectors import (
    get_billing_reminder_deals,
    get_birthday_message_deals,
    get_holiday_message_deals,
    get_lapse_reminder_deals,
    get_needs_more_info_deals,
    get_policy_packet_checkup_deals,
    get_quarterly_checkin_deals,
)

logger = logging.getLogger(__name__)


class BillingRemindersView(APIView):
    """
    GET /api/messaging/billing-reminders

    Get deals eligible for billing reminder messages (3 days before next billing).
    Used by cron jobs for automated messaging.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Only admins can access cron endpoints
        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_billing_reminder_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Billing reminders failed: {e}')
            return Response(
                {'error': 'Failed to fetch billing reminders', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BirthdayMessagesView(APIView):
    """
    GET /api/messaging/birthdays

    Get deals with clients having birthdays today.
    Used by cron jobs for automated birthday messages.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_birthday_message_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Birthday messages failed: {e}')
            return Response(
                {'error': 'Failed to fetch birthday deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class HolidayMessagesView(APIView):
    """
    GET /api/messaging/holidays

    Get deals eligible for holiday messages.

    Query params:
        holiday_name: Name of the holiday (required)
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        holiday_name = request.query_params.get('holiday_name')
        if not holiday_name:
            return Response(
                {'error': 'holiday_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            deals = get_holiday_message_deals(holiday_name=holiday_name)
            return Response({
                'deals': deals,
                'count': len(deals),
                'holiday_name': holiday_name,
            })

        except Exception as e:
            logger.error(f'Holiday messages failed: {e}')
            return Response(
                {'error': 'Failed to fetch holiday deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class LapseRemindersView(APIView):
    """
    GET /api/messaging/lapse-reminders

    Get deals eligible for lapse reminder messages.
    Used by cron jobs for automated lapse notifications.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_lapse_reminder_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Lapse reminders failed: {e}')
            return Response(
                {'error': 'Failed to fetch lapse deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class NeedsInfoView(APIView):
    """
    GET /api/messaging/needs-info

    Get deals where application needs more info.
    Used by cron jobs for automated notifications to agents.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_needs_more_info_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Needs more info failed: {e}')
            return Response(
                {'error': 'Failed to fetch needs info deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PolicyCheckupsView(APIView):
    """
    GET /api/messaging/policy-checkups

    Get deals for policy packet checkup (14 days after effective).
    Used by cron jobs for automated policy packet follow-ups.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_policy_packet_checkup_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Policy checkups failed: {e}')
            return Response(
                {'error': 'Failed to fetch policy checkup deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class QuarterlyCheckinsView(APIView):
    """
    GET /api/messaging/quarterly-checkins

    Get deals for quarterly check-in (every 90 days since effective).
    Used by cron jobs for automated quarterly follow-ups.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        is_admin = user.is_admin or user.role == 'admin'
        if not is_admin:
            return Response(
                {'error': 'Admin access required'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            deals = get_quarterly_checkin_deals()
            return Response({
                'deals': deals,
                'count': len(deals),
            })

        except Exception as e:
            logger.error(f'Quarterly checkins failed: {e}')
            return Response(
                {'error': 'Failed to fetch quarterly checkin deals', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
