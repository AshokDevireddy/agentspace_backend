"""
Agency Settings API Views

Endpoints:
- GET /api/agencies/{id}/settings - Get agency settings
- PATCH /api/agencies/{id}/settings - Update agency settings
- POST /api/agencies/{id}/logo - Upload agency logo
"""
import logging
import os
import uuid

from django.db import connection
from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.mixins import AuthenticatedAPIView

logger = logging.getLogger(__name__)

# Supabase Storage configuration
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')


class AgencySettingsView(AuthenticatedAPIView, APIView):
    """
    GET /api/agencies/{agency_id}/settings - Get agency settings
    PATCH /api/agencies/{agency_id}/settings - Update agency settings
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, agency_id: str):
        """Get agency settings."""
        user = self.get_user(request)

        # Verify user belongs to this agency
        if str(user.agency_id) != agency_id:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only access your own agency settings'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, name, display_name, logo_url, primary_color,
                        whitelabel_domain, phone_number, messaging_enabled,
                        lead_sources, discord_webhook_url, discord_notification_enabled,
                        discord_notification_template, discord_bot_username,
                        theme_mode, lapse_email_notifications_enabled,
                        lapse_email_subject, lapse_email_body,
                        sms_welcome_enabled, sms_welcome_template,
                        sms_billing_reminder_enabled, sms_billing_reminder_template,
                        sms_lapse_reminder_enabled, sms_lapse_reminder_template,
                        sms_birthday_enabled, sms_birthday_template,
                        sms_holiday_enabled, sms_holiday_template,
                        sms_quarterly_enabled, sms_quarterly_template,
                        sms_policy_packet_enabled, sms_policy_packet_template,
                        default_scoreboard_start_date
                    FROM public.agencies
                    WHERE id = %s
                """, [agency_id])
                row = cursor.fetchone()

            if not row:
                return Response(
                    {'error': 'NotFound', 'message': 'Agency not found'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response({
                'id': str(row[0]),
                'name': row[1],
                'display_name': row[2],
                'logo_url': row[3],
                'primary_color': row[4],
                'whitelabel_domain': row[5],
                'phone_number': row[6],
                'messaging_enabled': row[7],
                'lead_sources': row[8] or [],
                'discord_webhook_url': row[9],
                'discord_notification_enabled': row[10],
                'discord_notification_template': row[11],
                'discord_bot_username': row[12],
                'theme_mode': row[13],
                'lapse_email_notifications_enabled': row[14],
                'lapse_email_subject': row[15],
                'lapse_email_body': row[16],
                'sms_welcome_enabled': row[17],
                'sms_welcome_template': row[18],
                'sms_billing_reminder_enabled': row[19],
                'sms_billing_reminder_template': row[20],
                'sms_lapse_reminder_enabled': row[21],
                'sms_lapse_reminder_template': row[22],
                'sms_birthday_enabled': row[23],
                'sms_birthday_template': row[24],
                'sms_holiday_enabled': row[25],
                'sms_holiday_template': row[26],
                'sms_quarterly_enabled': row[27],
                'sms_quarterly_template': row[28],
                'sms_policy_packet_enabled': row[29],
                'sms_policy_packet_template': row[30],
                'default_scoreboard_start_date': str(row[31]) if row[31] else None,
            })

        except Exception as e:
            logger.error(f'Error getting agency settings: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to get agency settings'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def patch(self, request, agency_id: str):
        """Update agency settings."""
        user = self.get_user(request)

        # Verify user belongs to this agency and is admin
        if str(user.agency_id) != agency_id:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only update your own agency settings'},
                status=status.HTTP_403_FORBIDDEN
            )

        if not user.is_admin:
            return Response(
                {'error': 'Forbidden', 'message': 'Only admins can update agency settings'},
                status=status.HTTP_403_FORBIDDEN
            )

        # Allowed fields for update
        allowed_fields = [
            'name', 'display_name', 'logo_url', 'primary_color',
            'whitelabel_domain', 'phone_number', 'messaging_enabled',
            'lead_sources', 'discord_webhook_url', 'discord_notification_enabled',
            'discord_notification_template', 'discord_bot_username',
            'theme_mode', 'lapse_email_notifications_enabled',
            'lapse_email_subject', 'lapse_email_body',
            'sms_welcome_enabled', 'sms_welcome_template',
            'sms_billing_reminder_enabled', 'sms_billing_reminder_template',
            'sms_lapse_reminder_enabled', 'sms_lapse_reminder_template',
            'sms_birthday_enabled', 'sms_birthday_template',
            'sms_holiday_enabled', 'sms_holiday_template',
            'sms_quarterly_enabled', 'sms_quarterly_template',
            'sms_policy_packet_enabled', 'sms_policy_packet_template',
            'default_scoreboard_start_date',
        ]

        updates = []
        params = []

        for field in allowed_fields:
            if field in request.data:
                value = request.data[field]
                # Handle JSON fields
                if field == 'lead_sources':
                    import json
                    updates.append(f'{field} = %s::jsonb')
                    params.append(json.dumps(value) if value else '[]')
                else:
                    updates.append(f'{field} = %s')
                    params.append(value)

        if not updates:
            return Response(
                {'error': 'ValidationError', 'message': 'No valid fields to update'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            params.append(agency_id)
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE public.agencies
                    SET {', '.join(updates)}, updated_at = NOW()
                    WHERE id = %s
                """, params)

            return Response({
                'success': True,
                'message': 'Agency settings updated successfully'
            })

        except Exception as e:
            logger.error(f'Error updating agency settings: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to update agency settings'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AgencyLogoUploadView(AuthenticatedAPIView, APIView):
    """
    POST /api/agencies/{agency_id}/logo - Upload agency logo
    """
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, agency_id: str):
        """Upload agency logo to Supabase Storage."""
        user = self.get_user(request)

        # Verify user belongs to this agency and is admin
        if str(user.agency_id) != agency_id:
            return Response(
                {'error': 'Forbidden', 'message': 'You can only update your own agency logo'},
                status=status.HTTP_403_FORBIDDEN
            )

        if not user.is_admin:
            return Response(
                {'error': 'Forbidden', 'message': 'Only admins can update agency logo'},
                status=status.HTTP_403_FORBIDDEN
            )

        file = request.FILES.get('file')
        if not file:
            return Response(
                {'error': 'ValidationError', 'message': 'No file provided'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate file type
        allowed_types = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/svg+xml']
        if file.content_type not in allowed_types:
            return Response(
                {'error': 'ValidationError', 'message': 'Invalid file type. Allowed: JPEG, PNG, GIF, WebP, SVG'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate file size (max 5MB)
        max_size = 5 * 1024 * 1024
        if file.size > max_size:
            return Response(
                {'error': 'ValidationError', 'message': 'File too large. Maximum size is 5MB'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            import requests  # type: ignore[import-untyped]

            # Generate unique filename
            ext = file.name.split('.')[-1] if '.' in file.name else 'png'
            filename = f"{agency_id}/{uuid.uuid4()}.{ext}"

            # Upload to Supabase Storage
            upload_url = f"{SUPABASE_URL}/storage/v1/object/logos/{filename}"

            response = requests.post(
                upload_url,
                headers={
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                    'Content-Type': file.content_type,
                },
                data=file.read(),
                timeout=30
            )

            if not response.ok:
                logger.error(f'Supabase storage upload failed: {response.text}')
                return Response(
                    {'error': 'UploadError', 'message': 'Failed to upload file'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # Get public URL
            public_url = f"{SUPABASE_URL}/storage/v1/object/public/logos/{filename}"

            # Update agency logo_url
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.agencies
                    SET logo_url = %s, updated_at = NOW()
                    WHERE id = %s
                """, [public_url, agency_id])

            return Response({
                'success': True,
                'logo_url': public_url,
                'message': 'Logo uploaded successfully'
            })

        except Exception as e:
            logger.error(f'Error uploading logo: {e}')
            return Response(
                {'error': 'ServerError', 'message': 'Failed to upload logo'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
