"""
Client Services

Business logic for client operations.
"""
import logging
import os
from uuid import UUID

import httpx
from django.conf import settings
from django.db import connection, transaction

logger = logging.getLogger(__name__)

# Supabase configuration
SUPABASE_URL = getattr(settings, 'SUPABASE_URL', os.getenv('SUPABASE_URL', ''))
SUPABASE_SERVICE_KEY = getattr(settings, 'SUPABASE_SERVICE_ROLE_KEY', os.getenv('SUPABASE_SERVICE_ROLE_KEY', ''))
APP_URL = getattr(settings, 'APP_URL', os.getenv('NEXT_PUBLIC_APP_URL', 'http://localhost:3000'))


def _invite_user_by_email(email: str, redirect_url: str, agency_name: str) -> dict:
    """Invite a user via Supabase Auth admin API (sends email with magic link)."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f'{SUPABASE_URL}/auth/v1/admin/invite',
                json={
                    'email': email,
                    'redirect_to': redirect_url,
                    'data': {
                        'agency_name': agency_name,
                    },
                },
                headers={
                    'apikey': SUPABASE_SERVICE_KEY,
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                    'Content-Type': 'application/json',
                },
                timeout=10.0,
            )

            if response.status_code not in (200, 201):
                error_data = response.json() if response.content else {}
                error_msg = error_data.get('msg') or error_data.get('message') or 'Failed to send invite'
                logger.error(f'Supabase invite failed: {error_msg}')
                return {'success': False, 'error': error_msg}

            auth_user = response.json()
            return {'success': True, 'auth_user_id': auth_user.get('id')}

    except httpx.RequestError as e:
        logger.error(f'Supabase request error: {e}')
        return {'success': False, 'error': 'Authentication service unavailable'}


def _delete_supabase_user(auth_user_id: str) -> None:
    """Delete a Supabase auth user (cleanup on error)."""
    try:
        with httpx.Client() as client:
            client.delete(
                f'{SUPABASE_URL}/auth/v1/admin/users/{auth_user_id}',
                headers={
                    'apikey': SUPABASE_SERVICE_KEY,
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                },
                timeout=10.0,
            )
    except Exception as e:
        logger.error(f'Failed to cleanup auth user {auth_user_id}: {e}')


@transaction.atomic
def invite_client(
    *,
    inviter_id: UUID,
    agency_id: UUID,
    email: str,
    first_name: str,
    last_name: str,
    phone_number: str | None = None,
) -> dict:
    """
    Invite a new client to the agency.
    Creates Supabase auth user, sends invite email, and creates DB user record.

    Args:
        inviter_id: The inviting user's ID
        agency_id: The agency UUID
        email: Client's email
        first_name: Client's first name
        last_name: Client's last name
        phone_number: Optional phone number

    Returns:
        Dictionary with invited client details or error
    """
    email = email.strip().lower()

    with connection.cursor() as cursor:
        # Get agency info for whitelabel redirect URL
        cursor.execute("""
            SELECT whitelabel_domain, name FROM agencies WHERE id = %s
        """, [str(agency_id)])
        agency_row = cursor.fetchone()
        agency_whitelabel_domain = agency_row[0] if agency_row else None
        agency_name = agency_row[1] if agency_row else 'AgentSpace'

        # Build redirect URL - use /auth/confirm for implicit flow
        if agency_whitelabel_domain:
            protocol = 'https' if os.getenv('NODE_ENV') == 'production' else 'http'
            redirect_url = f'{protocol}://{agency_whitelabel_domain}/auth/confirm'
        else:
            redirect_url = f'{APP_URL}/auth/confirm'

        # Check if client already exists (including pre-invite and invited clients)
        cursor.execute("""
            SELECT id, email, status, auth_user_id, first_name, last_name
            FROM users
            WHERE email = %s AND role = 'client'
            LIMIT 1
        """, [email])

        existing = cursor.fetchone()

        # Handle pre-invite clients: create auth account and update to invited
        if existing and existing[2] == 'pre-invite':
            existing_id = existing[0]
            existing_first_name = existing[4]
            existing_last_name = existing[5]
            logger.info(f'Converting pre-invite client to invited: {existing_id}')

            # Create auth user and send invite email
            invite_result = _invite_user_by_email(email, redirect_url, agency_name)
            if not invite_result['success']:
                return {'success': False, 'error': invite_result.get('error', 'Failed to send invitation')}

            auth_user_id = invite_result['auth_user_id']

            # Update existing user record with auth_user_id and change status to invited
            cursor.execute("""
                UPDATE users
                SET
                    auth_user_id = %s,
                    status = 'invited',
                    first_name = %s,
                    last_name = %s,
                    phone_number = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id
            """, [
                auth_user_id,
                first_name or existing_first_name,
                last_name or existing_last_name,
                phone_number.strip() if phone_number else None,
                str(existing_id),
            ])

            updated = cursor.fetchone()
            if not updated:
                _delete_supabase_user(auth_user_id)
                return {'success': False, 'error': 'Failed to update client record'}

            return {
                'success': True,
                'user_id': str(existing_id),
                'message': 'Client invited successfully',
                'already_exists': False,
            }

        # If client already exists with invited, onboarding, or active status
        if existing:
            existing_status = existing[2]
            return {
                'success': True,
                'user_id': str(existing[0]),
                'message': 'Invitation already sent' if existing_status == 'invited' else 'Client already exists',
                'already_exists': True,
                'status': existing_status,
            }

        # New client - create auth account and user record
        invite_result = _invite_user_by_email(email, redirect_url, agency_name)
        if not invite_result['success']:
            return {'success': False, 'error': invite_result.get('error', 'Failed to send invitation')}

        auth_user_id = invite_result['auth_user_id']

        # Create user record with status='invited' and role='client'
        try:
            cursor.execute("""
                INSERT INTO users (
                    id, auth_user_id, email, first_name, last_name, phone_number,
                    agency_id, role, perm_level, is_admin, status,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'client', 'client', false, 'invited',
                        NOW(), NOW())
                RETURNING id, email, first_name, last_name, status
            """, [
                auth_user_id,  # Use auth_user_id as user id for consistency
                auth_user_id,
                email,
                first_name.strip(),
                last_name.strip(),
                phone_number.strip() if phone_number else None,
                str(agency_id),
            ])

            row = cursor.fetchone()
            if not row:
                _delete_supabase_user(auth_user_id)
                return {'success': False, 'error': 'Failed to create client record'}

            return {
                'success': True,
                'user_id': str(row[0]),
                'email': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'status': row[4],
                'message': 'Client invited successfully',
                'already_exists': False,
            }

        except Exception as e:
            logger.error(f'Failed to create client record: {e}')
            _delete_supabase_user(auth_user_id)
            return {'success': False, 'error': f'Failed to create client record: {e}'}


@transaction.atomic
def resend_client_invite(
    *,
    requester_id: UUID,
    agency_id: UUID,
    client_id: UUID,
) -> dict:
    """
    Resend an invitation to a client.
    Unlinks old auth account, deletes it, creates new invite, updates user record.

    Args:
        requester_id: The requesting user's ID (for permission checks)
        agency_id: The agency UUID
        client_id: The client to resend invite to

    Returns:
        Dictionary with success status and message or error
    """
    with connection.cursor() as cursor:
        # Get agency info for whitelabel redirect URL
        cursor.execute("""
            SELECT whitelabel_domain, name FROM agencies WHERE id = %s
        """, [str(agency_id)])
        agency_row = cursor.fetchone()
        agency_whitelabel_domain = agency_row[0] if agency_row else None
        agency_name = agency_row[1] if agency_row else 'AgentSpace'

        # Build redirect URL
        if agency_whitelabel_domain:
            protocol = 'https' if os.getenv('NODE_ENV') == 'production' else 'http'
            redirect_url = f'{protocol}://{agency_whitelabel_domain}/login'
        else:
            redirect_url = f'{APP_URL}/login'

        # Get the client to resend invite to
        cursor.execute("""
            SELECT id, auth_user_id, email, first_name, last_name, status, agency_id, role
            FROM users
            WHERE id = %s
        """, [str(client_id)])

        client_row = cursor.fetchone()
        if not client_row:
            return {'success': False, 'error': 'Client not found'}

        client_id_db, auth_user_id, email, first_name, last_name, status, client_agency_id, role = client_row

        # Verify the client is in the same agency
        if str(client_agency_id) != str(agency_id):
            return {'success': False, 'error': 'Cannot resend invites to clients from other agencies'}

        # Verify the client is in 'invited' or 'onboarding' status
        if status not in ('invited', 'onboarding'):
            return {'success': False, 'error': 'Can only resend invites to clients with invited or onboarding status'}

        # Verify the user has role of 'client'
        if role != 'client':
            return {'success': False, 'error': 'Can only resend invites to clients'}

        # 1. Unlink the old auth account from the user record
        if auth_user_id:
            logger.info(f'Unlinking auth account: {auth_user_id}')
            cursor.execute("""
                UPDATE users SET auth_user_id = NULL WHERE id = %s
            """, [str(client_id)])

            # 2. Delete the old auth account
            logger.info(f'Deleting old auth account: {auth_user_id}')
            _delete_supabase_user(auth_user_id)

        # 3. Create a new auth account and send invite email
        invite_result = _invite_user_by_email(email, redirect_url, agency_name)
        if not invite_result['success']:
            return {'success': False, 'error': invite_result.get('error', 'Failed to send invitation')}

        new_auth_user_id = invite_result['auth_user_id']

        # 4. Update the user record with the new auth_user_id and reset status to 'invited'
        cursor.execute("""
            UPDATE users
            SET auth_user_id = %s, status = 'invited', updated_at = NOW()
            WHERE id = %s
            RETURNING id
        """, [new_auth_user_id, str(client_id)])

        updated = cursor.fetchone()
        if not updated:
            # Cleanup: delete the newly created auth user
            _delete_supabase_user(new_auth_user_id)
            return {'success': False, 'error': 'Failed to link new auth account'}

        logger.info(f'User record updated with new auth_user_id for client {client_id}')

        return {
            'success': True,
            'message': f'Invitation resent successfully to {first_name} {last_name}',
        }
