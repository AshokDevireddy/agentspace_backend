"""
Agent Services

Business logic for agent operations.
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


@transaction.atomic
def update_agent_position(
    *,
    user_id: UUID,
    agent_id: UUID,
    position_id: UUID,
) -> dict:
    """
    Update an agent's position with permission checks.
    Translated from Supabase RPC: update_agent_position

    Args:
        user_id: The requesting user's ID
        agent_id: The agent to update
        position_id: The new position ID

    Returns:
        Dictionary with success status and optional error message
    """
    with connection.cursor() as cursor:
        # Get current user context
        cursor.execute("""
            SELECT
                u.agency_id,
                COALESCE(u.is_admin, false) OR u.perm_level = 'admin' OR u.role = 'admin' as is_admin
            FROM users u
            WHERE u.id = %s
            LIMIT 1
        """, [str(user_id)])

        user_row = cursor.fetchone()
        if not user_row:
            return {'success': False, 'error': 'User not found'}

        agency_id, is_admin = user_row

        # Check if position belongs to the same agency
        cursor.execute("""
            SELECT agency_id
            FROM positions
            WHERE id = %s
            LIMIT 1
        """, [str(position_id)])

        position_row = cursor.fetchone()
        if not position_row or position_row[0] != agency_id:
            return {'success': False, 'error': 'Invalid position for this agency'}

        # Check permissions: admin can update anyone, agents can update their downlines
        has_permission = is_admin

        if not is_admin:
            # Check if agent_id is in the downline of user_id
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT u.id
                    FROM users u
                    WHERE u.id = %s
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE d.id <> u.id  -- Prevent cycles
                )
                SELECT EXISTS (
                    SELECT 1 FROM downline WHERE id = %s
                )
            """, [str(user_id), str(agent_id)])

            has_permission = cursor.fetchone()[0]

        if not has_permission:
            return {'success': False, 'error': 'You do not have permission to update this agent'}

        # Update the agent's position
        cursor.execute("""
            UPDATE users
            SET
                position_id = %s,
                updated_at = NOW()
            WHERE id = %s
                AND agency_id = %s
            RETURNING id
        """, [str(position_id), str(agent_id), str(agency_id)])

        updated = cursor.fetchone() is not None

        if not updated:
            return {'success': False, 'error': 'Agent not found or does not belong to your agency'}

        return {'success': True}


@transaction.atomic
def assign_position_to_agent(
    *,
    agent_id: UUID,
    position_id: UUID | None,
    agency_id: UUID,
) -> dict | None:
    """
    Assign a position to an agent.

    Args:
        agent_id: The agent UUID
        position_id: The position UUID (or None to clear)
        agency_id: The agency UUID for security check

    Returns:
        Updated agent dictionary or None if not found
    """
    with connection.cursor() as cursor:
        if position_id:
            # Verify position belongs to agency
            cursor.execute("""
                SELECT id FROM positions
                WHERE id = %s AND agency_id = %s
            """, [str(position_id), str(agency_id)])
            if not cursor.fetchone():
                raise ValueError('Position not found or does not belong to your agency')

        cursor.execute("""
            UPDATE users
            SET position_id = %s, updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id, first_name, last_name, email, position_id, status
        """, [str(position_id) if position_id else None, str(agent_id), str(agency_id)])

        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row, strict=False))
        return None


@transaction.atomic
def invite_agent(
    *,
    inviter_id: UUID,
    agency_id: UUID,
    email: str,
    first_name: str,
    last_name: str,
    phone_number: str | None = None,
    position_id: UUID | None = None,
    perm_level: str = 'agent',
    upline_id: UUID | None = None,
    pre_invite_user_id: UUID | None = None,
) -> dict:
    """
    Invite a new agent to the agency.
    Creates Supabase auth user, sends invite email, and creates DB user record.

    Args:
        inviter_id: The inviting user's ID (becomes upline if upline_id not specified)
        agency_id: The agency UUID
        email: Invited agent's email
        first_name: Invited agent's first name
        last_name: Invited agent's last name
        phone_number: Optional phone number
        position_id: Optional position ID
        perm_level: Permission level ('agent' or 'admin')
        upline_id: Optional upline ID (defaults to inviter_id)
        pre_invite_user_id: If set, update existing pre-invite user instead of creating new

    Returns:
        Dictionary with invited user details or error
    """
    import uuid as uuid_module

    email = email.strip().lower()
    is_admin = perm_level == 'admin'
    role = 'admin' if is_admin else 'agent'
    effective_upline_id = upline_id or inviter_id

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
            redirect_url = f'{protocol}://{agency_whitelabel_domain}/auth/confirm'
        else:
            redirect_url = f'{APP_URL}/auth/confirm'

        # Handle pre-invite user update
        if pre_invite_user_id:
            cursor.execute("""
                SELECT id, status, agency_id FROM users WHERE id = %s
            """, [str(pre_invite_user_id)])
            pre_invite_row = cursor.fetchone()

            if not pre_invite_row:
                return {'success': False, 'error': 'Pre-invite user not found'}
            if pre_invite_row[1] != 'pre-invite':
                return {'success': False, 'error': 'User is not in pre-invite status'}
            if str(pre_invite_row[2]) != str(agency_id):
                return {'success': False, 'error': 'Cannot update users from other agencies'}

            # Check if email is in use by another user
            cursor.execute("""
                SELECT id FROM users WHERE email = %s AND id != %s
            """, [email, str(pre_invite_user_id)])
            if cursor.fetchone():
                return {'success': False, 'error': 'This email is already in use by another user'}

            # Create Supabase auth user and send invite
            auth_result = _invite_via_supabase(email, redirect_url, agency_name)
            if not auth_result['success']:
                return auth_result

            auth_user_id = auth_result['auth_user_id']

            # Update pre-invite user with auth info
            cursor.execute("""
                UPDATE users SET
                    auth_user_id = %s,
                    email = %s,
                    phone_number = %s,
                    role = %s,
                    upline_id = %s,
                    position_id = %s,
                    perm_level = %s,
                    is_admin = %s,
                    status = 'invited',
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, email, first_name, last_name, status
            """, [
                auth_user_id,
                email,
                phone_number.strip() if phone_number else None,
                role,
                str(effective_upline_id) if effective_upline_id else None,
                str(position_id) if position_id else None,
                perm_level,
                is_admin,
                str(pre_invite_user_id),
            ])

            row = cursor.fetchone()
            if not row:
                # Cleanup: delete auth user
                _delete_supabase_user(auth_user_id)
                return {'success': False, 'error': 'Failed to update user'}

            return {
                'success': True,
                'user_id': str(row[0]),
                'email': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'status': row[4],
                'message': 'Pre-invite user updated and invitation sent',
            }

        # Check if user already exists
        cursor.execute("""
            SELECT id, status FROM users WHERE email = %s AND agency_id = %s
        """, [email, str(agency_id)])

        existing = cursor.fetchone()
        if existing:
            existing_id, existing_status = existing
            if existing_status == 'active':
                return {'success': False, 'error': 'User with this email already exists'}
            if existing_status == 'invited':
                return {'success': False, 'error': 'An invitation has already been sent to this email'}

        # Validate position if provided
        if position_id:
            cursor.execute("""
                SELECT id FROM positions WHERE id = %s AND agency_id = %s
            """, [str(position_id), str(agency_id)])
            if not cursor.fetchone():
                return {'success': False, 'error': 'Invalid position'}

        # Create Supabase auth user and send invite email
        auth_result = _invite_via_supabase(email, redirect_url, agency_name)
        if not auth_result['success']:
            return auth_result

        auth_user_id = auth_result['auth_user_id']

        # Create user record
        try:
            from datetime import date
            cursor.execute("""
                INSERT INTO users (
                    id, auth_user_id, email, first_name, last_name, phone_number,
                    agency_id, upline_id, position_id, role, perm_level, is_admin,
                    status, total_prod, total_policies_sold, annual_goal, start_date,
                    theme_mode, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        'invited', 0, 0, 0, %s, 'system', NOW(), NOW())
                RETURNING id, email, first_name, last_name, status
            """, [
                auth_user_id,  # Use auth_user_id as user id for consistency
                auth_user_id,
                email,
                first_name.strip(),
                last_name.strip(),
                phone_number.strip() if phone_number else None,
                str(agency_id),
                str(effective_upline_id) if effective_upline_id else None,
                str(position_id) if position_id else None,
                role,
                perm_level,
                is_admin,
                date.today().isoformat(),
            ])

            row = cursor.fetchone()
            if not row:
                _delete_supabase_user(auth_user_id)
                return {'success': False, 'error': 'Failed to create user record'}

            return {
                'success': True,
                'user_id': str(row[0]),
                'email': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'status': row[4],
                'message': 'Agent invited successfully',
            }

        except Exception as e:
            logger.error(f'Failed to create user record: {e}')
            _delete_supabase_user(auth_user_id)
            return {'success': False, 'error': f'Failed to create user record: {e}'}


def _invite_via_supabase(email: str, redirect_url: str, agency_name: str) -> dict:
    """Send invitation via Supabase Auth admin API."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f'{SUPABASE_URL}/auth/v1/admin/users',
                json={
                    'email': email,
                    'email_confirm': False,  # Don't auto-confirm, send invite
                    'user_metadata': {'agency_name': agency_name},
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
                error_msg = error_data.get('msg') or error_data.get('message') or 'Failed to create auth user'
                logger.error(f'Supabase invite failed: {error_msg}')
                return {'success': False, 'error': error_msg}

            auth_user = response.json()
            auth_user_id = auth_user.get('id')

            # Now send the invite email
            invite_response = client.post(
                f'{SUPABASE_URL}/auth/v1/admin/generate_link',
                json={
                    'type': 'invite',
                    'email': email,
                    'redirect_to': redirect_url,
                },
                headers={
                    'apikey': SUPABASE_SERVICE_KEY,
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                    'Content-Type': 'application/json',
                },
                timeout=10.0,
            )

            if invite_response.status_code not in (200, 201):
                logger.warning(f'Failed to generate invite link: {invite_response.text}')
                # Still return success - user was created, they can request new invite

            return {'success': True, 'auth_user_id': auth_user_id}

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


def _invite_user_by_email(email: str, redirect_url: str) -> dict:
    """Invite a user via Supabase Auth admin API (sends email with magic link)."""
    try:
        with httpx.Client() as client:
            response = client.post(
                f'{SUPABASE_URL}/auth/v1/admin/invite',
                json={
                    'email': email,
                    'redirect_to': redirect_url,
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


@transaction.atomic
def resend_agent_invite(
    *,
    requester_id: UUID,
    agency_id: UUID,
    agent_id: UUID,
) -> dict:
    """
    Resend an invitation to an agent.
    Unlinks old auth account, deletes it, creates new invite, updates user record.

    Args:
        requester_id: The requesting user's ID (for permission checks)
        agency_id: The agency UUID
        agent_id: The agent to resend invite to

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

        # Build redirect URL
        if agency_whitelabel_domain:
            protocol = 'https' if os.getenv('NODE_ENV') == 'production' else 'http'
            redirect_url = f'{protocol}://{agency_whitelabel_domain}/login'
        else:
            redirect_url = f'{APP_URL}/login'

        # Get the agent to resend invite to
        cursor.execute("""
            SELECT id, auth_user_id, email, first_name, last_name, status, agency_id, role
            FROM users
            WHERE id = %s
        """, [str(agent_id)])

        agent_row = cursor.fetchone()
        if not agent_row:
            return {'success': False, 'error': 'Agent not found'}

        agent_id_db, auth_user_id, email, first_name, last_name, status, agent_agency_id, role = agent_row

        # Verify the agent is in the same agency
        if str(agent_agency_id) != str(agency_id):
            return {'success': False, 'error': 'Cannot resend invites to agents from other agencies'}

        # Verify the agent is in 'invited' or 'onboarding' status
        if status not in ('invited', 'onboarding'):
            return {'success': False, 'error': 'Can only resend invites to agents with invited or onboarding status'}

        # Verify the agent has role of 'agent' or 'admin'
        if role not in ('agent', 'admin'):
            return {'success': False, 'error': 'Can only resend invites to agents and admins'}

        # 1. Unlink the old auth account from the user record
        if auth_user_id:
            logger.info(f'Unlinking auth account: {auth_user_id}')
            cursor.execute("""
                UPDATE users SET auth_user_id = NULL WHERE id = %s
            """, [str(agent_id)])

            # 2. Delete the old auth account
            logger.info(f'Deleting old auth account: {auth_user_id}')
            _delete_supabase_user(auth_user_id)

        # 3. Create a new auth account and send invite email
        invite_result = _invite_user_by_email(email, redirect_url)
        if not invite_result['success']:
            return {'success': False, 'error': invite_result.get('error', 'Failed to send invitation')}

        new_auth_user_id = invite_result['auth_user_id']

        # 4. Update the user record with the new auth_user_id and reset status to 'invited'
        cursor.execute("""
            UPDATE users
            SET auth_user_id = %s, status = 'invited', updated_at = NOW()
            WHERE id = %s
            RETURNING id
        """, [new_auth_user_id, str(agent_id)])

        updated = cursor.fetchone()
        if not updated:
            # Cleanup: delete the newly created auth user
            _delete_supabase_user(new_auth_user_id)
            return {'success': False, 'error': 'Failed to link new auth account'}

        logger.info(f'User record updated with new auth_user_id for agent {agent_id}')

        return {
            'success': True,
            'message': f'Invitation resent successfully to {first_name} {last_name}',
        }


@transaction.atomic
def update_agent(
    *,
    requester_id: UUID,
    agent_id: UUID,
    agency_id: UUID,
    first_name: str | None = None,
    last_name: str | None = None,
    phone_number: str | None = None,
    email: str | None = None,
    upline_id: UUID | None = None,
) -> dict:
    """
    Update an agent's details.

    Permission: Admin can update anyone in agency, non-admin can update their downlines.

    Args:
        requester_id: The requesting user's ID
        agent_id: The agent to update
        agency_id: The agency UUID for security check
        first_name: New first name (optional)
        last_name: New last name (optional)
        phone_number: New phone number (optional)
        email: New email (optional) - syncs to Supabase auth if changed
        upline_id: New upline ID (optional)

    Returns:
        Dictionary with success status and updated agent data or error
    """
    with connection.cursor() as cursor:
        # Get requester context
        cursor.execute("""
            SELECT
                COALESCE(is_admin, false) OR perm_level = 'admin' OR role = 'admin' as is_admin
            FROM users
            WHERE id = %s AND agency_id = %s
            LIMIT 1
        """, [str(requester_id), str(agency_id)])

        requester_row = cursor.fetchone()
        if not requester_row:
            return {'success': False, 'error': 'Requester not found'}

        is_admin = requester_row[0]

        # Get the agent to update
        cursor.execute("""
            SELECT id, auth_user_id, email, first_name, last_name, phone_number, upline_id, agency_id
            FROM users
            WHERE id = %s
            LIMIT 1
        """, [str(agent_id)])

        agent_row = cursor.fetchone()
        if not agent_row:
            return {'success': False, 'error': 'Agent not found'}

        (agent_id_db, auth_user_id, current_email, current_first_name, current_last_name,
         current_phone, current_upline_id, agent_agency_id) = agent_row

        # Verify agent is in the same agency
        if str(agent_agency_id) != str(agency_id):
            return {'success': False, 'error': 'Cannot update agents from other agencies'}

        # Check permissions
        has_permission = is_admin

        if not is_admin:
            # Check if agent_id is in the downline of requester_id
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT u.id
                    FROM users u
                    WHERE u.id = %s
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE d.id <> u.id
                )
                SELECT EXISTS (
                    SELECT 1 FROM downline WHERE id = %s
                )
            """, [str(requester_id), str(agent_id)])

            has_permission = cursor.fetchone()[0]

        if not has_permission:
            return {'success': False, 'error': 'You do not have permission to update this agent'}

        # Build update fields
        update_fields = []
        update_values = []

        if first_name is not None and first_name != current_first_name:
            update_fields.append('first_name = %s')
            update_values.append(first_name.strip())

        if last_name is not None and last_name != current_last_name:
            update_fields.append('last_name = %s')
            update_values.append(last_name.strip())

        if phone_number is not None and phone_number != current_phone:
            update_fields.append('phone_number = %s')
            update_values.append(phone_number.strip() if phone_number else None)

        if upline_id is not None:
            upline_id_str = str(upline_id) if upline_id else None
            if upline_id_str != str(current_upline_id) if current_upline_id else None:
                # Verify upline exists in same agency
                if upline_id:
                    cursor.execute("""
                        SELECT id FROM users WHERE id = %s AND agency_id = %s
                    """, [str(upline_id), str(agency_id)])
                    if not cursor.fetchone():
                        return {'success': False, 'error': 'Upline agent not found in this agency'}
                update_fields.append('upline_id = %s')
                update_values.append(str(upline_id) if upline_id else None)

        # Handle email change - requires Supabase auth sync
        email_changed = False
        if email is not None and email.strip().lower() != current_email:
            new_email = email.strip().lower()
            # Check if email is already in use
            cursor.execute("""
                SELECT id FROM users WHERE email = %s AND id != %s
            """, [new_email, str(agent_id)])
            if cursor.fetchone():
                return {'success': False, 'error': 'Email is already in use'}
            update_fields.append('email = %s')
            update_values.append(new_email)
            email_changed = True

        if not update_fields:
            return {'success': True, 'message': 'No changes to apply', 'agent_id': str(agent_id)}

        # Add updated_at
        update_fields.append('updated_at = NOW()')

        # Build and execute update query
        update_values.append(str(agent_id))
        update_values.append(str(agency_id))

        cursor.execute(f"""
            UPDATE users
            SET {', '.join(update_fields)}
            WHERE id = %s AND agency_id = %s
            RETURNING id, email, first_name, last_name, phone_number, upline_id
        """, update_values)

        row = cursor.fetchone()
        if not row:
            return {'success': False, 'error': 'Failed to update agent'}

        # Sync email to Supabase auth if changed
        if email_changed and auth_user_id:
            try:
                _update_supabase_user_email(auth_user_id, email.strip().lower())
            except Exception as e:
                logger.warning(f'Failed to sync email to Supabase auth: {e}')
                # Don't fail the update, just log

        return {
            'success': True,
            'agent': {
                'id': str(row[0]),
                'email': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'phone_number': row[4],
                'upline_id': str(row[5]) if row[5] else None,
            },
        }


def _update_supabase_user_email(auth_user_id: str, new_email: str) -> None:
    """Update a Supabase auth user's email."""
    try:
        with httpx.Client() as client:
            response = client.put(
                f'{SUPABASE_URL}/auth/v1/admin/users/{auth_user_id}',
                json={'email': new_email},
                headers={
                    'apikey': SUPABASE_SERVICE_KEY,
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                    'Content-Type': 'application/json',
                },
                timeout=10.0,
            )
            if response.status_code not in (200, 201):
                logger.error(f'Failed to update Supabase user email: {response.text}')
    except httpx.RequestError as e:
        logger.error(f'Supabase request error updating email: {e}')


@transaction.atomic
def deactivate_agent(
    *,
    requester_id: UUID,
    agent_id: UUID,
    agency_id: UUID,
    reassign_downlines: bool = True,
) -> dict:
    """
    Soft-delete an agent by setting is_active=False.

    Permission: Admin only.

    Args:
        requester_id: The requesting user's ID
        agent_id: The agent to deactivate
        agency_id: The agency UUID for security check
        reassign_downlines: If True, reassign agent's downlines to agent's upline

    Returns:
        Dictionary with success status or error
    """
    with connection.cursor() as cursor:
        # Verify requester is admin
        cursor.execute("""
            SELECT
                COALESCE(is_admin, false) OR perm_level = 'admin' OR role = 'admin' as is_admin
            FROM users
            WHERE id = %s AND agency_id = %s
            LIMIT 1
        """, [str(requester_id), str(agency_id)])

        requester_row = cursor.fetchone()
        if not requester_row:
            return {'success': False, 'error': 'Requester not found'}

        if not requester_row[0]:
            return {'success': False, 'error': 'Admin access required to deactivate agents'}

        # Get the agent to deactivate
        cursor.execute("""
            SELECT id, auth_user_id, upline_id, email, first_name, last_name, is_active, agency_id
            FROM users
            WHERE id = %s
            LIMIT 1
        """, [str(agent_id)])

        agent_row = cursor.fetchone()
        if not agent_row:
            return {'success': False, 'error': 'Agent not found'}

        (agent_id_db, auth_user_id, upline_id, email, first_name, last_name,
         is_active, agent_agency_id) = agent_row

        # Verify agent is in the same agency
        if str(agent_agency_id) != str(agency_id):
            return {'success': False, 'error': 'Cannot deactivate agents from other agencies'}

        # Prevent self-deactivation
        if str(agent_id) == str(requester_id):
            return {'success': False, 'error': 'Cannot deactivate yourself'}

        # Check if already inactive
        if not is_active:
            return {'success': True, 'message': 'Agent is already inactive'}

        # Optionally reassign downlines to the agent's upline
        reassigned_count = 0
        if reassign_downlines and upline_id:
            cursor.execute("""
                UPDATE users
                SET upline_id = %s, updated_at = NOW()
                WHERE upline_id = %s AND agency_id = %s
                RETURNING id
            """, [str(upline_id), str(agent_id), str(agency_id)])
            reassigned_count = cursor.rowcount

        # Deactivate the agent
        cursor.execute("""
            UPDATE users
            SET is_active = false, status = 'inactive', updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(agent_id), str(agency_id)])

        if not cursor.fetchone():
            return {'success': False, 'error': 'Failed to deactivate agent'}

        # Deactivate Supabase auth user
        if auth_user_id:
            try:
                _deactivate_supabase_user(auth_user_id)
            except Exception as e:
                logger.warning(f'Failed to deactivate Supabase auth user: {e}')
                # Don't fail - DB change is primary

        return {
            'success': True,
            'message': f'Agent {first_name} {last_name} has been deactivated',
            'reassigned_downlines': reassigned_count,
        }


def _deactivate_supabase_user(auth_user_id: str) -> None:
    """Deactivate a Supabase auth user (ban them)."""
    try:
        with httpx.Client() as client:
            response = client.put(
                f'{SUPABASE_URL}/auth/v1/admin/users/{auth_user_id}',
                json={'ban_duration': 'none'},  # Permanent ban
                headers={
                    'apikey': SUPABASE_SERVICE_KEY,
                    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                    'Content-Type': 'application/json',
                },
                timeout=10.0,
            )
            if response.status_code not in (200, 201):
                logger.error(f'Failed to deactivate Supabase user: {response.text}')
    except httpx.RequestError as e:
        logger.error(f'Supabase request error deactivating user: {e}')
