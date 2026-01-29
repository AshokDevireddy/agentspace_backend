"""
SMS Services (P1-016, P2-029, P2-030, P2-031)

Business logic for SMS operations:
- Send individual messages
- Send bulk messages
- SMS template management
- Opt-out management
"""
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)

# Telnyx configuration (standardized SMS provider)
TELNYX_API_KEY = os.getenv('TELNYX_API_KEY')
TELNYX_API_URL = 'https://api.telnyx.com/v2/messages'


def normalize_phone_number(phone: str) -> str:
    """Normalize a phone number to E.164 format (+1XXXXXXXXXX) for sending SMS."""
    import re
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        return f'+1{digits}'
    if len(digits) == 11 and digits.startswith('1'):
        return f'+{digits}'
    if phone.startswith('+'):
        return f'+{digits}'
    return f'+{digits}'


def send_sms_via_telnyx(from_number: str, to_number: str, text: str) -> dict:
    """
    Send an SMS message via Telnyx API.

    Args:
        from_number: The sender phone number
        to_number: The recipient phone number
        text: The message content

    Returns:
        dict with 'success', 'message_id', and optionally 'error'
    """
    import requests  # type: ignore[import-untyped]

    if not TELNYX_API_KEY:
        return {'success': False, 'error': 'TELNYX_API_KEY is not configured'}

    normalized_from = normalize_phone_number(from_number)
    normalized_to = normalize_phone_number(to_number)

    try:
        response = requests.post(
            TELNYX_API_URL,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {TELNYX_API_KEY}',
            },
            json={
                'from': normalized_from,
                'to': normalized_to,
                'text': text,
            },
            timeout=30
        )

        if not response.ok:
            error_data = response.json()
            logger.error(f'Telnyx API error: {error_data}')
            return {'success': False, 'error': f'Telnyx API error: {response.status_code}'}

        data = response.json()
        return {
            'success': True,
            'message_id': data.get('data', {}).get('id'),
        }

    except Exception as e:
        logger.error(f'Telnyx send failed: {e}')
        return {'success': False, 'error': str(e)}




@dataclass
class SendMessageInput:
    """Input for sending a single SMS message."""
    conversation_id: UUID
    content: str
    from_number: str | None = None


@dataclass
class SendMessageResult:
    """Result of sending an SMS message."""
    success: bool
    message_id: UUID | None = None
    external_id: str | None = None
    error: str | None = None


@dataclass
class BulkSendInput:
    """Input for bulk SMS sending."""
    template_id: UUID | None = None
    content: str | None = None
    recipient_ids: list[UUID] | None = None
    recipient_type: str = 'client'  # 'client' or 'agent'


@dataclass
class BulkSendResult:
    """Result of bulk SMS sending."""
    success: bool
    total: int = 0
    sent: int = 0
    failed: int = 0
    errors: list[dict[Any, Any]] | None = None


def send_message(
    user: AuthenticatedUser,
    data: SendMessageInput,
) -> SendMessageResult:
    """
    Send an SMS message within an existing conversation.

    Args:
        user: The authenticated user sending the message
        data: Message data including conversation_id and content

    Returns:
        SendMessageResult with success status and message details
    """
    from apps.core.models import Conversation

    try:
        # Verify conversation access
        conversation = Conversation.objects.filter(  # type: ignore[attr-defined]
            id=data.conversation_id,
            agency_id=user.agency_id
        ).first()

        if not conversation:
            return SendMessageResult(success=False, error="Conversation not found")

        # Check opt-out status
        if conversation.sms_opt_in_status == 'opted_out':
            return SendMessageResult(success=False, error="Recipient has opted out of SMS")

        # Determine from number - get agency phone number if not provided
        from_number = data.from_number
        if not from_number:
            # Get agency phone number
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT phone_number FROM public.agencies WHERE id = %s
                """, [str(user.agency_id)])
                row = cursor.fetchone()
                if row:
                    from_number = row[0]

        if not from_number:
            return SendMessageResult(success=False, error="No from number configured")

        # Create message record first (pending status)
        message_id = uuid.uuid4()
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.messages (
                    id, conversation_id, content, direction, status, sent_by, created_at, updated_at
                )
                VALUES (%s, %s, %s, 'outbound', 'pending', %s, NOW(), NOW())
                RETURNING id
            """, [str(message_id), str(data.conversation_id), data.content, str(user.id)])

        # Send via Telnyx
        try:
            telnyx_result = send_sms_via_telnyx(
                from_number=from_number,
                to_number=conversation.phone_number,
                text=data.content
            )

            if not telnyx_result['success']:
                # Update message status to failed
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.messages
                        SET status = 'failed', updated_at = NOW()
                        WHERE id = %s
                    """, [str(message_id)])
                return SendMessageResult(
                    success=False,
                    message_id=message_id,
                    error=telnyx_result.get('error', 'Unknown Telnyx error')
                )

            external_id = telnyx_result.get('message_id')

            # Update message status to sent
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.messages
                    SET status = 'sent', external_id = %s, sent_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                """, [external_id, str(message_id)])

            # Update conversation last_message_at
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.conversations
                    SET last_message_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                """, [str(data.conversation_id)])

            # Increment user's message count
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET messages_sent_count = COALESCE(messages_sent_count, 0) + 1, updated_at = NOW()
                    WHERE id = %s
                """, [str(user.id)])

            logger.info(f"SMS sent successfully via Telnyx: {message_id} -> {external_id}")

            return SendMessageResult(
                success=True,
                message_id=message_id,
                external_id=external_id
            )

        except Exception as e:
            # Update message status to failed
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.messages
                    SET status = 'failed', updated_at = NOW()
                    WHERE id = %s
                """, [str(message_id)])

            logger.error(f"Telnyx send failed: {e}")
            return SendMessageResult(success=False, message_id=message_id, error=str(e))

    except Exception as e:
        logger.error(f"Send message failed: {e}")
        return SendMessageResult(success=False, error=str(e))


def send_bulk_messages(
    user: AuthenticatedUser,
    data: BulkSendInput,
) -> BulkSendResult:
    """
    Send bulk SMS messages to multiple recipients.

    Args:
        user: The authenticated user sending messages
        data: Bulk send data including template/content and recipient IDs

    Returns:
        BulkSendResult with success counts
    """
    if not data.recipient_ids:
        return BulkSendResult(success=False, errors=[{"error": "No recipients provided"}])

    # Get template content if using template
    content_template = data.content
    if data.template_id:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT content FROM public.sms_templates
                WHERE id = %s AND agency_id = %s AND is_active = true
            """, [str(data.template_id), str(user.agency_id)])
            row = cursor.fetchone()
            if row:
                content_template = row[0]
            else:
                return BulkSendResult(success=False, errors=[{"error": "Template not found"}])

    if not content_template:
        return BulkSendResult(success=False, errors=[{"error": "No content provided"}])

    # Get recipients with their conversation IDs
    recipient_ids_str = [str(rid) for rid in data.recipient_ids]

    if data.recipient_type == 'client':
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    c.id as client_id,
                    c.first_name,
                    c.last_name,
                    c.phone,
                    conv.id as conversation_id,
                    conv.sms_opt_in_status
                FROM public.clients c
                LEFT JOIN public.conversations conv ON conv.client_id = c.id AND conv.agency_id = %s
                WHERE c.id = ANY(%s::uuid[])
                    AND c.agency_id = %s
                    AND c.phone IS NOT NULL
            """, [str(user.agency_id), recipient_ids_str, str(user.agency_id)])
            recipients = cursor.fetchall()
    else:
        # Agent recipients
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    u.id as agent_id,
                    u.first_name,
                    u.last_name,
                    u.phone_number,
                    NULL as conversation_id,
                    NULL as sms_opt_in_status
                FROM public.users u
                WHERE u.id = ANY(%s::uuid[])
                    AND u.agency_id = %s
                    AND u.phone_number IS NOT NULL
            """, [recipient_ids_str, str(user.agency_id)])
            recipients = cursor.fetchall()

    total = len(recipients)
    sent = 0
    failed = 0
    errors = []

    for recipient in recipients:
        recipient_id, first_name, last_name, phone, conversation_id, opt_status = recipient

        # Check opt-out
        if opt_status == 'opted_out':
            failed += 1
            errors.append({"recipient_id": str(recipient_id), "error": "Opted out"})
            continue

        # Render template
        content = content_template
        content = content.replace('{{client_name}}', f"{first_name or ''} {last_name or ''}".strip())
        content = content.replace('{{first_name}}', first_name or '')
        content = content.replace('{{last_name}}', last_name or '')

        # Create or get conversation
        if not conversation_id and data.recipient_type == 'client':
            conv_id = uuid.uuid4()
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO public.conversations (
                        id, agency_id, agent_id, client_id, phone_number,
                        sms_opt_in_status, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, 'pending', NOW(), NOW())
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """, [str(conv_id), str(user.agency_id), str(user.id), str(recipient_id), phone])
                row = cursor.fetchone()
                conversation_id = row[0] if row else conv_id

        try:
            # Send message
            if conversation_id:
                result = send_message(user, SendMessageInput(
                    conversation_id=conversation_id,
                    content=content
                ))
                if result.success:
                    sent += 1
                else:
                    failed += 1
                    errors.append({"recipient_id": str(recipient_id), "error": result.error or "Unknown error"})
            else:
                # Direct send without conversation (for agents) - get agency phone
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT phone_number FROM public.agencies WHERE id = %s
                    """, [str(user.agency_id)])
                    agency_row = cursor.fetchone()
                    agency_phone = agency_row[0] if agency_row else None

                if not agency_phone:
                    failed += 1
                    errors.append({"recipient_id": str(recipient_id), "error": "No agency phone configured"})
                    continue

                telnyx_result = send_sms_via_telnyx(
                    from_number=agency_phone,
                    to_number=phone,
                    text=content
                )
                if telnyx_result['success']:
                    sent += 1
                else:
                    failed += 1
                    errors.append({"recipient_id": str(recipient_id), "error": telnyx_result.get('error', 'Unknown error')})

        except Exception as e:
            failed += 1
            errors.append({"recipient_id": str(recipient_id), "error": str(e)})

    return BulkSendResult(
        success=failed == 0,
        total=total,
        sent=sent,
        failed=failed,
        errors=errors if errors else None
    )


# =============================================================================
# SMS Templates (P2-029)
# =============================================================================

@dataclass
class TemplateInput:
    """Input for creating/updating SMS templates."""
    name: str
    template_type: str
    content: str
    is_active: bool = True


def create_template(user: AuthenticatedUser, data: TemplateInput) -> dict:
    """Create a new SMS template."""
    template_id = uuid.uuid4()

    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO public.sms_templates (
                id, agency_id, name, template_type, content, is_active, created_by, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id, created_at
        """, [
            str(template_id),
            str(user.agency_id),
            data.name,
            data.template_type,
            data.content,
            data.is_active,
            str(user.id)
        ])
        cursor.fetchone()

    result = get_template_by_id(template_id, user)
    return result if result is not None else {}


def update_template(template_id: UUID, user: AuthenticatedUser, data: TemplateInput) -> dict | None:
    """Update an existing SMS template."""
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.sms_templates
            SET name = %s, template_type = %s, content = %s, is_active = %s, updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [
            data.name,
            data.template_type,
            data.content,
            data.is_active,
            str(template_id),
            str(user.agency_id)
        ])
        row = cursor.fetchone()

    if not row:
        return None

    return get_template_by_id(template_id, user)


def delete_template(template_id: UUID, user: AuthenticatedUser) -> bool:
    """Delete an SMS template."""
    with connection.cursor() as cursor:
        cursor.execute("""
            DELETE FROM public.sms_templates
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(template_id), str(user.agency_id)])
        row = cursor.fetchone()

    return row is not None


def _template_row_to_dict(row) -> dict:
    """Convert a template database row to dict."""
    return {
        'id': str(row[0]),
        'name': row[1],
        'template_type': row[2],
        'content': row[3],
        'is_active': row[4],
        'created_at': row[5].isoformat() if row[5] else None,
        'updated_at': row[6].isoformat() if row[6] else None,
        'created_by': {
            'name': f"{row[7] or ''} {row[8] or ''}".strip()
        } if row[7] or row[8] else None
    }


def get_template_by_id(template_id: UUID, user: AuthenticatedUser) -> dict | None:
    """Get a single SMS template by ID."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                t.id, t.name, t.template_type, t.content, t.is_active,
                t.created_at, t.updated_at,
                u.first_name, u.last_name
            FROM public.sms_templates t
            LEFT JOIN public.users u ON u.id = t.created_by
            WHERE t.id = %s AND t.agency_id = %s
        """, [str(template_id), str(user.agency_id)])
        row = cursor.fetchone()

    return _template_row_to_dict(row) if row else None


def list_templates(user: AuthenticatedUser, template_type: str | None = None) -> list[dict]:
    """List all SMS templates for the agency."""
    params = [str(user.agency_id)]
    type_filter = ""
    if template_type:
        type_filter = "AND t.template_type = %s"
        params.append(template_type)

    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT
                t.id, t.name, t.template_type, t.content, t.is_active,
                t.created_at, t.updated_at,
                u.first_name, u.last_name
            FROM public.sms_templates t
            LEFT JOIN public.users u ON u.id = t.created_by
            WHERE t.agency_id = %s {type_filter}
            ORDER BY t.name
        """, params)
        rows = cursor.fetchall()

    return [_template_row_to_dict(row) for row in rows]


# =============================================================================
# Opt-out Management (P2-031)
# =============================================================================

def update_opt_status(
    user: AuthenticatedUser,
    conversation_id: UUID,
    opt_status: str,
) -> dict | None:
    """
    Update SMS opt-in/opt-out status for a conversation.

    Args:
        user: The authenticated user
        conversation_id: The conversation to update
        opt_status: 'opted_in', 'opted_out', or 'pending'

    Returns:
        Updated conversation dict or None if not found
    """
    valid_statuses = ['opted_in', 'opted_out', 'pending']
    if opt_status not in valid_statuses:
        raise ValueError(f"Invalid opt status. Must be one of: {valid_statuses}")

    timestamp_field = None
    if opt_status == 'opted_in':
        timestamp_field = 'opted_in_at'
    elif opt_status == 'opted_out':
        timestamp_field = 'opted_out_at'

    with connection.cursor() as cursor:
        if timestamp_field:
            cursor.execute(f"""
                UPDATE public.conversations
                SET sms_opt_in_status = %s, {timestamp_field} = NOW(), updated_at = NOW()
                WHERE id = %s AND agency_id = %s
                RETURNING id, phone_number, sms_opt_in_status, opted_in_at, opted_out_at
            """, [opt_status, str(conversation_id), str(user.agency_id)])
        else:
            cursor.execute("""
                UPDATE public.conversations
                SET sms_opt_in_status = %s, updated_at = NOW()
                WHERE id = %s AND agency_id = %s
                RETURNING id, phone_number, sms_opt_in_status, opted_in_at, opted_out_at
            """, [opt_status, str(conversation_id), str(user.agency_id)])

        row = cursor.fetchone()

    if not row:
        return None

    return {
        'id': str(row[0]),
        'phone_number': row[1],
        'sms_opt_in_status': row[2],
        'opted_in_at': row[3].isoformat() if row[3] else None,
        'opted_out_at': row[4].isoformat() if row[4] else None,
    }


def get_opted_out_numbers(user: AuthenticatedUser) -> list[dict]:
    """Get all opted-out phone numbers for the agency."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                c.id,
                c.phone_number,
                c.opted_out_at,
                cl.first_name,
                cl.last_name
            FROM public.conversations c
            LEFT JOIN public.clients cl ON cl.id = c.client_id
            WHERE c.agency_id = %s AND c.sms_opt_in_status = 'opted_out'
            ORDER BY c.opted_out_at DESC
        """, [str(user.agency_id)])
        rows = cursor.fetchall()

    return [
        {
            'conversation_id': str(row[0]),
            'phone_number': row[1],
            'opted_out_at': row[2].isoformat() if row[2] else None,
            'client_name': f"{row[3] or ''} {row[4] or ''}".strip() or None,
        }
        for row in rows
    ]


def handle_stop_keyword(phone_number: str, agency_id: UUID) -> bool:
    """
    Handle STOP keyword from inbound message.
    Updates all conversations with this phone number to opted_out.

    Args:
        phone_number: The phone number that sent STOP
        agency_id: The agency ID

    Returns:
        True if any conversations were updated
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.conversations
            SET sms_opt_in_status = 'opted_out', opted_out_at = NOW(), updated_at = NOW()
            WHERE phone_number = %s AND agency_id = %s
            RETURNING id
        """, [phone_number, str(agency_id)])
        rows = cursor.fetchall()

    logger.info(f"STOP keyword: opted out {len(rows)} conversations for {phone_number}")
    return len(rows) > 0


def handle_start_keyword(phone_number: str, agency_id: UUID) -> bool:
    """
    Handle START keyword from inbound message.
    Updates all conversations with this phone number to opted_in.

    Args:
        phone_number: The phone number that sent START
        agency_id: The agency ID

    Returns:
        True if any conversations were updated
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.conversations
            SET sms_opt_in_status = 'opted_in', opted_in_at = NOW(), updated_at = NOW()
            WHERE phone_number = %s AND agency_id = %s
            RETURNING id
        """, [phone_number, str(agency_id)])
        rows = cursor.fetchall()

    logger.info(f"START keyword: opted in {len(rows)} conversations for {phone_number}")
    return len(rows) > 0


# =============================================================================
# Draft Message Approval/Rejection
# =============================================================================


@dataclass
class ApproveDraftsResult:
    """Result of approving draft messages."""
    success: bool
    approved: int = 0
    failed: int = 0
    results: list[Any] | None = None
    errors: list[Any] | None = None


@dataclass
class RejectDraftsResult:
    """Result of rejecting draft messages."""
    success: bool
    rejected: int = 0


def approve_drafts(
    user: AuthenticatedUser,
    message_ids: list[str]
) -> ApproveDraftsResult:
    """
    Approve and send draft SMS messages.

    Args:
        user: The authenticated user approving the drafts
        message_ids: List of message IDs to approve

    Returns:
        ApproveDraftsResult with success counts and details
    """
    if not message_ids:
        return ApproveDraftsResult(success=False, errors=[{'error': 'No message IDs provided'}])

    results = []
    errors = []

    try:
        # Fetch draft messages with conversation and agency details
        message_ids_str = [str(mid) for mid in message_ids]
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    m.id,
                    m.content,
                    m.conversation_id,
                    c.phone_number as client_phone,
                    c.deal_id,
                    a.phone_number as agency_phone
                FROM public.messages m
                JOIN public.conversations c ON c.id = m.conversation_id
                JOIN public.deals d ON d.id = c.deal_id
                JOIN public.agencies a ON a.id = d.agency_id
                WHERE m.id = ANY(%s::uuid[])
                    AND m.status = 'draft'
                    AND d.agency_id = %s
            """, [message_ids_str, str(user.agency_id)])
            draft_messages = cursor.fetchall()

        if not draft_messages:
            return ApproveDraftsResult(
                success=True,
                approved=0,
                failed=0,
                results=[],
                errors=[{'error': 'No draft messages found with provided IDs'}]
            )

        for row in draft_messages:
            message_id, content, conversation_id, client_phone, deal_id, agency_phone = row

            if not agency_phone or not client_phone:
                errors.append({
                    'messageId': str(message_id),
                    'error': 'Missing phone numbers for sending SMS'
                })
                continue

            # Send SMS via Telnyx
            sms_result = send_sms_via_telnyx(
                from_number=agency_phone,
                to_number=client_phone,
                text=content
            )

            if sms_result['success']:
                # Update message status to 'sent'
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.messages
                        SET status = 'sent', sent_at = NOW(), updated_at = NOW()
                        WHERE id = %s
                    """, [str(message_id)])

                results.append({
                    'messageId': str(message_id),
                    'success': True,
                    'telnyxMessageId': sms_result.get('message_id'),
                })
                logger.info(f"Draft message {message_id} approved and sent")
            else:
                errors.append({
                    'messageId': str(message_id),
                    'error': sms_result.get('error', 'Unknown error'),
                })

        return ApproveDraftsResult(
            success=True,
            approved=len(results),
            failed=len(errors),
            results=results,
            errors=errors if errors else None
        )

    except Exception as e:
        logger.error(f'Error approving drafts: {e}')
        return ApproveDraftsResult(
            success=False,
            errors=[{'error': str(e)}]
        )


@dataclass
class UpdateDraftResult:
    """Result of updating a draft message."""
    success: bool
    message: dict | None = None
    error: str | None = None


def update_draft_body(
    user: AuthenticatedUser,
    message_id: UUID,
    new_body: str,
) -> UpdateDraftResult:
    """
    Update the body of a draft message.

    Args:
        user: The authenticated user
        message_id: The ID of the draft message to update
        new_body: The new message body

    Returns:
        UpdateDraftResult with the updated message or error
    """
    try:
        # Update draft message, ensuring it belongs to user's agency
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE public.messages m
                SET content = %s, updated_at = NOW()
                FROM public.conversations c
                JOIN public.deals d ON d.id = c.deal_id
                WHERE m.id = %s
                    AND m.conversation_id = c.id
                    AND d.agency_id = %s
                    AND m.status = 'draft'
                RETURNING m.id, m.conversation_id, m.content, m.direction,
                    m.status, m.sent_by, m.created_at, m.updated_at
            """, [new_body.strip(), str(message_id), str(user.agency_id)])
            row = cursor.fetchone()

        if not row:
            return UpdateDraftResult(
                success=False,
                error="Draft message not found"
            )

        logger.info(f"Draft message {message_id} body updated")

        return UpdateDraftResult(
            success=True,
            message={
                'id': str(row[0]),
                'conversation_id': str(row[1]),
                'body': row[2],  # Use 'body' to match frontend expectation
                'content': row[2],
                'direction': row[3],
                'status': row[4],
                'sent_by': str(row[5]) if row[5] else None,
                'created_at': row[6].isoformat() if row[6] else None,
                'updated_at': row[7].isoformat() if row[7] else None,
            }
        )

    except Exception as e:
        logger.error(f"Error updating draft body: {e}")
        return UpdateDraftResult(success=False, error=str(e))


def reject_drafts(
    user: AuthenticatedUser,
    message_ids: list[str]
) -> RejectDraftsResult:
    """
    Reject (delete) draft SMS messages.

    Args:
        user: The authenticated user rejecting the drafts
        message_ids: List of message IDs to reject

    Returns:
        RejectDraftsResult with rejection count
    """
    if not message_ids:
        return RejectDraftsResult(success=True, rejected=0)

    try:
        message_ids_str = [str(mid) for mid in message_ids]

        # Verify messages exist and are drafts within user's agency
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT m.id
                FROM public.messages m
                JOIN public.conversations c ON c.id = m.conversation_id
                JOIN public.deals d ON d.id = c.deal_id
                WHERE m.id = ANY(%s::uuid[])
                    AND m.status = 'draft'
                    AND d.agency_id = %s
            """, [message_ids_str, str(user.agency_id)])
            valid_ids = [str(row[0]) for row in cursor.fetchall()]

        if not valid_ids:
            logger.info("No valid draft messages found to delete")
            return RejectDraftsResult(success=True, rejected=0)

        # Delete the draft messages
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM public.messages
                WHERE id = ANY(%s::uuid[])
                    AND status = 'draft'
                RETURNING id
            """, [valid_ids])
            deleted = cursor.fetchall()

        deleted_count = len(deleted)
        logger.info(f"Rejected and deleted {deleted_count} draft messages")

        return RejectDraftsResult(success=True, rejected=deleted_count)

    except Exception as e:
        logger.error(f'Error rejecting drafts: {e}')
        return RejectDraftsResult(success=False, rejected=0)


# =============================================================================
# Mark Messages as Read
# =============================================================================

def mark_message_as_read(
    user: AuthenticatedUser,
    message_id: UUID,
) -> bool:
    """
    Mark a message as read by setting read_at timestamp.

    Args:
        user: The authenticated user
        message_id: The message ID to mark as read

    Returns:
        True if the message was marked as read, False otherwise
    """
    try:
        with connection.cursor() as cursor:
            # Only update if the user has access to this message
            # (message belongs to a conversation in their agency)
            cursor.execute("""
                UPDATE public.messages m
                SET read_at = NOW()
                FROM public.conversations c
                JOIN public.deals d ON d.id = c.deal_id
                WHERE m.id = %s
                    AND m.conversation_id = c.id
                    AND d.agency_id = %s
                    AND m.read_at IS NULL
                RETURNING m.id
            """, [str(message_id), str(user.agency_id)])
            result = cursor.fetchone()

        return result is not None

    except Exception as e:
        logger.error(f'Error marking message as read: {e}')
        return False


def find_conversation(
    user: AuthenticatedUser,
    agent_id: UUID | None = None,
    deal_id: UUID | None = None,
    phone: str | None = None,
) -> dict | None:
    """
    Find an existing conversation by agent_id, deal_id, or phone number.

    Args:
        user: The authenticated user
        agent_id: Optional agent ID to filter by
        deal_id: Optional deal ID to filter by
        phone: Optional phone number to filter by

    Returns:
        Conversation dict if found, None otherwise
    """
    if not any([agent_id, deal_id, phone]):
        return None

    try:
        conditions = ["c.agency_id = %s"]
        params = [str(user.agency_id)]

        if agent_id:
            conditions.append("c.agent_id = %s")
            params.append(str(agent_id))

        if deal_id:
            conditions.append("c.deal_id = %s")
            params.append(str(deal_id))

        if phone:
            normalized = normalize_phone_number(phone)
            conditions.append("c.phone_number = %s")
            params.append(normalized)

        where_clause = " AND ".join(conditions)

        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    c.id,
                    c.agency_id,
                    c.agent_id,
                    c.deal_id,
                    c.client_id,
                    c.phone_number,
                    c.sms_opt_in_status,
                    c.last_message_at,
                    c.created_at,
                    c.updated_at
                FROM public.conversations c
                WHERE {where_clause}
                ORDER BY c.updated_at DESC
                LIMIT 1
            """, params)
            row = cursor.fetchone()

        if not row:
            return None

        return {
            'id': str(row[0]),
            'agency_id': str(row[1]) if row[1] else None,
            'agent_id': str(row[2]) if row[2] else None,
            'deal_id': str(row[3]) if row[3] else None,
            'client_id': str(row[4]) if row[4] else None,
            'phone_number': row[5],
            'sms_opt_in_status': row[6],
            'last_message_at': row[7].isoformat() if row[7] else None,
            'created_at': row[8].isoformat() if row[8] else None,
            'updated_at': row[9].isoformat() if row[9] else None,
        }

    except Exception as e:
        logger.error(f'Error finding conversation: {e}')
        return None


@dataclass
class GetOrCreateConversationInput:
    """Input for get-or-create conversation."""
    agent_id: UUID
    deal_id: UUID | None = None
    client_id: UUID | None = None
    phone_number: str | None = None


@dataclass
class GetOrCreateConversationResult:
    """Result of get-or-create conversation operation."""
    success: bool
    conversation: dict | None = None
    created: bool = False
    error: str | None = None


def get_or_create_conversation(
    user: AuthenticatedUser,
    data: GetOrCreateConversationInput,
) -> GetOrCreateConversationResult:
    """
    Get an existing conversation or create a new one.
    This is an atomic operation to prevent race conditions.

    Args:
        user: The authenticated user
        data: Conversation data including agent_id, deal_id, client_id, phone_number

    Returns:
        GetOrCreateConversationResult with conversation data and created flag
    """
    if not data.phone_number and not data.deal_id:
        return GetOrCreateConversationResult(
            success=False,
            error="Either phone_number or deal_id is required"
        )

    try:
        normalized_phone = normalize_phone_number(data.phone_number) if data.phone_number else None

        # Try to find existing conversation first
        with connection.cursor() as cursor:
            # Build conditions based on provided data
            if data.deal_id:
                # If deal_id provided, use it as primary lookup
                cursor.execute("""
                    SELECT
                        c.id, c.agency_id, c.agent_id, c.deal_id, c.client_id,
                        c.phone_number, c.sms_opt_in_status, c.last_message_at,
                        c.created_at, c.updated_at
                    FROM public.conversations c
                    WHERE c.agency_id = %s
                        AND c.deal_id = %s
                    LIMIT 1
                """, [str(user.agency_id), str(data.deal_id)])
            elif normalized_phone:
                # If only phone provided, look up by agent + phone
                cursor.execute("""
                    SELECT
                        c.id, c.agency_id, c.agent_id, c.deal_id, c.client_id,
                        c.phone_number, c.sms_opt_in_status, c.last_message_at,
                        c.created_at, c.updated_at
                    FROM public.conversations c
                    WHERE c.agency_id = %s
                        AND c.agent_id = %s
                        AND c.phone_number = %s
                    LIMIT 1
                """, [str(user.agency_id), str(data.agent_id), normalized_phone])
            else:
                return GetOrCreateConversationResult(
                    success=False,
                    error="Either phone_number or deal_id is required"
                )

            existing = cursor.fetchone()

        if existing:
            return GetOrCreateConversationResult(
                success=True,
                conversation={
                    'id': str(existing[0]),
                    'agency_id': str(existing[1]) if existing[1] else None,
                    'agent_id': str(existing[2]) if existing[2] else None,
                    'deal_id': str(existing[3]) if existing[3] else None,
                    'client_id': str(existing[4]) if existing[4] else None,
                    'phone_number': existing[5],
                    'sms_opt_in_status': existing[6],
                    'last_message_at': existing[7].isoformat() if existing[7] else None,
                    'created_at': existing[8].isoformat() if existing[8] else None,
                    'updated_at': existing[9].isoformat() if existing[9] else None,
                },
                created=False
            )

        # If phone not provided but deal_id is, get phone from deal's client
        if not normalized_phone and data.deal_id:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT cl.phone, cl.id as client_id
                    FROM public.deals d
                    JOIN public.clients cl ON cl.id = d.client_id
                    WHERE d.id = %s AND d.agency_id = %s
                """, [str(data.deal_id), str(user.agency_id)])
                deal_row = cursor.fetchone()

            if deal_row and deal_row[0]:
                normalized_phone = normalize_phone_number(deal_row[0])
                if not data.client_id:
                    data.client_id = deal_row[1]

        if not normalized_phone:
            return GetOrCreateConversationResult(
                success=False,
                error="Could not determine phone number for conversation"
            )

        # Create new conversation
        conv_id = uuid.uuid4()
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.conversations (
                    id, agency_id, agent_id, deal_id, client_id, phone_number,
                    sms_opt_in_status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW())
                RETURNING id, agency_id, agent_id, deal_id, client_id, phone_number,
                    sms_opt_in_status, last_message_at, created_at, updated_at
            """, [
                str(conv_id),
                str(user.agency_id),
                str(data.agent_id),
                str(data.deal_id) if data.deal_id else None,
                str(data.client_id) if data.client_id else None,
                normalized_phone,
            ])
            row = cursor.fetchone()

        return GetOrCreateConversationResult(
            success=True,
            conversation={
                'id': str(row[0]),
                'agency_id': str(row[1]) if row[1] else None,
                'agent_id': str(row[2]) if row[2] else None,
                'deal_id': str(row[3]) if row[3] else None,
                'client_id': str(row[4]) if row[4] else None,
                'phone_number': row[5],
                'sms_opt_in_status': row[6],
                'last_message_at': row[7].isoformat() if row[7] else None,
                'created_at': row[8].isoformat() if row[8] else None,
                'updated_at': row[9].isoformat() if row[9] else None,
            },
            created=True
        )

    except Exception as e:
        logger.error(f'Error in get_or_create_conversation: {e}')
        return GetOrCreateConversationResult(success=False, error=str(e))


# =============================================================================
# Log Message (Create without sending)
# =============================================================================

@dataclass
class LogMessageInput:
    """Input for logging a message without sending."""
    conversation_id: UUID
    content: str
    direction: str  # 'inbound' or 'outbound'
    status: str = 'delivered'  # 'delivered', 'draft', 'pending', etc.
    metadata: dict | None = None


@dataclass
class LogMessageResult:
    """Result of logging a message."""
    success: bool
    message_id: UUID | None = None
    error: str | None = None


def log_message(
    user: AuthenticatedUser,
    data: LogMessageInput,
) -> LogMessageResult:
    """
    Log a message in the database without sending it via Telnyx.

    This is used for:
    - Recording inbound messages from webhooks
    - Creating draft messages for approval
    - Logging automated messages

    Args:
        user: The authenticated user
        data: Message data including conversation_id, content, direction, status

    Returns:
        LogMessageResult with success status and message_id
    """
    try:
        # Verify conversation access
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT c.id, c.agency_id
                FROM public.conversations c
                WHERE c.id = %s AND c.agency_id = %s
            """, [str(data.conversation_id), str(user.agency_id)])
            conv = cursor.fetchone()

        if not conv:
            return LogMessageResult(success=False, error="Conversation not found")

        is_draft = data.status == 'draft'
        now = None if is_draft else 'NOW()'

        # Create message record
        import json
        message_id = uuid.uuid4()
        metadata_json = json.dumps(data.metadata or {})

        # Build the SQL based on whether it's a draft or not
        sent_at_value = "NULL" if is_draft else "NOW()"
        read_at_value = "NULL" if is_draft or data.direction != 'outbound' else "NOW()"

        with connection.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO public.messages (
                    id, conversation_id, content, direction, status, sent_by,
                    metadata, sent_at, read_at, created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s::jsonb,
                    {sent_at_value},
                    {read_at_value},
                    NOW(), NOW()
                )
                RETURNING id
            """, [
                str(message_id),
                str(data.conversation_id),
                data.content,
                data.direction,
                data.status,
                str(user.id),
                metadata_json,
            ])

        # Update last_message_at in conversation (only for non-draft messages)
        if not is_draft:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.conversations
                    SET last_message_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                """, [str(data.conversation_id)])

        logger.info(f"Message logged: {message_id} ({data.status})")

        return LogMessageResult(success=True, message_id=message_id)

    except Exception as e:
        logger.error(f"Error logging message: {e}")
        return LogMessageResult(success=False, error=str(e))
