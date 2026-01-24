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
from typing import Optional
from uuid import UUID

from django.db import connection

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')


def get_twilio_client():
    """Get Twilio client instance."""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        raise ValueError("Twilio credentials not configured")

    from twilio.rest import Client
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


@dataclass
class SendMessageInput:
    """Input for sending a single SMS message."""
    conversation_id: UUID
    content: str
    from_number: Optional[str] = None


@dataclass
class SendMessageResult:
    """Result of sending an SMS message."""
    success: bool
    message_id: Optional[UUID] = None
    external_id: Optional[str] = None
    error: Optional[str] = None


@dataclass
class BulkSendInput:
    """Input for bulk SMS sending."""
    template_id: Optional[UUID] = None
    content: Optional[str] = None
    recipient_ids: list[UUID] = None
    recipient_type: str = 'client'  # 'client' or 'agent'


@dataclass
class BulkSendResult:
    """Result of bulk SMS sending."""
    success: bool
    total: int = 0
    sent: int = 0
    failed: int = 0
    errors: list[dict] = None


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
    from apps.core.models import Conversation, Message

    try:
        # Verify conversation access
        conversation = Conversation.objects.filter(
            id=data.conversation_id,
            agency_id=user.agency_id
        ).first()

        if not conversation:
            return SendMessageResult(success=False, error="Conversation not found")

        # Check opt-out status
        if conversation.sms_opt_in_status == 'opted_out':
            return SendMessageResult(success=False, error="Recipient has opted out of SMS")

        # Determine from number
        from_number = data.from_number or TWILIO_PHONE_NUMBER
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

        # Send via Twilio
        try:
            client = get_twilio_client()
            twilio_message = client.messages.create(
                body=data.content,
                from_=from_number,
                to=conversation.phone_number
            )
            external_id = twilio_message.sid

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

            logger.info(f"SMS sent successfully: {message_id} -> {external_id}")

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

            logger.error(f"Twilio send failed: {e}")
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
                    u.phone,
                    NULL as conversation_id,
                    NULL as sms_opt_in_status
                FROM public.users u
                WHERE u.id = ANY(%s::uuid[])
                    AND u.agency_id = %s
                    AND u.phone IS NOT NULL
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
                    errors.append({"recipient_id": str(recipient_id), "error": result.error})
            else:
                # Direct send without conversation (for agents)
                client = get_twilio_client()
                client.messages.create(
                    body=content,
                    from_=TWILIO_PHONE_NUMBER,
                    to=phone
                )
                sent += 1

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
        row = cursor.fetchone()

    return get_template_by_id(template_id, user)


def update_template(template_id: UUID, user: AuthenticatedUser, data: TemplateInput) -> Optional[dict]:
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


def get_template_by_id(template_id: UUID, user: AuthenticatedUser) -> Optional[dict]:
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


def list_templates(user: AuthenticatedUser, template_type: Optional[str] = None) -> list[dict]:
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
) -> Optional[dict]:
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
