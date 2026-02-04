"""
Messaging Services

Business logic for cron-triggered automated messaging.
Creates draft messages for approval based on various triggers.
"""
import logging
from dataclasses import dataclass, field
from uuid import UUID

from django.db import connection, transaction

from apps.messaging.selectors import (
    get_billing_reminder_deals,
    get_birthday_message_deals,
    get_holiday_message_deals,
    get_lapse_reminder_deals,
    get_needs_more_info_deals,
    get_policy_packet_checkup_deals,
    get_quarterly_checkin_deals,
)
from apps.sms.services import find_conversation, log_message, LogMessageInput
from apps.sms.templates_service import (
    replace_placeholders,
    get_template,
    batch_get_agency_sms_settings,
    DEFAULT_SMS_TEMPLATES,
)
from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


@dataclass
class MessageJobResult:
    """Result of running a messaging job."""
    total: int = 0
    created: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def _check_tier_allows_auto_sms(subscription_tier: str | None) -> bool:
    """
    Check if subscription tier allows automated SMS messages.

    Only Pro and Expert tiers get automated messaging.

    Args:
        subscription_tier: The user's subscription tier

    Returns:
        True if tier allows auto SMS
    """
    tier = subscription_tier or 'free'
    return tier in ('pro', 'expert')


def _get_system_user() -> AuthenticatedUser:
    """Get a system user for cron operations."""
    return AuthenticatedUser(
        id=UUID('00000000-0000-0000-0000-000000000000'),
        auth_user_id=UUID('00000000-0000-0000-0000-000000000000'),
        email='system@internal',
        agency_id=UUID('00000000-0000-0000-0000-000000000000'),
        role='admin',
        is_admin=True,
        status='active',
        perm_level='admin',
        subscription_tier='expert',
        first_name='System',
        last_name='Cron',
    )


def _find_conversation_for_deal(
    agent_id: str,
    deal_id: str,
    agency_id: str,
    client_phone: str,
) -> dict | None:
    """
    Find an existing conversation for a deal.

    Does NOT create new conversations for cron jobs.

    Args:
        agent_id: The agent UUID
        deal_id: The deal UUID
        agency_id: The agency UUID
        client_phone: The client phone number

    Returns:
        Conversation dict if found, None otherwise
    """
    # Create a temporary user context for the find operation
    user = AuthenticatedUser(
        id=UUID(agent_id),
        auth_user_id=UUID('00000000-0000-0000-0000-000000000000'),
        email='',
        agency_id=UUID(agency_id),
        role='agent',
        is_admin=False,
        status='active',
        perm_level=None,
        subscription_tier='pro',
    )

    # Try finding by deal_id first
    conv = find_conversation(
        user=user,
        deal_id=UUID(deal_id),
    )

    if conv:
        return conv

    # Try finding by phone number
    conv = find_conversation(
        user=user,
        agent_id=UUID(agent_id),
        phone=client_phone,
    )

    return conv


def _create_draft_message(
    agency_id: str,
    conversation_id: str,
    agent_id: str,
    message_body: str,
    metadata: dict,
) -> bool:
    """
    Create a draft message in the database.

    Args:
        agency_id: The agency UUID
        conversation_id: The conversation UUID
        agent_id: The agent UUID
        message_body: The message content
        metadata: Message metadata

    Returns:
        True if successful
    """
    # Create user context for the agent
    user = AuthenticatedUser(
        id=UUID(agent_id),
        auth_user_id=UUID('00000000-0000-0000-0000-000000000000'),
        email='',
        agency_id=UUID(agency_id),
        role='agent',
        is_admin=False,
        status='active',
        perm_level=None,
        subscription_tier='pro',
    )

    result = log_message(
        user=user,
        data=LogMessageInput(
            conversation_id=UUID(conversation_id),
            content=message_body,
            direction='outbound',
            status='draft',
            metadata=metadata,
        ),
    )

    return result.success


def run_birthday_messages() -> MessageJobResult:
    """
    Create birthday message drafts for eligible deals.

    Runs daily to find clients with birthdays today.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running birthday messages job')

    deals = get_birthday_message_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No birthday deals found')
        return result

    # Batch fetch agency settings
    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            # Check if messaging is enabled
            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            # Check if birthday messages are enabled for agency
            if agency_settings.get('sms_birthday_enabled') is False:
                result.skipped += 1
                continue

            # Check subscription tier
            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            # Find existing conversation
            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            # Check opt-in status
            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            # Get first name from client_name
            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            # Get template (custom or default)
            custom_template = agency_settings.get('sms_birthday_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('birthday', '')

            # Render message
            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
                'agency_name': deal.get('agency_name', ''),
            })

            # Create draft message
            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'birthday',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                },
            )

            if success:
                result.created += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing birthday deal: {e}")

    logger.info(
        f'Birthday messages completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_billing_reminders() -> MessageJobResult:
    """
    Create billing reminder drafts for deals with payments due in 3 days.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running billing reminders job')

    deals = get_billing_reminder_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No billing reminder deals found')
        return result

    # Batch fetch agency settings
    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if agency_settings.get('sms_billing_reminder_enabled') is False:
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            custom_template = agency_settings.get('sms_billing_reminder_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('billing_reminder', '')

            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
            })

            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'billing_reminder',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                    'billing_cycle': deal.get('billing_cycle'),
                    'next_billing_date': deal.get('next_billing_date'),
                },
            )

            if success:
                result.created += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing billing reminder deal: {e}")

    logger.info(
        f'Billing reminders completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_lapse_reminders() -> MessageJobResult:
    """
    Create lapse reminder drafts for deals in pending lapse status.

    Also updates deal status to track notification state.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running lapse reminders job')

    deals = get_lapse_reminder_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No lapse reminder deals found')
        return result

    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if agency_settings.get('sms_lapse_reminder_enabled') is False:
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            agent_name = f"{deal.get('agent_first_name', '')} {deal.get('agent_last_name', '')}".strip()
            agent_phone = deal.get('agent_phone', '')

            custom_template = agency_settings.get('sms_lapse_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('lapse_reminder', '')

            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
            })

            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'lapse_reminder',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                },
            )

            if success:
                result.created += 1

                # Update deal status to track notification
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            UPDATE public.deals
                            SET status_standardized = 'lapse_sms_notified',
                                updated_at = NOW()
                            WHERE id = %s
                        """, [deal['deal_id']])
                except Exception as e:
                    logger.error(f"Failed to update deal status: {e}")
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing lapse reminder deal: {e}")

    logger.info(
        f'Lapse reminders completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_quarterly_checkins() -> MessageJobResult:
    """
    Create quarterly check-in drafts for deals on 90-day anniversaries.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running quarterly check-ins job')

    deals = get_quarterly_checkin_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No quarterly check-in deals found')
        return result

    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if agency_settings.get('sms_quarterly_enabled') is False:
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            agent_name = f"{deal.get('agent_first_name', '')} {deal.get('agent_last_name', '')}".strip()
            agent_phone = deal.get('agent_phone', '')

            custom_template = agency_settings.get('sms_quarterly_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('quarterly', '')

            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
            })

            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'quarterly_checkin',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                    'days_since_effective': deal.get('days_since_effective'),
                },
            )

            if success:
                result.created += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing quarterly check-in deal: {e}")

    logger.info(
        f'Quarterly check-ins completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_policy_packet_checkups() -> MessageJobResult:
    """
    Create policy packet follow-up drafts for deals 14 days after effective.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running policy packet checkups job')

    deals = get_policy_packet_checkup_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No policy packet checkup deals found')
        return result

    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if agency_settings.get('sms_policy_packet_enabled') is False:
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            custom_template = agency_settings.get('sms_policy_packet_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('policy_packet', '')

            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
            })

            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'policy_packet',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                    'policy_effective_date': deal.get('policy_effective_date'),
                },
            )

            if success:
                result.created += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing policy packet deal: {e}")

    logger.info(
        f'Policy packet checkups completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_holiday_messages(holiday_name: str) -> MessageJobResult:
    """
    Create holiday message drafts for eligible clients.

    Args:
        holiday_name: Name of the holiday (e.g., "Happy Holidays", "Merry Christmas")

    Returns:
        MessageJobResult with counts
    """
    logger.info(f'Running holiday messages job for: {holiday_name}')

    deals = get_holiday_message_deals(holiday_name=holiday_name)
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No holiday message deals found')
        return result

    agency_ids = [UUID(d['agency_id']) for d in deals]
    agency_settings_map = batch_get_agency_sms_settings(agency_ids)

    for deal in deals:
        try:
            agency_id = deal['agency_id']
            agency_settings = agency_settings_map.get(agency_id, {})

            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if agency_settings.get('sms_holiday_enabled') is False:
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            conversation = _find_conversation_for_deal(
                agent_id=deal['agent_id'],
                deal_id=deal['deal_id'],
                agency_id=agency_id,
                client_phone=deal['client_phone'],
            )

            if not conversation:
                result.skipped += 1
                continue

            if conversation.get('sms_opt_in_status') != 'opted_in':
                result.skipped += 1
                continue

            client_name = deal.get('client_name', '')
            first_name = client_name.split()[0] if client_name else 'there'

            agent_name = f"{deal.get('agent_first_name', '')} {deal.get('agent_last_name', '')}".strip()

            custom_template = agency_settings.get('sms_holiday_template')
            template = custom_template or DEFAULT_SMS_TEMPLATES.get('holiday', '')

            message_body = replace_placeholders(template, {
                'client_first_name': first_name,
                'agent_name': agent_name,
                'holiday_greeting': holiday_name,
            })

            success = _create_draft_message(
                agency_id=agency_id,
                conversation_id=conversation['id'],
                agent_id=deal['agent_id'],
                message_body=message_body,
                metadata={
                    'automated': True,
                    'type': 'holiday',
                    'deal_id': deal['deal_id'],
                    'client_phone': deal['client_phone'],
                    'client_name': client_name,
                    'holiday_name': holiday_name,
                },
            )

            if success:
                result.created += 1
            else:
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing holiday message deal: {e}")

    logger.info(
        f'Holiday messages completed: {result.created} created, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result


def run_needs_info_notifications() -> MessageJobResult:
    """
    Create notification messages for deals needing more info.

    Note: These are internal notifications to agents, not SMS to clients.

    Returns:
        MessageJobResult with counts
    """
    logger.info('Running needs more info notifications job')

    deals = get_needs_more_info_deals()
    result = MessageJobResult(total=len(deals))

    if not deals:
        logger.info('No needs more info deals found')
        return result

    for deal in deals:
        try:
            if not deal.get('messaging_enabled', False):
                result.skipped += 1
                continue

            if not _check_tier_allows_auto_sms(deal.get('agent_subscription_tier')):
                result.skipped += 1
                continue

            # Update deal status to track notification
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE public.deals
                        SET status_standardized = 'needs_more_info_notified',
                            updated_at = NOW()
                        WHERE id = %s
                    """, [deal['deal_id']])
                result.created += 1
            except Exception as e:
                logger.error(f"Failed to update deal status: {e}")
                result.failed += 1

        except Exception as e:
            result.failed += 1
            result.errors.append(f"Deal {deal.get('deal_id', 'unknown')}: {e!s}")
            logger.error(f"Error processing needs info deal: {e}")

    logger.info(
        f'Needs info notifications completed: {result.created} processed, '
        f'{result.skipped} skipped, {result.failed} failed'
    )

    return result
