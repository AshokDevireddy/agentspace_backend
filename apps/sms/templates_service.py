"""
SMS Templates Service

Provides template management and rendering for automated SMS messages.
Ported from frontend sms-template-helpers.ts.
"""
import logging
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)


# Default templates used across the application
# These match the frontend DEFAULT_SMS_TEMPLATES in sms-template-helpers.ts
DEFAULT_SMS_TEMPLATES = {
    'welcome': (
        'Welcome {{client_first_name}}! Thank you for choosing {{agency_name}} for your '
        'life insurance needs. Your agent {{agent_name}} is here to help. Complete your '
        'account setup via the invitation sent to {{client_email}}. Msg&data rates may apply. '
        'Reply STOP to opt out.'
    ),
    'billing_reminder': (
        'Hi {{client_first_name}}, this is a friendly reminder that your insurance premium '
        'is due soon. Please ensure funds are available for your scheduled payment. Thank you!'
    ),
    'lapse_reminder': (
        'Hi {{client_first_name}}, your policy is pending lapse. Your agent {{agent_name}} '
        'will reach out shortly at this number: {{agent_phone}}'
    ),
    'birthday': (
        'Happy Birthday, {{client_first_name}}! Wishing you a great year ahead from your '
        'friends at {{agency_name}}.'
    ),
    'holiday': (
        "Hi {{client_first_name}}, this is {{agent_name}} your insurance agent! Just wanting "
        "to wish you a {{holiday_greeting}}! Hope you're having a good one!"
    ),
    'quarterly': (
        "Hello {{client_first_name}}, It's {{agent_name}}, the insurance agent that helped "
        "with your life insurance. I hope you and your family are doing well! If you ever "
        "have any questions feel free to call me at my personal number {{agent_phone}}.\n\n"
        "I'm reaching out for our quarterly review of your policy. Please let me know if "
        "you would like to make any of these changes:\n\n"
        "- Add more coverage\n"
        "- Change the premium draft date\n"
        "- Need help getting coverage for another family member/friend\n\n"
        "Please feel free to respond to this message with a good time and day to speak "
        "with me or feel free to give me a call at {{agent_phone}}"
    ),
    'policy_packet': (
        "Hello {{client_first_name}},\n\n"
        "I'm reaching out to see if you received your policy packet yet. If not, please "
        "let me know. Thank you.\n\n"
        'If you have received the policy then please respond "Yes."\n\n'
        "Lastly, please make sure that your beneficiary has my contact information. I'm "
        "here to service your family so please let me know if you need anything."
    ),
}

# Placeholder definitions for each template type
SMS_TEMPLATE_PLACEHOLDERS = {
    'welcome': ['client_first_name', 'agency_name', 'agent_name', 'client_email'],
    'billing_reminder': ['client_first_name'],
    'lapse_reminder': ['client_first_name', 'agent_name', 'agent_phone'],
    'birthday': ['client_first_name', 'agency_name'],
    'holiday': ['client_first_name', 'agent_name', 'holiday_greeting'],
    'quarterly': ['client_first_name', 'agent_name', 'agent_phone'],
    'policy_packet': ['client_first_name'],
}

# Mapping of template types to agency column names
AGENCY_TEMPLATE_COLUMNS = {
    'welcome': 'sms_welcome_template',
    'birthday': 'sms_birthday_template',
    'billing_reminder': 'sms_billing_reminder_template',
    'lapse_reminder': 'sms_lapse_template',
    'quarterly': 'sms_quarterly_template',
    'holiday': 'sms_holiday_template',
    'policy_packet': 'sms_policy_packet_template',
}

# Mapping of template types to agency enabled column names
AGENCY_TEMPLATE_ENABLED_COLUMNS = {
    'birthday': 'sms_birthday_enabled',
    'billing_reminder': 'sms_billing_reminder_enabled',
    'lapse_reminder': 'sms_lapse_reminder_enabled',
    'quarterly': 'sms_quarterly_enabled',
    'holiday': 'sms_holiday_enabled',
    'policy_packet': 'sms_policy_packet_enabled',
}


def replace_placeholders(template: str, context: dict) -> str:
    """
    Replace {{placeholder}} tokens with values from context.

    Uses double curly braces syntax: {{client_first_name}}

    Args:
        template: The template string with placeholders
        context: Dictionary mapping placeholder names to values

    Returns:
        The template with all placeholders replaced
    """
    result = template
    for key, value in context.items():
        placeholder = '{{' + key + '}}'
        result = result.replace(placeholder, str(value) if value else '')
    return result


def get_agency_template(agency_id: UUID, template_type: str) -> str | None:
    """
    Get custom agency template for a specific template type.

    Args:
        agency_id: The agency UUID
        template_type: One of 'welcome', 'birthday', 'billing_reminder',
                      'lapse_reminder', 'quarterly', 'holiday', 'policy_packet'

    Returns:
        The custom template string if set, None if not found or empty
    """
    column = AGENCY_TEMPLATE_COLUMNS.get(template_type)
    if not column:
        logger.warning(f'Unknown template type: {template_type}')
        return None

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT {column} FROM public.agencies WHERE id = %s
            """, [str(agency_id)])
            row = cursor.fetchone()

        if row and row[0]:
            return row[0]
        return None

    except Exception as e:
        logger.error(f'Error fetching agency template: {e}')
        return None


def get_template(agency_id: UUID, template_type: str) -> str:
    """
    Get the appropriate template for a message type.

    First checks for a custom agency template, then falls back to default.

    Args:
        agency_id: The agency UUID
        template_type: One of 'welcome', 'birthday', 'billing_reminder',
                      'lapse_reminder', 'quarterly', 'holiday', 'policy_packet'

    Returns:
        The template string (custom or default)
    """
    # Try agency custom template first
    agency_template = get_agency_template(agency_id, template_type)
    if agency_template:
        return agency_template

    # Fall back to default
    return DEFAULT_SMS_TEMPLATES.get(template_type, '')


def is_template_enabled(agency_id: UUID, template_type: str) -> bool:
    """
    Check if a specific template type is enabled for the agency.

    Args:
        agency_id: The agency UUID
        template_type: One of 'birthday', 'billing_reminder', 'lapse_reminder',
                      'quarterly', 'holiday', 'policy_packet'

    Returns:
        True if enabled, False otherwise
    """
    column = AGENCY_TEMPLATE_ENABLED_COLUMNS.get(template_type)
    if not column:
        # If no enabled column exists, default to True
        return True

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT {column} FROM public.agencies WHERE id = %s
            """, [str(agency_id)])
            row = cursor.fetchone()

        # Default to True if not explicitly disabled
        if row is None:
            return True
        return row[0] if row[0] is not None else True

    except Exception as e:
        logger.error(f'Error checking template enabled status: {e}')
        return True


def get_agency_sms_settings(agency_id: UUID) -> dict:
    """
    Get all SMS settings for an agency.

    Returns a dictionary with:
    - messaging_enabled: bool
    - Template enabled flags for each type
    - Custom templates for each type

    Args:
        agency_id: The agency UUID

    Returns:
        Dictionary of SMS settings
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    messaging_enabled,
                    sms_birthday_enabled,
                    sms_billing_reminder_enabled,
                    sms_lapse_reminder_enabled,
                    sms_quarterly_enabled,
                    sms_holiday_enabled,
                    sms_policy_packet_enabled,
                    sms_welcome_template,
                    sms_birthday_template,
                    sms_billing_reminder_template,
                    sms_lapse_template,
                    sms_quarterly_template,
                    sms_holiday_template,
                    sms_policy_packet_template
                FROM public.agencies
                WHERE id = %s
            """, [str(agency_id)])
            row = cursor.fetchone()

        if not row:
            return {
                'messaging_enabled': False,
            }

        return {
            'messaging_enabled': row[0] if row[0] is not None else False,
            'sms_birthday_enabled': row[1] if row[1] is not None else True,
            'sms_billing_reminder_enabled': row[2] if row[2] is not None else True,
            'sms_lapse_reminder_enabled': row[3] if row[3] is not None else True,
            'sms_quarterly_enabled': row[4] if row[4] is not None else True,
            'sms_holiday_enabled': row[5] if row[5] is not None else True,
            'sms_policy_packet_enabled': row[6] if row[6] is not None else True,
            'sms_welcome_template': row[7],
            'sms_birthday_template': row[8],
            'sms_billing_reminder_template': row[9],
            'sms_lapse_template': row[10],
            'sms_quarterly_template': row[11],
            'sms_holiday_template': row[12],
            'sms_policy_packet_template': row[13],
        }

    except Exception as e:
        logger.error(f'Error fetching agency SMS settings: {e}')
        return {
            'messaging_enabled': False,
        }


def batch_get_agency_sms_settings(agency_ids: list[UUID]) -> dict[str, dict]:
    """
    Get SMS settings for multiple agencies at once.

    More efficient than calling get_agency_sms_settings in a loop.

    Args:
        agency_ids: List of agency UUIDs

    Returns:
        Dictionary mapping agency_id (str) to settings dict
    """
    if not agency_ids:
        return {}

    try:
        # Deduplicate
        unique_ids = list(set(str(aid) for aid in agency_ids))

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    messaging_enabled,
                    sms_birthday_enabled,
                    sms_billing_reminder_enabled,
                    sms_lapse_reminder_enabled,
                    sms_quarterly_enabled,
                    sms_holiday_enabled,
                    sms_policy_packet_enabled,
                    sms_welcome_template,
                    sms_birthday_template,
                    sms_billing_reminder_template,
                    sms_lapse_template,
                    sms_quarterly_template,
                    sms_holiday_template,
                    sms_policy_packet_template
                FROM public.agencies
                WHERE id = ANY(%s::uuid[])
            """, [unique_ids])
            rows = cursor.fetchall()

        result = {}
        for row in rows:
            agency_id_str = str(row[0])
            result[agency_id_str] = {
                'messaging_enabled': row[1] if row[1] is not None else False,
                'sms_birthday_enabled': row[2] if row[2] is not None else True,
                'sms_billing_reminder_enabled': row[3] if row[3] is not None else True,
                'sms_lapse_reminder_enabled': row[4] if row[4] is not None else True,
                'sms_quarterly_enabled': row[5] if row[5] is not None else True,
                'sms_holiday_enabled': row[6] if row[6] is not None else True,
                'sms_policy_packet_enabled': row[7] if row[7] is not None else True,
                'sms_welcome_template': row[8],
                'sms_birthday_template': row[9],
                'sms_billing_reminder_template': row[10],
                'sms_lapse_template': row[11],
                'sms_quarterly_template': row[12],
                'sms_holiday_template': row[13],
                'sms_policy_packet_template': row[14],
            }

        return result

    except Exception as e:
        logger.error(f'Error batch fetching agency SMS settings: {e}')
        return {}
