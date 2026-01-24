"""
Messaging Selectors

Cron job queries for automated messaging translated from Supabase RPC functions:
- get_billing_reminder_deals_v2 -> get_billing_reminder_deals()
- get_birthday_message_deals -> get_birthday_message_deals()
- get_holiday_message_deals -> get_holiday_message_deals()
- get_lapse_reminder_deals -> get_lapse_reminder_deals()
- get_needs_more_info_deals -> get_needs_more_info_deals()
- get_policy_packet_checkup_deals -> get_policy_packet_checkup_deals()
- get_quarterly_checkin_deals -> get_quarterly_checkin_deals()
"""
import logging
from datetime import date, timedelta
from typing import Optional
import calendar

from django.db import connection

logger = logging.getLogger(__name__)


def calculate_next_billing_date(
    billing_day_of_month: Optional[int],
    billing_weekday: Optional[int],
    billing_cycle: str,
    reference_date: date,
    today: Optional[date] = None
) -> Optional[date]:
    """
    Calculate the next billing date based on billing pattern.

    Port of RPC calculate_next_billing_date function.

    Args:
        billing_day_of_month: Day of month (1-31) for monthly billing, or None
        billing_weekday: Weekday (0=Monday, 6=Sunday) for weekly billing, or None
        billing_cycle: 'monthly', 'quarterly', 'semi-annually', 'annually', or 'weekly'
        reference_date: The policy effective date or last billing date
        today: Override for current date (for testing)

    Returns:
        Next billing date, or None if can't be calculated
    """
    if today is None:
        today = date.today()

    if not reference_date:
        return None

    # Determine billing interval in months
    interval_map = {
        'monthly': 1,
        'quarterly': 3,
        'semi-annually': 6,
        'annually': 12,
        'weekly': 0,  # Special case handled separately
    }
    billing_cycle_lower = (billing_cycle or 'monthly').lower()
    interval_months = interval_map.get(billing_cycle_lower, 1)

    # Weekly billing uses weekday
    if billing_cycle_lower == 'weekly' and billing_weekday is not None:
        # Find next occurrence of billing_weekday after today
        days_ahead = billing_weekday - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        return today + timedelta(days=days_ahead)

    # Monthly/Quarterly/etc. billing uses day of month
    if billing_day_of_month is not None and 1 <= billing_day_of_month <= 31:
        # Start from reference_date and find next billing date after today
        current_year = reference_date.year
        current_month = reference_date.month

        # Generate billing dates until we find one after today
        for _ in range(60):  # Max 5 years of monthly billing
            # Clamp billing day to valid range for this month
            _, days_in_month = calendar.monthrange(current_year, current_month)
            actual_day = min(billing_day_of_month, days_in_month)

            try:
                billing_date = date(current_year, current_month, actual_day)
                if billing_date > today:
                    return billing_date
            except ValueError:
                pass  # Invalid date, skip

            # Move to next billing period
            current_month += interval_months
            while current_month > 12:
                current_month -= 12
                current_year += 1

        return None

    # Fallback: use reference_date day of month
    if reference_date:
        ref_day = reference_date.day
        current_year = reference_date.year
        current_month = reference_date.month

        for _ in range(60):
            _, days_in_month = calendar.monthrange(current_year, current_month)
            actual_day = min(ref_day, days_in_month)

            try:
                billing_date = date(current_year, current_month, actual_day)
                if billing_date > today:
                    return billing_date
            except ValueError:
                pass

            current_month += interval_months
            while current_month > 12:
                current_month -= 12
                current_year += 1

    return None


def get_billing_reminder_deals() -> list[dict]:
    """
    Get deals eligible for billing reminder messages (3 days before next billing).
    Translated from Supabase RPC: get_billing_reminder_deals_v2

    Supports two billing date calculation methods:
    1. billing_day_of_month: Uses specific day of month for billing
    2. policy_effective_date: Falls back to recurring dates from effective date

    Returns:
        List of deals with billing reminder info
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH active_deals AS (
                SELECT DISTINCT
                    d.id,
                    d.agent_id,
                    d.agency_id,
                    d.client_name,
                    d.client_phone,
                    d.billing_cycle,
                    d.policy_effective_date,
                    d.billing_day_of_month,
                    d.billing_weekday,
                    d.monthly_premium,
                    d.annual_premium,
                    COALESCE(sm.impact, 'neutral') AS status_impact,
                    u.first_name AS agent_first_name,
                    u.last_name AS agent_last_name,
                    COALESCE(u.subscription_tier, 'free') AS agent_subscription_tier,
                    a.name AS agency_name,
                    a.phone_number AS agency_phone,
                    a.messaging_enabled AS messaging_enabled
                FROM deals d
                INNER JOIN users u ON d.agent_id = u.id
                INNER JOIN agencies a ON d.agency_id = a.id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND LOWER(sm.raw_status) = LOWER(d.status)
                WHERE d.client_phone IS NOT NULL
                    AND d.billing_cycle IS NOT NULL
                    AND d.policy_effective_date IS NOT NULL
                    AND a.messaging_enabled = true
                    AND COALESCE(sm.impact, 'neutral') = 'positive'
            ),
            deals_with_calculated_dates AS (
                SELECT
                    ad.id AS deal_id,
                    ad.agent_id,
                    ad.agency_id,
                    ad.agent_first_name,
                    ad.agent_last_name,
                    ad.agent_subscription_tier,
                    ad.agency_name,
                    ad.agency_phone,
                    ad.messaging_enabled,
                    ad.client_name,
                    ad.client_phone,
                    ad.billing_cycle,
                    ad.billing_day_of_month,
                    ad.policy_effective_date,
                    ad.monthly_premium,
                    ad.annual_premium,
                    -- Method 1: Use billing_day_of_month if set
                    -- Calculate next occurrence of billing day in current or next month
                    CASE
                        WHEN ad.billing_day_of_month IS NOT NULL AND ad.billing_day_of_month BETWEEN 1 AND 31 THEN
                            CASE
                                -- If billing day hasn't passed this month, use this month
                                WHEN ad.billing_day_of_month > EXTRACT(DAY FROM CURRENT_DATE) THEN
                                    make_date(
                                        EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                        EXTRACT(MONTH FROM CURRENT_DATE)::int,
                                        LEAST(ad.billing_day_of_month,
                                            (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::date -
                                            DATE_TRUNC('month', CURRENT_DATE)::date + 1)::int
                                    )
                                -- Otherwise use next month (accounting for billing cycle)
                                ELSE
                                    make_date(
                                        EXTRACT(YEAR FROM CURRENT_DATE + (
                                            CASE ad.billing_cycle
                                                WHEN 'monthly' THEN INTERVAL '1 month'
                                                WHEN 'quarterly' THEN INTERVAL '3 months'
                                                WHEN 'semi-annually' THEN INTERVAL '6 months'
                                                WHEN 'annually' THEN INTERVAL '1 year'
                                                ELSE INTERVAL '1 month'
                                            END
                                        ))::int,
                                        EXTRACT(MONTH FROM CURRENT_DATE + (
                                            CASE ad.billing_cycle
                                                WHEN 'monthly' THEN INTERVAL '1 month'
                                                WHEN 'quarterly' THEN INTERVAL '3 months'
                                                WHEN 'semi-annually' THEN INTERVAL '6 months'
                                                WHEN 'annually' THEN INTERVAL '1 year'
                                                ELSE INTERVAL '1 month'
                                            END
                                        ))::int,
                                        LEAST(ad.billing_day_of_month, 28)::int
                                    )
                            END
                        ELSE NULL
                    END AS billing_day_calculated_date,
                    -- Method 2: Fallback to generate_series from policy_effective_date
                    (
                        SELECT dt::date
                        FROM generate_series(
                            ad.policy_effective_date::timestamp,
                            (ad.policy_effective_date +
                                CASE ad.billing_cycle
                                    WHEN 'monthly' THEN interval '1 month'
                                    WHEN 'quarterly' THEN interval '3 months'
                                    WHEN 'semi-annually' THEN interval '6 months'
                                    WHEN 'annually' THEN interval '1 year'
                                    ELSE interval '1 month'
                                END * 50)::timestamp,
                            CASE ad.billing_cycle
                                WHEN 'monthly' THEN interval '1 month'
                                WHEN 'quarterly' THEN interval '3 months'
                                WHEN 'semi-annually' THEN interval '6 months'
                                WHEN 'annually' THEN interval '1 year'
                                ELSE interval '1 month'
                            END
                        ) AS dt
                        WHERE dt::date > CURRENT_DATE
                        LIMIT 1
                    ) AS fallback_billing_date
                FROM active_deals ad
            )
            SELECT
                dwcd.deal_id,
                dwcd.agent_id,
                dwcd.agent_first_name,
                dwcd.agent_last_name,
                dwcd.agent_subscription_tier,
                dwcd.agency_id,
                dwcd.agency_name,
                dwcd.agency_phone,
                dwcd.messaging_enabled,
                dwcd.client_name,
                dwcd.client_phone,
                dwcd.billing_cycle,
                -- Prefer billing_day calculation, fall back to effective date method
                COALESCE(dwcd.billing_day_calculated_date, dwcd.fallback_billing_date) as next_billing_date,
                dwcd.policy_effective_date,
                dwcd.monthly_premium,
                dwcd.annual_premium
            FROM deals_with_calculated_dates dwcd
            WHERE COALESCE(dwcd.billing_day_calculated_date, dwcd.fallback_billing_date) = (CURRENT_DATE + INTERVAL '3 days')::date
                OR dwcd.policy_effective_date = (CURRENT_DATE + INTERVAL '3 days')::date
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'billing_cycle': row_dict['billing_cycle'],
            'next_billing_date': row_dict['next_billing_date'].isoformat() if row_dict['next_billing_date'] else None,
            'policy_effective_date': row_dict['policy_effective_date'].isoformat() if row_dict['policy_effective_date'] else None,
            'monthly_premium': float(row_dict['monthly_premium'] or 0),
            'annual_premium': float(row_dict['annual_premium'] or 0),
        })

    return result


def get_birthday_message_deals() -> list[dict]:
    """
    Get deals with clients having birthdays today.
    Translated from Supabase RPC: get_birthday_message_deals

    Returns:
        List of deals for birthday messages
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT
                d.id as deal_id,
                d.client_name,
                d.client_phone,
                d.date_of_birth,
                d.agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                d.agency_id,
                a.name as agency_name,
                a.phone_number as agency_phone,
                a.messaging_enabled
            FROM deals d
            INNER JOIN users u ON d.agent_id = u.id
            INNER JOIN agencies a ON d.agency_id = a.id
            LEFT JOIN status_mapping sm
                ON sm.carrier_id = d.carrier_id
                AND LOWER(sm.raw_status) = LOWER(d.status)
            WHERE d.client_phone IS NOT NULL
                AND d.date_of_birth IS NOT NULL
                AND a.messaging_enabled = true
                AND COALESCE(sm.impact, 'neutral') = 'positive'
                AND EXTRACT(MONTH FROM d.date_of_birth) = EXTRACT(MONTH FROM CURRENT_DATE)
                AND EXTRACT(DAY FROM d.date_of_birth) = EXTRACT(DAY FROM CURRENT_DATE)
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'date_of_birth': row_dict['date_of_birth'].isoformat() if row_dict['date_of_birth'] else None,
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
        })

    return result


def get_holiday_message_deals(holiday_name: str) -> list[dict]:
    """
    Get deals eligible for holiday messages.
    Translated from Supabase RPC: get_holiday_message_deals

    Args:
        holiday_name: Name of the holiday

    Returns:
        List of deals for holiday messages (deduplicated by client phone)
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT ON (d.client_phone)
                d.id as deal_id,
                d.client_name,
                d.client_phone,
                d.agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                d.agency_id,
                a.name as agency_name,
                a.phone_number as agency_phone,
                a.messaging_enabled,
                %s as holiday_name
            FROM deals d
            INNER JOIN users u ON d.agent_id = u.id
            INNER JOIN agencies a ON d.agency_id = a.id
            LEFT JOIN status_mapping sm
                ON sm.carrier_id = d.carrier_id
                AND LOWER(sm.raw_status) = LOWER(d.status)
            WHERE d.client_phone IS NOT NULL
                AND a.messaging_enabled = true
                AND COALESCE(sm.impact, 'neutral') = 'positive'
            ORDER BY d.client_phone, d.created_at DESC
        """, [holiday_name])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
            'holiday_name': row_dict['holiday_name'],
        })

    return result


def get_lapse_reminder_deals() -> list[dict]:
    """
    Get deals eligible for lapse reminder messages.
    Translated from Supabase RPC: get_lapse_reminder_deals

    Returns:
        List of deals in lapse pending status
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT
                d.id as deal_id,
                d.client_name,
                d.client_phone,
                d.agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                u.phone_number as agent_phone,
                COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                d.agency_id,
                a.name as agency_name,
                a.phone_number as agency_phone,
                a.messaging_enabled
            FROM deals d
            INNER JOIN users u ON d.agent_id = u.id
            INNER JOIN agencies a ON d.agency_id = a.id
            INNER JOIN status_mapping sm ON d.carrier_id = sm.carrier_id AND d.status = sm.raw_status
            WHERE d.client_phone IS NOT NULL
                AND a.messaging_enabled = true
                AND (d.status_standardized IS NULL OR d.status_standardized != 'lapse_notified')
                AND LOWER(sm.status_standardized) LIKE '%lapse pending%'
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_phone': row_dict['agent_phone'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
        })

    return result


def get_needs_more_info_deals() -> list[dict]:
    """
    Get deals where application needs more info.
    Translated from Supabase RPC: get_needs_more_info_deals

    Returns:
        List of deals with incomplete applications
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT
                d.id as deal_id,
                d.client_name,
                d.agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                d.agency_id,
                a.name as agency_name,
                a.messaging_enabled
            FROM deals d
            INNER JOIN users u ON d.agent_id = u.id
            INNER JOIN agencies a ON d.agency_id = a.id
            INNER JOIN status_mapping sm ON d.carrier_id = sm.carrier_id AND d.status = sm.raw_status
            WHERE a.messaging_enabled = true
                AND (d.status_standardized IS NULL OR d.status_standardized != 'needs_more_info_notified')
                AND (
                    LOWER(sm.status_standardized) LIKE '%application in progress%'
                    OR LOWER(sm.status_standardized) LIKE '%application incomplete%'
                )
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'messaging_enabled': row_dict['messaging_enabled'],
        })

    return result


def get_policy_packet_checkup_deals() -> list[dict]:
    """
    Get deals for policy packet checkup (14 days after effective).
    Translated from Supabase RPC: get_policy_packet_checkup_deals

    Returns:
        List of deals 14 days after policy effective date
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT
                d.id as deal_id,
                d.client_name,
                d.client_phone,
                d.policy_effective_date,
                d.agent_id,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                d.agency_id,
                a.name as agency_name,
                a.phone_number as agency_phone,
                a.messaging_enabled
            FROM deals d
            INNER JOIN users u ON d.agent_id = u.id
            INNER JOIN agencies a ON d.agency_id = a.id
            LEFT JOIN status_mapping sm
                ON sm.carrier_id = d.carrier_id
                AND LOWER(sm.raw_status) = LOWER(d.status)
            WHERE d.client_phone IS NOT NULL
                AND d.policy_effective_date IS NOT NULL
                AND a.messaging_enabled = true
                AND COALESCE(sm.impact, 'neutral') = 'positive'
                AND d.policy_effective_date = (CURRENT_DATE - INTERVAL '14 days')::date
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'policy_effective_date': row_dict['policy_effective_date'].isoformat() if row_dict['policy_effective_date'] else None,
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
        })

    return result


def get_quarterly_checkin_deals() -> list[dict]:
    """
    Get deals for quarterly check-in (every 90 days since effective).
    Translated from Supabase RPC: get_quarterly_checkin_deals

    Returns:
        List of deals on quarterly anniversaries
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH eligible_deals AS (
                SELECT DISTINCT
                    d.id,
                    d.client_name,
                    d.client_phone,
                    d.policy_effective_date,
                    d.agent_id,
                    d.agency_id,
                    u.first_name as agent_first_name,
                    u.last_name as agent_last_name,
                    u.phone_number as agent_phone,
                    COALESCE(u.subscription_tier, 'free') as agent_subscription_tier,
                    a.name as agency_name,
                    a.phone_number as agency_phone,
                    a.messaging_enabled,
                    (CURRENT_DATE - d.policy_effective_date) as days_since_effective
                FROM deals d
                INNER JOIN users u ON d.agent_id = u.id
                INNER JOIN agencies a ON d.agency_id = a.id
                LEFT JOIN status_mapping sm
                    ON sm.carrier_id = d.carrier_id
                    AND LOWER(sm.raw_status) = LOWER(d.status)
                WHERE d.client_phone IS NOT NULL
                    AND d.policy_effective_date IS NOT NULL
                    AND a.messaging_enabled = true
                    AND COALESCE(sm.impact, 'neutral') = 'positive'
                    AND (CURRENT_DATE - d.policy_effective_date) >= 90
            ),
            deals_with_quarterly_check AS (
                SELECT
                    ed.*,
                    MOD(ed.days_since_effective, 90) as remainder
                FROM eligible_deals ed
            )
            SELECT DISTINCT ON (dwq.client_phone)
                dwq.id as deal_id,
                dwq.client_name,
                dwq.client_phone,
                dwq.policy_effective_date,
                dwq.agent_id,
                dwq.agent_first_name,
                dwq.agent_last_name,
                dwq.agent_phone,
                dwq.agent_subscription_tier,
                dwq.agency_id,
                dwq.agency_name,
                dwq.agency_phone,
                dwq.messaging_enabled,
                dwq.days_since_effective
            FROM deals_with_quarterly_check dwq
            WHERE dwq.remainder = 0
            ORDER BY dwq.client_phone, dwq.policy_effective_date ASC
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result.append({
            'deal_id': str(row_dict['deal_id']),
            'client_name': row_dict['client_name'],
            'client_phone': row_dict['client_phone'],
            'policy_effective_date': row_dict['policy_effective_date'].isoformat() if row_dict['policy_effective_date'] else None,
            'agent_id': str(row_dict['agent_id']),
            'agent_first_name': row_dict['agent_first_name'],
            'agent_last_name': row_dict['agent_last_name'],
            'agent_phone': row_dict['agent_phone'],
            'agent_subscription_tier': row_dict['agent_subscription_tier'],
            'agency_id': str(row_dict['agency_id']),
            'agency_name': row_dict['agency_name'],
            'agency_phone': row_dict['agency_phone'],
            'messaging_enabled': row_dict['messaging_enabled'],
            'days_since_effective': row_dict['days_since_effective'],
        })

    return result
