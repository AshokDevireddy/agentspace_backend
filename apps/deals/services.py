"""
Deals Services (P1-011, P1-012, P1-013, P1-014)

Business logic for deal creation, updates, and status transitions.
Handles DealHierarchySnapshot capture on deal creation.
"""
import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.db import connection, transaction

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)


@dataclass
class BeneficiaryInput:
    """Input data for a beneficiary."""
    name: str | None = None
    relationship: str | None = None


@dataclass
class DealCreateInput:
    """Input data for creating a deal."""
    agency_id: UUID
    agent_id: UUID
    client_id: UUID | None = None
    carrier_id: UUID | None = None
    product_id: UUID | None = None
    policy_number: str | None = None
    application_number: str | None = None
    status: str | None = None
    status_standardized: str | None = None
    annual_premium: Decimal | None = None
    monthly_premium: Decimal | None = None
    policy_effective_date: str | None = None
    submission_date: str | None = None
    billing_cycle: str | None = None
    billing_day_of_month: str | None = None  # '1st', '2nd', '3rd', '4th'
    billing_weekday: str | None = None  # 'Monday', 'Tuesday', etc.
    lead_source: str | None = None
    # Client fields (for when client_id is not provided)
    client_name: str | None = None
    client_email: str | None = None
    client_phone: str | None = None
    client_address: str | None = None
    date_of_birth: str | None = None
    ssn_last_4: str | None = None
    ssn_benefit: bool | None = None
    notes: str | None = None
    beneficiaries: list[BeneficiaryInput] | None = None


@dataclass
class DealUpdateInput:
    """Input data for updating a deal."""
    client_id: UUID | None = None
    carrier_id: UUID | None = None
    product_id: UUID | None = None
    policy_number: str | None = None
    application_number: str | None = None
    status: str | None = None
    status_standardized: str | None = None
    annual_premium: Decimal | None = None
    monthly_premium: Decimal | None = None
    policy_effective_date: str | None = None
    submission_date: str | None = None
    billing_cycle: str | None = None
    billing_day_of_month: str | None = None  # '1st', '2nd', '3rd', '4th'
    billing_weekday: str | None = None  # 'Monday', 'Tuesday', etc.
    lead_source: str | None = None
    # Client fields
    client_name: str | None = None
    client_email: str | None = None
    client_phone: str | None = None
    client_address: str | None = None
    date_of_birth: str | None = None
    ssn_last_4: str | None = None
    ssn_benefit: bool | None = None
    notes: str | None = None
    beneficiaries: list[BeneficiaryInput] | None = None


class DealValidationError(Exception):
    """Custom exception for deal validation errors."""
    def __init__(self, message: str, code: str = 'validation_error', details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class DealLimitReachedError(DealValidationError):
    """Exception raised when free tier deal limit is reached."""
    def __init__(self):
        super().__init__(
            message='You have reached the maximum of 10 deals on the Free plan. Please upgrade your subscription to create more deals.',
            code='limit_reached',
        )


class PhoneAlreadyExistsError(DealValidationError):
    """Exception raised when phone number already exists for another deal."""
    def __init__(self, phone: str, existing_deal: dict):
        super().__init__(
            message=f"Phone number {phone} already exists for another deal in your agency ({existing_deal['client_name']}, Policy: {existing_deal['policy_number'] or 'N/A'}). Each deal must have a unique phone number within the agency.",
            code='phone_exists',
            details={'existing_deal_id': existing_deal['id']},
        )


class UplinePositionError(DealValidationError):
    """Exception raised when agents in upline don't have positions."""
    def __init__(self, agents_without_positions: list[dict]):
        names = ', '.join(a['name'] for a in agents_without_positions)
        super().__init__(
            message=f"Cannot create deal: The following agents in the upline hierarchy do not have positions assigned: {names}. All agents in the upline must have positions set before deals can be created.",
            code='missing_positions',
            details={'agents_without_positions': agents_without_positions},
        )


class CommissionMappingError(DealValidationError):
    """Exception raised when commission mappings are missing."""
    def __init__(self, positions_without_commissions: list[str]):
        super().__init__(
            message='Cannot create deal: Commission percentages are not configured for some positions in the upline hierarchy. Please contact your administrator to set up commission mappings for this product.',
            code='missing_commissions',
            details={'positions_without_commissions': positions_without_commissions},
        )


def normalize_phone_for_storage(phone: str | None) -> str | None:
    """Normalize phone number to E.164 format for storage."""
    if not phone:
        return None
    # Remove all non-digit characters
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f'+1{digits}'
    if len(digits) == 11 and digits.startswith('1'):
        return f'+{digits}'
    # Return as-is if we can't normalize
    return phone


def _check_subscription_limit(cursor, agent_id: UUID) -> None:
    """
    Check if agent has reached their deal creation limit on free tier.

    Raises:
        DealLimitReachedError: If limit is reached
    """
    cursor.execute("""
        SELECT subscription_tier, deals_created_count
        FROM public.users
        WHERE id = %s
    """, [str(agent_id)])
    row = cursor.fetchone()

    if row:
        subscription_tier = row[0] or 'free'
        deals_created = row[1] or 0
        if subscription_tier == 'free' and deals_created >= 10:
            raise DealLimitReachedError()


def _check_phone_uniqueness(cursor, phone: str, agency_id: UUID, exclude_deal_id: UUID | None = None) -> None:
    """
    Check if phone number already exists for another deal in the agency.

    Raises:
        PhoneAlreadyExistsError: If phone exists for another deal
    """
    normalized_phone = normalize_phone_for_storage(phone)
    if not normalized_phone:
        return

    query = """
        SELECT id, client_name, policy_number
        FROM public.deals
        WHERE client_phone = %s AND agency_id = %s
    """
    params: list[Any] = [normalized_phone, str(agency_id)]

    if exclude_deal_id:
        query += " AND id != %s"
        params.append(str(exclude_deal_id))

    query += " LIMIT 1"
    cursor.execute(query, params)
    row = cursor.fetchone()

    if row:
        raise PhoneAlreadyExistsError(
            phone=phone,
            existing_deal={
                'id': str(row[0]),
                'client_name': row[1],
                'policy_number': row[2],
            },
        )


def _validate_upline_positions(cursor, agent_id: UUID, product_id: UUID | None) -> None:
    """
    Validate that all agents in the upline have positions and commission mappings.

    Raises:
        UplinePositionError: If agents are missing positions
        CommissionMappingError: If commission mappings are missing
    """
    # Get upline chain with positions
    cursor.execute("""
        WITH RECURSIVE upline_chain AS (
            SELECT
                id,
                upline_id,
                position_id,
                first_name,
                last_name,
                0 as hierarchy_level
            FROM public.users
            WHERE id = %s

            UNION ALL

            SELECT
                u.id,
                u.upline_id,
                u.position_id,
                u.first_name,
                u.last_name,
                uc.hierarchy_level + 1
            FROM public.users u
            JOIN upline_chain uc ON u.id = uc.upline_id
            WHERE uc.hierarchy_level < 20
        )
        SELECT id, position_id, first_name, last_name
        FROM upline_chain
    """, [str(agent_id)])

    rows = cursor.fetchall()
    if not rows:
        raise DealValidationError(
            message='No upline hierarchy found for this agent. Cannot create deal.',
            code='no_upline',
        )

    # Check for agents without positions
    agents_without_positions = []
    position_ids = set()
    for row in rows:
        agent_uuid, position_id, first_name, last_name = row
        if not position_id:
            agents_without_positions.append({
                'id': str(agent_uuid),
                'name': f'{first_name or ""} {last_name or ""}'.strip(),
            })
        else:
            position_ids.add(str(position_id))

    if agents_without_positions:
        raise UplinePositionError(agents_without_positions)

    # Check commission mappings if product_id is provided
    if product_id and position_ids:
        cursor.execute("""
            SELECT position_id
            FROM public.position_product_commissions
            WHERE product_id = %s AND position_id = ANY(%s)
        """, [str(product_id), list(position_ids)])

        positions_with_commissions = {str(row[0]) for row in cursor.fetchall()}
        positions_without_commissions = position_ids - positions_with_commissions

        if positions_without_commissions:
            raise CommissionMappingError(list(positions_without_commissions))


def _upsert_beneficiaries(cursor, deal_id: UUID, agency_id: UUID, beneficiaries: list[BeneficiaryInput] | None) -> None:
    """
    Upsert beneficiaries for a deal (delete existing and insert new).
    """
    # Delete existing beneficiaries
    cursor.execute("""
        DELETE FROM public.beneficiaries WHERE deal_id = %s
    """, [str(deal_id)])

    if not beneficiaries:
        return

    # Normalize and insert beneficiaries
    for beneficiary in beneficiaries:
        raw_name = (beneficiary.name or '').strip()
        if not raw_name:
            continue

        # Split name into first and last
        first_space = raw_name.find(' ')
        if first_space != -1:
            first_name = raw_name[:first_space].strip()
            last_name = raw_name[first_space:].strip() or None
        else:
            first_name = raw_name
            last_name = None

        if not first_name:
            continue

        relationship = (beneficiary.relationship or '').strip() or None
        beneficiary_id = uuid.uuid4()

        cursor.execute("""
            INSERT INTO public.beneficiaries (id, deal_id, agency_id, first_name, last_name, relationship)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [str(beneficiary_id), str(deal_id), str(agency_id), first_name, last_name, relationship])


def _increment_deals_created_count(cursor, agent_id: UUID) -> None:
    """Increment the deals_created_count for an agent."""
    cursor.execute("""
        UPDATE public.users
        SET deals_created_count = COALESCE(deals_created_count, 0) + 1
        WHERE id = %s
    """, [str(agent_id)])


def create_deal(
    user: AuthenticatedUser,
    data: DealCreateInput,
) -> dict:
    """
    Create a new deal and capture hierarchy snapshot.

    Includes validations for:
    - Subscription tier limits (free users limited to 10 deals)
    - Phone number uniqueness within agency
    - Upline position assignments
    - Commission mapping existence

    Args:
        user: The authenticated user creating the deal
        data: Deal creation data

    Returns:
        Created deal dict with 'operation' key set to 'created'

    Raises:
        DealLimitReachedError: If free tier limit reached
        PhoneAlreadyExistsError: If phone already exists
        UplinePositionError: If upline agents missing positions
        CommissionMappingError: If commission mappings missing
        Exception: On database errors
    """
    deal_id = uuid.uuid4()
    normalized_phone = normalize_phone_for_storage(data.client_phone)

    with transaction.atomic(), connection.cursor() as cursor:
        # Step 1: Check subscription limits for free users
        _check_subscription_limit(cursor, data.agent_id)

        # Step 2: Check phone uniqueness within agency
        if normalized_phone:
            _check_phone_uniqueness(cursor, data.client_phone or '', data.agency_id)

        # Step 3: Validate upline positions and commission mappings
        if data.product_id:
            _validate_upline_positions(cursor, data.agent_id, data.product_id)

        # Step 4: Insert the deal with all fields
        cursor.execute("""
            INSERT INTO public.deals (
                id, agency_id, agent_id, client_id, carrier_id, product_id,
                policy_number, application_number, status, status_standardized,
                annual_premium, monthly_premium,
                policy_effective_date, submission_date,
                billing_cycle, billing_day_of_month, billing_weekday,
                lead_source, client_name, client_email, client_phone,
                client_address, date_of_birth, ssn_last_4, ssn_benefit, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at, updated_at
        """, [
            str(deal_id),
            str(data.agency_id),
            str(data.agent_id),
            str(data.client_id) if data.client_id else None,
            str(data.carrier_id) if data.carrier_id else None,
            str(data.product_id) if data.product_id else None,
            data.policy_number,
            data.application_number,
            data.status,
            data.status_standardized or 'pending',
            float(data.annual_premium) if data.annual_premium else None,
            float(data.monthly_premium) if data.monthly_premium else None,
            data.policy_effective_date,
            data.submission_date,
            data.billing_cycle,
            data.billing_day_of_month,
            data.billing_weekday,
            data.lead_source,
            data.client_name,
            data.client_email,
            normalized_phone,
            data.client_address,
            data.date_of_birth,
            data.ssn_last_4,
            data.ssn_benefit,
            data.notes,
        ])
        deal_row = cursor.fetchone()

        if not deal_row:
            raise Exception("Failed to create deal")

        # Step 5: Insert beneficiaries
        _upsert_beneficiaries(cursor, deal_id, data.agency_id, data.beneficiaries)

        # Step 6: Increment deals_created_count for the agent
        _increment_deals_created_count(cursor, data.agent_id)

        # Step 7: Capture hierarchy snapshot
        _capture_hierarchy_snapshot(cursor, deal_id, data.agent_id, data.product_id)

    # Fetch and return the complete deal
    result = get_deal_by_id(deal_id, user)
    if result:
        result['operation'] = 'created'
        result['message'] = 'Deal created successfully'
    return result if result is not None else {}


def update_deal(
    deal_id: UUID,
    user: AuthenticatedUser,
    data: DealUpdateInput,
) -> dict | None:
    """
    Update an existing deal.

    Includes phone uniqueness validation when client_phone is being updated.

    Args:
        deal_id: The deal ID to update
        user: The authenticated user
        data: Update data

    Returns:
        Updated deal dict or None if not found

    Raises:
        PhoneAlreadyExistsError: If phone already exists for another deal
        ValueError: If validation fails
    """
    # Build dynamic UPDATE query based on provided fields
    updates = []
    params: list[Any] = []

    if data.client_id is not None:
        updates.append("client_id = %s")
        params.append(str(data.client_id))

    if data.carrier_id is not None:
        updates.append("carrier_id = %s")
        params.append(str(data.carrier_id))

    if data.product_id is not None:
        updates.append("product_id = %s")
        params.append(str(data.product_id))

    if data.policy_number is not None:
        updates.append("policy_number = %s")
        params.append(data.policy_number)

    if data.application_number is not None:
        updates.append("application_number = %s")
        params.append(data.application_number)

    if data.status is not None:
        updates.append("status = %s")
        params.append(data.status)

    if data.status_standardized is not None:
        updates.append("status_standardized = %s")
        params.append(data.status_standardized)

    if data.annual_premium is not None:
        updates.append("annual_premium = %s")
        params.append(float(data.annual_premium))

    if data.monthly_premium is not None:
        updates.append("monthly_premium = %s")
        params.append(float(data.monthly_premium))

    if data.policy_effective_date is not None:
        updates.append("policy_effective_date = %s")
        params.append(data.policy_effective_date)

    if data.submission_date is not None:
        updates.append("submission_date = %s")
        params.append(data.submission_date)

    if data.billing_cycle is not None:
        updates.append("billing_cycle = %s")
        params.append(data.billing_cycle)

    if data.billing_day_of_month is not None:
        updates.append("billing_day_of_month = %s")
        params.append(data.billing_day_of_month)

    if data.billing_weekday is not None:
        updates.append("billing_weekday = %s")
        params.append(data.billing_weekday)

    if data.lead_source is not None:
        updates.append("lead_source = %s")
        params.append(data.lead_source)

    if data.client_name is not None:
        updates.append("client_name = %s")
        params.append(data.client_name)

    if data.client_email is not None:
        updates.append("client_email = %s")
        params.append(data.client_email)

    if data.client_address is not None:
        updates.append("client_address = %s")
        params.append(data.client_address)

    if data.date_of_birth is not None:
        updates.append("date_of_birth = %s")
        params.append(data.date_of_birth)

    if data.ssn_last_4 is not None:
        updates.append("ssn_last_4 = %s")
        params.append(data.ssn_last_4)

    if data.ssn_benefit is not None:
        updates.append("ssn_benefit = %s")
        params.append(data.ssn_benefit)

    if data.notes is not None:
        updates.append("notes = %s")
        params.append(data.notes)

    # Handle client_phone with uniqueness validation
    normalized_phone = None
    if data.client_phone is not None:
        normalized_phone = normalize_phone_for_storage(data.client_phone)
        updates.append("client_phone = %s")
        params.append(normalized_phone)

    if not updates:
        # Nothing to update, just return current deal
        return get_deal_by_id(deal_id, user)

    with transaction.atomic(), connection.cursor() as cursor:
        # Validate phone uniqueness if phone is being updated
        if data.client_phone is not None and normalized_phone:
            _check_phone_uniqueness(cursor, data.client_phone, user.agency_id, exclude_deal_id=deal_id)

        # Add updated_at
        updates.append("updated_at = NOW()")

        # Build and execute query
        update_sql = ", ".join(updates)
        params.extend([str(deal_id), str(user.agency_id)])

        cursor.execute(f"""
            UPDATE public.deals
            SET {update_sql}
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, params)

        row = cursor.fetchone()
        if not row:
            return None

        # Update beneficiaries if provided
        if data.beneficiaries is not None:
            _upsert_beneficiaries(cursor, deal_id, user.agency_id, data.beneficiaries)

        # Update conversation phone if client_phone changed
        if normalized_phone:
            cursor.execute("""
                UPDATE public.conversations
                SET client_phone = %s
                WHERE deal_id = %s AND is_active = true
            """, [normalized_phone, str(deal_id)])

    return get_deal_by_id(deal_id, user)


def update_deal_status(
    deal_id: UUID,
    user: AuthenticatedUser,
    new_status: str,
    new_status_standardized: str | None = None,
) -> dict | None:
    """
    Update deal status with validation.

    Args:
        deal_id: The deal ID
        user: The authenticated user
        new_status: New raw status value
        new_status_standardized: Optional standardized status

    Returns:
        Updated deal dict or None if not found
    """
    valid_standardized = ['active', 'pending', 'cancelled', 'lapsed', 'terminated']

    if new_status_standardized and new_status_standardized not in valid_standardized:
        raise ValueError(f"Invalid status_standardized. Must be one of: {valid_standardized}")

    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.deals
            SET
                status = %s,
                status_standardized = COALESCE(%s, status_standardized),
                updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [new_status, new_status_standardized, str(deal_id), str(user.agency_id)])

        row = cursor.fetchone()
        if not row:
            return None

    return get_deal_by_id(deal_id, user)


def update_deal_status_standardized(
    deal_id: UUID,
    user: AuthenticatedUser,
    new_status_standardized: str,
) -> bool:
    """
    Update only the status_standardized field of a deal.

    This is used by cron jobs to update notification status without
    requiring the raw status field. Allows any status_standardized value.

    Args:
        deal_id: The deal ID
        user: The authenticated user
        new_status_standardized: New standardized status value

    Returns:
        True if updated, False if not found or not accessible
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE public.deals
            SET
                status_standardized = %s,
                updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [new_status_standardized, str(deal_id), str(user.agency_id)])

        row = cursor.fetchone()
        return row is not None


def resolve_deal_notification(
    deal_id: UUID,
    user: AuthenticatedUser,
) -> dict | None:
    """
    Resolve a deal notification by setting status_standardized to NULL.

    Only allows resolving deals with 'lapse_notified' or 'needs_more_info_notified' status.

    Args:
        deal_id: The deal ID
        user: The authenticated user

    Returns:
        Dict with success info or None if deal not found/not accessible

    Raises:
        DealValidationError: If deal does not have a notified status
    """
    with connection.cursor() as cursor:
        # First check if the deal exists and has a notified status
        cursor.execute("""
            SELECT id, status_standardized
            FROM public.deals
            WHERE id = %s AND agency_id = %s
        """, [str(deal_id), str(user.agency_id)])

        row = cursor.fetchone()
        if not row:
            return None

        current_status = row[1]

        # Only allow resolving notified statuses
        if current_status not in ('lapse_notified', 'needs_more_info_notified'):
            raise DealValidationError(
                message='Deal does not have a notified status to resolve',
                code='invalid_status',
                details={'current_status': current_status},
            )

        # Update status_standardized to NULL
        cursor.execute("""
            UPDATE public.deals
            SET
                status_standardized = NULL,
                updated_at = NOW()
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(deal_id), str(user.agency_id)])

        updated_row = cursor.fetchone()
        if not updated_row:
            return None

        return {
            'success': True,
            'deal_id': str(deal_id),
            'message': 'Notification resolved successfully',
        }


@transaction.atomic
def delete_deal(
    deal_id: UUID,
    user: AuthenticatedUser,
) -> bool:
    """
    Delete a deal (hard delete).

    Args:
        deal_id: The deal ID
        user: The authenticated user

    Returns:
        True if deleted, False if not found
    """
    with connection.cursor() as cursor:
        # First delete hierarchy snapshots
        cursor.execute("""
            DELETE FROM public.deal_hierarchy_snapshots
            WHERE deal_id = %s
        """, [str(deal_id)])

        # Then delete the deal
        cursor.execute("""
            DELETE FROM public.deals
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, [str(deal_id), str(user.agency_id)])

        row = cursor.fetchone()
        return row is not None


def get_deal_by_id(deal_id: UUID, user: AuthenticatedUser) -> dict | None:
    """
    Get deal details by ID.

    Args:
        deal_id: The deal ID
        user: The authenticated user

    Returns:
        Deal dict or None if not found
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                d.id,
                d.agency_id,
                d.agent_id,
                d.client_id,
                d.carrier_id,
                d.product_id,
                d.policy_number,
                d.status,
                d.status_standardized,
                d.annual_premium,
                d.monthly_premium,
                d.policy_effective_date,
                d.submission_date,
                d.billing_cycle,
                d.lead_source,
                d.created_at,
                d.updated_at,
                -- Agent info
                agent.first_name as agent_first_name,
                agent.last_name as agent_last_name,
                agent.email as agent_email,
                -- Client info
                client.first_name as client_first_name,
                client.last_name as client_last_name,
                client.email as client_email,
                client.phone as client_phone,
                -- Carrier info
                carrier.name as carrier_name,
                carrier.display_name as carrier_display_name,
                -- Product info
                product.name as product_name
            FROM public.deals d
            LEFT JOIN public.users agent ON agent.id = d.agent_id
            LEFT JOIN public.clients client ON client.id = d.client_id
            LEFT JOIN public.carriers carrier ON carrier.id = d.carrier_id
            LEFT JOIN public.products product ON product.id = d.product_id
            WHERE d.id = %s AND d.agency_id = %s
        """, [str(deal_id), str(user.agency_id)])

        row = cursor.fetchone()
        if not row:
            return None

        columns = [col[0] for col in cursor.description]
        deal = dict(zip(columns, row, strict=False))

        # Get hierarchy snapshots
        cursor.execute("""
            SELECT
                dhs.id,
                dhs.agent_id,
                dhs.position_id,
                dhs.hierarchy_level,
                dhs.commission_percentage,
                dhs.created_at,
                u.first_name as agent_first_name,
                u.last_name as agent_last_name,
                p.name as position_name
            FROM public.deal_hierarchy_snapshots dhs
            LEFT JOIN public.users u ON u.id = dhs.agent_id
            LEFT JOIN public.positions p ON p.id = dhs.position_id
            WHERE dhs.deal_id = %s
            ORDER BY dhs.hierarchy_level
        """, [str(deal_id)])

        snapshot_columns = [col[0] for col in cursor.description]
        snapshots = [dict(zip(snapshot_columns, r, strict=False)) for r in cursor.fetchall()]

    return {
        'id': str(deal['id']),
        'policy_number': deal['policy_number'],
        'status': deal['status'],
        'status_standardized': deal['status_standardized'],
        'annual_premium': float(deal['annual_premium']) if deal['annual_premium'] else None,
        'monthly_premium': float(deal['monthly_premium']) if deal['monthly_premium'] else None,
        'policy_effective_date': str(deal['policy_effective_date']) if deal['policy_effective_date'] else None,
        'submission_date': str(deal['submission_date']) if deal['submission_date'] else None,
        'billing_cycle': deal['billing_cycle'],
        'lead_source': deal['lead_source'],
        'created_at': deal['created_at'].isoformat() if deal['created_at'] else None,
        'updated_at': deal['updated_at'].isoformat() if deal['updated_at'] else None,
        'agent': {
            'id': str(deal['agent_id']) if deal['agent_id'] else None,
            'first_name': deal['agent_first_name'],
            'last_name': deal['agent_last_name'],
            'email': deal['agent_email'],
            'name': f"{deal['agent_first_name'] or ''} {deal['agent_last_name'] or ''}".strip(),
        } if deal['agent_id'] else None,
        'client': {
            'id': str(deal['client_id']) if deal['client_id'] else None,
            'first_name': deal['client_first_name'],
            'last_name': deal['client_last_name'],
            'email': deal['client_email'],
            'phone': deal['client_phone'],
            'name': f"{deal['client_first_name'] or ''} {deal['client_last_name'] or ''}".strip(),
        } if deal['client_id'] else None,
        'carrier': {
            'id': str(deal['carrier_id']) if deal['carrier_id'] else None,
            'name': deal['carrier_name'],
            'display_name': deal['carrier_display_name'],
        } if deal['carrier_id'] else None,
        'product': {
            'id': str(deal['product_id']) if deal['product_id'] else None,
            'name': deal['product_name'],
        } if deal['product_id'] else None,
        'agency_id': str(deal['agency_id']),
        'hierarchy_snapshots': [
            {
                'id': str(s['id']),
                'agent_id': str(s['agent_id']) if s['agent_id'] else None,
                'agent_name': f"{s['agent_first_name'] or ''} {s['agent_last_name'] or ''}".strip(),
                'position_id': str(s['position_id']) if s['position_id'] else None,
                'position_name': s['position_name'],
                'hierarchy_level': s['hierarchy_level'],
                'commission_percentage': float(s['commission_percentage']) if s['commission_percentage'] else None,
                'created_at': s['created_at'].isoformat() if s['created_at'] else None,
            }
            for s in snapshots
        ],
    }


def _capture_hierarchy_snapshot(
    cursor,
    deal_id: UUID,
    agent_id: UUID,
    product_id: UUID | None,
) -> None:
    """
    Capture the agent hierarchy at deal creation time.

    Creates DealHierarchySnapshot records for the writing agent and all
    upline agents with their commission percentages.

    Args:
        cursor: Database cursor
        deal_id: The deal ID
        agent_id: The writing agent ID
        product_id: The product ID (for commission lookup)
    """
    # Get the agent's upline chain
    cursor.execute("""
        WITH RECURSIVE upline_chain AS (
            SELECT
                id,
                upline_id,
                position_id,
                0 as hierarchy_level
            FROM public.users
            WHERE id = %s

            UNION ALL

            SELECT
                u.id,
                u.upline_id,
                u.position_id,
                uc.hierarchy_level + 1
            FROM public.users u
            JOIN upline_chain uc ON u.id = uc.upline_id
            WHERE uc.hierarchy_level < 20
        )
        SELECT
            uc.id as agent_id,
            uc.position_id,
            uc.hierarchy_level,
            ppc.commission_percentage
        FROM upline_chain uc
        LEFT JOIN public.position_product_commissions ppc
            ON ppc.position_id = uc.position_id
            AND ppc.product_id = %s
        ORDER BY uc.hierarchy_level
    """, [str(agent_id), str(product_id) if product_id else None])

    rows = cursor.fetchall()

    # Insert hierarchy snapshots
    for row in rows:
        snapshot_id = uuid.uuid4()
        cursor.execute("""
            INSERT INTO public.deal_hierarchy_snapshots (
                id, deal_id, agent_id, position_id, hierarchy_level, commission_percentage
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [
            str(snapshot_id),
            str(deal_id),
            str(row[0]) if row[0] else None,  # agent_id
            str(row[1]) if row[1] else None,  # position_id
            row[2],  # hierarchy_level
            float(row[3]) if row[3] else None,  # commission_percentage
        ])

    logger.info(f"Created {len(rows)} hierarchy snapshots for deal {deal_id}")
