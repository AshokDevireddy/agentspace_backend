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
class DealCreateInput:
    """Input data for creating a deal."""
    agency_id: UUID
    agent_id: UUID
    client_id: UUID | None = None
    carrier_id: UUID | None = None
    product_id: UUID | None = None
    policy_number: str | None = None
    status: str | None = None
    status_standardized: str | None = None
    annual_premium: Decimal | None = None
    monthly_premium: Decimal | None = None
    policy_effective_date: str | None = None
    submission_date: str | None = None
    billing_cycle: str | None = None
    lead_source: str | None = None


@dataclass
class DealUpdateInput:
    """Input data for updating a deal."""
    client_id: UUID | None = None
    carrier_id: UUID | None = None
    product_id: UUID | None = None
    policy_number: str | None = None
    status: str | None = None
    status_standardized: str | None = None
    annual_premium: Decimal | None = None
    monthly_premium: Decimal | None = None
    policy_effective_date: str | None = None
    submission_date: str | None = None
    billing_cycle: str | None = None
    lead_source: str | None = None


def create_deal(
    user: AuthenticatedUser,
    data: DealCreateInput,
) -> dict:
    """
    Create a new deal and capture hierarchy snapshot.

    Args:
        user: The authenticated user creating the deal
        data: Deal creation data

    Returns:
        Created deal dict

    Raises:
        ValueError: If validation fails
        Exception: On database errors
    """
    deal_id = uuid.uuid4()

    with transaction.atomic(), connection.cursor() as cursor:
        # Insert the deal
        cursor.execute("""
                INSERT INTO public.deals (
                    id, agency_id, agent_id, client_id, carrier_id, product_id,
                    policy_number, status, status_standardized,
                    annual_premium, monthly_premium,
                    policy_effective_date, submission_date,
                    billing_cycle, lead_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at, updated_at
            """, [
            str(deal_id),
            str(data.agency_id),
            str(data.agent_id),
            str(data.client_id) if data.client_id else None,
            str(data.carrier_id) if data.carrier_id else None,
            str(data.product_id) if data.product_id else None,
            data.policy_number,
            data.status,
            data.status_standardized or 'pending',
            float(data.annual_premium) if data.annual_premium else None,
            float(data.monthly_premium) if data.monthly_premium else None,
            data.policy_effective_date,
            data.submission_date,
            data.billing_cycle,
            data.lead_source,
        ])
        deal_row = cursor.fetchone()

        if not deal_row:
            raise Exception("Failed to create deal")

        # Capture hierarchy snapshot
        _capture_hierarchy_snapshot(cursor, deal_id, data.agent_id, data.product_id)

    # Fetch and return the complete deal
    result = get_deal_by_id(deal_id, user)
    return result if result is not None else {}


def update_deal(
    deal_id: UUID,
    user: AuthenticatedUser,
    data: DealUpdateInput,
) -> dict | None:
    """
    Update an existing deal.

    Args:
        deal_id: The deal ID to update
        user: The authenticated user
        data: Update data

    Returns:
        Updated deal dict or None if not found

    Raises:
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

    if data.lead_source is not None:
        updates.append("lead_source = %s")
        params.append(data.lead_source)

    if not updates:
        # Nothing to update, just return current deal
        return get_deal_by_id(deal_id, user)

    # Add updated_at
    updates.append("updated_at = NOW()")

    # Build and execute query
    update_sql = ", ".join(updates)
    params.extend([str(deal_id), str(user.agency_id)])

    with connection.cursor() as cursor:
        cursor.execute(f"""
            UPDATE public.deals
            SET {update_sql}
            WHERE id = %s AND agency_id = %s
            RETURNING id
        """, params)

        row = cursor.fetchone()
        if not row:
            return None

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
