"""
Carrier Selectors

Query functions for carrier data following the selector pattern.

Uses Django ORM with select_related and prefetch_related for efficient queries.
"""
from uuid import UUID

from django.db.models import Prefetch

import math

from apps.core.models import AgentCarrierNumber, Carrier, Product, StatusMapping


def get_active_carriers() -> list[dict]:
    """
    Get all active carriers ordered by display_name.

    Uses Django ORM.

    Returns:
        List of carrier dictionaries with id, name, display_name, is_active, created_at
    """
    carriers = (
        Carrier.objects  # type: ignore[attr-defined]
        .filter(is_active=True)
        .order_by('name')  # Fallback since display_name may not exist
    )

    return [
        {
            'id': c.id,
            'name': c.name,
            'display_name': getattr(c, 'display_name', None),
            'is_active': c.is_active,
            'created_at': c.created_at,
        }
        for c in carriers
    ]


def get_carriers_with_products_for_agency(agency_id: UUID) -> list[dict]:
    """
    Get carriers that have products associated with the given agency.

    Uses Django ORM with Prefetch to prevent N+1 queries.

    Args:
        agency_id: The agency UUID

    Returns:
        List of carrier dictionaries with nested products
    """
    # Prefetch active products for this agency
    products_prefetch = Prefetch(
        'products',
        queryset=Product.objects.filter(  # type: ignore[attr-defined]
            agency_id=agency_id,
            is_active=True
        ).order_by('name'),
        to_attr='agency_products'
    )

    # Get carriers with active products for this agency
    carriers = (
        Carrier.objects  # type: ignore[attr-defined]
        .filter(
            is_active=True,
            products__agency_id=agency_id,
            products__is_active=True,
        )
        .distinct()
        .prefetch_related(products_prefetch)
        .order_by('name')
    )

    return [
        {
            'id': c.id,
            'name': c.name,
            'display_name': getattr(c, 'display_name', None),
            'is_active': c.is_active,
            'products': [
                {
                    'id': p.id,
                    'name': p.name,
                    'product_code': getattr(p, 'product_code', None),
                    'is_active': p.is_active,
                    'created_at': p.created_at,
                }
                for p in c.agency_products
            ],
        }
        for c in carriers
    ]


def get_carrier_names() -> list[dict]:
    """
    Get carrier names for dropdowns (lightweight query).

    Uses Django ORM with only() to minimize data transfer.

    Returns:
        List of carrier dictionaries with id and name only
    """
    carriers = (
        Carrier.objects  # type: ignore[attr-defined]
        .filter(is_active=True)
        .only('id', 'name')
        .order_by('name')
    )

    return [
        {
            'id': c.id,
            'name': getattr(c, 'display_name', None) or c.name,
        }
        for c in carriers
    ]


def get_carriers_for_agency(agency_id: UUID) -> list[dict]:
    """
    Get carriers associated with an agency (P2-030).

    Returns carriers that have products or deals associated with the agency.
    This is a lightweight list for agency-specific carrier selection.

    Args:
        agency_id: The agency UUID

    Returns:
        List of carrier dictionaries with id, name, display_name, is_active
    """
    # Get carriers that have products for this agency
    carriers = (
        Carrier.objects  # type: ignore[attr-defined]
        .filter(
            is_active=True,
            products__agency_id=agency_id,
        )
        .distinct()
        .order_by('name')
    )

    return [
        {
            'id': str(c.id),
            'name': c.name,
            'display_name': c.display_name,
            'is_active': c.is_active,
        }
        for c in carriers
    ]


def get_carrier_by_id(carrier_id: UUID) -> dict | None:
    """
    Get a single carrier by ID.

    Uses Django ORM.

    Args:
        carrier_id: The carrier UUID

    Returns:
        Carrier dictionary or None if not found
    """
    carrier = Carrier.objects.filter(id=carrier_id).first()  # type: ignore[attr-defined]

    if not carrier:
        return None

    return {
        'id': carrier.id,
        'name': carrier.name,
        'display_name': getattr(carrier, 'display_name', None),
        'code': getattr(carrier, 'code', None),
        'is_active': carrier.is_active,
        'created_at': carrier.created_at,
        'updated_at': carrier.updated_at,
    }


# =============================================================================
# Status Mappings (P1-020)
# =============================================================================

def get_status_mappings(carrier_id: UUID | None = None) -> list[dict]:
    """
    Get status mappings (P1-020).

    Returns carrier-specific status codes mapped to standardized statuses.

    Args:
        carrier_id: Optional carrier UUID to filter by

    Returns:
        List of status mapping dictionaries
    """
    qs = StatusMapping.objects.select_related('carrier').order_by('carrier__name', 'raw_status')  # type: ignore[attr-defined]

    if carrier_id:
        qs = qs.filter(carrier_id=carrier_id)

    return [
        {
            'id': str(sm.id),
            'carrier_id': str(sm.carrier_id),
            'carrier_name': sm.carrier.name if sm.carrier else None,
            'raw_status': sm.raw_status,
            'standardized_status': sm.standardized_status,
            'impact': sm.impact,
            'created_at': sm.created_at.isoformat() if sm.created_at else None,
            'updated_at': sm.updated_at.isoformat() if sm.updated_at else None,
        }
        for sm in qs
    ]


def get_standardized_statuses() -> list[dict]:
    """
    Get list of standardized status values (P1-020).

    Returns static list of valid standardized statuses with their metadata.
    """
    from apps.core.constants import STANDARDIZED_STATUSES
    return STANDARDIZED_STATUSES


# =============================================================================
# Contracts (Agent Carrier Numbers)
# =============================================================================

def get_contracts_paginated(
    agency_id: UUID,
    page: int = 1,
    limit: int = 20,
) -> dict:
    """
    Get paginated agent carrier numbers (contracts) for an agency.

    Uses Django ORM with select_related to prevent N+1 queries.

    Args:
        agency_id: The agency UUID
        page: Page number (1-indexed)
        limit: Items per page

    Returns:
        Dictionary with contracts list and pagination info
    """
    offset = (page - 1) * limit

    # Get total count
    total_count = AgentCarrierNumber.objects.filter(agency_id=agency_id).count()  # type: ignore[attr-defined]

    if total_count == 0:
        return {
            'contracts': [],
            'pagination': {
                'currentPage': page,
                'totalPages': 0,
                'totalCount': 0,
                'limit': limit,
                'hasNextPage': False,
                'hasPrevPage': False,
            },
        }

    # Get paginated contracts with related data
    contracts = (
        AgentCarrierNumber.objects  # type: ignore[attr-defined]
        .filter(agency_id=agency_id)
        .select_related('carrier', 'agent')
        .order_by('-start_date', '-created_at')
        [offset:offset + limit]
    )

    # Format contracts to match frontend expectations
    formatted_contracts = []
    for contract in contracts:
        # Format start date like frontend does
        start_date_str = '—'
        if contract.start_date:
            start_date_str = contract.start_date.strftime('%b %d, %Y')

        # Get carrier name (display_name preferred, fallback to name)
        carrier_name = 'Unknown'
        if contract.carrier:
            carrier_name = getattr(contract.carrier, 'display_name', None) or contract.carrier.name

        # Get agent name in "Last, First" format
        agent_name = 'Unknown'
        if contract.agent:
            agent_name = f"{contract.agent.last_name or ''}, {contract.agent.first_name or ''}".strip(', ')
            if not agent_name:
                agent_name = 'Unknown'

        formatted_contracts.append({
            'id': str(contract.id),
            'carrier': carrier_name,
            'agent': agent_name,
            'loa': contract.loa or 'None',
            'status': 'Active' if contract.is_active else 'Inactive',
            'startDate': start_date_str,
            'agentNumber': contract.agent_number,
        })

    total_pages = math.ceil(total_count / limit)

    return {
        'contracts': formatted_contracts,
        'pagination': {
            'currentPage': page,
            'totalPages': total_pages,
            'totalCount': total_count,
            'limit': limit,
            'hasNextPage': page < total_pages,
            'hasPrevPage': page > 1,
        },
    }
