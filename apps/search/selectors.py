"""
Search Selectors

Query functions for search operations.
Translates Supabase RPC functions to Django queries.
"""
from typing import List, Optional
from uuid import UUID

from django.db import connection


def search_agents_downline(
    user_id: UUID,
    search_query: str,
    limit: int = 10,
    search_type: str = 'downline',
    agency_id: Optional[UUID] = None,
) -> List[dict]:
    """
    Search for agents within user's downline or agency.
    Translated from Supabase RPC: get_agent_downline + search

    Args:
        user_id: The requesting user's ID
        search_query: Search term
        limit: Maximum results
        search_type: 'downline' or 'pre-invite'
        agency_id: Agency ID for pre-invite search

    Returns:
        List of matching agents
    """
    with connection.cursor() as cursor:
        if search_type == 'pre-invite':
            # Search for pre-invite users in the agency
            cursor.execute("""
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.status
                FROM users u
                WHERE u.agency_id = %s
                    AND u.status = 'pre-invite'
                    AND u.role <> 'client'
                    AND (
                        LOWER(u.first_name) LIKE %s
                        OR LOWER(u.last_name) LIKE %s
                        OR LOWER(u.email) LIKE %s
                        OR LOWER(CONCAT(u.first_name, ' ', u.last_name)) LIKE %s
                    )
                ORDER BY u.last_name, u.first_name
                LIMIT %s
            """, [
                str(agency_id),
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                limit,
            ])
        else:
            # Search in user's downline
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
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.status
                FROM users u
                JOIN downline d ON d.id = u.id
                WHERE u.status IN ('active', 'invited')
                    AND u.role <> 'client'
                    AND (
                        LOWER(u.first_name) LIKE %s
                        OR LOWER(u.last_name) LIKE %s
                        OR LOWER(u.email) LIKE %s
                        OR LOWER(CONCAT(u.first_name, ' ', u.last_name)) LIKE %s
                    )
                ORDER BY u.last_name, u.first_name
                LIMIT %s
            """, [
                str(user_id),
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                f'%{search_query.lower()}%',
                limit,
            ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def search_agents_all(
    user_id: UUID,
    agency_id: UUID,
    limit: int = 50,
) -> List[dict]:
    """
    Get all agents for options dropdown (no search filter).

    Args:
        user_id: The requesting user's ID
        agency_id: The user's agency ID
        limit: Maximum results

    Returns:
        List of agents
    """
    with connection.cursor() as cursor:
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
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.status
            FROM users u
            JOIN downline d ON d.id = u.id
            WHERE u.status IN ('active', 'invited')
                AND u.role <> 'client'
            ORDER BY u.last_name, u.first_name
            LIMIT %s
        """, [str(user_id), limit])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def search_clients_for_filter(
    user_id: UUID,
    search_term: str,
    limit: int = 20,
) -> List[dict]:
    """
    Search clients for filter dropdown.
    Translated from Supabase RPC: search_clients_for_filter

    Args:
        user_id: The requesting user's ID
        search_term: Search term
        limit: Maximum results

    Returns:
        List of matching clients
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH user_agency AS (
                SELECT agency_id FROM users WHERE id = %s LIMIT 1
            ),
            RECURSIVE user_downline AS (
                SELECT id FROM users WHERE id = %s
                UNION ALL
                SELECT u.id
                FROM users u
                JOIN user_downline ud ON u.upline_id = ud.id
            )
            SELECT DISTINCT
                c.id,
                c.first_name,
                c.last_name,
                c.email,
                c.phone,
                CONCAT(c.first_name, ' ', c.last_name) as display_name
            FROM clients c
            JOIN deals d ON d.client_id = c.id
            JOIN user_downline ud ON d.agent_id = ud.id
            WHERE c.agency_id = (SELECT agency_id FROM user_agency)
                AND (
                    LOWER(c.first_name) LIKE %s
                    OR LOWER(c.last_name) LIKE %s
                    OR LOWER(c.email) LIKE %s
                    OR LOWER(CONCAT(c.first_name, ' ', c.last_name)) LIKE %s
                )
            ORDER BY c.last_name, c.first_name
            LIMIT %s
        """, [
            str(user_id),
            str(user_id),
            f'%{search_term.lower()}%',
            f'%{search_term.lower()}%',
            f'%{search_term.lower()}%',
            f'%{search_term.lower()}%',
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
