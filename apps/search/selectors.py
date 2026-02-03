"""
Search Selectors

Query functions for search operations.
Translates Supabase RPC functions to Django queries.
"""
from typing import Any
from uuid import UUID

from django.db import connection


def search_agents_downline(
    user_id: UUID,
    search_query: str,
    limit: int = 10,
    search_type: str = 'downline',
    agency_id: UUID | None = None,
) -> list[dict]:
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
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_agents_all(
    user_id: UUID,
    agency_id: UUID,
    limit: int = 50,
) -> list[dict]:
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
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_clients_for_filter(
    user_id: UUID,
    search_term: str = '',
    limit: int = 20,
) -> list[dict]:
    """
    Search clients for filter dropdown.
    Translated from Supabase RPC: search_clients_for_filter

    Returns {value, label} format for select dropdowns.
    Searches users with role='client' who have deals.

    Args:
        user_id: The requesting user's ID
        search_term: Search term (empty returns all)
        limit: Maximum results

    Returns:
        List of {value, label} dicts
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH user_agency AS (
                SELECT agency_id FROM users WHERE id = %s LIMIT 1
            )
            SELECT
                u.id::text AS value,
                CONCAT(u.first_name, ' ', u.last_name) AS label
            FROM users u
            WHERE u.agency_id = (SELECT agency_id FROM user_agency)
                AND u.role = 'client'
                AND (
                    %s = ''
                    OR u.first_name ILIKE '%%' || %s || '%%'
                    OR u.last_name ILIKE '%%' || %s || '%%'
                    OR u.email ILIKE '%%' || %s || '%%'
                )
                -- Only show clients with deals
                AND EXISTS (SELECT 1 FROM deals d WHERE d.client_id = u.id)
            ORDER BY u.last_name, u.first_name
            LIMIT %s
        """, [
            str(user_id),
            search_term,
            search_term,
            search_term,
            search_term,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_agents_for_filter(
    user_id: UUID,
    search_term: str = '',
    limit: int = 20,
) -> list[dict]:
    """
    Search agents for filter dropdown.
    Translated from Supabase RPC: search_agents_for_filter

    Returns {value, label} format for select dropdowns.
    Only shows agents with deals.

    Args:
        user_id: The requesting user's ID
        search_term: Search term (empty returns all)
        limit: Maximum results

    Returns:
        List of {value, label} dicts
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH user_context AS (
                SELECT
                    u.agency_id,
                    COALESCE(u.is_admin, false) OR u.perm_level = 'admin' OR u.role = 'admin' as is_admin
                FROM users u
                WHERE u.id = %s
                LIMIT 1
            ),
            downline AS (
                SELECT %s::uuid AS id
                UNION
                SELECT d.id
                FROM (
                    WITH RECURSIVE dl AS (
                        SELECT id FROM users WHERE id = %s
                        UNION ALL
                        SELECT u.id FROM users u JOIN dl ON u.upline_id = dl.id
                    )
                    SELECT id FROM dl WHERE id != %s
                ) d
            ),
            agent_ids AS (
                SELECT id FROM downline
                WHERE NOT (SELECT is_admin FROM user_context)
            )
            SELECT
                u.id::text AS value,
                CONCAT(u.last_name, ', ', u.first_name) AS label
            FROM users u
            WHERE u.agency_id = (SELECT agency_id FROM user_context)
                AND u.role <> 'client'
                AND (
                    (SELECT is_admin FROM user_context)
                    OR u.id IN (SELECT id FROM agent_ids)
                )
                AND (
                    %s = ''
                    OR u.first_name ILIKE '%%' || %s || '%%'
                    OR u.last_name ILIKE '%%' || %s || '%%'
                )
                AND EXISTS (SELECT 1 FROM deals d WHERE d.agent_id = u.id)
            ORDER BY u.last_name, u.first_name
            LIMIT %s
        """, [
            str(user_id),
            str(user_id),
            str(user_id),
            str(user_id),
            search_term,
            search_term,
            search_term,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_agents_fuzzy(
    query: str,
    agency_id: UUID,
    allowed_agent_ids: list[UUID] | None = None,
    limit: int = 20,
    similarity_threshold: float = 0.3,
) -> list[dict]:
    """
    Fuzzy search for agents using pg_trgm similarity.
    Translated from Supabase RPC: search_agents_fuzzy

    Args:
        query: Search query
        agency_id: Agency ID
        allowed_agent_ids: Optional list of allowed agent IDs (for permission scoping)
        limit: Maximum results
        similarity_threshold: Minimum similarity score (0.0-1.0)

    Returns:
        List of matching agents with similarity scores
    """
    with connection.cursor() as cursor:
        # Build agent filter
        agent_filter = ""
        params: list[Any] = [str(agency_id), query, query, query, query, query, query, query, query]

        if allowed_agent_ids:
            agent_ids_str = [str(aid) for aid in allowed_agent_ids]
            agent_filter = "AND u.id = ANY(%s::uuid[])"
            params.append(agent_ids_str)  # type: ignore[arg-type]

        cursor.execute(f"""
            WITH search_results AS (
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.total_prod,
                    GREATEST(
                        COALESCE(similarity(LOWER(u.first_name), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(u.last_name), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(u.email), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(CONCAT(u.first_name, ' ', u.last_name)), LOWER(%s)), 0::real)
                    ) as sim_score,
                    CASE
                        WHEN LOWER(u.first_name) = LOWER(%s)
                            OR LOWER(u.last_name) = LOWER(%s)
                            OR LOWER(u.email) = LOWER(%s)
                            OR LOWER(CONCAT(u.first_name, ' ', u.last_name)) = LOWER(%s)
                        THEN 'exact'
                        WHEN LOWER(u.first_name) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(u.last_name) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(u.email) ILIKE '%%' || LOWER(%s) || '%%'
                        THEN 'fuzzy'
                        ELSE 'suggestion'
                    END as match_type
                FROM users u
                WHERE u.agency_id = %s
                    AND u.role != 'client'
                    {agent_filter}
                    AND (
                        COALESCE(u.first_name, '') %% %s
                        OR COALESCE(u.last_name, '') %% %s
                        OR COALESCE(u.email, '') %% %s
                        OR CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')) %% %s
                    )
            )
            SELECT
                sr.id,
                sr.first_name,
                sr.last_name,
                sr.email,
                sr.total_prod,
                sr.sim_score as similarity_score,
                sr.match_type
            FROM search_results sr
            WHERE sr.sim_score >= %s
            ORDER BY
                CASE sr.match_type
                    WHEN 'exact' THEN 1
                    WHEN 'fuzzy' THEN 2
                    ELSE 3
                END,
                sr.sim_score DESC
            LIMIT %s
        """, [
            query, query, query, query,  # similarity calculations
            query, query, query, query,  # exact match checks
            query, query, query,  # ILIKE checks
            str(agency_id),
        ] + ([agent_ids_str] if allowed_agent_ids else []) + [
            query, query, query, query,  # trigram checks
            similarity_threshold,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_clients_fuzzy(
    query: str,
    agency_id: UUID,
    allowed_agent_ids: list[UUID] | None = None,
    limit: int = 20,
    similarity_threshold: float = 0.3,
) -> list[dict]:
    """
    Fuzzy search for clients using pg_trgm similarity.
    Translated from Supabase RPC: search_clients_fuzzy

    Args:
        query: Search query
        agency_id: Agency ID
        allowed_agent_ids: Optional list of allowed agent IDs (for permission scoping)
        limit: Maximum results
        similarity_threshold: Minimum similarity score (0.0-1.0)

    Returns:
        List of matching clients with similarity scores
    """
    with connection.cursor() as cursor:
        agent_filter = ""
        if allowed_agent_ids:
            agent_ids_str = [str(aid) for aid in allowed_agent_ids]
            agent_filter = "AND c.agent_id = ANY(%s::uuid[])"

        cursor.execute(f"""
            WITH search_results AS (
                SELECT
                    c.id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    c.phone,
                    c.agent_id,
                    GREATEST(
                        COALESCE(similarity(LOWER(c.first_name), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(c.last_name), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(c.email), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(c.phone), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(CONCAT(c.first_name, ' ', c.last_name)), LOWER(%s)), 0::real)
                    ) as sim_score,
                    CASE
                        WHEN LOWER(c.first_name) = LOWER(%s)
                            OR LOWER(c.last_name) = LOWER(%s)
                            OR LOWER(c.email) = LOWER(%s)
                            OR LOWER(c.phone) = LOWER(%s)
                            OR LOWER(CONCAT(c.first_name, ' ', c.last_name)) = LOWER(%s)
                        THEN 'exact'
                        WHEN LOWER(c.first_name) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(c.last_name) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(c.email) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(c.phone) ILIKE '%%' || LOWER(%s) || '%%'
                        THEN 'fuzzy'
                        ELSE 'suggestion'
                    END as match_type
                FROM clients c
                WHERE c.agency_id = %s
                    {agent_filter}
                    AND (
                        COALESCE(c.first_name, '') %% %s
                        OR COALESCE(c.last_name, '') %% %s
                        OR COALESCE(c.email, '') %% %s
                        OR COALESCE(c.phone, '') %% %s
                        OR CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, '')) %% %s
                    )
            )
            SELECT
                sr.id,
                sr.first_name,
                sr.last_name,
                sr.email,
                sr.phone,
                sr.agent_id,
                sr.sim_score as similarity_score,
                sr.match_type
            FROM search_results sr
            WHERE sr.sim_score >= %s
            ORDER BY
                CASE sr.match_type
                    WHEN 'exact' THEN 1
                    WHEN 'fuzzy' THEN 2
                    ELSE 3
                END,
                sr.sim_score DESC
            LIMIT %s
        """, [
            query, query, query, query, query,  # similarity calculations
            query, query, query, query, query,  # exact match checks
            query, query, query, query,  # ILIKE checks
            str(agency_id),
        ] + ([agent_ids_str] if allowed_agent_ids else []) + [
            query, query, query, query, query,  # trigram checks
            similarity_threshold,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_policies_fuzzy(
    query: str,
    agency_id: UUID,
    allowed_agent_ids: list[UUID] | None = None,
    limit: int = 20,
    similarity_threshold: float = 0.3,
) -> list[dict]:
    """
    Fuzzy search for policies/deals using pg_trgm similarity.
    Translated from Supabase RPC: search_policies_fuzzy

    Args:
        query: Search query
        agency_id: Agency ID
        allowed_agent_ids: Optional list of allowed agent IDs (for permission scoping)
        limit: Maximum results
        similarity_threshold: Minimum similarity score (0.0-1.0)

    Returns:
        List of matching deals with similarity scores
    """
    with connection.cursor() as cursor:
        agent_filter = ""
        if allowed_agent_ids:
            agent_ids_str = [str(aid) for aid in allowed_agent_ids]
            agent_filter = "AND d.agent_id = ANY(%s::uuid[])"

        cursor.execute(f"""
            WITH search_results AS (
                SELECT
                    d.id,
                    d.policy_number,
                    d.application_number,
                    d.client_name,
                    d.agent_id,
                    d.carrier_id,
                    d.annual_premium,
                    d.status_standardized,
                    GREATEST(
                        COALESCE(similarity(LOWER(d.policy_number), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(d.application_number), LOWER(%s)), 0::real),
                        COALESCE(similarity(LOWER(d.client_name), LOWER(%s)), 0::real)
                    ) as sim_score,
                    CASE
                        WHEN LOWER(d.policy_number) = LOWER(%s)
                            OR LOWER(d.application_number) = LOWER(%s)
                            OR LOWER(d.client_name) = LOWER(%s)
                        THEN 'exact'
                        WHEN LOWER(d.policy_number) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(d.application_number) ILIKE '%%' || LOWER(%s) || '%%'
                            OR LOWER(d.client_name) ILIKE '%%' || LOWER(%s) || '%%'
                        THEN 'fuzzy'
                        ELSE 'suggestion'
                    END as match_type
                FROM deals d
                WHERE d.agency_id = %s
                    {agent_filter}
                    AND (
                        COALESCE(d.policy_number, '') %% %s
                        OR COALESCE(d.application_number, '') %% %s
                        OR COALESCE(d.client_name, '') %% %s
                    )
            )
            SELECT
                sr.id,
                sr.policy_number,
                sr.application_number,
                sr.client_name,
                sr.agent_id,
                sr.carrier_id,
                sr.annual_premium,
                sr.status_standardized,
                sr.sim_score as similarity_score,
                sr.match_type
            FROM search_results sr
            WHERE sr.sim_score >= %s
            ORDER BY
                CASE sr.match_type
                    WHEN 'exact' THEN 1
                    WHEN 'fuzzy' THEN 2
                    ELSE 3
                END,
                sr.sim_score DESC
            LIMIT %s
        """, [
            query, query, query,  # similarity calculations
            query, query, query,  # exact match checks
            query, query, query,  # ILIKE checks
            str(agency_id),
        ] + ([agent_ids_str] if allowed_agent_ids else []) + [
            query, query, query,  # trigram checks
            similarity_threshold,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def search_policy_numbers_for_filter(
    user_id: UUID,
    search_term: str = '',
    limit: int = 20,
) -> list[dict]:
    """
    Search policy numbers for filter dropdown.
    Translated from Supabase RPC: search_policy_numbers_for_filter

    Returns {value, label} format for select dropdowns.

    Args:
        user_id: The requesting user's ID
        search_term: Search term (empty returns all)
        limit: Maximum results

    Returns:
        List of {value, label} dicts
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH user_context AS (
                SELECT
                    u.agency_id,
                    COALESCE(u.is_admin, false) OR u.perm_level = 'admin' OR u.role = 'admin' as is_admin
                FROM users u
                WHERE u.id = %s
                LIMIT 1
            ),
            downline AS (
                SELECT %s::uuid AS id
                UNION
                SELECT dl.id
                FROM (
                    WITH RECURSIVE dlr AS (
                        SELECT id FROM users WHERE id = %s
                        UNION ALL
                        SELECT u.id FROM users u JOIN dlr ON u.upline_id = dlr.id
                    )
                    SELECT id FROM dlr WHERE id != %s
                ) dl
            ),
            agent_ids AS (
                SELECT id FROM downline
                WHERE NOT (SELECT is_admin FROM user_context)
            )
            SELECT DISTINCT
                d.policy_number AS value,
                d.policy_number AS label
            FROM deals d
            WHERE d.agency_id = (SELECT agency_id FROM user_context)
                AND d.policy_number IS NOT NULL
                AND d.policy_number <> ''
                AND (
                    (SELECT is_admin FROM user_context)
                    OR d.agent_id IN (SELECT id FROM agent_ids)
                )
                AND (
                    %s = ''
                    OR d.policy_number ILIKE '%%' || %s || '%%'
                )
            ORDER BY d.policy_number
            LIMIT %s
        """, [
            str(user_id),
            str(user_id),
            str(user_id),
            str(user_id),
            search_term,
            search_term,
            limit,
        ])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
