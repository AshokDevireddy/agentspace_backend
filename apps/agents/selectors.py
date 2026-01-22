"""
Agent Selectors

Query functions for agent data following the selector pattern.
Translates Supabase RPC functions to Django queries.
"""
from typing import Any, Dict, List, Optional
from uuid import UUID

from django.db import connection


def get_agent_downline(agent_id: UUID) -> List[dict]:
    """
    Get all agents in the downline hierarchy of a given agent.
    Translated from Supabase RPC: get_agent_downline

    Uses recursive CTE to traverse downward from agent_id.

    Args:
        agent_id: The root agent to get downline for

    Returns:
        List of agents in downline including self with depth level
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE downline AS (
                SELECT u.id, u.first_name, u.last_name, u.email, 0 as depth
                FROM users u
                WHERE u.id = %s
                UNION ALL
                SELECT u.id, u.first_name, u.last_name, u.email, d.depth + 1
                FROM users u
                JOIN downline d ON u.upline_id = d.id
                WHERE d.depth < 20  -- Safety limit to prevent infinite loops
            )
            SELECT d.id, d.first_name, d.last_name, d.email, d.depth as level
            FROM downline d
            ORDER BY d.depth, d.last_name, d.first_name
        """, [str(agent_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agent_upline_chain(agent_id: UUID) -> List[dict]:
    """
    Get the complete upline chain from an agent to the top of hierarchy.
    Translated from Supabase RPC: get_agent_upline_chain

    Args:
        agent_id: The agent to get upline chain for

    Returns:
        Upline chain from self to top
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE upline_chain AS (
                SELECT u.id AS agent_id, u.upline_id, 0 AS depth
                FROM users u
                WHERE u.id = %s
                UNION ALL
                SELECT u.id AS agent_id, u.upline_id, uc.depth + 1 AS depth
                FROM users u
                INNER JOIN upline_chain uc ON u.id = uc.upline_id
                WHERE uc.upline_id IS NOT NULL AND uc.depth < 20
            )
            SELECT uc.agent_id, uc.upline_id, uc.depth
            FROM upline_chain uc
            ORDER BY uc.depth ASC
        """, [str(agent_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agent_options(user_id: UUID, include_full_agency: bool = False) -> List[dict]:
    """
    Get agent options for dropdown/select components.
    Translated from Supabase RPC: get_agent_options

    Args:
        user_id: The requesting user's ID
        include_full_agency: If True, include all agency agents

    Returns:
        List of agent options with agent_id and display_name
    """
    with connection.cursor() as cursor:
        if include_full_agency:
            # Get all agents in user's agency
            cursor.execute("""
                WITH current_usr AS (
                    SELECT id, agency_id FROM users WHERE id = %s LIMIT 1
                )
                SELECT
                    u.id as agent_id,
                    CONCAT(u.last_name, ', ', u.first_name) as display_name
                FROM users u
                JOIN current_usr cu ON cu.agency_id = u.agency_id
                WHERE u.role <> 'client'
                    AND u.is_active = true
                ORDER BY u.last_name, u.first_name
            """, [str(user_id)])
        else:
            # Get only user's downline
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT u.id, u.first_name, u.last_name
                    FROM users u
                    WHERE u.id = %s
                    UNION ALL
                    SELECT u.id, u.first_name, u.last_name
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE d.id <> u.id  -- Prevent cycles
                )
                SELECT
                    d.id as agent_id,
                    CONCAT(d.last_name, ', ', d.first_name) as display_name
                FROM downline d
                ORDER BY d.last_name, d.first_name
            """, [str(user_id)])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agents_hierarchy_nodes(user_id: UUID, include_full_agency: bool = False) -> List[dict]:
    """
    Get hierarchy nodes for building tree view.
    Translated from Supabase RPC: get_agents_hierarchy_nodes

    Args:
        user_id: The requesting user's ID
        include_full_agency: If True, include all agency agents

    Returns:
        List of agents with hierarchy info for tree building
    """
    with connection.cursor() as cursor:
        if include_full_agency:
            cursor.execute("""
                WITH current_usr AS (
                    SELECT id, agency_id FROM users WHERE id = %s LIMIT 1
                )
                SELECT
                    u.id as agent_id,
                    u.first_name,
                    u.last_name,
                    u.perm_level,
                    u.upline_id,
                    u.position_id,
                    p.name as position_name,
                    p.level as position_level
                FROM users u
                JOIN current_usr cu ON cu.agency_id = u.agency_id
                LEFT JOIN positions p ON p.id = u.position_id
                WHERE u.role <> 'client'
                    AND u.is_active = true
                ORDER BY u.last_name, u.first_name
            """, [str(user_id)])
        else:
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT u.id
                    FROM users u
                    WHERE u.id = %s
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                )
                SELECT
                    u.id as agent_id,
                    u.first_name,
                    u.last_name,
                    u.perm_level,
                    u.upline_id,
                    u.position_id,
                    p.name as position_name,
                    p.level as position_level
                FROM users u
                JOIN downline d ON d.id = u.id
                LEFT JOIN positions p ON p.id = u.position_id
                WHERE u.role <> 'client'
                    AND u.is_active = true
                ORDER BY u.last_name, u.first_name
            """, [str(user_id)])

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agents_table(
    user_id: UUID,
    filters: Optional[Dict[str, Any]] = None,
    include_full_agency: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> List[dict]:
    """
    Get paginated agent table data with filtering.
    Translated from Supabase RPC: get_agents_table

    Args:
        user_id: The requesting user's ID
        filters: Filter dictionary with keys like status, agent_name, etc.
        include_full_agency: Include all agency agents
        limit: Page size
        offset: Page offset

    Returns:
        List of agent rows with total_count for pagination
    """
    filters = filters or {}

    # Extract filter values
    status_filter = filters.get('status')
    agent_name = filters.get('agent_name')
    in_upline = filters.get('in_upline')
    direct_upline = filters.get('direct_upline')
    in_downline = filters.get('in_downline')
    direct_downline = filters.get('direct_downline')
    position_id = filters.get('position_id')

    with connection.cursor() as cursor:
        # Build the query dynamically based on filters
        params = [str(user_id)]
        where_clauses = ["u.role <> 'client'", "u.is_active = true"]

        # Base query - either full agency or user's downline
        if include_full_agency:
            base_cte = """
                WITH current_usr AS (
                    SELECT id, agency_id FROM users WHERE id = %s LIMIT 1
                ),
                visible_agents AS (
                    SELECT u.id
                    FROM users u
                    JOIN current_usr cu ON cu.agency_id = u.agency_id
                    WHERE u.role <> 'client' AND u.is_active = true
                )
            """
        else:
            base_cte = """
                WITH current_usr AS (
                    SELECT id, agency_id FROM users WHERE id = %s LIMIT 1
                ),
                RECURSIVE downline AS (
                    SELECT u.id
                    FROM users u
                    WHERE u.id = (SELECT id FROM current_usr)
                    UNION ALL
                    SELECT u.id
                    FROM users u
                    JOIN downline d ON u.upline_id = d.id
                ),
                visible_agents AS (
                    SELECT id FROM downline
                )
            """

        # Status filter
        if status_filter and status_filter != 'all':
            where_clauses.append("u.status = %s")
            params.append(status_filter)

        # Position filter
        if position_id and position_id != 'all':
            if position_id is None:
                where_clauses.append("u.position_id IS NULL")
            else:
                where_clauses.append("u.position_id = %s")
                params.append(position_id)

        # Agent name filter (search by name)
        if agent_name and agent_name != 'all':
            where_clauses.append("(LOWER(u.first_name) LIKE %s OR LOWER(u.last_name) LIKE %s OR LOWER(CONCAT(u.first_name, ' ', u.last_name)) LIKE %s)")
            pattern = f'%{agent_name.lower()}%'
            params.extend([pattern, pattern, pattern])

        # Direct upline filter
        if direct_upline is not None and direct_upline != 'all':
            if direct_upline == '':
                where_clauses.append("u.upline_id IS NULL")
            else:
                # Find upline by name
                where_clauses.append("""
                    u.upline_id IN (
                        SELECT id FROM users
                        WHERE LOWER(CONCAT(first_name, ' ', last_name)) LIKE %s
                    )
                """)
                params.append(f'%{direct_upline.lower()}%')

        where_clause = " AND ".join(where_clauses)

        # Full query with pagination and counts
        query = f"""
            {base_cte}
            SELECT
                u.id as agent_id,
                u.first_name,
                u.last_name,
                u.email,
                u.perm_level,
                u.upline_id,
                upline.first_name || ' ' || upline.last_name as upline_name,
                u.created_at,
                u.status,
                COALESCE(u.total_prod, 0) as total_prod,
                COALESCE(u.total_policies_sold, 0) as total_policies_sold,
                (
                    SELECT COUNT(*)
                    FROM users du
                    WHERE du.upline_id = u.id AND du.is_active = true
                ) as downline_count,
                u.position_id,
                p.name as position_name,
                p.level as position_level,
                COUNT(*) OVER() as total_count
            FROM users u
            JOIN visible_agents va ON va.id = u.id
            LEFT JOIN users upline ON upline.id = u.upline_id
            LEFT JOIN positions p ON p.id = u.position_id
            WHERE {where_clause}
            ORDER BY u.last_name, u.first_name
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agents_without_positions(user_id: UUID) -> List[dict]:
    """
    Get agents who don't have a position assigned.
    Translated from Supabase RPC: get_agents_without_positions

    Args:
        user_id: The requesting user's ID

    Returns:
        List of agents without positions
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH current_usr AS (
                SELECT id, agency_id, is_admin, perm_level, role
                FROM users WHERE id = %s LIMIT 1
            ),
            is_admin_user AS (
                SELECT
                    CASE WHEN is_admin OR perm_level = 'admin' OR role = 'admin'
                    THEN true ELSE false END as is_admin
                FROM current_usr
            ),
            RECURSIVE downline AS (
                SELECT u.id
                FROM users u
                WHERE u.id = (SELECT id FROM current_usr)
                UNION ALL
                SELECT u.id
                FROM users u
                JOIN downline d ON u.upline_id = d.id
            ),
            visible_agents AS (
                SELECT u.id
                FROM users u
                JOIN current_usr cu ON cu.agency_id = u.agency_id
                WHERE (SELECT is_admin FROM is_admin_user)
                UNION
                SELECT id FROM downline
            )
            SELECT
                u.id as agent_id,
                u.first_name,
                u.last_name,
                u.email,
                u.phone as phone_number,
                u.role,
                upline.first_name || ' ' || upline.last_name as upline_name,
                u.created_at
            FROM users u
            JOIN visible_agents va ON va.id = u.id
            LEFT JOIN users upline ON upline.id = u.upline_id
            WHERE u.position_id IS NULL
                AND u.role <> 'client'
                AND u.is_active = true
            ORDER BY u.last_name, u.first_name
        """, [str(user_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agent_downlines_with_details(agent_id: UUID) -> List[dict]:
    """
    Get direct downlines with position details and metrics.

    Args:
        agent_id: The parent agent ID

    Returns:
        List of downline agents with details
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.position_id,
                u.status,
                u.created_at,
                p.name as position_name,
                p.level as position_level
            FROM users u
            LEFT JOIN positions p ON p.id = u.position_id
            WHERE u.upline_id = %s
                AND u.is_active = true
            ORDER BY u.created_at DESC
        """, [str(agent_id)])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def check_agent_upline_positions(agent_id: UUID) -> dict:
    """
    Check if all agents in the upline chain have positions assigned.
    Translated from Supabase RPC: check_agent_upline_positions

    Traverses from the given agent up to the top of hierarchy,
    checking if each agent has a position assigned.

    Args:
        agent_id: The agent to start checking from

    Returns:
        Dictionary with:
        - has_all_positions: boolean
        - missing_positions: list of agents without positions
        - total_checked: count of checked agents
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE upline_chain AS (
                -- Start with the given agent
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.position_id,
                    u.upline_id,
                    0 as depth
                FROM users u
                WHERE u.id = %s

                UNION ALL

                -- Traverse up the hierarchy
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.position_id,
                    u.upline_id,
                    uc.depth + 1
                FROM users u
                JOIN upline_chain uc ON u.id = uc.upline_id
                WHERE uc.upline_id IS NOT NULL
                    AND uc.depth < 50  -- Safety limit
            )
            SELECT
                id as agent_id,
                first_name,
                last_name,
                email,
                position_id,
                upline_id IS NULL as is_top_of_hierarchy
            FROM upline_chain
            ORDER BY depth ASC
        """, [str(agent_id)])

        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    all_have_positions = True
    missing_positions = []

    for row in rows:
        if row['position_id'] is None:
            all_have_positions = False
            missing_positions.append({
                'agent_id': str(row['agent_id']),
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'email': row['email'],
                'is_top_of_hierarchy': row['is_top_of_hierarchy'],
            })

    return {
        'has_all_positions': all_have_positions,
        'missing_positions': missing_positions,
        'total_checked': len(rows),
    }


def get_agents_debt_production(
    user_id: UUID,
    agent_ids: List[UUID],
    start_date: str,
    end_date: str,
) -> List[dict]:
    """
    Calculate debt and production metrics for agents.
    Translated from Supabase RPC: get_agents_debt_production

    Args:
        user_id: The requesting user's ID
        agent_ids: List of agent UUIDs to calculate metrics for
        start_date: Start of date range
        end_date: End of date range

    Returns:
        List of metrics for each agent
    """
    if not agent_ids:
        return []

    with connection.cursor() as cursor:
        # Convert UUIDs to strings for SQL
        agent_ids_str = [str(aid) for aid in agent_ids]

        cursor.execute("""
            WITH agent_list AS (
                SELECT unnest(%s::uuid[]) as agent_id
            ),
            -- Individual production (writing agent's own deals)
            individual_prod AS (
                SELECT
                    d.agent_id,
                    COALESCE(SUM(COALESCE(d.annual_premium, 0)), 0) as individual_production,
                    COUNT(*) as individual_production_count
                FROM deals d
                JOIN agent_list al ON al.agent_id = d.agent_id
                WHERE d.policy_effective_date >= %s::date
                    AND d.policy_effective_date < %s::date
                    AND d.status_standardized NOT IN ('cancelled', 'lapsed', 'terminated')
                GROUP BY d.agent_id
            ),
            -- Hierarchy production (deals from downlines via deal_hierarchy_snapshot)
            hierarchy_prod AS (
                SELECT
                    dhs.agent_id,
                    COALESCE(SUM(COALESCE(d.annual_premium, 0)), 0) as hierarchy_production,
                    COUNT(*) as hierarchy_production_count
                FROM deal_hierarchy_snapshot dhs
                JOIN deals d ON d.id = dhs.deal_id
                JOIN agent_list al ON al.agent_id = dhs.agent_id
                WHERE d.policy_effective_date >= %s::date
                    AND d.policy_effective_date < %s::date
                    AND d.status_standardized NOT IN ('cancelled', 'lapsed', 'terminated')
                    AND dhs.agent_id <> d.agent_id  -- Exclude self (counted in individual)
                GROUP BY dhs.agent_id
            ),
            -- Individual debt (lapsed deals where agent is writing agent)
            individual_debt AS (
                SELECT
                    d.agent_id,
                    COALESCE(SUM(
                        CASE
                            WHEN d.status_standardized IN ('lapsed', 'cancelled', 'terminated')
                            THEN COALESCE(d.annual_premium, 0) * 0.75
                            ELSE 0
                        END
                    ), 0) as individual_debt,
                    COUNT(*) FILTER (
                        WHERE d.status_standardized IN ('lapsed', 'cancelled', 'terminated')
                    ) as individual_debt_count
                FROM deals d
                JOIN agent_list al ON al.agent_id = d.agent_id
                WHERE d.policy_effective_date >= %s::date
                    AND d.policy_effective_date < %s::date
                GROUP BY d.agent_id
            ),
            -- Hierarchy debt (lapsed deals from downlines)
            hierarchy_debt AS (
                SELECT
                    dhs.agent_id,
                    COALESCE(SUM(
                        CASE
                            WHEN d.status_standardized IN ('lapsed', 'cancelled', 'terminated')
                            THEN COALESCE(d.annual_premium, 0) * 0.75
                            ELSE 0
                        END
                    ), 0) as hierarchy_debt,
                    COUNT(*) FILTER (
                        WHERE d.status_standardized IN ('lapsed', 'cancelled', 'terminated')
                    ) as hierarchy_debt_count
                FROM deal_hierarchy_snapshot dhs
                JOIN deals d ON d.id = dhs.deal_id
                JOIN agent_list al ON al.agent_id = dhs.agent_id
                WHERE d.policy_effective_date >= %s::date
                    AND d.policy_effective_date < %s::date
                    AND dhs.agent_id <> d.agent_id
                GROUP BY dhs.agent_id
            )
            SELECT
                al.agent_id,
                COALESCE(ip.individual_production, 0) as individual_production,
                COALESCE(ip.individual_production_count, 0) as individual_production_count,
                COALESCE(hp.hierarchy_production, 0) as hierarchy_production,
                COALESCE(hp.hierarchy_production_count, 0) as hierarchy_production_count,
                COALESCE(id.individual_debt, 0) as individual_debt,
                COALESCE(id.individual_debt_count, 0) as individual_debt_count,
                COALESCE(hd.hierarchy_debt, 0) as hierarchy_debt,
                COALESCE(hd.hierarchy_debt_count, 0) as hierarchy_debt_count,
                CASE
                    WHEN COALESCE(ip.individual_production, 0) + COALESCE(hp.hierarchy_production, 0) > 0
                    THEN (COALESCE(id.individual_debt, 0) + COALESCE(hd.hierarchy_debt, 0)) /
                         (COALESCE(ip.individual_production, 0) + COALESCE(hp.hierarchy_production, 0))
                    ELSE NULL
                END as debt_to_production_ratio
            FROM agent_list al
            LEFT JOIN individual_prod ip ON ip.agent_id = al.agent_id
            LEFT JOIN hierarchy_prod hp ON hp.agent_id = al.agent_id
            LEFT JOIN individual_debt id ON id.agent_id = al.agent_id
            LEFT JOIN hierarchy_debt hd ON hd.agent_id = al.agent_id
        """, [
            agent_ids_str,
            start_date, end_date,
            start_date, end_date,
            start_date, end_date,
            start_date, end_date,
        ])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
