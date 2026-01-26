"""
Agent Selectors

Query functions for agent data following the selector pattern.
Translates Supabase RPC functions to Django queries.

Uses django-cte 2.0 for recursive CTE support where needed.
"""
from typing import Any
from uuid import UUID

from django.db import connection
from django.db.models import F, IntegerField, Value
from django_cte import With


def get_agent_downline(agent_id: UUID) -> list[dict]:
    """
    Get all agents in the downline hierarchy of a given agent.
    Translated from Supabase RPC: get_agent_downline

    Uses django-cte for recursive traversal.

    Args:
        agent_id: The root agent to get downline for

    Returns:
        List of agents in downline including self with depth level
    """
    from apps.core.models import User

    def make_downline_cte(cte):
        # Anchor: the agent themselves at depth 0
        anchor = (
            User.objects.filter(id=agent_id)  # type: ignore[attr-defined]
            .annotate(depth=Value(0, output_field=IntegerField()))
            .values('id', 'first_name', 'last_name', 'email', 'depth')
        )

        # Recursive: get children, increment depth (limit to 20)
        recursive = (
            cte.join(User, upline_id=cte.col.id)
            .annotate(depth=cte.col.depth + 1)
            .filter(depth__lt=20)
            .values('id', 'first_name', 'last_name', 'email', 'depth')
        )

        return anchor.union(recursive, all=True)

    cte = With.recursive(make_downline_cte)

    # Query the CTE results
    results = (
        cte.queryset()
        .with_cte(cte)
        .annotate(level=F('depth'))
        .order_by('depth', 'last_name', 'first_name')
    )

    return [
        {
            'id': r['id'],
            'first_name': r['first_name'],
            'last_name': r['last_name'],
            'email': r['email'],
            'level': r['depth'],
        }
        for r in results.values('id', 'first_name', 'last_name', 'email', 'depth')
    ]


def get_agent_upline_chain(agent_id: UUID) -> list[dict]:
    """
    Get the complete upline chain from an agent to the top of hierarchy.
    Translated from Supabase RPC: get_agent_upline_chain

    Uses django-cte for recursive traversal.

    Args:
        agent_id: The agent to get upline chain for

    Returns:
        Upline chain from self to top
    """
    from apps.core.models import User

    def make_upline_cte(cte):
        # Anchor: the agent themselves
        anchor = (
            User.objects.filter(id=agent_id)  # type: ignore[attr-defined]
            .annotate(depth=Value(0, output_field=IntegerField()))
            .values('id', 'upline_id', 'depth')
        )

        # Recursive: follow upline_id chain (limit to 20)
        recursive = (
            cte.join(User, id=cte.col.upline_id)
            .annotate(depth=cte.col.depth + 1)
            .filter(depth__lt=20)
            .values('id', 'upline_id', 'depth')
        )

        return anchor.union(recursive, all=True)

    cte = With.recursive(make_upline_cte)

    results = (
        cte.queryset()
        .with_cte(cte)
        .order_by('depth')
    )

    return [
        {
            'agent_id': r['id'],
            'upline_id': r['upline_id'],
            'depth': r['depth'],
        }
        for r in results.values('id', 'upline_id', 'depth')
    ]


def get_agent_options(user_id: UUID, include_full_agency: bool = False) -> list[dict]:
    """
    Get agent options for dropdown/select components.
    Translated from Supabase RPC: get_agent_options

    Uses ORM for full agency, django-cte for downline mode.

    Args:
        user_id: The requesting user's ID
        include_full_agency: If True, include all agency agents

    Returns:
        List of agent options with agent_id and display_name
    """
    from apps.core.models import User

    if include_full_agency:
        # Get user's agency first
        user = User.objects.filter(id=user_id).values('agency_id').first()  # type: ignore[attr-defined]
        if not user:
            return []

        # Get all active agents in the agency using ORM
        agents = (
            User.objects.filter(  # type: ignore[attr-defined]
                agency_id=user['agency_id'],
                is_active=True
            )
            .exclude(role='client')
            .order_by('last_name', 'first_name')
        )

        return [
            {
                'agent_id': a.id,
                'display_name': f"{a.last_name or ''}, {a.first_name or ''}".strip(', '),
            }
            for a in agents
        ]
    else:
        # Use django-cte for recursive downline
        def make_downline_cte(cte):
            anchor = (
                User.objects.filter(id=user_id)  # type: ignore[attr-defined]
                .values('id', 'first_name', 'last_name')
            )

            recursive = (
                cte.join(User, upline_id=cte.col.id)
                .values('id', 'first_name', 'last_name')
            )

            return anchor.union(recursive, all=True)

        cte = With.recursive(make_downline_cte)

        results = (
            cte.queryset()
            .with_cte(cte)
            .order_by('last_name', 'first_name')
        )

        return [
            {
                'agent_id': r['id'],
                'display_name': f"{r['last_name'] or ''}, {r['first_name'] or ''}".strip(', '),
            }
            for r in results.values('id', 'first_name', 'last_name')
        ]


def get_agents_hierarchy_nodes(user_id: UUID, include_full_agency: bool = False) -> list[dict]:
    """
    Get hierarchy nodes for building tree view.
    Translated from Supabase RPC: get_agents_hierarchy_nodes

    Uses ORM with select_related for full agency, django-cte for downline.

    Args:
        user_id: The requesting user's ID
        include_full_agency: If True, include all agency agents

    Returns:
        List of agents with hierarchy info for tree building
    """
    from apps.core.models import User

    if include_full_agency:
        # Get user's agency
        user = User.objects.filter(id=user_id).values('agency_id').first()  # type: ignore[attr-defined]
        if not user:
            return []

        # Get all active agents with position info using ORM
        agents = (
            User.objects.filter(  # type: ignore[attr-defined]
                agency_id=user['agency_id'],
                is_active=True
            )
            .exclude(role='client')
            .select_related('position')
            .order_by('last_name', 'first_name')
        )

        return [
            {
                'agent_id': a.id,
                'first_name': a.first_name,
                'last_name': a.last_name,
                'perm_level': a.perm_level,
                'upline_id': a.upline_id,
                'position_id': a.position_id,
                'position_name': a.position.name if a.position else None,
                'position_level': a.position.level if a.position else None,
            }
            for a in agents
        ]
    else:
        # Use django-cte for recursive downline, then join with full user data
        def make_downline_cte(cte):
            anchor = User.objects.filter(id=user_id).values('id')  # type: ignore[attr-defined]
            recursive = cte.join(User, upline_id=cte.col.id).values('id')
            return anchor.union(recursive, all=True)

        cte = With.recursive(make_downline_cte)

        # Get downline IDs
        downline_ids = list(
            cte.queryset()
            .with_cte(cte)
            .values_list('id', flat=True)
        )

        # Fetch full user data with positions using ORM
        agents = (
            User.objects.filter(id__in=downline_ids, is_active=True)  # type: ignore[attr-defined]
            .exclude(role='client')
            .select_related('position')
            .order_by('last_name', 'first_name')
        )

        return [
            {
                'agent_id': a.id,
                'first_name': a.first_name,
                'last_name': a.last_name,
                'perm_level': a.perm_level,
                'upline_id': a.upline_id,
                'position_id': a.position_id,
                'position_name': a.position.name if a.position else None,
                'position_level': a.position.level if a.position else None,
            }
            for a in agents
        ]


# Allowed filter keys for get_agents_table() - security whitelist
ALLOWED_AGENT_TABLE_FILTER_KEYS = frozenset({
    'status',
    'agent_name',
    'in_upline',
    'direct_upline',
    'in_downline',
    'direct_downline',
    'position_id',
})


def get_agents_table(
    user_id: UUID,
    filters: dict[str, Any] | None = None,
    include_full_agency: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """
    Get paginated agent table data with filtering.
    Translated from Supabase RPC: get_agents_table

    Note: Kept as raw SQL due to complex dynamic filtering requirements.
    This is a P3 function - partial ORM conversion planned.

    Args:
        user_id: The requesting user's ID
        filters: Filter dictionary with keys like status, agent_name, etc.
        include_full_agency: Include all agency agents
        limit: Page size
        offset: Page offset

    Returns:
        List of agent rows with total_count for pagination

    Raises:
        ValueError: If an invalid filter key is provided
    """
    filters = filters or {}

    # Validate filter keys against whitelist to prevent injection
    invalid_keys = set(filters.keys()) - ALLOWED_AGENT_TABLE_FILTER_KEYS
    if invalid_keys:
        raise ValueError(f"Invalid filter key(s): {', '.join(sorted(invalid_keys))}")

    # Extract filter values
    status_filter = filters.get('status')
    agent_name = filters.get('agent_name')
    filters.get('in_upline')
    direct_upline = filters.get('direct_upline')
    filters.get('in_downline')
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
                WITH RECURSIVE
                current_usr AS (
                    SELECT id, agency_id FROM users WHERE id = %s LIMIT 1
                ),
                downline AS (
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
            where_clauses.append(
                "(LOWER(u.first_name) LIKE %s OR LOWER(u.last_name) LIKE %s OR "
                "LOWER(CONCAT(u.first_name, ' ', u.last_name)) LIKE %s)"
            )
            pattern = f'%{agent_name.lower()}%'
            params.extend([pattern, pattern, pattern])

        # Direct upline filter
        if direct_upline is not None and direct_upline != 'all':
            if direct_upline == '':
                where_clauses.append("u.upline_id IS NULL")
            else:
                where_clauses.append("""
                    u.upline_id IN (
                        SELECT id FROM users
                        WHERE LOWER(CONCAT(first_name, ' ', last_name)) LIKE %s
                    )
                """)
                params.append(f'%{direct_upline.lower()}%')

        # Direct downline filter
        if direct_downline is not None and direct_downline != 'all':
            where_clauses.append("""
                u.id IN (
                    SELECT upline_id FROM users
                    WHERE LOWER(CONCAT(first_name, ' ', last_name)) LIKE %s
                )
            """)
            params.append(f'%{direct_downline.lower()}%')

        where_sql = " AND ".join(where_clauses)

        # Main query
        query = f"""
            {base_cte}
            SELECT
                u.id as agent_id,
                u.first_name,
                u.last_name,
                u.email,
                u.phone_number as phone_number,
                u.status,
                u.perm_level,
                u.position_id,
                p.name as position_name,
                p.level as position_level,
                u.upline_id,
                upline.first_name || ' ' || upline.last_name as upline_name,
                u.created_at,
                COUNT(*) OVER() as total_count
            FROM users u
            JOIN visible_agents va ON va.id = u.id
            LEFT JOIN positions p ON p.id = u.position_id
            LEFT JOIN users upline ON upline.id = u.upline_id
            WHERE {where_sql}
            ORDER BY u.last_name, u.first_name
            LIMIT %s OFFSET %s
        """
        params.extend([str(limit), str(offset)])

        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def get_agents_without_positions(user_id: UUID) -> list[dict]:
    """
    Get agents who don't have a position assigned.
    Translated from Supabase RPC: get_agents_without_positions

    Uses django-cte for downline visibility, ORM for final query.

    Args:
        user_id: The requesting user's ID

    Returns:
        List of agents without positions
    """
    from apps.core.models import User

    # Get user info to determine visibility
    user = User.objects.filter(id=user_id).values(  # type: ignore[attr-defined]
        'id', 'agency_id', 'is_admin', 'perm_level', 'role'
    ).first()

    if not user:
        return []

    is_admin = user['is_admin'] or user['perm_level'] == 'admin' or user['role'] == 'admin'

    if is_admin:
        # Admin sees all agency agents without positions
        agents = (
            User.objects.filter(  # type: ignore[attr-defined]
                agency_id=user['agency_id'],
                position_id__isnull=True,
                is_active=True
            )
            .exclude(role='client')
            .select_related('upline')
            .order_by('last_name', 'first_name')
        )
    else:
        # Use django-cte to get downline IDs
        def make_downline_cte(cte):
            anchor = User.objects.filter(id=user_id).values('id')  # type: ignore[attr-defined]
            recursive = cte.join(User, upline_id=cte.col.id).values('id')
            return anchor.union(recursive, all=True)

        cte = With.recursive(make_downline_cte)
        downline_ids = list(
            cte.queryset()
            .with_cte(cte)
            .values_list('id', flat=True)
        )

        # Filter to agents without positions in downline
        agents = (
            User.objects.filter(  # type: ignore[attr-defined]
                id__in=downline_ids,
                position_id__isnull=True,
                is_active=True
            )
            .exclude(role='client')
            .select_related('upline')
            .order_by('last_name', 'first_name')
        )

    return [
        {
            'agent_id': a.id,
            'first_name': a.first_name,
            'last_name': a.last_name,
            'email': a.email,
            'phone_number': a.phone,
            'role': a.role,
            'upline_name': f"{a.upline.first_name or ''} {a.upline.last_name or ''}".strip() if a.upline else None,
            'created_at': a.created_at,
        }
        for a in agents
    ]


def get_agent_downlines_with_details(agent_id: UUID) -> list[dict]:
    """
    Get direct downlines with position details and metrics.

    Uses Django ORM with select_related to prevent N+1 queries.

    Args:
        agent_id: The parent agent ID

    Returns:
        List of downline agents with details
    """
    from apps.core.models import User

    downlines = (
        User.objects.filter(upline_id=agent_id, is_active=True)  # type: ignore[attr-defined]
        .select_related('position')
        .order_by('-created_at')
    )

    return [
        {
            'id': u.id,
            'first_name': u.first_name,
            'last_name': u.last_name,
            'position_id': u.position_id,
            'status': u.status,
            'created_at': u.created_at,
            'position_name': u.position.name if u.position else None,
            'position_level': u.position.level if u.position else None,
        }
        for u in downlines
    ]


def check_agent_upline_positions(agent_id: UUID) -> dict:
    """
    Check if all agents in the upline chain have positions assigned.
    Translated from Supabase RPC: check_agent_upline_positions

    Uses django-cte for upline traversal.

    Args:
        agent_id: The agent to start checking from

    Returns:
        Dictionary with:
        - has_all_positions: boolean
        - missing_positions: list of agents without positions
        - total_checked: count of checked agents
    """
    from apps.core.models import User

    def make_upline_cte(cte):
        # Anchor: start with the given agent
        anchor = (
            User.objects.filter(id=agent_id)  # type: ignore[attr-defined]
            .annotate(depth=Value(0, output_field=IntegerField()))
            .values('id', 'first_name', 'last_name', 'email', 'position_id', 'upline_id', 'depth')
        )

        # Recursive: follow upline chain (limit to 50)
        recursive = (
            cte.join(User, id=cte.col.upline_id)
            .annotate(depth=cte.col.depth + 1)
            .filter(depth__lt=50)
            .values('id', 'first_name', 'last_name', 'email', 'position_id', 'upline_id', 'depth')
        )

        return anchor.union(recursive, all=True)

    cte = With.recursive(make_upline_cte)

    results = list(
        cte.queryset()
        .with_cte(cte)
        .order_by('depth')
        .values('id', 'first_name', 'last_name', 'email', 'position_id', 'upline_id')
    )

    all_have_positions = True
    missing_positions = []

    for row in results:
        if row['position_id'] is None:
            all_have_positions = False
            missing_positions.append({
                'agent_id': str(row['id']),
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'email': row['email'],
                'is_top_of_hierarchy': row['upline_id'] is None,
            })

    return {
        'has_all_positions': all_have_positions,
        'missing_positions': missing_positions,
        'total_checked': len(results),
    }


def get_agents_debt_production(
    user_id: UUID,
    agent_ids: list[UUID],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Calculate debt and production metrics for agents.
    Translated from Supabase RPC: get_agents_debt_production

    Note: Kept as raw SQL due to complex multi-CTE aggregations.
    This is a P4 function - too complex for ORM conversion.

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
                FROM deal_hierarchy_snapshots dhs
                JOIN deals d ON d.id = dhs.deal_id
                JOIN agent_list al ON al.agent_id = dhs.agent_id
                WHERE d.policy_effective_date >= %s::date
                    AND d.policy_effective_date < %s::date
                    AND d.status_standardized NOT IN ('cancelled', 'lapsed', 'terminated')
                    AND dhs.agent_id <> d.agent_id
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
                FROM deal_hierarchy_snapshots dhs
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
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def get_agent_detail(agent_id: UUID, requesting_user_id: UUID) -> dict | None:
    """
    Get full agent details including profile, performance stats, and hierarchy info.
    Implements P1-007: Agent Detail Endpoint.

    Args:
        agent_id: The agent to get details for
        requesting_user_id: The requesting user's ID for permission checking

    Returns:
        Agent details dict or None if not found/not accessible
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE
            -- Get requesting user info for visibility check
            requesting_user AS (
                SELECT id, agency_id, is_admin, perm_level, role
                FROM users
                WHERE id = %s
            ),
            -- Get downline for visibility (if not admin)
            downline AS (
                SELECT id
                FROM users
                WHERE upline_id = (SELECT id FROM requesting_user)
                UNION ALL
                SELECT u.id
                FROM users u
                JOIN downline d ON u.upline_id = d.id
            ),
            -- Combined visibility
            visible_agents AS (
                SELECT id FROM requesting_user
                UNION
                SELECT id FROM downline
                UNION
                SELECT u.id FROM users u, requesting_user ru
                WHERE ru.is_admin = TRUE AND u.agency_id = ru.agency_id
            )
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.phone_number,
                u.status,
                u.role,
                u.perm_level,
                u.is_admin,
                u.is_active,
                u.subscription_tier,
                u.start_date,
                u.annual_goal,
                u.total_prod,
                u.total_policies_sold,
                u.theme_mode,
                u.created_at,
                u.updated_at,
                u.position_id,
                p.name as position_name,
                p.level as position_level,
                u.upline_id,
                upline.first_name || ' ' || upline.last_name as upline_name,
                u.agency_id,
                a.name as agency_name,
                (SELECT COUNT(*) FROM users WHERE upline_id = u.id) as direct_downline_count
            FROM users u
            JOIN visible_agents va ON va.id = u.id
            LEFT JOIN positions p ON p.id = u.position_id
            LEFT JOIN users upline ON upline.id = u.upline_id
            LEFT JOIN agencies a ON a.id = u.agency_id
            WHERE u.id = %s
        """, [str(requesting_user_id), str(agent_id)])

        row = cursor.fetchone()
        if not row:
            return None

        columns = [col[0] for col in cursor.description]
        agent = dict(zip(columns, row, strict=False))

        # Get performance metrics
        cursor.execute("""
            SELECT
                COALESCE(COUNT(*), 0) as total_deals,
                COALESCE(COUNT(*) FILTER (WHERE status_standardized = 'active'), 0) as active_deals,
                COALESCE(COUNT(*) FILTER (WHERE status_standardized = 'pending'), 0) as pending_deals,
                COALESCE(COUNT(*) FILTER (WHERE status_standardized IN ('cancelled', 'lapsed')), 0) as lost_deals,
                COALESCE(SUM(annual_premium) FILTER (WHERE status_standardized = 'active'), 0) as total_premium,
                COALESCE(AVG(annual_premium) FILTER (WHERE status_standardized = 'active'), 0) as avg_premium
            FROM deals
            WHERE agent_id = %s
        """, [str(agent_id)])

        perf_row = cursor.fetchone()
        perf_columns = [col[0] for col in cursor.description]
        performance = dict(zip(perf_columns, perf_row, strict=False)) if perf_row else {}

        # Get hierarchy depth
        cursor.execute("""
            WITH RECURSIVE upline_chain AS (
                SELECT id, upline_id, 0 as depth
                FROM users
                WHERE id = %s
                UNION ALL
                SELECT u.id, u.upline_id, uc.depth + 1
                FROM users u
                JOIN upline_chain uc ON u.id = uc.upline_id
                WHERE uc.depth < 20
            )
            SELECT MAX(depth) as hierarchy_depth
            FROM upline_chain
        """, [str(agent_id)])

        depth_row = cursor.fetchone()
        hierarchy_depth = depth_row[0] if depth_row and depth_row[0] else 0

        return {
            'id': str(agent['id']),
            'first_name': agent['first_name'],
            'last_name': agent['last_name'],
            'email': agent['email'],
            'phone': agent['phone_number'],
            'status': agent['status'],
            'role': agent['role'],
            'perm_level': agent['perm_level'],
            'is_admin': agent['is_admin'],
            'is_active': agent['is_active'],
            'subscription_tier': agent['subscription_tier'],
            'start_date': str(agent['start_date']) if agent['start_date'] else None,
            'annual_goal': float(agent['annual_goal']) if agent['annual_goal'] else None,
            'total_prod': float(agent['total_prod']) if agent['total_prod'] else 0,
            'total_policies_sold': agent['total_policies_sold'] or 0,
            'theme_mode': agent['theme_mode'],
            'created_at': agent['created_at'].isoformat() if agent['created_at'] else None,
            'updated_at': agent['updated_at'].isoformat() if agent['updated_at'] else None,
            'position': {
                'id': str(agent['position_id']) if agent['position_id'] else None,
                'name': agent['position_name'],
                'level': agent['position_level'],
            } if agent['position_id'] else None,
            'upline': {
                'id': str(agent['upline_id']) if agent['upline_id'] else None,
                'name': agent['upline_name'],
            } if agent['upline_id'] else None,
            'agency': {
                'id': str(agent['agency_id']) if agent['agency_id'] else None,
                'name': agent['agency_name'],
            },
            'hierarchy': {
                'depth': hierarchy_depth,
                'direct_downline_count': agent['direct_downline_count'] or 0,
            },
            'performance': {
                'total_deals': performance.get('total_deals', 0),
                'active_deals': performance.get('active_deals', 0),
                'pending_deals': performance.get('pending_deals', 0),
                'lost_deals': performance.get('lost_deals', 0),
                'total_premium': float(performance.get('total_premium', 0)),
                'avg_premium': float(performance.get('avg_premium', 0)),
            },
        }


def get_agent_downline_with_depth(
    agent_id: UUID,
    max_depth: int | None = None,
    include_self: bool = True,
) -> list[dict]:
    """
    Get all agents in the downline hierarchy with optional depth limit.
    Implements P1-008: Recursive Downline Endpoint.

    Args:
        agent_id: The root agent to get downline for
        max_depth: Maximum depth to traverse (None for unlimited, max 20)
        include_self: Whether to include the agent themselves

    Returns:
        List of agents in downline with depth level and details
    """
    effective_max_depth = min(max_depth or 20, 20)

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE downline AS (
                -- Anchor: the agent themselves at depth 0
                SELECT
                    id,
                    first_name,
                    last_name,
                    email,
                    phone_number,
                    status,
                    position_id,
                    upline_id,
                    0 as depth
                FROM users
                WHERE id = %s

                UNION ALL

                -- Recursive: get children, increment depth
                SELECT
                    u.id,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.phone_number,
                    u.status,
                    u.position_id,
                    u.upline_id,
                    d.depth + 1
                FROM users u
                JOIN downline d ON u.upline_id = d.id
                WHERE d.depth < %s
            )
            SELECT
                d.id,
                d.first_name,
                d.last_name,
                d.email,
                d.phone_number,
                d.status,
                d.depth,
                d.position_id,
                p.name as position_name,
                p.level as position_level,
                d.upline_id,
                (SELECT COUNT(*) FROM users WHERE upline_id = d.id) as direct_downline_count
            FROM downline d
            LEFT JOIN positions p ON p.id = d.position_id
            WHERE %s OR d.depth > 0
            ORDER BY d.depth, d.last_name, d.first_name
        """, [str(agent_id), effective_max_depth, include_self])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        return [
            {
                'id': str(row[columns.index('id')]),
                'first_name': row[columns.index('first_name')],
                'last_name': row[columns.index('last_name')],
                'email': row[columns.index('email')],
                'phone': row[columns.index('phone_number')],
                'status': row[columns.index('status')],
                'depth': row[columns.index('depth')],
                'position': {
                    'id': str(row[columns.index('position_id')]) if row[columns.index('position_id')] else None,
                    'name': row[columns.index('position_name')],
                    'level': row[columns.index('position_level')],
                } if row[columns.index('position_id')] else None,
                'upline_id': str(row[columns.index('upline_id')]) if row[columns.index('upline_id')] else None,
                'direct_downline_count': row[columns.index('direct_downline_count')] or 0,
            }
            for row in rows
        ]
