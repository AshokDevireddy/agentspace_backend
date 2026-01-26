"""
Centralized Hierarchy Traversal Utilities for AgentSpace.

This module provides optimized functions for traversing user hierarchies
using PostgreSQL recursive CTEs. These functions are used throughout the
application for downline/upline operations.

All functions use raw SQL for optimal performance.
"""
from uuid import UUID

from django.db import connection


def get_downline_ids(
    user_id: UUID,
    agency_id: UUID,
    max_depth: int | None = None,
    include_self: bool = False
) -> list[UUID]:
    """
    Get all user IDs in a user's downline (recursive).

    Uses a recursive CTE for efficient hierarchy traversal.

    Args:
        user_id: The root user ID to start traversal from
        agency_id: Agency ID for multi-tenancy filtering
        max_depth: Maximum depth to traverse (None for unlimited)
        include_self: Whether to include the user themselves in the result

    Returns:
        List of user IDs in the downline
    """
    # Validate max_depth to prevent SQL injection - must be positive integer or None
    if max_depth is not None:
        max_depth = int(max_depth)
        if max_depth < 1:
            max_depth = None

    with connection.cursor() as cursor:
        if max_depth is not None:
            # Use parameterized query with depth limit
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT id, 1 as depth
                    FROM public.users
                    WHERE upline_id = %s AND agency_id = %s

                    UNION ALL

                    SELECT u.id, d.depth + 1
                    FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s AND d.depth < %s
                )
                SELECT id FROM downline
            """, [str(user_id), str(agency_id), str(agency_id), max_depth])
        else:
            # No depth limit
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT id, 1 as depth
                    FROM public.users
                    WHERE upline_id = %s AND agency_id = %s

                    UNION ALL

                    SELECT u.id, d.depth + 1
                    FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s
                )
                SELECT id FROM downline
            """, [str(user_id), str(agency_id), str(agency_id)])

        result = [row[0] for row in cursor.fetchall()]

    if include_self:
        result.insert(0, user_id)

    return result


def get_upline_ids(user_id: UUID, include_self: bool = False) -> list[UUID]:
    """
    Get the chain of uplines from a user to the root.

    Uses a recursive CTE for efficient traversal.

    Args:
        user_id: The starting user ID
        include_self: Whether to include the user themselves in the result

    Returns:
        List of user IDs from direct upline to root (ordered by proximity)
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE upline_chain AS (
                SELECT id, upline_id, 1 as depth
                FROM public.users
                WHERE id = %s

                UNION ALL

                SELECT u.id, u.upline_id, uc.depth + 1
                FROM public.users u
                JOIN upline_chain uc ON u.id = uc.upline_id
                WHERE u.id IS NOT NULL
            )
            SELECT id FROM upline_chain
            WHERE depth > 1
            ORDER BY depth
        """, [str(user_id)])
        result = [row[0] for row in cursor.fetchall()]

    if include_self:
        result.insert(0, user_id)

    return result


def is_in_downline(upline_id: UUID, target_id: UUID, agency_id: UUID) -> bool:
    """
    Check if a target user is in an upline's downline.

    Uses a recursive CTE for efficient traversal with early exit.

    Args:
        upline_id: The potential upline user ID
        target_id: The user ID to check if in downline
        agency_id: Agency ID for multi-tenancy filtering

    Returns:
        True if target is in upline's downline, False otherwise
    """
    # Quick check: user is not in their own downline
    if str(upline_id) == str(target_id):
        return False

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE downline AS (
                SELECT id FROM public.users WHERE id = %s

                UNION ALL

                SELECT u.id FROM public.users u
                JOIN downline d ON u.upline_id = d.id
                WHERE u.agency_id = %s
            )
            SELECT 1 FROM downline WHERE id = %s LIMIT 1
        """, [str(upline_id), str(agency_id), str(target_id)])
        return cursor.fetchone() is not None


def get_all_agency_agent_ids(agency_id: UUID, exclude_clients: bool = True) -> list[UUID]:
    """
    Get all agent IDs in an agency.

    Args:
        agency_id: The agency ID
        exclude_clients: Whether to exclude users with role='client'

    Returns:
        List of user IDs in the agency
    """
    with connection.cursor() as cursor:
        if exclude_clients:
            cursor.execute("""
                SELECT id FROM public.users
                WHERE agency_id = %s AND role != 'client'
            """, [str(agency_id)])
        else:
            cursor.execute("""
                SELECT id FROM public.users
                WHERE agency_id = %s
            """, [str(agency_id)])
        return [row[0] for row in cursor.fetchall()]


def is_in_agency(user_id: UUID, agency_id: UUID) -> bool:
    """
    Check if a user belongs to an agency.

    Args:
        user_id: The user ID to check
        agency_id: The agency ID

    Returns:
        True if user is in agency, False otherwise
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 1 FROM public.users
            WHERE id = %s AND agency_id = %s
            LIMIT 1
        """, [str(user_id), str(agency_id)])
        return cursor.fetchone() is not None
