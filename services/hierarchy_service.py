"""
Hierarchy Service (P1-016)

Provides methods for navigating and validating the agent hierarchy.
"""
import logging
from typing import Optional
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)


class HierarchyService:
    """
    Service for managing agent hierarchy relationships.

    All methods operate within the context of a single agency for multi-tenancy.
    """

    @staticmethod
    def get_downline(
        user_id: UUID,
        agency_id: UUID,
        max_depth: int | None = None,
        include_self: bool = False
    ) -> list[UUID]:
        """
        Get all agents in a user's downline (recursive).

        Uses a recursive CTE for efficient hierarchy traversal.

        Args:
            user_id: The root user ID
            agency_id: Agency ID for multi-tenancy filtering
            max_depth: Maximum depth to traverse (None for unlimited)
            include_self: Whether to include the user themselves

        Returns:
            List of user IDs in the downline
        """
        depth_clause = f"AND depth < {max_depth}" if max_depth else ""

        with connection.cursor() as cursor:
            cursor.execute(f"""
                WITH RECURSIVE downline AS (
                    SELECT id, 1 as depth
                    FROM public.users
                    WHERE upline_id = %s AND agency_id = %s

                    UNION ALL

                    SELECT u.id, d.depth + 1
                    FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s {depth_clause}
                )
                SELECT id FROM downline
            """, [str(user_id), str(agency_id), str(agency_id)])

            result = [row[0] for row in cursor.fetchall()]

            if include_self:
                result.insert(0, user_id)

            return result

    @staticmethod
    def get_upline_chain(user_id: UUID) -> list[UUID]:
        """
        Get the chain of uplines from a user to the root.

        Args:
            user_id: The starting user ID

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
            return [row[0] for row in cursor.fetchall()]

    @staticmethod
    def get_visible_agent_ids(
        user_id: UUID,
        agency_id: UUID,
        is_admin: bool = False,
        include_full_agency: bool = False
    ) -> list[UUID]:
        """
        Get list of agent IDs visible to a user based on their role and hierarchy.

        Args:
            user_id: The requesting user's ID
            agency_id: Agency ID for multi-tenancy
            is_admin: Whether the user is an admin
            include_full_agency: If True and user is admin, return all agency agents

        Returns:
            List of agent UUIDs the user can access
        """
        if include_full_agency and is_admin:
            # Admin sees all agents in agency
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM public.users
                    WHERE agency_id = %s AND role != 'client'
                """, [str(agency_id)])
                return [row[0] for row in cursor.fetchall()]

        # Regular user sees themselves and their downline
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT id FROM public.users WHERE id = %s

                    UNION ALL

                    SELECT u.id FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s
                )
                SELECT id FROM downline
            """, [str(user_id), str(agency_id)])
            return [row[0] for row in cursor.fetchall()]

    @staticmethod
    def is_in_hierarchy(
        user_id: UUID,
        target_id: UUID,
        agency_id: UUID,
        direction: str = 'downline'
    ) -> bool:
        """
        Check if a target user is in the user's hierarchy.

        Args:
            user_id: The reference user ID
            target_id: The target user to check
            agency_id: Agency ID for multi-tenancy
            direction: 'downline' or 'upline'

        Returns:
            True if target is in the specified hierarchy direction
        """
        if str(user_id) == str(target_id):
            return True

        if direction == 'downline':
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
                """, [str(user_id), str(agency_id), str(target_id)])
                return cursor.fetchone() is not None
        else:  # upline
            with connection.cursor() as cursor:
                cursor.execute("""
                    WITH RECURSIVE upline AS (
                        SELECT id, upline_id FROM public.users WHERE id = %s

                        UNION ALL

                        SELECT u.id, u.upline_id FROM public.users u
                        JOIN upline ul ON u.id = ul.upline_id
                        WHERE u.id IS NOT NULL
                    )
                    SELECT 1 FROM upline WHERE id = %s LIMIT 1
                """, [str(user_id), str(target_id)])
                return cursor.fetchone() is not None

    @staticmethod
    def can_access_user(
        requesting_user_id: UUID,
        requesting_user_agency_id: UUID,
        requesting_user_is_admin: bool,
        target_user_id: UUID
    ) -> bool:
        """
        Check if a user can access another user's data.

        Rules:
        - Users can always access their own data
        - Admins can access anyone in their agency
        - Agents can access their downlines

        Args:
            requesting_user_id: The user making the request
            requesting_user_agency_id: The requesting user's agency
            requesting_user_is_admin: Whether the requesting user is admin
            target_user_id: The user whose data is being accessed

        Returns:
            True if access is allowed
        """
        # Users can always access themselves
        if str(requesting_user_id) == str(target_user_id):
            return True

        # Admins can access anyone in their agency
        if requesting_user_is_admin:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM public.users
                    WHERE id = %s AND agency_id = %s
                    LIMIT 1
                """, [str(target_user_id), str(requesting_user_agency_id)])
                return cursor.fetchone() is not None

        # Check if target is in downline
        return HierarchyService.is_in_hierarchy(
            requesting_user_id,
            target_user_id,
            requesting_user_agency_id,
            direction='downline'
        )

    @staticmethod
    def get_hierarchy_depth(user_id: UUID, agency_id: UUID) -> int:
        """
        Get the depth of a user in the hierarchy (distance from root).

        Args:
            user_id: The user ID
            agency_id: Agency ID

        Returns:
            Depth (0 for root, 1 for their direct downlines, etc.)
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE upline AS (
                    SELECT id, upline_id, 0 as depth
                    FROM public.users
                    WHERE id = %s AND agency_id = %s

                    UNION ALL

                    SELECT u.id, u.upline_id, ul.depth + 1
                    FROM public.users u
                    JOIN upline ul ON u.id = ul.upline_id
                    WHERE u.agency_id = %s
                )
                SELECT MAX(depth) FROM upline
            """, [str(user_id), str(agency_id), str(agency_id)])
            result = cursor.fetchone()
            return result[0] if result and result[0] is not None else 0

    @staticmethod
    def validate_upline_assignment(
        agent_id: UUID,
        new_upline_id: UUID,
        agency_id: UUID
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that an upline assignment won't create a cycle.

        Args:
            agent_id: The agent being assigned a new upline
            new_upline_id: The proposed new upline
            agency_id: Agency ID

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Can't be your own upline
        if str(agent_id) == str(new_upline_id):
            return False, "An agent cannot be their own upline"

        # Check if new upline is in agent's downline (would create cycle)
        if HierarchyService.is_in_hierarchy(
            agent_id, new_upline_id, agency_id, direction='downline'
        ):
            return False, "Cannot assign upline that is in agent's downline (would create cycle)"

        # Verify new upline exists and is in same agency
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM public.users
                WHERE id = %s AND agency_id = %s
                LIMIT 1
            """, [str(new_upline_id), str(agency_id)])
            if cursor.fetchone() is None:
                return False, "New upline not found in agency"

        return True, None
