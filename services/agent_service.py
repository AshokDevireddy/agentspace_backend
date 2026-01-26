"""
Agent Service

Translated from Supabase RPC functions related to agent management,
hierarchy operations, and agent options.

Priority: P0 - Core Operations, P0 - Hierarchy
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .base import BaseService, PaginationResult

# ============================================================================
# Data Transfer Objects (DTOs)
# ============================================================================

@dataclass
class AgentDownlineResult:
    """Result from get_agent_downline."""
    id: UUID
    first_name: str
    last_name: str
    email: str
    level: int  # Depth in hierarchy (0 = self)


@dataclass
class AgentUplineChainResult:
    """Result from get_agent_upline_chain."""
    agent_id: UUID
    upline_id: UUID | None
    depth: int  # 0 = self, 1 = direct upline, etc.


@dataclass
class AgentOption:
    """Result from get_agent_options."""
    agent_id: UUID
    display_name: str  # Format: "LastName, FirstName"


@dataclass
class AgentTableRow:
    """Result from get_agents_table."""
    agent_id: UUID
    first_name: str
    last_name: str
    perm_level: str
    upline_id: UUID | None
    upline_name: str | None
    created_at: str
    status: str
    total_prod: float
    total_policies_sold: int
    downline_count: int
    position_id: UUID | None
    position_name: str | None
    position_level: int | None
    total_count: int  # For pagination


@dataclass
class AgentDebtProductionResult:
    """Result from get_agents_debt_production."""
    agent_id: UUID
    individual_debt: float
    individual_debt_count: int
    individual_production: float
    individual_production_count: int
    hierarchy_debt: float
    hierarchy_debt_count: int
    hierarchy_production: float
    hierarchy_production_count: int
    debt_to_production_ratio: float


# ============================================================================
# Agent Service Implementation
# ============================================================================

class AgentService(BaseService):
    """
    Service for agent-related operations.

    Handles:
    - Agent hierarchy (upline/downline)
    - Agent options for dropdowns
    - Agent table with filtering and pagination
    - Agent debt/production calculations
    """

    # ========================================================================
    # P0 - Hierarchy Functions
    # ========================================================================

    def get_agent_downline(self, agent_id: UUID) -> list[AgentDownlineResult]:
        """
        Translated from Supabase RPC: get_agent_downline

        Get all agents in the downline hierarchy of a given agent.

        Original SQL Logic:
        - Uses recursive CTE starting from agent_id
        - Joins users on upline_id to traverse downward
        - Includes depth level (0 = self, 1 = direct reports, etc.)
        - Orders by depth, then last_name, first_name

        Args:
            agent_id: The root agent to get downline for

        Returns:
            List[AgentDownlineResult]: All agents in downline including self
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH RECURSIVE downline AS (
        #   SELECT u.id, u.first_name, u.last_name, u.email, 0 as depth
        #   FROM public.users u
        #   WHERE u.id = agent_id
        #   UNION ALL
        #   SELECT u.id, u.first_name, u.last_name, u.email, d.depth + 1
        #   FROM public.users u
        #   JOIN downline d ON u.upline_id = d.id
        # )
        # SELECT d.id, d.first_name, d.last_name, d.email, d.depth as level
        # FROM downline d
        # ORDER BY d.depth, d.last_name, d.first_name;
        return []

    def get_agent_upline_chain(self, agent_id: UUID) -> list[AgentUplineChainResult]:
        """
        Translated from Supabase RPC: get_agent_upline_chain

        Get the complete upline chain from an agent to the top of hierarchy.

        Original SQL Logic:
        - Uses recursive CTE starting from agent_id
        - Traverses upward via upline_id
        - Includes depth (0 = self, positive = ancestors)
        - Orders by depth ascending

        Args:
            agent_id: The agent to get upline chain for

        Returns:
            List[AgentUplineChainResult]: Upline chain from self to top
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH RECURSIVE upline_chain AS (
        #   SELECT u.id AS agent_id, u.upline_id, 0 AS depth
        #   FROM public.users u
        #   WHERE u.id = p_agent_id
        #   UNION ALL
        #   SELECT u.id AS agent_id, u.upline_id, uc.depth + 1 AS depth
        #   FROM public.users u
        #   INNER JOIN upline_chain uc ON u.id = uc.upline_id
        #   WHERE uc.upline_id IS NOT NULL
        # )
        # SELECT uc.agent_id, uc.upline_id, uc.depth
        # FROM upline_chain uc
        # ORDER BY uc.depth ASC;
        return []

    # ========================================================================
    # P0 - Core Operations
    # ========================================================================

    def get_agent_options(
        self,
        include_full_agency: bool = False
    ) -> list[AgentOption]:
        """
        Translated from Supabase RPC: get_agent_options

        Get agent options for dropdown/select components.

        Original SQL Logic:
        - Gets all agents in user's agency (excluding clients)
        - If include_full_agency=False, filters to only user's downline
        - Returns agent_id and display_name (LastName, FirstName format)
        - Orders by last_name, first_name

        Args:
            include_full_agency: If True, include all agency agents.
                                If False, only include user's downline.

        Returns:
            List[AgentOption]: Agent options for dropdowns
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH current_usr AS (
        #   SELECT id, agency_id FROM users WHERE id = p_user_id LIMIT 1
        # ),
        # all_agents AS (
        #   SELECT u.id, u.first_name, u.last_name, u.role
        #   FROM users u
        #   JOIN current_usr cu ON cu.agency_id = u.agency_id
        #   WHERE u.role <> 'client'
        # ),
        # downline AS (
        #   SELECT gd.id FROM current_usr cu
        #   JOIN public.get_agent_downline(cu.id) gd ON true
        # )
        # SELECT a.id as agent_id,
        #        concat(a.last_name, ', ', a.first_name) as display_name
        # FROM all_agents a
        # JOIN current_usr cu ON true
        # WHERE p_include_full_agency
        #    OR EXISTS (SELECT 1 FROM downline d WHERE d.id = a.id)
        # ORDER BY a.last_name, a.first_name;
        return []

    def get_agents_table(
        self,
        filters: dict[str, Any] | None = None,
        include_full_agency: bool = False,
        limit: int = 50,
        offset: int = 0
    ) -> PaginationResult[AgentTableRow]:
        """
        Translated from Supabase RPC: get_agents_table

        Get paginated agent table data with filtering.

        Original SQL Logic:
        - Gets agents visible to user (based on hierarchy or full agency)
        - Supports multiple filter types:
          - status: Filter by agent status
          - agent_name: Filter by specific agent name
          - in_upline/in_downline: Filter by hierarchy relationship
          - direct_upline/direct_downline: Filter by direct relationship
          - position_id: Filter by position (supports 'all' keyword)
        - Joins position data and calculates downline counts
        - Returns paginated results with total_count

        Args:
            filters: Optional filter dictionary with keys:
                - status: Agent status filter
                - agent_name: Specific agent name
                - in_upline: Agent name that must be in upline
                - direct_upline: Direct upline agent name (or 'all')
                - in_downline: Agent name that must be in downline
                - direct_downline: Direct report agent name
                - position_id: Position UUID or 'all'
            include_full_agency: Include all agency agents
            limit: Page size
            offset: Page offset

        Returns:
            PaginationResult[AgentTableRow]: Paginated agent data
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL is complex with multiple CTEs:
        # 1. current_usr - Get requesting user's context
        # 2. filters - Parse filter JSON
        # 3. all_agents - Get all agents in agency
        # 4. downline - Get user's downline for visibility
        # 5. visible_base - Apply visibility rules
        # 6. name_targets - Resolve name filters to IDs
        # 7. downline_relationships - Build relationship graph
        # 8. filtered - Apply all filters
        # 9. Final SELECT with JOINs for upline_name, position, downline_count
        return PaginationResult(items=[], total_count=0, has_more=False)

    # ========================================================================
    # P0 - Commission/Money Functions
    # ========================================================================

    def get_agents_debt_production(
        self,
        agent_ids: list[UUID],
        start_date: str,
        end_date: str
    ) -> list[AgentDebtProductionResult]:
        """
        Translated from Supabase RPC: get_agents_debt_production

        Calculate debt and production metrics for multiple agents.

        Original SQL Logic:
        - Builds hierarchy tree for all requested agents using recursive CTE
        - Calculates individual debt (from lapsed deals with negative status):
          - Early lapse (<=30 days): Full commission is debt
          - Late lapse (>30 days): Prorated over 9 months
        - Calculates individual production (writing agent's deals)
        - Calculates team production (via deal_hierarchy_snapshot)
        - Calculates hierarchy metrics (downlines only, excluding self)
        - Returns debt-to-production ratio

        Args:
            agent_ids: List of agent UUIDs to calculate metrics for
            start_date: Start of date range (policy_effective_date)
            end_date: End of date range

        Returns:
            List[AgentDebtProductionResult]: Metrics for each agent
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL uses multiple CTEs:
        # 1. agent_tree - Recursive CTE building hierarchy for all agents
        # 2. individual_debt_calc - Calculate debt from lapsed deals
        #    - Uses deal_hierarchy_snapshot and status_mapping
        #    - Early lapse: full commission * (agent_pct / total_pct)
        #    - Late lapse: prorated over 9 months
        # 3. individual_prod_calc - Personal deals production
        #    - Excludes negative status (unless lapsed >7 days)
        # 4. team_prod_calc - Team production via deal_hierarchy_snapshot
        # 5. hierarchy_metrics - Aggregate downline metrics
        # 6. Final SELECT combining all calculations
        return []

    def get_agent_debt(self, agent_id: UUID) -> dict[str, Any]:
        """
        Translated from Supabase RPC: get_agent_debt

        Get detailed debt information for a specific agent.

        Original SQL Logic:
        - Authorization check (admin or in downline)
        - Finds lapsed deals where agent is in hierarchy
        - Uses status_mapping to identify negative impact
        - Calculates debt based on:
          - Original commission: annual_premium * 0.75 * (agent_pct / total_pct)
          - Early lapse (<=30 days): Full commission is debt
          - Late lapse: Prorated over 9 months remaining

        Args:
            agent_id: The agent to get debt for

        Returns:
            dict: {
                'total_debt': Decimal,
                'lapsed_deals_count': int,
                'debt_breakdown': List[dict] - Per-deal breakdown
            }
        """
        # TODO: Implement Django ORM equivalent
        # Verify authorization first
        if not self._check_hierarchy_access(agent_id):
            return {
                'total_debt': 0,
                'lapsed_deals_count': 0,
                'debt_breakdown': []
            }

        # Original SQL:
        # WITH lapsed_deals AS (
        #   SELECT d.id, d.annual_premium, d.policy_effective_date,
        #          d.updated_at AS lapse_date, d.status, d.carrier_id,
        #          d.client_name, d.policy_number,
        #          dhs.commission_percentage AS agent_commission_pct,
        #          (SELECT SUM(commission_percentage) FROM deal_hierarchy_snapshot
        #           WHERE deal_id = d.id) AS hierarchy_total_pct
        #   FROM deals d
        #   INNER JOIN deal_hierarchy_snapshot dhs ON dhs.deal_id = d.id
        #   INNER JOIN status_mapping sm ON sm.carrier_id = d.carrier_id
        #     AND LOWER(sm.raw_status) = LOWER(d.status) AND sm.impact = 'negative'
        #   WHERE dhs.agent_id = p_agent_id
        #     AND d.annual_premium IS NOT NULL
        #     AND d.policy_effective_date IS NOT NULL
        #     AND dhs.commission_percentage IS NOT NULL
        # ),
        # debt_calculations AS (
        #   -- Calculate days_active, months_active, original_commission
        #   -- Determine if early (<= 30 days) or late lapse
        # ),
        # final_debt AS (
        #   -- Calculate actual_debt based on early/late lapse rules
        # )
        return {
            'total_debt': 0,
            'lapsed_deals_count': 0,
            'debt_breakdown': []
        }


# ============================================================================
# Type Aliases for External Use
# ============================================================================

AgentServiceResult = AgentDownlineResult | AgentUplineChainResult | AgentOption | AgentTableRow
