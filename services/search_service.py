"""
Search Service

Translated from Supabase RPC functions related to fuzzy search
across agents, clients, and policies.

Priority: P2 - Search
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from .base import BaseService


# ============================================================================
# Data Transfer Objects (DTOs)
# ============================================================================

@dataclass
class FilterOption:
    """Generic filter option (value/label pair)."""
    value: str
    label: str


@dataclass
class AgentSearchResult:
    """Result from search_agents_fuzzy."""
    id: UUID
    first_name: str
    last_name: str
    email: str
    total_prod: Optional[Decimal]
    similarity_score: float
    match_type: str  # 'exact', 'fuzzy', 'suggestion'


@dataclass
class ClientSearchResult:
    """Result from search_clients_fuzzy."""
    id: UUID
    first_name: str
    last_name: str
    email: Optional[str]
    phone: Optional[str]
    agent_id: Optional[UUID]
    similarity_score: float
    match_type: str  # 'exact', 'fuzzy', 'suggestion'


@dataclass
class PolicySearchResult:
    """Result from search_policies_fuzzy."""
    id: UUID
    policy_number: Optional[str]
    application_number: Optional[str]
    client_name: str
    agent_id: UUID
    carrier_id: UUID
    annual_premium: Optional[Decimal]
    status_standardized: Optional[str]
    similarity_score: float
    match_type: str  # 'exact', 'fuzzy', 'suggestion'


# ============================================================================
# Search Service Implementation
# ============================================================================

class SearchService(BaseService):
    """
    Service for search operations across entities.

    Handles:
    - Filter option searches (for dropdowns)
    - Fuzzy search across agents, clients, policies
    - Similarity-based ranking
    """

    # ========================================================================
    # P2 - Filter Option Functions (for Dropdowns)
    # ========================================================================

    def search_agents_for_filter(
        self,
        search_term: str = '',
        limit: int = 20
    ) -> List[FilterOption]:
        """
        Translated from Supabase RPC: search_agents_for_filter

        Search agents for filter dropdown options.

        Original SQL Logic:
        - Scopes to user's agency
        - Non-admins: Only user's downline
        - Admins: All agency agents
        - Filters by first_name or last_name ILIKE search_term
        - Only includes agents with deals
        - Returns value (ID) and label (LastName, FirstName)
        - 5-second statement timeout for safety

        Args:
            search_term: Optional search string
            limit: Maximum results to return

        Returns:
            List[FilterOption]: Agent options for dropdown
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # SET LOCAL statement_timeout = '5s';
        #
        # SELECT u.agency_id, COALESCE(u.is_admin, false)
        #        OR u.perm_level = 'admin' OR u.role = 'admin'
        # INTO v_agency_id, v_is_admin
        # FROM users u WHERE u.id = p_user_id;
        #
        # IF NOT v_is_admin THEN
        #   SELECT array_agg(id) INTO v_agent_ids
        #   FROM (
        #     SELECT p_user_id AS id
        #     UNION
        #     SELECT id FROM public.get_agent_downline(p_user_id)
        #   ) scoped;
        # END IF;
        #
        # RETURN QUERY
        # SELECT u.id::text AS value,
        #        CONCAT(u.last_name, ', ', u.first_name) AS label
        # FROM users u
        # WHERE u.agency_id = v_agency_id
        #   AND u.role <> 'client'
        #   AND (v_is_admin OR u.id = ANY(COALESCE(v_agent_ids, ARRAY[]::uuid[])))
        #   AND (p_search_term = ''
        #        OR u.first_name ILIKE '%' || p_search_term || '%'
        #        OR u.last_name ILIKE '%' || p_search_term || '%')
        #   AND EXISTS (SELECT 1 FROM deals d WHERE d.agent_id = u.id)
        # ORDER BY u.last_name, u.first_name
        # LIMIT p_limit;
        pass

    def search_clients_for_filter(
        self,
        search_term: str = '',
        limit: int = 20
    ) -> List[FilterOption]:
        """
        Translated from Supabase RPC: search_clients_for_filter

        Search clients for filter dropdown options.

        Original SQL Logic:
        - Scopes to user's agency
        - Filters clients where role = 'client'
        - Searches first_name, last_name, email by ILIKE
        - Only includes clients with deals
        - Returns value (ID) and label (FirstName LastName)
        - 5-second statement timeout

        Args:
            search_term: Optional search string
            limit: Maximum results to return

        Returns:
            List[FilterOption]: Client options for dropdown
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # SELECT u.agency_id INTO v_agency_id
        # FROM users u WHERE u.id = p_user_id;
        #
        # RETURN QUERY
        # SELECT u.id::text AS value,
        #        CONCAT(u.first_name, ' ', u.last_name) AS label
        # FROM users u
        # WHERE u.agency_id = v_agency_id
        #   AND u.role = 'client'
        #   AND (p_search_term = ''
        #        OR u.first_name ILIKE '%' || p_search_term || '%'
        #        OR u.last_name ILIKE '%' || p_search_term || '%'
        #        OR u.email ILIKE '%' || p_search_term || '%')
        #   AND EXISTS (SELECT 1 FROM deals d WHERE d.client_id = u.id)
        # ORDER BY u.last_name, u.first_name
        # LIMIT p_limit;
        pass

    def search_policy_numbers_for_filter(
        self,
        search_term: str = '',
        limit: int = 20
    ) -> List[FilterOption]:
        """
        Search policy numbers for filter dropdown.

        Business Logic:
        - Scopes to user's visible deals (admin: all agency, else: hierarchy)
        - Searches policy_number by ILIKE
        - Returns value (policy_number) and label (policy_number - client_name)

        Args:
            search_term: Optional search string
            limit: Maximum results to return

        Returns:
            List[FilterOption]: Policy number options for dropdown
        """
        # TODO: Implement Django ORM equivalent
        pass

    # ========================================================================
    # P2 - Fuzzy Search Functions
    # ========================================================================

    def search_agents_fuzzy(
        self,
        query: str,
        allowed_agent_ids: Optional[List[UUID]] = None,
        limit: int = 20,
        similarity_threshold: float = 0.3
    ) -> List[AgentSearchResult]:
        """
        Translated from Supabase RPC: search_agents_fuzzy

        Fuzzy search agents using pg_trgm similarity.

        Original SQL Logic:
        - Uses PostgreSQL similarity() function (pg_trgm extension)
        - Calculates similarity score against:
          - first_name, last_name, email, full_name
        - Takes GREATEST of all similarity scores
        - Match types:
          - 'exact': Exact match on any field
          - 'fuzzy': ILIKE match (contains)
          - 'suggestion': Only similarity match
        - Filters: agency_id match, role != 'client', optional allowed_agent_ids
        - Filters by similarity_threshold
        - Orders by match_type priority, then similarity DESC

        Args:
            query: Search query string
            allowed_agent_ids: Optional list of agent IDs to restrict search
            limit: Maximum results to return
            similarity_threshold: Minimum similarity score (0-1)

        Returns:
            List[AgentSearchResult]: Ranked search results
        """
        # TODO: Implement Django ORM equivalent
        # Requires pg_trgm extension for similarity()
        #
        # Original SQL:
        # WITH search_results AS (
        #   SELECT
        #     u.id, u.first_name, u.last_name, u.email, u.total_prod,
        #     GREATEST(
        #       COALESCE(similarity(LOWER(u.first_name), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(u.last_name), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(u.email), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(CONCAT(u.first_name, ' ', u.last_name)), LOWER(p_query)), 0)
        #     ) as sim_score,
        #     CASE
        #       WHEN LOWER(u.first_name) = LOWER(p_query)
        #         OR LOWER(u.last_name) = LOWER(p_query)
        #         OR LOWER(u.email) = LOWER(p_query)
        #         OR LOWER(CONCAT(u.first_name, ' ', u.last_name)) = LOWER(p_query)
        #       THEN 'exact'
        #       WHEN LOWER(u.first_name) ILIKE '%' || LOWER(p_query) || '%'
        #         OR LOWER(u.last_name) ILIKE '%' || LOWER(p_query) || '%'
        #         OR LOWER(u.email) ILIKE '%' || LOWER(p_query) || '%'
        #       THEN 'fuzzy'
        #       ELSE 'suggestion'
        #     END as match_type
        #   FROM users u
        #   WHERE u.agency_id = p_agency_id
        #     AND u.role != 'client'
        #     AND (p_allowed_agent_ids IS NULL OR u.id = ANY(p_allowed_agent_ids))
        #     AND (COALESCE(u.first_name, '') % p_query
        #          OR COALESCE(u.last_name, '') % p_query
        #          OR COALESCE(u.email, '') % p_query
        #          OR CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')) % p_query)
        # )
        # SELECT * FROM search_results
        # WHERE sim_score >= p_similarity_threshold
        # ORDER BY
        #   CASE match_type WHEN 'exact' THEN 1 WHEN 'fuzzy' THEN 2 ELSE 3 END,
        #   sim_score DESC
        # LIMIT p_limit;
        pass

    def search_clients_fuzzy(
        self,
        query: str,
        allowed_agent_ids: Optional[List[UUID]] = None,
        limit: int = 20,
        similarity_threshold: float = 0.3
    ) -> List[ClientSearchResult]:
        """
        Translated from Supabase RPC: search_clients_fuzzy

        Fuzzy search clients using pg_trgm similarity.

        Original SQL Logic:
        - Similar to search_agents_fuzzy but searches clients table
        - Calculates similarity against:
          - first_name, last_name, email, phone, full_name
        - Same match_type classification
        - Filters by agency_id and optional allowed_agent_ids
        - Uses pg_trgm % operator for trigram matching

        Args:
            query: Search query string
            allowed_agent_ids: Optional list to restrict by client's agent
            limit: Maximum results to return
            similarity_threshold: Minimum similarity score (0-1)

        Returns:
            List[ClientSearchResult]: Ranked search results
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH search_results AS (
        #   SELECT c.id, c.first_name, c.last_name, c.email, c.phone, c.agent_id,
        #     GREATEST(
        #       COALESCE(similarity(LOWER(c.first_name), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(c.last_name), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(c.email), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(c.phone), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(CONCAT(c.first_name, ' ', c.last_name)), LOWER(p_query)), 0)
        #     ) as sim_score,
        #     CASE ... END as match_type
        #   FROM clients c
        #   WHERE c.agency_id = p_agency_id
        #     AND (p_allowed_agent_ids IS NULL OR c.agent_id = ANY(p_allowed_agent_ids))
        #     AND (COALESCE(c.first_name, '') % p_query
        #          OR COALESCE(c.last_name, '') % p_query
        #          OR COALESCE(c.email, '') % p_query
        #          OR COALESCE(c.phone, '') % p_query
        #          OR CONCAT(...) % p_query)
        # )
        # SELECT * FROM search_results
        # WHERE sim_score >= p_similarity_threshold
        # ORDER BY match_type priority, sim_score DESC
        # LIMIT p_limit;
        pass

    def search_policies_fuzzy(
        self,
        query: str,
        allowed_agent_ids: Optional[List[UUID]] = None,
        limit: int = 20,
        similarity_threshold: float = 0.3
    ) -> List[PolicySearchResult]:
        """
        Translated from Supabase RPC: search_policies_fuzzy

        Fuzzy search policies/deals using pg_trgm similarity.

        Original SQL Logic:
        - Searches deals table
        - Calculates similarity against:
          - policy_number, application_number, client_name
        - Same match_type classification
        - Filters by agency_id and optional allowed_agent_ids
        - Returns deal info with status_standardized

        Args:
            query: Search query string
            allowed_agent_ids: Optional list to restrict by deal's agent
            limit: Maximum results to return
            similarity_threshold: Minimum similarity score (0-1)

        Returns:
            List[PolicySearchResult]: Ranked search results
        """
        # TODO: Implement Django ORM equivalent
        # Original SQL:
        # WITH search_results AS (
        #   SELECT d.id, d.policy_number, d.application_number, d.client_name,
        #          d.agent_id, d.carrier_id, d.annual_premium, d.status_standardized,
        #     GREATEST(
        #       COALESCE(similarity(LOWER(d.policy_number), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(d.application_number), LOWER(p_query)), 0),
        #       COALESCE(similarity(LOWER(d.client_name), LOWER(p_query)), 0)
        #     ) as sim_score,
        #     CASE
        #       WHEN LOWER(d.policy_number) = LOWER(p_query)
        #         OR LOWER(d.application_number) = LOWER(p_query)
        #         OR LOWER(d.client_name) = LOWER(p_query)
        #       THEN 'exact'
        #       WHEN LOWER(d.policy_number) ILIKE '%' || LOWER(p_query) || '%'
        #         OR LOWER(d.application_number) ILIKE '%' || LOWER(p_query) || '%'
        #         OR LOWER(d.client_name) ILIKE '%' || LOWER(p_query) || '%'
        #       THEN 'fuzzy'
        #       ELSE 'suggestion'
        #     END as match_type
        #   FROM deals d
        #   WHERE d.agency_id = p_agency_id
        #     AND (p_allowed_agent_ids IS NULL OR d.agent_id = ANY(p_allowed_agent_ids))
        #     AND (COALESCE(d.policy_number, '') % p_query
        #          OR COALESCE(d.application_number, '') % p_query
        #          OR COALESCE(d.client_name, '') % p_query)
        # )
        # SELECT * FROM search_results
        # WHERE sim_score >= p_similarity_threshold
        # ORDER BY match_type priority, sim_score DESC
        # LIMIT p_limit;
        pass


# ============================================================================
# Type Aliases for External Use
# ============================================================================

SearchServiceResult = FilterOption | AgentSearchResult | ClientSearchResult | PolicySearchResult
