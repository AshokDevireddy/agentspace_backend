"""
Search API Views

Provides search endpoints:
- GET /api/search-agents - Search agents in downline
- GET /api/deals/search-clients - Search clients for filter
"""
import logging
import re

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    search_agents_downline,
    search_agents_all,
    search_clients_for_filter,
    search_agents_for_filter,
    search_agents_fuzzy,
    search_clients_fuzzy,
    search_policies_fuzzy,
    search_policy_numbers_for_filter,
)

logger = logging.getLogger(__name__)


class SearchAgentsView(APIView):
    """
    GET /api/search-agents

    Search for agents within user's downline or agency.

    Query params:
        q: Search query (min 2 chars, or empty with format=options)
        limit: Max results (default: 10, max: 20)
        type: 'downline' (default) or 'pre-invite'
        format: 'options' returns {value, label} format for select
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        query = request.query_params.get('q', '').strip()
        limit_param = request.query_params.get('limit')
        search_type = request.query_params.get('type', 'downline')
        new_format = request.query_params.get('format')

        # Validate query
        allow_empty = new_format == 'options'
        if not query or len(query) < 2:
            if not allow_empty:
                return Response(
                    {'error': 'Search query must be at least 2 characters long'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Parse limit
        limit = 10
        if limit_param:
            try:
                limit = min(int(limit_param), 20)
                if limit <= 0:
                    raise ValueError()
            except ValueError:
                return Response(
                    {'error': 'Invalid limit parameter'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Sanitize query for SQL LIKE
        sanitized_query = re.sub(r'[%_]', r'\\\g<0>', query)

        try:
            if query and len(query) >= 2:
                agents = search_agents_downline(
                    user.id,
                    sanitized_query,
                    limit=50,  # Get more for client-side filtering
                    search_type=search_type,
                    agency_id=user.agency_id if search_type == 'pre-invite' else None,
                )

                # Multi-word search filtering
                search_words = sanitized_query.split()
                if len(search_words) > 1:
                    filtered = []
                    for agent in agents:
                        full_name = f"{agent.get('first_name', '')} {agent.get('last_name', '')}".lower()
                        email = (agent.get('email') or '').lower()
                        query_lower = sanitized_query.lower()

                        # Check full query match
                        if query_lower in full_name or query_lower in email:
                            filtered.append(agent)
                            continue

                        # Check all words match
                        all_match = all(
                            word.lower() in full_name or word.lower() in email
                            for word in search_words
                        )
                        if all_match:
                            filtered.append(agent)

                    agents = filtered
            else:
                # No query - return all downline agents
                agents = search_agents_all(user.id, user.agency_id, limit=50)

            # Apply limit
            agents = agents[:limit]

            # Format response
            if new_format == 'options':
                options = [
                    {
                        'value': str(agent['id']),
                        'label': f"{agent['first_name']} {agent['last_name']}"
                               + (f" - {agent['email']}" if agent.get('email') else "")
                               + (" (Pre-invite)" if agent.get('status') == 'pre-invite' else ""),
                    }
                    for agent in agents
                ]
                return Response(options)

            return Response(agents)

        except Exception as e:
            logger.error(f'Search agents failed: {e}')
            return Response(
                {'error': 'Internal Server Error', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchClientsView(APIView):
    """
    GET /api/deals/search-clients

    Search clients for deal filter dropdown.

    Query params:
        q: Search query
        limit: Max results (default: 20, max: 100)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        search_term = request.query_params.get('q', '')
        limit_param = request.query_params.get('limit', '20')

        try:
            limit = min(int(limit_param), 100)
        except ValueError:
            limit = 20

        try:
            clients = search_clients_for_filter(
                user.id,
                search_term,
                limit=limit,
            )
            return Response(clients)
        except Exception as e:
            logger.error(f'Search clients failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchAgentsForFilterView(APIView):
    """
    GET /api/deals/search-agents

    Search agents for filter dropdown (with deals only).
    Translated from Supabase RPC: search_agents_for_filter

    Query params:
        q: Search term (empty returns all)
        limit: Max results (default: 20)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        search_term = request.query_params.get('q', '')
        try:
            limit = min(int(request.query_params.get('limit', 20)), 100)
        except ValueError:
            limit = 20

        try:
            results = search_agents_for_filter(user.id, search_term, limit)
            return Response(results)
        except Exception as e:
            logger.error(f'Search agents for filter failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchAgentsFuzzyView(APIView):
    """
    GET /api/search-agents/fuzzy

    Fuzzy search for agents using pg_trgm similarity.
    Translated from Supabase RPC: search_agents_fuzzy

    Query params:
        q: Search query (required)
        limit: Max results (default: 20)
        threshold: Similarity threshold 0.0-1.0 (default: 0.3)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        query = request.query_params.get('q', '').strip()
        if not query:
            return Response(
                {'error': 'Search query is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            limit = min(int(request.query_params.get('limit', 20)), 100)
        except ValueError:
            limit = 20

        try:
            threshold = float(request.query_params.get('threshold', 0.3))
            threshold = max(0.0, min(1.0, threshold))
        except ValueError:
            threshold = 0.3

        # Get allowed agent IDs based on user permissions
        is_admin = user.is_admin or user.role == 'admin'
        allowed_ids = None if is_admin else None  # Fuzzy search handles this internally

        try:
            results = search_agents_fuzzy(
                query=query,
                agency_id=user.agency_id,
                allowed_agent_ids=allowed_ids,
                limit=limit,
                similarity_threshold=threshold,
            )
            return Response(results)
        except Exception as e:
            logger.error(f'Fuzzy search agents failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchClientsFuzzyView(APIView):
    """
    GET /api/search-clients/fuzzy

    Fuzzy search for clients using pg_trgm similarity.
    Translated from Supabase RPC: search_clients_fuzzy

    Query params:
        q: Search query (required)
        limit: Max results (default: 20)
        threshold: Similarity threshold 0.0-1.0 (default: 0.3)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        query = request.query_params.get('q', '').strip()
        if not query:
            return Response(
                {'error': 'Search query is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            limit = min(int(request.query_params.get('limit', 20)), 100)
        except ValueError:
            limit = 20

        try:
            threshold = float(request.query_params.get('threshold', 0.3))
            threshold = max(0.0, min(1.0, threshold))
        except ValueError:
            threshold = 0.3

        try:
            results = search_clients_fuzzy(
                query=query,
                agency_id=user.agency_id,
                allowed_agent_ids=None,  # Permission checking done internally
                limit=limit,
                similarity_threshold=threshold,
            )
            return Response(results)
        except Exception as e:
            logger.error(f'Fuzzy search clients failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchPoliciesFuzzyView(APIView):
    """
    GET /api/search-policies

    Fuzzy search for policies/deals using pg_trgm similarity.
    Translated from Supabase RPC: search_policies_fuzzy

    Query params:
        q: Search query (required)
        limit: Max results (default: 20)
        threshold: Similarity threshold 0.0-1.0 (default: 0.3)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        query = request.query_params.get('q', '').strip()
        if not query:
            return Response(
                {'error': 'Search query is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            limit = min(int(request.query_params.get('limit', 20)), 100)
        except ValueError:
            limit = 20

        try:
            threshold = float(request.query_params.get('threshold', 0.3))
            threshold = max(0.0, min(1.0, threshold))
        except ValueError:
            threshold = 0.3

        try:
            results = search_policies_fuzzy(
                query=query,
                agency_id=user.agency_id,
                allowed_agent_ids=None,
                limit=limit,
                similarity_threshold=threshold,
            )
            return Response(results)
        except Exception as e:
            logger.error(f'Fuzzy search policies failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SearchPolicyNumbersForFilterView(APIView):
    """
    GET /api/deals/search-policy-numbers

    Search policy numbers for filter dropdown.
    Translated from Supabase RPC: search_policy_numbers_for_filter

    Query params:
        q: Search term (empty returns all)
        limit: Max results (default: 20)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        search_term = request.query_params.get('q', '')
        try:
            limit = min(int(request.query_params.get('limit', 20)), 100)
        except ValueError:
            limit = 20

        try:
            results = search_policy_numbers_for_filter(user.id, search_term, limit)
            return Response(results)
        except Exception as e:
            logger.error(f'Search policy numbers failed: {e}')
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
