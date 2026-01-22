"""
Agents API Views

Provides agent-related endpoints:
- GET /api/agents - List agents (table or tree view)
- GET /api/agents/{id} - Get single agent
- GET /api/agents/downlines - Get agent's downlines
- GET /api/agents/without-positions - Get agents without positions
- POST /api/agents/assign-position - Assign position to agent
"""
import logging
from datetime import date, datetime
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context
from .selectors import (
    check_agent_upline_positions,
    get_agent_downline,
    get_agent_options,
    get_agents_hierarchy_nodes,
    get_agents_table,
    get_agents_without_positions,
    get_agent_downlines_with_details,
    get_agents_debt_production,
    get_agent_upline_chain,
)
from .services import assign_position_to_agent, update_agent_position

logger = logging.getLogger(__name__)


def format_position(perm_level: str) -> str:
    """Format position/perm_level for display."""
    value = perm_level or 'agent'
    return value.capitalize()


def format_datetime(iso: str) -> str:
    """Format ISO datetime for display."""
    if not iso:
        return 'N/A'
    try:
        dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        return dt.strftime('%b %d, %Y %I:%M %p')
    except (ValueError, TypeError):
        return 'N/A'


def build_tree(rows: list, root_id: str) -> dict:
    """Build hierarchy tree from flat rows."""
    by_id = {str(row['agent_id']): {**row, 'children': []} for row in rows}

    for row in rows:
        agent_id = str(row['agent_id'])
        upline_id = str(row['upline_id']) if row.get('upline_id') else None
        if upline_id and upline_id in by_id and agent_id != upline_id:
            by_id[upline_id]['children'].append(agent_id)

    # Find reachable nodes from root
    reachable = set()
    def visit(node_id, seen):
        if node_id in seen:
            return
        seen.add(node_id)
        node = by_id.get(node_id)
        if node:
            for child_id in node['children']:
                visit(child_id, seen)

    visit(str(root_id), reachable)

    def to_node(node_id):
        if node_id not in reachable:
            return None
        node = by_id.get(node_id)
        if not node:
            return None

        display_position = node.get('position_name') or format_position(node.get('perm_level'))

        return {
            'name': f"{node['first_name']} {node['last_name']}",
            'attributes': {
                'position': display_position,
            },
            'children': [
                child for child in (to_node(cid) for cid in node['children']) if child
            ],
        }

    root_node = to_node(str(root_id))
    return root_node or {'name': 'Unknown', 'attributes': {'position': 'Agent'}, 'children': []}


class AgentsListView(APIView):
    """
    GET /api/agents

    Get agents list (table or tree view).

    Query params:
        view: 'table' (default) or 'tree'
        page: Page number (default: 1)
        limit: Page size (default: 20)
        status: Filter by status
        agentName: Filter by agent name
        inUpline: Filter agents with this person in upline
        directUpline: Filter by direct upline
        inDownline: Filter agents with this person in downline
        directDownline: Filter by direct downline
        positionId: Filter by position
        startMonth: Start month for metrics (YYYY-MM)
        endMonth: End month for metrics (YYYY-MM)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Parse query params
        view = request.query_params.get('view', 'table')
        page = int(request.query_params.get('page', 1))
        limit = int(request.query_params.get('limit', 20))
        offset = (page - 1) * limit

        is_admin = user.is_admin or user.role == 'admin' or user.perm_level == 'admin'

        try:
            if view == 'tree':
                # Tree view - return hierarchy nodes
                hierarchy_rows = get_agents_hierarchy_nodes(user.id, include_full_agency=is_admin)

                if not hierarchy_rows:
                    return Response({
                        'tree': {
                            'name': f'{user.email}',
                            'attributes': {'position': format_position(user.perm_level)},
                            'children': [],
                        },
                    })

                tree = build_tree(hierarchy_rows, str(user.id))
                return Response({'tree': tree})

            # Table view
            filters = {
                'status': request.query_params.get('status'),
                'agent_name': request.query_params.get('agentName'),
                'in_upline': request.query_params.get('inUpline'),
                'direct_upline': request.query_params.get('directUpline'),
                'in_downline': request.query_params.get('inDownline'),
                'direct_downline': request.query_params.get('directDownline'),
                'position_id': request.query_params.get('positionId'),
            }

            # Handle special filter values
            if filters['direct_upline'] == 'all':
                filters['direct_upline'] = 'all'
            elif filters['direct_upline'] == 'not_set':
                filters['direct_upline'] = None

            if filters['position_id'] == 'all':
                filters['position_id'] = 'all'
            elif filters['position_id'] == 'not_set':
                filters['position_id'] = None

            include_full_agency = is_admin and view == 'table'

            table_rows = get_agents_table(
                user.id,
                filters=filters,
                include_full_agency=include_full_agency,
                limit=limit,
                offset=offset,
            )

            total_count = table_rows[0]['total_count'] if table_rows else 0
            total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

            # Get agent IDs for debt/production metrics
            agent_ids = [row['agent_id'] for row in table_rows]

            # Calculate date range
            start_month = request.query_params.get('startMonth')
            end_month = request.query_params.get('endMonth')

            now = date.today()
            if start_month and end_month:
                start_year, start_m = map(int, start_month.split('-'))
                end_year, end_m = map(int, end_month.split('-'))
                start_date = date(start_year, start_m, 1)
                end_date = date(end_year, end_m + 1, 1) if end_m < 12 else date(end_year + 1, 1, 1)
            else:
                start_date = date(now.year, 1, 1)
                end_date = date(now.year + 1, 1, 1)

            # Get debt/production metrics
            debt_production_map = {}
            if agent_ids:
                try:
                    metrics = get_agents_debt_production(
                        user.id,
                        agent_ids,
                        start_date.isoformat(),
                        end_date.isoformat(),
                    )
                    debt_production_map = {str(m['agent_id']): m for m in metrics}
                except Exception as e:
                    logger.warning(f'Failed to get debt/production metrics: {e}')

            # Format agents for response
            agents = []
            for row in table_rows:
                position = format_position(row['perm_level'])
                total_prod = float(row.get('total_prod') or 0)
                metrics = debt_production_map.get(str(row['agent_id']), {})

                agents.append({
                    'id': str(row['agent_id']),
                    'name': f"{row['first_name']} {row['last_name']}",
                    'position': position,
                    'upline': row.get('upline_name') or 'None',
                    'created': format_datetime(row.get('created_at')),
                    'earnings': f"$0.00 / ${total_prod:.2f}",
                    'downlines': int(row.get('downline_count') or 0),
                    'status': row.get('status') or 'active',
                    'badge': position,
                    'position_id': str(row['position_id']) if row.get('position_id') else None,
                    'position_name': row.get('position_name'),
                    'position_level': row.get('position_level'),
                    'email': row.get('email'),
                    'first_name': row.get('first_name'),
                    'last_name': row.get('last_name'),
                    'children': [],
                    # Debt/production metrics
                    'individual_debt': float(metrics.get('individual_debt') or 0),
                    'individual_debt_count': int(metrics.get('individual_debt_count') or 0),
                    'individual_production': float(metrics.get('individual_production') or 0),
                    'individual_production_count': int(metrics.get('individual_production_count') or 0),
                    'hierarchy_debt': float(metrics.get('hierarchy_debt') or 0),
                    'hierarchy_debt_count': int(metrics.get('hierarchy_debt_count') or 0),
                    'hierarchy_production': float(metrics.get('hierarchy_production') or 0),
                    'hierarchy_production_count': int(metrics.get('hierarchy_production_count') or 0),
                    'debt_to_production_ratio': float(metrics['debt_to_production_ratio']) if metrics.get('debt_to_production_ratio') is not None else None,
                })

            # Get agent options for filter dropdowns
            all_agents = []
            if view == 'table':
                option_rows = get_agent_options(user.id, include_full_agency=include_full_agency)
                all_agents = [
                    {'id': str(row['agent_id']), 'name': row['display_name']}
                    for row in option_rows
                ]

            return Response({
                'agents': agents,
                'allAgents': all_agents,
                'pagination': {
                    'currentPage': page,
                    'totalPages': total_pages,
                    'totalCount': total_count,
                    'limit': limit,
                    'hasNextPage': page < total_pages,
                    'hasPrevPage': page > 1,
                },
            })

        except Exception as e:
            logger.error(f'Agents list failed: {e}')
            return Response(
                {'error': 'Internal Server Error', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AgentDownlinesView(APIView):
    """
    GET /api/agents/downlines?agentId={uuid}

    Get direct downlines for an agent.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        agent_id = request.query_params.get('agentId')
        if not agent_id:
            return Response(
                {'error': 'Missing agent ID', 'detail': 'Agent ID is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            agent_uuid = UUID(agent_id)
        except ValueError:
            return Response(
                {'error': 'Invalid agentId', 'detail': 'agentId must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            downlines = get_agent_downlines_with_details(agent_uuid)

            # Get metrics for downlines
            downline_ids = [d['id'] for d in downlines]
            metrics_map = {}

            if downline_ids:
                now = date.today()
                start_date = date(now.year, 1, 1)
                end_date = date(now.year + 1, 1, 1)

                try:
                    metrics = get_agents_debt_production(
                        agent_uuid,
                        downline_ids,
                        start_date.isoformat(),
                        end_date.isoformat(),
                    )
                    metrics_map = {str(m['agent_id']): m for m in metrics}
                except Exception as e:
                    logger.warning(f'Failed to get metrics: {e}')

            formatted_downlines = []
            for d in downlines:
                m = metrics_map.get(str(d['id']), {})
                formatted_downlines.append({
                    'id': str(d['id']),
                    'name': f"{d['first_name']} {d['last_name']}",
                    'position': d.get('position_name'),
                    'position_level': d.get('position_level'),
                    'badge': d.get('position_name') or 'Agent',
                    'status': d.get('status') or 'active',
                    'created_at': str(d.get('created_at')),
                    'individual_debt': float(m.get('individual_debt') or 0),
                    'individual_debt_count': int(m.get('individual_debt_count') or 0),
                    'individual_production': float(m.get('individual_production') or 0),
                    'individual_production_count': int(m.get('individual_production_count') or 0),
                    'hierarchy_debt': float(m.get('hierarchy_debt') or 0),
                    'hierarchy_debt_count': int(m.get('hierarchy_debt_count') or 0),
                    'hierarchy_production': float(m.get('hierarchy_production') or 0),
                    'hierarchy_production_count': int(m.get('hierarchy_production_count') or 0),
                    'debt_to_production_ratio': float(m['debt_to_production_ratio']) if m.get('debt_to_production_ratio') is not None else None,
                })

            return Response({
                'agentId': agent_id,
                'downlines': formatted_downlines,
                'downlineCount': len(formatted_downlines),
            })

        except Exception as e:
            logger.error(f'Agent downlines failed: {e}')
            return Response(
                {'error': 'Failed to fetch downlines', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AgentsWithoutPositionsView(APIView):
    """
    GET /api/agents/without-positions

    Get agents who don't have a position assigned.

    Query params:
        q: Search query (optional)
        all: If 'true', return all agents regardless of search
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        search_query = request.query_params.get('q', '').strip()
        fetch_all = search_query == '*' or request.query_params.get('all') == 'true'

        try:
            agents_without_positions = get_agents_without_positions(user.id)

            # Format agents
            agents = []
            for a in agents_without_positions:
                agents.append({
                    'agent_id': str(a['agent_id']),
                    'first_name': a.get('first_name'),
                    'last_name': a.get('last_name'),
                    'email': a.get('email'),
                    'phone_number': a.get('phone_number'),
                    'role': a.get('role'),
                    'upline_name': a.get('upline_name'),
                    'created_at': str(a.get('created_at')) if a.get('created_at') else None,
                    'position_id': None,
                    'position_name': None,
                    'has_position': False,
                })

            # If search or fetch all, also get agents with positions
            if search_query or fetch_all:
                # For now, just filter the list by search query
                if search_query and not fetch_all:
                    search_lower = search_query.lower()
                    agents = [
                        a for a in agents
                        if (a.get('first_name') or '').lower().find(search_lower) >= 0
                        or (a.get('last_name') or '').lower().find(search_lower) >= 0
                        or (a.get('email') or '').lower().find(search_lower) >= 0
                        or f"{a.get('first_name', '')} {a.get('last_name', '')}".lower().find(search_lower) >= 0
                    ]

            return Response({
                'agents': agents,
                'count': len(agents_without_positions),  # Badge count is still without positions
            })

        except Exception as e:
            logger.error(f'Agents without positions failed: {e}')
            return Response(
                {'error': 'Failed to fetch agents', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AssignPositionView(APIView):
    """
    POST /api/agents/assign-position

    Assign a position to an agent.

    Request body:
        {
            "agent_id": "uuid",
            "position_id": "uuid"  // or null to clear
        }
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized', 'detail': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        data = request.data
        agent_id = data.get('agent_id')
        position_id = data.get('position_id')

        if not agent_id:
            return Response(
                {'error': 'Missing agent_id', 'detail': 'agent_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            agent_uuid = UUID(agent_id)
            position_uuid = UUID(position_id) if position_id else None
        except ValueError:
            return Response(
                {'error': 'Invalid UUID', 'detail': 'agent_id and position_id must be valid UUIDs'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = assign_position_to_agent(
                agent_id=agent_uuid,
                position_id=position_uuid,
                agency_id=user.agency_id,
            )
            if not result:
                return Response(
                    {'error': 'Agent not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            return Response({'success': True, 'agent': result})
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f'Assign position failed: {e}')
            return Response(
                {'error': 'Failed to assign position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CheckUplinePositionsView(APIView):
    """
    GET /api/agents/{agent_id}/upline-positions

    Check if all agents in the upline chain have positions assigned.
    Translated from Supabase RPC: check_agent_upline_positions
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, agent_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            agent_uuid = UUID(agent_id)
        except ValueError:
            return Response(
                {'error': 'Invalid agent_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = check_agent_upline_positions(agent_uuid)
            return Response(result)

        except Exception as e:
            logger.error(f'Check upline positions failed: {e}')
            return Response(
                {'error': 'Failed to check upline positions', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UpdateAgentPositionView(APIView):
    """
    PATCH /api/agents/{agent_id}/position

    Update an agent's position with permission checks.
    Translated from Supabase RPC: update_agent_position

    Request body:
        {
            "position_id": "uuid"
        }
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, agent_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            agent_uuid = UUID(agent_id)
        except ValueError:
            return Response(
                {'error': 'Invalid agent_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        position_id = request.data.get('position_id')
        if not position_id:
            return Response(
                {'error': 'position_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            position_uuid = UUID(position_id)
        except ValueError:
            return Response(
                {'error': 'Invalid position_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            result = update_agent_position(
                user_id=user.id,
                agent_id=agent_uuid,
                position_id=position_uuid,
            )

            if result.get('success'):
                return Response(result)
            else:
                return Response(
                    {'error': result.get('error', 'Unknown error')},
                    status=status.HTTP_400_BAD_REQUEST
                )

        except Exception as e:
            logger.error(f'Update agent position failed: {e}')
            return Response(
                {'error': 'Failed to update position', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GetAgentUplineChainView(APIView):
    """
    GET /api/agents/{agent_id}/upline-chain

    Get the complete upline chain from an agent to the top of hierarchy.
    Translated from Supabase RPC: get_agent_upline_chain
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, agent_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            agent_uuid = UUID(agent_id)
        except ValueError:
            return Response(
                {'error': 'Invalid agent_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            chain = get_agent_upline_chain(agent_uuid)

            # Format response
            formatted_chain = []
            for row in chain:
                formatted_chain.append({
                    'agent_id': str(row['agent_id']),
                    'upline_id': str(row['upline_id']) if row['upline_id'] else None,
                    'depth': row['depth'],
                })

            return Response({
                'agent_id': agent_id,
                'upline_chain': formatted_chain,
                'chain_length': len(formatted_chain),
            })

        except Exception as e:
            logger.error(f'Get agent upline chain failed: {e}')
            return Response(
                {'error': 'Failed to get upline chain', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
