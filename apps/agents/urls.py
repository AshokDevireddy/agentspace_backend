"""
Agents API URLs

All routes are relative to /api/agents/
"""
from django.urls import path

from . import views

urlpatterns = [
    # List endpoints
    path('', views.AgentsListView.as_view(), name='agents_list'),
    path('downlines', views.AgentDownlinesView.as_view(), name='agent_downlines'),
    path('without-positions', views.AgentsWithoutPositionsView.as_view(), name='agents_without_positions'),
    path('assign-position', views.AssignPositionView.as_view(), name='assign_position'),
    path('invite', views.AgentInviteView.as_view(), name='agent_invite'),
    path(
        'check-positions',
        views.CheckCurrentUserUplinePositionsView.as_view(),
        name='check_current_user_upline_positions',
    ),

    # Agent-specific endpoints (P1-007, P1-008)
    path('<str:agent_id>', views.AgentDetailView.as_view(), name='agent_detail'),
    path('<str:agent_id>/downline', views.AgentRecursiveDownlineView.as_view(), name='agent_recursive_downline'),
    path('<str:agent_id>/upline-positions', views.CheckUplinePositionsView.as_view(), name='check_upline_positions'),
    path('<str:agent_id>/position', views.UpdateAgentPositionView.as_view(), name='update_agent_position'),
    path('<str:agent_id>/upline-chain', views.GetAgentUplineChainView.as_view(), name='get_upline_chain'),
]
