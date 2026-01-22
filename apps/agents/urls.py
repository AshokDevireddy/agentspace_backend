"""
Agents API URLs

All routes are relative to /api/agents/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('', views.AgentsListView.as_view(), name='agents_list'),
    path('downlines', views.AgentDownlinesView.as_view(), name='agent_downlines'),
    path('without-positions', views.AgentsWithoutPositionsView.as_view(), name='agents_without_positions'),
    path('assign-position', views.AssignPositionView.as_view(), name='assign_position'),
]
