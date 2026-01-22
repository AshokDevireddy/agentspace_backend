"""
Search API URLs

These are mounted at different paths in the main urls.py:
- /api/search-agents -> search_agents
- /api/deals/search-clients -> search_clients
"""
from django.urls import path

from . import views

# Search agents URL pattern (mounted at /api/search-agents)
search_agents_urlpatterns = [
    path('', views.SearchAgentsView.as_view(), name='search_agents'),
]

# Search clients URL pattern (mounted at /api/deals/search-clients)
search_clients_urlpatterns = [
    path('', views.SearchClientsView.as_view(), name='search_clients'),
]
