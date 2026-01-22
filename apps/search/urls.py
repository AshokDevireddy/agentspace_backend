"""
Search API URLs

These are mounted at different paths in the main urls.py:
- /api/search-agents -> search_agents
- /api/search-agents/fuzzy -> search_agents_fuzzy
- /api/search-clients/fuzzy -> search_clients_fuzzy
- /api/search-policies -> search_policies_fuzzy
- /api/deals/search-clients -> search_clients
- /api/deals/search-agents -> search_agents_for_filter
- /api/deals/search-policy-numbers -> search_policy_numbers
"""
from django.urls import path

from . import views

# Search agents URL pattern (mounted at /api/search-agents)
search_agents_urlpatterns = [
    path('', views.SearchAgentsView.as_view(), name='search_agents'),
    path('fuzzy', views.SearchAgentsFuzzyView.as_view(), name='search_agents_fuzzy'),
]

# Search clients URL pattern (mounted at /api/deals/search-clients)
search_clients_urlpatterns = [
    path('', views.SearchClientsView.as_view(), name='search_clients'),
]

# Search agents for filter (mounted at /api/deals/search-agents)
search_agents_filter_urlpatterns = [
    path('', views.SearchAgentsForFilterView.as_view(), name='search_agents_for_filter'),
]

# Search policy numbers (mounted at /api/deals/search-policy-numbers)
search_policy_numbers_urlpatterns = [
    path('', views.SearchPolicyNumbersForFilterView.as_view(), name='search_policy_numbers'),
]

# Fuzzy search clients (mounted at /api/search-clients/fuzzy)
search_clients_fuzzy_urlpatterns = [
    path('', views.SearchClientsFuzzyView.as_view(), name='search_clients_fuzzy'),
]

# Fuzzy search policies (mounted at /api/search-policies)
search_policies_urlpatterns = [
    path('', views.SearchPoliciesFuzzyView.as_view(), name='search_policies_fuzzy'),
]
