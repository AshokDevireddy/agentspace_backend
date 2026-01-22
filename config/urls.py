"""
URL Configuration for AgentSpace Backend API

All routes are prefixed with /api/ to match Next.js conventions.
"""
from django.contrib import admin
from django.urls import include, path

from apps.core.views import health_check
from apps.search.urls import search_agents_urlpatterns, search_clients_urlpatterns

urlpatterns = [
    # Django Admin
    path('admin/', admin.site.urls),

    # Health check endpoint (public)
    path('api/health', health_check, name='health_check'),

    # Authentication endpoints
    path('api/auth/', include('apps.auth_api.urls')),

    # User profile endpoints
    path('api/user/', include('apps.auth_api.user_urls')),

    # Dashboard endpoints
    path('api/dashboard/', include('apps.dashboard.urls')),

    # Carriers endpoints
    path('api/carriers/', include('apps.carriers.urls')),

    # Products endpoints
    path('api/products/', include('apps.products.urls')),

    # Positions endpoints
    path('api/positions/', include('apps.positions.urls')),

    # Agents endpoints
    path('api/agents/', include('apps.agents.urls')),

    # Search endpoints (mounted at different paths)
    path('api/search-agents', include(search_agents_urlpatterns)),
    path('api/deals/search-clients', include(search_clients_urlpatterns)),
]
