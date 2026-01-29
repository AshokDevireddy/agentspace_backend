"""
URL Configuration for AgentSpace Backend API

All routes are prefixed with /api/ to match Next.js conventions.
"""
from django.contrib import admin
from django.urls import include, path

from apps.core.views import health_check
from apps.clients.urls import client_dashboard_urlpatterns
from apps.search.urls import (
    search_agents_filter_urlpatterns,
    search_agents_urlpatterns,
    search_clients_fuzzy_urlpatterns,
    search_clients_urlpatterns,
    search_policies_urlpatterns,
    search_policy_numbers_urlpatterns,
)

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

    # Deals endpoints (P2-027, P2-028)
    path('api/deals/', include('apps.deals.urls')),

    # Expected Payouts endpoints (P2-029)
    path('api/expected-payouts/', include('apps.payouts.urls')),

    # SMS endpoints (P2-033 to P2-035)
    path('api/sms/', include('apps.sms.urls')),

    # Clients endpoints (P2-037)
    path('api/clients/', include('apps.clients.urls')),

    # Client self-service endpoints (for users with role='client')
    path('api/client/', include(client_dashboard_urlpatterns)),

    # Agencies endpoints (configuration settings)
    path('api/agencies/', include('apps.agencies.urls')),

    # Analytics endpoints
    path('api/analytics/', include('apps.analytics.urls')),

    # Messaging endpoints (cron jobs)
    path('api/messaging/', include('apps.messaging.urls')),

    # NIPR job management endpoints
    path('api/nipr/', include('apps.nipr.urls')),

    # Ingest endpoints (policy report processing)
    path('api/ingest/', include('apps.ingest.urls')),

    # AI endpoints (P1-015)
    path('api/ai/', include('apps.ai.urls')),

    # Onboarding endpoints (server-side onboarding state)
    path('api/onboarding/', include('apps.onboarding.urls')),

    # Webhooks (Stripe, etc.)
    path('api/webhooks/', include('apps.webhooks.urls')),

    # Search endpoints (mounted at different paths)
    path('api/search-agents/', include(search_agents_urlpatterns)),
    path('api/search-clients/fuzzy', include(search_clients_fuzzy_urlpatterns)),
    path('api/search-policies', include(search_policies_urlpatterns)),
    path('api/deals/search-clients', include(search_clients_urlpatterns)),
    path('api/deals/search-agents', include(search_agents_filter_urlpatterns)),
    path('api/deals/search-policy-numbers', include(search_policy_numbers_urlpatterns)),
]
