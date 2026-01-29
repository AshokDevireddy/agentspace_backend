"""
Clients URL Configuration
"""
from django.urls import path

from .views import (
    ClientDashboardView,
    ClientDealsView,
    ClientDetailView,
    ClientInviteView,
    ClientsListView,
)

urlpatterns = [
    path('', ClientsListView.as_view(), name='clients-list'),
    # Alias for get_clients_overview RPC
    path('overview', ClientsListView.as_view(), name='clients-overview'),
    # Invite endpoint (must be before <str:client_id> to avoid conflict)
    path('invite', ClientInviteView.as_view(), name='client-invite'),
    path('<str:client_id>', ClientDetailView.as_view(), name='client-detail'),
]

# Client self-service endpoints (for users with role='client')
client_dashboard_urlpatterns = [
    path('dashboard', ClientDashboardView.as_view(), name='client-dashboard'),
    path('deals', ClientDealsView.as_view(), name='client-deals'),
]
