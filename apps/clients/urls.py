"""
Clients URL Configuration
"""
from django.urls import path

from .views import ClientDetailView, ClientsListView

urlpatterns = [
    path('', ClientsListView.as_view(), name='clients-list'),
    # Alias for get_clients_overview RPC
    path('overview', ClientsListView.as_view(), name='clients-overview'),
    path('<str:client_id>', ClientDetailView.as_view(), name='client-detail'),
]
