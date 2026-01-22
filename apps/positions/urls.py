"""
Positions API URLs

All routes are relative to /api/positions/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('', views.PositionsListView.as_view(), name='positions_list'),
    path('product-commissions', views.PositionCommissionsView.as_view(), name='position_commissions'),
    path('product-commissions/sync', views.SyncCommissionsView.as_view(), name='sync_commissions'),
    path('product-commissions/<str:commission_id>', views.PositionCommissionDetailView.as_view(), name='commission_detail'),
    path('<str:position_id>', views.PositionDetailView.as_view(), name='position_detail'),
]
