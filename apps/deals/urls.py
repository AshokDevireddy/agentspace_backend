"""
Deals URL Configuration (P1-011, P1-012, P1-013, P1-014, P2-027, P2-028)
"""
from django.urls import path

from .views import (
    DealsListCreateView,
    DealDetailView,
    DealStatusView,
    BookOfBusinessView,
    FilterOptionsView,
)

urlpatterns = [
    # Main CRUD endpoints
    path('', DealsListCreateView.as_view(), name='deals_list_create'),
    path('<str:deal_id>', DealDetailView.as_view(), name='deal_detail'),
    path('<str:deal_id>/status', DealStatusView.as_view(), name='deal_status'),

    # Legacy/alias endpoints (backwards compatibility)
    path('book-of-business', BookOfBusinessView.as_view(), name='book-of-business'),
    path('filter-options', FilterOptionsView.as_view(), name='filter-options'),
    path('static-filter-options', FilterOptionsView.as_view(), name='static-filter-options'),
]
