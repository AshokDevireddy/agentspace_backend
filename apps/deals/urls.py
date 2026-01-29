"""
Deals URL Configuration (P1-011, P1-012, P1-013, P1-014, P2-027, P2-028)
"""
from django.urls import path

from .views import (
    BookOfBusinessView,
    DealByPhoneView,
    DealDetailView,
    DealsListCreateView,
    DealStatusView,
    FilterOptionsView,
    FormDataView,
    ProductsByCarrierView,
)

urlpatterns = [
    # Main CRUD endpoints
    path('', DealsListCreateView.as_view(), name='deals_list_create'),

    # Search/filter endpoints (must come before <str:deal_id> to avoid conflict)
    path('by-phone', DealByPhoneView.as_view(), name='deal_by_phone'),
    path('book-of-business', BookOfBusinessView.as_view(), name='book-of-business'),
    path('filter-options', FilterOptionsView.as_view(), name='filter-options'),
    path('static-filter-options', FilterOptionsView.as_view(), name='static-filter-options'),
    path('form-data', FormDataView.as_view(), name='form-data'),
    path('products-by-carrier', ProductsByCarrierView.as_view(), name='products-by-carrier'),

    # Detail endpoints (must come after specific paths)
    path('<str:deal_id>', DealDetailView.as_view(), name='deal_detail'),
    path('<str:deal_id>/status', DealStatusView.as_view(), name='deal_status'),
]
