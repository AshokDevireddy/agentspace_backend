"""
Carriers API URLs

All routes are relative to /api/carriers/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('', views.CarriersListView.as_view(), name='carriers_list'),
    path('names', views.CarrierNamesView.as_view(), name='carriers_names'),
    path('agency', views.AgencyCarriersView.as_view(), name='carriers_agency'),
    path('with-products', views.CarriersWithProductsView.as_view(), name='carriers_with_products'),
]
