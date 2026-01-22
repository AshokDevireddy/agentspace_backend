"""
Deals URL Configuration
"""
from django.urls import path

from .views import BookOfBusinessView, FilterOptionsView

urlpatterns = [
    path('book-of-business', BookOfBusinessView.as_view(), name='book-of-business'),
    path('filter-options', FilterOptionsView.as_view(), name='filter-options'),
    # Alias for get_static_filter_options RPC
    path('static-filter-options', FilterOptionsView.as_view(), name='static-filter-options'),
]
