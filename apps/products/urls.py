"""
Products API URLs

All routes are relative to /api/products/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('', views.ProductsListView.as_view(), name='products_list'),
    path('all', views.AllProductsView.as_view(), name='products_all'),
    path('<str:product_id>', views.ProductDetailView.as_view(), name='product_detail'),
]
