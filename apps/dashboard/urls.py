"""
Dashboard API URLs

All routes are relative to /api/dashboard/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('summary', views.DashboardSummaryView.as_view(), name='dashboard_summary'),
    path('scoreboard', views.ScoreboardView.as_view(), name='dashboard_scoreboard'),
    path('production', views.ProductionView.as_view(), name='dashboard_production'),
]
