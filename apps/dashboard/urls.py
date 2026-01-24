"""
Dashboard API URLs

All routes are relative to /api/dashboard/
"""
from django.urls import path

from . import views

urlpatterns = [
    path('summary', views.DashboardSummaryView.as_view(), name='dashboard_summary'),
    path('scoreboard', views.ScoreboardView.as_view(), name='dashboard_scoreboard'),
    path('scoreboard-lapsed', views.ScoreboardLapsedView.as_view(), name='dashboard_scoreboard_lapsed'),
    path('scoreboard-billing-cycle', views.ScoreboardBillingCycleView.as_view(), name='dashboard_scoreboard_billing_cycle'),
    path('production', views.ProductionView.as_view(), name='dashboard_production'),
]
