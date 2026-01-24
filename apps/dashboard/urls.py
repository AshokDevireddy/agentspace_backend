"""
Dashboard API URLs (P2-032, P2-033, P2-034, P2-035)

All routes are relative to /api/dashboard/
"""
from django.urls import path

from . import views

urlpatterns = [
    # Core dashboard endpoints
    path('summary', views.DashboardSummaryView.as_view(), name='dashboard_summary'),
    path('scoreboard', views.ScoreboardView.as_view(), name='dashboard_scoreboard'),
    path('scoreboard-lapsed', views.ScoreboardLapsedView.as_view(), name='dashboard_scoreboard_lapsed'),
    path('scoreboard-billing-cycle', views.ScoreboardBillingCycleView.as_view(), name='dashboard_scoreboard_billing_cycle'),
    path('production', views.ProductionView.as_view(), name='dashboard_production'),

    # Widgets (P2-032)
    path('widgets', views.WidgetsListView.as_view(), name='widgets_list'),
    path('widgets/reorder', views.WidgetsReorderView.as_view(), name='widgets_reorder'),
    path('widgets/<str:widget_id>', views.WidgetDetailView.as_view(), name='widget_detail'),

    # Reports (P2-033)
    path('reports', views.ReportsListView.as_view(), name='reports_list'),
    path('reports/<str:report_id>', views.ReportDetailView.as_view(), name='report_detail'),
    path('reports/<str:report_id>/generate', views.ReportGenerateView.as_view(), name='report_generate'),

    # Scheduled Reports (P2-034)
    path('scheduled-reports', views.ScheduledReportsListView.as_view(), name='scheduled_reports_list'),
    path('scheduled-reports/<str:report_id>', views.ScheduledReportDetailView.as_view(), name='scheduled_report_detail'),

    # Export (P2-035)
    path('export', views.ExportView.as_view(), name='export'),
]
