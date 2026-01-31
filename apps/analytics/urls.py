from django.urls import path

from .views import (
    AgentDealsAnalyticsView,
    AnalyticsSplitView,
    CarrierMetricsView,
    DealsAnalyticsView,
    DownlineDistributionView,
    PersistencyAnalyticsView,
)

urlpatterns = [
    path('split-view', AnalyticsSplitView.as_view(), name='analytics_split_view'),
    path('downline-distribution', DownlineDistributionView.as_view(), name='analytics_downline_distribution'),
    path('deals', DealsAnalyticsView.as_view(), name='analytics_deals'),
    path('persistency', PersistencyAnalyticsView.as_view(), name='analytics_persistency'),
    path('agent-deals', AgentDealsAnalyticsView.as_view(), name='analytics_agent_deals'),
    path('carrier-metrics', CarrierMetricsView.as_view(), name='analytics_carrier_metrics'),
]
