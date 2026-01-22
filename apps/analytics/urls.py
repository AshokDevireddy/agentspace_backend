from django.urls import path

from .views import (
    AnalyticsSplitView,
    DownlineDistributionView,
    DealsAnalyticsView,
    PersistencyAnalyticsView,
)

urlpatterns = [
    path('split-view', AnalyticsSplitView.as_view(), name='analytics_split_view'),
    path('downline-distribution', DownlineDistributionView.as_view(), name='analytics_downline_distribution'),
    path('deals', DealsAnalyticsView.as_view(), name='analytics_deals'),
    path('persistency', PersistencyAnalyticsView.as_view(), name='analytics_persistency'),
]
