"""
Agency API URLs

All routes are relative to /api/agencies/
"""
from django.urls import path

from .views import (
    AgencyByDomainView,
    AgencyByPhoneView,
    AgencyDetailView,
    AgencyLogoUploadView,
    AgencyPhoneView,
    AgencyScoreboardSettingsView,
    AgencySettingsView,
)

urlpatterns = [
    # Public lookup by domain (for whitelabel)
    path('by-domain', AgencyByDomainView.as_view(), name='agency_by_domain'),

    # Find agency by phone (authenticated, for webhooks/cron)
    path('by-phone', AgencyByPhoneView.as_view(), name='agency_by_phone'),

    # Agency details
    path('<str:agency_id>', AgencyDetailView.as_view(), name='agency_detail'),
    path('<str:agency_id>/phone', AgencyPhoneView.as_view(), name='agency_phone'),

    # Agency settings
    path('<str:agency_id>/settings', AgencySettingsView.as_view(), name='agency_settings'),
    path('<str:agency_id>/scoreboard-settings', AgencyScoreboardSettingsView.as_view(), name='agency_scoreboard_settings'),
    path('<str:agency_id>/logo', AgencyLogoUploadView.as_view(), name='agency_logo'),
]
