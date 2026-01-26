"""
Agency API URLs

All routes are relative to /api/agencies/
"""
from django.urls import path

from .views import AgencyLogoUploadView, AgencySettingsView

urlpatterns = [
    path('<str:agency_id>/settings', AgencySettingsView.as_view(), name='agency_settings'),
    path('<str:agency_id>/logo', AgencyLogoUploadView.as_view(), name='agency_logo'),
]
