"""
User API URLs

All routes are relative to /api/user/
"""
from django.urls import path

from . import views

urlpatterns = [
    # User profile endpoints (require authentication)
    path('profile', views.UserProfileView.as_view(), name='user_profile'),
    path('complete-onboarding', views.CompleteOnboardingView.as_view(), name='user_complete_onboarding'),
    # SMS usage (subscription limits, message counts)
    path('sms-usage', views.UserSmsUsageView.as_view(), name='user_sms_usage'),
    # User by ID (for looking up other users)
    path('<str:user_id>', views.UserByIdView.as_view(), name='user_by_id'),
    # Update user's NIPR carriers
    path('<str:user_id>/carriers', views.UserCarriersView.as_view(), name='user_carriers'),
    # Update user's NIPR data (carriers and states)
    path('<str:user_id>/nipr-data', views.UserNIPRDataView.as_view(), name='user_nipr_data'),
]
