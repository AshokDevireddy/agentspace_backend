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
]
