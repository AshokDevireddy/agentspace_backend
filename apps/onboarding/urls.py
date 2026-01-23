"""
Onboarding URL Configuration

Routes for onboarding flow management endpoints.
"""
from django.urls import path

from .views import (
    OnboardingProgressView,
    NiprStatusView,
    NiprJobStatusView,
    InvitationsView,
    InvitationDetailView,
    SendInvitationsView,
    CompleteOnboardingView,
)
from .sse import NiprProgressSSEView

urlpatterns = [
    # Progress management
    path('progress', OnboardingProgressView.as_view(), name='onboarding_progress'),

    # NIPR verification
    path('nipr/status', NiprStatusView.as_view(), name='nipr_status'),
    path('nipr/job/<str:job_id>', NiprJobStatusView.as_view(), name='nipr_job_status'),
    path('nipr/sse', NiprProgressSSEView.as_view(), name='nipr_progress_sse'),

    # Team invitations
    path('invitations', InvitationsView.as_view(), name='onboarding_invitations'),
    path('invitations/<int:index>', InvitationDetailView.as_view(), name='onboarding_invitation_detail'),
    path('invitations/send', SendInvitationsView.as_view(), name='send_invitations'),

    # Completion
    path('complete', CompleteOnboardingView.as_view(), name='complete_onboarding'),
]
