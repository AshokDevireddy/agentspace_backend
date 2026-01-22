"""
Authentication API URLs

All routes are relative to /api/auth/
"""
from django.urls import path

from . import views

urlpatterns = [
    # Public auth endpoints
    path('login', views.LoginView.as_view(), name='auth_login'),
    path('logout', views.LogoutView.as_view(), name='auth_logout'),
    path('register', views.RegisterView.as_view(), name='auth_register'),
    path('verify-invite', views.VerifyInviteView.as_view(), name='auth_verify_invite'),
    path('setup-account', views.SetupAccountView.as_view(), name='auth_setup_account'),
    path('forgot-password', views.ForgotPasswordView.as_view(), name='auth_forgot_password'),
    path('reset-password', views.ResetPasswordView.as_view(), name='auth_reset_password'),

    # Session management
    path('refresh', views.RefreshTokenView.as_view(), name='auth_refresh'),
    path('session', views.SessionView.as_view(), name='auth_session'),
]
