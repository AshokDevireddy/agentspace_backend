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
    # Stripe-related endpoints for payment operations
    path('stripe-profile', views.UserStripeProfileView.as_view(), name='user_stripe_profile'),
    path('stripe-customer-id', views.UserStripeCustomerIdView.as_view(), name='user_stripe_customer_id'),
    path('subscription-tier', views.UserSubscriptionTierView.as_view(), name='user_subscription_tier'),
    # User by ID (for looking up other users)
    path('<str:user_id>', views.UserByIdView.as_view(), name='user_by_id'),
    # Update user's NIPR carriers
    path('<str:user_id>/carriers', views.UserCarriersView.as_view(), name='user_carriers'),
    # Update user's NIPR data (carriers and states)
    path('<str:user_id>/nipr-data', views.UserNIPRDataView.as_view(), name='user_nipr_data'),
]


# Users API URLs (relative to /api/users/)
users_urlpatterns = [
    # Get user by Supabase auth_user_id (for onboarding flows)
    path('by-auth-id/<str:auth_user_id>', views.UserByAuthIdView.as_view(), name='user_by_auth_id'),
    path('by-auth-id/<str:auth_user_id>/onboarding', views.UserByAuthIdOnboardingView.as_view(), name='user_by_auth_id_onboarding'),
]
