"""
Webhook URL Configuration

Routes:
- POST /api/webhooks/stripe - Stripe webhook events
- POST /api/webhooks/stripe/checkout-session - Create checkout session
- POST /api/webhooks/stripe/portal-session - Create customer portal session
- POST /api/webhooks/stripe/change-subscription - Change subscription tier
"""
from django.urls import path

from .views import (
    StripeWebhookView,
    CreateCheckoutSessionView,
    CreatePortalSessionView,
    ChangeSubscriptionView,
)

urlpatterns = [
    # Stripe webhook (public, signature-verified)
    path('stripe', StripeWebhookView.as_view(), name='stripe_webhook'),

    # Stripe session management (authenticated)
    path('stripe/checkout-session', CreateCheckoutSessionView.as_view(), name='stripe_checkout_session'),
    path('stripe/portal-session', CreatePortalSessionView.as_view(), name='stripe_portal_session'),
    path('stripe/change-subscription', ChangeSubscriptionView.as_view(), name='stripe_change_subscription'),
]
