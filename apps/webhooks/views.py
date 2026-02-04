"""
Webhook Views - Stripe webhook and session handling

Endpoints:
- POST /api/webhooks/stripe - Handle Stripe webhooks
- POST /api/webhooks/stripe/checkout-session - Create checkout session
- POST /api/webhooks/stripe/portal-session - Create customer portal session
- POST /api/webhooks/stripe/change-subscription - Change subscription tier
"""
import logging
import os

from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import SupabaseJWTAuthentication, get_user_context

from .services import (
    handle_checkout_completed,
    handle_payment_intent_succeeded,
    handle_subscription_deleted,
    handle_subscription_updated,
)
from .stripe_service import (
    create_checkout_session,
    create_portal_session,
    change_subscription,
)

logger = logging.getLogger(__name__)

STRIPE_WEBHOOK_SECRET = os.getenv('STRIPE_WEBHOOK_SECRET')


@method_decorator(csrf_exempt, name='dispatch')
class StripeWebhookView(APIView):
    """
    POST /api/webhooks/stripe - Handle Stripe webhook events

    This endpoint receives webhooks from Stripe for subscription and payment events.
    No authentication required (public webhook endpoint).
    Webhook signature verification is used for security.
    """

    # Disable authentication for webhook
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Handle Stripe webhook events."""
        try:
            import stripe

            # Get raw body for signature verification
            payload = request.body
            signature = request.META.get('HTTP_STRIPE_SIGNATURE')

            if not signature:
                logger.warning('No Stripe signature provided')
                return Response(
                    {'error': 'No signature provided'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Verify webhook signature
            # SECURITY FIX: Always verify signature, even in DEBUG mode
            # Bypassing signature verification creates a security hole that
            # could be exploited if DEBUG accidentally gets enabled in production
            if not STRIPE_WEBHOOK_SECRET:
                logger.error('STRIPE_WEBHOOK_SECRET not configured')
                return Response(
                    {'error': 'Webhook not configured'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            try:
                event = stripe.Webhook.construct_event(
                    payload,
                    signature,
                    STRIPE_WEBHOOK_SECRET,
                )
            except stripe.error.SignatureVerificationError as e:
                logger.error(f'Webhook signature verification failed: {e}')
                return Response(
                    {'error': 'Invalid signature'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            event_type = event.get('type') if isinstance(event, dict) else event.type
            event_data = event.get('data', {}).get('object', {}) if isinstance(event, dict) else event.data.object

            logger.info(f'Received Stripe webhook: {event_type}')

            # Route to appropriate handler
            if event_type == 'checkout.session.completed':
                result = handle_checkout_completed(
                    event_data if isinstance(event_data, dict) else event_data.to_dict()
                )
            elif event_type == 'payment_intent.succeeded':
                result = handle_payment_intent_succeeded(
                    event_data if isinstance(event_data, dict) else event_data.to_dict()
                )
            elif event_type == 'customer.subscription.updated':
                result = handle_subscription_updated(
                    event_data if isinstance(event_data, dict) else event_data.to_dict()
                )
            elif event_type == 'customer.subscription.deleted':
                result = handle_subscription_deleted(
                    event_data if isinstance(event_data, dict) else event_data.to_dict()
                )
            else:
                logger.info(f'Unhandled event type: {event_type}')
                return Response({'received': True})

            if result.success:
                return Response({
                    'received': True,
                    'user_id': result.user_id,
                    'tier': result.tier,
                    'status': result.status,
                })
            else:
                logger.error(f'Webhook handler failed: {result.error}')
                return Response(
                    {'error': result.error},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f'Stripe webhook error: {e}', exc_info=True)
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def get(self, request):
        """Health check for webhook endpoint."""
        return Response({
            'status': 'ok',
            'message': 'Stripe webhook endpoint is active'
        })


class CreateCheckoutSessionView(APIView):
    """
    POST /api/webhooks/stripe/checkout-session

    Create a Stripe checkout session for a new subscription.

    Request body:
        price_id: The Stripe price ID
        success_url: URL to redirect on successful checkout
        cancel_url: URL to redirect on cancelled checkout
        coupon_code: Optional coupon code
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        price_id = request.data.get('price_id')
        success_url = request.data.get('success_url')
        cancel_url = request.data.get('cancel_url')
        coupon_code = request.data.get('coupon_code')

        if not price_id:
            return Response(
                {'error': 'price_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not success_url or not cancel_url:
            return Response(
                {'error': 'success_url and cancel_url are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        result = create_checkout_session(
            user_id=user.id,
            price_id=price_id,
            success_url=success_url,
            cancel_url=cancel_url,
            coupon_code=coupon_code,
        )

        if result.success:
            return Response({
                'session_id': result.session_id,
                'url': result.url,
            })
        else:
            status_code = status.HTTP_400_BAD_REQUEST
            if 'admin' in (result.error or '').lower():
                status_code = status.HTTP_403_FORBIDDEN
            return Response(
                {'error': result.error},
                status=status_code
            )


class CreatePortalSessionView(APIView):
    """
    POST /api/webhooks/stripe/portal-session

    Create a Stripe customer portal session.

    Request body:
        return_url: URL to redirect when leaving the portal
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        return_url = request.data.get('return_url')
        if not return_url:
            return Response(
                {'error': 'return_url is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        result = create_portal_session(
            user_id=user.id,
            return_url=return_url,
        )

        if result.success:
            return Response({'url': result.url})
        else:
            return Response(
                {'error': result.error},
                status=status.HTTP_400_BAD_REQUEST
            )


class ChangeSubscriptionView(APIView):
    """
    POST /api/webhooks/stripe/change-subscription

    Change subscription tier. Handles upgrades, downgrades, and cancellations.

    Request body:
        new_tier: The target subscription tier ('free', 'basic', 'pro', 'expert')
        coupon_code: Optional coupon code (for upgrades from free)
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        new_tier = request.data.get('new_tier')
        coupon_code = request.data.get('coupon_code')

        if not new_tier:
            return Response(
                {'error': 'new_tier is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Get origin for redirect URLs
        origin = request.META.get('HTTP_ORIGIN', 'http://localhost:3000')

        result = change_subscription(
            user_id=user.id,
            new_tier=new_tier,
            coupon_code=coupon_code,
            base_url=origin,
        )

        if result.success:
            response_data = {
                'success': True,
                'status': result.status,
                'new_tier': result.new_tier,
            }
            if result.effective_date:
                response_data['effective_date'] = result.effective_date
            if result.checkout_url:
                response_data['checkout_url'] = result.checkout_url

            return Response(response_data)
        else:
            status_code = status.HTTP_400_BAD_REQUEST
            if 'admin' in (result.error or '').lower():
                status_code = status.HTTP_403_FORBIDDEN
            return Response(
                {'success': False, 'error': result.error},
                status=status_code
            )
