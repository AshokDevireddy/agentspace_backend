"""
Webhook Views - Stripe webhook handling

Endpoints:
- POST /api/webhooks/stripe - Handle Stripe webhooks
"""
import logging
import os

from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .services import (
    handle_checkout_completed,
    handle_payment_intent_succeeded,
    handle_subscription_deleted,
    handle_subscription_updated,
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
            try:
                event = stripe.Webhook.construct_event(
                    payload,
                    signature,
                    STRIPE_WEBHOOK_SECRET,
                )
            except stripe.error.SignatureVerificationError as e:
                logger.error(f'Webhook signature verification failed: {e}')

                # In development, allow bypassing signature verification
                if os.getenv('DEBUG', 'False').lower() == 'true':
                    logger.warning('DEV MODE: Bypassing signature verification')
                    import json
                    event = json.loads(payload)
                else:
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
