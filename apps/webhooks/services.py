"""
Webhook Services - Stripe subscription handling

Business logic for processing Stripe webhook events:
- Subscription creation and updates
- Payment processing
- Top-up credit handling
"""
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)

# Stripe configuration
STRIPE_SECRET_KEY = os.getenv('STRIPE_SECRET_KEY')
STRIPE_WEBHOOK_SECRET = os.getenv('STRIPE_WEBHOOK_SECRET')

# Tier price ID mappings
TIER_PRICE_IDS = {
    'basic': os.getenv('STRIPE_BASIC_PRICE_ID'),
    'pro': os.getenv('STRIPE_PRO_PRICE_ID'),
    'expert': os.getenv('STRIPE_EXPERT_PRICE_ID'),
}

# Metered price IDs
METERED_PRICE_IDS = {
    'basic_messages': os.getenv('STRIPE_BASIC_METERED_MESSAGES_PRICE_ID'),
    'pro_messages': os.getenv('STRIPE_PRO_METERED_MESSAGES_PRICE_ID'),
    'expert_messages': os.getenv('STRIPE_EXPERT_METERED_MESSAGES_PRICE_ID'),
    'expert_ai': os.getenv('STRIPE_EXPERT_METERED_AI_PRICE_ID'),
}


def get_tier_from_price_id(price_id: str) -> str:
    """Get subscription tier from Stripe price ID."""
    for tier, pid in TIER_PRICE_IDS.items():
        if pid == price_id:
            return tier
    return 'free'


def get_tier_price_id(tier: str) -> str | None:
    """Get Stripe price ID from tier name."""
    return TIER_PRICE_IDS.get(tier)


@dataclass
class SubscriptionUpdateResult:
    """Result of a subscription update operation."""
    success: bool
    user_id: str | None = None
    tier: str | None = None
    status: str | None = None
    error: str | None = None


def handle_checkout_completed(session_data: dict) -> SubscriptionUpdateResult:
    """
    Handle checkout.session.completed event.

    Args:
        session_data: Stripe checkout session object

    Returns:
        SubscriptionUpdateResult with operation status
    """
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        user_id = session_data.get('metadata', {}).get('user_id')
        if not user_id:
            logger.error('Missing user_id in checkout session metadata')
            return SubscriptionUpdateResult(success=False, error='Missing user_id')

        mode = session_data.get('mode')

        if mode == 'subscription':
            subscription_id = session_data.get('subscription')
            if not subscription_id:
                return SubscriptionUpdateResult(success=False, error='No subscription ID')

            # Retrieve subscription to get price ID
            subscription = stripe.Subscription.retrieve(subscription_id)
            price_id = subscription.items.data[0].price.id if subscription.items.data else None

            if not price_id:
                return SubscriptionUpdateResult(success=False, error='No price ID in subscription')

            tier = get_tier_from_price_id(price_id)

            # Get billing cycle dates
            period_start = getattr(subscription, 'current_period_start', None)
            period_end = getattr(subscription, 'current_period_end', None)

            # Fall back to item-level fields for newer API versions
            if not period_start and subscription.items.data:
                item = subscription.items.data[0]
                period_start = getattr(item, 'current_period_start', None)
                period_end = getattr(item, 'current_period_end', None)

            if not period_start or not period_end:
                logger.error(f'Missing billing cycle dates in subscription: {subscription_id}')
                return SubscriptionUpdateResult(success=False, error='Missing billing cycle dates')

            billing_cycle_start = datetime.fromtimestamp(period_start).isoformat()
            billing_cycle_end = datetime.fromtimestamp(period_end).isoformat()

            # Update user subscription
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET
                        subscription_status = 'active',
                        subscription_tier = %s,
                        stripe_subscription_id = %s,
                        billing_cycle_start = %s,
                        billing_cycle_end = %s,
                        messages_sent_count = 0,
                        messages_reset_date = %s,
                        ai_requests_count = 0,
                        ai_requests_reset_date = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, [
                    tier,
                    subscription_id,
                    billing_cycle_start,
                    billing_cycle_end,
                    billing_cycle_start,
                    billing_cycle_start,
                    user_id,
                ])

            logger.info(f'Subscription activated for user {user_id}: {tier} tier')

            # Add metered prices to subscription
            _add_metered_prices(subscription_id, tier, subscription.items.data)

            return SubscriptionUpdateResult(
                success=True,
                user_id=user_id,
                tier=tier,
                status='active'
            )

        elif mode == 'payment':
            # One-time payment - handled in payment_intent.succeeded
            logger.info(f'One-time payment checkout completed for user {user_id}')
            return SubscriptionUpdateResult(success=True, user_id=user_id)

        return SubscriptionUpdateResult(success=True)

    except Exception as e:
        logger.error(f'Error handling checkout completed: {e}', exc_info=True)
        return SubscriptionUpdateResult(success=False, error=str(e))


def handle_payment_intent_succeeded(payment_intent_data: dict) -> SubscriptionUpdateResult:
    """
    Handle payment_intent.succeeded event for top-up payments.

    Args:
        payment_intent_data: Stripe payment intent object

    Returns:
        SubscriptionUpdateResult with operation status
    """
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        payment_intent_id = payment_intent_data.get('id')

        # Get the checkout session to access metadata
        sessions = stripe.checkout.Session.list(
            payment_intent=payment_intent_id,
            limit=1,
        )

        if not sessions.data:
            logger.info('No session found for payment intent - may be subscription payment')
            return SubscriptionUpdateResult(success=True)

        session = sessions.data[0]
        metadata = session.metadata or {}

        user_id = metadata.get('user_id')
        topup_type = metadata.get('topup_type')
        topup_quantity = int(metadata.get('topup_quantity', 0))

        if not user_id or not topup_type or not topup_quantity:
            logger.info('Not a top-up payment - missing metadata')
            return SubscriptionUpdateResult(success=True)

        # Update user credits
        if topup_type == 'message_topup':
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET messages_topup_credits = COALESCE(messages_topup_credits, 0) + %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, [topup_quantity, user_id])
        elif topup_type == 'ai_topup':
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET ai_requests_topup_credits = COALESCE(ai_requests_topup_credits, 0) + %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, [topup_quantity, user_id])

        # Record purchase
        amount_cents = payment_intent_data.get('amount', 0)
        currency = payment_intent_data.get('currency', 'usd')
        created_at = datetime.fromtimestamp(payment_intent_data.get('created', 0)).isoformat()

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.purchases (
                    user_id, purchase_type, stripe_payment_intent_id,
                    amount_cents, currency, quantity, description,
                    status, purchased_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'completed', %s, NOW(), NOW())
            """, [
                user_id,
                topup_type,
                payment_intent_id,
                amount_cents,
                currency,
                topup_quantity,
                f'{topup_quantity} {"messages" if topup_type == "message_topup" else "AI requests"} top-up',
                created_at,
            ])

        logger.info(f'Top-up successful: User {user_id} received {topup_quantity} {topup_type} credits')

        return SubscriptionUpdateResult(success=True, user_id=user_id)

    except Exception as e:
        logger.error(f'Error handling payment intent succeeded: {e}', exc_info=True)
        return SubscriptionUpdateResult(success=False, error=str(e))


def handle_subscription_updated(subscription_data: dict) -> SubscriptionUpdateResult:
    """
    Handle customer.subscription.updated event.

    Args:
        subscription_data: Stripe subscription object

    Returns:
        SubscriptionUpdateResult with operation status
    """
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        user_id = subscription_data.get('metadata', {}).get('user_id')
        if not user_id:
            logger.error('Missing user_id in subscription metadata')
            return SubscriptionUpdateResult(success=False, error='Missing user_id')

        subscription_id = subscription_data.get('id')
        subscription_status = subscription_data.get('status')

        # Map Stripe status to our status
        status = 'active' if subscription_status == 'active' else \
                 'past_due' if subscription_status == 'past_due' else \
                 'canceled' if subscription_status == 'canceled' else 'free'

        # Get tier from price ID
        items_data = subscription_data.get('items', {}).get('data', [])
        price_id = items_data[0].get('price', {}).get('id') if items_data else None
        tier = get_tier_from_price_id(price_id) if price_id else 'free'

        # Get billing cycle dates
        period_start = subscription_data.get('current_period_start')
        period_end = subscription_data.get('current_period_end')

        # Fall back to item-level fields
        if not period_start and items_data:
            period_start = items_data[0].get('current_period_start')
            period_end = items_data[0].get('current_period_end')

        if not period_start or not period_end:
            logger.error(f'Missing billing cycle dates in subscription: {subscription_id}')
            return SubscriptionUpdateResult(success=False, error='Missing billing cycle dates')

        billing_cycle_start = datetime.fromtimestamp(period_start).isoformat()
        billing_cycle_end = datetime.fromtimestamp(period_end).isoformat()

        # Get current user data to check for renewal
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT billing_cycle_start, billing_cycle_end, scheduled_tier_change, subscription_tier
                FROM public.users
                WHERE id = %s
            """, [user_id])
            row = cursor.fetchone()

        current_billing_start = row[0] if row else None
        scheduled_tier_change = row[2] if row else None
        current_tier = row[3] if row else None

        # Check if this is a true renewal
        is_renewal = (
            current_billing_start and
            str(current_billing_start) != billing_cycle_start and
            datetime.fromisoformat(billing_cycle_start.replace('Z', '+00:00')) >
            datetime.fromisoformat(str(current_billing_start).replace('Z', '+00:00'))
        )

        update_fields = {
            'subscription_status': status,
            'billing_cycle_start': billing_cycle_start,
            'billing_cycle_end': billing_cycle_end,
        }

        if is_renewal:
            logger.info(f'Billing cycle renewal detected for user {user_id}')

            # Reset usage counts
            update_fields['messages_sent_count'] = 0
            update_fields['messages_reset_date'] = billing_cycle_start
            update_fields['ai_requests_count'] = 0
            update_fields['ai_requests_reset_date'] = billing_cycle_start

            # Apply scheduled tier change if exists
            if scheduled_tier_change:
                logger.info(f'Applying scheduled tier change: {current_tier} -> {scheduled_tier_change}')
                update_fields['subscription_tier'] = scheduled_tier_change
                update_fields['scheduled_tier_change'] = None
                update_fields['scheduled_tier_change_date'] = None

                # Update Stripe subscription price
                _update_subscription_tier(subscription_id, scheduled_tier_change, items_data)
            else:
                update_fields['subscription_tier'] = tier
        else:
            # Not a renewal - check for scheduled downgrade
            if scheduled_tier_change:
                # Keep current tier if downgrade is scheduled
                update_fields['subscription_tier'] = current_tier
            else:
                # Immediate upgrade
                update_fields['subscription_tier'] = tier

        # Build and execute update query
        set_clauses = [f"{k} = %s" for k in update_fields.keys()]
        values = list(update_fields.values())
        values.append(user_id)

        with connection.cursor() as cursor:
            cursor.execute(f"""
                UPDATE public.users
                SET {', '.join(set_clauses)}, updated_at = NOW()
                WHERE id = %s
            """, values)

        final_tier = update_fields.get('subscription_tier', tier)
        logger.info(f'Subscription updated for user {user_id}: {final_tier} tier, status: {status}')

        return SubscriptionUpdateResult(
            success=True,
            user_id=user_id,
            tier=final_tier,
            status=status
        )

    except Exception as e:
        logger.error(f'Error handling subscription updated: {e}', exc_info=True)
        return SubscriptionUpdateResult(success=False, error=str(e))


def handle_subscription_deleted(subscription_data: dict) -> SubscriptionUpdateResult:
    """
    Handle customer.subscription.deleted event.

    Args:
        subscription_data: Stripe subscription object

    Returns:
        SubscriptionUpdateResult with operation status
    """
    try:
        user_id = subscription_data.get('metadata', {}).get('user_id')
        if not user_id:
            logger.error('Missing user_id in subscription metadata')
            return SubscriptionUpdateResult(success=False, error='Missing user_id')

        # Reset to free tier
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE public.users
                SET
                    subscription_status = 'canceled',
                    subscription_tier = 'free',
                    stripe_subscription_id = NULL,
                    updated_at = NOW()
                WHERE id = %s
            """, [user_id])

        logger.info(f'Subscription canceled for user {user_id} - reverted to free tier')

        return SubscriptionUpdateResult(
            success=True,
            user_id=user_id,
            tier='free',
            status='canceled'
        )

    except Exception as e:
        logger.error(f'Error handling subscription deleted: {e}', exc_info=True)
        return SubscriptionUpdateResult(success=False, error=str(e))


def _add_metered_prices(subscription_id: str, tier: str, existing_items: list) -> None:
    """Add metered prices to a subscription."""
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        metered_prices = []
        if tier == 'basic':
            metered_prices.append(METERED_PRICE_IDS.get('basic_messages'))
        elif tier == 'pro':
            metered_prices.append(METERED_PRICE_IDS.get('pro_messages'))
        elif tier == 'expert':
            metered_prices.append(METERED_PRICE_IDS.get('expert_messages'))
            metered_prices.append(METERED_PRICE_IDS.get('expert_ai'))

        # Get existing price IDs
        existing_price_ids = [item.price.id for item in existing_items]

        for metered_price in metered_prices:
            if metered_price and metered_price not in existing_price_ids:
                try:
                    stripe.SubscriptionItem.create(
                        subscription=subscription_id,
                        price=metered_price,
                    )
                    logger.info(f'Added metered price {metered_price} to subscription {subscription_id}')
                except Exception as e:
                    logger.error(f'Failed to add metered price {metered_price}: {e}')

    except Exception as e:
        logger.error(f'Error adding metered prices: {e}')


def _update_subscription_tier(subscription_id: str, new_tier: str, existing_items: list) -> None:
    """Update Stripe subscription to a new tier."""
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        new_price_id = get_tier_price_id(new_tier)
        if not new_price_id:
            return

        main_item = existing_items[0] if existing_items else None
        if not main_item:
            return

        # Build items update
        items_to_update = [{
            'id': main_item.get('id'),
            'price': new_price_id,
        }]

        # Delete old metered prices
        for item in existing_items[1:]:
            items_to_update.append({
                'id': item.get('id'),
                'deleted': True,
            })

        # Update subscription
        stripe.Subscription.modify(
            subscription_id,
            items=items_to_update,
            proration_behavior='none',
        )

        # Add new tier's metered prices
        _add_metered_prices(subscription_id, new_tier, [])

        logger.info(f'Updated subscription {subscription_id} to {new_tier} tier')

    except Exception as e:
        logger.error(f'Error updating subscription tier: {e}')
