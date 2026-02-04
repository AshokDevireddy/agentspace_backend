"""
Stripe Service

Business logic for Stripe checkout, portal, and subscription management.
Handles checkout sessions, billing portal, and subscription changes.
"""
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)

# Stripe configuration
STRIPE_SECRET_KEY = os.getenv('STRIPE_SECRET_KEY')

# Tier price ID mappings
TIER_PRICE_IDS = {
    'basic': os.getenv('STRIPE_BASIC_PRICE_ID'),
    'pro': os.getenv('STRIPE_PRO_PRICE_ID'),
    'expert': os.getenv('STRIPE_EXPERT_PRICE_ID'),
}

# Metered price IDs for usage-based billing
METERED_PRICE_IDS = {
    'basic_messages': os.getenv('STRIPE_BASIC_METERED_MESSAGES_PRICE_ID'),
    'pro_messages': os.getenv('STRIPE_PRO_METERED_MESSAGES_PRICE_ID'),
    'expert_messages': os.getenv('STRIPE_EXPERT_METERED_MESSAGES_PRICE_ID'),
    'expert_ai': os.getenv('STRIPE_EXPERT_METERED_AI_PRICE_ID'),
}

# Tier hierarchy for determining upgrades vs downgrades
TIER_HIERARCHY = {
    'free': 0,
    'basic': 1,
    'pro': 2,
    'expert': 3,
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
class CheckoutSessionResult:
    """Result of creating a checkout session."""
    success: bool
    session_id: str | None = None
    url: str | None = None
    error: str | None = None


@dataclass
class PortalSessionResult:
    """Result of creating a portal session."""
    success: bool
    url: str | None = None
    error: str | None = None


@dataclass
class SubscriptionChangeResult:
    """Result of changing a subscription."""
    success: bool
    status: str | None = None  # 'upgraded', 'scheduled', 'checkout_required'
    new_tier: str | None = None
    effective_date: str | None = None
    checkout_url: str | None = None
    error: str | None = None


def create_checkout_session(
    user_id: UUID,
    price_id: str,
    success_url: str,
    cancel_url: str,
    coupon_code: str | None = None,
) -> CheckoutSessionResult:
    """
    Create a Stripe checkout session for a new subscription.

    Args:
        user_id: The user's UUID
        price_id: The Stripe price ID for the subscription
        success_url: URL to redirect on successful checkout
        cancel_url: URL to redirect on cancelled checkout
        coupon_code: Optional coupon code to apply

    Returns:
        CheckoutSessionResult with session details
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        # Get user data
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT email, stripe_customer_id, is_admin, first_name, last_name, auth_user_id
                FROM public.users
                WHERE id = %s
            """, [str(user_id)])
            row = cursor.fetchone()

        if not row:
            return CheckoutSessionResult(success=False, error='User not found')

        email, customer_id, is_admin, first_name, last_name, auth_user_id = row

        # Validate Expert tier access
        tier = get_tier_from_price_id(price_id)
        if tier == 'expert' and not is_admin:
            return CheckoutSessionResult(
                success=False,
                error='Expert tier is only available for admin users'
            )

        # Create or get Stripe customer
        if not customer_id:
            customer = stripe.Customer.create(
                email=email,
                name=f"{first_name or ''} {last_name or ''}".strip() or None,
                metadata={
                    'user_id': str(user_id),
                    'auth_user_id': str(auth_user_id) if auth_user_id else None,
                },
            )
            customer_id = customer.id

            # Save customer ID to database
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET stripe_customer_id = %s, updated_at = NOW()
                    WHERE id = %s
                """, [customer_id, str(user_id)])

        # Validate and check coupon if provided
        discounts = []
        if coupon_code:
            try:
                coupon = stripe.Coupon.retrieve(coupon_code)
                if not coupon.valid:
                    return CheckoutSessionResult(
                        success=False,
                        error='This coupon is no longer active'
                    )

                # Check if customer has already used any coupon (one per lifetime)
                customer = stripe.Customer.retrieve(customer_id)
                if not customer.deleted and customer.metadata.get('has_used_coupon') == 'true':
                    return CheckoutSessionResult(
                        success=False,
                        error='You have already used your one-time promotional discount'
                    )

                discounts = [{'coupon': coupon_code}]
            except stripe.error.InvalidRequestError:
                return CheckoutSessionResult(
                    success=False,
                    error='Invalid coupon code'
                )

        # Create checkout session
        session_config = {
            'customer': customer_id,
            'line_items': [{'price': price_id, 'quantity': 1}],
            'mode': 'subscription',
            'success_url': success_url,
            'cancel_url': cancel_url,
            'allow_promotion_codes': True,
            'metadata': {
                'user_id': str(user_id),
                'tier': tier,
            },
            'subscription_data': {
                'metadata': {
                    'user_id': str(user_id),
                    'tier': tier,
                },
            },
        }

        if discounts:
            session_config['discounts'] = discounts
            session_config['metadata']['applied_coupon'] = coupon_code

        session = stripe.checkout.Session.create(**session_config)

        logger.info(f'Checkout session created for user {user_id}: {session.id}')

        return CheckoutSessionResult(
            success=True,
            session_id=session.id,
            url=session.url,
        )

    except Exception as e:
        logger.error(f'Error creating checkout session: {e}')
        return CheckoutSessionResult(success=False, error=str(e))


def create_portal_session(
    user_id: UUID,
    return_url: str,
) -> PortalSessionResult:
    """
    Create a Stripe customer portal session.

    Args:
        user_id: The user's UUID
        return_url: URL to redirect when leaving the portal

    Returns:
        PortalSessionResult with portal URL
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        # Get user's Stripe customer ID
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT stripe_customer_id
                FROM public.users
                WHERE id = %s
            """, [str(user_id)])
            row = cursor.fetchone()

        if not row or not row[0]:
            return PortalSessionResult(success=False, error='No subscription found')

        customer_id = row[0]

        # Create portal session
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=return_url,
        )

        logger.info(f'Portal session created for user {user_id}')

        return PortalSessionResult(
            success=True,
            url=session.url,
        )

    except Exception as e:
        logger.error(f'Error creating portal session: {e}')
        return PortalSessionResult(success=False, error=str(e))


def change_subscription(
    user_id: UUID,
    new_tier: str,
    coupon_code: str | None = None,
    base_url: str | None = None,
) -> SubscriptionChangeResult:
    """
    Handle subscription tier changes.

    - Upgrades are applied immediately with proration
    - Downgrades are scheduled for next billing cycle
    - Changes from free tier require a new checkout session

    Args:
        user_id: The user's UUID
        new_tier: The target subscription tier
        coupon_code: Optional coupon code for upgrades from free
        base_url: Base URL for redirect URLs (for checkout sessions)

    Returns:
        SubscriptionChangeResult with change details
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        # Validate tier
        if new_tier not in ['free', 'basic', 'pro', 'expert']:
            return SubscriptionChangeResult(success=False, error='Invalid tier specified')

        # Get user data
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    subscription_tier,
                    stripe_subscription_id,
                    stripe_customer_id,
                    is_admin,
                    billing_cycle_end
                FROM public.users
                WHERE id = %s
            """, [str(user_id)])
            row = cursor.fetchone()

        if not row:
            return SubscriptionChangeResult(success=False, error='User not found')

        current_tier, subscription_id, customer_id, is_admin, billing_cycle_end = row
        current_tier = current_tier or 'free'

        # Check for same tier
        if current_tier == new_tier:
            return SubscriptionChangeResult(success=False, error='Already on this tier')

        # Validate Expert tier access
        if new_tier == 'expert' and not is_admin:
            return SubscriptionChangeResult(
                success=False,
                error='Expert tier is only available for admin users'
            )

        # Handle downgrade to free (cancellation)
        if new_tier == 'free':
            if not subscription_id:
                return SubscriptionChangeResult(
                    success=False,
                    error='No active subscription to cancel'
                )

            # Schedule cancellation at period end
            stripe.Subscription.modify(
                subscription_id,
                cancel_at_period_end=True,
            )

            # Store scheduled change
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET scheduled_tier_change = %s,
                        scheduled_tier_change_date = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, ['free', billing_cycle_end, str(user_id)])

            logger.info(f'Subscription cancellation scheduled for user {user_id}')

            return SubscriptionChangeResult(
                success=True,
                status='scheduled',
                new_tier='free',
                effective_date=billing_cycle_end.isoformat() if billing_cycle_end else None,
            )

        new_price_id = get_tier_price_id(new_tier)
        if not new_price_id:
            return SubscriptionChangeResult(
                success=False,
                error='Invalid tier configuration'
            )

        # Handle upgrade from free tier (create new subscription)
        if current_tier == 'free':
            if not base_url:
                base_url = 'http://localhost:3000'

            checkout_result = create_checkout_session(
                user_id=user_id,
                price_id=new_price_id,
                success_url=f'{base_url}/user/profile?upgrade=success',
                cancel_url=f'{base_url}/user/profile?upgrade=cancelled',
                coupon_code=coupon_code,
            )

            if not checkout_result.success:
                return SubscriptionChangeResult(
                    success=False,
                    error=checkout_result.error,
                )

            return SubscriptionChangeResult(
                success=True,
                status='checkout_required',
                new_tier=new_tier,
                checkout_url=checkout_result.url,
            )

        # Handle existing subscription changes
        if not subscription_id:
            return SubscriptionChangeResult(
                success=False,
                error='No active subscription found'
            )

        # Determine if upgrade or downgrade
        current_level = TIER_HIERARCHY.get(current_tier, 0)
        new_level = TIER_HIERARCHY.get(new_tier, 0)
        is_upgrade = new_level > current_level

        # Retrieve subscription
        subscription = stripe.Subscription.retrieve(
            subscription_id,
            expand=['items.data.price', 'discounts']
        )

        if not subscription or not subscription.items.data:
            return SubscriptionChangeResult(
                success=False,
                error='Invalid subscription state'
            )

        main_item = subscription.items.data[0]

        if is_upgrade:
            # UPGRADE: Immediate change with proration
            logger.info(f'Processing upgrade: {current_tier} -> {new_tier} for user {user_id}')

            # Check for active discounts
            subscription_discounts = getattr(subscription, 'discounts', []) or []
            has_subscription_discount = bool(subscription_discounts and subscription_discounts[0].coupon)

            customer = stripe.Customer.retrieve(
                subscription.customer if isinstance(subscription.customer, str) else subscription.customer.id
            )
            has_customer_discount = not customer.deleted and customer.discount

            has_active_discount = has_subscription_discount or has_customer_discount

            # Build items update
            items_to_update = [{'id': main_item.id, 'price': new_price_id}]

            # Delete old metered prices
            for item in subscription.items.data[1:]:
                items_to_update.append({'id': item.id, 'deleted': True})

            # Update subscription
            update_config = {
                'items': items_to_update,
                'proration_behavior': 'none' if has_active_discount else 'always_invoice',
                'billing_cycle_anchor': 'unchanged',
            }

            # Preserve existing discount
            if has_active_discount and subscription_discounts:
                existing_coupon_id = subscription_discounts[0].coupon.id
                update_config['discounts'] = [{'coupon': existing_coupon_id}]

            stripe.Subscription.modify(subscription_id, **update_config)

            # Add new tier's metered prices
            _add_metered_prices_for_tier(subscription_id, new_tier, is_admin)

            # Update tier immediately
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET subscription_tier = %s,
                        scheduled_tier_change = NULL,
                        scheduled_tier_change_date = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                """, [new_tier, str(user_id)])

            logger.info(f'Upgrade completed for user {user_id}: {new_tier}')

            return SubscriptionChangeResult(
                success=True,
                status='upgraded',
                new_tier=new_tier,
            )

        else:
            # DOWNGRADE: Schedule for next billing cycle
            # Get period end from subscription or item
            period_end = getattr(subscription, 'current_period_end', None)
            if not period_end and subscription.items.data:
                period_end = getattr(subscription.items.data[0], 'current_period_end', None)

            if not period_end:
                return SubscriptionChangeResult(
                    success=False,
                    error='Unable to schedule downgrade - missing billing cycle data'
                )

            effective_date = datetime.fromtimestamp(period_end).isoformat()

            logger.info(f'Scheduling downgrade: {current_tier} -> {new_tier} for user {user_id}, effective {effective_date}')

            # Store scheduled change (don't modify Stripe subscription yet)
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.users
                    SET scheduled_tier_change = %s,
                        scheduled_tier_change_date = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, [new_tier, effective_date, str(user_id)])

            return SubscriptionChangeResult(
                success=True,
                status='scheduled',
                new_tier=new_tier,
                effective_date=effective_date,
            )

    except Exception as e:
        logger.error(f'Error changing subscription: {e}')
        return SubscriptionChangeResult(success=False, error=str(e))


def _add_metered_prices_for_tier(subscription_id: str, tier: str, is_admin: bool) -> None:
    """Add the metered prices appropriate for a tier."""
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    metered_prices = []

    if tier == 'basic':
        price = METERED_PRICE_IDS.get('basic_messages')
        if price:
            metered_prices.append(price)
    elif tier == 'pro':
        price = METERED_PRICE_IDS.get('pro_messages')
        if price:
            metered_prices.append(price)
    elif tier == 'expert':
        messages_price = METERED_PRICE_IDS.get('expert_messages')
        if messages_price:
            metered_prices.append(messages_price)
        # Only admins get AI metered price
        if is_admin:
            ai_price = METERED_PRICE_IDS.get('expert_ai')
            if ai_price:
                metered_prices.append(ai_price)

    for price_id in metered_prices:
        try:
            stripe.SubscriptionItem.create(
                subscription=subscription_id,
                price=price_id,
            )
            logger.info(f'Added metered price {price_id} to subscription {subscription_id}')
        except Exception as e:
            logger.error(f'Failed to add metered price {price_id}: {e}')


def report_usage(
    customer_id: str,
    event_name: str,
    quantity: int = 1,
) -> bool:
    """
    Report metered usage to Stripe Billing Meters.

    Args:
        customer_id: The Stripe customer ID
        event_name: The usage event name (e.g., 'sms_messages', 'ai_requests')
        quantity: The quantity to report (default 1)

    Returns:
        True if successful
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        stripe.billing.MeterEvent.create(
            event_name=event_name,
            payload={
                'stripe_customer_id': customer_id,
                'value': str(quantity),
            },
        )
        logger.debug(f'Usage reported: {event_name} x {quantity} for {customer_id}')
        return True

    except Exception as e:
        logger.error(f'Error reporting usage: {e}')
        return False
