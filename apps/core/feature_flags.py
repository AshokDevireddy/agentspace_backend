"""
Feature Flag System (P0-006)

Provides utilities for checking feature flags stored in the database.
Supports global flags and per-agency overrides.
"""
import logging
from functools import lru_cache
from typing import Optional
from uuid import UUID

from django.core.cache import cache
from django.db import connection

logger = logging.getLogger(__name__)

# Cache timeout for feature flags (5 minutes)
FEATURE_FLAG_CACHE_TIMEOUT = 300


class FeatureFlags:
    """
    Feature flag names for Django endpoint migration.
    """
    # Authentication endpoints
    USE_DJANGO_AUTH = 'use_django_auth'
    USE_DJANGO_AUTH_LOGIN = 'use_django_auth_login'
    USE_DJANGO_AUTH_SESSION = 'use_django_auth_session'
    USE_DJANGO_AUTH_REFRESH = 'use_django_auth_refresh'

    # Dashboard endpoints
    USE_DJANGO_DASHBOARD = 'use_django_dashboard'
    USE_DJANGO_DASHBOARD_SUMMARY = 'use_django_dashboard_summary'
    USE_DJANGO_DASHBOARD_SCOREBOARD = 'use_django_dashboard_scoreboard'

    # Agent endpoints
    USE_DJANGO_AGENTS = 'use_django_agents'
    USE_DJANGO_AGENTS_LIST = 'use_django_agents_list'
    USE_DJANGO_AGENTS_DOWNLINES = 'use_django_agents_downlines'
    USE_DJANGO_AGENTS_WITHOUT_POSITIONS = 'use_django_agents_without_positions'
    USE_DJANGO_SEARCH_AGENTS = 'use_django_search_agents'

    # Deal endpoints
    USE_DJANGO_DEALS = 'use_django_deals'
    USE_DJANGO_DEALS_BOB = 'use_django_deals_bob'
    USE_DJANGO_DEALS_FILTERS = 'use_django_deals_filters'

    # Other endpoints
    USE_DJANGO_CARRIERS = 'use_django_carriers'
    USE_DJANGO_PRODUCTS = 'use_django_products'
    USE_DJANGO_POSITIONS = 'use_django_positions'
    USE_DJANGO_SMS = 'use_django_sms'
    USE_DJANGO_PAYOUTS = 'use_django_payouts'
    USE_DJANGO_CLIENTS = 'use_django_clients'


def get_feature_flag(
    flag_name: str,
    agency_id: Optional[UUID] = None,
    default: bool = False
) -> bool:
    """
    Check if a feature flag is enabled.

    Checks in order:
    1. Agency-specific override (if agency_id provided)
    2. Global flag
    3. Default value

    Args:
        flag_name: The name of the feature flag
        agency_id: Optional agency ID for agency-specific check
        default: Default value if flag doesn't exist

    Returns:
        True if the feature is enabled
    """
    # Build cache key
    cache_key = f'feature_flag:{flag_name}'
    if agency_id:
        cache_key = f'{cache_key}:agency:{agency_id}'

    # Check cache first
    cached_value = cache.get(cache_key)
    if cached_value is not None:
        return cached_value

    try:
        with connection.cursor() as cursor:
            if agency_id:
                # Check agency-specific flag first
                cursor.execute("""
                    SELECT is_enabled, rollout_percentage
                    FROM public.feature_flags
                    WHERE name = %s AND agency_id = %s
                    LIMIT 1
                """, [flag_name, str(agency_id)])
                row = cursor.fetchone()

                if row:
                    is_enabled = row[0]
                    # Cache and return
                    cache.set(cache_key, is_enabled, FEATURE_FLAG_CACHE_TIMEOUT)
                    return is_enabled

            # Check global flag
            cursor.execute("""
                SELECT is_enabled, rollout_percentage
                FROM public.feature_flags
                WHERE name = %s AND agency_id IS NULL
                LIMIT 1
            """, [flag_name])
            row = cursor.fetchone()

            if row:
                is_enabled = row[0]
                cache.set(cache_key, is_enabled, FEATURE_FLAG_CACHE_TIMEOUT)
                return is_enabled

    except Exception as e:
        logger.warning(f'Error checking feature flag {flag_name}: {e}')

    # Return default if not found
    cache.set(cache_key, default, FEATURE_FLAG_CACHE_TIMEOUT)
    return default


def is_feature_enabled(flag_name: str, agency_id: Optional[UUID] = None) -> bool:
    """
    Convenience function to check if a feature is enabled.

    Args:
        flag_name: The feature flag name
        agency_id: Optional agency ID

    Returns:
        True if enabled, False otherwise
    """
    return get_feature_flag(flag_name, agency_id, default=False)


def get_all_feature_flags(agency_id: Optional[UUID] = None) -> dict[str, bool]:
    """
    Get all feature flags and their states.

    Args:
        agency_id: Optional agency ID for agency-specific flags

    Returns:
        Dictionary of flag_name -> is_enabled
    """
    flags = {}

    try:
        with connection.cursor() as cursor:
            # Get global flags
            cursor.execute("""
                SELECT name, is_enabled
                FROM public.feature_flags
                WHERE agency_id IS NULL
            """)
            for row in cursor.fetchall():
                flags[row[0]] = row[1]

            # Overlay agency-specific flags if provided
            if agency_id:
                cursor.execute("""
                    SELECT name, is_enabled
                    FROM public.feature_flags
                    WHERE agency_id = %s
                """, [str(agency_id)])
                for row in cursor.fetchall():
                    flags[row[0]] = row[1]

    except Exception as e:
        logger.warning(f'Error getting feature flags: {e}')

    return flags


def set_feature_flag(
    flag_name: str,
    is_enabled: bool,
    agency_id: Optional[UUID] = None,
    description: Optional[str] = None,
    rollout_percentage: int = 100
) -> bool:
    """
    Set a feature flag value.

    Args:
        flag_name: The feature flag name
        is_enabled: Whether the flag should be enabled
        agency_id: Optional agency ID for agency-specific flag
        description: Optional description of the flag
        rollout_percentage: Percentage of users to enable for (0-100)

    Returns:
        True if successful
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.feature_flags (name, is_enabled, agency_id, description, rollout_percentage)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name, COALESCE(agency_id, '00000000-0000-0000-0000-000000000000'::uuid))
                DO UPDATE SET
                    is_enabled = EXCLUDED.is_enabled,
                    description = COALESCE(EXCLUDED.description, feature_flags.description),
                    rollout_percentage = EXCLUDED.rollout_percentage,
                    updated_at = NOW()
            """, [
                flag_name,
                is_enabled,
                str(agency_id) if agency_id else None,
                description,
                rollout_percentage
            ])

        # Invalidate cache
        cache_key = f'feature_flag:{flag_name}'
        if agency_id:
            cache_key = f'{cache_key}:agency:{agency_id}'
        cache.delete(cache_key)

        return True

    except Exception as e:
        logger.error(f'Error setting feature flag {flag_name}: {e}')
        return False


def clear_feature_flag_cache():
    """
    Clear all cached feature flag values.

    Call this when feature flags are updated externally.
    """
    # This is a simple approach - in production you might want
    # to use cache tags or a more sophisticated invalidation strategy
    try:
        cache.clear()
    except Exception as e:
        logger.warning(f'Error clearing feature flag cache: {e}')


# =============================================================================
# Django Endpoint Checks
# =============================================================================

def should_use_django_auth(agency_id: Optional[UUID] = None) -> bool:
    """Check if Django auth endpoints should be used."""
    return is_feature_enabled(FeatureFlags.USE_DJANGO_AUTH, agency_id)


def should_use_django_dashboard(agency_id: Optional[UUID] = None) -> bool:
    """Check if Django dashboard endpoints should be used."""
    return is_feature_enabled(FeatureFlags.USE_DJANGO_DASHBOARD, agency_id)


def should_use_django_agents(agency_id: Optional[UUID] = None) -> bool:
    """Check if Django agent endpoints should be used."""
    return is_feature_enabled(FeatureFlags.USE_DJANGO_AGENTS, agency_id)


def should_use_django_deals(agency_id: Optional[UUID] = None) -> bool:
    """Check if Django deal endpoints should be used."""
    return is_feature_enabled(FeatureFlags.USE_DJANGO_DEALS, agency_id)


def get_django_endpoints_status(agency_id: Optional[UUID] = None) -> dict[str, bool]:
    """
    Get status of all Django endpoint feature flags.

    Returns:
        Dictionary of endpoint category -> is_enabled
    """
    return {
        'auth': should_use_django_auth(agency_id),
        'dashboard': should_use_django_dashboard(agency_id),
        'agents': should_use_django_agents(agency_id),
        'deals': should_use_django_deals(agency_id),
        'carriers': is_feature_enabled(FeatureFlags.USE_DJANGO_CARRIERS, agency_id),
        'products': is_feature_enabled(FeatureFlags.USE_DJANGO_PRODUCTS, agency_id),
        'positions': is_feature_enabled(FeatureFlags.USE_DJANGO_POSITIONS, agency_id),
        'sms': is_feature_enabled(FeatureFlags.USE_DJANGO_SMS, agency_id),
        'payouts': is_feature_enabled(FeatureFlags.USE_DJANGO_PAYOUTS, agency_id),
        'clients': is_feature_enabled(FeatureFlags.USE_DJANGO_CLIENTS, agency_id),
    }
