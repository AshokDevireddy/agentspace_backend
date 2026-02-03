"""
Custom Throttle Classes for AgentSpace Backend

Provides rate limiting for security-sensitive endpoints.
"""
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle


class AuthRateThrottle(AnonRateThrottle):
    """
    Rate limiting for authentication endpoints.

    Applied to: login, register, forgot-password, reset-password
    Prevents brute-force attacks on auth endpoints.
    """
    scope = 'auth'


class UploadRateThrottle(UserRateThrottle):
    """
    Rate limiting for file upload endpoints.

    Prevents abuse of storage resources.
    """
    scope = 'uploads'


class BurstRateThrottle(UserRateThrottle):
    """
    Rate limiting for burst-sensitive endpoints.

    Used for endpoints that could be abused with rapid requests.
    """
    scope = 'burst'
