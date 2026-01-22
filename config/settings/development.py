"""
Django Development Settings

Use these settings for local development.
"""
from .base import *  # noqa: F401, F403

# =============================================================================
# Debug Settings
# =============================================================================

DEBUG = True

ALLOWED_HOSTS = ['localhost', '127.0.0.1', '0.0.0.0']

# =============================================================================
# CORS Settings - Allow local Next.js dev server
# =============================================================================

CORS_ALLOWED_ORIGINS = [
    'http://localhost:3000',
    'http://127.0.0.1:3000',
]

CORS_ALLOW_ALL_ORIGINS = False  # Keep strict even in dev

# =============================================================================
# Database - Allow local connection without SSL
# =============================================================================

DATABASES['default']['OPTIONS']['sslmode'] = config(  # noqa: F405
    'SUPABASE_DB_SSLMODE',
    default='prefer'
)

# =============================================================================
# Logging - More verbose in development
# =============================================================================

LOGGING['root']['level'] = 'DEBUG'  # noqa: F405
LOGGING['loggers']['django']['level'] = 'INFO'  # noqa: F405
