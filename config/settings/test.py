"""
Django Test Settings for AgentSpace Backend

Uses SQLite in-memory database for fast testing.
Override managed=False models to allow Django to create tables.
"""
from .base import *  # noqa: F401, F403

# =============================================================================
# Debug Mode for Tests
# =============================================================================

DEBUG = False

# =============================================================================
# Database - Use PostgreSQL for tests to match production
# =============================================================================

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('TEST_DB_NAME', default='agentspace_test'),  # noqa: F405
        'USER': config('TEST_DB_USER', default='postgres'),  # noqa: F405
        'PASSWORD': config('TEST_DB_PASSWORD', default='postgres'),  # noqa: F405
        'HOST': config('TEST_DB_HOST', default='localhost'),  # noqa: F405
        'PORT': config('TEST_DB_PORT', default='5432'),  # noqa: F405
        'OPTIONS': {},
        'TEST': {
            'NAME': 'agentspace_test',
        },
    }
}

# =============================================================================
# Speed Optimizations for Tests
# =============================================================================

# Use faster password hasher for tests
PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]

# Disable logging during tests
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'handlers': {
        'null': {
            'class': 'logging.NullHandler',
        },
    },
    'root': {
        'handlers': ['null'],
        'level': 'CRITICAL',
    },
}

# =============================================================================
# REST Framework Test Settings
# =============================================================================

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.AllowAny',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'UNAUTHENTICATED_USER': None,
}

# =============================================================================
# Supabase Mock Configuration
# =============================================================================

SUPABASE_URL = 'http://localhost:54321'
SUPABASE_ANON_KEY = 'test-anon-key'
SUPABASE_SERVICE_ROLE_KEY = 'test-service-role-key'
SUPABASE_JWT_SECRET = 'test-jwt-secret-key-for-testing-purposes-only'

# Cron authentication secret for testing
CRON_SECRET = 'test-cron-secret'

# =============================================================================
# CORS - Allow all for tests
# =============================================================================

CORS_ALLOW_ALL_ORIGINS = True
