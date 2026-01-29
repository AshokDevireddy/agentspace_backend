"""
Django Base Settings for AgentSpace Backend

This file contains all shared settings used across environments.
Environment-specific settings are in development.py and production.py.
"""
from pathlib import Path

from decouple import Csv, config

# Build paths inside the project like this: BASE_DIR / 'subdir'
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# =============================================================================
# Core Settings
# =============================================================================

SECRET_KEY = config('DJANGO_SECRET_KEY', default='django-insecure-change-me-in-production')

DEBUG = config('DJANGO_DEBUG', default=True, cast=bool)

ALLOWED_HOSTS = config('ALLOWED_HOSTS', default='localhost,127.0.0.1', cast=Csv())

# =============================================================================
# Application Definition
# =============================================================================

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.postgres',

    'rest_framework',
    'corsheaders',

    # Local apps
    'apps.core',
    'apps.auth_api',
    'apps.dashboard',
    'apps.carriers',
    'apps.products',
    'apps.positions',
    'apps.agents',
    'apps.search',
    'apps.deals',      # P2-027, P2-028
    'apps.payouts',    # P2-029
    'apps.sms',        # P2-033 to P2-035
    'apps.clients',    # P2-037
    'apps.analytics',  # Analytics split view, downline distribution
    'apps.messaging',  # Cron messaging (birthday, lapse, billing, etc.)
    'apps.nipr',       # NIPR job management
    'apps.ingest',     # Policy report ingest/processing
    'apps.ai',         # AI conversations (P1-015)
    'apps.onboarding', # Onboarding flow state management
    'apps.agencies',   # Agency settings and configuration
    'apps.webhooks',   # Stripe and other webhooks
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'apps.core.middleware.SupabaseAuthMiddleware',
    'apps.core.middleware.AgencyContextMiddleware',  # Sets request.agency_id for multi-tenancy
]

ROOT_URLCONF = 'config.urls'

WSGI_APPLICATION = 'config.wsgi.application'
ASGI_APPLICATION = 'config.asgi.application'

# =============================================================================
# Templates (required for admin)
# =============================================================================

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# =============================================================================
# Database
# Connects to existing Supabase PostgreSQL - no migrations run
# =============================================================================

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('SUPABASE_DB_NAME', default='postgres'),
        'USER': config('SUPABASE_DB_USER', default='postgres'),
        'PASSWORD': config('SUPABASE_DB_PASSWORD', default=''),
        'HOST': config('SUPABASE_DB_HOST', default='localhost'),
        'PORT': config('SUPABASE_DB_PORT', default='5432'),
        'OPTIONS': {
            'sslmode': config('SUPABASE_DB_SSLMODE', default='require'),
        },
    }
}

# =============================================================================
# Supabase Configuration
# =============================================================================

SUPABASE_URL = config('NEXT_PUBLIC_SUPABASE_URL', default='')
SUPABASE_ANON_KEY = config('NEXT_PUBLIC_SUPABASE_ANON_KEY', default='')
SUPABASE_SERVICE_ROLE_KEY = config('SUPABASE_SERVICE_ROLE_KEY', default='')
SUPABASE_JWT_SECRET = config('SUPABASE_JWT_SECRET', default='')

# Cron authentication secret (shared with frontend)
CRON_SECRET = config('CRON_SECRET', default='')

# =============================================================================
# REST Framework
# =============================================================================

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'apps.core.authentication.SupabaseJWTAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'EXCEPTION_HANDLER': 'apps.core.exceptions.custom_exception_handler',
    'UNAUTHENTICATED_USER': None,
}

# =============================================================================
# CORS Configuration
# =============================================================================

CORS_ALLOWED_ORIGINS = config(
    'CORS_ALLOWED_ORIGINS',
    default='http://localhost:3000',
    cast=Csv()
)

CORS_ALLOW_CREDENTIALS = True

CORS_ALLOW_HEADERS = [
    'accept',
    'accept-encoding',
    'authorization',
    'content-type',
    'origin',
    'user-agent',
    'x-csrftoken',
    'x-requested-with',
]

# =============================================================================
# Internationalization
# =============================================================================

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = False
USE_TZ = True

# =============================================================================
# Static files
# =============================================================================

STATIC_URL = '/static/'

# =============================================================================
# Application Settings
# =============================================================================

APP_URL = config('APP_URL', default='http://localhost:3000')

# User status flow
USER_STATUS_FLOW = {
    'PRE_INVITE': 'pre-invite',
    'INVITED': 'invited',
    'ONBOARDING': 'onboarding',
    'ACTIVE': 'active',
    'INACTIVE': 'inactive',
}

# =============================================================================
# Logging
# =============================================================================

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': config('DJANGO_LOG_LEVEL', default='INFO'),
            'propagate': False,
        },
        'apps': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

# =============================================================================
# Default primary key field type
# =============================================================================

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
