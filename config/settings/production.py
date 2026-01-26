"""
Django Production Settings

Use these settings for production deployment.
"""
from .base import *  # noqa: F401, F403

# =============================================================================
# Security Settings
# =============================================================================

DEBUG = False

# Get allowed hosts from environment
ALLOWED_HOSTS = config('ALLOWED_HOSTS', cast=Csv())  # noqa: F405

# Security headers
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = 'DENY'

# HTTPS settings (enable when using HTTPS)
SECURE_SSL_REDIRECT = config('SECURE_SSL_REDIRECT', default=True, cast=bool)  # noqa: F405
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True

# HSTS settings
SECURE_HSTS_SECONDS = 31536000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

# =============================================================================
# CORS Settings - Strict production origins
# =============================================================================

CORS_ALLOWED_ORIGINS = config(  # noqa: F405
    'CORS_ALLOWED_ORIGINS',
    cast=Csv()  # noqa: F405
)

CORS_ALLOW_ALL_ORIGINS = False

# =============================================================================
# Database - Require SSL in production
# =============================================================================

DATABASES['default']['OPTIONS']['sslmode'] = 'require'  # noqa: F405

# =============================================================================
# Logging - Less verbose in production
# =============================================================================

LOGGING = LOGGING.copy()  # noqa: F405  # type: ignore[name-defined]
LOGGING['root']['level'] = 'INFO'  # type: ignore[index]
LOGGING['loggers']['django']['level'] = 'WARNING'  # type: ignore[index]
