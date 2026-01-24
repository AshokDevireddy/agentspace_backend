"""
Pytest Configuration for AgentSpace Backend Tests

Key Features:
- Enables managed=True for unmanaged models during tests
- Provides fixtures for authenticated users and API clients
- Sets up factory_boy for model factories
"""
import uuid
from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from django.apps import apps
from rest_framework.test import APIClient


# =============================================================================
# Database Setup - Enable managed=True for unmanaged models
# =============================================================================

@pytest.fixture(scope='session')
def django_db_setup(django_db_blocker):
    """
    Override database setup to enable managed=True for all models.
    This allows Django to create tables for models that normally
    point to existing Supabase tables (managed=False).
    """
    with django_db_blocker.unblock():
        # Enable managed=True for all unmanaged models
        unmanaged_models = []
        for model in apps.get_models():
            if not model._meta.managed:
                model._meta.managed = True
                unmanaged_models.append(model.__name__)

        # Import after modifying models
        from django.core.management import call_command

        # Create all tables
        call_command('migrate', '--run-syncdb', verbosity=0)


@pytest.fixture(scope='session')
def django_db_modify_db_settings():
    """Allow database access in session-scoped fixtures."""
    pass


# =============================================================================
# AuthenticatedUser Mock
# =============================================================================

class MockAuthenticatedUser:
    """Mock user for testing that mimics AuthenticatedUser from authentication.py."""

    def __init__(
        self,
        user_id: uuid.UUID | None = None,
        agency_id: uuid.UUID | None = None,
        email: str = 'test@example.com',
        role: str = 'agent',
        is_admin: bool = False,
        first_name: str = 'Test',
        last_name: str = 'User',
        position_id: uuid.UUID | None = None,
        upline_id: uuid.UUID | None = None,
    ):
        self.id = user_id or uuid.uuid4()
        self.agency_id = agency_id or uuid.uuid4()
        self.auth_user_id = uuid.uuid4()
        self.email = email
        self.role = role
        self.is_admin = is_admin
        self.first_name = first_name
        self.last_name = last_name
        self.position_id = position_id
        self.upline_id = upline_id
        self.is_authenticated = True

    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}"


@pytest.fixture
def mock_user():
    """Create a basic mock authenticated user."""
    return MockAuthenticatedUser()


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    return MockAuthenticatedUser(role='admin', is_admin=True)


# =============================================================================
# API Client Fixtures
# =============================================================================

@pytest.fixture
def api_client():
    """Basic API client without authentication."""
    return APIClient()


@pytest.fixture
def authenticated_client(api_client, mock_user, mocker):
    """
    API client with mocked authentication.
    Patches get_user_context to return mock_user.
    """
    # Mock the authentication
    mocker.patch(
        'apps.core.authentication.get_user_context',
        return_value=mock_user
    )

    # Also mock the middleware authentication
    mocker.patch(
        'apps.core.middleware.SupabaseAuthMiddleware.__call__',
        side_effect=lambda request: request
    )

    return api_client, mock_user


@pytest.fixture
def admin_client(api_client, mock_admin_user, mocker):
    """
    API client with mocked admin authentication.
    """
    mocker.patch(
        'apps.core.authentication.get_user_context',
        return_value=mock_admin_user
    )

    return api_client, mock_admin_user


# =============================================================================
# Common Test Data Fixtures
# =============================================================================

@pytest.fixture
def agency_id():
    """Generate a consistent agency ID for tests."""
    return uuid.uuid4()


@pytest.fixture
def user_id():
    """Generate a consistent user ID for tests."""
    return uuid.uuid4()


@pytest.fixture
def sample_dates():
    """Common date fixtures for testing."""
    return {
        'today': date.today(),
        'start_of_year': date(date.today().year, 1, 1),
        'end_of_year': date(date.today().year, 12, 31),
        'last_month_start': date(date.today().year, date.today().month - 1 or 12, 1),
    }


# =============================================================================
# pytest-django Configuration
# =============================================================================

def pytest_configure(config):
    """Configure pytest-django settings."""
    import django
    from django.conf import settings

    # Use test settings
    if not settings.configured:
        settings.configure(
            DJANGO_SETTINGS_MODULE='config.settings.test'
        )
        django.setup()
