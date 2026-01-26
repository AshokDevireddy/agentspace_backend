"""
Integration Test Fixtures

Provides real database fixtures using Factory Boy.
These fixtures create actual database records for true integration testing.
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest
from rest_framework.test import APIClient

from tests.conftest import MockAuthenticatedUser
from tests.factories import (
    AgencyFactory,
    CarrierFactory,
    ClientFactory,
    ConversationFactory,
    DealFactory,
    MessageFactory,
    PositionFactory,
    ProductFactory,
    SmsTemplateFactory,
    StatusMappingFactory,
    UserFactory,
)

# =============================================================================
# Agency & User Fixtures
# =============================================================================


@pytest.fixture
def agency(db):
    """Create a test agency."""
    return AgencyFactory(
        name='Test Insurance Agency',
        sms_enabled=True,
    )


@pytest.fixture
def admin_user(agency):
    """Create an admin user for the agency."""
    position = PositionFactory(agency=agency, name='Agency Owner', level=0)
    return UserFactory(
        agency=agency,
        position=position,
        role='admin',
        is_admin=True,
        first_name='Admin',
        last_name='User',
    )


@pytest.fixture
def agent_user(agency, admin_user):
    """Create a regular agent under the admin."""
    position = PositionFactory(agency=agency, name='Sales Agent', level=1)
    return UserFactory(
        agency=agency,
        position=position,
        upline=admin_user,
        role='agent',
        is_admin=False,
        first_name='Agent',
        last_name='Smith',
    )


@pytest.fixture
def downline_agent(agency, agent_user):
    """Create an agent who reports to agent_user (downline)."""
    position = PositionFactory(agency=agency, name='Junior Agent', level=2)
    return UserFactory(
        agency=agency,
        position=position,
        upline=agent_user,
        role='agent',
        is_admin=False,
        first_name='Downline',
        last_name='Jones',
    )


# =============================================================================
# Product Catalog Fixtures
# =============================================================================


@pytest.fixture
def test_carrier(agency):
    """Create a test carrier."""
    return CarrierFactory(name='Test Life Insurance', code='TLI')


@pytest.fixture
def test_product(test_carrier, agency):
    """Create a test product."""
    return ProductFactory(
        carrier=test_carrier,
        agency=agency,
        name='Term Life 20',
    )


@pytest.fixture
def test_products(test_carrier, agency):
    """Create multiple test products."""
    return [
        ProductFactory(carrier=test_carrier, agency=agency, name='Term Life 20'),
        ProductFactory(carrier=test_carrier, agency=agency, name='Whole Life'),
        ProductFactory(carrier=test_carrier, agency=agency, name='Universal Life'),
    ]


# =============================================================================
# Client Fixtures
# =============================================================================


@pytest.fixture
def test_client(agency):
    """Create a test client."""
    return ClientFactory(
        agency=agency,
        first_name='John',
        last_name='Client',
        email='john.client@example.com',
        phone='+15551234567',
    )


@pytest.fixture
def test_clients(agency):
    """Create multiple test clients."""
    return [
        ClientFactory(agency=agency) for _ in range(5)
    ]


# =============================================================================
# Deal Fixtures
# =============================================================================


@pytest.fixture
def test_deal(agency, agent_user, test_client, test_product, test_carrier):
    """Create a single test deal."""
    return DealFactory(
        agency=agency,
        agent=agent_user,
        client=test_client,
        product=test_product,
        carrier=test_carrier,
        status='Active',
        status_standardized='active',
        annual_premium=Decimal('12000.00'),
        policy_effective_date=date.today() - timedelta(days=30),
    )


@pytest.fixture
def test_deals(agency, agent_user, test_product, test_carrier):
    """Create multiple test deals with various statuses."""
    deals = []
    statuses = [
        ('Active', 'active'),
        ('Active', 'active'),
        ('Pending', 'pending'),
        ('Lapsed', 'lapsed'),
        ('Active', 'active'),
    ]

    for i, (status, status_std) in enumerate(statuses):
        client = ClientFactory(agency=agency)
        deal = DealFactory(
            agency=agency,
            agent=agent_user,
            client=client,
            product=test_product,
            carrier=test_carrier,
            status=status,
            status_standardized=status_std,
            annual_premium=Decimal(str(10000 + i * 1000)),
            policy_effective_date=date.today() - timedelta(days=i * 10),
        )
        deals.append(deal)

    return deals


@pytest.fixture
def downline_deals(agency, downline_agent, test_product, test_carrier):
    """Create deals for a downline agent."""
    deals = []
    for i in range(3):
        client = ClientFactory(agency=agency)
        deal = DealFactory(
            agency=agency,
            agent=downline_agent,
            client=client,
            product=test_product,
            carrier=test_carrier,
            annual_premium=Decimal(str(8000 + i * 500)),
        )
        deals.append(deal)
    return deals


# =============================================================================
# SMS Fixtures
# =============================================================================


@pytest.fixture
def test_conversation(agency, agent_user, test_client):
    """Create a test SMS conversation."""
    return ConversationFactory(
        agency=agency,
        agent=agent_user,
        client=test_client,
        phone_number=test_client.phone,
        sms_opt_in_status='opted_in',
    )


@pytest.fixture
def test_messages(test_conversation, agent_user):
    """Create test messages in a conversation."""
    messages = []
    # Outbound message
    messages.append(MessageFactory(
        conversation=test_conversation,
        content='Hello! Following up on your policy.',
        direction='outbound',
        status='sent',
        sent_by=agent_user,
    ))
    # Inbound response
    messages.append(MessageFactory(
        conversation=test_conversation,
        content='Thanks for reaching out!',
        direction='inbound',
        status='received',
        sent_by=None,
    ))
    return messages


@pytest.fixture
def test_sms_template(agency, admin_user):
    """Create a test SMS template."""
    return SmsTemplateFactory(
        agency=agency,
        name='Welcome Message',
        content='Welcome {{client_name}}! Thank you for choosing us.',
        category='welcome',
        created_by=admin_user,
    )


# =============================================================================
# Status Mapping Fixtures
# =============================================================================


@pytest.fixture
def status_mappings(test_carrier):
    """Create status mappings for a carrier."""
    return [
        StatusMappingFactory(
            carrier=test_carrier,
            raw_status='Active',
            standardized_status='active',
            impact='positive',
        ),
        StatusMappingFactory(
            carrier=test_carrier,
            raw_status='Pending',
            standardized_status='pending',
            impact='neutral',
        ),
        StatusMappingFactory(
            carrier=test_carrier,
            raw_status='Lapsed',
            standardized_status='lapsed',
            impact='negative',
        ),
        StatusMappingFactory(
            carrier=test_carrier,
            raw_status='Cancelled',
            standardized_status='cancelled',
            impact='negative',
        ),
    ]


# =============================================================================
# API Client Fixtures
# =============================================================================


@pytest.fixture
def api_client():
    """Create a basic API client."""
    return APIClient()


@pytest.fixture
def mock_auth_user(agent_user):
    """Create a MockAuthenticatedUser matching the agent_user fixture."""
    return MockAuthenticatedUser(
        user_id=agent_user.id,
        agency_id=agent_user.agency_id,
        email=agent_user.email,
        role=agent_user.role,
        is_admin=agent_user.is_admin,
        first_name=agent_user.first_name,
        last_name=agent_user.last_name,
        position_id=agent_user.position_id,
        upline_id=agent_user.upline_id,
    )


@pytest.fixture
def mock_auth_admin(admin_user):
    """Create a MockAuthenticatedUser matching the admin_user fixture."""
    return MockAuthenticatedUser(
        user_id=admin_user.id,
        agency_id=admin_user.agency_id,
        email=admin_user.email,
        role=admin_user.role,
        is_admin=admin_user.is_admin,
        first_name=admin_user.first_name,
        last_name=admin_user.last_name,
        position_id=admin_user.position_id,
        upline_id=admin_user.upline_id if admin_user.upline else None,
    )


@pytest.fixture
def authenticated_api_client(api_client, mock_auth_user, mocker):
    """API client with mocked authentication for agent user."""
    mocker.patch(
        'apps.core.authentication.get_user_context',
        return_value=mock_auth_user
    )
    return api_client, mock_auth_user


@pytest.fixture
def admin_api_client(api_client, mock_auth_admin, mocker):
    """API client with mocked authentication for admin user."""
    mocker.patch(
        'apps.core.authentication.get_user_context',
        return_value=mock_auth_admin
    )
    return api_client, mock_auth_admin
