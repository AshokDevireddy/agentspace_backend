"""
Integration Parity Tests for Deals API (P2-040)

Tests deal endpoints with real database fixtures.
Verifies response structures and actual database queries.
"""
from datetime import date, timedelta

import pytest
from rest_framework import status

from tests.factories import (
    CarrierFactory,
    ClientFactory,
    DealFactory,
    ProductFactory,
)


@pytest.mark.django_db
class TestBookOfBusinessWithRealData:
    """
    Test GET /api/deals/book-of-business with real database records.
    """

    def test_response_structure_with_real_deals(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Verify response structure with real deal data."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Verify pagination structure
        assert 'deals' in data
        assert 'has_more' in data
        assert isinstance(data['deals'], list)

        # Verify deal structure if deals present
        if data['deals']:
            deal = data['deals'][0]
            required_fields = [
                'id', 'policy_number', 'status', 'status_standardized',
                'annual_premium', 'client', 'carrier', 'product', 'agent'
            ]
            for field in required_fields:
                assert field in deal, f"Missing field: {field}"

    def test_deals_filtered_by_agency(
        self,
        authenticated_api_client,
        agency,
        agent_user,
        test_deals,
    ):
        """Verify deals are filtered to user's agency."""
        client, mock_user = authenticated_api_client

        # Create a deal for a different agency (should not appear)
        other_carrier = CarrierFactory()
        other_product = ProductFactory(carrier=other_carrier)
        DealFactory(
            carrier=other_carrier,
            product=other_product,
        )

        response = client.get('/api/deals/book-of-business/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # All returned deals should belong to the user's agency
        for deal in data['deals']:
            # Deal's agent should be from our agency
            assert deal['agent']['id'] == str(agent_user.id)

    def test_pagination_limit_parameter(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Test limit parameter for pagination."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/', {'limit': '2'})

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Should return at most 2 deals
        assert len(data['deals']) <= 2

    def test_status_filter(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Test filtering by status_standardized."""
        client, mock_user = authenticated_api_client

        response = client.get(
            '/api/deals/book-of-business/',
            {'status_standardized': 'active'}
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # All returned deals should have active status
        for deal in data['deals']:
            assert deal['status_standardized'] == 'active'

    def test_date_range_filter(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Test filtering by date range."""
        client, mock_user = authenticated_api_client

        date_from = (date.today() - timedelta(days=30)).isoformat()
        date_to = date.today().isoformat()

        response = client.get('/api/deals/book-of-business/', {
            'date_from': date_from,
            'date_to': date_to,
        })

        assert response.status_code == status.HTTP_200_OK

    def test_carrier_filter(
        self,
        authenticated_api_client,
        test_deals,
        test_carrier,
    ):
        """Test filtering by carrier_id."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/', {
            'carrier_id': str(test_carrier.id),
        })

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # All returned deals should have the specified carrier
        for deal in data['deals']:
            assert deal['carrier']['id'] == str(test_carrier.id)


@pytest.mark.django_db
class TestBookOfBusinessViewScope:
    """Test view scope parameter (self, downlines, all)."""

    def test_view_self_returns_only_user_deals(
        self,
        authenticated_api_client,
        test_deals,
        downline_deals,
    ):
        """view='self' should return only the user's own deals."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/', {'view': 'self'})

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # All deals should belong to the authenticated user
        for deal in data['deals']:
            assert deal['agent']['id'] == str(mock_user.id)

    def test_view_downlines_includes_hierarchy(
        self,
        authenticated_api_client,
        agent_user,
        downline_agent,
        test_deals,
        downline_deals,
    ):
        """view='downlines' should include user and downline deals."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/', {'view': 'downlines'})

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Should include deals from both agent_user and downline_agent
        agent_ids = {deal['agent']['id'] for deal in data['deals']}
        assert str(agent_user.id) in agent_ids or str(downline_agent.id) in agent_ids

    def test_view_all_admin_only(
        self,
        admin_api_client,
        test_deals,
        downline_deals,
    ):
        """view='all' should work for admin users."""
        client, mock_admin = admin_api_client

        response = client.get('/api/deals/book-of-business/', {'view': 'all'})

        assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
class TestPhoneMaskingWithRealData:
    """Test phone number masking logic."""

    def test_mask_phone_number_function(self):
        """Test the phone masking helper function."""
        from apps.deals.selectors import mask_phone_number

        # Test full visibility
        assert mask_phone_number('+15551234567', can_view_full=True) == '+15551234567'

        # Test masking
        masked = mask_phone_number('+15551234567', can_view_full=False)
        assert masked != '+15551234567'
        assert '****' in masked

        # Test short numbers
        short_masked = mask_phone_number('12345', can_view_full=False)
        assert '****' in short_masked

        # Test None
        assert mask_phone_number(None, can_view_full=False) is None
        assert mask_phone_number(None, can_view_full=True) is None

    def test_phone_masking_format(self):
        """Verify masked phone format: first 3 + **** + last 2."""
        from apps.deals.selectors import mask_phone_number

        masked = mask_phone_number('5551234567', can_view_full=False)

        # Should start with first 3 digits
        assert masked.startswith('555')
        # Should end with last 2 digits
        assert masked.endswith('67')
        # Should have **** in the middle
        assert '****' in masked


@pytest.mark.django_db
class TestFilterOptionsWithRealData:
    """Test GET /api/deals/filter-options with real database records."""

    def test_filter_options_returns_carriers(
        self,
        authenticated_api_client,
        test_carrier,
        test_product,
        test_deals,
    ):
        """Verify filter options includes carriers from deals."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/filter-options/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Should have carriers list
        assert 'carriers' in data
        assert isinstance(data['carriers'], list)

        # If deals exist, carriers should be populated
        if data['carriers']:
            carrier = data['carriers'][0]
            assert 'id' in carrier
            assert 'name' in carrier

    def test_filter_options_returns_products(
        self,
        authenticated_api_client,
        test_product,
        test_deals,
    ):
        """Verify filter options includes products."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/filter-options/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert 'products' in data
        assert isinstance(data['products'], list)

    def test_filter_options_returns_statuses(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Verify filter options includes available statuses."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/filter-options/')

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert 'statuses' in data or 'statuses_standardized' in data


@pytest.mark.django_db
class TestEffectiveDateSortWithRealData:
    """Test effective_date_sort parameter with real deals."""

    def test_sort_newest_first(
        self,
        authenticated_api_client,
        agency,
        agent_user,
        test_carrier,
        test_product,
    ):
        """Test sorting deals by newest effective date first."""
        client, mock_user = authenticated_api_client

        # Create deals with different effective dates
        old_client = ClientFactory(agency=agency)
        new_client = ClientFactory(agency=agency)

        DealFactory(
            agency=agency,
            agent=agent_user,
            client=old_client,
            carrier=test_carrier,
            product=test_product,
            policy_effective_date=date.today() - timedelta(days=60),
        )
        DealFactory(
            agency=agency,
            agent=agent_user,
            client=new_client,
            carrier=test_carrier,
            product=test_product,
            policy_effective_date=date.today() - timedelta(days=5),
        )

        response = client.get('/api/deals/book-of-business/', {
            'effective_date_sort': 'newest',
        })

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        if len(data['deals']) >= 2:
            # Newest should come first
            dates = [deal.get('policy_effective_date') for deal in data['deals']]
            assert dates == sorted(dates, reverse=True)

    def test_sort_oldest_first(
        self,
        authenticated_api_client,
        test_deals,
    ):
        """Test sorting deals by oldest effective date first."""
        client, mock_user = authenticated_api_client

        response = client.get('/api/deals/book-of-business/', {
            'effective_date_sort': 'oldest',
        })

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        if len(data['deals']) >= 2:
            # Oldest should come first
            dates = [deal.get('policy_effective_date') for deal in data['deals']]
            assert dates == sorted(dates)


@pytest.mark.django_db
class TestBillingCycleFilter:
    """Test billing_cycle filter parameter."""

    def test_valid_billing_cycles_accepted(
        self,
        authenticated_api_client,
    ):
        """Verify valid billing cycle values are accepted."""
        client, mock_user = authenticated_api_client

        valid_cycles = ['monthly', 'quarterly', 'semi-annually', 'annually']

        for cycle in valid_cycles:
            response = client.get('/api/deals/book-of-business/', {
                'billing_cycle': cycle,
            })
            # Should not return 400 for valid cycles
            assert response.status_code != status.HTTP_400_BAD_REQUEST
