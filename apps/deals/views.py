"""
Deals API Views (P1-011, P1-012, P1-013, P1-014, P2-027, P2-028)

Endpoints:
- GET /api/deals - List deals (book of business)
- POST /api/deals - Create a new deal
- GET /api/deals/{id} - Get deal details
- PUT/PATCH /api/deals/{id} - Update a deal
- DELETE /api/deals/{id} - Delete a deal
- POST /api/deals/{id}/status - Update deal status
- GET /api/deals/filter-options - Get filter options
"""
import logging
from decimal import Decimal

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.constants import PAGINATION
from apps.core.mixins import AuthenticatedAPIView

from .selectors import get_book_of_business, get_post_deal_form_data, get_products_by_carrier, get_static_filter_options
from .services import (
    DealCreateInput,
    DealUpdateInput,
    create_deal,
    delete_deal,
    get_deal_by_id,
    update_deal,
    update_deal_status,
)

logger = logging.getLogger(__name__)


class DealsListCreateView(AuthenticatedAPIView, APIView):
    """GET/POST /api/deals - List deals or create a new deal."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get paginated book of business (deals)."""
        user = self.get_user(request)

        limit = min(
            int(request.query_params.get('limit', PAGINATION["default_limit"])),
            PAGINATION["max_limit"],
        )

        cursor_date = (
            request.query_params.get('cursor_policy_effective_date') or
            request.query_params.get('cursor_created_at')
        )
        cursor_policy_effective_date = self.parse_date(cursor_date)

        cursor_id = request.query_params.get('cursor_id')
        cursor_uuid = self.parse_uuid_optional(cursor_id)

        # Parse filter params
        carrier_id = self.parse_uuid_optional(request.query_params.get('carrier_id'))
        product_id = self.parse_uuid_optional(request.query_params.get('product_id'))
        agent_id = self.parse_uuid_optional(request.query_params.get('agent_id'))
        client_id = self.parse_uuid_optional(request.query_params.get('client_id'))

        status_filter = request.query_params.get('status')
        status_standardized = request.query_params.get('status_standardized')

        date_from = self.parse_date(
            request.query_params.get('date_from') or
            request.query_params.get('effective_date_start')
        )
        date_to = self.parse_date(
            request.query_params.get('date_to') or
            request.query_params.get('effective_date_end')
        )

        search_query = request.query_params.get('search', '').strip() or None
        policy_number = request.query_params.get('policy_number', '').strip() or None
        billing_cycle = request.query_params.get('billing_cycle', '').strip() or None
        lead_source = request.query_params.get('lead_source', '').strip() or None
        view = request.query_params.get('view', 'downlines')
        effective_date_sort = request.query_params.get('effective_date_sort')

        is_admin = user.is_admin or user.role == 'admin'

        result = get_book_of_business(
            user=user,
            limit=limit,
            cursor_policy_effective_date=cursor_policy_effective_date,
            cursor_id=cursor_uuid,
            carrier_id=carrier_id,
            product_id=product_id,
            agent_id=agent_id,
            client_id=client_id,
            status=status_filter,
            status_standardized=status_standardized,
            date_from=date_from,
            date_to=date_to,
            search_query=search_query,
            policy_number=policy_number,
            billing_cycle=billing_cycle,
            lead_source=lead_source,
            view=view,
            effective_date_sort=effective_date_sort,
            include_full_agency=is_admin,
        )

        return Response(result)

    def post(self, request):
        """Create a new deal (P1-012)."""
        user = self.get_user(request)
        data = request.data

        agent_id = data.get('agent_id')
        if not agent_id:
            return Response(
                {'error': 'agent_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        agent_uuid = self.parse_uuid_optional(agent_id)
        if not agent_uuid:
            return Response(
                {'error': 'Invalid agent_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        input_data = DealCreateInput(
            agency_id=user.agency_id,
            agent_id=agent_uuid,
            client_id=self.parse_uuid_optional(data.get('client_id')),
            carrier_id=self.parse_uuid_optional(data.get('carrier_id')),
            product_id=self.parse_uuid_optional(data.get('product_id')),
            policy_number=data.get('policy_number'),
            status=data.get('status'),
            status_standardized=data.get('status_standardized'),
            annual_premium=Decimal(str(data['annual_premium'])) if data.get('annual_premium') else None,
            monthly_premium=Decimal(str(data['monthly_premium'])) if data.get('monthly_premium') else None,
            policy_effective_date=data.get('policy_effective_date'),
            submission_date=data.get('submission_date'),
            billing_cycle=data.get('billing_cycle'),
            lead_source=data.get('lead_source'),
        )

        deal = create_deal(user, input_data)
        return Response(deal, status=status.HTTP_201_CREATED)


class DealDetailView(AuthenticatedAPIView, APIView):
    """GET/PUT/PATCH/DELETE /api/deals/{id} - Deal CRUD operations."""

    permission_classes = [IsAuthenticated]

    def get(self, request, deal_id):
        """Get deal details (P1-011)."""
        user = self.get_user(request)
        deal_uuid = self.parse_uuid(deal_id, "deal_id")

        deal = get_deal_by_id(deal_uuid, user)
        if not deal:
            return Response(
                {'error': 'Deal not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response(deal)

    def put(self, request, deal_id):
        """Full update of a deal (P1-013)."""
        return self._update_deal(request, deal_id)

    def patch(self, request, deal_id):
        """Partial update of a deal (P1-013)."""
        return self._update_deal(request, deal_id)

    def delete(self, request, deal_id):
        """Delete a deal."""
        user = self.get_user(request)
        deal_uuid = self.parse_uuid(deal_id, "deal_id")

        deleted = delete_deal(deal_uuid, user)
        if not deleted:
            return Response(
                {'error': 'Deal not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response({'success': True})

    def _update_deal(self, request, deal_id):
        """Update deal helper."""
        user = self.get_user(request)
        deal_uuid = self.parse_uuid(deal_id, "deal_id")
        data = request.data

        input_data = DealUpdateInput(
            client_id=self.parse_uuid_optional(data.get('client_id')) if 'client_id' in data else None,
            carrier_id=self.parse_uuid_optional(data.get('carrier_id')) if 'carrier_id' in data else None,
            product_id=self.parse_uuid_optional(data.get('product_id')) if 'product_id' in data else None,
            policy_number=data.get('policy_number') if 'policy_number' in data else None,
            status=data.get('status') if 'status' in data else None,
            status_standardized=data.get('status_standardized') if 'status_standardized' in data else None,
            annual_premium=Decimal(str(data['annual_premium'])) if 'annual_premium' in data and data['annual_premium'] else None,
            monthly_premium=Decimal(str(data['monthly_premium'])) if 'monthly_premium' in data and data['monthly_premium'] else None,
            policy_effective_date=data.get('policy_effective_date') if 'policy_effective_date' in data else None,
            submission_date=data.get('submission_date') if 'submission_date' in data else None,
            billing_cycle=data.get('billing_cycle') if 'billing_cycle' in data else None,
            lead_source=data.get('lead_source') if 'lead_source' in data else None,
        )

        deal = update_deal(deal_uuid, user, input_data)
        if not deal:
            return Response(
                {'error': 'Deal not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response(deal)


class DealStatusView(AuthenticatedAPIView, APIView):
    """POST /api/deals/{id}/status - Update deal status (P1-014)."""

    permission_classes = [IsAuthenticated]

    def post(self, request, deal_id):
        """Update deal status."""
        user = self.get_user(request)
        deal_uuid = self.parse_uuid(deal_id, "deal_id")

        new_status = request.data.get('status')
        new_status_standardized = request.data.get('status_standardized')

        if not new_status:
            return Response(
                {'error': 'status is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        deal = update_deal_status(deal_uuid, user, new_status, new_status_standardized)
        if not deal:
            return Response(
                {'error': 'Deal not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        return Response(deal)


class BookOfBusinessView(AuthenticatedAPIView, APIView):
    """GET /api/deals/book-of-business - Alias for GET /api/deals."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        view = DealsListCreateView()
        view.request = request
        return view.get(request)


class FilterOptionsView(AuthenticatedAPIView, APIView):
    """GET /api/deals/filter-options - Get filter options for deals."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        options = get_static_filter_options(user)
        return Response(options)


class FormDataView(AuthenticatedAPIView, APIView):
    """GET /api/deals/form-data - Get form data for Post A Deal page."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        form_data = get_post_deal_form_data(user)
        return Response(form_data)


class ProductsByCarrierView(AuthenticatedAPIView, APIView):
    """GET /api/deals/products-by-carrier - Get products for a specific carrier."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        carrier_id = request.query_params.get('carrier_id')

        if not carrier_id:
            return Response(
                {'error': 'carrier_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        carrier_uuid = self.parse_uuid(carrier_id, "carrier_id")
        products = get_products_by_carrier(user, carrier_uuid)
        return Response(products)
