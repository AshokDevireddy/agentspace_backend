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
- GET /api/deals/by-phone - Find deal by client phone number
"""
import logging
from decimal import Decimal

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import CronSecretAuthentication, SupabaseJWTAuthentication
from apps.core.constants import PAGINATION
from apps.core.mixins import AuthenticatedAPIView

from .selectors import get_book_of_business, get_post_deal_form_data, get_products_by_carrier, get_static_filter_options
from .services import (
    BeneficiaryInput,
    CommissionMappingError,
    DealCreateInput,
    DealLimitReachedError,
    DealUpdateInput,
    DealValidationError,
    PhoneAlreadyExistsError,
    UplinePositionError,
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
        client_phone = request.query_params.get('client_phone', '').strip() or None
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
            client_phone=client_phone,
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

        # Parse beneficiaries if provided
        beneficiaries = None
        if data.get('beneficiaries'):
            beneficiaries = [
                BeneficiaryInput(
                    name=b.get('name'),
                    relationship=b.get('relationship'),
                )
                for b in data['beneficiaries']
                if isinstance(b, dict)
            ]

        input_data = DealCreateInput(
            agency_id=user.agency_id,
            agent_id=agent_uuid,
            client_id=self.parse_uuid_optional(data.get('client_id')),
            carrier_id=self.parse_uuid_optional(data.get('carrier_id')),
            product_id=self.parse_uuid_optional(data.get('product_id')),
            policy_number=data.get('policy_number'),
            application_number=data.get('application_number'),
            status=data.get('status'),
            status_standardized=data.get('status_standardized'),
            annual_premium=Decimal(str(data['annual_premium'])) if data.get('annual_premium') else None,
            monthly_premium=Decimal(str(data['monthly_premium'])) if data.get('monthly_premium') else None,
            policy_effective_date=data.get('policy_effective_date'),
            submission_date=data.get('submission_date'),
            billing_cycle=data.get('billing_cycle'),
            billing_day_of_month=data.get('billing_day_of_month'),
            billing_weekday=data.get('billing_weekday'),
            lead_source=data.get('lead_source'),
            client_name=data.get('client_name'),
            client_email=data.get('client_email'),
            client_phone=data.get('client_phone'),
            client_address=data.get('client_address'),
            date_of_birth=data.get('date_of_birth'),
            ssn_last_4=data.get('ssn_last_4'),
            ssn_benefit=data.get('ssn_benefit'),
            notes=data.get('notes'),
            beneficiaries=beneficiaries,
        )

        try:
            deal = create_deal(user, input_data)
            return Response(deal, status=status.HTTP_201_CREATED)
        except DealLimitReachedError as e:
            return Response(
                {'error': e.message, 'message': e.message, 'limit_reached': True},
                status=status.HTTP_403_FORBIDDEN
            )
        except PhoneAlreadyExistsError as e:
            return Response(
                {'error': e.message, 'existing_deal_id': e.details.get('existing_deal_id')},
                status=status.HTTP_409_CONFLICT
            )
        except UplinePositionError as e:
            return Response(
                {'error': e.message, 'agents_without_positions': e.details.get('agents_without_positions')},
                status=status.HTTP_400_BAD_REQUEST
            )
        except CommissionMappingError as e:
            return Response(
                {'error': e.message, 'positions_without_commissions': e.details.get('positions_without_commissions')},
                status=status.HTTP_400_BAD_REQUEST
            )
        except DealValidationError as e:
            return Response(
                {'error': e.message},
                status=status.HTTP_400_BAD_REQUEST
            )


class DealDetailView(AuthenticatedAPIView, APIView):
    """
    GET/PUT/PATCH/DELETE /api/deals/{id} - Deal CRUD operations.

    Supports CronSecretAuthentication for server-to-server calls.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
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

        # Parse beneficiaries if provided
        beneficiaries = None
        if 'beneficiaries' in data and data.get('beneficiaries') is not None:
            beneficiaries = [
                BeneficiaryInput(
                    name=b.get('name'),
                    relationship=b.get('relationship'),
                )
                for b in data['beneficiaries']
                if isinstance(b, dict)
            ]

        input_data = DealUpdateInput(
            client_id=self.parse_uuid_optional(data.get('client_id')) if 'client_id' in data else None,
            carrier_id=self.parse_uuid_optional(data.get('carrier_id')) if 'carrier_id' in data else None,
            product_id=self.parse_uuid_optional(data.get('product_id')) if 'product_id' in data else None,
            policy_number=data.get('policy_number') if 'policy_number' in data else None,
            application_number=data.get('application_number') if 'application_number' in data else None,
            status=data.get('status') if 'status' in data else None,
            status_standardized=data.get('status_standardized') if 'status_standardized' in data else None,
            annual_premium=Decimal(str(data['annual_premium'])) if 'annual_premium' in data and data['annual_premium'] else None,
            monthly_premium=Decimal(str(data['monthly_premium'])) if 'monthly_premium' in data and data['monthly_premium'] else None,
            policy_effective_date=data.get('policy_effective_date') if 'policy_effective_date' in data else None,
            submission_date=data.get('submission_date') if 'submission_date' in data else None,
            billing_cycle=data.get('billing_cycle') if 'billing_cycle' in data else None,
            billing_day_of_month=data.get('billing_day_of_month') if 'billing_day_of_month' in data else None,
            billing_weekday=data.get('billing_weekday') if 'billing_weekday' in data else None,
            lead_source=data.get('lead_source') if 'lead_source' in data else None,
            client_name=data.get('client_name') if 'client_name' in data else None,
            client_email=data.get('client_email') if 'client_email' in data else None,
            client_phone=data.get('client_phone') if 'client_phone' in data else None,
            client_address=data.get('client_address') if 'client_address' in data else None,
            date_of_birth=data.get('date_of_birth') if 'date_of_birth' in data else None,
            ssn_last_4=data.get('ssn_last_4') if 'ssn_last_4' in data else None,
            ssn_benefit=data.get('ssn_benefit') if 'ssn_benefit' in data else None,
            notes=data.get('notes') if 'notes' in data else None,
            beneficiaries=beneficiaries,
        )

        try:
            deal = update_deal(deal_uuid, user, input_data)
            if not deal:
                return Response(
                    {'error': 'Deal not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            deal['message'] = 'Deal updated successfully'
            return Response(deal)
        except PhoneAlreadyExistsError as e:
            return Response(
                {'error': e.message, 'existing_deal_id': e.details.get('existing_deal_id')},
                status=status.HTTP_409_CONFLICT
            )
        except DealValidationError as e:
            return Response(
                {'error': e.message},
                status=status.HTTP_400_BAD_REQUEST
            )


class DealStatusView(AuthenticatedAPIView, APIView):
    """
    POST /api/deals/{id}/status - Update deal status (P1-014)
    PATCH /api/deals/{id}/status - Update status_standardized only (for cron jobs)
    """

    permission_classes = [IsAuthenticated]

    def post(self, request, deal_id):
        """Update deal status (requires status field)."""
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

    def patch(self, request, deal_id):
        """
        Update status_standardized only (for cron jobs).

        This endpoint allows updating only the status_standardized field
        without requiring the status field. Used by automated cron jobs.
        """
        user = self.get_user(request)
        deal_uuid = self.parse_uuid(deal_id, "deal_id")

        new_status_standardized = request.data.get('status_standardized')

        if not new_status_standardized:
            return Response(
                {'error': 'status_standardized is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        from .services import update_deal_status_standardized
        result = update_deal_status_standardized(deal_uuid, user, new_status_standardized)

        if not result:
            return Response(
                {'error': 'Deal not found or not accessible'},
                status=status.HTTP_404_NOT_FOUND
            )

        return Response({
            'success': True,
            'deal_id': str(deal_uuid),
            'status_standardized': new_status_standardized,
        })


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


class DealByPhoneView(AuthenticatedAPIView, APIView):
    """
    GET /api/deals/by-phone - Find deal by client phone number

    Query params:
    - phone: The client phone number to search for (required)
    - agency_id: Optional agency ID filter (defaults to user's agency)

    Supports CronSecretAuthentication for server-to-server calls.
    """
    authentication_classes = [CronSecretAuthentication, SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)

        phone = request.query_params.get('phone', '').strip()
        if not phone:
            return Response(
                {'error': 'phone query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Use provided agency_id or default to user's agency
        agency_id = request.query_params.get('agency_id')
        if agency_id:
            # Verify user has access to this agency
            if str(user.agency_id) != agency_id and not user.is_admin:
                return Response(
                    {'error': 'Forbidden', 'message': 'You can only search within your own agency'},
                    status=status.HTTP_403_FORBIDDEN
                )
        else:
            agency_id = str(user.agency_id)

        from .selectors import find_deal_by_client_phone
        deal = find_deal_by_client_phone(phone, agency_id)

        if not deal:
            return Response({
                'found': False,
                'deal': None
            })

        return Response({
            'found': True,
            'deal': deal
        })
