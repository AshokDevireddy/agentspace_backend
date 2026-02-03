"""
Onboarding API Views

Provides endpoints for onboarding flow management:
- GET/PATCH /api/onboarding/progress - Get/update onboarding state
- POST /api/onboarding/nipr/start - Start NIPR verification
- GET /api/onboarding/nipr/status - Get NIPR job status
- POST /api/onboarding/nipr/upload - Upload NIPR document
- GET /api/onboarding/nipr/sse - SSE stream for NIPR progress
- POST /api/onboarding/invitations - Add pending invitation
- DELETE /api/onboarding/invitations/{index} - Remove pending invitation
- POST /api/onboarding/invitations/send - Send all pending invitations
- POST /api/onboarding/complete - Complete onboarding
"""
import logging
from uuid import UUID

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.authentication import get_user_context

from .selectors import (
    check_nipr_already_completed,
    get_nipr_job_with_progress,
    get_onboarding_progress,
)
from apps.agents.services import invite_agent

from .services import (
    add_pending_invitation,
    clear_pending_invitations,
    complete_onboarding,
    create_onboarding_progress,
    remove_pending_invitation,
    store_nipr_carriers,
    update_nipr_status,
    update_onboarding_step,
)

logger = logging.getLogger(__name__)


class OnboardingProgressView(APIView):
    """
    GET /api/onboarding/progress
    Get current onboarding state for the authenticated user.

    PATCH /api/onboarding/progress
    Update onboarding state (step, NIPR status, etc).
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        progress = get_onboarding_progress(user.id)

        # If no progress exists, create it
        if not progress:
            progress = create_onboarding_progress(user.id)

        # Also check if NIPR was already completed (from users table)
        nipr_check = check_nipr_already_completed(user.id)
        if nipr_check['completed'] and progress.get('nipr_status') == 'pending':
            # Update progress to reflect completed NIPR
            update_nipr_status(user.id, 'completed', carriers=nipr_check['carriers'])
            progress = get_onboarding_progress(user.id)

        return Response(progress)

    def patch(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Handle step update
        step = request.data.get('step')
        if step:
            try:
                updated = update_onboarding_step(user.id, step)
                if not updated:
                    return Response(
                        {'error': 'Failed to update step'},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
            except ValueError as e:
                return Response(
                    {'error': str(e)},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Handle NIPR status update
        nipr_status = request.data.get('nipr_status')
        if nipr_status:
            try:
                update_nipr_status(
                    user_id=user.id,
                    status=nipr_status,
                    job_id=request.data.get('nipr_job_id'),
                    carriers=request.data.get('nipr_carriers'),
                    licensed_states=request.data.get('nipr_licensed_states'),
                )
            except ValueError as e:
                return Response(
                    {'error': str(e)},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Return updated progress
        progress = get_onboarding_progress(user.id)
        return Response(progress)


class NiprStatusView(APIView):
    """
    GET /api/onboarding/nipr/status
    Check if NIPR verification is already complete.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        result = check_nipr_already_completed(user.id)
        return Response(result)


class NiprJobStatusView(APIView):
    """
    GET /api/onboarding/nipr/job/{job_id}
    Get detailed status of a NIPR job including queue position.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, job_id):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            job_uuid = UUID(job_id)
        except ValueError:
            return Response(
                {'error': 'Invalid job_id format'},
                status=status.HTTP_400_BAD_REQUEST
            )

        job = get_nipr_job_with_progress(job_uuid)
        if not job:
            return Response(
                {'error': 'Job not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Handle completed job - store carriers
        if job['status'] == 'completed' and job['result_carriers']:
            store_nipr_carriers(user.id, job['result_carriers'])

        return Response(job)


class InvitationsView(APIView):
    """
    GET /api/onboarding/invitations
    Get pending invitations.

    POST /api/onboarding/invitations
    Add a new pending invitation.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        progress = get_onboarding_progress(user.id)
        if not progress:
            return Response({'invitations': []})

        return Response({'invitations': progress.get('pending_invitations', [])})

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Validate required fields
        required_fields = ['firstName', 'lastName', 'email']
        for field in required_fields:
            if not request.data.get(field):
                return Response(
                    {'error': f'{field} is required'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        invitation = {
            'firstName': request.data.get('firstName', '').strip(),
            'lastName': request.data.get('lastName', '').strip(),
            'email': request.data.get('email', '').strip().lower(),
            'phoneNumber': request.data.get('phoneNumber', '').strip(),
            'permissionLevel': request.data.get('permissionLevel', 'agent'),
            'uplineAgentId': request.data.get('uplineAgentId'),
            'preInviteUserId': request.data.get('preInviteUserId'),
        }

        # Ensure progress exists
        progress = get_onboarding_progress(user.id)
        if not progress:
            create_onboarding_progress(user.id)

        added = add_pending_invitation(user.id, invitation)
        if not added:
            return Response(
                {'error': 'Failed to add invitation'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response({'success': True, 'invitation': invitation})


class InvitationDetailView(APIView):
    """
    DELETE /api/onboarding/invitations/{index}
    Remove a pending invitation by index.
    """
    permission_classes = [IsAuthenticated]

    def delete(self, request, index):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            idx = int(index)
        except ValueError:
            return Response(
                {'error': 'Invalid index'},
                status=status.HTTP_400_BAD_REQUEST
            )

        removed = remove_pending_invitation(user.id, idx)
        if not removed:
            return Response(
                {'error': 'Invitation not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        return Response({'success': True})


class SendInvitationsView(APIView):
    """
    POST /api/onboarding/invitations/send
    Send all pending invitations and clear the list.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        # Get and clear pending invitations
        invitations = clear_pending_invitations(user.id)

        if not invitations:
            return Response({
                'success': True,
                'sent': 0,
                'message': 'No invitations to send'
            })

        # Send each invitation using the invite_agent service
        results = []
        errors = []

        for inv in invitations:
            try:
                # Map invitation fields to invite_agent parameters
                result = invite_agent(
                    inviter_id=user.id,
                    agency_id=user.agency_id,
                    email=inv.get('email', ''),
                    first_name=inv.get('firstName', ''),
                    last_name=inv.get('lastName', ''),
                    phone_number=inv.get('phoneNumber'),
                    perm_level=inv.get('permissionLevel', 'agent'),
                    upline_id=UUID(inv['uplineAgentId']) if inv.get('uplineAgentId') else None,
                    pre_invite_user_id=UUID(inv['preInviteUserId']) if inv.get('preInviteUserId') else None,
                )

                if result.get('success'):
                    results.append({
                        'email': inv.get('email'),
                        'success': True,
                        'user_id': result.get('user_id'),
                    })
                else:
                    errors.append({
                        'email': inv.get('email'),
                        'error': result.get('error', 'Unknown error'),
                    })
            except Exception as e:
                logger.error(f'Failed to send invitation to {inv.get("email")}: {e}')
                errors.append({
                    'email': inv.get('email'),
                    'error': str(e),
                })

        return Response({
            'success': len(errors) == 0,
            'sent': len(results),
            'failed': len(errors),
            'results': results,
            'errors': errors,
        })


class CompleteOnboardingView(APIView):
    """
    POST /api/onboarding/complete
    Mark onboarding as complete and activate user.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = get_user_context(request)
        if not user:
            return Response(
                {'error': 'Unauthorized'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        if user.status != 'onboarding':
            return Response(
                {'error': 'User is not in onboarding state'},
                status=status.HTTP_400_BAD_REQUEST
            )

        completed = complete_onboarding(user.id)
        if not completed:
            return Response(
                {'error': 'Failed to complete onboarding'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response({
            'success': True,
            'message': 'Onboarding completed',
            'status': 'active'
        })
