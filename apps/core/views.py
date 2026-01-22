"""
Core Views for AgentSpace Backend

Contains health check and other utility endpoints.
"""
from django.db import connection
from django.http import JsonResponse
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny


@api_view(['GET'])
@permission_classes([AllowAny])
def health_check(request):
    """
    Health check endpoint for deployment verification.

    Returns:
        - 200: Service is healthy
        - 503: Service is unhealthy (database connection failed)

    Response includes:
        - status: 'healthy' or 'unhealthy'
        - database: 'connected' or error message
        - authenticated: True if user is authenticated (for testing auth)
        - user_id: User ID if authenticated (for debugging)
    """
    response_data = {
        'status': 'healthy',
        'service': 'agentspace-backend',
        'database': 'unknown',
    }

    # Check database connection
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1')
            cursor.fetchone()
        response_data['database'] = 'connected'
    except Exception as e:
        response_data['status'] = 'unhealthy'
        response_data['database'] = f'error: {str(e)}'
        return JsonResponse(response_data, status=503)

    # Include auth info if present (for testing)
    user = getattr(request, 'user', None)
    if user and hasattr(user, 'id'):
        response_data['authenticated'] = True
        response_data['user_id'] = str(user.id)
        response_data['agency_id'] = str(user.agency_id)
        response_data['role'] = user.role
    else:
        response_data['authenticated'] = False

    return JsonResponse(response_data)
