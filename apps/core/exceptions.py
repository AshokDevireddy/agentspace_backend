"""
Custom Exception Handling for AgentSpace Backend

Provides consistent error response format across all API endpoints.
"""
import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import exception_handler

logger = logging.getLogger(__name__)


def custom_exception_handler(exc, context):
    """
    Custom exception handler that provides consistent error format.

    Error Response Format:
    {
        "error": "ErrorType",
        "message": "Human-readable error message",
        "details": {...}  // Optional, additional context
    }
    """
    # Call REST framework's default exception handler first
    response = exception_handler(exc, context)

    if response is not None:
        # Standardize the response format
        error_data = {
            'error': exc.__class__.__name__,
            'message': str(exc.detail) if hasattr(exc, 'detail') else str(exc),
        }

        # Add status code to error data
        if hasattr(exc, 'status_code'):
            error_data['status_code'] = exc.status_code

        # Handle DRF validation errors specially
        if hasattr(exc, 'detail'):
            if isinstance(exc.detail, dict):
                error_data['details'] = exc.detail
                # Create a summary message from field errors
                messages = []
                for field, errors in exc.detail.items():
                    if isinstance(errors, list):
                        messages.append(f"{field}: {', '.join(str(e) for e in errors)}")
                    else:
                        messages.append(f"{field}: {errors}")
                error_data['message'] = '; '.join(messages)
            elif isinstance(exc.detail, list):
                error_data['message'] = ', '.join(str(e) for e in exc.detail)

        response.data = error_data

    else:
        # Handle unexpected exceptions
        logger.exception(f'Unhandled exception: {exc}')

        error_data = {
            'error': 'InternalServerError',
            'message': 'An unexpected error occurred',
        }

        response = Response(
            error_data,
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

    return response


class APIException(Exception):
    """
    Base exception class for API errors.

    Usage:
        raise APIException('Something went wrong', status_code=400)
    """
    def __init__(self, message: str, status_code: int = 400, details: dict | None = None):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)


class ValidationError(APIException):
    """Raised when request validation fails."""
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message, status_code=400, details=details)


class AuthenticationError(APIException):
    """Raised when authentication fails."""
    def __init__(self, message: str = 'Authentication required'):
        super().__init__(message, status_code=401)


class PermissionDeniedError(APIException):
    """Raised when user lacks permission."""
    def __init__(self, message: str = 'Permission denied'):
        super().__init__(message, status_code=403)


class NotFoundError(APIException):
    """Raised when a resource is not found."""
    def __init__(self, message: str = 'Resource not found'):
        super().__init__(message, status_code=404)


class ConflictError(APIException):
    """Raised when there's a conflict (e.g., duplicate resource)."""
    def __init__(self, message: str = 'Resource conflict'):
        super().__init__(message, status_code=409)
