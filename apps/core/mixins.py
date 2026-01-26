"""
Core View Mixins

Provides standardized authentication, error handling, and response patterns
for all API views in the application.
"""
from uuid import UUID

from rest_framework.response import Response

from .authentication import AuthenticatedUser, get_user_context
from .exceptions import APIException as APIError
from .exceptions import ValidationError


class AuthenticatedAPIView:
    """
    Mixin providing standardized authentication and error handling.

    Usage:
        class MyView(AuthenticatedAPIView, APIView):
            def get(self, request):
                user = self.get_user(request)  # Raises if not authenticated
                # ... view logic
    """

    def get_user(self, request) -> AuthenticatedUser:
        """
        Get authenticated user or raise 401.

        Returns:
            AuthenticatedUser instance

        Raises:
            Returns 401 response if not authenticated
        """
        user = get_user_context(request)
        if not user:
            raise APIError("Authentication required", status_code=401)
        return user

    def parse_uuid(self, value: str, field_name: str = "id") -> UUID:
        """
        Parse string to UUID or raise validation error.

        Args:
            value: String value to parse
            field_name: Name of field for error message

        Returns:
            UUID instance

        Raises:
            ValidationError if invalid format
        """
        if not value:
            raise ValidationError(f"{field_name} is required")
        try:
            return UUID(value)
        except ValueError as err:
            raise ValidationError(f"Invalid {field_name} format") from err

    def parse_uuid_optional(self, value: str) -> UUID | None:
        """Parse string to UUID, return None if empty or invalid."""
        if not value:
            return None
        try:
            return UUID(value)
        except ValueError:
            return None

    def parse_date(self, value: str, fmt: str = "%Y-%m-%d"):
        """Parse date string, return None if empty or invalid."""
        if not value:
            return None
        try:
            from datetime import datetime
            return datetime.strptime(value, fmt).date()
        except ValueError:
            return None

    def error_response(self, error: APIError) -> Response:
        """Build standardized error response."""
        data: dict = {"error": error.message}
        if error.details:
            data["details"] = error.details
        return Response(data, status=error.status_code)

    def success_response(self, data=None, status_code: int = 200) -> Response:
        """Build standardized success response."""
        if data is None:
            data = {"success": True}
        return Response(data, status=status_code)


def handle_api_errors(func):
    """
    Decorator to handle APIError exceptions in view methods.

    Usage:
        @handle_api_errors
        def get(self, request):
            user = self.get_user(request)
            # ...
    """
    def wrapper(self, request, *args, **kwargs):
        try:
            return func(self, request, *args, **kwargs)
        except APIError as e:
            return Response(
                {"error": e.message, "details": e.details} if e.details else {"error": e.message},
                status=e.status_code
            )
    return wrapper
