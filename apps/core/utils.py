"""
Utility functions for AgentSpace Backend

Common helpers used across serializers and views.
"""


def format_full_name(first_name: str | None, last_name: str | None) -> str:
    """
    Format first and last name into a full name string.

    Args:
        first_name: The first name (can be None)
        last_name: The last name (can be None)

    Returns:
        Formatted full name with whitespace trimmed
    """
    return f"{first_name or ''} {last_name or ''}".strip()
