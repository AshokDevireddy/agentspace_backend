"""
Core managers for User and Deal models.
"""
from .user import UserQuerySet, UserManager
from .deal import DealQuerySet, DealManager
from .conversation import ConversationQuerySet, ConversationManager

__all__ = [
    'UserQuerySet',
    'UserManager',
    'DealQuerySet',
    'DealManager',
    'ConversationQuerySet',
    'ConversationManager',
]
