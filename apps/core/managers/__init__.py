"""
Core managers for User and Deal models.
"""
from .conversation import ConversationManager, ConversationQuerySet
from .deal import DealManager, DealQuerySet
from .user import UserManager, UserQuerySet

__all__ = [
    'UserQuerySet',
    'UserManager',
    'DealQuerySet',
    'DealManager',
    'ConversationQuerySet',
    'ConversationManager',
]
