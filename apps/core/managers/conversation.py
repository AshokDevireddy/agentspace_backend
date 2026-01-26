"""
Conversation QuerySet and Manager for optimized SMS conversation queries.
"""

from django.db import models
from django.db.models import OuterRef, Q, Subquery

from apps.core.querysets import HierarchyQuerySetMixin, ViewModeQuerySetMixin


class ConversationQuerySet(HierarchyQuerySetMixin, ViewModeQuerySetMixin, models.QuerySet):
    """
    Custom QuerySet for Conversation model with hierarchy and visibility support.
    """

    def active(self):
        """Filter to active conversations (not archived)."""
        return self.filter(is_archived=False)

    def archived(self):
        """Filter to archived conversations."""
        return self.filter(is_archived=True)

    def opted_in(self):
        """Filter to conversations where client has opted in to SMS."""
        return self.filter(sms_opt_in_status='opted_in')

    def for_agency(self, agency_id):
        """Filter to conversations in a specific agency."""
        return self.filter(agency_id=agency_id)

    def for_client(self, client_id):
        """Filter to conversations for a specific client."""
        return self.filter(client_id=client_id)

    def with_relations(self):
        """
        Include commonly needed relations.

        Optimized chain for conversation listing views.
        """
        return self.select_related(
            'client',
            'agent',
            'agent__position',
            'agency'
        )

    def with_client(self):
        """Include client details."""
        return self.select_related('client')

    def with_agent(self):
        """Include agent details."""
        return self.select_related('agent', 'agent__position')

    def with_last_message(self):
        """
        Annotate with the last message details.

        Adds last_message_at and last_message_content annotations.
        """
        from apps.core.models import Message

        # Subquery for last message timestamp
        last_message_subquery = (
            Message.objects.filter(conversation_id=OuterRef('id'))
            .order_by('-created_at')
            .values('created_at')[:1]
        )

        # Subquery for last message content
        last_content_subquery = (
            Message.objects.filter(conversation_id=OuterRef('id'))
            .order_by('-created_at')
            .values('content')[:1]
        )

        return self.annotate(
            last_message_at=Subquery(last_message_subquery),
            last_message_content=Subquery(last_content_subquery)
        )

    def with_unread_count(self, for_agent: bool = True):
        """
        Annotate with unread message count.

        Args:
            for_agent: If True, count unread by agent. If False, count unread by client.
        """
        from django.db.models import Count


        if for_agent:
            # Count messages from client that agent hasn't read
            return self.annotate(
                unread_count=Count(
                    'messages',
                    filter=Q(
                        messages__direction='inbound',
                        messages__is_read=False
                    )
                )
            )
        else:
            # Count messages from agent that client hasn't read
            return self.annotate(
                unread_count=Count(
                    'messages',
                    filter=Q(
                        messages__direction='outbound',
                        messages__is_read=False
                    )
                )
            )

    def search(self, query: str):
        """
        Search conversations by client name, phone, or message content.

        Args:
            query: Search string

        Returns:
            Filtered queryset
        """
        if not query:
            return self

        return self.filter(
            Q(client__first_name__icontains=query) |
            Q(client__last_name__icontains=query) |
            Q(client__phone__icontains=query)
        )

    def order_by_recent(self):
        """Order by most recently updated (last message)."""
        return self.order_by('-updated_at')


class ConversationManager(models.Manager):
    """
    Manager for Conversation model with optimized queries.
    """

    def get_queryset(self):
        return ConversationQuerySet(self.model, using=self._db)

    def active(self):
        return self.get_queryset().active()

    def opted_in(self):
        return self.get_queryset().opted_in()

    def with_relations(self):
        return self.get_queryset().with_relations()

    def with_last_message(self):
        return self.get_queryset().with_last_message()

    def for_agency(self, agency_id):
        return self.get_queryset().for_agency(agency_id)

    def search(self, query: str):
        return self.get_queryset().search(query)

    def for_view_mode(self, user, view_mode='downlines'):
        return self.get_queryset().for_view_mode(user, view_mode)

    def visible_to(self, user):
        return self.get_queryset().visible_to(user)

    def in_downline_of(self, user_id, agency_id, max_depth=None, include_self=True):
        return self.get_queryset().in_downline_of(
            user_id, agency_id, max_depth, include_self
        )
