"""
SMS Model Factories

Factories for Conversation and Message models.
"""
import uuid

import factory
from django.utils import timezone
from faker import Faker

from apps.core.models import (
    Conversation,
    Message,
)
from tests.factories.core import (
    AgencyFactory,
    UserFactory,
)
from tests.factories.deals import DealFactory

fake = Faker()


class ConversationFactory(factory.django.DjangoModelFactory):
    """Factory for Conversation model."""

    class Meta:
        model = Conversation

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    agent = factory.SubFactory(UserFactory, agency=factory.SelfAttribute('..agency'))
    deal = None  # Optional, set explicitly when needed
    client_phone = factory.LazyAttribute(lambda _: fake.phone_number()[:20])
    last_message_at = factory.LazyAttribute(lambda _: timezone.now())
    is_active = True
    sms_opt_in_status = 'opted_in'

    class Params:
        with_deal = factory.Trait(
            deal=factory.SubFactory(DealFactory, agency=factory.SelfAttribute('..agency'))
        )
        opted_out = factory.Trait(
            sms_opt_in_status='opted_out',
            opted_out_at=factory.LazyAttribute(lambda _: timezone.now())
        )


class MessageFactory(factory.django.DjangoModelFactory):
    """Factory for Message model."""

    class Meta:
        model = Message

    id = factory.LazyFunction(uuid.uuid4)
    conversation = factory.SubFactory(ConversationFactory)
    body = factory.LazyAttribute(lambda _: fake.sentence())
    direction = 'outbound'
    status = 'sent'
    sender = factory.SubFactory(
        UserFactory,
        agency=factory.SelfAttribute('..conversation.agency')
    )
    receiver = factory.SubFactory(
        UserFactory,
        agency=factory.SelfAttribute('..conversation.agency')
    )
    read_at = None
    sent_at = factory.LazyAttribute(lambda _: timezone.now())

    class Params:
        inbound = factory.Trait(
            direction='inbound',
            status='received',
        )
        failed = factory.Trait(
            status='failed'
        )
