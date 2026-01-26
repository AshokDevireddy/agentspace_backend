"""
SMS Model Factories

Factories for Conversation, Message, DraftMessage, and SmsTemplate models.
"""
import uuid

import factory
from django.utils import timezone
from faker import Faker

from apps.core.models import (
    Conversation,
    DraftMessage,
    Message,
    SmsTemplate,
)
from tests.factories.core import (
    AgencyFactory,
    ClientFactory,
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
    client = factory.SubFactory(ClientFactory, agency=factory.SelfAttribute('..agency'))
    deal = None  # Optional, set explicitly when needed
    phone_number = factory.LazyAttribute(lambda _: fake.phone_number()[:20])
    last_message_at = factory.LazyAttribute(lambda _: timezone.now())
    unread_count = 0
    is_archived = False
    sms_opt_in_status = 'opted_in'

    class Params:
        with_deal = factory.Trait(
            deal=factory.SubFactory(DealFactory, agency=factory.SelfAttribute('..agency'))
        )
        opted_out = factory.Trait(
            sms_opt_in_status='opted_out',
            opted_out_at=factory.LazyAttribute(lambda _: timezone.now())
        )
        archived = factory.Trait(
            is_archived=True
        )


class MessageFactory(factory.django.DjangoModelFactory):
    """Factory for Message model."""

    class Meta:
        model = Message

    id = factory.LazyFunction(uuid.uuid4)
    conversation = factory.SubFactory(ConversationFactory)
    content = factory.LazyAttribute(lambda _: fake.sentence())
    direction = 'outbound'
    status = 'sent'
    external_id = factory.LazyAttribute(lambda _: fake.uuid4())
    sent_by = factory.SubFactory(
        UserFactory,
        agency=factory.SelfAttribute('..conversation.agency')
    )
    is_read = False
    sent_at = factory.LazyAttribute(lambda _: timezone.now())

    class Params:
        inbound = factory.Trait(
            direction='inbound',
            status='received',
            sent_by=None
        )
        failed = factory.Trait(
            status='failed'
        )


class DraftMessageFactory(factory.django.DjangoModelFactory):
    """Factory for DraftMessage model."""

    class Meta:
        model = DraftMessage

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    agent = factory.SubFactory(UserFactory, agency=factory.SelfAttribute('..agency'))
    conversation = factory.SubFactory(
        ConversationFactory,
        agency=factory.SelfAttribute('..agency')
    )
    content = factory.LazyAttribute(lambda _: fake.paragraph())
    status = 'pending'

    class Params:
        approved = factory.Trait(
            status='approved'
        )
        rejected = factory.Trait(
            status='rejected'
        )


class SmsTemplateFactory(factory.django.DjangoModelFactory):
    """Factory for SmsTemplate model."""

    class Meta:
        model = SmsTemplate

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    name = factory.LazyAttribute(lambda _: f"{fake.word().title()} Template")
    content = factory.LazyAttribute(
        lambda _: f"Hello {{{{client_name}}}}, {fake.sentence()}"
    )
    category = factory.LazyAttribute(lambda _: fake.random_element([
        'welcome', 'reminder', 'follow-up', 'birthday', 'anniversary'
    ]))
    is_active = True
    created_by = factory.SubFactory(UserFactory, agency=factory.SelfAttribute('..agency'))
