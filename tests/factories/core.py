"""
Core Model Factories

Factories for Agency, User, Position, Carrier, Product, Client, and related models.
"""
import uuid
from datetime import date, timedelta

import factory
from factory import fuzzy
from faker import Faker

from apps.core.models import (
    Agency,
    User,
    Position,
    Carrier,
    Product,
    Client,
    PositionProductCommission,
)

fake = Faker()


class AgencyFactory(factory.django.DjangoModelFactory):
    """Factory for Agency model."""

    class Meta:
        model = Agency

    id = factory.LazyFunction(uuid.uuid4)
    name = factory.LazyAttribute(lambda _: f"{fake.company()} Insurance")
    display_name = factory.LazyAttribute(lambda o: o.name)
    logo_url = factory.LazyAttribute(lambda _: fake.image_url())
    primary_color = factory.LazyAttribute(lambda _: fake.hex_color())
    sms_enabled = False


class PositionFactory(factory.django.DjangoModelFactory):
    """Factory for Position model."""

    class Meta:
        model = Position

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    name = factory.LazyAttribute(lambda _: fake.job())
    description = factory.LazyAttribute(lambda _: fake.sentence())
    level = factory.Sequence(lambda n: n)
    is_active = True


class UserFactory(factory.django.DjangoModelFactory):
    """Factory for User model."""

    class Meta:
        model = User

    id = factory.LazyFunction(uuid.uuid4)
    auth_user_id = factory.LazyFunction(uuid.uuid4)
    email = factory.LazyAttribute(lambda _: fake.unique.email())
    first_name = factory.LazyAttribute(lambda _: fake.first_name())
    last_name = factory.LazyAttribute(lambda _: fake.last_name())
    phone = factory.LazyAttribute(lambda _: fake.phone_number()[:20])
    agency = factory.SubFactory(AgencyFactory)
    position = factory.SubFactory(PositionFactory, agency=factory.SelfAttribute('..agency'))
    role = 'agent'
    is_admin = False
    is_active = True
    status = 'active'
    start_date = factory.LazyAttribute(lambda _: date.today() - timedelta(days=fake.random_int(30, 365)))
    annual_goal = factory.LazyAttribute(lambda _: fake.pydecimal(min_value=50000, max_value=500000, right_digits=2))
    total_prod = 0
    total_policies_sold = 0

    @factory.lazy_attribute
    def upline(self):
        """Optionally create an upline."""
        return None

    class Params:
        with_upline = factory.Trait(
            upline=factory.SubFactory(
                'tests.factories.core.UserFactory',
                agency=factory.SelfAttribute('..agency'),
            )
        )
        admin = factory.Trait(
            role='admin',
            is_admin=True,
        )


class CarrierFactory(factory.django.DjangoModelFactory):
    """Factory for Carrier model."""

    class Meta:
        model = Carrier

    id = factory.LazyFunction(uuid.uuid4)
    name = factory.LazyAttribute(lambda _: f"{fake.company()} Life")
    code = factory.LazyAttribute(lambda _: fake.lexify(text='???').upper())
    is_active = True


class ProductFactory(factory.django.DjangoModelFactory):
    """Factory for Product model."""

    class Meta:
        model = Product

    id = factory.LazyFunction(uuid.uuid4)
    carrier = factory.SubFactory(CarrierFactory)
    agency = factory.SubFactory(AgencyFactory)
    name = factory.LazyAttribute(lambda _: f"{fake.word().title()} Life Insurance")
    is_active = True


class ClientFactory(factory.django.DjangoModelFactory):
    """Factory for Client model."""

    class Meta:
        model = Client

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    first_name = factory.LazyAttribute(lambda _: fake.first_name())
    last_name = factory.LazyAttribute(lambda _: fake.last_name())
    email = factory.LazyAttribute(lambda _: fake.unique.email())
    phone = factory.LazyAttribute(lambda _: fake.phone_number()[:20])


class PositionProductCommissionFactory(factory.django.DjangoModelFactory):
    """Factory for PositionProductCommission model."""

    class Meta:
        model = PositionProductCommission

    id = factory.LazyFunction(uuid.uuid4)
    position = factory.SubFactory(PositionFactory)
    product = factory.SubFactory(ProductFactory, agency=factory.SelfAttribute('..position.agency'))
    commission_percentage = factory.LazyAttribute(
        lambda _: fake.pydecimal(min_value=40, max_value=90, right_digits=2)
    )
