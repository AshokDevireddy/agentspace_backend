"""
Deal Model Factories

Factories for Deal, DealHierarchySnapshot, and StatusMapping models.
"""
import uuid
from datetime import date, timedelta
from decimal import Decimal

import factory
from factory import fuzzy
from faker import Faker

from apps.core.models import (
    Deal,
    DealHierarchySnapshot,
    StatusMapping,
)
from tests.factories.core import (
    AgencyFactory,
    UserFactory,
    CarrierFactory,
    ProductFactory,
    ClientFactory,
    PositionFactory,
)

fake = Faker()


class DealFactory(factory.django.DjangoModelFactory):
    """Factory for Deal model."""

    class Meta:
        model = Deal

    id = factory.LazyFunction(uuid.uuid4)
    agency = factory.SubFactory(AgencyFactory)
    agent = factory.SubFactory(UserFactory, agency=factory.SelfAttribute('..agency'))
    client = factory.SubFactory(ClientFactory, agency=factory.SelfAttribute('..agency'))
    carrier = factory.SubFactory(CarrierFactory)
    product = factory.SubFactory(ProductFactory, agency=factory.SelfAttribute('..agency'))
    policy_number = factory.LazyAttribute(lambda _: fake.bothify(text='POL-########'))
    status = 'Active'
    status_standardized = 'active'
    annual_premium = factory.LazyAttribute(
        lambda _: Decimal(str(fake.pydecimal(min_value=1000, max_value=50000, right_digits=2)))
    )
    monthly_premium = factory.LazyAttribute(
        lambda o: o.annual_premium / 12 if o.annual_premium else None
    )
    policy_effective_date = factory.LazyAttribute(
        lambda _: date.today() - timedelta(days=fake.random_int(1, 180))
    )
    submission_date = factory.LazyAttribute(
        lambda o: o.policy_effective_date - timedelta(days=fake.random_int(5, 30))
    )

    class Params:
        lapsed = factory.Trait(
            status='Lapsed',
            status_standardized='lapsed',
        )
        pending = factory.Trait(
            status='Pending',
            status_standardized='pending',
        )
        cancelled = factory.Trait(
            status='Cancelled',
            status_standardized='cancelled',
        )


class DealHierarchySnapshotFactory(factory.django.DjangoModelFactory):
    """
    Factory for DealHierarchySnapshot model.

    This captures the agent hierarchy at deal creation time.
    """

    class Meta:
        model = DealHierarchySnapshot

    id = factory.LazyFunction(uuid.uuid4)
    deal = factory.SubFactory(DealFactory)
    agent = factory.SubFactory(UserFactory, agency=factory.SelfAttribute('..deal.agency'))
    position = factory.SubFactory(PositionFactory, agency=factory.SelfAttribute('..deal.agency'))
    hierarchy_level = 0  # 0 = writing agent
    commission_percentage = factory.LazyAttribute(
        lambda _: Decimal(str(fake.pydecimal(min_value=40, max_value=90, right_digits=2)))
    )

    class Params:
        as_upline = factory.Trait(
            hierarchy_level=1,
            commission_percentage=factory.LazyAttribute(
                lambda _: Decimal(str(fake.pydecimal(min_value=5, max_value=20, right_digits=2)))
            ),
        )
        as_upline_level_2 = factory.Trait(
            hierarchy_level=2,
            commission_percentage=factory.LazyAttribute(
                lambda _: Decimal(str(fake.pydecimal(min_value=2, max_value=10, right_digits=2)))
            ),
        )


class StatusMappingFactory(factory.django.DjangoModelFactory):
    """Factory for StatusMapping model."""

    class Meta:
        model = StatusMapping

    id = factory.LazyFunction(uuid.uuid4)
    carrier = factory.SubFactory(CarrierFactory)
    raw_status = factory.LazyAttribute(lambda _: fake.word().title())
    standardized_status = 'active'
    impact = 'positive'

    class Params:
        negative = factory.Trait(
            raw_status='Lapsed',
            standardized_status='lapsed',
            impact='negative',
        )
        neutral = factory.Trait(
            raw_status='Pending',
            standardized_status='pending',
            impact='neutral',
        )
