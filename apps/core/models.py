"""
Core Models for AgentSpace Django Backend

These are UNMANAGED models that map to existing Supabase PostgreSQL tables.
They do NOT create migrations - Django reads from existing tables.
"""
import uuid
from django.db import models


class Agency(models.Model):
    """
    Represents an insurance agency in the system.
    Maps to: public.agencies
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255)
    display_name = models.CharField(max_length=255, null=True, blank=True)
    logo_url = models.TextField(null=True, blank=True)
    primary_color = models.CharField(max_length=50, null=True, blank=True)
    whitelabel_domain = models.CharField(max_length=255, null=True, blank=True)
    sms_enabled = models.BooleanField(default=False)
    sms_template_id = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False  # Don't create migrations
        db_table = 'agencies'
        verbose_name_plural = 'Agencies'

    def __str__(self):
        return self.display_name or self.name


class User(models.Model):
    """
    Represents a user in the system.
    Maps to: public.users
    """
    ROLE_CHOICES = [
        ('admin', 'Admin'),
        ('agent', 'Agent'),
        ('client', 'Client'),
    ]

    STATUS_CHOICES = [
        ('pre-invite', 'Pre-Invite'),
        ('invited', 'Invited'),
        ('onboarding', 'Onboarding'),
        ('active', 'Active'),
        ('inactive', 'Inactive'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    auth_user_id = models.UUIDField(unique=True, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=50, null=True, blank=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='users'
    )
    role = models.CharField(max_length=50, choices=ROLE_CHOICES, default='agent')
    is_admin = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='invited')
    perm_level = models.CharField(max_length=50, null=True, blank=True)
    subscription_tier = models.CharField(max_length=50, null=True, blank=True)
    upline = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='downlines'
    )
    position = models.ForeignKey(
        'Position',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='users'
    )
    start_date = models.DateField(null=True, blank=True)
    annual_goal = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )
    total_prod = models.DecimalField(
        max_digits=15, decimal_places=2, default=0
    )
    total_policies_sold = models.IntegerField(default=0)
    theme_mode = models.CharField(max_length=20, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'users'

    def __str__(self):
        return f"{self.first_name or ''} {self.last_name or ''} ({self.email or 'No email'})".strip()

    @property
    def full_name(self):
        return f"{self.first_name or ''} {self.last_name or ''}".strip() or self.email


class Position(models.Model):
    """
    Represents a position/rank in an agency hierarchy.
    Maps to: public.positions
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='positions'
    )
    level = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'positions'

    def __str__(self):
        return f"{self.name} (Level {self.level})"


class Carrier(models.Model):
    """
    Represents an insurance carrier.
    Maps to: public.carriers
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255)
    code = models.CharField(max_length=50, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'carriers'

    def __str__(self):
        return self.name


class Product(models.Model):
    """
    Represents an insurance product offered by a carrier.
    Maps to: public.products
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255)
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='products'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='products'
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'products'

    def __str__(self):
        return f"{self.name} ({self.carrier.name if self.carrier else 'No carrier'})"


class Client(models.Model):
    """
    Represents a client/customer.
    Maps to: public.clients
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=50, null=True, blank=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='clients'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'clients'

    def __str__(self):
        return f"{self.first_name or ''} {self.last_name or ''} ({self.email or 'No email'})".strip()


class Deal(models.Model):
    """
    Represents an insurance policy/deal.
    Maps to: public.deals
    """
    STATUS_STANDARDIZED_CHOICES = [
        ('active', 'Active'),
        ('pending', 'Pending'),
        ('cancelled', 'Cancelled'),
        ('lapsed', 'Lapsed'),
        ('terminated', 'Terminated'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='deals'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    client = models.ForeignKey(
        Client,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    product = models.ForeignKey(
        Product,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    policy_number = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=255, null=True, blank=True)
    status_standardized = models.CharField(
        max_length=50,
        choices=STATUS_STANDARDIZED_CHOICES,
        null=True,
        blank=True
    )
    annual_premium = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )
    monthly_premium = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )
    policy_effective_date = models.DateField(null=True, blank=True)
    submission_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'deals'

    def __str__(self):
        client_name = f"{self.client.first_name or ''} {self.client.last_name or ''}".strip() if self.client else 'No client'
        return f"{self.policy_number or 'No policy#'} - {client_name}"

    @property
    def client_name(self):
        if self.client:
            return f"{self.client.first_name or ''} {self.client.last_name or ''}".strip()
        return ''
