"""
Core Models for AgentSpace Django Backend

These are UNMANAGED models that map to existing Supabase PostgreSQL tables.
They do NOT create migrations - Django reads from existing tables.
"""
import uuid
from typing import TYPE_CHECKING

from django.contrib.postgres.fields import ArrayField
from django.db import models

from .constants import STATUS_STANDARDIZED_CHOICES
from .utils import format_full_name

if TYPE_CHECKING:
    from uuid import UUID


class Agency(models.Model):
    """
    Represents an insurance agency in the system.
    Maps to: public.agencies
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.TextField()
    display_name = models.TextField()
    logo_url = models.TextField(null=True, blank=True)
    primary_color = models.TextField(null=True, blank=True, default='0 0% 0%')
    whitelabel_domain = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    code = models.TextField()
    is_active = models.BooleanField(default=True, null=True)
    phone_number = models.TextField(null=True, blank=True)
    lead_sources = ArrayField(models.TextField(), default=list, blank=True)
    messaging_enabled = models.BooleanField(default=False)

    # Discord integration
    discord_webhook_url = models.TextField(null=True, blank=True)
    discord_notification_enabled = models.BooleanField(default=False)
    discord_notification_template = models.TextField(null=True, blank=True)
    discord_bot_username = models.TextField(null=True, blank=True, default='AgentSpace Deal Bot')

    # Deactivation tracking
    deactivated_post_a_deal = models.BooleanField(null=True, blank=True, default=False)

    # Display settings
    theme_mode = models.TextField(null=True, blank=True, default='light')
    default_scoreboard_start_date = models.DateField(null=True, blank=True)

    # Lapse email notifications
    lapse_email_notifications_enabled = models.BooleanField(default=False)
    lapse_email_subject = models.TextField(null=True, blank=True)
    lapse_email_body = models.TextField(null=True, blank=True)

    sms_welcome_enabled = models.BooleanField(default=True, null=True)
    sms_welcome_template = models.TextField(null=True, blank=True)
    sms_billing_reminder_enabled = models.BooleanField(default=True, null=True)
    sms_billing_reminder_template = models.TextField(null=True, blank=True)
    sms_lapse_reminder_enabled = models.BooleanField(default=True, null=True)
    sms_lapse_reminder_template = models.TextField(null=True, blank=True)
    sms_birthday_enabled = models.BooleanField(default=True, null=True)
    sms_birthday_template = models.TextField(null=True, blank=True)
    sms_holiday_enabled = models.BooleanField(default=True, null=True)
    sms_holiday_template = models.TextField(null=True, blank=True)
    sms_quarterly_enabled = models.BooleanField(default=True, null=True)
    sms_quarterly_template = models.TextField(null=True, blank=True)
    sms_policy_packet_enabled = models.BooleanField(default=True, null=True)
    sms_policy_packet_template = models.TextField(null=True, blank=True)

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
    email = models.TextField(null=True, blank=True)
    first_name = models.TextField()
    last_name = models.TextField(null=True, blank=True)
    phone_number = models.TextField(null=True, blank=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='users'
    )
    role = models.TextField(choices=ROLE_CHOICES, default='agent')
    is_admin = models.BooleanField(default=False, null=True)
    is_active = models.BooleanField(default=True, null=True)
    status = models.TextField(choices=STATUS_CHOICES, default='onboarding')
    perm_level = models.TextField(default='agent', null=True, blank=True)
    subscription_tier = models.TextField(default='free', null=True, blank=True)
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
        max_digits=12, decimal_places=2, default=0, null=True, blank=True
    )
    total_prod = models.DecimalField(max_digits=15, decimal_places=2, default=0, null=True)
    total_policies_sold = models.DecimalField(max_digits=15, decimal_places=0, default=0, null=True)
    theme_mode = models.CharField(max_length=10, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    billing_cycle_start = models.DateTimeField(null=True, blank=True)
    billing_cycle_end = models.DateTimeField(null=True, blank=True)
    messages_sent_count = models.IntegerField(default=0, null=True)
    ai_requests_count = models.IntegerField(default=0, null=True)

    # Generated columns (read-only, database-computed)
    first_name_lc = models.TextField(null=True, blank=True, editable=False)
    last_name_lc = models.TextField(null=True, blank=True, editable=False)
    email_lc = models.TextField(null=True, blank=True, editable=False)
    phone_last10 = models.TextField(null=True, blank=True, editable=False)
    full_name_norm = models.TextField(null=True, blank=True, editable=False)

    subscription_status = models.TextField(default='free', null=True, blank=True)
    stripe_customer_id = models.TextField(null=True, blank=True)
    stripe_subscription_id = models.TextField(null=True, blank=True)
    scheduled_tier_change = models.TextField(null=True, blank=True)
    scheduled_tier_change_date = models.DateTimeField(null=True, blank=True)

    deals_created_count = models.IntegerField(default=0, null=True)
    ai_requests_reset_date = models.DateTimeField(null=True, blank=True)
    messages_reset_date = models.DateTimeField(null=True, blank=True)

    unique_carriers = ArrayField(models.TextField(), default=list, null=True, blank=True)
    licensed_states = ArrayField(models.TextField(), default=list, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'users'

    def __str__(self):
        name = format_full_name(self.first_name, self.last_name)
        email_part = self.email or 'No email'
        return f"{name} ({email_part})".strip() if name else f"({email_part})"

    @property
    def full_name(self):
        return format_full_name(self.first_name, self.last_name) or self.email

    @property
    def is_administrator(self) -> bool:
        """Check if user has administrator privileges."""
        return self.is_admin or self.role == 'admin'

    # Hierarchy Methods - delegates to centralized hierarchy module
    def get_downline(self, max_depth: int | None = None) -> list['UUID']:
        """
        Get all agents in this user's downline (recursive).

        Uses a recursive CTE for efficient hierarchy traversal.

        Args:
            max_depth: Maximum depth to traverse (None for unlimited)

        Returns:
            List of user IDs in the downline (excludes self)
        """
        from apps.core.hierarchy import get_downline_ids
        # Get agency_id from agency foreign key
        agency_id = self.agency.id if self.agency else None
        if not agency_id:
            return []
        return get_downline_ids(self.id, agency_id, max_depth, include_self=False)

    def get_upline_chain(self) -> list['UUID']:
        """
        Get the chain of uplines from this user to the root.

        Returns:
            List of user IDs from direct upline to root (ordered)
        """
        from apps.core.hierarchy import get_upline_ids
        return get_upline_ids(self.id, include_self=False)

    def is_in_downline(self, target_user_id: 'UUID') -> bool:
        """
        Check if a user is in this user's downline.

        Args:
            target_user_id: The user ID to check

        Returns:
            True if target is in downline
        """
        from apps.core.hierarchy import is_in_downline
        # Get agency_id from agency foreign key
        agency_id = self.agency.id if self.agency else None
        if not agency_id:
            return False
        return is_in_downline(self.id, target_user_id, agency_id)

    @property
    def direct_downlines(self) -> models.QuerySet:
        """Get direct downlines (one level only)."""
        return User.objects.filter(upline_id=self.id)  # type: ignore[attr-defined]

    @property
    def downline_count(self) -> int:
        """Get total count of all agents in downline (recursive)."""
        return len(self.get_downline())


class Position(models.Model):
    """
    Represents a position/rank in an agency hierarchy.
    Maps to: public.positions
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.TextField()
    description = models.TextField(null=True, blank=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='positions'
    )
    level = models.IntegerField()
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'positions'
        ordering = ['level', 'name']

    def __str__(self):
        return f"{self.name} (Level {self.level})"


class Carrier(models.Model):
    """
    Represents an insurance carrier.
    Maps to: public.carriers
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.TextField()
    display_name = models.TextField()
    is_active = models.BooleanField(default=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        managed = False
        db_table = 'carriers'

    def __str__(self):
        return self.display_name or self.name


class Product(models.Model):
    """
    Represents an insurance product offered by a carrier.
    Maps to: public.products
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.TextField()
    product_code = models.TextField(null=True, blank=True)
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='products'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='products'
    )
    is_active = models.BooleanField(default=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = False
        db_table = 'products'

    def __str__(self):
        return f"{self.name} ({self.carrier.name if self.carrier else 'No carrier'})"


class Deal(models.Model):
    """
    Represents an insurance policy/deal.
    Maps to: public.deals
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='deals'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='deals'
    )
    product = models.ForeignKey(
        Product,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deals'
    )
    policy_number = models.TextField(null=True, blank=True)
    application_number = models.TextField(null=True, blank=True)
    status = models.TextField(default='draft', null=True, blank=True)
    status_standardized = models.TextField(null=True, blank=True)
    annual_premium = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    monthly_premium = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    policy_effective_date = models.DateField(null=True, blank=True)
    submission_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    billing_cycle = models.TextField(null=True, blank=True)
    lead_source = models.TextField(null=True, blank=True)
    notes = models.TextField(null=True, blank=True)

    client_name = models.TextField(null=True, blank=True)
    client_phone = models.TextField(null=True, blank=True)
    client_email = models.TextField(null=True, blank=True)
    client_address = models.TextField(null=True, blank=True)
    client_gender = models.TextField(null=True, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)
    ssn_last_4 = models.TextField(null=True, blank=True)

    # Generated columns - read-only, computed by PostgreSQL
    client_email_lc = models.TextField(null=True, blank=True, editable=False)
    client_phone10 = models.TextField(null=True, blank=True, editable=False)
    client_name_norm = models.TextField(null=True, blank=True, editable=False)

    split_agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='split_deals',
        db_column='split_agent_id'
    )
    split_percentage = models.DecimalField(
        max_digits=5, decimal_places=2, null=True, blank=True
    )
    referral_count = models.IntegerField(default=0, null=True, blank=True)

    writing_agent_number = models.TextField(null=True, blank=True)
    is_loa = models.BooleanField(null=True, blank=True)
    policy_sync_id = models.TextField(null=True, blank=True)

    issue_age = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    face_value = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    payment_method = models.TextField(null=True, blank=True)
    payment_cycle_premium = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)

    state = models.TextField(null=True, blank=True)
    zipcode = models.TextField(null=True, blank=True)

    # Generated columns - read-only, computed by PostgreSQL
    effective_month = models.DateField(null=True, blank=True, editable=False)
    age_band = models.TextField(null=True, blank=True, editable=False)
    report_type = models.TextField(null=True, blank=True)

    last_paid_premium_date = models.DateField(null=True, blank=True)
    lapse_date = models.DateTimeField(null=True, blank=True)
    billing_day_of_month = models.TextField(null=True, blank=True)
    billing_weekday = models.TextField(null=True, blank=True)
    ssn_benefit = models.BooleanField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'deals'

    def __str__(self):
        return f"{self.policy_number or 'No policy#'} - {self.client_name or 'No client'}"


class PositionProductCommission(models.Model):
    """
    Commission rates for position/product combinations.
    Maps to: public.position_product_commissions
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    position = models.ForeignKey(
        Position,
        on_delete=models.CASCADE,
        related_name='product_commissions'
    )
    product = models.ForeignKey(
        Product,
        on_delete=models.CASCADE,
        related_name='position_commissions'
    )
    commission_percentage = models.DecimalField(
        max_digits=5, decimal_places=2,
        help_text='Commission percentage (e.g., 75.00 for 75%)'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'position_product_commissions'
        unique_together = [['position', 'product']]

    def __str__(self):
        return f"{self.position.name} - {self.product.name}: {self.commission_percentage}%"


class DealHierarchySnapshot(models.Model):
    """
    Captures the agent hierarchy at the time a deal was created.
    Used for commission calculations to preserve historical hierarchy.
    Maps to: public.deal_hierarchy_snapshot (singular, composite PK)
    """
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name='hierarchy_snapshots'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='deal_hierarchy_entries'
    )
    upline = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deal_hierarchy_downlines',
        db_column='upline_id'
    )
    commission_percentage = models.DecimalField(max_digits=5, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        managed = False
        db_table = 'deal_hierarchy_snapshot'
        unique_together = [['deal', 'agent']]

    def __str__(self):
        return f"Deal {self.deal_id} - Agent {self.agent}: {self.commission_percentage}%"


class Beneficiary(models.Model):
    """
    Beneficiary information for a deal.
    Maps to: public.beneficiaries
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='beneficiaries'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='beneficiaries'
    )
    first_name = models.TextField(null=True, blank=True)
    last_name = models.TextField(null=True, blank=True)
    relationship = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'beneficiaries'

    def __str__(self):
        name = format_full_name(self.first_name, self.last_name)
        return f"{name} ({self.relationship or 'Unknown'})" if name else f"({self.relationship or 'Unknown'})"


class StatusMapping(models.Model):
    """
    Maps carrier-specific status codes to standardized statuses.
    Maps to: public.status_mapping
    """
    IMPACT_CHOICES = [
        ('positive', 'Positive'),
        ('negative', 'Negative'),
        ('neutral', 'Neutral'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='status_mappings'
    )
    raw_status = models.TextField()
    standardized_status = models.TextField(null=True, blank=True, db_column='status_standardized')
    impact = models.TextField(choices=IMPACT_CHOICES)
    placement = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'status_mapping'
        unique_together = [['carrier', 'raw_status']]

    def __str__(self):
        return f"{self.carrier.name}: {self.raw_status} -> {self.standardized_status}"


class Conversation(models.Model):
    """
    SMS conversation thread.
    Maps to: public.conversations
    """
    SMS_OPT_IN_CHOICES = [
        ('opted_in', 'Opted In'),
        ('opted_out', 'Opted Out'),
        ('pending', 'Pending'),
    ]

    TYPE_CHOICES = [
        ('sms', 'SMS'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='conversations'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='conversations'
    )
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        related_name='conversations'
    )
    client_phone = models.TextField(null=True, blank=True)
    type = models.TextField(default='sms')
    last_message_at = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    sms_opt_in_status = models.TextField(default='opted_in', null=True, blank=True)
    opted_in_at = models.DateTimeField(null=True, blank=True)
    opted_out_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'conversations'

    def __str__(self):
        return f"Conversation {self.client_phone or self.id}"


class Message(models.Model):
    """
    Individual SMS message.
    Maps to: public.messages
    """
    DIRECTION_CHOICES = [
        ('inbound', 'Inbound'),
        ('outbound', 'Outbound'),
    ]

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('sent', 'Sent'),
        ('delivered', 'Delivered'),
        ('failed', 'Failed'),
        ('received', 'Received'),
    ]

    MESSAGE_TYPE_CHOICES = [
        ('sms', 'SMS'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name='messages'
    )
    sender = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='sent_messages',
        db_column='sender_id'
    )
    receiver = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='received_messages',
        db_column='receiver_id'
    )
    body = models.TextField()
    direction = models.TextField()
    message_type = models.TextField(default='sms')
    sent_at = models.DateTimeField(null=True, blank=True)
    status = models.TextField(default='delivered')
    metadata = models.JSONField(default=dict, blank=True)
    read_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'messages'
        ordering = ['sent_at']

    def __str__(self):
        return f"{self.direction}: {self.body[:50]}..."


class AIConversation(models.Model):
    """
    AI chat conversation session.
    Maps to: public.ai_conversations
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='ai_conversations'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='ai_conversations'
    )
    title = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'ai_conversations'

    def __str__(self):
        return f"AI Chat: {self.title or 'Untitled'} ({self.user})"


class AIMessage(models.Model):
    """
    Individual message in an AI conversation.
    Maps to: public.ai_messages
    """
    ROLE_CHOICES = [
        ('user', 'User'),
        ('assistant', 'Assistant'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    conversation = models.ForeignKey(
        AIConversation,
        on_delete=models.CASCADE,
        related_name='messages'
    )
    role = models.TextField(choices=ROLE_CHOICES)
    content = models.TextField()
    tool_calls = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # Token tracking (P1-015)
    tokens_used = models.IntegerField(null=True, blank=True)

    # Chart generation (P1-015)
    chart_code = models.TextField(null=True, blank=True)
    chart_data = models.JSONField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'ai_messages'
        ordering = ['created_at']

    def __str__(self):
        return f"{self.role}: {self.content[:50]}..."


class FeatureFlag(models.Model):
    """
    Feature flags for controlling feature rollout.
    Maps to: public.feature_flags
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(null=True, blank=True)
    is_enabled = models.BooleanField(default=False)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='feature_flags',
        help_text='If null, flag is global'
    )
    rollout_percentage = models.IntegerField(
        default=0,
        help_text='Percentage of users to enable for (0-100)'
    )
    metadata = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'feature_flags'

    def __str__(self):
        scope = f"Agency: {self.agency.name}" if self.agency else "Global"
        return f"{self.name} ({scope}) - {'Enabled' if self.is_enabled else 'Disabled'}"


# =============================================================================
# NEW MODELS - Added for Supabase DB Sync
# =============================================================================


class NIProJob(models.Model):
    """
    NIPR license verification job.
    Maps to: public.nipr_jobs
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='nipr_jobs'
    )
    last_name = models.TextField()
    npn = models.TextField()
    ssn_last4 = models.TextField()
    dob = models.TextField()
    status = models.TextField(default='pending')
    progress = models.IntegerField(default=0)
    progress_message = models.TextField(null=True, blank=True)
    result_files = ArrayField(models.TextField(), default=list, blank=True)
    result_carriers = ArrayField(models.TextField(), default=list, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    locked_until = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'nipr_jobs'

    def __str__(self):
        return f"NIPR Job {self.id} - {self.last_name} ({self.status})"


class IngestJob(models.Model):
    """
    File ingestion job for processing uploaded files.
    Maps to: public.ingest_job
    """
    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='ingest_jobs'
    )
    expected_files = models.IntegerField()
    parsed_files = models.IntegerField(default=0)
    status = models.TextField(default='parsing')
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    client_job_id = models.TextField(null=True, blank=True)
    watcher_created_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'ingest_job'

    def __str__(self):
        return f"Ingest Job {self.job_id} - {self.status}"


class IngestJobFile(models.Model):
    """
    Individual file within an ingestion job.
    Maps to: public.ingest_job_file
    """
    file_id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    job = models.ForeignKey(
        IngestJob,
        on_delete=models.CASCADE,
        to_field='job_id',
        related_name='files'
    )
    file_name = models.TextField()
    status = models.TextField(default='received')
    parsed_rows = models.IntegerField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'ingest_job_file'

    def __str__(self):
        return f"{self.file_name} ({self.status})"


class AgentCarrierNumber(models.Model):
    """
    Agent's carrier-specific identification number.
    Maps to: public.agent_carrier_numbers
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agent = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='carrier_numbers'
    )
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='agent_numbers'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='agent_carrier_numbers'
    )
    agent_number = models.TextField()
    is_active = models.BooleanField(null=True, blank=True, default=True)
    notes = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    loa = models.TextField(null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'agent_carrier_numbers'

    def __str__(self):
        return f"{self.agent} - {self.carrier}: {self.agent_number}"


class ACNLoadAudit(models.Model):
    """
    Audit log for Agent Carrier Number loading operations.
    Maps to: public.acn_load_audit
    """
    id = models.BigAutoField(primary_key=True)
    run_id = models.UUIDField(default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='acn_load_audits'
    )
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='acn_load_audits'
    )
    agent_number = models.TextField(null=True, blank=True)
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='acn_load_audits'
    )
    reason = models.TextField()
    details = models.TextField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'acn_load_audit'

    def __str__(self):
        return f"ACN Audit {self.id} - {self.reason[:50]}"


class AgentNameCollisionLog(models.Model):
    """
    Log of agent name collisions during matching.
    Maps to: public.agent_name_collision_log
    """
    id = models.BigAutoField(primary_key=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='name_collision_logs'
    )
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='name_collision_logs'
    )
    agent_number = models.TextField()
    first_name = models.TextField()
    last_name = models.TextField()
    matched_user_ids = ArrayField(models.UUIDField())
    chosen_user_id = models.UUIDField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'agent_name_collision_log'

    def __str__(self):
        return f"Collision: {self.first_name} {self.last_name} ({self.agent_number})"


class AIAuditLog(models.Model):
    """
    Audit log for AI tool usage.
    Maps to: public.ai_audit_log
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='ai_audit_logs'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='ai_audit_logs'
    )
    tool_name = models.TextField()
    input_summary = models.JSONField(null=True, blank=True)
    was_allowed = models.BooleanField(default=True)
    error_message = models.TextField(null=True, blank=True)
    scope = models.TextField(null=True, blank=True)
    execution_time_ms = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'ai_audit_log'

    def __str__(self):
        return f"AI Audit: {self.tool_name} by {self.user}"


class LapseNotificationQueue(models.Model):
    """
    Queue for lapse notification processing.
    Maps to: public.lapse_notification_queue
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        related_name='lapse_notifications'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='lapse_notifications'
    )
    status = models.TextField(default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    http_request_id = models.BigIntegerField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'lapse_notification_queue'

    def __str__(self):
        return f"Lapse Notification {self.id} - {self.status}"


class ParsingInfo(models.Model):
    """
    Carrier portal parsing credentials.
    Maps to: public.parsing_info
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    carrier = models.ForeignKey(
        Carrier,
        on_delete=models.CASCADE,
        related_name='parsing_info'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='parsing_info'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='parsing_info'
    )
    login = models.TextField()
    password = models.TextField()

    class Meta:
        managed = False
        db_table = 'parsing_info'

    def __str__(self):
        return f"Parsing Info: {self.carrier} - {self.login}"


class PolicyReportStagingSyncLog(models.Model):
    """
    Sync log for policy report staging operations.
    Maps to: public.policy_report_staging_sync_log
    """
    id = models.BigAutoField(primary_key=True)
    run_id = models.UUIDField()
    staging_id = models.UUIDField()
    reason = models.TextField()
    details = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'policy_report_staging_sync_log'

    def __str__(self):
        return f"Staging Sync {self.id} - {self.reason[:50]}"


# =============================================================================
# Manager Assignments
# =============================================================================

# Import and assign managers after all models are defined
from apps.core.managers.conversation import ConversationManager
from apps.core.managers.deal import DealManager
from apps.core.managers.user import UserManager

# Assign custom managers to models
User.objects = UserManager()  # type: ignore[attr-defined]
User.objects.model = User  # type: ignore[attr-defined]

Deal.objects = DealManager()  # type: ignore[attr-defined]
Deal.objects.model = Deal  # type: ignore[attr-defined]

Conversation.objects = ConversationManager()  # type: ignore[attr-defined]
Conversation.objects.model = Conversation  # type: ignore[attr-defined]
