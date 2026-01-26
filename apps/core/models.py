"""
Core Models for AgentSpace Django Backend

These are UNMANAGED models that map to existing Supabase PostgreSQL tables.
They do NOT create migrations - Django reads from existing tables.
"""
import uuid
from typing import TYPE_CHECKING

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
    name = models.CharField(max_length=255)
    display_name = models.CharField(max_length=255, null=True, blank=True)
    logo_url = models.TextField(null=True, blank=True)
    primary_color = models.CharField(max_length=50, null=True, blank=True)
    whitelabel_domain = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Core fields (P1-010)
    code = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    phone_number = models.TextField(null=True, blank=True)
    lead_sources = models.JSONField(default=list, blank=True)
    messaging_enabled = models.BooleanField(default=False)

    # Discord integration
    discord_webhook_url = models.TextField(null=True, blank=True)
    discord_notification_enabled = models.BooleanField(default=False)
    discord_notification_template = models.TextField(null=True, blank=True)
    discord_bot_username = models.TextField(null=True, blank=True)

    # Deactivation tracking
    deactivated_post_a_deal = models.BooleanField(null=True, blank=True)

    # Display settings
    theme_mode = models.TextField(null=True, blank=True)
    default_scoreboard_start_date = models.DateField(null=True, blank=True)

    # Lapse email notifications
    lapse_email_notifications_enabled = models.BooleanField(default=False)
    lapse_email_subject = models.TextField(null=True, blank=True)
    lapse_email_body = models.TextField(null=True, blank=True)

    # SMS templates (P1-010) - DB defaults all enabled to true
    sms_welcome_enabled = models.BooleanField(default=True)
    sms_welcome_template = models.TextField(null=True, blank=True)
    sms_billing_reminder_enabled = models.BooleanField(default=True)
    sms_billing_reminder_template = models.TextField(null=True, blank=True)
    sms_lapse_reminder_enabled = models.BooleanField(default=True)
    sms_lapse_reminder_template = models.TextField(null=True, blank=True)
    sms_birthday_enabled = models.BooleanField(default=True)
    sms_birthday_template = models.TextField(null=True, blank=True)
    sms_holiday_enabled = models.BooleanField(default=True)
    sms_holiday_template = models.TextField(null=True, blank=True)
    sms_quarterly_enabled = models.BooleanField(default=True)
    sms_quarterly_template = models.TextField(null=True, blank=True)
    sms_policy_packet_enabled = models.BooleanField(default=True)
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
    email = models.CharField(max_length=255, null=True, blank=True)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=50, null=True, blank=True)
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

    # Billing cycle fields (P1-009)
    billing_cycle_start = models.DateTimeField(null=True, blank=True)
    billing_cycle_end = models.DateTimeField(null=True, blank=True)

    # Usage tracking (P1-009)
    messages_sent_count = models.IntegerField(default=0)
    ai_requests_count = models.IntegerField(default=0)

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
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='positions'
    )
    level = models.IntegerField(default=0)
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
    name = models.CharField(max_length=255)
    display_name = models.CharField(max_length=255, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

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
    name = models.CharField(max_length=255)
    product_code = models.TextField(null=True, blank=True)
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
        name = format_full_name(self.first_name, self.last_name)
        email_part = self.email or 'No email'
        return f"{name} ({email_part})".strip() if name else f"({email_part})"


class Deal(models.Model):
    """
    Represents an insurance policy/deal.
    Maps to: public.deals
    """
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

    # Additional deal fields (P2-027)
    billing_cycle = models.CharField(
        max_length=50, null=True, blank=True,
        help_text='Billing frequency: monthly, quarterly, semi-annually, annually'
    )
    lead_source = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'deals'

    def __str__(self):
        client_name = format_full_name(self.client.first_name, self.client.last_name) if self.client else 'No client'
        return f"{self.policy_number or 'No policy#'} - {client_name}"

    @property
    def client_name(self):
        if self.client:
            return format_full_name(self.client.first_name, self.client.last_name)
        return ''


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
    # Note: DB uses composite primary key (deal_id, agent_id), not UUID
    # Django doesn't support composite PKs natively, so we use deal as PK for ORM
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name='hierarchy_snapshots'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
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
    commission_percentage = models.DecimalField(
        max_digits=10, decimal_places=2
    )
    created_at = models.DateTimeField(auto_now_add=True)

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
        related_name='beneficiaries'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='beneficiaries'
    )
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    relationship = models.CharField(max_length=100, null=True, blank=True)
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
    raw_status = models.CharField(
        max_length=255,
        help_text='The carrier-specific status string'
    )
    standardized_status = models.CharField(
        max_length=50, null=True, blank=True,
        db_column='status_standardized',
        help_text='The normalized status (active, pending, cancelled, lapsed, terminated)'
    )
    impact = models.CharField(
        max_length=20,
        choices=IMPACT_CHOICES,
        default='neutral',
        help_text='Impact on persistency calculations'
    )
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
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='conversations'
    )
    deal = models.ForeignKey(
        Deal,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='conversations'
    )
    # DB column is 'client_phone', not 'phone_number'
    client_phone = models.TextField(null=True, blank=True)
    type = models.CharField(
        max_length=20,
        choices=TYPE_CHOICES,
        default='sms'
    )
    last_message_at = models.DateTimeField(null=True, blank=True)
    # DB uses is_active (default true), not is_archived
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # SMS opt-in tracking - DB defaults to 'opted_in'
    sms_opt_in_status = models.CharField(
        max_length=20,
        choices=SMS_OPT_IN_CHOICES,
        default='opted_in',
        null=True,
        blank=True
    )
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
        on_delete=models.SET_NULL,
        null=True,
        related_name='sent_messages',
        db_column='sender_id'
    )
    receiver = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='received_messages',
        db_column='receiver_id'
    )
    body = models.TextField()
    direction = models.CharField(
        max_length=20,
        choices=DIRECTION_CHOICES
    )
    message_type = models.CharField(
        max_length=20,
        choices=MESSAGE_TYPE_CHOICES,
        default='sms'
    )
    sent_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='delivered'
    )
    metadata = models.JSONField(default=dict, blank=True)
    read_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'messages'
        ordering = ['sent_at']

    def __str__(self):
        return f"{self.direction}: {self.body[:50]}..."


class DraftMessage(models.Model):
    """
    SMS draft messages pending approval.
    Maps to: public.draft_messages
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('approved', 'Approved'),
        ('rejected', 'Rejected'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='draft_messages'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='draft_messages'
    )
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='draft_messages'
    )
    content = models.TextField()
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending'
    )
    approved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='approved_drafts'
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    rejection_reason = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'draft_messages'

    def __str__(self):
        return f"Draft by {self.agent}: {self.content[:50]}..."


class SmsTemplate(models.Model):
    """
    SMS message templates for automated messaging.
    Maps to: public.sms_templates
    """
    TEMPLATE_TYPE_CHOICES = [
        ('welcome', 'Welcome'),
        ('billing_reminder', 'Billing Reminder'),
        ('lapse_reminder', 'Lapse Reminder'),
        ('birthday', 'Birthday'),
        ('holiday', 'Holiday'),
        ('quarterly', 'Quarterly Check-in'),
        ('policy_packet', 'Policy Packet'),
        ('custom', 'Custom'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='sms_templates'
    )
    name = models.CharField(max_length=255)
    template_type = models.CharField(
        max_length=50,
        choices=TEMPLATE_TYPE_CHOICES,
        default='custom'
    )
    content = models.TextField(
        help_text='Template content with placeholders: {{client_name}}, {{agent_name}}, {{policy_number}}'
    )
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(
        'User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='created_sms_templates'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'sms_templates'
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.template_type})"


class DashboardWidget(models.Model):
    """
    Configurable dashboard widgets for users.
    Maps to: public.dashboard_widgets
    """
    WIDGET_TYPE_CHOICES = [
        ('stats_card', 'Stats Card'),
        ('chart', 'Chart'),
        ('table', 'Table'),
        ('leaderboard', 'Leaderboard'),
        ('calendar', 'Calendar'),
        ('activity_feed', 'Activity Feed'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user = models.ForeignKey(
        'User',
        on_delete=models.CASCADE,
        related_name='dashboard_widgets'
    )
    widget_type = models.CharField(
        max_length=50,
        choices=WIDGET_TYPE_CHOICES
    )
    title = models.CharField(max_length=255)
    position = models.IntegerField(default=0)
    config = models.JSONField(
        default=dict,
        help_text='Widget-specific configuration'
    )
    is_visible = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'dashboard_widgets'
        ordering = ['position']

    def __str__(self):
        return f"{self.title} ({self.widget_type})"


class Report(models.Model):
    """
    Generated reports.
    Maps to: public.reports
    """
    REPORT_TYPE_CHOICES = [
        ('production', 'Production Report'),
        ('pipeline', 'Pipeline Report'),
        ('team_performance', 'Team Performance'),
        ('revenue', 'Revenue Report'),
        ('commission', 'Commission Report'),
    ]

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('generating', 'Generating'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    FORMAT_CHOICES = [
        ('csv', 'CSV'),
        ('xlsx', 'Excel'),
        ('pdf', 'PDF'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='reports'
    )
    user = models.ForeignKey(
        'User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='reports'
    )
    report_type = models.CharField(
        max_length=50,
        choices=REPORT_TYPE_CHOICES
    )
    title = models.CharField(max_length=255)
    parameters = models.JSONField(
        default=dict,
        help_text='Report parameters (date range, filters, etc.)'
    )
    format = models.CharField(
        max_length=10,
        choices=FORMAT_CHOICES,
        default='csv'
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending'
    )
    file_url = models.TextField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'reports'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.title} ({self.report_type}) - {self.status}"


class ScheduledReport(models.Model):
    """
    Scheduled report configurations.
    Maps to: public.scheduled_reports
    """
    FREQUENCY_CHOICES = [
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('quarterly', 'Quarterly'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='scheduled_reports'
    )
    user = models.ForeignKey(
        'User',
        on_delete=models.CASCADE,
        related_name='scheduled_reports'
    )
    report_type = models.CharField(
        max_length=50,
        choices=Report.REPORT_TYPE_CHOICES
    )
    title = models.CharField(max_length=255)
    parameters = models.JSONField(
        default=dict,
        help_text='Report parameters template'
    )
    format = models.CharField(
        max_length=10,
        choices=Report.FORMAT_CHOICES,
        default='csv'
    )
    frequency = models.CharField(
        max_length=20,
        choices=FREQUENCY_CHOICES
    )
    email_recipients = models.JSONField(
        default=list,
        help_text='List of email addresses to send report to'
    )
    is_active = models.BooleanField(default=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'scheduled_reports'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.title} ({self.frequency})"


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
    title = models.CharField(max_length=255, null=True, blank=True)
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
        ('system', 'System'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    conversation = models.ForeignKey(
        AIConversation,
        on_delete=models.CASCADE,
        related_name='messages'
    )
    role = models.CharField(
        max_length=20,
        choices=ROLE_CHOICES
    )
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
    last_name = models.CharField(max_length=255)
    npn = models.CharField(max_length=50)
    ssn_last4 = models.CharField(max_length=4)
    dob = models.CharField(max_length=50)
    status = models.CharField(max_length=50, default='pending')
    progress = models.IntegerField(default=0)
    progress_message = models.TextField(null=True, blank=True)
    result_files = models.JSONField(default=list, null=True, blank=True)
    result_carriers = models.JSONField(default=list, null=True, blank=True)
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
    status = models.CharField(max_length=50, default='pending')
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    client_job_id = models.CharField(max_length=255, null=True, blank=True)
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
    file_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50, default='received')
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
    agent_number = models.CharField(max_length=255)
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
    run_id = models.UUIDField()
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
    agent_number = models.CharField(max_length=255, null=True, blank=True)
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
    agent_number = models.CharField(max_length=255)
    first_name = models.CharField(max_length=255)
    last_name = models.CharField(max_length=255)
    matched_user_ids = models.JSONField()
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
    status = models.CharField(max_length=50, default='pending')
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

    WARNING: Contains sensitive credential data.
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
        null=True,
        blank=True,
        related_name='parsing_info'
    )
    agency = models.ForeignKey(
        Agency,
        on_delete=models.CASCADE,
        related_name='parsing_info'
    )
    login = models.CharField(max_length=255)
    password = models.CharField(max_length=255)  # SENSITIVE - never expose in API

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
