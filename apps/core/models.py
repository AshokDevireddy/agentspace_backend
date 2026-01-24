"""
Core Models for AgentSpace Django Backend

These are UNMANAGED models that map to existing Supabase PostgreSQL tables.
They do NOT create migrations - Django reads from existing tables.
"""
import uuid
from typing import TYPE_CHECKING

from django.db import connection, models

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
    sms_enabled = models.BooleanField(default=False)
    sms_template_id = models.CharField(max_length=255, null=True, blank=True)
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

    # Display settings
    theme_mode = models.TextField(null=True, blank=True)
    default_scoreboard_start_date = models.DateField(null=True, blank=True)

    # Lapse email notifications
    lapse_email_notifications_enabled = models.BooleanField(default=False)
    lapse_email_subject = models.TextField(null=True, blank=True)
    lapse_email_body = models.TextField(null=True, blank=True)

    # SMS templates (P1-010)
    sms_welcome_enabled = models.BooleanField(default=False)
    sms_welcome_template = models.TextField(null=True, blank=True)
    sms_billing_reminder_enabled = models.BooleanField(default=False)
    sms_billing_reminder_template = models.TextField(null=True, blank=True)
    sms_lapse_reminder_enabled = models.BooleanField(default=False)
    sms_lapse_reminder_template = models.TextField(null=True, blank=True)
    sms_birthday_enabled = models.BooleanField(default=False)
    sms_birthday_template = models.TextField(null=True, blank=True)
    sms_holiday_enabled = models.BooleanField(default=False)
    sms_holiday_template = models.TextField(null=True, blank=True)
    sms_quarterly_enabled = models.BooleanField(default=False)
    sms_quarterly_template = models.TextField(null=True, blank=True)
    sms_policy_packet_enabled = models.BooleanField(default=False)
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
        return f"{self.first_name or ''} {self.last_name or ''} ({self.email or 'No email'})".strip()

    @property
    def full_name(self):
        return f"{self.first_name or ''} {self.last_name or ''}".strip() or self.email

    # =========================================================================
    # Hierarchy Methods (P1-009)
    # =========================================================================

    def get_downline(self, max_depth: int | None = None) -> list['UUID']:
        """
        Get all agents in this user's downline (recursive).

        Uses a recursive CTE for efficient hierarchy traversal.

        Args:
            max_depth: Maximum depth to traverse (None for unlimited)

        Returns:
            List of user IDs in the downline (excludes self)
        """
        depth_clause = f"AND depth < {max_depth}" if max_depth else ""

        with connection.cursor() as cursor:
            cursor.execute(f"""
                WITH RECURSIVE downline AS (
                    SELECT id, 1 as depth
                    FROM public.users
                    WHERE upline_id = %s AND agency_id = %s

                    UNION ALL

                    SELECT u.id, d.depth + 1
                    FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s {depth_clause}
                )
                SELECT id FROM downline
            """, [str(self.id), str(self.agency_id), str(self.agency_id)])
            return [row[0] for row in cursor.fetchall()]

    def get_upline_chain(self) -> list['UUID']:
        """
        Get the chain of uplines from this user to the root.

        Returns:
            List of user IDs from direct upline to root (ordered)
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE upline_chain AS (
                    SELECT id, upline_id, 1 as depth
                    FROM public.users
                    WHERE id = %s

                    UNION ALL

                    SELECT u.id, u.upline_id, uc.depth + 1
                    FROM public.users u
                    JOIN upline_chain uc ON u.id = uc.upline_id
                    WHERE u.id IS NOT NULL
                )
                SELECT id FROM upline_chain
                WHERE depth > 1
                ORDER BY depth
            """, [str(self.id)])
            return [row[0] for row in cursor.fetchall()]

    def is_in_downline(self, target_user_id: 'UUID') -> bool:
        """
        Check if a user is in this user's downline.

        Args:
            target_user_id: The user ID to check

        Returns:
            True if target is in downline
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE downline AS (
                    SELECT id FROM public.users WHERE id = %s

                    UNION ALL

                    SELECT u.id FROM public.users u
                    JOIN downline d ON u.upline_id = d.id
                    WHERE u.agency_id = %s
                )
                SELECT 1 FROM downline WHERE id = %s LIMIT 1
            """, [str(self.id), str(self.agency_id), str(target_user_id)])
            return cursor.fetchone() is not None

    @property
    def direct_downlines(self) -> models.QuerySet:
        """Get direct downlines (one level only)."""
        return User.objects.filter(upline_id=self.id)

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
        client_name = f"{self.client.first_name or ''} {self.client.last_name or ''}".strip() if self.client else 'No client'
        return f"{self.policy_number or 'No policy#'} - {client_name}"

    @property
    def client_name(self):
        if self.client:
            return f"{self.client.first_name or ''} {self.client.last_name or ''}".strip()
        return ''


# =============================================================================
# Position Product Commissions (P1-011)
# =============================================================================

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


# =============================================================================
# Deal Hierarchy Snapshot (P1-012)
# =============================================================================

class DealHierarchySnapshot(models.Model):
    """
    Captures the agent hierarchy at the time a deal was created.
    Used for commission calculations to preserve historical hierarchy.
    Maps to: public.deal_hierarchy_snapshots
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    deal = models.ForeignKey(
        Deal,
        on_delete=models.CASCADE,
        related_name='hierarchy_snapshots'
    )
    agent = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='deal_hierarchy_entries'
    )
    position = models.ForeignKey(
        Position,
        on_delete=models.SET_NULL,
        null=True,
        related_name='deal_hierarchy_entries'
    )
    hierarchy_level = models.IntegerField(
        help_text='Level in hierarchy (0 = writing agent, 1 = direct upline, etc.)'
    )
    commission_percentage = models.DecimalField(
        max_digits=5, decimal_places=2, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'deal_hierarchy_snapshots'
        ordering = ['deal', 'hierarchy_level']

    def __str__(self):
        return f"Deal {self.deal_id} - Level {self.hierarchy_level}: {self.agent}"


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
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    relationship = models.CharField(max_length=100, null=True, blank=True)
    percentage = models.DecimalField(
        max_digits=5, decimal_places=2, null=True, blank=True,
        help_text='Percentage of benefit (e.g., 100.00 for 100%)'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'beneficiaries'

    def __str__(self):
        return f"{self.first_name} {self.last_name} ({self.relationship})"


# =============================================================================
# Status Mapping (P1-013)
# =============================================================================

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
        help_text='The normalized status (active, pending, cancelled, lapsed, terminated)'
    )
    impact = models.CharField(
        max_length=20,
        choices=IMPACT_CHOICES,
        default='neutral',
        help_text='Impact on persistency calculations'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'status_mapping'
        unique_together = [['carrier', 'raw_status']]

    def __str__(self):
        return f"{self.carrier.name}: {self.raw_status} -> {self.standardized_status}"


# =============================================================================
# SMS Models (P1-014)
# =============================================================================

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
    client = models.ForeignKey(
        Client,
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
    phone_number = models.CharField(max_length=50)
    last_message_at = models.DateTimeField(null=True, blank=True)
    unread_count = models.IntegerField(default=0)
    is_archived = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # SMS opt-in tracking (P1-014)
    sms_opt_in_status = models.CharField(
        max_length=20,
        choices=SMS_OPT_IN_CHOICES,
        default='pending',
        null=True,
        blank=True
    )
    opted_in_at = models.DateTimeField(null=True, blank=True)
    opted_out_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'conversations'

    def __str__(self):
        client_name = f"{self.client.first_name} {self.client.last_name}".strip() if self.client else self.phone_number
        return f"Conversation with {client_name}"


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

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name='messages'
    )
    content = models.TextField()
    direction = models.CharField(
        max_length=20,
        choices=DIRECTION_CHOICES
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending'
    )
    external_id = models.CharField(
        max_length=255, null=True, blank=True,
        help_text='External message ID from Telnyx/Twilio'
    )
    sent_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='sent_messages'
    )
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Delivery tracking (P1-014)
    sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'messages'
        ordering = ['created_at']

    def __str__(self):
        return f"{self.direction}: {self.content[:50]}..."


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


# =============================================================================
# AI Chat Models (P1-015)
# =============================================================================

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
    is_active = models.BooleanField(default=True)
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
    tool_results = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # Token tracking (P1-015)
    input_tokens = models.IntegerField(null=True, blank=True)
    output_tokens = models.IntegerField(null=True, blank=True)
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


# =============================================================================
# Feature Flags Model (P0-006)
# =============================================================================

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
