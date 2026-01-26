"""
Core Serializers for AgentSpace Django Backend

DRF Serializers for all core models following best practices:
- Separate Read/Write serializers where needed
- Explicit field definitions (no fields = '__all__')
- Nested serializers with prefetch optimization
"""
from rest_framework import serializers

from .models import (
    Agency,
    AIConversation,
    AIMessage,
    Beneficiary,
    Carrier,
    Client,
    Conversation,
    Deal,
    DealHierarchySnapshot,
    DraftMessage,
    Message,
    Position,
    PositionProductCommission,
    Product,
    StatusMapping,
    User,
)
from .utils import format_full_name

# Agency Serializers

class AgencySerializer(serializers.ModelSerializer):
    """Read serializer for Agency."""

    class Meta:
        model = Agency
        fields = [
            'id',
            'name',
            'display_name',
            'logo_url',
            'primary_color',
            'whitelabel_domain',
            'sms_enabled',
            'created_at',
            'updated_at',
            # New fields (P1-010)
            'code',
            'is_active',
            'phone_number',
            'lead_sources',
            'messaging_enabled',
            'discord_webhook_url',
            'discord_notification_enabled',
            'discord_notification_template',
            'theme_mode',
            'default_scoreboard_start_date',
            'lapse_email_notifications_enabled',
            'lapse_email_subject',
            'lapse_email_body',
            # SMS templates
            'sms_welcome_enabled',
            'sms_welcome_template',
            'sms_billing_reminder_enabled',
            'sms_billing_reminder_template',
            'sms_lapse_reminder_enabled',
            'sms_lapse_reminder_template',
            'sms_birthday_enabled',
            'sms_birthday_template',
            'sms_holiday_enabled',
            'sms_holiday_template',
            'sms_quarterly_enabled',
            'sms_quarterly_template',
            'sms_policy_packet_enabled',
            'sms_policy_packet_template',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class AgencyMinimalSerializer(serializers.ModelSerializer):
    """Minimal Agency serializer for nested representations."""

    class Meta:
        model = Agency
        fields = ['id', 'name', 'display_name']


# Position Serializers

class PositionSerializer(serializers.ModelSerializer):
    """Read serializer for Position."""

    class Meta:
        model = Position
        fields = [
            'id',
            'name',
            'description',
            'level',
            'is_active',
            'agency_id',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class PositionMinimalSerializer(serializers.ModelSerializer):
    """Minimal Position serializer for nested representations."""

    class Meta:
        model = Position
        fields = ['id', 'name', 'level']


class PositionCreateSerializer(serializers.ModelSerializer):
    """Write serializer for Position creation."""

    class Meta:
        model = Position
        fields = ['name', 'description', 'level', 'is_active', 'agency']


# User/Agent Serializers

class UserMinimalSerializer(serializers.ModelSerializer):
    """Minimal User serializer for nested representations."""
    full_name = serializers.CharField(read_only=True)

    class Meta:
        model = User
        fields = ['id', 'email', 'first_name', 'last_name', 'full_name']


class UserListSerializer(serializers.ModelSerializer):
    """Lightweight User serializer for list views."""
    full_name = serializers.CharField(read_only=True)
    position_name = serializers.CharField(source='position.name', read_only=True, allow_null=True)
    position_level = serializers.IntegerField(source='position.level', read_only=True, allow_null=True)
    upline_name = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            'id',
            'email',
            'first_name',
            'last_name',
            'full_name',
            'phone_number',
            'role',
            'is_admin',
            'status',
            'position_id',
            'position_name',
            'position_level',
            'upline_id',
            'upline_name',
            'total_prod',
            'total_policies_sold',
            'created_at',
        ]

    def get_upline_name(self, obj):
        if obj.upline:
            return format_full_name(obj.upline.first_name, obj.upline.last_name)
        return None


class UserDetailSerializer(serializers.ModelSerializer):
    """Full User serializer for detail views."""
    full_name = serializers.CharField(read_only=True)
    agency = AgencyMinimalSerializer(read_only=True)
    position = PositionMinimalSerializer(read_only=True)
    upline = UserMinimalSerializer(read_only=True)
    downline_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = User
        fields = [
            'id',
            'auth_user_id',
            'email',
            'first_name',
            'last_name',
            'full_name',
            'phone_number',
            'agency',
            'role',
            'is_admin',
            'is_active',
            'status',
            'perm_level',
            'subscription_tier',
            'position',
            'upline',
            'downline_count',
            'start_date',
            'annual_goal',
            'total_prod',
            'total_policies_sold',
            'theme_mode',
            'created_at',
            'updated_at',
            # Billing cycle fields (P1-009)
            'billing_cycle_start',
            'billing_cycle_end',
            # Usage tracking (P1-009)
            'messages_sent_count',
            'ai_requests_count',
        ]


class UserCreateSerializer(serializers.ModelSerializer):
    """Write serializer for User creation."""

    class Meta:
        model = User
        fields = [
            'email',
            'first_name',
            'last_name',
            'phone_number',
            'agency',
            'role',
            'is_admin',
            'status',
            'position',
            'upline',
            'start_date',
            'annual_goal',
        ]


class UserUpdateSerializer(serializers.ModelSerializer):
    """Write serializer for User updates."""

    class Meta:
        model = User
        fields = [
            'first_name',
            'last_name',
            'phone_number',
            'role',
            'is_admin',
            'is_active',
            'status',
            'position',
            'upline',
            'start_date',
            'annual_goal',
            'theme_mode',
        ]


class AgentWithMetricsSerializer(serializers.Serializer):
    """Agent serializer with computed debt/production metrics."""
    id = serializers.UUIDField()
    name = serializers.CharField()
    email = serializers.EmailField(allow_null=True)
    first_name = serializers.CharField(allow_null=True)
    last_name = serializers.CharField(allow_null=True)
    position = serializers.CharField(allow_null=True)
    position_id = serializers.UUIDField(allow_null=True)
    position_name = serializers.CharField(allow_null=True)
    position_level = serializers.IntegerField(allow_null=True)
    upline = serializers.CharField(allow_null=True)
    status = serializers.CharField()
    badge = serializers.CharField()
    created = serializers.CharField()
    earnings = serializers.CharField()
    downlines = serializers.IntegerField()

    # Debt/production metrics
    individual_debt = serializers.DecimalField(max_digits=15, decimal_places=2)
    individual_debt_count = serializers.IntegerField()
    individual_production = serializers.DecimalField(max_digits=15, decimal_places=2)
    individual_production_count = serializers.IntegerField()
    hierarchy_debt = serializers.DecimalField(max_digits=15, decimal_places=2)
    hierarchy_debt_count = serializers.IntegerField()
    hierarchy_production = serializers.DecimalField(max_digits=15, decimal_places=2)
    hierarchy_production_count = serializers.IntegerField()
    debt_to_production_ratio = serializers.DecimalField(
        max_digits=10, decimal_places=4, allow_null=True
    )


# Carrier Serializers

class CarrierSerializer(serializers.ModelSerializer):
    """Read serializer for Carrier."""

    class Meta:
        model = Carrier
        fields = [
            'id',
            'name',
            'code',
            'is_active',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class CarrierMinimalSerializer(serializers.ModelSerializer):
    """Minimal Carrier serializer for nested representations."""

    class Meta:
        model = Carrier
        fields = ['id', 'name', 'code']


class CarrierWithProductsSerializer(serializers.ModelSerializer):
    """Carrier serializer with nested products."""
    products = serializers.SerializerMethodField()

    class Meta:
        model = Carrier
        fields = ['id', 'name', 'code', 'is_active', 'products']

    def get_products(self, obj):
        # Use prefetched products if available
        products = getattr(obj, 'prefetched_products', obj.products.filter(is_active=True))
        return ProductMinimalSerializer(products, many=True).data


# Product Serializers

class ProductSerializer(serializers.ModelSerializer):
    """Read serializer for Product."""
    carrier_name = serializers.CharField(source='carrier.name', read_only=True, allow_null=True)

    class Meta:
        model = Product
        fields = [
            'id',
            'name',
            'carrier_id',
            'carrier_name',
            'agency_id',
            'is_active',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class ProductMinimalSerializer(serializers.ModelSerializer):
    """Minimal Product serializer for nested representations."""

    class Meta:
        model = Product
        fields = ['id', 'name']


class ProductDetailSerializer(serializers.ModelSerializer):
    """Full Product serializer with carrier details."""
    carrier = CarrierMinimalSerializer(read_only=True)

    class Meta:
        model = Product
        fields = [
            'id',
            'name',
            'carrier',
            'agency_id',
            'is_active',
            'created_at',
            'updated_at',
        ]


# Client Serializers

class ClientSerializer(serializers.ModelSerializer):
    """Read serializer for Client."""
    full_name = serializers.SerializerMethodField()

    class Meta:
        model = Client
        fields = [
            'id',
            'first_name',
            'last_name',
            'full_name',
            'email',
            'phone_number',
            'agency_id',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

    def get_full_name(self, obj):
        return format_full_name(obj.first_name, obj.last_name)


class ClientMinimalSerializer(serializers.ModelSerializer):
    """Minimal Client serializer for nested representations."""
    full_name = serializers.SerializerMethodField()

    class Meta:
        model = Client
        fields = ['id', 'first_name', 'last_name', 'full_name', 'email', 'phone']

    def get_full_name(self, obj):
        return format_full_name(obj.first_name, obj.last_name)


class ClientCreateSerializer(serializers.ModelSerializer):
    """Write serializer for Client creation."""

    class Meta:
        model = Client
        fields = ['first_name', 'last_name', 'email', 'phone', 'agency']


# Deal Serializers

class DealListSerializer(serializers.ModelSerializer):
    """Lightweight Deal serializer for list views."""
    agent_name = serializers.SerializerMethodField()
    client_name = serializers.CharField(read_only=True)
    carrier_name = serializers.CharField(source='carrier.name', read_only=True, allow_null=True)
    product_name = serializers.CharField(source='product.name', read_only=True, allow_null=True)

    class Meta:
        model = Deal
        fields = [
            'id',
            'policy_number',
            'status',
            'status_standardized',
            'agent_id',
            'agent_name',
            'client_id',
            'client_name',
            'carrier_id',
            'carrier_name',
            'product_id',
            'product_name',
            'annual_premium',
            'monthly_premium',
            'policy_effective_date',
            'submission_date',
            'created_at',
        ]

    def get_agent_name(self, obj):
        if obj.agent:
            return format_full_name(obj.agent.first_name, obj.agent.last_name)
        return None


class DealDetailSerializer(serializers.ModelSerializer):
    """Full Deal serializer for detail views."""
    agent = UserMinimalSerializer(read_only=True)
    client = ClientMinimalSerializer(read_only=True)
    carrier = CarrierMinimalSerializer(read_only=True)
    product = ProductMinimalSerializer(read_only=True)
    beneficiaries = serializers.SerializerMethodField()

    class Meta:
        model = Deal
        fields = [
            'id',
            'policy_number',
            'status',
            'status_standardized',
            'agent',
            'client',
            'carrier',
            'product',
            'annual_premium',
            'monthly_premium',
            'policy_effective_date',
            'submission_date',
            'beneficiaries',
            'created_at',
            'updated_at',
        ]

    def get_beneficiaries(self, obj):
        beneficiaries = getattr(obj, 'prefetched_beneficiaries', obj.beneficiaries.all())
        return BeneficiarySerializer(beneficiaries, many=True).data


class DealCreateSerializer(serializers.ModelSerializer):
    """Write serializer for Deal creation."""

    class Meta:
        model = Deal
        fields = [
            'policy_number',
            'status',
            'agent',
            'client',
            'carrier',
            'product',
            'annual_premium',
            'monthly_premium',
            'policy_effective_date',
            'submission_date',
            'agency',
        ]


# Beneficiary Serializers

class BeneficiarySerializer(serializers.ModelSerializer):
    """Read serializer for Beneficiary."""
    full_name = serializers.SerializerMethodField()

    class Meta:
        model = Beneficiary
        fields = [
            'id',
            'first_name',
            'last_name',
            'full_name',
            'relationship',
            'percentage',
            'deal_id',
        ]

    def get_full_name(self, obj):
        return format_full_name(obj.first_name, obj.last_name)


# Position Product Commission Serializers

class PositionProductCommissionSerializer(serializers.ModelSerializer):
    """Read serializer for PositionProductCommission."""
    position_name = serializers.CharField(source='position.name', read_only=True)
    product_name = serializers.CharField(source='product.name', read_only=True)

    class Meta:
        model = PositionProductCommission
        fields = [
            'id',
            'position_id',
            'position_name',
            'product_id',
            'product_name',
            'commission_percentage',
            'created_at',
            'updated_at',
        ]


# Deal Hierarchy Snapshot Serializers

class DealHierarchySnapshotSerializer(serializers.ModelSerializer):
    """Read serializer for DealHierarchySnapshot."""
    agent_name = serializers.SerializerMethodField()
    position_name = serializers.CharField(source='position.name', read_only=True, allow_null=True)

    class Meta:
        model = DealHierarchySnapshot
        fields = [
            'id',
            'deal_id',
            'agent_id',
            'agent_name',
            'position_id',
            'position_name',
            'hierarchy_level',
            'commission_percentage',
            'created_at',
        ]

    def get_agent_name(self, obj):
        if obj.agent:
            return format_full_name(obj.agent.first_name, obj.agent.last_name)
        return None


# Status Mapping Serializers

class StatusMappingSerializer(serializers.ModelSerializer):
    """Read serializer for StatusMapping."""
    carrier_name = serializers.CharField(source='carrier.name', read_only=True)

    class Meta:
        model = StatusMapping
        fields = [
            'id',
            'carrier_id',
            'carrier_name',
            'raw_status',
            'standardized_status',
            'impact',
            'created_at',
            'updated_at',
        ]


class StatusMappingCreateSerializer(serializers.ModelSerializer):
    """Write serializer for StatusMapping creation."""

    class Meta:
        model = StatusMapping
        fields = ['carrier', 'raw_status', 'standardized_status', 'impact']


# SMS/Conversation Serializers

class ConversationSerializer(serializers.ModelSerializer):
    """Read serializer for Conversation."""
    agent_name = serializers.SerializerMethodField()
    client_name = serializers.SerializerMethodField()
    last_message_preview = serializers.CharField(read_only=True, allow_null=True)

    class Meta:
        model = Conversation
        fields = [
            'id',
            'agency_id',
            'agent_id',
            'agent_name',
            'client_id',
            'client_name',
            'deal_id',
            'phone_number',
            'unread_count',
            'is_archived',
            'last_message_at',
            'last_message_preview',
            'created_at',
            'updated_at',
            # SMS opt-in tracking (P1-014)
            'sms_opt_in_status',
            'opted_in_at',
            'opted_out_at',
        ]

    def get_agent_name(self, obj):
        if obj.agent:
            return format_full_name(obj.agent.first_name, obj.agent.last_name)
        return None

    def get_client_name(self, obj):
        if obj.client:
            return format_full_name(obj.client.first_name, obj.client.last_name)
        return None


class MessageSerializer(serializers.ModelSerializer):
    """Read serializer for Message."""
    sent_by_name = serializers.SerializerMethodField()

    class Meta:
        model = Message
        fields = [
            'id',
            'conversation_id',
            'direction',
            'content',
            'status',
            'external_id',
            'sent_by_id',
            'sent_by_name',
            'is_read',
            'sent_at',  # Delivery tracking (P1-014)
            'created_at',
            'updated_at',
        ]

    def get_sent_by_name(self, obj):
        if obj.sent_by:
            return format_full_name(obj.sent_by.first_name, obj.sent_by.last_name)
        return None


class MessageCreateSerializer(serializers.ModelSerializer):
    """Write serializer for Message creation."""

    class Meta:
        model = Message
        fields = ['conversation', 'direction', 'content']


class DraftMessageSerializer(serializers.ModelSerializer):
    """Read serializer for DraftMessage."""
    agent_name = serializers.SerializerMethodField()
    approved_by_name = serializers.SerializerMethodField()

    class Meta:
        model = DraftMessage
        fields = [
            'id',
            'agency_id',
            'conversation_id',
            'agent_id',
            'agent_name',
            'content',
            'status',
            'approved_by_id',
            'approved_by_name',
            'approved_at',
            'rejection_reason',
            'created_at',
            'updated_at',
        ]

    def get_agent_name(self, obj):
        if obj.agent:
            return format_full_name(obj.agent.first_name, obj.agent.last_name)
        return None

    def get_approved_by_name(self, obj):
        if obj.approved_by:
            return format_full_name(obj.approved_by.first_name, obj.approved_by.last_name)
        return None


# Pagination Serializers

class PaginationSerializer(serializers.Serializer):
    """Standard pagination response structure."""
    currentPage = serializers.IntegerField()
    totalPages = serializers.IntegerField()
    totalCount = serializers.IntegerField()
    limit = serializers.IntegerField()
    hasNextPage = serializers.BooleanField()
    hasPrevPage = serializers.BooleanField()


# Dashboard/Analytics Serializers

class DashboardSummarySerializer(serializers.Serializer):
    """Dashboard summary data."""
    active_policies = serializers.IntegerField()
    monthly_commissions = serializers.DecimalField(max_digits=15, decimal_places=2)
    new_policies = serializers.IntegerField()
    total_clients = serializers.IntegerField()


class CarrierActivePolicySerializer(serializers.Serializer):
    """Carrier breakdown in dashboard."""
    carrier_id = serializers.UUIDField()
    carrier = serializers.CharField()
    active_policies = serializers.IntegerField()


class DashboardDataSerializer(serializers.Serializer):
    """Complete dashboard data response."""
    your_deals = DashboardSummarySerializer()
    downline_production = DashboardSummarySerializer()


class LeaderboardEntrySerializer(serializers.Serializer):
    """Leaderboard entry."""
    rank = serializers.IntegerField()
    agent_id = serializers.UUIDField()
    agent_name = serializers.CharField()
    position = serializers.CharField(allow_null=True)
    production = serializers.DecimalField(max_digits=15, decimal_places=2)
    deals_count = serializers.IntegerField()


class ScoreboardSerializer(serializers.Serializer):
    """Scoreboard response."""
    entries = LeaderboardEntrySerializer(many=True)
    user_rank = serializers.IntegerField(allow_null=True)
    user_production = serializers.DecimalField(max_digits=15, decimal_places=2, allow_null=True)


# Filter Options Serializers

class FilterOptionSerializer(serializers.Serializer):
    """Generic filter option."""
    id = serializers.CharField()
    name = serializers.CharField()


class DealFilterOptionsSerializer(serializers.Serializer):
    """Deal filter options response."""
    carriers = FilterOptionSerializer(many=True)
    products = FilterOptionSerializer(many=True)
    agents = FilterOptionSerializer(many=True)
    statuses = FilterOptionSerializer(many=True)


# Expected Payouts Serializers

class ExpectedPayoutSerializer(serializers.Serializer):
    """Expected payout entry."""
    deal_id = serializers.UUIDField()
    policy_number = serializers.CharField(allow_null=True)
    client_name = serializers.CharField()
    carrier_name = serializers.CharField()
    product_name = serializers.CharField(allow_null=True)
    annual_premium = serializers.DecimalField(max_digits=15, decimal_places=2, allow_null=True)
    commission_percentage = serializers.DecimalField(max_digits=5, decimal_places=2, allow_null=True)
    expected_commission = serializers.DecimalField(max_digits=15, decimal_places=2, allow_null=True)
    policy_effective_date = serializers.DateField(allow_null=True)
    status = serializers.CharField(allow_null=True)


class ExpectedPayoutsResponseSerializer(serializers.Serializer):
    """Expected payouts response."""
    payouts = ExpectedPayoutSerializer(many=True)
    total_expected = serializers.DecimalField(max_digits=15, decimal_places=2)
    pagination = PaginationSerializer()


# AI Conversation/Message Serializers

class AIConversationListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for list views."""
    message_count = serializers.SerializerMethodField()

    class Meta:
        model = AIConversation
        fields = ['id', 'title', 'user_id', 'is_active',
                  'message_count', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']

    def get_message_count(self, obj):
        return getattr(obj, 'message_count', obj.messages.count())


class AIMessageSerializer(serializers.ModelSerializer):
    """Read serializer for AI messages."""

    class Meta:
        model = AIMessage
        fields = [
            'id',
            'conversation_id',
            'role',
            'content',
            'tool_calls',
            'tool_results',
            'created_at',
            # Token tracking (P1-015)
            'input_tokens',
            'output_tokens',
            'tokens_used',
            # Chart generation (P1-015)
            'chart_code',
            'chart_data',
        ]
        read_only_fields = ['id', 'created_at']


class AIConversationDetailSerializer(serializers.ModelSerializer):
    """Full serializer with messages."""
    messages = AIMessageSerializer(many=True, read_only=True)

    class Meta:
        model = AIConversation
        fields = ['id', 'title', 'user_id', 'agency_id', 'is_active',
                  'messages', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']


class AIConversationCreateSerializer(serializers.ModelSerializer):
    """Write serializer for creating conversations."""

    class Meta:
        model = AIConversation
        fields = ['title']


class AIMessageCreateSerializer(serializers.ModelSerializer):
    """Write serializer for creating messages."""

    class Meta:
        model = AIMessage
        fields = ['conversation', 'role', 'content', 'tool_calls', 'tool_results']
