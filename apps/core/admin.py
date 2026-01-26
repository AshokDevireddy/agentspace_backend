"""
Django Admin Configuration for AgentSpace

Provides admin interface for viewing and managing data.
All models are read-only by default (unmanaged tables).
"""
from django.contrib import admin

from .models import Agency, Carrier, Client, Deal, Position, Product, User


@admin.register(Agency)
class AgencyAdmin(admin.ModelAdmin):
    """Admin interface for agencies."""
    list_display = ['name', 'display_name', 'whitelabel_domain', 'sms_enabled', 'created_at']
    list_filter = ['sms_enabled', 'created_at']
    search_fields = ['name', 'display_name', 'whitelabel_domain']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['name']


@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    """Admin interface for users."""
    list_display = ['email', 'full_name', 'role', 'status', 'is_admin', 'agency', 'position', 'created_at']
    list_filter = ['role', 'status', 'is_admin', 'is_active', 'agency']
    search_fields = ['email', 'first_name', 'last_name']
    readonly_fields = ['id', 'auth_user_id', 'created_at', 'updated_at']
    ordering = ['-created_at']

    fieldsets = (
        ('Identity', {
            'fields': ('id', 'auth_user_id', 'email', 'first_name', 'last_name', 'phone_number')
        }),
        ('Organization', {
            'fields': ('agency', 'role', 'is_admin', 'status', 'perm_level', 'position', 'upline')
        }),
        ('Subscription', {
            'fields': ('subscription_tier',)
        }),
        ('Performance', {
            'fields': ('annual_goal', 'total_prod', 'total_policies_sold', 'start_date')
        }),
        ('Settings', {
            'fields': ('is_active', 'theme_mode')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    """Admin interface for positions."""
    list_display = ['name', 'agency', 'level', 'created_at']
    list_filter = ['agency', 'level']
    search_fields = ['name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['agency', 'level', 'name']


@admin.register(Carrier)
class CarrierAdmin(admin.ModelAdmin):
    """Admin interface for carriers."""
    list_display = ['name', 'code', 'is_active', 'created_at']
    list_filter = ['is_active']
    search_fields = ['name', 'code']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['name']


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    """Admin interface for products."""
    list_display = ['name', 'carrier', 'agency', 'is_active', 'created_at']
    list_filter = ['is_active', 'carrier', 'agency']
    search_fields = ['name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['carrier', 'name']


@admin.register(Client)
class ClientAdmin(admin.ModelAdmin):
    """Admin interface for clients."""
    list_display = ['__str__', 'email', 'phone', 'agency', 'created_at']
    list_filter = ['agency', 'created_at']
    search_fields = ['first_name', 'last_name', 'email', 'phone']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['-created_at']


@admin.register(Deal)
class DealAdmin(admin.ModelAdmin):
    """Admin interface for deals."""
    list_display = ['policy_number', 'client_name', 'carrier', 'status_standardized', 'annual_premium', 'agent', 'submission_date']
    list_filter = ['status_standardized', 'carrier', 'agency', 'submission_date']
    search_fields = ['policy_number', 'client__first_name', 'client__last_name', 'agent__first_name', 'agent__last_name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    ordering = ['-submission_date', '-created_at']
    date_hierarchy = 'submission_date'

    fieldsets = (
        ('Policy Info', {
            'fields': ('id', 'policy_number', 'status', 'status_standardized')
        }),
        ('Parties', {
            'fields': ('agency', 'agent', 'client', 'carrier', 'product')
        }),
        ('Financials', {
            'fields': ('annual_premium', 'monthly_premium')
        }),
        ('Dates', {
            'fields': ('policy_effective_date', 'submission_date')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


# Customize admin site header
admin.site.site_header = 'AgentSpace Administration'
admin.site.site_title = 'AgentSpace Admin'
admin.site.index_title = 'Dashboard'
