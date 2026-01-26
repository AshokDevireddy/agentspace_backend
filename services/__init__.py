"""
Django Service Layer - Translated from Supabase RPC Functions

This module contains service classes that provide the business logic layer,
translating from Supabase PostgreSQL RPC functions to Django ORM equivalents.

Service Organization:
- BaseService: Common utilities and patterns
- AgentService: Agent management, hierarchy, options
- DealService: Book of business, payouts, debt calculations
- AnalyticsService: Dashboard data, analytics, charts
- SMSService: Conversations, messages
- SearchService: Fuzzy search across agents, clients, policies
"""

from .agent_service import AgentService
from .analytics_service import AnalyticsService
from .base import BaseService
from .deal_service import DealService
from .search_service import SearchService
from .sms_service import SMSService

__all__ = [
    'BaseService',
    'AgentService',
    'DealService',
    'AnalyticsService',
    'SMSService',
    'SearchService',
]
