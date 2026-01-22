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

from .base import BaseService
from .agent_service import AgentService
from .deal_service import DealService
from .analytics_service import AnalyticsService
from .sms_service import SMSService
from .search_service import SearchService

__all__ = [
    'BaseService',
    'AgentService',
    'DealService',
    'AnalyticsService',
    'SMSService',
    'SearchService',
]
