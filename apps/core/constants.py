"""
Core Constants

Centralized configuration values for the application.
"""

# Rate limits for API endpoints
RATE_LIMITS = {
    "sms_send": "60/m",
    "sms_bulk": "10/h",
    "ai_chat": "30/m",
}

# Pagination defaults
PAGINATION = {
    "default_limit": 20,
    "max_limit": 100,
}

# Export settings
EXPORT = {
    "pdf_cell_max_length": 50,
    "csv_max_rows": 10000,
    "excel_max_rows": 50000,
}

# Standardized statuses for deals
STANDARDIZED_STATUSES = [
    {
        "value": "active",
        "label": "Active",
        "impact": "positive",
        "description": "Policy is active and in force",
    },
    {
        "value": "pending",
        "label": "Pending",
        "impact": "neutral",
        "description": "Policy is pending approval or processing",
    },
    {
        "value": "cancelled",
        "label": "Cancelled",
        "impact": "negative",
        "description": "Policy was cancelled by request",
    },
    {
        "value": "lapsed",
        "label": "Lapsed",
        "impact": "negative",
        "description": "Policy lapsed due to non-payment",
    },
    {
        "value": "terminated",
        "label": "Terminated",
        "impact": "negative",
        "description": "Policy was terminated",
    },
]

# SMS opt-in statuses
OPT_STATUSES = ["opted_in", "opted_out", "pending"]

# Report types
REPORT_TYPES = ["production", "pipeline", "team_performance", "revenue", "commission"]

# Report frequencies
REPORT_FREQUENCIES = ["daily", "weekly", "monthly", "quarterly"]

# Export formats
EXPORT_FORMATS = ["csv", "xlsx", "pdf"]

# Widget types
WIDGET_TYPES = [
    "stats_card",
    "chart",
    "table",
    "leaderboard",
    "pipeline",
    "activity",
]

# Billing cycles
BILLING_CYCLES = ["monthly", "quarterly", "semi-annually", "annually"]

# Suggestion types for AI
AI_SUGGESTION_TYPES = ["deals", "clients", "performance", "general"]

# Analytics insight types
AI_INSIGHT_TYPES = ["performance", "revenue", "team", "general"]
