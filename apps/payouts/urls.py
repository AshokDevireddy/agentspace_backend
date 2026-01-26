"""
Payouts URL Configuration
"""
from django.urls import path

from .views import AgentDebtView, ExpectedPayoutsView

urlpatterns = [
    path('', ExpectedPayoutsView.as_view(), name='expected-payouts'),
    path('debt', AgentDebtView.as_view(), name='agent-debt'),
]
