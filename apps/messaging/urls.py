from django.urls import path

from .views import (
    BillingRemindersView,
    BirthdayMessagesView,
    HolidayMessagesView,
    LapseRemindersView,
    NeedsInfoView,
    PolicyCheckupsView,
    QuarterlyCheckinsView,
)

urlpatterns = [
    path('billing-reminders', BillingRemindersView.as_view(), name='messaging_billing_reminders'),
    path('birthdays', BirthdayMessagesView.as_view(), name='messaging_birthdays'),
    path('holidays', HolidayMessagesView.as_view(), name='messaging_holidays'),
    path('lapse-reminders', LapseRemindersView.as_view(), name='messaging_lapse_reminders'),
    path('needs-info', NeedsInfoView.as_view(), name='messaging_needs_info'),
    path('policy-checkups', PolicyCheckupsView.as_view(), name='messaging_policy_checkups'),
    path('quarterly-checkins', QuarterlyCheckinsView.as_view(), name='messaging_quarterly_checkins'),
]
