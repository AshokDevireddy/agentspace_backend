from django.urls import path

from .views import (
    # Query endpoints
    BillingRemindersView,
    BirthdayMessagesView,
    HolidayMessagesView,
    LapseRemindersView,
    NeedsInfoView,
    PolicyCheckupsView,
    QuarterlyCheckinsView,
    # Run endpoints
    RunBirthdayMessagesView,
    RunBillingRemindersView,
    RunLapseRemindersView,
    RunQuarterlyCheckinsView,
    RunPolicyPacketCheckupsView,
    RunHolidayMessagesView,
    RunNeedsInfoNotificationsView,
)

urlpatterns = [
    # Query endpoints - GET eligible deals
    path('billing-reminders', BillingRemindersView.as_view(), name='messaging_billing_reminders'),
    path('birthdays', BirthdayMessagesView.as_view(), name='messaging_birthdays'),
    path('holidays', HolidayMessagesView.as_view(), name='messaging_holidays'),
    path('lapse-reminders', LapseRemindersView.as_view(), name='messaging_lapse_reminders'),
    path('needs-info', NeedsInfoView.as_view(), name='messaging_needs_info'),
    path('policy-checkups', PolicyCheckupsView.as_view(), name='messaging_policy_checkups'),
    path('quarterly-checkins', QuarterlyCheckinsView.as_view(), name='messaging_quarterly_checkins'),

    # Run endpoints - POST to execute message creation
    path('run/birthday-messages', RunBirthdayMessagesView.as_view(), name='messaging_run_birthday'),
    path('run/billing-reminders', RunBillingRemindersView.as_view(), name='messaging_run_billing'),
    path('run/lapse-reminders', RunLapseRemindersView.as_view(), name='messaging_run_lapse'),
    path('run/quarterly-checkins', RunQuarterlyCheckinsView.as_view(), name='messaging_run_quarterly'),
    path('run/policy-packet-checkups', RunPolicyPacketCheckupsView.as_view(), name='messaging_run_policy_packet'),
    path('run/holiday-messages', RunHolidayMessagesView.as_view(), name='messaging_run_holiday'),
    path('run/needs-info-notifications', RunNeedsInfoNotificationsView.as_view(), name='messaging_run_needs_info'),
]
