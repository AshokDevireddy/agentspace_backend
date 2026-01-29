from django.urls import path

from .views import (
    AcquireJobView,
    CheckNIPRCompletedView,
    CompleteJobView,
    CreateJobView,
    HasPendingJobsView,
    JobStatusView,
    ReleaseLocksView,
    UpdateProgressView,
)

urlpatterns = [
    path('jobs', CreateJobView.as_view(), name='nipr_create_job'),
    path('check-completed', CheckNIPRCompletedView.as_view(), name='nipr_check_completed'),
    path('acquire-job', AcquireJobView.as_view(), name='nipr_acquire_job'),
    path('complete-job', CompleteJobView.as_view(), name='nipr_complete_job'),
    path('job-progress', UpdateProgressView.as_view(), name='nipr_update_progress'),
    path('release-locks', ReleaseLocksView.as_view(), name='nipr_release_locks'),
    path('has-pending', HasPendingJobsView.as_view(), name='nipr_has_pending'),
    path('job/<str:job_id>', JobStatusView.as_view(), name='nipr_job_status'),
]
