"""
Onboarding Models

UNMANAGED models that map to existing PostgreSQL tables.
Used for tracking onboarding progress and NIPR verification state.
"""
import uuid
from django.db import models


class OnboardingProgress(models.Model):
    """
    Tracks user's onboarding progress and state.
    Maps to: public.onboarding_progress

    Replaces localStorage-based state management with server-side persistence.
    """
    STEP_CHOICES = [
        ('nipr_verification', 'NIPR Verification'),
        ('team_invitation', 'Team Invitation'),
        ('completed', 'Completed'),
    ]

    NIPR_STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('skipped', 'Skipped'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user_id = models.UUIDField(unique=True)  # FK to users.id
    current_step = models.CharField(
        max_length=50,
        choices=STEP_CHOICES,
        default='nipr_verification'
    )

    # NIPR verification state
    nipr_status = models.CharField(
        max_length=20,
        choices=NIPR_STATUS_CHOICES,
        default='pending'
    )
    nipr_job_id = models.UUIDField(null=True, blank=True)
    nipr_carriers = models.JSONField(default=list)
    nipr_licensed_states = models.JSONField(default=dict)  # {resident: [], nonResident: []}

    # Team invitations (draft before commit)
    pending_invitations = models.JSONField(default=list)  # List of invitation drafts

    # Timestamps
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'onboarding_progress'
        verbose_name = 'Onboarding Progress'
        verbose_name_plural = 'Onboarding Progress'

    def __str__(self):
        return f"Onboarding for {self.user_id}: {self.current_step}"

    @property
    def is_completed(self) -> bool:
        """Check if onboarding is complete."""
        return self.current_step == 'completed' and self.completed_at is not None

    @property
    def nipr_is_done(self) -> bool:
        """Check if NIPR step is complete (either verified or skipped)."""
        return self.nipr_status in ('completed', 'skipped')
