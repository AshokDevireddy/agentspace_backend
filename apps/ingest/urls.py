"""
Ingest URL Configuration
"""
from django.urls import path

from .views import (
    EnqueueJobView,
    OrchestrateIngestView,
    SyncStagingView,
    StagingSummaryView,
    CreateClientsFromDealsView,
    CreateClientsFromStagingView,
    CreateUsersFromStagingView,
    CreateProductsFromStagingView,
    CreateWritingAgentNumbersView,
    DedupeStagingView,
    UpsertProductsView,
    FillAgentCarrierNumbersView,
    FillAgentCarrierNumbersWithAuditView,
    LinkStagedAgentNumbersView,
    SyncAgentCarrierNumbersView,
)

urlpatterns = [
    # Existing endpoints
    path('enqueue-job', EnqueueJobView.as_view(), name='ingest_enqueue_job'),
    path('orchestrate', OrchestrateIngestView.as_view(), name='ingest_orchestrate'),
    path('sync-staging', SyncStagingView.as_view(), name='ingest_sync_staging'),
    path('staging-summary', StagingSummaryView.as_view(), name='ingest_staging_summary'),

    # Phase 1: High Priority Ingest Functions
    path('create-clients-from-deals', CreateClientsFromDealsView.as_view(), name='ingest_create_clients_from_deals'),
    path('create-clients-from-staging', CreateClientsFromStagingView.as_view(), name='ingest_create_clients_from_staging'),
    path('create-users-from-staging', CreateUsersFromStagingView.as_view(), name='ingest_create_users_from_staging'),
    path('create-products-from-staging', CreateProductsFromStagingView.as_view(), name='ingest_create_products_from_staging'),
    path('create-writing-agent-numbers', CreateWritingAgentNumbersView.as_view(), name='ingest_create_writing_agent_numbers'),
    path('dedupe-staging', DedupeStagingView.as_view(), name='ingest_dedupe_staging'),
    path('upsert-products', UpsertProductsView.as_view(), name='ingest_upsert_products'),

    # Phase 3: Remaining Ingest Functions
    path('fill-agent-carrier-numbers', FillAgentCarrierNumbersView.as_view(), name='ingest_fill_agent_carrier_numbers'),
    path('fill-agent-carrier-numbers-with-audit', FillAgentCarrierNumbersWithAuditView.as_view(), name='ingest_fill_agent_carrier_numbers_audit'),
    path('link-staged-agent-numbers', LinkStagedAgentNumbersView.as_view(), name='ingest_link_staged_agent_numbers'),
    path('sync-agent-carrier-numbers', SyncAgentCarrierNumbersView.as_view(), name='ingest_sync_agent_carrier_numbers'),
]
