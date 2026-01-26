"""
Ingest Services

Policy report processing functions translated from Supabase RPC functions:
- enqueue_policy_report_parse_job
- orchestrate_policy_report_ingest_with_agency_id
- sync_policy_report_staging_to_deals_with_agency_id
"""
import logging
from datetime import datetime
from uuid import UUID

from django.db import connection, transaction

logger = logging.getLogger(__name__)


def enqueue_policy_report_parse_job(
    bucket: str,
    path: str,
    carrier: str,
    agency_id: UUID,
    priority: int = 0,
    delay_sec: int = 0,
) -> int | None:
    """
    Enqueue a policy report parse job.
    Translated from Supabase RPC: enqueue_policy_report_parse_job

    This function enqueues a job to the pgmq queue for processing.

    Args:
        bucket: Storage bucket name
        path: File path in bucket
        carrier: Carrier name
        agency_id: Agency ID
        priority: Job priority (default 0)
        delay_sec: Delay in seconds before processing

    Returns:
        Message ID if successful, None otherwise
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT pgmq.send(
                    queue_name => 'parse_policy_reports_queue',
                    msg => jsonb_build_object(
                        'bucket', %s,
                        'path', %s,
                        'carrier', %s,
                        'agency_id', %s,
                        'priority', %s,
                        'enqueued_at', now()
                    ),
                    delay => %s
                )
            """, [bucket, path, carrier, str(agency_id), priority, delay_sec])

            result = cursor.fetchone()
            return result[0] if result else None

    except Exception as e:
        logger.error(f'Failed to enqueue policy report parse job: {e}')
        raise


@transaction.atomic
def orchestrate_policy_report_ingest(agency_id: UUID) -> dict:
    """
    Orchestrate the full policy report ingest process.
    Translated from Supabase RPC: orchestrate_policy_report_ingest_with_agency_id

    This function:
    1. Dedupes staging rows
    2. Normalizes staging data
    3. Creates users from staging
    4. Creates writing agent numbers
    5. Syncs staging to deals
    6. Links/creates clients

    Args:
        agency_id: Agency ID to process

    Returns:
        Result dictionary with timing and status
    """
    start_time = datetime.now()
    durations = {}

    try:
        with connection.cursor() as cursor:
            # 0) Dedupe staging rows for this agency
            t0 = datetime.now()
            cursor.execute("""
                SELECT public.dedupe_policy_report_staging_with_agency_id(
                    p_agency_id => %s,
                    p_dry_run => false
                )
            """, [str(agency_id)])
            durations['dedupe'] = str(datetime.now() - t0)

            # 0.5) Normalize staging data
            t1 = datetime.now()
            cursor.execute("""
                SELECT app.orchestrate_normalization(%s)
            """, [str(agency_id)])
            durations['normalize'] = str(datetime.now() - t1)

            # 2) Users (agents)
            t2 = datetime.now()
            cursor.execute("""
                SELECT public.create_users_from_policy_report_staging_with_agency_id(%s)
            """, [str(agency_id)])
            durations['users'] = str(datetime.now() - t2)

            # 3) Writing agent numbers
            t3 = datetime.now()
            cursor.execute("""
                SELECT public.create_writing_agent_numbers_from_policy_report_staging_with_agency_id(%s)
            """, [str(agency_id)])
            durations['numbers'] = str(datetime.now() - t3)

            # 4) Deals sync
            t4 = datetime.now()
            cursor.execute("""
                SELECT public.sync_policy_report_staging_to_deals_with_agency_id(p_agency_id => %s)
            """, [str(agency_id)])
            sync_result = cursor.fetchone()
            sync_result = sync_result[0] if sync_result else {}
            durations['sync'] = str(datetime.now() - t4)

            # 5) Link/Create clients
            t5 = datetime.now()
            cursor.execute("""
                SELECT public.create_clients_from_deals_with_agency_id(p_agency_id => %s)
            """, [str(agency_id)])
            link_result = cursor.fetchone()
            link_result = link_result[0] if link_result else {}
            durations['link_clients'] = str(datetime.now() - t5)

        durations['total'] = str(datetime.now() - start_time)

        return {
            'ok': True,
            'started_at': start_time.isoformat(),
            'durations': durations,
            'sync_result': sync_result,
            'link_result': link_result,
        }

    except Exception as e:
        logger.error(f'Orchestrate policy report ingest failed: {e}')
        return {
            'ok': False,
            'error': str(e),
            'started_at': start_time.isoformat(),
            'durations': durations,
        }


def sync_policy_report_staging_to_deals(agency_id: UUID) -> dict:
    """
    Sync policy report staging data to deals table.
    Translated from Supabase RPC: sync_policy_report_staging_to_deals_with_agency_id

    Args:
        agency_id: Agency ID to process

    Returns:
        Result dictionary with counts
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT public.sync_policy_report_staging_to_deals_with_agency_id(p_agency_id => %s)
            """, [str(agency_id)])

            result = cursor.fetchone()
            return result[0] if result else {'ok': False}

    except Exception as e:
        logger.error(f'Sync staging to deals failed: {e}')
        return {
            'ok': False,
            'error': str(e),
        }


def get_staging_summary(agency_id: UUID) -> dict:
    """
    Get summary of policy report staging data for an agency.

    Args:
        agency_id: Agency ID

    Returns:
        Summary statistics
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT carrier_name) as carrier_count,
                    COUNT(DISTINCT policy_number) as policy_count,
                    COUNT(DISTINCT agent_name) as agent_count,
                    MIN(created_at) as earliest_record,
                    MAX(created_at) as latest_record
                FROM policy_report_staging
                WHERE agency_id = %s
            """, [str(agency_id)])

            row = cursor.fetchone()
            if row:
                return {
                    'total_rows': row[0],
                    'carrier_count': row[1],
                    'policy_count': row[2],
                    'agent_count': row[3],
                    'earliest_record': row[4].isoformat() if row[4] else None,
                    'latest_record': row[5].isoformat() if row[5] else None,
                }
            return {
                'total_rows': 0,
                'carrier_count': 0,
                'policy_count': 0,
                'agent_count': 0,
                'earliest_record': None,
                'latest_record': None,
            }

    except Exception as e:
        logger.error(f'Get staging summary failed: {e}')
        raise


def create_clients_from_deals(agency_id: UUID) -> dict:
    """
    Create client users from deal data.
    Translated from Supabase RPC: create_clients_from_deals_with_agency_id

    This function:
    1. Finds deals that need a client_id
    2. Normalizes client names and contact info
    3. Links existing clients by phone/email
    4. Creates new client users where needed
    5. Updates deals with client_id references

    Args:
        agency_id: Agency ID to process

    Returns:
        Dictionary with counts of linked and created clients
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT public.create_clients_from_deals_with_agency_id(p_agency_id => %s)
            """, [str(agency_id)])
            result = cursor.fetchone()
            return result[0] if result else {
                'linked_phone': 0,
                'linked_email': 0,
                'created_clients': 0,
            }

    except Exception as e:
        logger.error(f'Create clients from deals failed: {e}')
        raise


def create_clients_from_policy_staging(agency_id: UUID) -> dict:
    """
    Create client users from policy staging data.
    Translated from Supabase RPC: create_clients_from_policy_staging

    Args:
        agency_id: Agency ID to process

    Returns:
        Dictionary with processed_count and created_users
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.create_clients_from_policy_staging(p_agency_id => %s)
            """, [str(agency_id)])
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'created_users': row[1],
                }
            return {'processed_count': 0, 'created_users': 0}

    except Exception as e:
        logger.error(f'Create clients from policy staging failed: {e}')
        raise


def create_users_from_staging(agency_id: UUID) -> dict:
    """
    Create agent users from policy report staging data.
    Translated from Supabase RPC: create_users_from_policy_report_staging_with_agency_id

    This function:
    1. Extracts unique agent names from staging
    2. Parses names into first/last
    3. Creates user records where they don't exist

    Args:
        agency_id: Agency ID to process

    Returns:
        Dictionary with processed_count and created_users
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.create_users_from_policy_report_staging_with_agency_id(%s)
            """, [str(agency_id)])
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'created_users': row[1],
                }
            return {'processed_count': 0, 'created_users': 0}

    except Exception as e:
        logger.error(f'Create users from staging failed: {e}')
        raise


def create_products_from_staging(agency_id: UUID) -> dict:
    """
    Create products from policy report staging data.
    Translated from Supabase RPC: create_products_from_policy_report_staging_with_agency_id

    This function:
    1. Finds distinct product values from staging
    2. Reactivates inactive products if they match
    3. Updates existing products with missing data
    4. Creates new products where needed

    Args:
        agency_id: Agency ID to process

    Returns:
        Dictionary with counts of candidates, reactivated, updated, inserted
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT public.create_products_from_policy_report_staging_with_agency_id(%s)
            """, [str(agency_id)])
            result = cursor.fetchone()
            return result[0] if result else {
                'candidates': 0,
                'reactivated': 0,
                'updated': 0,
                'inserted': 0,
            }

    except Exception as e:
        logger.error(f'Create products from staging failed: {e}')
        raise


def create_writing_agent_numbers(agency_id: UUID) -> dict:
    """
    Create writing agent numbers from policy report staging.
    Translated from Supabase RPC: create_writing_agent_numbers_from_policy_report_staging_with_agency_id

    This function:
    1. Extracts unique agent name + carrier + number combinations
    2. Matches agents by parsed first/last name
    3. Inserts agent_carrier_numbers records

    Args:
        agency_id: Agency ID to process

    Returns:
        Dictionary with processed_count and inserted_acn
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.create_writing_agent_numbers_from_policy_report_staging_with_agency_id(%s)
            """, [str(agency_id)])
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'inserted_acn': row[1],
                }
            return {'processed_count': 0, 'inserted_acn': 0}

    except Exception as e:
        logger.error(f'Create writing agent numbers failed: {e}')
        raise


def dedupe_staging(agency_id: UUID, dry_run: bool = False) -> dict:
    """
    Deduplicate policy report staging rows.
    Translated from Supabase RPC: dedupe_policy_report_staging_with_agency_id

    This function:
    1. Fingerprints each row (excluding id)
    2. Identifies duplicate fingerprints
    3. Deletes duplicate rows (keeping one per fingerprint)

    Args:
        agency_id: Agency ID to process
        dry_run: If True, just report what would be deleted

    Returns:
        Dictionary with total, distinct, dupe_rows, deleted counts
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT public.dedupe_policy_report_staging_with_agency_id(
                    p_agency_id => %s,
                    p_dry_run => %s
                )
            """, [str(agency_id), dry_run])
            result = cursor.fetchone()
            return result[0] if result else {
                'scope': str(agency_id),
                'total': 0,
                'distinct': 0,
                'dupe_rows': 0,
                'deleted': 0,
            }

    except Exception as e:
        logger.error(f'Dedupe staging failed: {e}')
        raise


def upsert_products_from_staging() -> dict:
    """
    Upsert products from staging with logging.
    Translated from Supabase RPC: upsert_products_from_staging_logged

    This function processes all agencies (not agency-scoped).

    Returns:
        Dictionary with candidates, updated, need_insert, inserted counts
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT public.upsert_products_from_staging_logged()
            """)
            result = cursor.fetchone()
            return result[0] if result else {
                'candidates': 0,
                'updated': 0,
                'need_insert': 0,
                'inserted': 0,
            }

    except Exception as e:
        logger.error(f'Upsert products from staging failed: {e}')
        raise


def fill_agent_carrier_numbers_from_staging() -> dict:
    """
    Fill agent carrier numbers from staging.
    Translated from Supabase RPC: fill_agent_carrier_numbers_from_staging

    Returns:
        Dictionary with processed_count and inserted_acn
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.fill_agent_carrier_numbers_from_staging()
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'inserted_acn': row[1],
                }
            return {'processed_count': 0, 'inserted_acn': 0}

    except Exception as e:
        logger.error(f'Fill agent carrier numbers failed: {e}')
        raise


def fill_agent_carrier_numbers_with_audit() -> dict:
    """
    Fill agent carrier numbers with detailed audit trail.
    Translated from Supabase RPC: fill_agent_carrier_numbers_from_staging_with_audit

    Returns:
        Dictionary with audit information
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.fill_agent_carrier_numbers_from_staging_with_audit()
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'run_id': str(row[0]) if row[0] else None,
                    'candidates': row[1],
                    'triple_exists': row[2],
                    'pair_exists': row[3],
                    'batch_dedup': row[4],
                    'inserted': row[5],
                    'conflict_race': row[6],
                }
            return {
                'run_id': None,
                'candidates': 0,
                'triple_exists': 0,
                'pair_exists': 0,
                'batch_dedup': 0,
                'inserted': 0,
                'conflict_race': 0,
            }

    except Exception as e:
        logger.error(f'Fill agent carrier numbers with audit failed: {e}')
        raise


def link_staged_agent_numbers() -> dict:
    """
    Link staged agent numbers to users.
    Translated from Supabase RPC: link_staged_agent_numbers

    Returns:
        Dictionary with processed_count, created_users, upserted_acn
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.link_staged_agent_numbers()
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'created_users': row[1],
                    'upserted_acn': row[2],
                }
            return {'processed_count': 0, 'created_users': 0, 'upserted_acn': 0}

    except Exception as e:
        logger.error(f'Link staged agent numbers failed: {e}')
        raise


def sync_agent_carrier_numbers_from_staging() -> dict:
    """
    Sync agent carrier numbers from staging.
    Translated from Supabase RPC: sync_agent_carrier_numbers_from_staging

    Returns:
        Dictionary with processed_count, created_users, upserted_acn
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM public.sync_agent_carrier_numbers_from_staging()
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'processed_count': row[0],
                    'created_users': row[1],
                    'upserted_acn': row[2],
                }
            return {'processed_count': 0, 'created_users': 0, 'upserted_acn': 0}

    except Exception as e:
        logger.error(f'Sync agent carrier numbers failed: {e}')
        raise
