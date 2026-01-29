"""
Carriers Services

Business logic for carrier-related operations.
"""
import logging
from uuid import UUID

from django.db import connection

logger = logging.getLogger(__name__)


def create_or_update_carrier_login(
    *,
    user_id: UUID,
    agency_id: UUID,
    carrier_name: str,
    login: str,
    password: str,
) -> dict:
    """
    Create or update carrier portal login credentials.

    If a record exists for the same agency + carrier + login, updates the password.
    Otherwise, creates a new parsing_info record.

    Args:
        user_id: The admin user's ID (becomes agent_id)
        agency_id: The agency UUID
        carrier_name: Name of the carrier
        login: Login username/email
        password: Login password

    Returns:
        Dictionary with success status and data or error
    """
    try:
        with connection.cursor() as cursor:
            # Look up carrier by name
            cursor.execute("""
                SELECT id FROM carriers
                WHERE name = %s AND is_active = true
                LIMIT 1
            """, [carrier_name])
            carrier_row = cursor.fetchone()

            if not carrier_row:
                return {
                    'success': False,
                    'error': f'No active carrier found with name "{carrier_name}"'
                }

            carrier_id = carrier_row[0]

            # Check for existing parsing_info for this agency + carrier + login
            cursor.execute("""
                SELECT id, password FROM parsing_info
                WHERE agency_id = %s AND carrier_id = %s AND login = %s
                LIMIT 1
            """, [str(agency_id), str(carrier_id), login])
            existing = cursor.fetchone()

            if existing:
                existing_id, existing_password = existing
                # If password is different, update it
                if existing_password != password:
                    cursor.execute("""
                        UPDATE parsing_info
                        SET password = %s
                        WHERE id = %s
                        RETURNING id, created_at
                    """, [password, str(existing_id)])
                    updated = cursor.fetchone()
                    logger.info(f"Updated carrier login for {carrier_name} - {login}")
                    return {
                        'success': True,
                        'data': {
                            'id': str(updated[0]),
                            'created_at': updated[1].isoformat() if updated[1] else None,
                        }
                    }
                else:
                    # Password is the same, no update needed
                    logger.info(f"Carrier login unchanged for {carrier_name} - {login}")
                    return {
                        'success': True,
                        'data': {
                            'id': str(existing_id),
                        }
                    }
            else:
                # Create new parsing_info record
                import uuid
                new_id = uuid.uuid4()
                cursor.execute("""
                    INSERT INTO parsing_info (id, carrier_id, agent_id, agency_id, login, password, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id, created_at
                """, [str(new_id), str(carrier_id), str(user_id), str(agency_id), login, password])
                inserted = cursor.fetchone()
                logger.info(f"Created carrier login for {carrier_name} - {login}")
                return {
                    'success': True,
                    'data': {
                        'id': str(inserted[0]),
                        'created_at': inserted[1].isoformat() if inserted[1] else None,
                    }
                }

    except Exception as e:
        logger.error(f'Create/update carrier login failed: {e}')
        return {
            'success': False,
            'error': str(e)
        }
