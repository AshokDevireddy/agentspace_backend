"""
Supabase Storage Service for Policy Reports

Provides Supabase Storage operations for policy report file management:
- Direct file uploads
- File listing for agency/carrier folders
- File deletion
"""
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

import httpx
from django.conf import settings

logger = logging.getLogger(__name__)

# Configuration
SUPABASE_URL = getattr(settings, 'SUPABASE_URL', os.getenv('SUPABASE_URL', ''))
SUPABASE_SERVICE_KEY = getattr(
    settings, 'SUPABASE_SERVICE_ROLE_KEY', os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
)
POLICY_REPORTS_BUCKET = os.getenv('SUPABASE_POLICY_REPORTS_BUCKET_NAME', 'policy-reports')

# Allowed content types for upload
ALLOWED_CONTENT_TYPES = [
    'text/csv',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
]

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB per file


def _get_storage_headers() -> dict:
    """Get headers for Supabase Storage API requests."""
    return {
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'apikey': SUPABASE_SERVICE_KEY,
    }


def _sanitize_filename(filename: str) -> str:
    """Sanitize filename to be safe for storage paths."""
    return re.sub(r'[^a-zA-Z0-9._-]', '_', filename)


def _sanitize_carrier_name(carrier_name: str) -> str:
    """
    Sanitize carrier name to be safe for file paths.
    Removes special characters and normalizes the name.
    """
    sanitized = re.sub(r'[^a-zA-Z0-9\s_-]', '', carrier_name)
    return re.sub(r'\s+', '_', sanitized).lower().strip()


@dataclass
class UploadResult:
    """Result of file upload operation."""
    success: bool
    file_name: Optional[str] = None
    storage_path: Optional[str] = None
    size: Optional[int] = None
    content_type: Optional[str] = None
    error: Optional[str] = None


@dataclass
class FileListResult:
    """Result of file listing operation."""
    success: bool
    files: list = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.files is None:
            self.files = []


@dataclass
class DeleteResult:
    """Result of file deletion operation."""
    success: bool
    deleted_count: int = 0
    error: Optional[str] = None


def validate_file(content_type: str, size: int, file_name: str) -> Optional[str]:
    """
    Validate file before upload.

    Returns error message if validation fails, None if valid.
    """
    if content_type not in ALLOWED_CONTENT_TYPES:
        return f'Invalid file type: {content_type}. Only CSV and Excel files are allowed.'

    if size > MAX_FILE_SIZE:
        return f'File size exceeds limit. Maximum size is 10MB.'

    if size == 0:
        return 'File is empty'

    return None


def generate_storage_path(agency_id: UUID, carrier_name: str, file_name: str) -> str:
    """
    Generate the storage path for an uploaded file.
    Format: {agency_id}/{carrier_name}/{timestamp}_{filename}
    """
    sanitized_carrier = _sanitize_carrier_name(carrier_name)
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    sanitized_filename = _sanitize_filename(file_name)

    return f'{agency_id}/{sanitized_carrier}/{timestamp}_{sanitized_filename}'


def upload_file(
    agency_id: UUID,
    carrier_name: str,
    file_content: bytes,
    file_name: str,
    content_type: str,
) -> UploadResult:
    """
    Upload a file to Supabase Storage.

    Args:
        agency_id: The agency UUID
        carrier_name: Carrier name (will be sanitized)
        file_content: File bytes
        file_name: Original file name
        content_type: MIME type

    Returns:
        UploadResult with path or error
    """
    # Validate file
    validation_error = validate_file(content_type, len(file_content), file_name)
    if validation_error:
        return UploadResult(success=False, error=validation_error)

    try:
        # Generate storage path
        storage_path = generate_storage_path(agency_id, carrier_name, file_name)

        # Upload to Supabase Storage
        upload_url = f'{SUPABASE_URL}/storage/v1/object/{POLICY_REPORTS_BUCKET}/{storage_path}'

        headers = _get_storage_headers()
        headers['Content-Type'] = content_type

        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                upload_url,
                content=file_content,
                headers=headers,
            )

            if not response.is_success:
                logger.error(f'Supabase storage upload failed: {response.text}')
                return UploadResult(
                    success=False,
                    error=f'Storage upload failed: {response.status_code}',
                )

        return UploadResult(
            success=True,
            file_name=file_name,
            storage_path=storage_path,
            size=len(file_content),
            content_type=content_type,
        )

    except httpx.RequestError as e:
        logger.error(f'Storage request error: {e}')
        return UploadResult(success=False, error='Storage service unavailable')
    except Exception as e:
        logger.error(f'Unexpected error uploading file: {e}')
        return UploadResult(success=False, error='Failed to upload file')


def list_agency_files(
    agency_id: UUID,
    prefix: Optional[str] = None,
    limit: int = 100,
) -> FileListResult:
    """
    List files in an agency's storage folder.

    Args:
        agency_id: The agency UUID
        prefix: Optional prefix within agency folder (e.g., carrier name)
        limit: Maximum number of files to return

    Returns:
        FileListResult with list of files or error
    """
    try:
        # Build the folder path
        folder_path = str(agency_id)
        if prefix:
            folder_path = f'{folder_path}/{_sanitize_carrier_name(prefix)}'

        # List files via Supabase Storage API
        list_url = f'{SUPABASE_URL}/storage/v1/object/list/{POLICY_REPORTS_BUCKET}'

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                list_url,
                headers=_get_storage_headers(),
                json={
                    'prefix': folder_path,
                    'limit': limit,
                },
            )

            if not response.is_success:
                # Empty folder is not an error
                if response.status_code == 404:
                    return FileListResult(success=True, files=[])
                logger.error(f'Failed to list files: {response.text}')
                return FileListResult(success=False, error='Failed to list files')

            data = response.json()

        # Format file list
        files = []
        for item in data:
            if item.get('name'):  # Skip folder entries
                files.append({
                    'name': item['name'],
                    'id': item.get('id'),
                    'size': item.get('metadata', {}).get('size', 0),
                    'created_at': item.get('created_at'),
                    'updated_at': item.get('updated_at'),
                })

        return FileListResult(success=True, files=files)

    except httpx.RequestError as e:
        logger.error(f'Storage request error: {e}')
        return FileListResult(success=False, error='Storage service unavailable')
    except Exception as e:
        logger.error(f'Unexpected error listing files: {e}')
        return FileListResult(success=False, error='Failed to list files')


def list_carrier_files(
    agency_id: UUID,
    carrier_name: str,
    limit: int = 100,
) -> FileListResult:
    """
    List files for a specific carrier within an agency.

    Args:
        agency_id: The agency UUID
        carrier_name: The carrier name
        limit: Maximum number of files to return

    Returns:
        FileListResult with list of files or error
    """
    return list_agency_files(agency_id, prefix=carrier_name, limit=limit)


def delete_file(storage_path: str) -> DeleteResult:
    """
    Delete a single file from storage.

    Args:
        storage_path: The full storage path of the file

    Returns:
        DeleteResult with success status
    """
    try:
        delete_url = f'{SUPABASE_URL}/storage/v1/object/{POLICY_REPORTS_BUCKET}'

        with httpx.Client(timeout=30.0) as client:
            response = client.request(
                'DELETE',
                delete_url,
                headers=_get_storage_headers(),
                json={'prefixes': [storage_path]},
            )

            if not response.is_success:
                logger.error(f'Failed to delete file: {response.text}')
                return DeleteResult(success=False, error='Failed to delete file')

        return DeleteResult(success=True, deleted_count=1)

    except httpx.RequestError as e:
        logger.error(f'Storage request error: {e}')
        return DeleteResult(success=False, error='Storage service unavailable')
    except Exception as e:
        logger.error(f'Unexpected error deleting file: {e}')
        return DeleteResult(success=False, error='Failed to delete file')


def delete_carrier_folder(
    agency_id: UUID,
    carrier_name: str,
) -> DeleteResult:
    """
    Delete all files for a carrier within an agency.
    Used when replacing carrier files with new uploads.

    Args:
        agency_id: The agency UUID
        carrier_name: The carrier name

    Returns:
        DeleteResult with count of deleted files
    """
    try:
        # First list all files in the carrier folder
        sanitized_carrier = _sanitize_carrier_name(carrier_name)
        folder_path = f'{agency_id}/{sanitized_carrier}'

        list_result = list_agency_files(agency_id, prefix=carrier_name, limit=1000)
        if not list_result.success:
            return DeleteResult(success=False, error=list_result.error)

        if not list_result.files:
            # No files to delete
            return DeleteResult(success=True, deleted_count=0)

        # Build list of file paths to delete
        file_paths = [f'{folder_path}/{f["name"]}' for f in list_result.files]

        # Delete all files
        delete_url = f'{SUPABASE_URL}/storage/v1/object/{POLICY_REPORTS_BUCKET}'

        with httpx.Client(timeout=60.0) as client:
            response = client.request(
                'DELETE',
                delete_url,
                headers=_get_storage_headers(),
                json={'prefixes': file_paths},
            )

            if not response.is_success:
                logger.error(f'Failed to delete carrier folder: {response.text}')
                return DeleteResult(success=False, error='Failed to delete files')

        deleted_count = len(file_paths)
        logger.info(f'Deleted {deleted_count} files from carrier folder: {folder_path}')
        return DeleteResult(success=True, deleted_count=deleted_count)

    except httpx.RequestError as e:
        logger.error(f'Storage request error: {e}')
        return DeleteResult(success=False, error='Storage service unavailable')
    except Exception as e:
        logger.error(f'Unexpected error deleting carrier folder: {e}')
        return DeleteResult(success=False, error='Failed to delete carrier folder')


def replace_carrier_files(
    agency_id: UUID,
    carrier_name: str,
    file_content: bytes,
    file_name: str,
    content_type: str,
) -> UploadResult:
    """
    Replace existing files in carrier folder with new upload.
    Deletes all existing files, then uploads the new file.

    Args:
        agency_id: The agency UUID
        carrier_name: Carrier name
        file_content: File bytes
        file_name: Original file name
        content_type: MIME type

    Returns:
        UploadResult with path or error
    """
    # First delete existing files
    delete_result = delete_carrier_folder(agency_id, carrier_name)
    if not delete_result.success:
        return UploadResult(
            success=False,
            error=f'Failed to delete existing files: {delete_result.error}',
        )

    deleted_count = delete_result.deleted_count
    if deleted_count > 0:
        logger.info(f'Deleted {deleted_count} existing files for carrier {carrier_name}')

    # Upload new file
    upload_result = upload_file(
        agency_id=agency_id,
        carrier_name=carrier_name,
        file_content=file_content,
        file_name=file_name,
        content_type=content_type,
    )

    return upload_result
