"""
SFTP Client Utilities

Provides SFTP client functionality with SSH key authentication and automatic retries.
Supports file uploads with directory creation and robust error handling.
"""

import logging
import time
from pathlib import Path
from typing import Tuple

import paramiko
from paramiko import SFTPClient, SSHClient

from utils.config import settings

logger = logging.getLogger(__name__)


def get_sftp_client() -> Tuple[SSHClient, SFTPClient]:
    """
    Create SFTP client connection using SSH key authentication.

    Uses configuration from settings for host, port, username, key path, and optional passphrase.
    Disables host key checking for challenge environment.

    Returns:
        Tuple of (ssh_client, sftp_client)

    Raises:
        paramiko.AuthenticationException: If SSH key authentication fails
        paramiko.SSHException: If SSH connection fails
        FileNotFoundError: If SSH key file not found
        IOError: If connection cannot be established
    """
    if not settings.SFTP_HOST:
        raise ValueError("SFTP_HOST is not configured")

    if not settings.SFTP_USERNAME:
        raise ValueError("SFTP_USERNAME is not configured")

    # Load SSH private key
    key_path = Path(settings.SFTP_KEY_PATH)
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key file not found: {key_path}")

    try:
        # Try to load RSA key with optional passphrase
        if settings.SFTP_KEY_PASSPHRASE:
            private_key = paramiko.RSAKey.from_private_key_file(
                str(key_path),
                password=settings.SFTP_KEY_PASSPHRASE
            )
        else:
            private_key = paramiko.RSAKey.from_private_key_file(str(key_path))
    except paramiko.PasswordRequiredException:
        raise ValueError("SSH key requires passphrase but SFTP_KEY_PASSPHRASE not set")
    except paramiko.SSHException as e:
        raise paramiko.SSHException(f"Failed to load SSH key: {e}") from e

    # Create SSH client
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect with SSH key authentication
        ssh_client.connect(
            hostname=settings.SFTP_HOST,
            port=settings.SFTP_PORT,
            username=settings.SFTP_USERNAME,
            pkey=private_key,
            timeout=settings.SFTP_TIMEOUT,
            auth_timeout=settings.SFTP_TIMEOUT,
        )

        # Open SFTP channel
        sftp_client = ssh_client.open_sftp()
        # Note: SFTPClient doesn't have settimeout, timeout is handled by SSH connection

        return ssh_client, sftp_client

    except Exception as e:
        ssh_client.close()
        raise IOError(f"Failed to establish SFTP connection: {e}") from e


def upload_file(local_path: str, remote_dir: str, remote_name: str | None = None, retries: int = 3) -> None:
    """
    Upload file to SFTP server with automatic retry and directory creation.

    Args:
        local_path: Path to local file to upload
        remote_dir: Remote directory path (will be created if needed)
        remote_name: Remote filename (uses local basename if None)
        retries: Number of retry attempts on failure

    Raises:
        FileNotFoundError: If local file doesn't exist
        IOError: If upload fails after all retries
        ValueError: If SFTP configuration is invalid
    """
    local_file = Path(local_path)
    if not local_file.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    if not local_file.is_file():
        raise ValueError(f"Local path is not a file: {local_path}")

    # Determine remote filename
    if remote_name is None:
        remote_name = local_file.name

    remote_path = f"{remote_dir.rstrip('/')}/{remote_name}"

    ssh_client = None
    sftp_client = None
    last_error = None

    for attempt in range(retries + 1):
        try:
            # Establish SFTP connection
            ssh_client, sftp_client = get_sftp_client()

            # Ensure remote directory exists
            _ensure_remote_dir(sftp_client, remote_dir)

            # Upload file
            sftp_client.put(str(local_file), remote_path)

            # Verify upload by checking remote file size
            remote_stat = sftp_client.stat(remote_path)
            local_stat = local_file.stat()

            if remote_stat.st_size != local_stat.st_size:
                raise IOError(f"Upload verification failed: size mismatch (local={local_stat.st_size}, remote={remote_stat.st_size})")

            logger.info("Uploaded to SFTP: remote_path=%s", remote_path)
            return

        except Exception as e:
            last_error = e

            if attempt < retries:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    "SFTP upload failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, retries + 1, wait_time, str(e)
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    "SFTP upload failed after %d attempts: remote_path=%s, error=%s",
                    retries + 1, remote_path, str(e)
                )

        finally:
            # Clean up connections
            if sftp_client:
                try:
                    sftp_client.close()
                except Exception:
                    pass
            if ssh_client:
                try:
                    ssh_client.close()
                except Exception:
                    pass

    # Re-raise the last error if all retries failed
    raise IOError(f"SFTP upload failed after {retries + 1} attempts: {last_error}") from last_error


def _ensure_remote_dir(sftp_client: SFTPClient, remote_dir: str) -> None:
    """
    Ensure remote directory exists, creating it recursively if needed.

    Args:
        sftp_client: Active SFTP client connection
        remote_dir: Remote directory path to create

    Raises:
        IOError: If directory creation fails
    """
    if not remote_dir or remote_dir == "/":
        return

    # Normalize path
    remote_dir = remote_dir.rstrip("/")

    try:
        # Check if directory already exists
        sftp_client.stat(remote_dir)
        return  # Directory exists
    except FileNotFoundError:
        pass  # Directory doesn't exist, need to create

    # Create parent directories first
    parent_dir = str(Path(remote_dir).parent)
    if parent_dir != "/" and parent_dir != remote_dir:
        _ensure_remote_dir(sftp_client, parent_dir)

    # Create the directory
    try:
        sftp_client.mkdir(remote_dir)
        logger.debug("Created remote directory: %s", remote_dir)
    except IOError as e:
        # Check if directory was created by another process
        try:
            sftp_client.stat(remote_dir)
            return  # Directory now exists
        except FileNotFoundError:
            raise IOError(f"Failed to create remote directory {remote_dir}: {e}") from e