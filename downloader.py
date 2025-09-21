"""Network operations manager for the book downloader application."""

import os
from pathlib import Path
from typing import BinaryIO, Callable, Optional, Tuple, Union

import network
network.init()
import requests
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Event
from urllib.parse import urlparse

from logger import setup_logger
from config import PROXIES
from env import (
    MAX_RETRY,
    DEFAULT_SLEEP,
    RATE_LIMIT_MAX_SLEEP,
    USE_CF_BYPASS,
    USING_EXTERNAL_BYPASSER,
)
if USE_CF_BYPASS:
    if USING_EXTERNAL_BYPASSER:
        from cloudflare_bypasser_external import get_bypassed_page
    else:
        from cloudflare_bypasser import get_bypassed_page

logger = setup_logger(__name__)

RATE_LIMIT_STATUS_CODES = {429, 503}
DOWNLOAD_CHUNK_SIZE = 64 * 1024
PROGRESS_MIN_INCREMENT = 1.0
PROGRESS_MIN_INTERVAL = 0.25


def _parse_retry_after(retry_after: Optional[str]) -> Optional[float]:
    """Parse Retry-After header value into seconds."""

    if not retry_after:
        return None

    retry_after = retry_after.strip()
    if not retry_after:
        return None

    if retry_after.isdigit():
        return float(retry_after)

    try:
        retry_datetime = parsedate_to_datetime(retry_after)
    except (TypeError, ValueError):
        return None

    if retry_datetime is None:
        return None

    if retry_datetime.tzinfo is None:
        retry_datetime = retry_datetime.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    wait_seconds = (retry_datetime - now).total_seconds()
    return max(wait_seconds, 0.0)


def _rate_limit_wait_details(response: requests.Response, consecutive_attempts: int) -> Optional[Tuple[float, Optional[str]]]:
    """Return wait time and header details when a rate limit status is detected."""

    if response.status_code not in RATE_LIMIT_STATUS_CODES:
        return None

    header_value = response.headers.get("Retry-After")
    wait_seconds = _parse_retry_after(header_value) if header_value is not None else None

    if wait_seconds is None:
        wait_seconds = DEFAULT_SLEEP * (2 ** consecutive_attempts)

    wait_seconds = max(0.0, min(wait_seconds, RATE_LIMIT_MAX_SLEEP))
    return wait_seconds, header_value


def html_get_page(url: str, retry: int = MAX_RETRY, use_bypasser: bool = False) -> str:
    """Fetch HTML content from a URL with retry mechanism."""

    retries_remaining = retry
    rate_limit_attempts = 0
    current_use_bypasser = use_bypasser

    while retries_remaining >= 0:
        response: Optional[requests.Response] = None
        try:
            logger.debug(
                f"html_get_page: {url}, retry: {retries_remaining}, use_bypasser: {current_use_bypasser}"
            )

            if current_use_bypasser and USE_CF_BYPASS:
                logger.info(f"GET Using Cloudflare Bypasser for: {url}")
                page = get_bypassed_page(url)
                logger.debug(f"Success getting: {url}")
                return str(page)

            logger.info(f"GET: {url}")
            response = requests.get(url, proxies=PROXIES)

            wait_details = _rate_limit_wait_details(response, rate_limit_attempts)
            if wait_details is not None:
                wait_seconds, header_value = wait_details
                logger.warning(
                    f"Rate limit detected for URL {url} (status {response.status_code}); "
                    f"waiting {wait_seconds:.2f}s before retrying (Retry-After: {header_value or 'not provided'})"
                )
                response.close()
                response = None
                rate_limit_attempts += 1
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                continue

            rate_limit_attempts = 0
            response.raise_for_status()
            logger.debug(f"Success getting: {url}")
            time.sleep(1)
            return str(response.text)

        except Exception as e:
            if retries_remaining == 0:
                logger.error_trace(f"Failed to fetch page: {url}, error: {e}")
                return ""

            if current_use_bypasser and USE_CF_BYPASS:
                logger.warning(f"Exception while using cloudflare bypass for URL: {url}")
                logger.warning(f"Exception: {e}")
                logger.warning(f"Response: {response}")
            elif response is not None and response.status_code == 404:
                logger.warning(f"404 error for URL: {url}")
                return ""
            elif response is not None and response.status_code == 403:
                logger.warning(
                    f"403 detected for URL: {url}. Should retry using cloudflare bypass."
                )
                current_use_bypasser = True
                rate_limit_attempts = 0
                retries_remaining -= 1
                continue

            sleep_time = DEFAULT_SLEEP * (MAX_RETRY - retries_remaining + 1)
            logger.warning(
                f"Retrying GET {url} in {sleep_time} seconds due to error: {e}"
            )
            time.sleep(sleep_time)
            retries_remaining -= 1
            rate_limit_attempts = 0

        finally:
            if response is not None:
                response.close()

    return ""

def _parse_size_to_bytes(size: str) -> Optional[int]:
    """Parse size strings like "1.2 mb" into a byte count."""

    if not size:
        return None

    cleaned = size.strip().lower()
    if not cleaned:
        return None

    cleaned = cleaned.replace(" ", "").replace(",", ".")

    units = {"kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3}
    for unit, multiplier in units.items():
        if cleaned.endswith(unit):
            try:
                value = float(cleaned[: -len(unit)])
            except ValueError:
                return None
            return int(value * multiplier)

    try:
        return int(float(cleaned))
    except ValueError:
        return None


def _open_destination(
    destination: Union[str, os.PathLike[str], BinaryIO]
) -> Tuple[BinaryIO, Optional[Path], bool]:
    """Return a writable binary handle for the destination."""

    if hasattr(destination, "write"):
        return destination, None, False

    if isinstance(destination, (str, os.PathLike)):
        path = Path(destination)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("wb")
        return handle, path, True

    raise TypeError("destination must be a path or binary file-like object")


def download_url(
    link: str,
    destination: Union[str, os.PathLike[str], BinaryIO],
    size: str = "",
    progress_callback: Optional[Callable[[float], None]] = None,
    cancel_flag: Optional[Event] = None,
) -> bool:
    """Stream content from URL into the provided destination.

    Args:
        link: URL to download from.
        destination: Path or writable binary handle where the content should be stored.
        size: Optional human-readable size hint used to improve progress estimation.
        progress_callback: Optional callback receiving percentage completion updates.
        cancel_flag: Optional threading.Event to signal cancellation.

    Returns:
        bool: True if the download completed successfully, False otherwise.
    """

    rate_limit_attempts = 0
    response: Optional[requests.Response] = None
    file_handle: Optional[BinaryIO] = None
    destination_path: Optional[Path] = None
    close_file = False
    success = False

    try:
        file_handle, destination_path, close_file = _open_destination(destination)
    except Exception as exc:
        logger.error_trace(f"Failed to open destination for download: {exc}")
        return False

    try:
        logger.info(f"Downloading from: {link}")

        while True:
            response = requests.get(link, stream=True, proxies=PROXIES)

            wait_details = _rate_limit_wait_details(response, rate_limit_attempts)
            if wait_details is None:
                rate_limit_attempts = 0
                break

            wait_seconds, header_value = wait_details
            logger.warning(
                f"Rate limit detected for download {link} (status {response.status_code}); "
                f"waiting {wait_seconds:.2f}s before retrying (Retry-After: {header_value or 'not provided'})"
            )
            response.close()
            response = None
            rate_limit_attempts += 1
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        if response is None:
            return False

        response.raise_for_status()

        total_size = _parse_size_to_bytes(size)
        if total_size is None:
            content_length = response.headers.get("content-length")
            if content_length is not None:
                try:
                    total_size = int(content_length)
                except (TypeError, ValueError):
                    total_size = None

        bytes_downloaded = 0
        last_report_percent = -1.0
        last_report_time = time.monotonic()

        reported_completion = False

        if progress_callback is not None:
            try:
                progress_callback(0.0)
            except Exception:
                logger.warning("Progress callback raised an exception at start", exc_info=True)
            if total_size:
                last_report_percent = 0.0

        cancelled = False

        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if cancel_flag is not None and cancel_flag.is_set():
                cancelled = True
                break

            if not chunk:
                continue

            file_handle.write(chunk)
            bytes_downloaded += len(chunk)

            if cancel_flag is not None and cancel_flag.is_set():
                cancelled = True
                break

            if progress_callback is not None and total_size:
                percent = min(bytes_downloaded / total_size * 100.0, 100.0)
                now = time.monotonic()
                if (
                    percent >= 100.0
                    or percent - last_report_percent >= PROGRESS_MIN_INCREMENT
                    or now - last_report_time >= PROGRESS_MIN_INTERVAL
                ):
                    try:
                        progress_callback(percent)
                    except Exception:
                        logger.warning("Progress callback raised an exception", exc_info=True)
                    last_report_percent = percent
                    last_report_time = now
                    if percent >= 100.0:
                        reported_completion = True

        if cancelled:
            logger.info(f"Download cancelled: {link}")
            return False

        success = True

        if progress_callback is not None and not reported_completion:
            try:
                progress_callback(100.0)
            except Exception:
                logger.warning("Progress callback raised an exception at completion", exc_info=True)

        if total_size and bytes_downloaded < total_size * 0.9:
            content_type = response.headers.get("content-type", "")
            if content_type.startswith("text/html"):
                logger.warning(
                    f"Failed to download content for {link}. Found HTML content instead."
                )
                success = False
                return False

        return True
    except requests.exceptions.RequestException as e:
        logger.error_trace(f"Failed to download from {link}: {e}")
        return False
    finally:
        if response is not None:
            response.close()
        if file_handle is not None:
            try:
                file_handle.flush()
            except Exception:
                pass
            if close_file:
                file_handle.close()
        if not success and destination_path is not None:
            try:
                if destination_path.exists():
                    destination_path.unlink()
            except Exception:
                logger.warning(
                    f"Failed to remove incomplete download at {destination_path}",
                    exc_info=True,
                )

def get_absolute_url(base_url: str, url: str) -> str:
    """Get absolute URL from relative URL and base URL.
    
    Args:
        base_url: Base URL
        url: Relative URL
    """
    if url.strip() == "":
        return ""
    if url.strip("#") == "":
        return ""
    if url.startswith("http"):
        return url
    parsed_url = urlparse(url)
    parsed_base = urlparse(base_url)
    if parsed_url.netloc == "" or parsed_url.scheme == "":
        parsed_url = parsed_url._replace(netloc=parsed_base.netloc, scheme=parsed_base.scheme)
    return parsed_url.geturl()
