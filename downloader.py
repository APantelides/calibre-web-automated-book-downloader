"""Network operations manager for the book downloader application."""

import network
network.init()
import requests
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Callable, Optional, Tuple
from urllib.parse import urlparse
from tqdm import tqdm
from threading import Event
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

def download_url(link: str, size: str = "", progress_callback: Optional[Callable[[float], None]] = None, cancel_flag: Optional[Event] = None) -> Optional[BytesIO]:
    """Download content from URL into a BytesIO buffer.
    
    Args:
        link: URL to download from
        
    Returns:
        BytesIO: Buffer containing downloaded content if successful
    """
    rate_limit_attempts = 0
    response: Optional[requests.Response] = None

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
            return None

        response.raise_for_status()

        total_size : float = 0.0
        try:
            # we assume size is in MB
            total_size = float(size.strip().replace(" ", "").replace(",", ".").upper()[:-2].strip()) * 1024 * 1024
        except:
            total_size = float(response.headers.get('content-length', 0))
        
        buffer = BytesIO()

        # Initialize the progress bar with your guess
        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc='Downloading')
        for chunk in response.iter_content(chunk_size=1000):
            buffer.write(chunk)
            pbar.update(len(chunk))
            if progress_callback is not None:
                progress_callback(pbar.n * 100.0 / total_size)
            if cancel_flag is not None and cancel_flag.is_set():
                logger.info(f"Download cancelled: {link}")
                return None
            
        pbar.close()
        if buffer.tell() * 0.1 < total_size * 0.9:
            # Check the content of the buffer if its HTML or binary
            if response.headers.get('content-type', '').startswith('text/html'):
                logger.warn(f"Failed to download content for {link}. Found HTML content instead.")
                return None
        return buffer
    except requests.exceptions.RequestException as e:
        logger.error_trace(f"Failed to download from {link}: {e}")
        return None
    finally:
        if response is not None:
            response.close()

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
