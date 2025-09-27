"""Backend logic for the book download application."""

import threading, time
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import subprocess
import os
import hashlib
import json
from datetime import datetime
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
    wait,
    FIRST_COMPLETED,
    ALL_COMPLETED,
)
from threading import Event

from logger import setup_logger
from config import CUSTOM_SCRIPT
from env import INGEST_DIR, TMP_DIR, MAIN_LOOP_SLEEP_TIME, USE_BOOK_TITLE, MAX_CONCURRENT_DOWNLOADS, DOWNLOAD_PROGRESS_UPDATE_INTERVAL
from models import book_queue, BookInfo, QueueStatus, SearchFilters, DuplicateEntry
import book_manager

logger = setup_logger(__name__)

_DUPLICATE_STATE_FILE = Path(__file__).resolve().parent / "data" / "duplicate-review.json"
_DUPLICATE_STATE_LOCK = threading.Lock()

def _sanitize_filename(filename: str) -> str:
    """Sanitize a filename by replacing spaces with underscores and removing invalid characters."""
    keepcharacters = (' ','.','_')
    return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


def _hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return a SHA-256 hash of the file at ``path``."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_duplicate_state_unlocked() -> Dict[str, Dict[str, Any]]:
    try:
        with _DUPLICATE_STATE_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        return {}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error_trace(f"Failed to load duplicate state: {exc}")
    return {}


def _load_duplicate_state() -> Dict[str, Dict[str, Any]]:
    with _DUPLICATE_STATE_LOCK:
        return _load_duplicate_state_unlocked()


def _save_duplicate_state_unlocked(state: Dict[str, Dict[str, Any]]) -> None:
    try:
        _DUPLICATE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _DUPLICATE_STATE_FILE.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error_trace(f"Failed to save duplicate state: {exc}")


def _save_duplicate_state(state: Dict[str, Dict[str, Any]]) -> None:
    with _DUPLICATE_STATE_LOCK:
        _save_duplicate_state_unlocked(state)


def set_duplicate_reviewed(group_id: str, reviewed: bool) -> None:
    """Persist the review state for a duplicate group."""
    if not group_id:
        raise ValueError("group_id is required")

    with _DUPLICATE_STATE_LOCK:
        state = _load_duplicate_state_unlocked()
        if reviewed:
            state[group_id] = {
                "reviewed": True,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
        else:
            state.pop(group_id, None)
        _save_duplicate_state_unlocked(state)


def resolve_ingest_file(relative_path: str) -> Path:
    """Return a safe absolute path within ``INGEST_DIR`` for the given relative path."""
    if not relative_path:
        raise ValueError("relative_path is required")

    root = INGEST_DIR.resolve()
    candidate = (root / relative_path).resolve()
    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(relative_path)
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError("Path escapes ingest directory")
    return candidate


def list_duplicate_groups() -> Dict[str, Any]:
    """Return potential duplicate groups within ``INGEST_DIR``."""
    if not INGEST_DIR.exists():
        return {"groups": []}

    state = _load_duplicate_state()
    root = INGEST_DIR.resolve()
    entries: List[Dict[str, Any]] = []

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue

        try:
            relative_path = path.relative_to(root).as_posix()
        except ValueError:
            continue

        sanitized_stem = _sanitize_filename(path.stem).lower() or path.stem.lower()
        try:
            file_hash = _hash_file(path)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error_trace(f"Unable to hash {path}: {exc}")
            continue

        stat = path.stat()
        entries.append(
            {
                "name": path.name,
                "relative_path": relative_path,
                "size": stat.st_size,
                "modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
                "stem": sanitized_stem,
                "hash": file_hash,
                "extension": path.suffix.lstrip("."),
            }
        )

    groups: List[Dict[str, Any]] = []

    stem_map: Dict[str, List[Dict[str, Any]]] = {}
    hash_map: Dict[str, List[Dict[str, Any]]] = {}

    for entry in entries:
        stem_map.setdefault(entry["stem"], []).append(entry)
        hash_map.setdefault(entry["hash"], []).append(entry)

    def build_group(group_type: str, key: str, files: List[Dict[str, Any]]) -> Dict[str, Any]:
        group_id = f"{group_type}:{key}"
        review_state = state.get(group_id, {})
        return {
            "id": group_id,
            "type": group_type,
            "key": key,
            "files": files,
            "reviewed": bool(review_state.get("reviewed")),
            "reviewed_at": review_state.get("timestamp"),
        }

    for stem, files in stem_map.items():
        if len(files) > 1:
            groups.append(build_group("stem", stem, files))

    for file_hash, files in hash_map.items():
        if len(files) > 1:
            groups.append(build_group("hash", file_hash, files))

    groups.sort(key=lambda g: (g["type"], g["key"]))
    return {"groups": groups}

def search_books(query: str, filters: SearchFilters) -> List[Dict[str, Any]]:
    """Search for books matching the query.
    
    Args:
        query: Search term
        filters: Search filters object
        
    Returns:
        List[Dict]: List of book information dictionaries
    """
    try:
        books = book_manager.search_books(query, filters)
        return [_book_info_to_dict(book) for book in books]
    except Exception as e:
        logger.error_trace(f"Error searching books: {e}")
        return []

def get_book_info(book_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information for a specific book.
    
    Args:
        book_id: Book identifier
        
    Returns:
        Optional[Dict]: Book information dictionary if found
    """
    try:
        book = book_manager.get_book_info(book_id)
        return _book_info_to_dict(book)
    except Exception as e:
        logger.error_trace(f"Error getting book info: {e}")
        return None

def _build_ingest_paths(book_info: BookInfo) -> Tuple[Path, Path, str]:
    """Replicate ingest filename logic for duplicate detection."""
    if USE_BOOK_TITLE:
        sanitized_title = _sanitize_filename(book_info.title)
        if not sanitized_title:
            sanitized_title = "book"
        unique_suffix = hashlib.md5(book_info.id.encode("utf-8")).hexdigest()[:8]
        filename_stem = f"{sanitized_title}-{unique_suffix}"
    else:
        filename_stem = book_info.id

    extension = f".{book_info.format}" if book_info.format else ""
    book_name = f"{filename_stem}{extension}"
    final_path = INGEST_DIR / book_name
    intermediate_path = INGEST_DIR / f"{book_info.id}.crdownload"
    return final_path, intermediate_path, filename_stem


def detect_duplicate(book_info: BookInfo) -> Optional[DuplicateEntry]:
    """Return duplicate metadata if the ingest directory already contains the book."""
    final_path, intermediate_path, _ = _build_ingest_paths(book_info)

    status = book_queue.get_status_for(book_info.id)
    existing_path: Optional[str] = None
    reason: Optional[str] = None

    if status and status not in [QueueStatus.ERROR, QueueStatus.DONE, QueueStatus.CANCELLED]:
        reason = "queued"
        existing_book = book_queue.get_book(book_info.id)
        if existing_book and existing_book.download_path:
            existing_path = existing_book.download_path
    elif final_path.exists():
        reason = "on_disk"
        existing_path = str(final_path)
    elif intermediate_path.exists():
        reason = "downloading"
        existing_path = str(intermediate_path)

    if not reason:
        return None

    duplicate = DuplicateEntry(
        book_id=book_info.id,
        book_info=book_info,
        ingest_path=str(final_path),
        reason=reason,
        existing_path=existing_path,
        status=status.value if isinstance(status, QueueStatus) else None,
    )
    return duplicate


def _duplicate_entry_to_dict(entry: DuplicateEntry) -> Dict[str, Any]:
    """Serialize duplicate entries for API responses."""
    payload = entry.to_dict()
    return payload


def queue_book(book_id: str, priority: int = 0, force: bool = False) -> Tuple[bool, Optional[DuplicateEntry]]:
    """Add a book to the download queue with specified priority.

    Args:
        book_id: Book identifier
        priority: Priority level (lower number = higher priority)

    Returns:
        Tuple[bool, Optional[DuplicateEntry]]: Success flag and duplicate metadata if rejected.
    """
    try:
        book_info = book_manager.get_book_info(book_id)
        duplicate_entry: Optional[DuplicateEntry] = None

        if force:
            book_queue.resolve_duplicate(book_id)
        else:
            duplicate_entry = detect_duplicate(book_info)
            if duplicate_entry:
                duplicate_entry.priority = priority
                book_queue.record_duplicate(duplicate_entry)
                logger.info(
                    "Duplicate detected for %s: reason=%s", book_info.title, duplicate_entry.reason
                )
                return False, duplicate_entry

        book_queue.add(book_id, book_info, priority)
        logger.info(f"Book queued with priority {priority}: {book_info.title}")
        return True, None
    except Exception as e:
        logger.error_trace(f"Error queueing book: {e}")
        return False, None


def list_duplicates() -> List[Dict[str, Any]]:
    """Return all recorded duplicate entries."""
    return [_duplicate_entry_to_dict(entry) for entry in book_queue.list_duplicates()]


def remove_duplicate(book_id: str) -> Optional[Dict[str, Any]]:
    """Remove a duplicate entry without queueing the book."""
    entry = book_queue.resolve_duplicate(book_id)
    if not entry:
        return None
    return _duplicate_entry_to_dict(entry)


def force_duplicate(book_id: str, priority: Optional[int] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """Attempt to queue a duplicate entry, overriding detection."""
    entry = book_queue.resolve_duplicate(book_id)
    if not entry:
        return False, None, "Duplicate entry not found"

    target_priority = priority if priority is not None else entry.priority
    entry.priority = target_priority
    success, duplicate = queue_book(book_id, target_priority, force=True)
    if success:
        return True, _duplicate_entry_to_dict(entry), None

    # Queue failed; restore the entry for later review
    if duplicate:
        book_queue.record_duplicate(duplicate)
        return False, _duplicate_entry_to_dict(duplicate), "Failed to queue duplicate"

    entry.priority = target_priority
    book_queue.record_duplicate(entry)
    return False, _duplicate_entry_to_dict(entry), "Failed to queue duplicate"

def queue_status() -> Dict[str, Dict[str, Any]]:
    """Get current status of the download queue.
    
    Returns:
        Dict: Queue status organized by status type
    """
    status = book_queue.get_status()
    serialized_status: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for status_type, books in status.items():
        serialized_books: Dict[str, Dict[str, Any]] = {}
        for book_id, book_info in books.items():
            if book_info.download_path and not os.path.exists(book_info.download_path):
                book_info.download_path = None

            serialized_books[book_id] = _book_info_to_dict(book_info)

        serialized_status[status_type.value] = serialized_books

    return serialized_status

def get_book_data(book_id: str) -> Tuple[Optional[bytes], BookInfo]:
    """Get book data for a specific book, including its title.

    Args:
        book_id: Book identifier

    Returns:
        Tuple[Optional[bytes], str]: Book data if available, and the book title
    """
    book_info: Optional[BookInfo] = None
    try:
        book_info = book_queue._book_data[book_id]
        path = book_info.download_path
        with open(path, "rb") as f:
            return f.read(), book_info
    except Exception as e:
        logger.error_trace(f"Error getting book data: {e}")
        if book_info is not None:
            book_info.download_path = None
            return None, book_info
        return None, BookInfo(id=book_id, title="Unknown")

def _book_info_to_dict(book: BookInfo) -> Dict[str, Any]:
    """Convert BookInfo object to dictionary representation."""
    return {
        key: value for key, value in book.__dict__.items()
        if value is not None
    }

def _download_book_with_cancellation(book_id: str, cancel_flag: Event) -> Optional[str]:
    """Download and process a book with cancellation support.
    
    Args:
        book_id: Book identifier
        cancel_flag: Threading event to signal cancellation
        
    Returns:
        str: Path to the downloaded book if successful, None otherwise
    """
    try:
        # Check for cancellation before starting
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before starting: {book_id}")
            return None
            
        book_info = book_queue._book_data[book_id]
        logger.info(f"Starting download: {book_info.title}")

        if USE_BOOK_TITLE:
            sanitized_title = _sanitize_filename(book_info.title)
            if not sanitized_title:
                sanitized_title = "book"
            unique_suffix = hashlib.md5(book_id.encode("utf-8")).hexdigest()[:8]
            filename_stem = f"{sanitized_title}-{unique_suffix}"
        else:
            filename_stem = book_id

        extension = f".{book_info.format}" if book_info.format else ""
        book_name = f"{filename_stem}{extension}"
        book_path = TMP_DIR / book_name

        # Check cancellation before download
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before book manager call: {book_id}")
            return None
        
        progress_callback = lambda progress: update_download_progress(book_id, progress)
        success = book_manager.download_book(book_info, book_path, progress_callback, cancel_flag)
        
        # Stop progress updates
        cancel_flag.wait(0.1)  # Brief pause for progress thread cleanup
        
        if cancel_flag.is_set():
            logger.info(f"Download cancelled during download: {book_id}")
            # Clean up partial download
            if book_path.exists():
                book_path.unlink()
            return None
            
        if not success:
            raise Exception("Unknown error downloading book")

        # Check cancellation before post-processing
        if cancel_flag.is_set():
            logger.info(f"Download cancelled before post-processing: {book_id}")
            if book_path.exists():
                book_path.unlink()
            return None

        if CUSTOM_SCRIPT:
            logger.info(f"Running custom script: {CUSTOM_SCRIPT}")
            subprocess.run([CUSTOM_SCRIPT, book_path])
            
        intermediate_path = INGEST_DIR / f"{book_id}.crdownload"
        final_path = INGEST_DIR / book_name
        
        if os.path.exists(book_path):
            logger.info(f"Moving book to ingest directory: {book_path} -> {final_path}")
            try:
                shutil.move(book_path, intermediate_path)
            except Exception as e:
                logger.debug(
                    f"Error moving book: {e}, will try copying instead"
                )
                try:
                    intermediate_path.unlink(missing_ok=True)
                except Exception as cleanup_error:
                    logger.debug(
                        "Error removing stale intermediate file before copy: %s",
                        cleanup_error,
                    )
                try:
                    shutil.copy2(book_path, intermediate_path)
                except Exception as copy_error:
                    logger.debug(
                        "Error copying book: %s, will try copying without permissions instead",
                        copy_error,
                    )
                    try:
                        intermediate_path.unlink(missing_ok=True)
                    except Exception as cleanup_error:
                        logger.debug(
                            "Error removing stale intermediate file before fallback copy: %s",
                            cleanup_error,
                        )
                    shutil.copyfile(book_path, intermediate_path)
                if book_path.exists():
                    book_path.unlink()

            # Final cancellation check before completing
            if cancel_flag.is_set():
                logger.info(f"Download cancelled before final rename: {book_id}")
                if intermediate_path.exists():
                    intermediate_path.unlink()
                return None
                
            os.replace(intermediate_path, final_path)
            logger.info(f"Download completed successfully: {book_info.title}")
            
        return str(final_path)
    except Exception as e:
        if cancel_flag.is_set():
            logger.info(f"Download cancelled during error handling: {book_id}")
        else:
            logger.error_trace(f"Error downloading book: {e}")
        return None

def update_download_progress(book_id: str, progress: float) -> None:
    """Update download progress."""
    book_queue.update_progress(book_id, progress)

def cancel_download(book_id: str) -> bool:
    """Cancel a download.
    
    Args:
        book_id: Book identifier to cancel
        
    Returns:
        bool: True if cancellation was successful
    """
    return book_queue.cancel_download(book_id)

def set_book_priority(book_id: str, priority: int) -> bool:
    """Set priority for a queued book.
    
    Args:
        book_id: Book identifier
        priority: New priority level (lower = higher priority)
        
    Returns:
        bool: True if priority was successfully changed
    """
    return book_queue.set_priority(book_id, priority)

def reorder_queue(book_priorities: Dict[str, int]) -> bool:
    """Bulk reorder queue.
    
    Args:
        book_priorities: Dict mapping book_id to new priority
        
    Returns:
        bool: True if reordering was successful
    """
    return book_queue.reorder_queue(book_priorities)

def get_queue_order() -> List[Dict[str, any]]:
    """Get current queue order for display."""
    return book_queue.get_queue_order()

def get_active_downloads() -> List[str]:
    """Get list of currently active downloads."""
    return book_queue.get_active_downloads()

def clear_completed() -> int:
    """Clear all completed downloads from tracking."""
    return book_queue.clear_completed()

def _process_single_download(book_id: str, cancel_flag: Event) -> None:
    """Process a single download job."""
    try:
        book_queue.update_status(book_id, QueueStatus.DOWNLOADING)
        download_path = _download_book_with_cancellation(book_id, cancel_flag)
        
        if cancel_flag.is_set():
            book_queue.update_status(book_id, QueueStatus.CANCELLED)
            return
            
        if download_path:
            book_queue.update_download_path(book_id, download_path)
            new_status = QueueStatus.AVAILABLE
        else:
            new_status = QueueStatus.ERROR
            
        book_queue.update_status(book_id, new_status)
        
        logger.info(
            f"Book {book_id} download {'successful' if download_path else 'failed'}"
        )
        
    except Exception as e:
        if not cancel_flag.is_set():
            logger.error_trace(f"Error in download processing: {e}")
            book_queue.update_status(book_id, QueueStatus.ERROR)
        else:
            logger.info(f"Download cancelled: {book_id}")
            book_queue.update_status(book_id, QueueStatus.CANCELLED)

def concurrent_download_loop(stop_event: Optional[Event] = None) -> None:
    """Main download coordinator using ThreadPoolExecutor for concurrent downloads."""
    logger.info(
        f"Starting concurrent download loop with {MAX_CONCURRENT_DOWNLOADS} workers"
    )

    with ThreadPoolExecutor(
        max_workers=MAX_CONCURRENT_DOWNLOADS, thread_name_prefix="BookDownload"
    ) as executor:
        active_futures: Dict[Future, str] = {}

        while True:
            if stop_event and stop_event.is_set():
                logger.info("Shutdown signal received for download coordinator")
                # Wait for any in-flight downloads to finish gracefully
                if active_futures:
                    done, _ = wait(
                        active_futures.keys(),
                        return_when=ALL_COMPLETED,
                    )
                    for future in done:
                        book_id = active_futures.pop(future, None)
                        if book_id is None:
                            continue
                        try:
                            future.result()
                        except Exception as e:
                            logger.error_trace(
                                f"Future exception during shutdown for {book_id}: {e}"
                            )
                break

            # Start new downloads if we have capacity
            started_download = False
            while len(active_futures) < MAX_CONCURRENT_DOWNLOADS:
                block_for_job = not active_futures
                timeout = 0.1 if stop_event and block_for_job else None
                next_download = book_queue.get_next(block=block_for_job, timeout=timeout)
                if not next_download:
                    break

                book_id, cancel_flag = next_download
                logger.info(f"Starting concurrent download: {book_id}")

                future = executor.submit(_process_single_download, book_id, cancel_flag)
                active_futures[future] = book_id
                started_download = True

            if started_download:
                # Immediately loop to check for additional available slots or completions
                continue

            if not active_futures:
                # No active work—wait for new jobs to arrive
                wait_timeout = 0.1 if stop_event else None
                book_queue.wait_for_item(timeout=wait_timeout)
                continue

            wait_timeout = 0.1 if stop_event else None
            done, _ = wait(
                active_futures.keys(),
                timeout=wait_timeout,
                return_when=FIRST_COMPLETED,
            )

            if not done:
                # Timed out waiting (likely because we're checking for shutdown)
                continue

            for future in done:
                book_id = active_futures.pop(future, None)
                if book_id is None:
                    continue
                try:
                    future.result()
                except Exception as e:
                    logger.error_trace(f"Future exception for {book_id}: {e}")

def _is_truthy(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


disable_coordinator = _is_truthy(os.getenv("DISABLE_DOWNLOAD_COORDINATOR", "false"))

if not disable_coordinator:
    # Start concurrent download coordinator
    download_coordinator_thread = threading.Thread(
        target=concurrent_download_loop,
        daemon=True,
        name="DownloadCoordinator",
    )
    download_coordinator_thread.start()

    logger.info(
        f"Download system initialized with {MAX_CONCURRENT_DOWNLOADS} concurrent workers"
    )
else:
    download_coordinator_thread = None
    logger.info("Download coordinator disabled by configuration")
