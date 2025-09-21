import threading
import time

import pytest

import backend
import models
from models import BookInfo, QueueStatus


@pytest.fixture
def isolated_queue(monkeypatch):
    queue = models.BookQueue()
    monkeypatch.setattr(models, "book_queue", queue)
    monkeypatch.setattr(backend, "book_queue", queue)
    return queue


def _wait_for_drained(queue: models.BookQueue) -> None:
    for _ in range(200):
        if not queue.get_active_downloads():
            return
        time.sleep(0.01)
    raise AssertionError("active downloads did not drain in time")


def test_concurrent_download_loop_schedules_next_job(monkeypatch, isolated_queue):
    monkeypatch.setattr(backend, "MAX_CONCURRENT_DOWNLOADS", 2)

    job_ids = [f"book-{idx}" for idx in range(3)]
    start_events = {job_id: threading.Event() for job_id in job_ids}
    release_events = {job_id: threading.Event() for job_id in job_ids}
    start_times: dict[str, float] = {}

    def fake_process(book_id: str, cancel_flag: threading.Event) -> None:
        isolated_queue.update_status(book_id, QueueStatus.DOWNLOADING)
        start_times[book_id] = time.monotonic()
        start_events[book_id].set()
        release_events[book_id].wait(timeout=5)
        if cancel_flag.is_set():
            isolated_queue.update_status(book_id, QueueStatus.CANCELLED)
        else:
            isolated_queue.update_status(book_id, QueueStatus.AVAILABLE)

    monkeypatch.setattr(backend, "_process_single_download", fake_process)

    stop_event = threading.Event()
    coordinator = threading.Thread(
        target=backend.concurrent_download_loop,
        kwargs={"stop_event": stop_event},
        daemon=True,
    )
    coordinator.start()

    for idx, job_id in enumerate(job_ids):
        info = BookInfo(id=job_id, title=f"title-{idx}", format="epub")
        isolated_queue.add(job_id, info, priority=idx)

    assert start_events[job_ids[0]].wait(1.0)
    assert start_events[job_ids[1]].wait(1.0)
    assert not start_events[job_ids[2]].is_set()

    release_time = time.monotonic()
    release_events[job_ids[0]].set()

    assert start_events[job_ids[2]].wait(1.0)
    assert start_times[job_ids[2]] - release_time < 1.0

    release_events[job_ids[1]].set()
    release_events[job_ids[2]].set()

    _wait_for_drained(isolated_queue)

    stop_event.set()
    coordinator.join(timeout=2)
    assert not coordinator.is_alive()


def test_cancelled_queue_item_removed_before_processing(monkeypatch, isolated_queue):
    monkeypatch.setattr(backend, "MAX_CONCURRENT_DOWNLOADS", 1)

    cancelled_id = "cancelled"
    remaining_id = "remaining"

    call_event = threading.Event()
    download_calls: list[str] = []

    def fake_download(book_id: str, cancel_flag: threading.Event) -> None:
        download_calls.append(book_id)
        call_event.set()
        return None

    monkeypatch.setattr(backend, "_download_book_with_cancellation", fake_download)

    cancelled_info = BookInfo(id=cancelled_id, title="cancel", format="epub")
    remaining_info = BookInfo(id=remaining_id, title="keep", format="epub")

    isolated_queue.add(cancelled_id, cancelled_info, priority=0)
    isolated_queue.add(remaining_id, remaining_info, priority=1)
    assert isolated_queue.cancel_download(cancelled_id)

    assert backend.clear_completed() == 1

    stop_event = threading.Event()
    coordinator = threading.Thread(
        target=backend.concurrent_download_loop,
        kwargs={"stop_event": stop_event},
        daemon=True,
    )
    coordinator.start()

    assert call_event.wait(1.0)
    _wait_for_drained(isolated_queue)

    stop_event.set()
    coordinator.join(timeout=2)
    assert not coordinator.is_alive()

    assert cancelled_id not in download_calls
    assert remaining_id in download_calls


def test_concurrent_download_loop_handles_future_errors(monkeypatch, isolated_queue):
    monkeypatch.setattr(backend, "MAX_CONCURRENT_DOWNLOADS", 1)

    job_ids = ["fail", "ok"]
    start_events = {job_id: threading.Event() for job_id in job_ids}
    completion_events = {job_id: threading.Event() for job_id in job_ids}
    start_times: dict[str, float] = {}

    def fake_process(book_id: str, cancel_flag: threading.Event) -> None:
        isolated_queue.update_status(book_id, QueueStatus.DOWNLOADING)
        start_times[book_id] = time.monotonic()
        start_events[book_id].set()
        if book_id == "fail":
            isolated_queue.update_status(book_id, QueueStatus.ERROR)
            completion_events[book_id].set()
            raise RuntimeError("boom")
        isolated_queue.update_status(book_id, QueueStatus.AVAILABLE)
        completion_events[book_id].set()

    monkeypatch.setattr(backend, "_process_single_download", fake_process)

    stop_event = threading.Event()
    coordinator = threading.Thread(
        target=backend.concurrent_download_loop,
        kwargs={"stop_event": stop_event},
        daemon=True,
    )
    coordinator.start()

    failing_info = BookInfo(id="fail", title="bad", format="epub")
    ok_info = BookInfo(id="ok", title="good", format="epub")
    isolated_queue.add("fail", failing_info, priority=0)
    isolated_queue.add("ok", ok_info, priority=1)

    assert start_events["fail"].wait(1.0)
    assert completion_events["fail"].wait(1.0)
    failure_end = time.monotonic()

    assert start_events["ok"].wait(1.0)
    assert start_times["ok"] - failure_end < 1.0
    assert completion_events["ok"].wait(1.0)

    _wait_for_drained(isolated_queue)

    stop_event.set()
    coordinator.join(timeout=2)
    assert not coordinator.is_alive()


def test_clear_completed_prevents_stale_job_processing(monkeypatch, isolated_queue):
    monkeypatch.setattr(backend, "MAX_CONCURRENT_DOWNLOADS", 1)

    retrieved_event = threading.Event()
    release_event = threading.Event()
    download_called = threading.Event()

    original_get = isolated_queue._queue.get
    first_call = True

    def blocking_get(*args, **kwargs):
        nonlocal first_call
        item = original_get(*args, **kwargs)
        if first_call:
            first_call = False
            retrieved_event.set()
            if not release_event.wait(timeout=1.0):
                raise TimeoutError("release_event was not set")
        return item

    monkeypatch.setattr(isolated_queue._queue, "get", blocking_get)

    def fake_download(book_id: str, cancel_flag: threading.Event) -> None:
        download_called.set()
        return None

    monkeypatch.setattr(
        backend,
        "_download_book_with_cancellation",
        fake_download,
    )

    book_id = "stale-job"
    info = BookInfo(id=book_id, title="stale", format="epub")
    isolated_queue.add(book_id, info, priority=0)
    assert isolated_queue.cancel_download(book_id)

    stop_event = threading.Event()
    coordinator = threading.Thread(
        target=backend.concurrent_download_loop,
        kwargs={"stop_event": stop_event},
        daemon=True,
    )
    coordinator.start()

    try:
        try:
            assert retrieved_event.wait(1.0)
            assert backend.clear_completed() == 1
        finally:
            release_event.set()

        assert not download_called.wait(0.3)
        assert isolated_queue.get_active_downloads() == []
    finally:
        stop_event.set()
        coordinator.join(timeout=2)
        assert not coordinator.is_alive()
