"""Tests for queue status serialization."""

import json

import pytest

import app as flask_app_module
import backend
import models
from models import BookInfo, QueueStatus


@pytest.fixture
def isolated_queue(monkeypatch):
    """Provide a fresh queue instance isolated from global state."""

    queue = models.BookQueue()
    monkeypatch.setattr(models, "book_queue", queue)
    monkeypatch.setattr(backend, "book_queue", queue)
    monkeypatch.setattr(flask_app_module.backend, "book_queue", queue)
    return queue


def test_queue_status_returns_serializable_dict(isolated_queue, tmp_path):
    """queue_status should return only JSON-serializable primitives."""

    available_book_id = "book-available"
    available_info = BookInfo(id=available_book_id, title="With File", format="epub")
    isolated_queue.add(available_book_id, available_info, priority=1)

    download_path = tmp_path / "present.epub"
    download_path.write_text("content")
    isolated_queue.update_download_path(available_book_id, str(download_path))
    isolated_queue.update_status(available_book_id, QueueStatus.AVAILABLE)

    missing_book_id = "book-missing"
    missing_info = BookInfo(id=missing_book_id, title="No File", format="pdf")
    isolated_queue.add(missing_book_id, missing_info, priority=2)
    missing_path = tmp_path / "missing.epub"
    isolated_queue.update_download_path(missing_book_id, str(missing_path))
    isolated_queue.update_status(missing_book_id, QueueStatus.AVAILABLE)

    result = backend.queue_status()

    expected_statuses = {status.value for status in QueueStatus}
    assert set(result) == expected_statuses

    available_books = result[QueueStatus.AVAILABLE.value]
    assert available_book_id in available_books
    available_serialized = available_books[available_book_id]
    assert isinstance(available_serialized, dict)
    assert available_serialized["download_path"] == str(download_path)

    done_books = result[QueueStatus.DONE.value]
    assert missing_book_id in done_books
    missing_serialized = done_books[missing_book_id]
    assert isinstance(missing_serialized, dict)
    assert "download_path" not in missing_serialized

    # Should be JSON serializable without raising errors
    json.dumps(result)


def test_api_status_endpoint_serializes_response(isolated_queue):
    """The /api/status endpoint should serialize queue status correctly."""

    book_id = "queued-book"
    info = BookInfo(id=book_id, title="Queued Book", format="mobi")
    isolated_queue.add(book_id, info, priority=0)

    client = flask_app_module.app.test_client()
    response = client.get("/api/status")

    assert response.status_code == 200
    payload = response.get_json()

    queued_books = payload[QueueStatus.QUEUED.value]
    assert queued_books[book_id]["title"] == "Queued Book"

    # Confirm the payload is fully JSON serializable
    json.dumps(payload)
