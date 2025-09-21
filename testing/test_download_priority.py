"""Tests for validating the /api/download priority parameter."""

import pytest

import app as flask_app_module
import backend


@pytest.fixture
def client():
    """Return a Flask test client for the application."""

    return flask_app_module.app.test_client()


@pytest.fixture
def queue_stub(monkeypatch):
    """Stub out backend.queue_book and capture calls."""

    calls = []

    def fake_queue(book_id, priority):
        calls.append((book_id, priority))
        return True

    monkeypatch.setattr(backend, "queue_book", fake_queue)
    monkeypatch.setattr(flask_app_module.backend, "queue_book", fake_queue)
    return calls


def test_api_download_rejects_empty_priority(client, queue_stub):
    """An empty priority value should be rejected with a 400 response."""

    response = client.get("/api/download", query_string={"id": "book-empty", "priority": ""})

    assert response.status_code == 400
    assert response.get_json() == {"error": "Invalid priority value: must be an integer."}
    assert queue_stub == []


def test_api_download_rejects_alpha_priority(client, queue_stub):
    """Non-numeric priority values should be rejected."""

    response = client.get("/api/download", query_string={"id": "book-alpha", "priority": "high"})

    assert response.status_code == 400
    assert response.get_json() == {"error": "Invalid priority value: must be an integer."}
    assert queue_stub == []


def test_api_download_allows_missing_priority(client, queue_stub):
    """Omitting the priority parameter should queue the book at default priority."""

    response = client.get("/api/download", query_string={"id": "book-default"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "queued"
    assert payload["priority"] == 0
    assert queue_stub == [("book-default", 0)]


def test_api_download_accepts_valid_priority(client, queue_stub):
    """Numeric priority values should be accepted and forwarded to the backend."""

    response = client.get("/api/download", query_string={"id": "book-priority", "priority": "5"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "queued"
    assert payload["priority"] == 5
    assert queue_stub == [("book-priority", 5)]
