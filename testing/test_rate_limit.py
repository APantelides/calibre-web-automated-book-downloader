import io
import os
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest
import requests


# Ensure optional integrations are disabled during unit tests before importing the
# application modules. This prevents heavy dependencies (like the Cloudflare
# bypasser) from being imported and avoids network calls triggered from module
# import side effects.
os.environ.setdefault("USE_CF_BYPASS", "false")
os.environ.setdefault("AA_BASE_URL", "https://example.com")
os.environ.setdefault("HTTP_PROXY", "")
os.environ.setdefault("HTTPS_PROXY", "")

import downloader


class DummyResponse:
    def __init__(self, status_code, headers=None, text="", content=b"", iter_content_factory=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self._content = content or text.encode("utf-8")
        self._closed = False
        self._iter_content_factory = iter_content_factory
        self.iter_content_calls = []

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def iter_content(self, chunk_size=1):
        self.iter_content_calls.append(chunk_size)
        if self._iter_content_factory is not None:
            yield from self._iter_content_factory(chunk_size)
            return

        if not self._content:
            yield b""
            return

        yield self._content

    def close(self):
        self._closed = True


@pytest.fixture
def sleep_calls(monkeypatch):
    calls = []

    def fake_sleep(seconds):
        calls.append(seconds)

    monkeypatch.setattr(downloader.time, "sleep", fake_sleep)
    return calls


def test_html_get_page_respects_retry_after_seconds(monkeypatch, sleep_calls):
    retry_after_seconds = 2
    responses = [
        DummyResponse(429, headers={"Retry-After": str(retry_after_seconds)}),
        DummyResponse(200, text="ok"),
    ]

    def fake_get(url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    result = downloader.html_get_page("http://example.com", retry=1)

    assert result == "ok"
    assert pytest.approx(retry_after_seconds) == sleep_calls[0]
    assert len(sleep_calls) == 1


def test_html_get_page_retry_after_http_date(monkeypatch, sleep_calls):
    retry_delay = timedelta(seconds=5)
    future = datetime.now(timezone.utc) + retry_delay
    responses = [
        DummyResponse(429, headers={"Retry-After": future.strftime("%a, %d %b %Y %H:%M:%S GMT")}),
        DummyResponse(200, text="ok"),
    ]

    def fake_get(url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    result = downloader.html_get_page("http://example.com")

    assert result == "ok"
    assert pytest.approx(retry_delay.total_seconds(), abs=1.0) == sleep_calls[0]
    assert len(sleep_calls) == 1


def test_html_get_page_success_does_not_sleep(monkeypatch, sleep_calls):
    responses = [DummyResponse(200, text="ok")]

    def fake_get(url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    result = downloader.html_get_page("http://example.com")

    assert result == "ok"
    assert sleep_calls == []


def test_download_url_respects_retry_after_seconds(monkeypatch, sleep_calls, tmp_path):
    retry_after_seconds = 3
    body = b"payload"
    responses = [
        DummyResponse(429, headers={"Retry-After": str(retry_after_seconds)}),
        DummyResponse(200, headers={"content-length": str(len(body))}, content=body),
    ]

    def fake_get(url, stream=False, **kwargs):
        assert stream is True
        return responses.pop(0)

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    destination = tmp_path / "file.bin"
    result = downloader.download_url("http://example.com/file", destination)

    assert result is True
    assert destination.read_bytes() == body
    assert len(responses) == 0
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(retry_after_seconds)


def test_download_url_retry_after_http_date(monkeypatch, sleep_calls, tmp_path):
    retry_delay = timedelta(seconds=7)
    future = datetime.now(timezone.utc) + retry_delay
    body = b"content"
    responses = [
        DummyResponse(429, headers={"Retry-After": future.strftime("%a, %d %b %Y %H:%M:%S GMT")}),
        DummyResponse(200, headers={"content-length": str(len(body))}, content=body),
    ]

    def fake_get(url, stream=False, **kwargs):
        assert stream is True
        return responses.pop(0)

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    destination = tmp_path / "file.bin"
    result = downloader.download_url("http://example.com/file", destination)

    assert result is True
    assert destination.read_bytes() == body
    assert len(responses) == 0
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(retry_delay.total_seconds(), abs=1.0)


def test_download_url_streams_large_chunks(monkeypatch, tmp_path):
    body = b"A" * (70 * 1024)

    def chunk_generator(chunk_size):
        for index in range(0, len(body), chunk_size):
            yield body[index : index + chunk_size]

    response = DummyResponse(
        200,
        headers={"content-length": str(len(body))},
        content=body,
        iter_content_factory=chunk_generator,
    )

    def fake_get(url, stream=False, **kwargs):
        assert stream is True
        return response

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    destination = tmp_path / "large.bin"
    progress_updates = []

    result = downloader.download_url(
        "http://example.com/large",
        destination,
        progress_callback=progress_updates.append,
    )

    assert result is True
    assert destination.read_bytes() == body
    assert response.iter_content_calls
    assert response.iter_content_calls[0] >= downloader.DOWNLOAD_CHUNK_SIZE
    assert progress_updates[0] == pytest.approx(0.0)
    assert progress_updates[-1] == pytest.approx(100.0)


def test_download_url_cancellation_cleans_up(monkeypatch, tmp_path):
    body = b"B" * (128 * 1024)
    cancel_flag = Event()

    def chunk_generator(chunk_size):
        yield body[:chunk_size]
        cancel_flag.set()
        yield body[chunk_size : 2 * chunk_size]

    response = DummyResponse(
        200,
        headers={"content-length": str(len(body))},
        content=body,
        iter_content_factory=chunk_generator,
    )

    def fake_get(url, stream=False, **kwargs):
        assert stream is True
        return response

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    destination = tmp_path / "cancel.bin"
    progress_updates = []

    result = downloader.download_url(
        "http://example.com/cancel",
        destination,
        progress_callback=progress_updates.append,
        cancel_flag=cancel_flag,
    )

    assert result is False
    assert cancel_flag.is_set()
    assert not destination.exists()
    assert progress_updates
    assert progress_updates[0] == pytest.approx(0.0)
    assert all(update < 100.0 for update in progress_updates)


def test_download_url_writes_to_file_handle(monkeypatch):
    body = b"hello world"
    response = DummyResponse(
        200,
        headers={"content-length": str(len(body))},
        content=body,
    )

    def fake_get(url, stream=False, **kwargs):
        assert stream is True
        return response

    monkeypatch.setattr(downloader.requests, "get", fake_get)

    buffer = io.BytesIO()

    result = downloader.download_url("http://example.com/file", buffer)

    assert result is True
    assert buffer.getvalue() == body
