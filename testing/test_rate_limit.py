import os
from datetime import datetime, timedelta, timezone
from io import BytesIO

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
    def __init__(self, status_code, headers=None, text="", content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self._content = content or text.encode("utf-8")
        self._closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def iter_content(self, chunk_size=1):
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
    assert 1 in sleep_calls  # success sleep


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


def _install_dummy_tqdm(monkeypatch):
    class DummyTqdm:
        def __init__(self, *args, **kwargs):
            self.n = 0

        def update(self, amount):
            self.n += amount

        def close(self):
            pass

    monkeypatch.setattr(downloader, "tqdm", DummyTqdm)


def test_download_url_respects_retry_after_seconds(monkeypatch, sleep_calls):
    _install_dummy_tqdm(monkeypatch)

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

    result = downloader.download_url("http://example.com/file")

    assert isinstance(result, BytesIO)
    assert result.getvalue() == body
    assert len(responses) == 0
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(retry_after_seconds)


def test_download_url_retry_after_http_date(monkeypatch, sleep_calls):
    _install_dummy_tqdm(monkeypatch)

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

    result = downloader.download_url("http://example.com/file")

    assert isinstance(result, BytesIO)
    assert result.getvalue() == body
    assert len(responses) == 0
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(retry_delay.total_seconds(), abs=1.0)
