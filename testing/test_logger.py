import logging

from logger import CustomLogger


def test_error_trace_respects_explicit_exc_info(monkeypatch):
    """Calling error_trace with exc_info=True should not raise and reaches Logger.error."""

    logger = CustomLogger("test-logger")

    calls = []

    def fake_error(self, msg, *args, **kwargs):
        calls.append((msg, args, kwargs))

    monkeypatch.setattr(logging.Logger, "error", fake_error)

    log_resource_usage_calls = []

    def fake_log_resource_usage(self):
        log_resource_usage_calls.append(True)

    monkeypatch.setattr(CustomLogger, "log_resource_usage", fake_log_resource_usage)

    logger.error_trace("msg", exc_info=True)

    assert log_resource_usage_calls, "log_resource_usage should be called before delegating"
    assert calls == [("msg", (), {"exc_info": True})]
