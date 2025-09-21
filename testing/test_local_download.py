import pytest

import app as flask_app
import backend
import models


@pytest.fixture
def client(monkeypatch):
    queue = models.BookQueue()
    monkeypatch.setattr(models, "book_queue", queue)
    monkeypatch.setattr(backend, "book_queue", queue)

    flask_app.app.config.update(TESTING=True)
    with flask_app.app.test_client() as client:
        yield client


def test_localdownload_missing_id_returns_404(client):
    response = client.get("/api/localdownload", query_string={"id": "missing"})

    assert response.status_code == 404
    assert response.is_json
    assert response.get_json() == {"error": "File not found"}
