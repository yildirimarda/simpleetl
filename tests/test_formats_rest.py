"""Tests for the REST API reader/writer (Phase 8.4).

All HTTP calls are mocked via unittest.mock so no network is required.
"""

import json
import sys
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from simpleetl.formats.rest_api import RestApiReader, RestApiWriter, _parse_link_header


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(
    body: Any,
    *,
    status_code: int = 200,
    content_type: str = "application/json",
    headers: dict | None = None,
) -> MagicMock:
    """Build a mock requests.Response-like object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"Content-Type": content_type, **(headers or {})}
    resp.text = json.dumps(body) if isinstance(body, (dict, list)) else body
    resp.json = Mock(return_value=body)
    resp.raise_for_status = Mock()
    return resp


# ---------------------------------------------------------------------------
# _parse_link_header
# ---------------------------------------------------------------------------


class TestParseLinkHeader:
    def test_next_rel(self):
        header = '<https://api.example.com/items?page=2>; rel="next"'
        links = _parse_link_header(header)
        assert links["next"] == "https://api.example.com/items?page=2"

    def test_multiple_rels(self):
        header = (
            '<https://api.example.com/items?page=2>; rel="next", '
            '<https://api.example.com/items?page=1>; rel="prev"'
        )
        links = _parse_link_header(header)
        assert "next" in links
        assert "prev" in links

    def test_empty_header(self):
        assert _parse_link_header("") == {}

    def test_no_rel(self):
        header = "<https://api.example.com/items>"
        assert _parse_link_header(header) == {}


# ---------------------------------------------------------------------------
# RestApiReader — initialisation
# ---------------------------------------------------------------------------


class TestRestApiReaderInit:
    def test_default_auth_type(self):
        reader = RestApiReader()
        assert reader.auth_type == "none"

    def test_invalid_auth_type_raises(self):
        with pytest.raises(ValueError, match="Invalid auth_type"):
            RestApiReader(auth_type="magic")

    def test_all_valid_auth_types(self):
        for auth in ("none", "bearer", "api_key", "basic"):
            RestApiReader(auth_type=auth)  # should not raise


# ---------------------------------------------------------------------------
# RestApiReader — no-pagination
# ---------------------------------------------------------------------------


class TestRestApiReaderNoPagination:
    def _reader(self) -> RestApiReader:
        return RestApiReader()

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_read_returns_dataframe(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response([{"a": 1}, {"a": 2}])
        mock_get_session.return_value = session

        df = self._reader().read("https://api.example.com/data")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_read_extracts_data_key(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response(
            {"items": [{"x": 1}, {"x": 2}, {"x": 3}]}
        )
        mock_get_session.return_value = session

        df = self._reader().read("https://api.example.com/data", data_key="items")
        assert len(df) == 3
        assert "x" in df.columns

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_read_single_dict_wrapped(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response({"id": 1, "name": "Alice"})
        mock_get_session.return_value = session

        df = self._reader().read("https://api.example.com/user/1")
        assert len(df) == 1
        assert df["name"].iloc[0] == "Alice"


# ---------------------------------------------------------------------------
# RestApiReader — offset pagination
# ---------------------------------------------------------------------------


class TestRestApiReaderOffsetPagination:
    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_offset_stops_on_empty(self, mock_get_session):
        session = MagicMock()
        # page 1: 2 records, page 2: empty
        session.request.side_effect = [
            _make_response([{"n": 1}, {"n": 2}]),
            _make_response([]),
        ]
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="offset",
            page_size=2,
        )
        assert len(df) == 2

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_offset_stops_when_partial_page(self, mock_get_session):
        session = MagicMock()
        session.request.side_effect = [
            _make_response([{"n": i} for i in range(3)]),
            _make_response([{"n": 99}]),
        ]
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="offset",
            page_size=3,
        )
        assert len(df) == 4

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_offset_passes_params(self, mock_get_session):
        session = MagicMock()
        session.request.side_effect = [
            _make_response([{"n": 1}]),  # partial → stop
        ]
        mock_get_session.return_value = session

        RestApiReader().read(
            "https://api.example.com/data",
            pagination="offset",
            page_size=10,
            offset_param="start",
            limit_param="count",
        )
        call_kwargs = session.request.call_args
        params_sent = call_kwargs[1]["params"]
        assert "start" in params_sent
        assert "count" in params_sent


# ---------------------------------------------------------------------------
# RestApiReader — cursor pagination
# ---------------------------------------------------------------------------


class TestRestApiReaderCursorPagination:
    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_cursor_follows_next_cursor(self, mock_get_session):
        session = MagicMock()
        session.request.side_effect = [
            _make_response(
                {"data": [{"id": 1}], "next_cursor": "abc123"},
                content_type="application/json",
            ),
            _make_response(
                {"data": [{"id": 2}], "next_cursor": None},
                content_type="application/json",
            ),
        ]
        session.request.return_value.__iter__ = Mock(return_value=iter([]))
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="cursor",
            data_key="data",
        )
        assert len(df) == 2

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_cursor_stops_when_no_cursor(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response(
            {"items": [{"v": 1}]}  # no next_cursor key
        )
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="cursor",
            data_key="items",
        )
        assert len(df) == 1


# ---------------------------------------------------------------------------
# RestApiReader — link pagination
# ---------------------------------------------------------------------------


class TestRestApiReaderLinkPagination:
    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_link_follows_next(self, mock_get_session):
        session = MagicMock()
        page2_url = "https://api.example.com/data?page=2"
        session.request.side_effect = [
            _make_response(
                [{"x": 1}],
                headers={"Link": f'<{page2_url}>; rel="next"'},
            ),
            _make_response([{"x": 2}]),
        ]
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="link",
        )
        assert len(df) == 2

    @patch("simpleetl.formats.rest_api.RestApiReader._get_session")
    def test_link_stops_when_no_next(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response([{"x": 1}])
        mock_get_session.return_value = session

        df = RestApiReader().read(
            "https://api.example.com/data",
            pagination="link",
        )
        assert len(df) == 1


# ---------------------------------------------------------------------------
# RestApiReader — authentication
# ---------------------------------------------------------------------------


class TestRestApiReaderAuth:
    def test_bearer_auth_sets_header(self):
        mock_requests = MagicMock()
        mock_session = MagicMock()
        mock_session.headers = {}
        mock_requests.Session.return_value = mock_session

        with patch.dict(sys.modules, {"requests": mock_requests}):
            reader = RestApiReader(auth_type="bearer", auth_token="my-token")
            reader._get_session()

        assert mock_session.headers.get("Authorization") == "Bearer my-token"

    def test_api_key_sets_header(self):
        mock_requests = MagicMock()
        mock_session = MagicMock()
        mock_session.headers = {}
        mock_requests.Session.return_value = mock_session

        with patch.dict(sys.modules, {"requests": mock_requests}):
            reader = RestApiReader(auth_type="api_key", api_key="secret")
            reader._get_session()

        assert mock_session.headers.get("X-API-Key") == "secret"

    def test_invalid_pagination_raises(self):
        reader = RestApiReader()
        with patch.object(reader, "_get_session") as mock_sess:
            mock_sess.return_value = MagicMock()
            with pytest.raises(ValueError, match="Invalid pagination"):
                list(reader.read_chunks("http://x.com", pagination="unknown"))

    def test_missing_requests_raises_import_error(self):
        original = sys.modules.get("requests")
        sys.modules["requests"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ImportError, match="requests"):
                from simpleetl.formats.rest_api import _require_requests

                _require_requests()
        finally:
            if original is not None:
                sys.modules["requests"] = original
            else:
                del sys.modules["requests"]


# ---------------------------------------------------------------------------
# RestApiWriter
# ---------------------------------------------------------------------------


class TestRestApiWriter:
    @patch("simpleetl.formats.rest_api.RestApiWriter._get_session")
    def test_write_posts_records(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response({"ok": True})
        mock_get_session.return_value = session

        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        RestApiWriter().write(df, "https://api.example.com/ingest")
        assert session.request.called

    @patch("simpleetl.formats.rest_api.RestApiWriter._get_session")
    def test_write_batches_correctly(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response({"ok": True})
        mock_get_session.return_value = session

        df = pd.DataFrame({"id": range(25)})
        RestApiWriter().write(df, "https://api.example.com/ingest", batch_size=10)
        # 25 records at batch_size=10 → 3 requests
        assert session.request.call_count == 3

    @patch("simpleetl.formats.rest_api.RestApiWriter._get_session")
    def test_write_with_record_key(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response({"ok": True})
        mock_get_session.return_value = session

        df = pd.DataFrame({"x": [1]})
        RestApiWriter().write(
            df, "https://api.example.com/ingest", record_key="records"
        )
        payload = session.request.call_args[1]["json"]
        assert isinstance(payload, dict)
        assert "records" in payload

    @patch("simpleetl.formats.rest_api.RestApiWriter._get_session")
    def test_write_empty_df(self, mock_get_session):
        session = MagicMock()
        session.request.return_value = _make_response([])
        mock_get_session.return_value = session

        df = pd.DataFrame({"x": pd.Series([], dtype="int64")})
        RestApiWriter().write(df, "https://api.example.com/ingest")
        # 0 records → 0 batches → 0 requests
        assert session.request.call_count == 0
