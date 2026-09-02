"""
REST API reader and writer for SimpleETL.

Requires the ``requests`` optional dependency::

    pip install simpleetl[rest]
    # or
    pip install requests

Supports common authentication strategies (Bearer token, API key, HTTP Basic)
and three pagination modes: offset/limit, cursor, and RFC 5988 Link-header.
"""

import io
import time
from typing import Any, Dict, Iterator, Optional

import pandas as pd

from .base import DataReader, DataWriter
from simpleetl.core.retry import RetryCircuitBreaker


def _require_requests():
    try:
        import requests  # noqa: F401
    except ImportError:
        raise ImportError(
            "requests is required for REST API format support. "
            "Install it with: pip install simpleetl[rest]"
        )


def _parse_link_header(header: str) -> Dict[str, str]:
    """Parse an RFC 5988 Link header into a rel→url mapping."""
    links: Dict[str, str] = {}
    for part in header.split(","):
        part = part.strip()
        segments = [s.strip() for s in part.split(";")]
        if not segments:
            continue
        url = segments[0].strip("<>")
        rel = None
        for seg in segments[1:]:
            if seg.startswith("rel="):
                rel = seg[4:].strip('"')
        if rel:
            links[rel] = url
    return links


class RestApiReader(DataReader):
    """Read data from a REST API endpoint.

    Args:
        auth_type: Authentication strategy.  One of ``"none"`` (default),
            ``"bearer"``, ``"api_key"``, or ``"basic"``.
        auth_token: Bearer token (required when *auth_type* is ``"bearer"``).
        api_key: API key value (required when *auth_type* is ``"api_key"``).
        api_key_header: Header name used to send the API key
            (default: ``"X-API-Key"``).  Ignored when *api_key_param* is set.
        api_key_param: Query-string parameter name for the API key.  When set
            the key is sent as a query parameter instead of a header.
        username: HTTP Basic username.
        password: HTTP Basic password.
        headers: Extra HTTP headers added to every request.
        timeout: Request timeout in seconds (default: 30).
        verify_ssl: Verify SSL certificates (default: True).
        requests_per_second: Optional rate limit.  Sleeps between requests to
            stay within the given rate.

    Example::

        reader = RestApiReader(auth_type="bearer", auth_token="tok_xyz")
        df = reader.read(
            "https://api.example.com/orders",
            data_key="orders",
            pagination="offset",
            page_size=200,
        )
    """

    def __init__(
        self,
        *,
        auth_type: str = "none",
        auth_token: Optional[str] = None,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        api_key_param: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        verify_ssl: bool = True,
        requests_per_second: Optional[float] = None,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        breaker_threshold: int = 5,
    ) -> None:
        valid_auth = {"none", "bearer", "api_key", "basic"}
        if auth_type not in valid_auth:
            raise ValueError(
                f"Invalid auth_type '{auth_type}'. Must be one of {sorted(valid_auth)}."
            )
        self.auth_type = auth_type
        self.auth_token = auth_token
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.api_key_param = api_key_param
        self.username = username
        self.password = password
        self.extra_headers: Dict[str, str] = headers or {}
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.requests_per_second = requests_per_second
        self._last_request_time: Optional[float] = None
        self.retry_circuit = RetryCircuitBreaker(
            max_retries=max_retries,
            backoff_base=backoff_base,
            breaker_threshold=breaker_threshold,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_session(self) -> Any:
        _require_requests()
        import requests  # noqa: PLC0415

        session = requests.Session()
        if self.auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.auth_type == "api_key" and not self.api_key_param:
            session.headers[self.api_key_header] = self.api_key or ""
        elif self.auth_type == "basic":
            session.auth = (self.username or "", self.password or "")
        session.headers.update(self.extra_headers)
        return session

    def _rate_limit(self) -> None:
        if not self.requests_per_second:
            return
        now = time.monotonic()
        if self._last_request_time is not None:
            wait = (1.0 / self.requests_per_second) - (now - self._last_request_time)
            if wait > 0:
                time.sleep(wait)
        self._last_request_time = time.monotonic()

    def _build_params(
        self,
        base: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = dict(base or {})
        if extra:
            result.update(extra)
        if self.auth_type == "api_key" and self.api_key_param:
            result[self.api_key_param] = self.api_key or ""
        return result

    def _parse_response(
        self,
        response: Any,
        *,
        data_key: Optional[str] = None,
    ) -> pd.DataFrame:
        """Convert a response to a DataFrame."""
        content_type = response.headers.get("Content-Type", "")
        text = response.text.strip()
        if "application/json" in content_type or text.startswith(("{", "[")):
            data = response.json()
            if data_key and isinstance(data, dict):
                data = data.get(data_key, [])
            if isinstance(data, dict):
                data = [data]
            return pd.DataFrame(data)
        # Fall back to CSV
        return pd.read_csv(io.StringIO(text))

    def _do_request(
        self,
        session: Any,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        self._rate_limit()

        def _request():
            response = session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            return response

        return self.retry_circuit.call(_request)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        source: str,
        *,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data_key: Optional[str] = None,
        pagination: str = "none",
        page_size: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        cursor_param: str = "cursor",
        cursor_response_key: str = "next_cursor",
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch all pages and return a single concatenated DataFrame.

        Args:
            source: API endpoint URL.
            method: HTTP method (default: ``"GET"``).
            params: Base query-string parameters added to every request.
            json_body: JSON body for POST/PUT requests.
            data_key: Top-level JSON key that contains the record list.
            pagination: Strategy — ``"none"`` (single request), ``"offset"``,
                ``"cursor"``, or ``"link"`` (RFC 5988 Link header).
            page_size: Records per page (used with ``"offset"`` and
                ``"cursor"`` pagination).
            offset_param: Query-param name for the page offset.
            limit_param: Query-param name for the page size.
            cursor_param: Query-param name for the page cursor.
            cursor_response_key: JSON response key containing the next cursor.

        Returns:
            All fetched records concatenated into a single DataFrame.
        """
        frames = list(
            self.read_chunks(
                source,
                method=method,
                params=params,
                json_body=json_body,
                data_key=data_key,
                pagination=pagination,
                page_size=page_size,
                offset_param=offset_param,
                limit_param=limit_param,
                cursor_param=cursor_param,
                cursor_response_key=cursor_response_key,
            )
        )
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def read_chunks(
        self,
        source: str,
        chunk_size: int = 100,
        max_buffer_mb: float = 0,
        *,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data_key: Optional[str] = None,
        pagination: str = "none",
        page_size: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        cursor_param: str = "cursor",
        cursor_response_key: str = "next_cursor",
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """Iterate over API pages, yielding one DataFrame per page.

        Args:
            source: API endpoint URL.
            chunk_size: Alias for *page_size* (kept for interface consistency).
            For other parameters see :meth:`read`.

        Yields:
            One DataFrame per API page.

        Raises:
            ImportError: If ``requests`` is not installed.
            requests.HTTPError: If any request returns a non-2xx status.
        """
        valid_pagination = {"none", "offset", "cursor", "link"}
        if pagination not in valid_pagination:
            raise ValueError(
                f"Invalid pagination '{pagination}'. "
                f"Must be one of {sorted(valid_pagination)}."
            )

        effective_page_size = chunk_size if chunk_size != 100 else page_size
        session = self._get_session()

        if pagination == "none":
            resp = self._do_request(
                session,
                method,
                source,
                params=self._build_params(params),
                json_body=json_body,
            )
            yield self._parse_response(resp, data_key=data_key)

        elif pagination == "offset":
            offset = 0
            while True:
                offset_extra: Dict[str, Any] = {
                    offset_param: offset,
                    limit_param: effective_page_size,
                }
                resp = self._do_request(
                    session,
                    method,
                    source,
                    params=self._build_params(params, offset_extra),
                    json_body=json_body,
                )
                df = self._parse_response(resp, data_key=data_key)
                if df.empty:
                    break
                yield df
                if len(df) < effective_page_size:
                    break
                offset += effective_page_size

        elif pagination == "cursor":
            cursor: Optional[str] = None
            while True:
                cursor_extra: Dict[str, Any] = {limit_param: effective_page_size}
                if cursor:
                    cursor_extra[cursor_param] = cursor
                resp = self._do_request(
                    session,
                    method,
                    source,
                    params=self._build_params(params, cursor_extra),
                    json_body=json_body,
                )
                raw = resp.json()
                data = raw
                if data_key and isinstance(raw, dict):
                    data = raw.get(data_key, [])
                if not data:
                    break
                yield pd.DataFrame(data if isinstance(data, list) else [data])
                cursor = None
                if isinstance(raw, dict):
                    cursor = raw.get(cursor_response_key)
                if not cursor:
                    break

        elif pagination == "link":
            current_url: Optional[str] = source
            first = True
            while current_url:
                req_params = self._build_params(params) if first else None
                resp = self._do_request(
                    session,
                    method,
                    current_url,
                    params=req_params,
                    json_body=json_body if first else None,
                )
                first = False
                yield self._parse_response(resp, data_key=data_key)
                link_header = resp.headers.get("Link", "")
                links = _parse_link_header(link_header)
                current_url = links.get("next")


class RestApiWriter(DataWriter):
    """POST (or PUT) DataFrame records to a REST API endpoint.

    Args:
        auth_type: Authentication strategy (same options as
            :class:`RestApiReader`).
        auth_token: Bearer token.
        api_key: API key value.
        api_key_header: Header name for the API key.
        username: HTTP Basic username.
        password: HTTP Basic password.
        headers: Extra HTTP headers.
        timeout: Request timeout in seconds (default: 30).
        verify_ssl: Verify SSL certificates (default: True).

    Example::

        writer = RestApiWriter(auth_type="bearer", auth_token="tok_xyz")
        writer.write(df, "https://api.example.com/ingest", batch_size=50)
    """

    def __init__(
        self,
        *,
        auth_type: str = "none",
        auth_token: Optional[str] = None,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        username: Optional[str] = None,
        password: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        verify_ssl: bool = True,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        breaker_threshold: int = 5,
    ) -> None:
        self.auth_type = auth_type
        self.auth_token = auth_token
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.username = username
        self.password = password
        self.extra_headers: Dict[str, str] = headers or {}
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.retry_circuit = RetryCircuitBreaker(
            max_retries=max_retries,
            backoff_base=backoff_base,
            breaker_threshold=breaker_threshold,
        )

    def _get_session(self) -> Any:
        _require_requests()
        import requests  # noqa: PLC0415

        session = requests.Session()
        session.headers["Content-Type"] = "application/json"
        if self.auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.auth_type == "api_key":
            session.headers[self.api_key_header] = self.api_key or ""
        elif self.auth_type == "basic":
            session.auth = (self.username or "", self.password or "")
        session.headers.update(self.extra_headers)
        return session

    def write(
        self,
        data: pd.DataFrame,
        destination: str,
        *,
        method: str = "POST",
        batch_size: int = 100,
        record_key: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Send *data* records to the API endpoint.

        Args:
            data: DataFrame whose rows are serialised as JSON objects.
            destination: API endpoint URL.
            method: HTTP method (default: ``"POST"``).
            batch_size: Number of records per request (default: 100).
            record_key: When set, wraps each batch under this JSON key,
                e.g. ``{"records": [...]}`` instead of ``[...]``.

        Raises:
            ImportError: If ``requests`` is not installed.
            requests.HTTPError: If any request returns a non-2xx status.
        """
        session = self._get_session()
        records = data.to_dict(orient="records")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            payload: Any = {record_key: batch} if record_key else batch

            def _post():
                resp = session.request(
                    method,
                    destination,
                    json=payload,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                resp.raise_for_status()
                return resp

            self.retry_circuit.call(_post)


# Convenience type alias for external imports
__all__ = ["RestApiReader", "RestApiWriter"]
