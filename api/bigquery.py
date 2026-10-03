"""Read-only access to the marts, with a byte cap and a short-lived in-memory cache.

The pipeline refreshes the marts once per weekday, so results are cached for
`cache_ttl_seconds` (an hour by default) and then read again. Values come back
as plain JSON-friendly Python types (NUMERIC becomes float).
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from google.cloud import bigquery

from api.settings import Settings

Row = dict[str, Any]


class MartsClient:
    def __init__(
        self,
        settings: Settings,
        client: bigquery.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.settings = settings
        self._client = client
        self._clock = clock
        self._cache: OrderedDict[tuple, tuple[float, list[Row]]] = OrderedDict()
        self._lock = threading.Lock()

    @property
    def client(self) -> bigquery.Client:
        # Created on first use, so the app starts (and tests run) without credentials.
        if self._client is None:
            self._client = bigquery.Client(
                project=self.settings.gcp_project, location=self.settings.bq_location
            )
        return self._client

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[Row]:
        params = {name: tuple(v) if isinstance(v, list) else v for name, v in (params or {}).items()}
        key = (sql, tuple(sorted(params.items())))
        now = self._clock()
        with self._lock:
            cached = self._cache.get(key)
            if cached and now - cached[0] < self.settings.cache_ttl_seconds:
                self._cache.move_to_end(key)
                return cached[1]

        job_config = bigquery.QueryJobConfig(
            query_parameters=[_parameter(name, value) for name, value in params.items()],
            maximum_bytes_billed=self.settings.max_bytes_billed,
        )
        rows = [
            {column: _plain(value) for column, value in row.items()}
            for row in self.client.query(sql, job_config=job_config).result()
        ]

        with self._lock:
            self._cache[key] = (now, rows)
            self._cache.move_to_end(key)
            while len(self._cache) > self.settings.query_cache_size:
                self._cache.popitem(last=False)
        return rows


def _parameter(name: str, value: Any) -> bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter:
    if isinstance(value, tuple):
        if not value:
            raise ValueError(f"Query parameter {name!r} is an empty list")
        return bigquery.ArrayQueryParameter(name, _bq_type(name, value[0]), list(value))
    return bigquery.ScalarQueryParameter(name, _bq_type(name, value), value)


def _bq_type(name: str, value: Any) -> str:
    # bool before int: bool is a subclass of int.
    for python_type, bq_type in ((bool, "BOOL"), (int, "INT64"), (float, "FLOAT64"), (date, "DATE"), (str, "STRING")):
        if isinstance(value, python_type):
            return bq_type
    raise TypeError(f"Unsupported query parameter {name!r} of type {type(value).__name__}")


def _plain(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value
