

from __future__ import annotations

from typing import Awaitable, Protocol, Iterable, AsyncIterable, Mapping, Any, Sequence


Raw = Mapping[str, Any]


class Source(Protocol):
    """
    Synchronous ingestion source contract.

    A Source represents a raw data producer inside the ingestion layer.

    Responsibilities:
        - fetch / read raw data from an external system
        - yield raw payloads one by one

    It MUST:
        - be ingestion-only (no runtime semantics)
        - not emit Tick
        - not normalize data
        - not know about Engine / Strategy / Mode
        - not advance strategy time

    It MUST NOT:
        - align data
        - cache windows
        - import quant_engine.*
    """

    def __iter__(self) -> Sequence[Raw] | Awaitable[Sequence[Raw]]:
        ...


class AsyncSource(Protocol):
    """
    Asynchronous ingestion source contract.

    Same semantics as Source, but for async / streaming producers
    (e.g. WebSocket listeners).
    exchange adapter, vendor SDK, etc.
    """

    def __aiter__(self) -> Sequence[Raw] | Awaitable[Sequence[Raw]]:
        ...