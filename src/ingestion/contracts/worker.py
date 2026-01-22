from __future__ import annotations
from typing import Protocol
from collections.abc import Awaitable, Callable
from ingestion.contracts.tick import IngestionTick


class IngestWorker(Protocol):
    """
    Ingestion worker contract.

    An IngestWorker is responsible ONLY for:
        - fetching / listening to external data sources
        - normalizing raw payloads
        - emitting IngestionTick objects

    It MUST NOT:
        - import quant_engine.*
        - know about Engine / Strategy / Mode
        - align data
        - cache windows
        - advance time
        - block on runtime semantics

    Lifecycle:
        world -> worker.run(emit_tick)
    """

    async def run(
        self,
        emit: Callable[[IngestionTick], Awaitable[None] | None],
    ) -> None:
        """
        Start the ingestion loop.

        Parameters
        ----------
        emit:
            Callback used to emit normalized IngestionTick objects downstream.
            May be synchronous (returns None) or asynchronous (returns an awaitable). Workers must treat it as non-blocking from their perspective and await it only if it returns an awaitable.
        """
        ...