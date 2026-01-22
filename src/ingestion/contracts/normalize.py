
from __future__ import annotations
from typing import Protocol, Mapping, Any
from ingestion.contracts.tick import IngestionTick, Domain

class Normalizer(Protocol):
    """
    Normalizer contract.

    A Normalizer converts raw source payloads into a normalized IngestionTick.

    Responsibilities:
        - validate and normalize raw payload schema
        - map source-specific fields into canonical fields
        - construct IngestionTick with correct semantics

    It MUST:
        - be pure (no side effects)
        - not access runtime state
        - not cache or align data
        - not infer strategy time
        - not import quant_engine.*

    It MUST NOT:
        - perform windowing or aggregation
        - enrich data with derived quantities
        - perform lookahead fixes
    """

    def normalize(
        self,
        *,
        raw: Mapping[str, Any],
    ) -> IngestionTick:
        """
        Normalize a raw payload into an IngestionTick.

        Parameters
        ----------
        raw:
            Raw payload from source (REST / WS / file).
        symbol:
            Instrument symbol.
        domain:
            Target data domain.

        Returns
        -------
        IngestionTick
            A fully normalized tick ready to be emitted by ingestion.
        """
        ...