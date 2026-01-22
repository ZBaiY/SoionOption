from __future__ import annotations

from typing import Iterable, List, Dict, Any

from quant_engine.runtime.snapshot import EngineSnapshot


class ArtifactStore:
    """
    Runtime artifact container.

    Semantics:
      - Collects EngineSnapshot objects emitted by Driver.
      - Owns no runtime control flow.
      - Owns no persistence or IO.
      - Acts as a read-only data source for analysis / reporting layers.
      - Snapshot timestamps are epoch milliseconds (int).

    Typical usage:
        store = ArtifactStore()
        for snap in driver.snapshots:
            store.ingest(snap)
    """

    def __init__(self) -> None:
        self._snapshots: List[EngineSnapshot] = []

    # -------------------------------------------------
    # Ingestion
    # -------------------------------------------------

    def ingest(self, snapshot: EngineSnapshot) -> None:
        """
        Ingest a single EngineSnapshot.
        """
        self._snapshots.append(snapshot)

    def ingest_many(self, snapshots: Iterable[EngineSnapshot]) -> None:
        """
        Ingest multiple EngineSnapshot objects.
        """
        for snap in snapshots:
            self.ingest(snap)

    # -------------------------------------------------
    # Accessors
    # -------------------------------------------------

    @property
    def snapshots(self) -> List[EngineSnapshot]:
        """
        Return all collected snapshots.
        """
        return self._snapshots

    def to_dict(self) -> List[Dict[str, Any]]:
        """
        Convert snapshots into a list of plain dictionaries.

        This is a convenience helper for notebooks / quick inspection.
        """
        out: List[Dict[str, Any]] = []
        for s in self._snapshots:
            out.append(
                {
                    "timestamp": int(s.timestamp),
                    "mode": s.mode.value,
                    "decision_score": s.decision_score,
                    "target_position": s.target_position,
                    "fills": s.fills,
                    "portfolio": s.portfolio.to_dict(),
                }
            )
        return out
