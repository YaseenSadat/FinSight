"""Run manifests: the audit record of every pipeline run.

A manifest captures the exact request, library versions, per-symbol outcome,
row accounting, quality-check results, and content hashes of the gold files a
run wrote. It is persisted after every stage, which is also how stages pass
state to each other when they run as separate Airflow tasks.
"""

from __future__ import annotations

import json
import posixpath
import secrets
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from finsight._version import __version__
from finsight.errors import RunNotFoundError
from finsight.models import FetchRequest
from finsight.quality import CheckResult
from finsight.storage import Layout, Storage


class SymbolStatus(StrEnum):
    PENDING = "pending"
    INGESTED = "ingested"
    TRANSFORMED = "transformed"
    PUBLISHED = "published"
    NO_DATA = "no_data"
    NOT_FOUND = "not_found"
    FAILED = "failed"
    REJECTED = "rejected"


#: Statuses that mean a symbol could not be delivered.
FAILURE_STATUSES = frozenset({SymbolStatus.NOT_FOUND, SymbolStatus.FAILED, SymbolStatus.REJECTED})


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


def new_run_id(now: datetime | None = None) -> str:
    """Sortable, collision-resistant run id, e.g. ``20260930T171500Z-3fa2c1``."""
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(3)}"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class SymbolResult:
    symbol: str
    status: SymbolStatus = SymbolStatus.PENDING
    message: str | None = None
    attempts: int = 0
    bronze_path: str | None = None
    bronze_rows: int | None = None
    silver_rows: int | None = None
    dropped_rows: int | None = None
    duplicate_rows: int | None = None
    gold_path: str | None = None
    gold_sha256: str | None = None
    gold_total_rows: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    first_date: str | None = None
    last_date: str | None = None
    checks: list[CheckResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        data["checks"] = [check.to_dict() for check in self.checks]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SymbolResult:
        values = dict(data)
        values["status"] = SymbolStatus(values["status"])
        values["checks"] = [CheckResult.from_dict(c) for c in values.get("checks", [])]
        return cls(**values)


@dataclass
class RunManifest:
    run_id: str
    request: FetchRequest
    status: RunStatus = RunStatus.RUNNING
    created_at: str = field(default_factory=_now)
    finished_at: str | None = None
    finsight_version: str = __version__
    provider_version: str | None = None
    engine: str | None = None
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)
    symbols: dict[str, SymbolResult] = field(default_factory=dict)

    @classmethod
    def start(cls, request: FetchRequest, run_id: str | None = None) -> RunManifest:
        manifest = cls(run_id=run_id or new_run_id(), request=request)
        manifest.symbols = {s: SymbolResult(symbol=s) for s in request.symbols}
        return manifest

    # Stage bookkeeping ------------------------------------------------------------------

    def begin_stage(self, name: str) -> None:
        self.stages[name] = {"started_at": _now(), "finished_at": None}

    def end_stage(self, name: str, **details: Any) -> None:
        stage = self.stages.setdefault(name, {"started_at": _now()})
        stage["finished_at"] = _now()
        stage.update(details)

    def symbols_with(self, *statuses: SymbolStatus) -> list[SymbolResult]:
        return [r for r in self.symbols.values() if r.status in statuses]

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for result in self.symbols.values():
            counts[result.status.value] = counts.get(result.status.value, 0) + 1
        return counts

    def finalize(self) -> RunStatus:
        """Derive the overall status from per-symbol outcomes."""
        failed = len(self.symbols_with(*FAILURE_STATUSES))
        delivered = len(self.symbols_with(SymbolStatus.PUBLISHED, SymbolStatus.NO_DATA))
        if failed == 0:
            self.status = RunStatus.SUCCEEDED
        elif delivered:
            self.status = RunStatus.PARTIAL
        else:
            self.status = RunStatus.FAILED
        self.finished_at = _now()
        return self.status

    # Serialisation ----------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "finsight_version": self.finsight_version,
            "provider_version": self.provider_version,
            "engine": self.engine,
            "request": self.request.to_dict(),
            "summary": self.counts(),
            "stages": self.stages,
            "symbols": {s: r.to_dict() for s, r in self.symbols.items()},
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunManifest:
        return cls(
            run_id=data["run_id"],
            request=FetchRequest.from_dict(data["request"]),
            status=RunStatus(data["status"]),
            created_at=data["created_at"],
            finished_at=data.get("finished_at"),
            finsight_version=data.get("finsight_version", "unknown"),
            provider_version=data.get("provider_version"),
            engine=data.get("engine"),
            stages=data.get("stages", {}),
            symbols={s: SymbolResult.from_dict(r) for s, r in data.get("symbols", {}).items()},
        )


class ManifestStore:
    """Persists manifests under ``<root>/runs/``."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.layout = Layout(storage)

    def save(self, manifest: RunManifest) -> str:
        path = self.layout.manifest_file(manifest.run_id)
        self.storage.write_text(path, manifest.to_json())
        return path

    def load(self, run_id: str) -> RunManifest:
        path = self.layout.manifest_file(run_id)
        if not self.storage.exists(path):
            raise RunNotFoundError(f"no run with id {run_id!r}")
        return RunManifest.from_dict(json.loads(self.storage.read_text(path)))

    def run_ids(self) -> list[str]:
        """All run ids, newest first."""
        paths = self.storage.glob(posixpath.join(self.layout.runs_dir(), "*.json"))
        return sorted((posixpath.basename(p).removesuffix(".json") for p in paths), reverse=True)

    def list(self, limit: int | None = None) -> list[RunManifest]:
        ids = self.run_ids()
        return [self.load(run_id) for run_id in (ids[:limit] if limit else ids)]

    def latest(self) -> RunManifest | None:
        ids = self.run_ids()
        return self.load(ids[0]) if ids else None
