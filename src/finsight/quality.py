"""Data quality checks.

A :class:`Check` counts the rows of a frame that violate one rule. Checks with
``ERROR`` severity block publication to the gold layer; ``WARNING`` checks are
recorded in the run manifest but do not block.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

import pandas as pd


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class Check:
    """A named rule that returns the number of rows violating it."""

    name: str
    description: str
    severity: Severity
    count_failures: Callable[[pd.DataFrame], int]

    def run(self, frame: pd.DataFrame) -> CheckResult:
        failures = int(self.count_failures(frame)) if len(frame) else 0
        return CheckResult(
            name=self.name,
            description=self.description,
            severity=self.severity,
            passed=failures == 0,
            failed_rows=failures,
        )


@dataclass(frozen=True)
class CheckResult:
    name: str
    description: str
    severity: Severity
    passed: bool
    failed_rows: int

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["severity"] = self.severity.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CheckResult:
        return cls(**{**data, "severity": Severity(data["severity"])})


@dataclass(frozen=True)
class ValidationReport:
    results: tuple[CheckResult, ...]

    @property
    def passed(self) -> bool:
        """True when no ERROR-severity check failed."""
        return not self.errors

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.WARNING]

    def summary(self) -> str:
        failed = self.errors + self.warnings
        if not failed:
            return f"all {len(self.results)} checks passed"
        return "; ".join(f"{r.name} ({r.severity.value}): {r.failed_rows} rows" for r in failed)


def run_checks(frame: pd.DataFrame, checks: Iterable[Check]) -> ValidationReport:
    return ValidationReport(tuple(check.run(frame) for check in checks))


# Reusable check factories -------------------------------------------------------


def not_null(columns: Sequence[str], severity: Severity = Severity.ERROR) -> Check:
    cols = list(columns)
    return Check(
        name="not_null",
        description=f"{', '.join(cols)} must not be null",
        severity=severity,
        count_failures=lambda df: int(df[cols].isna().any(axis=1).sum()),
    )


def unique(columns: Sequence[str], severity: Severity = Severity.ERROR) -> Check:
    cols = list(columns)
    return Check(
        name="unique_key",
        description=f"({', '.join(cols)}) must be unique",
        severity=severity,
        count_failures=lambda df: int(df.duplicated(subset=cols).sum()),
    )


def rule(
    name: str,
    description: str,
    violations: Callable[[pd.DataFrame], pd.Series],
    severity: Severity = Severity.ERROR,
) -> Check:
    """Build a check from a function returning a boolean mask of violating rows."""
    return Check(
        name=name,
        description=description,
        severity=severity,
        count_failures=lambda df: int(violations(df).fillna(False).sum()),
    )
