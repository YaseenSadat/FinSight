"""Exception hierarchy.

Every error FinSight raises on purpose derives from :class:`FinSightError`, so
callers can catch the whole family with a single ``except`` clause.
"""

from __future__ import annotations

from collections.abc import Iterable


class FinSightError(Exception):
    """Base class for all FinSight errors."""


class ConfigError(FinSightError):
    """The runtime configuration is missing a value or contains an invalid one."""


class RequestError(FinSightError):
    """A data request is invalid or not supported by the chosen provider.

    All problems are collected and reported together so users can fix them in
    one pass instead of discovering them one at a time.
    """

    def __init__(self, problems: Iterable[str]):
        self.problems = list(problems)
        super().__init__("; ".join(self.problems) or "invalid request")


class UnknownProviderError(FinSightError):
    """No provider is registered under the requested name."""


class UnknownDatasetError(FinSightError):
    """No dataset is registered under the requested name."""


class ProviderError(FinSightError):
    """A provider failed to return data for a symbol.

    ``retryable`` marks transient failures (rate limits, timeouts) that the
    pipeline should retry with backoff.
    """

    retryable: bool = False

    def __init__(self, message: str, *, symbol: str | None = None):
        self.symbol = symbol
        super().__init__(message)


class TransientProviderError(ProviderError):
    """A temporary upstream failure such as a timeout or connection reset."""

    retryable = True


class RateLimitError(TransientProviderError):
    """The upstream service is throttling requests."""


class SymbolNotFoundError(ProviderError):
    """The provider does not recognise the symbol."""


class ContractError(FinSightError):
    """Data does not match the dataset contract it claims to implement."""


class RunNotFoundError(FinSightError):
    """No run manifest exists for the requested run id."""
