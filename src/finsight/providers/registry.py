"""Provider registry.

Built-in providers are registered on import. Third-party packages can add
providers without modifying FinSight by exposing a :class:`Provider` subclass
under the ``finsight.providers`` entry-point group::

    [project.entry-points."finsight.providers"]
    myvendor = "finsight_myvendor:MyVendorProvider"
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from importlib.metadata import entry_points

from finsight.errors import UnknownProviderError
from finsight.providers.base import Provider

logger = logging.getLogger(__name__)

ProviderFactory = Callable[[], Provider]

_factories: dict[str, ProviderFactory] = {}
_instances: dict[str, Provider] = {}
_lock = threading.Lock()
_entry_points_loaded = False


def register_provider(
    name: str, factory: ProviderFactory | Provider, *, replace: bool = False
) -> None:
    """Register a provider under ``name``.

    Args:
        name: Registry key (lower-case).
        factory: A zero-argument callable (usually the class) that builds the
            provider, or a ready-made instance.
        replace: Allow overwriting an existing registration.
    """
    key = name.strip().lower()
    with _lock:
        if key in _factories and not replace:
            raise ValueError(f"provider {key!r} is already registered")
        _instances.pop(key, None)
        if isinstance(factory, Provider):
            instance = factory
            _factories[key] = lambda: instance
        else:
            _factories[key] = factory


def unregister_provider(name: str) -> None:
    with _lock:
        _factories.pop(name, None)
        _instances.pop(name, None)


def _load_entry_points() -> None:
    global _entry_points_loaded
    if _entry_points_loaded:
        return
    _entry_points_loaded = True
    for ep in entry_points(group="finsight.providers"):
        if ep.name in _factories:
            continue
        try:
            register_provider(ep.name, ep.load())
        except Exception:
            logger.exception("failed to load provider plugin %r", ep.name)


def provider_names() -> list[str]:
    _load_entry_points()
    return sorted(_factories)


def get_provider(name: str) -> Provider:
    """Return the (cached) provider instance registered under ``name``."""
    _load_entry_points()
    key = name.strip().lower()
    with _lock:
        if key in _instances:
            return _instances[key]
        factory = _factories.get(key)
        if factory is None:
            known = ", ".join(sorted(_factories)) or "none"
            raise UnknownProviderError(f"unknown provider {name!r} (available: {known})")
    instance = factory()
    with _lock:
        return _instances.setdefault(key, instance)


def available_providers() -> list[Provider]:
    return [get_provider(name) for name in provider_names()]
