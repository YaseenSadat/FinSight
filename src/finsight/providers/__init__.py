"""Market data providers and the provider registry."""

from finsight.providers.base import DatasetSupport, IntervalSupport, Provider
from finsight.providers.registry import (
    available_providers,
    get_provider,
    provider_names,
    register_provider,
    unregister_provider,
)
from finsight.providers.synthetic import SyntheticProvider
from finsight.providers.yahoo import YahooProvider

register_provider(YahooProvider.name, YahooProvider, replace=True)
register_provider(SyntheticProvider.name, SyntheticProvider, replace=True)

__all__ = [
    "DatasetSupport",
    "IntervalSupport",
    "Provider",
    "SyntheticProvider",
    "YahooProvider",
    "available_providers",
    "get_provider",
    "provider_names",
    "register_provider",
    "unregister_provider",
]
