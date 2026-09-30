"""Dataset contracts and the dataset registry."""

from __future__ import annotations

from finsight.datasets.base import ConformResult, Dataset
from finsight.datasets.ohlcv import OHLCVDataset
from finsight.errors import UnknownDatasetError

_DATASETS: dict[str, Dataset] = {}


def register_dataset(dataset: Dataset, *, replace: bool = False) -> None:
    if dataset.name in _DATASETS and not replace:
        raise ValueError(f"dataset {dataset.name!r} is already registered")
    _DATASETS[dataset.name] = dataset


def unregister_dataset(name: str) -> None:
    _DATASETS.pop(name.strip().lower(), None)


def get_dataset(name: str) -> Dataset:
    key = name.strip().lower()
    if key not in _DATASETS:
        known = ", ".join(sorted(_DATASETS)) or "none"
        raise UnknownDatasetError(f"unknown dataset {name!r} (available: {known})")
    return _DATASETS[key]


def available_datasets() -> list[Dataset]:
    return [_DATASETS[name] for name in sorted(_DATASETS)]


register_dataset(OHLCVDataset())

__all__ = [
    "ConformResult",
    "Dataset",
    "OHLCVDataset",
    "available_datasets",
    "get_dataset",
    "register_dataset",
    "unregister_dataset",
]
