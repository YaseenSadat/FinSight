"""Installed package version (kept separate to avoid import cycles)."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("finsight")
except PackageNotFoundError:  # running from a source checkout without installing
    __version__ = "0.0.0.dev0"
