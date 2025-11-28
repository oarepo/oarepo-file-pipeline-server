#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Key provider implementations for different environments."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any

from oarepo_c4gh.key import C4GHKey, HTTPKey


class KeyProvider(ABC):
    """Abstract base class for key providers."""

    @abstractmethod
    def create_key(self, identifier: str) -> Any:
        """Create a key instance from an identifier.

        :param identifier: Key identifier (URL, file path, or key string)
        :return: Key instance compatible with KeyCollection
        """


class HTTPKeyProvider(KeyProvider):
    """Key provider that creates HTTPKey instances for HSM servers."""

    def create_key(self, identifier: str) -> HTTPKey:
        """Create an HTTPKey from a URL.

        :param identifier: URL of the HSM server
        :return: HTTPKey instance
        """
        return HTTPKey(identifier)


class LocalKeyProvider(KeyProvider):
    """Key provider that creates local C4GH keys for testing.

    This provider can load keys from:
    - File paths (if identifier is a path)
    - Environment variables (if identifier starts with 'env:')
    - Direct key strings (if identifier contains key material)
    """

    def create_key(self, identifier: str) -> C4GHKey:
        """Create a C4GHKey from local source.

        :param identifier: Can be:
            - File path (e.g., "/path/to/key.sec")
            - Environment variable (e.g., "env:MY_KEY_VAR")
            - Direct key string (e.g., "-----BEGIN CRYPT4GH PRIVATE KEY-----...")
        :return: C4GHKey instance
        """
        from pathlib import Path

        if identifier.startswith("env:"):
            # Load from environment variable
            env_var = identifier[4:]
            key_string = os.environ.get(env_var)
            if not key_string:
                raise ValueError(f"Environment variable {env_var} not found")
            return C4GHKey.from_string(key_string)
        path = Path(identifier)
        if path.is_file():
            # Load from file
            with path.open() as f:
                key_string = f.read()
            return C4GHKey.from_string(key_string)
        # Assume it's a direct key string
        return C4GHKey.from_string(identifier)


def get_key_provider() -> KeyProvider:
    """Get the configured key provider based on environment.

    Looks for KEY_PROVIDER environment variable:
    - "http" or "HTTPKeyProvider": Returns HTTPKeyProvider (default)
    - "local" or "LocalKeyProvider": Returns LocalKeyProvider
    - Custom path: Imports and instantiates custom provider

    :return: KeyProvider instance
    """
    provider_name = os.environ.get("KEY_PROVIDER", "http").lower()

    if provider_name in ("http", "httpkeyprovider"):
        return HTTPKeyProvider()
    if provider_name in ("local", "localkeyprovider"):
        return LocalKeyProvider()
    # Try to import custom provider
    try:
        from typing import cast

        module_path, class_name = provider_name.rsplit(".", 1)
        import importlib

        module = importlib.import_module(module_path)
        provider_class = getattr(module, class_name)
        return cast("KeyProvider", provider_class())
    except (ValueError, ImportError, AttributeError) as e:
        raise ValueError(
            f"Invalid KEY_PROVIDER: {provider_name}. Use 'http', 'local', or a valid module path. Error: {e}"
        ) from e
