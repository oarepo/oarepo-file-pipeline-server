#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""HSM Configuration Manager for managing HSM server URLs and KeyCollection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from joserfc.jwk import RSAKey
from oarepo_c4gh.key import KeyCollection

if TYPE_CHECKING:
    from oarepo_file_pipeline_server.key_provider import KeyProvider


class KeyManagementService:
    """Manager for HSM server configuration and KeyCollection.

    This class handles loading HSM server URLs from a JSON configuration file,
    managing the KeyCollection, and providing methods to add/remove keys dynamically.
    """

    def __init__(self, config_path: str | Path = "hsm_config.json", key_provider: KeyProvider | None = None) -> None:
        """Initialize the HSM configuration manager.

        :param config_path: Path to the JSON configuration file.
        :param key_provider: Optional KeyProvider instance. If None, uses get_key_provider().
        """
        self.config_path = Path(config_path)
        self.hsm_servers: dict[str, str] = {}
        self.rsa_keys: dict[str, str] = {}
        self.key_collection: KeyCollection | None = None
        self._rsa_key_objects: dict[str, RSAKey] = {}

        # Set key provider (lazy import to avoid circular dependency)
        if key_provider is None:
            from oarepo_file_pipeline_server.key_provider import get_key_provider

            self.key_provider = get_key_provider()
        else:
            self.key_provider = key_provider

        self.load_config()

    def load_config(self) -> None:
        """Load HSM server and RSA key configuration from JSON file.

        If the file doesn't exist, raise an error.
        """
        if not self.config_path.exists():
            raise ValueError(f"HSM configuration file {self.config_path} does not exist.")

        try:
            with Path(self.config_path).open("rb") as f:
                data = json.load(f)
                self.hsm_servers = data.get("hsm_servers", {})
                self.rsa_keys = data.get("rsa_keys", {})
            self._initialize_key_collection()
            self._initialize_rsa_keys()
        except (OSError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to load HSM configuration from {self.config_path}: {e}") from e

    def _initialize_key_collection(self) -> None:
        """Initialize or reinitialize the KeyCollection from configured HSM servers."""
        if not self.hsm_servers:
            raise ValueError("No HSM servers configured. Please add at least one server.")

        keys = [self.key_provider.create_key(identifier) for identifier in self.hsm_servers.values()]
        self.key_collection = KeyCollection(*keys)

    def _initialize_rsa_keys(self) -> None:
        """Initialize RSA key objects from configuration."""
        self._rsa_key_objects = {}
        for key_name, key_pem in self.rsa_keys.items():
            try:
                self._rsa_key_objects[key_name] = RSAKey.import_key(key_pem)
            except Exception as e:
                raise RuntimeError(f"Failed to load RSA key '{key_name}': {e}") from e

    def add_key(self, name: str, url: str, reinitialize: bool = True) -> None:
        """Add a new HSM server Crypt4GH HTTP key.

        :param name: Name/identifier for the HSM server.
        :param url: URL of the HSM server.
        :param reinitialize: Whether to reinitialize the KeyCollection after adding.
        :raises ValueError: If a server with the same name already exists.
        """
        if name in self.hsm_servers:
            raise ValueError(f"HSM server with name '{name}' already exists.")

        self.hsm_servers[name] = url
        self.save_config(self.hsm_servers)

        if reinitialize:
            self._initialize_key_collection()

    def revoke_key(self, name: str, reinitialize: bool = True) -> None:
        """Revoke (remove) an HSM server Crypt4GH HTTP key.

        :param name: Name/identifier of the HSM server to remove.
        :param reinitialize: Whether to reinitialize the KeyCollection after removal.
        :raises KeyError: If the server name doesn't exist.
        :raises ValueError: If trying to remove the last server.
        """
        if name not in self.hsm_servers:
            raise KeyError(f"HSM server with name '{name}' not found.")

        if len(self.hsm_servers) == 1:
            raise ValueError("Cannot remove the last HSM server. At least one server must be configured.")

        del self.hsm_servers[name]
        self.save_config(self.hsm_servers)

        if reinitialize:
            self._initialize_key_collection()

    def update_key(self, name: str, url: str, reinitialize: bool = True) -> None:
        """Update an existing HSM server Crypt4GH HTTP key URL.

        :param name: Name/identifier of the HSM server to update.
        :param url: New URL for the HSM server.
        :param reinitialize: Whether to reinitialize the KeyCollection after update.
        :raises KeyError: If the server name doesn't exist.
        """
        if name not in self.hsm_servers:
            raise KeyError(f"HSM server with name '{name}' not found.")

        self.hsm_servers[name] = url
        self.save_config(self.hsm_servers)

        if reinitialize:
            self._initialize_key_collection()

    def list_keys(self) -> dict[str, str]:
        """List all configured HSM server keys.

        :return: Dictionary of server names to URLs.
        """
        return self.hsm_servers.copy()

    def get_crypt4gh_key_collection(self) -> KeyCollection:
        """Get the current KeyCollection.

        :return: The KeyCollection instance.
        :raises RuntimeError: If KeyCollection is not initialized.
        """
        if self.key_collection is None:
            raise RuntimeError("KeyCollection is not initialized. Please load configuration first.")
        return self.key_collection

    def reinitialize(self) -> None:
        """Reinitialize the KeyCollection from the current configuration."""
        self._initialize_key_collection()

    def get_rsa_key(self, key_name: str) -> RSAKey:
        """Get an RSA key by name.

        :param key_name: Name of the RSA key (e.g., 'server_private_key', 'repo_public_key').
        :return: RSAKey instance.
        :raises KeyError: If the key doesn't exist.
        """
        if key_name not in self._rsa_key_objects:
            raise KeyError(f"RSA key '{key_name}' not found in configuration.")
        return self._rsa_key_objects[key_name]

    def add_rsa_key(self, key_name: str, key_pem: str) -> None:
        """Add or update an RSA key.

        :param key_name: Name for the RSA key (e.g., 'server_private_key').
        :param key_pem: PEM-encoded RSA key.
        :raises ValueError: If the key is invalid.
        """
        try:
            # Validate the key by importing it
            RSAKey.import_key(key_pem)
            self.rsa_keys[key_name] = key_pem
            self.save_config()
            self._initialize_rsa_keys()
        except Exception as e:
            raise ValueError(f"Invalid RSA key: {e}") from e

    def remove_rsa_key(self, key_name: str) -> None:
        """Remove an RSA key.

        :param key_name: Name of the RSA key to remove.
        :raises KeyError: If the key doesn't exist.
        """
        if key_name not in self.rsa_keys:
            raise KeyError(f"RSA key '{key_name}' not found.")

        del self.rsa_keys[key_name]
        self.save_config()
        self._initialize_rsa_keys()

    def list_rsa_keys(self) -> list[str]:
        """List all configured RSA key names.

        :return: List of RSA key names.
        """
        return list(self.rsa_keys.keys())

    def save_config(self, servers: dict[str, str] | None = None) -> None:
        """Save the current configuration to the JSON file.

        :param servers: Optional dictionary of servers to save. If None, uses current hsm_servers.
        """
        if servers is not None:
            self.hsm_servers = servers

        config_data = {
            "hsm_servers": self.hsm_servers,
            "rsa_keys": self.rsa_keys,
        }

        with Path(self.config_path).open("w") as f:
            json.dump(config_data, f, indent=2)

    def reload_config(self) -> None:
        """Reload configuration from the JSON file and reinitialize KeyCollection."""
        self.load_config()
