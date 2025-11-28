#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""CLI commands for managing HSM server configuration."""

from __future__ import annotations

import sys

import click

from oarepo_file_pipeline_server.config import KEY_MANAGEMENT_SERVICE


@click.group()
def cli() -> None:
    """OARepo File Pipeline Server - HSM Configuration Management."""


@cli.group()
def hsm() -> None:
    """Manage HSM server configuration."""


@hsm.command("list")
def list_keys() -> None:
    """List all configured HSM server keys."""
    try:
        servers = KEY_MANAGEMENT_SERVICE.list_keys()

        if not servers:
            click.echo("No HSM servers configured.")
            return

        click.echo("Configured HSM servers:")
        click.echo("")
        for name, url in servers.items():
            click.echo(f"  {name}: {url}")

    except (OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@hsm.command("add")
@click.argument("name")
@click.argument("url")
@click.option(
    "--no-reinit",
    is_flag=True,
    help="Do not reinitialize KeyCollection after adding",
)
def add_key(name: str, url: str, no_reinit: bool) -> None:
    r"""Add a new HSM server key.

    \b
    Arguments:
        NAME: Name/identifier for the HSM server
        URL: URL of the HSM server
    """
    try:
        KEY_MANAGEMENT_SERVICE.add_key(name, url, reinitialize=not no_reinit)
        click.echo(f"Successfully added HSM server '{name}': {url}")

    except (ValueError, OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@hsm.command("remove")
@click.argument("name")
@click.option(
    "--no-reinit",
    is_flag=True,
    help="Do not reinitialize KeyCollection after removal",
)
def revoke_key(name: str, no_reinit: bool) -> None:
    r"""Remove (revoke) an HSM server key.

    \b
    Arguments:
        NAME: Name/identifier of the HSM server to remove
    """
    try:
        KEY_MANAGEMENT_SERVICE.revoke_key(name, reinitialize=not no_reinit)
        click.echo(f"Successfully removed HSM server '{name}'")

    except (KeyError, ValueError, OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@hsm.command("update")
@click.argument("name")
@click.argument("url")
@click.option(
    "--no-reinit",
    is_flag=True,
    help="Do not reinitialize KeyCollection after update",
)
def update_key(name: str, url: str, no_reinit: bool) -> None:
    r"""Update an existing HSM server key URL.

    \b
    Arguments:
        NAME: Name/identifier of the HSM server to update
        URL: New URL for the HSM server
    """
    try:
        KEY_MANAGEMENT_SERVICE.update_key(name, url, reinitialize=not no_reinit)
        click.echo(f"Successfully updated HSM server '{name}': {url}")

    except (KeyError, OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@hsm.command("reload")
def reload_config() -> None:
    """Reload HSM configuration from file and reinitialize KeyCollection."""
    try:
        KEY_MANAGEMENT_SERVICE.reload_config()
        click.echo("Successfully reloaded HSM configuration")

        servers = KEY_MANAGEMENT_SERVICE.list_keys()
        click.echo(f"Loaded {len(servers)} HSM server(s):")
        for name, url in servers.items():
            click.echo(f"  {name}: {url}")

    except (OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@hsm.command("init")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing configuration file",
)
def init_config(force: bool) -> None:
    """Initialize a new HSM configuration file with default settings."""
    config_path = KEY_MANAGEMENT_SERVICE.config_path

    if config_path.exists() and not force:
        click.echo(f"Error: Configuration file '{config_path}' already exists. Use --force to overwrite.", err=True)
        sys.exit(1)

    try:
        KEY_MANAGEMENT_SERVICE.reload_config()
        click.echo(f"Successfully initialized HSM configuration at '{config_path}'")

        servers = KEY_MANAGEMENT_SERVICE.list_keys()
        click.echo(f"Created with {len(servers)} default HSM server(s):")
        for name, url in servers.items():
            click.echo(f"  {name}: {url}")

    except (OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def rsa() -> None:
    """Manage RSA keys for JWT/JWE encryption."""


@rsa.command("list")
def list_rsa_keys() -> None:
    """List all configured RSA keys."""
    try:
        keys = KEY_MANAGEMENT_SERVICE.list_rsa_keys()

        if not keys:
            click.echo("No RSA keys configured.")
            return

        click.echo("Configured RSA keys:")
        click.echo("")
        for key_name in keys:
            click.echo(f"  - {key_name}")

    except (OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@rsa.command("add")
@click.argument("key_name")
@click.argument("key_file", type=click.Path(exists=True, dir_okay=False))
def add_rsa_key(key_name: str, key_file: str) -> None:
    r"""Add or update an RSA key from a PEM file.

    \b
    Arguments:
        KEY_NAME: Name for the RSA key (e.g., 'server_private_key', 'repo_public_key')
        KEY_FILE: Path to PEM file containing the RSA key
    """
    try:
        from pathlib import Path

        with Path(key_file).open() as f:
            key_pem = f.read()

        KEY_MANAGEMENT_SERVICE.add_rsa_key(key_name, key_pem)
        click.echo(f"Successfully added RSA key '{key_name}'")

    except (ValueError, OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@rsa.command("remove")
@click.argument("key_name")
def remove_rsa_key(key_name: str) -> None:
    r"""Remove an RSA key.

    \b
    Arguments:
        KEY_NAME: Name of the RSA key to remove
    """
    try:
        KEY_MANAGEMENT_SERVICE.remove_rsa_key(key_name)
        click.echo(f"Successfully removed RSA key '{key_name}'")

    except (KeyError, OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@rsa.command("show")
@click.argument("key_name")
def show_rsa_key(key_name: str) -> None:
    r"""Show the PEM content of an RSA key.

    \b
    Arguments:
        KEY_NAME: Name of the RSA key to show
    """
    try:
        if key_name not in KEY_MANAGEMENT_SERVICE.rsa_keys:
            click.echo(f"Error: RSA key '{key_name}' not found", err=True)
            sys.exit(1)

        key_pem = KEY_MANAGEMENT_SERVICE.rsa_keys[key_name]
        click.echo(key_pem)

    except (OSError, RuntimeError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
