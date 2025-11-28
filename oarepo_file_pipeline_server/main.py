#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""UWSGI server implementation for file pipeline processing.

This module provides a WSGI/UWSGI-compatible server that processes
file pipelines by fetching encrypted tokens from Redis, decrypting them,
and executing the specified pipeline steps.

URL pattern: /pipeline/<token_id>
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

import redis

from oarepo_file_pipeline_server.config import KEY_MANAGEMENT_SERVICE, STEP_DEFINITIONS
from oarepo_file_pipeline_server.utils import get_payload, get_pipeline_step_obj

if TYPE_CHECKING:
    from collections.abc import Iterable
    from wsgiref.types import StartResponse, WSGIEnvironment

    from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
    from oarepo_file_pipeline_server.pipeline_steps.base import StepIO

logger = logging.getLogger(__name__)


def split_and_clean(path: str) -> list[str]:
    """Split path by slashes and cleans up to one heading and trailing empty element."""
    # remove leading and trailing slashes
    path = path.strip("/")
    return [x for x in path.split("/") if x]


def make_response(
    start_response: StartResponse,
    status: str,
    body: str,
    content_type: str = "application/json",
) -> list[bytes]:
    """Make HTTP response."""
    start_response(status, [("Content-Type", content_type)])
    return [body.encode("utf-8")]


def make_not_found(start_response: StartResponse) -> list[bytes]:
    """Create a Not Found response."""
    return make_response(
        start_response,
        "404 Not Found",
        json.dumps({"error": "Not Found", "message": "Token not found or expired"}),
    )


def make_error(
    start_response: StartResponse,
    message: str,
    status: str = "500 Internal Server Error",
) -> list[bytes]:
    """Create an error response."""
    logger.error("Error response: %s - %s", status, message)
    return make_response(
        start_response,
        status,
        json.dumps({"error": status.split(" ", 1)[0], "message": message}),
    )


class FilePipelineServer:
    """A UWSGI-compatible server that processes file pipelines.

    The server fetches encrypted tokens from Redis, decrypts them to obtain
    pipeline step definitions, and executes those steps.

    URL pattern: /pipeline/<token_id>
    """

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        prefix: str = "pipeline",
    ) -> None:
        """Initialize the file pipeline server.

        :param redis_host: Redis server hostname
        :param redis_port: Redis server port
        :param redis_db: Redis database number
        :param prefix: URL path prefix (e.g., "pipeline" for /pipeline/<token_id>)
        :raises RuntimeError: If required configuration is missing
        """
        # Validate required RSA keys
        logger.info("Validating server configuration...")
        self._validate_configuration()

        # Initialize Redis connection
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
            # Test Redis connection
            self.redis_client.ping()
            logger.info("Connected to Redis at %s:%s, database %s", redis_host, redis_port, redis_db)
        except redis.ConnectionError as e:
            raise RuntimeError(f"Failed to connect to Redis at {redis_host}:{redis_port}: {e}") from e

        self._prefix = split_and_clean(prefix)
        # Expected request length: prefix + token_id
        self._required_request_length = len(self._prefix) + 1
        logger.info("File pipeline server initialized with prefix: /%s/", "/".join(self._prefix))

    def _validate_configuration(self) -> None:
        """Validate that all required keys are configured.

        :raises RuntimeError: If any required key is missing
        """
        errors = []

        # Check RSA server private key
        try:
            KEY_MANAGEMENT_SERVICE.get_rsa_key("server_private_key")
            logger.info("RSA server private key found")
        except KeyError:
            errors.append("RSA server private key ('server_private_key') not found in configuration")

        # Check RSA repository public key
        try:
            KEY_MANAGEMENT_SERVICE.get_rsa_key("repo_public_key")
            logger.info("RSA repository public key found")
        except KeyError:
            errors.append("RSA repository public key ('repo_public_key') not found in configuration")

        # Check at least one Crypt4GH key is configured
        try:
            hsm_servers = KEY_MANAGEMENT_SERVICE.list_keys()
            if not hsm_servers:
                errors.append("No Crypt4GH keys configured. At least one key is required.")
            else:
                logger.info("Found %s Crypt4GH key(s): %s", len(hsm_servers), ", ".join(hsm_servers.keys()))
        except (OSError, RuntimeError) as e:
            errors.append(f"Failed to list Crypt4GH keys: {e}")

        if errors:
            error_message = "Server configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            logger.error(error_message)
            raise RuntimeError(error_message)

    def process_pipeline(self, token_id: str) -> PipelineData:
        """Process a pipeline by fetching and decrypting the token from Redis."""
        # Fetch token from Redis
        jwe_token = self.redis_client.get(f"{token_id}")  # type: ignore[assignment]
        if jwe_token is None:
            raise ValueError("Token not found or expired")

        # Delete token after retrieval (single-use)
        self.redis_client.delete(f"{token_id}")

        # Decrypt and get payload with pipeline steps
        payload = get_payload(jwe_token)  # type: ignore[arg-type]

        # Extract pipeline steps from payload
        steps = payload["pipeline_steps"]
        if not steps:
            raise ValueError("No pipeline steps found in payload")

        # Start with no input (first step will use source_url from args)
        current_input: StepIO | None = None
        step_results: StepIO | None = None

        # Execute each pipeline step in order
        for step_config in steps:
            step_type = step_config.get("type")
            step_args = step_config.get("arguments", {})
            if not step_type:
                raise ValueError("Step type is required")

            # Get step class from configuration
            step_class_path = STEP_DEFINITIONS.get(step_type)
            if not step_class_path:
                raise ValueError(f"Unknown step type: {step_type}")

            pipeline_step_obj = get_pipeline_step_obj(step_type)()  # initialize step

            # Process step
            step_results = pipeline_step_obj.process(current_input, step_args)  # type: ignore[arg-type]
            current_input = step_results

        # After all steps, assert exactly one file in the stream
        if current_input is None:
            raise ValueError("Pipeline produced no output")

        if step_results is None:
            raise ValueError("Pipeline produced no step results")

        assert len(step_results) == 1, "Pipeline should produce exactly one output file"  # noqa: S101

        # Get the single output file
        return next(iter(current_input))

    def _validate_request_path(self, request_list: list[str]) -> tuple[str | None, str | None]:
        """Validate request path and extract token_id.

        :return: Tuple of (error_message, token_id). If error_message is not None, request is invalid.
        """
        # Validate request length
        if len(request_list) < self._required_request_length:
            return (
                f"Invalid request path. Expected: /{'/'.join(self._prefix)}/<token_id>",
                None,
            )

        # Check for prefix
        if request_list[: len(self._prefix)] != self._prefix:
            return (
                f"Invalid prefix. Expected: /{'/'.join(self._prefix)}/",
                None,
            )

        # Extract token_id
        token_id = request_list[len(self._prefix)]
        return (None, token_id)

    def _handle_pipeline_exception(self, e: Exception, token_id: str, start_response: StartResponse) -> Iterable[bytes]:
        """Handle exceptions during pipeline processing."""
        if isinstance(e, ValueError):
            # Token not found or validation errors
            error_msg = str(e)
            if "Token not found" in error_msg or "expired" in error_msg:
                logger.warning("Token not found or expired: %s", token_id)
                return make_not_found(start_response)
            logger.exception("Pipeline processing error for token %s: %s", token_id, error_msg)
            return make_error(
                start_response,
                f"Pipeline validation error: {error_msg}",
                "400 Bad Request",
            )

        if isinstance(e, KeyError):
            # Missing required configuration or data
            logger.exception("Configuration or data error for token %s", token_id)
            return make_error(
                start_response,
                f"Configuration error: Missing required key {e}",
            )

        if isinstance(e, redis.RedisError):
            # Redis connection or operation errors
            logger.exception("Redis error for token %s", token_id)
            return make_error(
                start_response,
                f"Storage service error: {e}",
                "503 Service Unavailable",
            )

        # Log the full error with stack trace for debugging
        logger.exception("Unexpected error processing token %s", token_id)
        return make_error(
            start_response,
            f"An unexpected error occurred while processing the pipeline: {type(e).__name__}: {e}",
        )

    def handle_path_request(self, request_path: str, start_response: StartResponse) -> Iterable[bytes]:
        """Handle pipeline processing requests based on URL path."""
        request_list = split_and_clean(request_path)

        # Validate request path
        error_msg, token_id = self._validate_request_path(request_list)
        if error_msg or token_id is None:
            return make_error(start_response, error_msg or "Invalid request", "400 Bad Request")

        try:
            # Process the pipeline and get output file
            output_file = self.process_pipeline(token_id)

            # Get metadata for response headers
            metadata = output_file.metadata
            content_type = metadata.get("media_type", "application/octet-stream")
            file_name = metadata.get("file_name", "output")
            download = metadata.get("download", True)

            # Set response headers
            # Get download flag from metadata (default to True).
            # For example return just JSON response for validation step
            headers = [("Content-Type", content_type)]
            if download:
                headers.append(("Content-Disposition", f'attachment; filename="{file_name}"'))

            start_response("200 OK", headers)

            # Stream file in chunks
            def stream_file() -> Iterable[bytes]:
                """Generate file chunks."""
                try:
                    yield from output_file.stream
                finally:
                    # Clean up if the PipelineData has a close method
                    close_method = getattr(output_file.stream, "close", None)
                    if close_method is not None:
                        close_method()

            return stream_file()

        except Exception as e:  # noqa: BLE001
            return self._handle_pipeline_exception(e, token_id, start_response)

    def handle_uwsgi_request(self, env: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
        """UWSGI/WSGI request handler."""
        return self.handle_path_request(env["PATH_INFO"], start_response)


# Global server instance for UWSGI
_server_instance = None
_server_init_error = None


def application(env: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
    """UWSGI application entry point.

    This function is called by UWSGI for each HTTP request.
    Redis connection parameters can be configured via environment variables:
    - REDIS_HOST (default: localhost)
    - REDIS_PORT (default: 6379)
    - REDIS_DB (default: 0)
    """
    global _server_instance, _server_init_error  # noqa: PLW0603

    if _server_instance is None and _server_init_error is None:
        try:
            # Read Redis configuration from environment variables
            redis_host = os.environ.get("REDIS_HOST", "localhost")
            redis_port = int(os.environ.get("REDIS_PORT", "6379"))
            redis_db = int(os.environ.get("REDIS_DB", "0"))

            logger.info("Initializing File Pipeline Server...")
            logger.info("Redis configuration: host=%s, port=%s, db=%s", redis_host, redis_port, redis_db)

            _server_instance = FilePipelineServer(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                prefix="pipeline",
            )
            logger.info("File Pipeline Server successfully initialized and ready to process requests")
        except RuntimeError as e:
            _server_init_error = str(e)
            logger.critical("Failed to initialize File Pipeline Server: %s", e)
        except Exception as e:
            _server_init_error = f"Unexpected initialization error: {type(e).__name__}: {e}"
            logger.critical("Unexpected error during server initialization: %s", e, exc_info=True)

    # If server failed to initialize, return error for all requests
    if _server_init_error is not None:
        return make_error(
            start_response,
            f"Server initialization failed: {_server_init_error}",
            "503 Service Unavailable",
        )

    # Type assertion: if _server_init_error is None, _server_instance must be initialized
    assert _server_instance is not None  # noqa: S101
    return _server_instance.handle_uwsgi_request(env, start_response)
