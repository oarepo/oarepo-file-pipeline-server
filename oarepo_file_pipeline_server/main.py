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
from typing import TYPE_CHECKING

import redis

from oarepo_file_pipeline_server.config import STEP_DEFINITIONS
from oarepo_file_pipeline_server.utils import get_payload, get_pipeline_step_obj

if TYPE_CHECKING:
    from collections.abc import Iterable
    from wsgiref.types import StartResponse, WSGIEnvironment

    from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
    from oarepo_file_pipeline_server.pipeline_steps.base import StepResults


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
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self._prefix = split_and_clean(prefix)
        # Expected request length: prefix + token_id
        self._required_request_length = len(self._prefix) + 1

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
        current_input: Iterable[PipelineData] | None = None
        step_results: StepResults | None = None

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

            # Update input for next step
            current_input = step_results.results

        # After all steps, assert exactly one file in the stream
        if current_input is None:
            raise ValueError("Pipeline produced no output")

        if step_results is None:
            raise ValueError("Pipeline produced no step results")

        assert step_results.file_count == 1, "Pipeline should produce exactly one output file"  # noqa: S101

        # Get the single output file
        return next(iter(current_input))

    def handle_path_request(self, request_path: str, start_response: StartResponse) -> Iterable[bytes]:
        """Handle pipeline processing requests based on URL path."""
        request_list = split_and_clean(request_path)

        # Validate request length
        if len(request_list) < self._required_request_length:
            return make_error(
                start_response,
                f"Invalid request path. Expected: /{'/'.join(self._prefix)}/<token_id>",
                "400 Bad Request",
            )

        # Check for prefix
        if request_list[: len(self._prefix)] != self._prefix:
            return make_error(
                start_response,
                f"Invalid prefix. Expected: /{'/'.join(self._prefix)}/",
                "400 Bad Request",
            )

        # Extract token_id
        token_id = request_list[len(self._prefix)]

        try:
            # Process the pipeline and get output file
            output_file = self.process_pipeline(token_id)

            # Get metadata for response headers
            metadata = output_file.metadata
            content_type = metadata.get("media_type", "application/octet-stream")
            file_name = metadata.get("file_name", "output")

            # Set response headers
            start_response(
                "200 OK",
                [
                    ("Content-Type", content_type),
                    ("Content-Disposition", f'attachment; filename="{file_name}"'),
                ],
            )

            # Stream file in chunks
            def stream_file() -> Iterable[bytes]:
                """Generate file chunks."""
                try:
                    yield from output_file
                finally:
                    # Clean up if the PipelineData has a close method
                    close_method = getattr(output_file, "close", None)
                    if close_method is not None:
                        close_method()

            return stream_file()

        except ValueError:
            return make_not_found(start_response)

        except Exception as e:  # noqa: BLE001
            # Log the full error
            return make_error(start_response, str(e))

    def handle_uwsgi_request(self, env: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
        """UWSGI/WSGI request handler."""
        return self.handle_path_request(env["PATH_INFO"], start_response)


# Global server instance for UWSGI
_server_instance = None


def application(env: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
    """UWSGI application entry point.

    This function is called by UWSGI for each HTTP request.
    """
    global _server_instance  # noqa: PLW0603

    if _server_instance is None:
        _server_instance = FilePipelineServer(redis_host="localhost", redis_port=6379, redis_db=0, prefix="pipeline")

    return _server_instance.handle_uwsgi_request(env, start_response)
