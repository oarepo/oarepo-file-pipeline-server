#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""UWSGI entry point for the file pipeline server.

This script imports and exposes the WSGI application from the main module.
"""

from oarepo_file_pipeline_server.main import application

# The application callable is imported and ready for uwsgi
__all__ = ["application"]
