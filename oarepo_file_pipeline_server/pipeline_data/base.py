#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Protocol defining the data structure for pipeline operations.

This protocol ensures that any class implementing it will support the essential
methods for handling data in a synchronous pipeline. The data structure must
at least support reading, iterating, and providing chunks of data.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import io


@dataclasses.dataclass
class PipelineData:
    """Data structure representing a file in the pipeline.

    Contains a stream for reading file data and metadata dictionary with
    information like file_name, media_type or source_url.
    """

    stream: io.RawIOBase
    metadata: dict
