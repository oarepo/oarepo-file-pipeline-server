#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Abstract class for individual pipeline step definition"""

import abc
from typing import AsyncIterator

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PipelineStep(abc.ABC):
    """Abstract base class for a step in a processing pipeline."""
    produces_multiple_outputs: bool = False

    @abc.abstractmethod
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Process the input data and yield the output data.

        The method should be implemented by subclasses to define the specific logic
        of the pipeline step. It processes data from the input iterator and yields
        processed data as an iterator of `PipelineData`.

        :param inputs: An asynchronous iterator of `PipelineData` objects representing
                       the inputs for this pipeline step. Can be `None` if no inputs are provided.
        :param args: A dictionary of additional arguments passed to the pipeline step.
                     This can be used to customize processing (e.g., source_url, directory_name to extract etc.).
        :return: An asynchronous iterator of `PipelineData` objects representing the processed outputs.
                 Can return `None` if no output is produced.
        """
        pass




