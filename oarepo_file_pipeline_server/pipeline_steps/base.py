#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Abstract class for individual pipeline step definition."""

from __future__ import annotations

import abc

from oarepo_file_pipeline_server.pipeline_data.base import PipelineData

UNKNOWN_FILE_COUNT = -1


class StepIO(list[PipelineData]):
    """Class representing the results of a pipeline step."""


class PipelineStep(abc.ABC):
    """Abstract base class for a step in a processing pipeline."""

    @abc.abstractmethod
    def process(self, inputs: StepIO, args: dict) -> StepIO:
        """Process the input data and yield the output data.

        The method should be implemented by subclasses to define the specific logic
        of the pipeline step. It processes data from the input iterator and yields
        processed data as an iterator of `PipelineData`.

        :param inputs: An iterator of `PipelineData` objects representing
                       the inputs for this pipeline step. Can be `None` if no inputs are provided.
        :param args: A dictionary of additional arguments passed to the pipeline step.
                     This can be used to customize processing (e.g., source_url, directory_name to extract etc.).
        :return: An iterator of `PipelineData` objects representing the processed outputs.
                 Can return `None` if no output is produced.
        """
