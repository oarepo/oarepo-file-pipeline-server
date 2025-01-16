import mimetypes
import os
import zipfile
from typing import AsyncIterator

from oarepo_file_pipeline_server.async_to_sync.sync_runner import ResultQueue, sync_stream_runner
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class ExtractDirectoryZip(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Extracts a directory zip from the input zip file.
        """
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
            print("Read file from s3")
        else:
            raise ValueError("No input nor source_url were provided.")

        directory_name = args.get("directory_name")
        results = await sync_stream_runner(extract_directory_zip, input_stream, directory_name)

        item_type, item_value = await results.get()
        while item_type != 'complete':
            if item_type == 'error':
                raise item_value

            if item_type != 'startfile' :
                raise ValueError(f"Implementation error: {item_type}")

            yield QueuePipelineData(results, metadata=item_value)
            item_type, item_value = await results.get()


def extract_directory_zip(input_stream, directory_name, result_queue: ResultQueue):
    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")

    if not directory_name:
        raise ValueError("Directory name to extract is not specified in arguments.")

    with zipfile.ZipFile(input_stream, 'r') as zip_file:
        # Normalize directory name to ensure it ends with 1 '/'
        # in case user made a typo with 2 backslashes instead of 1 etc.
        directory_name = directory_name.rstrip("/") + "/"
        namelist = []
        file_count = 0
        for file_name in zip_file.namelist():
            namelist.append(file_name)
            if file_name.startswith(directory_name):
                if not file_name.endswith("/"):
                    file_count += 1


        for file_name in namelist:
            if file_name.startswith(directory_name) and not file_name.endswith("/"):  # Exclude subdirectories
                with zip_file.open(file_name, 'r') as extracted_file:
                    filename = os.path.basename(file_name)
                    mime_type, _ = mimetypes.guess_type(filename)
                    print(f'Extracting {file_name}...')
                    result_queue.put('startfile', {
                        'file_name': file_name,
                        'media_type': mime_type if mime_type else "application/octet-stream",
                        'total_file_count': file_count,
                    })

                    while True:
                        chunk = extracted_file.read(1024 * 1024)
                        if not chunk:
                            break

                        result_queue.put('chunk', chunk)

                    result_queue.put('endfile', b'EOF')
