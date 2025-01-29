# oarepo-file-pipeline-server

Server that handles processing pipeline steps like extract file/directory from zip file, preview picture/zip file, 
create zip file, crypt4gh.

Result from last step is streamed asynchronously 

## PipelineStep and PipelineData Overview
### PipelineStep

The PipelineStep class is an abstract base class designed to represent individual steps in a data processing pipeline. Each step processes a stream of data and may produce multiple outputs or none at all.

Attributes

    produces_multiple_outputs: A boolean flag indicating whether the step produces multiple outputs. Defaults to False.

Methods

    process(inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        Inputs: An asynchronous iterator of PipelineData objects, empty if first step
        Arguments: A dictionary (args) for passing additional parameters, should contain source_url if inputs are empty
        Returns: An asynchronous iterator of PipelineData objects . Can return None if no output is produced.

### PipelineData

The PipelineData protocol defines the expected structure of the data that flows through the pipeline. Any object that adheres to this protocol can be processed by the pipeline steps.
Each PipelineData subclasses holds metadata property to access key information about data

Methods

    read(n: int = -1) -> bytes:
        Arguments: An integer n (default -1), specifying the number of bytes to read from the data stream.
        Returns: A bytes object containing the read data.
    __aiter__():
        Returns an asynchronous iterator, which allows iterating over the data.
    __anext__() -> bytes:
        Returns the next chunk of data as a bytes object when called on the asynchronous iterator.

### PipelineData subclasses

#### QueuePipelineData
This class represents pipeline data stored in an asyncio.Queue, and it provides methods for reading data in chunks asynchronously.
QueuePipelineData supports read() in the stream

#### BytesPipelineData
This class represents pipeline data from a BytesIO stream, allowing you to read data from it in chunks asynchronously.
BytesPipelineData supports read(), seek(), tell() in the stream

Class is meant to keep smaller amount of data, for example zip namelist 

#### UrlPipelineData
This class represents pipeline data from a URL, with support for reading data in chunks by using HTTP range requests.
UrlPipelineData support read(), seek(), tell() function in the stream

### PipelineStep subclasses

#### PreviewZip

This class is a pipeline step that processes ZIP files. It asynchronously retrieves input data, extracts details from the 
ZIP file (such as filenames, MIME types, and sizes), and returns the data as a JSON object.

If first step in pipeline steps, `args` must contain `source_url`, otherwise ValueError is raised.
Metadata will contain only 
1) `media_type = application/json`

Example output:
   ```
   "file_name": {
        "is_dir": false/true,
        "file_size": number,
        "modified_time": "%Y-%m-%d %H:%M:%S",
        "compressed_size": number,
        "compress_type": number,
        "media_type": mime_type else ""
    }
   ```

#### PreviewPicture

Processes image files and generates a resized preview if the image dimensions exceed the specified `max_width` or `max_height`
parameters. If the image dimensions are smaller or equal to the provided maximum values, the original image is returned.

If first step in pipeline steps, `args` must contain `source_url`, otherwise ValueError is raised. `max_width`, `max_height` 
must be present everytime

Metadata will contain:
1) `file_name` if available 
2) `media_type` MIME type of the image (e.g. `image/jpeg`)
3) `mode` Image color mode for example (e.g. RGB, RGBA)
4) `width`
5) `height` 

#### ExtractFileZip

Step extracts a specific `file_name` from a ZIP archive, processing it as part of the pipeline. The file is retrieved from 
the ZIP archive and returned in chunks, along with associated metadata.

If first step in pipeline steps, `args` must contain `source_url`, otherwise ValueError is raised. `file_name` must be present everytime

Metadata will contain:
1) `file_name` 
2) `media_type` MIME type of the image (e.g. `text/plain`) or `application/octet-stream` if not recognized

#### ExtractDirectoryZip

Can produce multiple outputs (multiple QueuePipelineData)

Step extracts a specific `directory_name` from a ZIP archive, processing it as part of the pipeline. The file is retrieved from 
the ZIP archive and returned in chunks, along with associated metadata.

If first step in pipeline steps, `args` must contain `source_url`, otherwise ValueError is raised. `directory_name` must be present everytime

Metadata will contain:
1) `file_name` 
2) `media_type` MIME type of the image (e.g. `text/plain`) or `application/octet-stream` if not recognized

#### CreateZip
Creates a ZIP archive from a list of input files. It collects the input data, compresses it into a ZIP file, and 
produces the resulting archive as ZipPipelineData for further processing in the pipeline.

Right now only used if last step is producing multiple output, meaning last step was ExtractDirectoryZip

Metadata will contain:
1) `file_name` The name of the resulting ZIP file, which is hardcoded as `created.zip`
2) `media_type` The MIME type of the resulting file, which is set to `application/zip`
3) `headers` Contains a `Content-Disposition` header, which includes the name of the ZIP file for download 

#### Crypt4GH
Pipeline step to add a new recipient to Crypt4GH file.

Metadata will contain:
1) `file_name` The name of the resulting Crypt4GH file, which is hardcoded as `crypt4gh.c4gh`
2) `media_type` The MIME type of the resulting file, which is set to `application/octet-stream`

## Example usages
Pipeline will contain only 1 step (PreviewZip)

The server will receive in the payload:
```
pipeline_steps = [
   {'type' : "preview_zip",
      "arguments": {
         "source_url": file_url,
      }
   },
]
```

Step with data from the URL
```
step = PreviewZip() # init step class

outputs = step.process(None, args = {
   'source_url`: file_url
}) # define outputs with file url

output = await anext(outputs) # get single output
buffer = await output.read() # will read all of the data in BytesIO buffer

```


Step with data from the inputs 
```
with open(some_path, "rb") as f:
   zip_stream = io.BytesIO(f.read()) # some example file

async def get_data():
     yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream) # get async iterator

step = PreviewZip() # init step class

outputs = step.process(get_data(), args = {}) # define outputs with input data
output = await anext(outputs) # get single output

buffer = await output.read() # will read all of the data in BytesIO buffer
```

