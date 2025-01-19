"""Protocol of data structure for pipeline. Need to have at least read, aiter, anext."""

from typing import Protocol, runtime_checkable, Self


@runtime_checkable
class PipelineData(Protocol):
    async def read(self, n:int=1) -> bytes:
        ...

    def __aiter__(self) -> Self:
        ...

    async def __anext__(self) -> bytes:
        ...