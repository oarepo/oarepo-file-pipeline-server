from typing import Protocol, runtime_checkable


@runtime_checkable
class PipelineData(Protocol):
    async def read(self, n:int=1):
        ...

    def __aiter__(self):
        ...

    async def __anext__(self):
        ...