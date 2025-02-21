from abc import ABC, abstractmethod
from schemes import Coin


class ExchangeInterface(ABC):
    __url__: str = ''

    @abstractmethod
    async def get(self) -> tuple[str, Coin]:
        pass
