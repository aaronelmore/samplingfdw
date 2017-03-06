import logging
from multicorn.utils import log_to_postgres
import sys
from typing import Type, Dict

from samplingFdw.sampling_strategy import SamplingStragegy


class _SamplingStrategyRegistry:
    def __init__(self) -> None:
        self.registry = {}  # type: Dict[str, SamplingStrategy]

    def register(self, name: str):
        def wrapper(sampling_strategy: Type[SamplingStragegy]
                    ) -> Type[SamplingStragegy]:
            if not isinstance(sampling_strategy, SamplingStragegy):
                log_to_postgres(
                    logging.ERROR,
                    "Sampling stragegy {} should be a subclass of class SamplingStragegy".
                    format(name))
            self._set_strategy(name, sampling_strategy())
            return sampling_strategy

        return wrapper

    def _set_strategy(self, name: str, strategy: SamplingStragegy) -> None:
        if name in self.registry:
            log_to_postgres(logging.WARNING,
                            "Overwriting registered strategy for " + name)
        self.registry[name] = strategy

    def get_strategy(self, name: str) -> SamplingStragegy:
        if name not in self.registry:
            log_to_postgres(logging.ERROR,
                            "No strategy registered for " + name)
        return self.registry[name]


SamplingStrategyRegistry = _SamplingStrategyRegistry()
