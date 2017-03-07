import logging
from multicorn.utils import log_to_postgres
import sys
from typing import Type, Dict

from samplingfdw.sampling_strategy import SamplingStrategy


class _SamplingStrategyRegistry:
    def __init__(self) -> None:
        self.registry = {}  # type: Dict[str, Type[SamplingStrategy]]

    def register(self, name: str):
        def wrapper(sampling_strategy: Type[SamplingStrategy]
                    ) -> Type[SamplingStrategy]:
            if not isinstance(sampling_strategy, SamplingStrategy):
                log_to_postgres(
                    logging.ERROR,
                    "Sampling stragegy {} should be a subclass of class SamplingStrategy".
                    format(name))
            self._set_strategy(name, sampling_strategy)
            return sampling_strategy

        return wrapper

    def _set_strategy(self, name: str,
                      strategy: Type[SamplingStrategy]) -> None:
        if name in self.registry:
            log_to_postgres(logging.WARNING,
                            "Overwriting registered strategy for " + name)
        self.registry[name] = strategy

    def get_strategy(self, name: str) -> Type[SamplingStrategy]:
        if name not in self.registry:
            log_to_postgres(logging.ERROR,
                            "No strategy registered for " + name)
        return self.registry[name]


SamplingStrategyRegistry = _SamplingStrategyRegistry()
