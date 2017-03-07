import logging
from multicorn.utils import log_to_postgres
import sys
from typing import Type, Dict, Callable

from samplingfdw.sampling_strategy import SamplingStrategy


class _SamplingStrategyRegistry:
    def __init__(self):  # type: () -> None
        self.registry = {}  # type: Dict[str, Type[SamplingStrategy]]

    def register(self, name):
        # type: (str) -> Callable[[Type[SamplingStrategy]], Type[SamplingStrategy]]
        def wrapper(sampling_strategy):
            # type: (Type[SamplingStrategy]) -> Type[SamplingStrategy]
            self._set_strategy(name, sampling_strategy)
            return sampling_strategy

        return wrapper

    def _set_strategy(self, name, strategy):
        # type: (str, Type[SamplingStrategy]) -> None
        if name in self.registry:
            log_to_postgres("Overwriting registered strategy for " + name,
                            logging.WARNING)
        self.registry[name] = strategy

    def get_strategy(self, name):  # type: (str) -> Type[SamplingStrategy]
        if name not in self.registry:
            log_to_postgres("No strategy registered for " + name,
                            logging.ERROR)
        return self.registry[name]


SamplingStrategyRegistry = _SamplingStrategyRegistry()
