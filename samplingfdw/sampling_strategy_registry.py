import logging
from multicorn.utils import log_to_postgres
import sys
from typing import Type, Dict, Callable

from samplingfdw.sampling_strategy import SamplingStrategy


class _SamplingStrategyRegistry:
    """A registry that allows SamplingStrategy classes to be registered and
    looked up.
    """

    def __init__(self):  # type: () -> None
        self.registry = {}  # type: Dict[str, Type[SamplingStrategy]]

    def register(self, name):
        # type: (str) -> Callable[[Type[SamplingStrategy]], Type[SamplingStrategy]]
        """A decorator that can be used to register a SamplingStrategy class
        using the supplied name as a key.
        """

        def wrapper(sampling_strategy):
            # type: (Type[SamplingStrategy]) -> Type[SamplingStrategy]
            if name in self.registry:
                log_to_postgres("Overwriting registered strategy for " + name,
                                logging.WARNING)
            self.registry[name] = sampling_strategy
            return sampling_strategy

        return wrapper

    def get_strategy(self, name):  # type: (str) -> Type[SamplingStrategy]
        """Returns the strategy registered for the supplied name, if one
        exists.
        """
        if name not in self.registry:
            log_to_postgres("No strategy registered for " + name,
                            logging.ERROR)
        return self.registry[name]


SamplingStrategyRegistry = _SamplingStrategyRegistry()
