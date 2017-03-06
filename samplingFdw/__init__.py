import logging
from multicorn import ForeignDataWrapper, ColumnDefinition, Qual, SortKey
from multicorn.utils import log_to_postgres
import psycopg2
from typing import Dict, List, Iterable, Sequence, Any

from samplingFdw.sampling_strategy_registry import SamplingStrategyRegistry


class SamplingForeignDataWrapper(ForeignDataWrapper):
    def __init__(self,
                 options: Dict[str, str],
                 columns: Dict[str, ColumnDefinition]) -> None:
        super(LazyForeignDataWrapper, self).__init__(options, columns)
        self.options = options
        self.columns = columns
        self._local_connection = None  # type: psycopg2.connection
        self._remote_connection = None  # type: psycopg2.connection

        if "sampling_strategy" not in options:
            log_to_postgres(
                logging.ERROR,
                "The options passed to {} should contain a sampling_strategy field".
                format(self.__class__.__name__))
        self._sampling_strategy = SamplingStrategyRegistry.get_strategy(
            options["sampling_strategy"])
        del self.options["sampling_strategy"]

    def execute(self,
                quals: List[Qual],
                columns: List[str],
                pathkeys: List[SortKey]=[]) -> Iterable[Sequence[Any]]:
        local_cur = self.local_connection.cursor()
        local_results = self._sampling_strategy.fetch_locally(
            local_cur, quals, columns, pathkeys)
        if local_results is not None:
            return local_results

        remote_cur = self.remote_connection.cursor()
        return self._sampling_strategy.fetch_remotely(remote_cur, quals,
                                                      columns, pathkeys)

    @property
    def local_connection(self) -> psycopg2.connection:
        if self._local_connection is None:
            self._local_connection = psycopg2.connect(dbname="postgres")
        return self._local_connection

    @property
    def remote_connection(self) -> psycopg2.connection:
        if self._remote_connection is None:
            self._remote_connection = psycopg2.connect(**self.options)
        return self._remote_connection
