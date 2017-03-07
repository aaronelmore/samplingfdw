import itertools
import logging
from multicorn import ForeignDataWrapper, ColumnDefinition, Qual, SortKey
from multicorn.utils import log_to_postgres
import psycopg2
from typing import Dict, List, Iterable, Any

from samplingfdw.sampling_strategy_registry import SamplingStrategyRegistry


class SamplingFdw(ForeignDataWrapper):
    registry = {}  # type: Dict[str, SamplingFdw]

    def __init__(self,
                 options: Dict[str, str],
                 columns: Dict[str, ColumnDefinition]) -> None:
        super().__init__(options, columns)
        for option in ["sampling_strategy", "name", "table_name"]:
            if option not in options:
                log_to_postgres(
                    "The options passed to {} should contain a {} field".
                    format(self.__class__.__name__, option), logging.ERROR)

        self.local_options = {}  # type: Dict[str, str]
        self.remote_options = {}  # type: Dict[str, str]
        for connection_param in ["dbname", "user", "password", "host", "port"]:
            if "local_" + connection_param in options:
                self.local_options[connection_param] = (
                    options["local_" + connection_param])
            if "remote_" + connection_param in options:
                self.remote_options[connection_param] = (
                    options["remote_" + connection_param])
        self._local_connection = None  # type: psycopg2.connection
        self._remote_connection = None  # type: psycopg2.connection

        self.table_name = options["table_name"]
        self.sampling_strategy = SamplingStrategyRegistry.get_strategy(
            options["sampling_strategy"])(self.table_name, options, columns)
        self.sampling_fdw_registry[options["name"]] = self

        with self.remote_connection:
            with self.local_connection:
                self.rows_stored_locally = self.sampling_strategy.on_open(
                    self.remote_connection.cursor(),
                    self.local_connection.cursor())

    def execute(self,
                quals: List[Qual],
                columns: List[str],
                pathkeys: List[SortKey]=[]) -> Iterable[Any]:
        with self.local_connection:
            local_results = self.sampling_strategy.fetch_locally(
                self.local_connection.cursor(), quals, columns, pathkeys)
        if local_results is not None:
            return local_results

        with self.remote_connection:
            remote_results = self.sampling_strategy.fetch_remotely(
                self.remote_connection.cursor(), quals, columns, pathkeys)
        remote_results, remote_results_copy = itertools.tee(remote_results)
        with self.local_connection:
            self.rows_stored_locally += (
                self.sampling_strategy.store_results_locally(
                    self.local_connection.cursor(), remote_results))
        return remote_results_copy

    @property
    def rowid_column(self) -> str:
        return self.sampling_strategy.rowid_column

    def insert(self, values: Dict[str, Any]) -> Dict[str, Any]:
        with self.local_connection:
            self.rows_stored_locally += self.sampling_strategy.insert_locally(
                self.local_connection.cursor(), values)

        with self.remote_connection:
            return self.sampling_strategy.insert_remotely(
                self.remote_connection.cursor(), values)

    def update(self, oldvalues: Dict[str, Any],
               newvalues: Dict[str, Any]) -> Dict[str, Any]:
        with self.local_connection:
            self.rows_stored_locally += self.sampling_strategy.update_locally(
                self.local_connection.cursor(), oldvalues, newvalues)

        with self.remote_connection:
            return self.sampling_strategy.update_remotely(
                self.remote_connection.cursor(), oldvalues, newvalues)

    def delete(self, oldvalues: Dict[str, Any]) -> None:
        with self.local_connection:
            self.rows_stored_locally -= self.sampling_strategy.delete_locally(
                self.local_connection.cursor(), oldvalues)

        with self.remote_connection:
            self.sampling_strategy.delete_remotely(
                self.remote_connection.cursor(), oldvalues)

    @property
    def local_connection(self) -> psycopg2.connection:
        if self._local_connection is None:
            self._local_connection = psycopg2.connect(**self.local_options)
        return self._local_connection

    @property
    def remote_connection(self) -> psycopg2.connection:
        if self._remote_connection is None:
            self._remote_connection = psycopg2.connect(**self.remote_options)
        return self._remote_connection


class MetadataFdw(ForeignDataWrapper):
    def __init__(self,
                 options: Dict[str, str],
                 columns: Dict[str, ColumnDefinition]) -> None:
        super().__init__(options, columns)

    def execute(self,
                quals: List[Qual],
                columns: List[str],
                pathkeys: List[SortKey]=[]) -> Iterable[Any]:
        for name, sampling_fdw in SamplingFdw.registry.items():
            yield {
                "name": name,
                "table_name": sampling_fdw.table_name,
                "rows_stored_locally": sampling_fdw.rows_stored_locally
            }

    @property
    def rowid_column(self) -> str:
        return "name"

    def insert(self, values: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError(
            "{} does not support insertion".format(self.__class__.__name__))

    def delete(self, oldvalues: Dict[str, Any]) -> None:
        raise NotImplementedError(
            "{} does not support deletion".format(self.__class__.__name__))

    def update(self, oldvalues: Dict[str, Any],
               newvalues: Dict[str, Any]) -> Dict[str, Any]:
        for key in oldvalues:
            if (oldvalues[key] != newvalues[key] and
                    key != "rows_stored_locally"):
                log_to_postgres(
                    "The only column that can be modified in this FDW is rows_stored_locally",
                    logging.ERROR)

        sampling_fdw = SamplingFdw.registry[oldvalues["name"]]
        with sampling_fdw.remote_connection:
            with sampling_fdw.local_connection:
                sampling_fdw.rows_stored_locally = (
                    sampling_fdw.sampling_strategy.fetch_more_rows(
                        sampling_fdw.remote_connection.cursor(),
                        sampling_fdw.local_connection.cursor(),
                        oldvalues["rows_stored_locally"],
                        newvalues["rows_stored_locally"]))
        newvalues["rows_stored_locally"] = sampling_fdw.rows_stored_locally
        return newvalues
