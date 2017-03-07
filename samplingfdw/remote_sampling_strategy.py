from multicorn import Qual, SortKey
from multicorn.utils import log_to_postgres
import psycopg2
from typing import List, Iterable, Any, Optional, Dict

from samplingfdw.sampling_strategy import SamplingStrategy
from samplingfdw.sampling_strategy_registry import SamplingStrategyRegistry


@SamplingStrategyRegistry.register("remote_sampling_strategy")
class RemoteSamplingStrategy(SamplingStrategy):
    def fetch_remotely(self,
                       remote_cursor: psycopg2.cursor,
                       quals: List[Qual],
                       columns: List[str],
                       pathkeys: List[SortKey]=[]) -> Optional[Iterable[Any]]:
        self.execute_fetch_statement(remote_cursor, quals, columns, pathkeys)
        return remote_cursor

    @property
    def rowid_column(self) -> str:
        row_id_column = self.options.get("primary_key", None)
        if row_id_column is None:
            log_to_postgres(
                "You need to declare a primary key option in order to use the write API"
            )
        return row_id_column

    def insert_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        values: Dict[str, Any]) -> Dict[str, Any]:
        self.execute_insert_statement(remote_cursor, values)
        return values

    def update_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        oldvalues: Dict[str, Any],
                        newvalues: Dict[str, Any]) -> Dict[str, Any]:
        self.execute_update_statement(remote_cursor, oldvalues, newvalues)
        return newvalues

    def delete_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        oldvalues: Dict[str, Any]) -> None:
        self.execute_delete_statement(remote_cursor, oldvalues)
