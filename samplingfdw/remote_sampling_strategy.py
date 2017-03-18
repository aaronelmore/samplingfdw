from multicorn import Qual, SortKey
from multicorn.utils import log_to_postgres
import psycopg2
from typing import List, Iterable, Any, Dict

from samplingfdw.sampling_strategy import SamplingStrategy
from samplingfdw.sampling_strategy_registry import SamplingStrategyRegistry


@SamplingStrategyRegistry.register("remote_sampling_strategy")
class RemoteSamplingStrategy(SamplingStrategy):
    """A sample implementation of SamplingStrategy that only makes queries
    against the remote database and stores nothing locally.

    Accepted options:
        primary_key -- Identifies a column which is a primary key in the remote RDBMS.
                       This options is required for INSERT, UPDATE and DELETE operations
    """

    def fetch_remotely(self, remote_cursor, quals, columns, sortkeys=None):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> Iterable[Any]
        """Executes the supplied query against the remote database and returns
        the result.
        """
        self.execute_fetch_statement(remote_cursor, quals, columns, sortkeys)
        for result in remote_cursor:
            yield dict(zip(columns, result))

    @property
    def rowid_column(self):  # type: () -> str
        """Returns the 'primary_key' option if it is specified by the user."""
        row_id_column = self.options.get("primary_key", None)
        if row_id_column is None:
            log_to_postgres(
                "You need to declare a primary_key option in order to use the write API"
            )
        return row_id_column

    def insert_remotely(self, remote_cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> Dict[str, Any]
        """Executes the supplied insert statement against the remote databse.
        """
        self.execute_insert_statement(remote_cursor, values)
        return values

    def update_remotely(self, remote_cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
        """Executes the supplied update statement against the remote databse."""
        self.execute_update_statement(remote_cursor, oldvalues, newvalues)
        return newvalues

    def delete_remotely(self, remote_cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> None
        """Executes the supplied delete statement against the remote databse.
        """
        self.execute_delete_statement(remote_cursor, oldvalues)
