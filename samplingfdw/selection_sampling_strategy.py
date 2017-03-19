import logging
from multicorn import Qual, SortKey
from multicorn.utils import log_to_postgres
import psycopg2
from typing import List, Iterable, Any, Dict, Optional

from samplingfdw.sampling_strategy import SamplingStrategy
from samplingfdw.sampling_strategy_registry import SamplingStrategyRegistry


@SamplingStrategyRegistry.register("selection_sampling_strategy")
class SelectionSamplingStrategy(SamplingStrategy):
    """SamplingStrategy that stores only specified values of a specified column locally.

    Accepted options:
        column        -- The column which the supplied column_values are for.
        column_values -- The comma delimited values for the supplied column that
                         will be selected to be stored in the local database.
        primary_key   -- (optional) Identifies a column which is a primary key
                         in the remote RDBMS. This options is required for
                         INSERT, UPDATE and DELETE operations.
    """

    def on_open(self, remote_cursor, local_cursor):
        # type: (psycopg2.cursor, psycopg2.cursor) -> int
        """If the table table_name.local does not exist in the local database,
        creates the table and loads it with all rows that have a value in
        column_values for the column specified in options.

        This function returns the number of rows currently in the local table.
        """
        for option in ["column", "column_values"]:
            if option not in self.options:
                log_to_postgres(
                    "The options passed to {} should contain a {} field".
                    format(self.__class__.__name__, option), logging.ERROR)
        if self.options["column"] not in self.columns:
            log_to_postgres(
                "Expected column {} to be in the list of requested columns for {}".
                format(self.options["column"],
                       self.__class__.__name__), logging.ERROR)

        self.local_table_name = "_local_" + self.table_name
        self.selection_quals = [
            Qual(self.options["column"], "=", column_value)
            for column_value in self.options["column_values"].split(",")
        ]
        if self.table_exists(local_cursor, self.local_table_name):
            return self.get_count(local_cursor, self.local_table_name)

        self.create_table(local_cursor, self.local_table_name,
                          self.columns.values())
        self.execute_fetch_statement(remote_cursor, self.table_name,
                                     self.selection_quals, None)
        self.insert_values(local_cursor, self.local_table_name, remote_cursor)
        return self.get_count(local_cursor, self.local_table_name)

    def fetch_locally(self, local_cursor, quals, columns, sortkeys=None):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> Optional[Iterable[Any]]
        """If one of the quals in the fetch query selects only rows with a
        value from column_values in column, the query can be run on the local
        databse, otherwise we need to run the query on the remote database.
        """
        for qual in self.selection_quals:
            if qual in quals:
                self.execute_fetch_statement(local_cursor,
                                             self.local_table_name, quals,
                                             columns, sortkeys)
                for result in local_cursor:
                    yield dict(zip(columns, result))

    def fetch_remotely(self, remote_cursor, quals, columns, sortkeys=None):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> Iterable[Any]
        """Executes the supplied query against the remote database and returns
        the result.
        """
        self.execute_fetch_statement(remote_cursor, self.table_name, quals,
                                     columns, sortkeys)
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

    def insert_locally(self, local_cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> int
        """If the value for column is one of the values in column_values, we
        need to insert into the local table.
        """
        column_values = self.options["column_values"].split(",")
        if values.get(self.options["column"], None) in column_values:
            return self.execute_insert_statement(local_cursor,
                                                 self.local_table_name, values)
        return 0

    def insert_remotely(self, remote_cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> Dict[str, Any]
        """Executes the supplied insert statement against the remote databse.
        """
        self.execute_insert_statement(remote_cursor, self.table_name, values)
        return values

    def update_locally(self, local_cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> int
        """If we are updating a row that has a value in column_values for
        column, we need to update it locally.
        """
        rows_added = 0
        column = self.options["column"]
        column_values = self.options["column_values"].split(",")
        if (oldvalues.get(column, None) in column_values) or (
                newvalues.get(column, None) in column_values):
            rows_added = self.execute_update_statement(
                local_cursor, self.local_table_name, oldvalues, newvalues)

        if (oldvalues.get(column, None) in column_values) and (
                newvalues.get(column, None) in column_values):
            return 0
        if oldvalues.get(column, None) in column_values:
            return -rows_added
        if newvalues.get(column, None) in column_values:
            return rows_added
        return 0

    def update_remotely(self, remote_cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
        """Executes the supplied update statement against the remote databse."""
        self.execute_update_statement(remote_cursor, self.table_name,
                                      oldvalues, newvalues)
        return newvalues

    def delete_locally(self, local_cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> int
        """If we are deleting a row tthat has a value in column_values for
        column, we need to delete it locally.
        """
        column_values = self.options["column_values"].split(",")
        if oldvalues.get(self.options["column"], None) in column_values:
            return self.execute_delete_statement(
                local_cursor, self.local_table_name, oldvalues)
        return 0

    def delete_remotely(self, remote_cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> None
        """Executes the supplied delete statement against the remote databse.
        """
        self.execute_delete_statement(remote_cursor, self.table_name,
                                      oldvalues)
