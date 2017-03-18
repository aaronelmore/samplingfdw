from multicorn import Qual, SortKey, ColumnDefinition
import psycopg2
from typing import List, Iterable, Any, Optional, Dict


class SamplingStrategy(object):
    """Subclasses of this class can be plugged in to SamplingFdw to determine
    what operations get executed on the local server, and what operations get
    executed on the remote server when a SQL statement is executed on the FDW.
    """

    def __init__(self, table_name, options, columns):
        # type: (str, Dict[str, str], Dict[str, ColumnDefinition]) -> None
        self.table_name = table_name
        self.options = options
        self.columns = columns

    def execute_fetch_statement(self, cursor, quals, columns, sortkeys):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> None
        """Converts the supplied quals, columns, and sortkeys to a fetch
        statement, and executes it on the supplied cursor.
        """
        select_clause = ", ".join(columns) if len(columns) > 0 else "*"
        statement = "SELECT {} FROM {}".format(select_clause, self.table_name)
        if len(quals) > 0:
            statement += " " + " AND ".join(str(qual) for qual in quals)
        cursor.execute(statement + ";")

    def execute_insert_statement(self, cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> None
        """Converts the supplied values into an insert statement and executes
        it on the supplied cursor.
        """
        statement = ("INSERT INTO {} {} VALUES {}".format(
            self.table_name,
            tuple(values.keys()), tuple(["%s" for _ in values])))
        cursor.execute(statement, values.values())

    def execute_update_statement(self, cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> None
        """Converts the supplied values into an update statement and executes
        it on the supplied cursor.
        """
        newvalue_clause = ", ".join("{} = %s".format(value)
                                    for value in newvalues.keys())
        where_clause = " AND ".join("{} = %s".format(value)
                                    for value in oldvalues.keys())
        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name, newvalue_clause, where_clause)
        cursor.execute(statement,
                       list(newvalues.values()) + list(oldvalues.values()))

    def execute_delete_statement(self, cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> None
        """Converts the supplied values into a delete statement and executes it
        on the supplied cursor.
        """
        where_clause = " AND ".join("{} = %s".format(value)
                                    for value in oldvalues.keys())
        statement = "DELETE FROM {} WHERE {}".format(self.table_name,
                                                     where_clause)
        cursor.execute(statement, oldvalues.values())

    def on_open(self, remote_cursor, local_cursor):
        # type: (psycopg2.cursor, psycopg2.cursor) -> int
        """This function gets executed when the SamplingFdw is first created.

        This can be used to ensure that all required caching structures are set
        up in the local database and the initial sampled data is stored in the
        local database.

        It is important to note the ForeignDataWrapper objects are short-lived.
        This means they are created when the first query is made against the
        FDW, and destroyed when the connection to the Postgres database is
        closed.
        """
        return 0

    def fetch_locally(self, local_cursor, quals, columns, sortkeys=None):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> Optional[Iterable[Any]]
        """This function is used to retrieve results from the local database.

        If the given query can be resolved by the information in the local
        database, the result of the query is returned.
        Otherwise, this function can return None to attempt to fetch the data
        from the remote database.
        """
        return None

    def fetch_remotely(self, remote_cursor, quals, columns, sortkeys=None):
        # type: (psycopg2.cursor, List[Qual], List[str], List[SortKey]) -> Iterable[Any]
        """This function is used to retrieve results from the remote database.
        """
        return None

    def store_results_locally(self, local_cursor, fetch_results):
        # type: (psycopg2.cursor, Iterable[Any]) -> int
        """Inserts results retrieved from the remote database into the local
        database.

        Whenever query results are retrieved from the remote databse, this
        function is called to insert the results into the local database.
        This function returns the number of rows added to the local database.
        """
        return 0

    @property
    def rowid_column(self):  # type: () -> str
        """Primary key column of the remote database."""
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def insert_locally(self, local_cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> int
        """Inserts a row into the local database with the supplied values for
        its columns.

        This function returns the number of rows added to the database.
        """
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def insert_remotely(self, remote_cursor, values):
        # type: (psycopg2.cursor, Dict[str, Any]) -> Dict[str, Any]
        """Inserts a row into the remote database with the supplied values for
        its columns.

        This function returns the values inserted into the databbase.
        """
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def update_locally(self, local_cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> int
        """Updates a row in the local database using the supplied values.

        This function returns the number of rows added to the database, if new
        rows were added.
        """
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def update_remotely(self, remote_cursor, oldvalues, newvalues):
        # type: (psycopg2.cursor, Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
        """Updates a row in the remote database using the supplied values.

        This function returns the new values in the database.
        """
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def delete_locally(self, local_cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> int
        """Deletes rows in the local database matching the supplied values.

        This function returns the number of rows deleted from the local
        database.
        """
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def delete_remotely(self, remote_cursor, oldvalues):
        # type: (psycopg2.cursor, Dict[str, Any]) -> None
        """Deletes rows in the remote database matching the supplied values."""
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def fetch_more_rows(self, remote_cursor, local_cursor, oldvalue, newvalue):
        # type: (psycopg2.cursor, psycopg2.cursor, int, int) -> int
        """Increases the size of the sample from the remote database that is
        stored in the local database.

        This is a callback that is called when a user updates the
        rows_stored_locally value in MetadataFdw.
        It is passed the old value of rows_stored_locally, the new value, and
        cursors to both the llocal and remote databases.
        This function is expected to enlarge the number of rows of the remote
        table that are stored in the local table to be at least newvalue.
        This function returns the number of rows stored in the local table
        after it is run.
        """
        return oldvalue
