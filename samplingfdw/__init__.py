"""
Purpose
-------

This FDW can be used to access data stored in a remote Postgres database.
Users can define strategies for caching and retrieving data locally.

A secondary FDW, ``MetadataFdw`` can be used to view and modify metadata about
the active ``ForeignDataWrapper``s.

Required Options
~~~~~~~~~~~~~~~~

``sampling_strategy``
  The name of the sampling strategy to use.
  The sampling strategy must be registered by using the
  ``SamplingStrategyRegistry.register`` function for it to be recognized.

``name``
  The name of the FDW connection, for use in ``MetadataFdw``.

``table_name``
  The table name in the remote Postgres database.

When defining the table, the local column names will be used to retrieve the
remote column data.
Moreover, the local column types will be used to interpret
the results in the remote table.

Connection Options
~~~~~~~~~~~~~~~~~~

``local_dbname``
  The local database name.

``local_user``
  The local username.

``local_password``
  The local password.

``local_host``
  The local host.

``local_port``
  The local port.

``remote_dbname``
  The remote database name.

``remote_user``
  The remote username.

``remote_password``
  The remote password.

``remote_host``
  The remote host.

``remote_port``
  The remote port.

Usage example
-------------

For a connection to a remote database:

.. code-block:: sql
  CREATE SERVER sampling_srv foreign data wrapper multicorn options (
      wrapper 'multicorn.samplingfdw.SamplingFdw'
  );
  create foreign table my_table (
    column1 integer,
    column2 varchar
  ) server sampling_srv options (
    sampling_strategy 'remote_sampling_strategy',
    name 'my_sampling_fdw',
    tablename 'table',
    local_dname 'local_db',
    remote_host 'host',
    remote_dbname 'remote_db'
  );

For a connection to a listing of all the active local ``SamplingFdw``s:

.. code-block:: sql
  CREATE SERVER metadata_srv foreign data wrapper multicorn options (
      wrapper 'multicorn.samplingfdw.MetadataFdw'
  );
  create foreign table metadata_table (
    name varchar,
    table_name varchar,
    rows_stored_locally integer
  ) server metadata_srv;

"""

import glob
import itertools
import logging
from multicorn import ForeignDataWrapper, ColumnDefinition, Qual, SortKey
from multicorn.utils import log_to_postgres
import os.path
import psycopg2
from typing import Dict, List, Iterable, Any

from samplingfdw.sampling_strategy_registry import SamplingStrategyRegistry

# Ensure that every other python file in this directory gets included in this
# file, so every registered SamplingStrategy will be found.
_modules = glob.glob(os.path.dirname(__file__) + "/*.py")
__all__ = [os.path.basename(f)[:-3] for f in _modules if os.path.isfile(f)]


class SamplingFdw(ForeignDataWrapper):
    """A sampling foregin data wrapper that fetches data from a foreign database
    and caches it locally.

    This foregin data wrapper allows the user to specify how data is
    sampled from a remote server and cached locally.
    """
    # Stores all active SamplingFdws so that MetadataFdw can list them
    registry = {}  # type: Dict[str, SamplingFdw]

    def __init__(self, options, columns):
        # type: (Dict[str, str], Dict[str, ColumnDefinition]) -> None
        super(SamplingFdw, self).__init__(options, columns)
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
        self.registry[options["name"]] = self

        with self.remote_connection, self.local_connection:
            self.rows_stored_locally = self.sampling_strategy.on_open(
                self.remote_connection.cursor(),
                self.local_connection.cursor())

    def execute(self, quals, columns, pathkeys=[]):
        # type: (List[Qual], List[str], List[SortKey]) -> Iterable[Any]
        """Fetches data from the FDW.

        This function will first query the local database using the
        user-supplied sampling strategy.
        If no results are returned, the remote database will be queried, and
        the results of the query will be inserted into the local database using
        the sampling strategy, and returned to the user.
        """
        with self.local_connection:
            local_results = self.sampling_strategy.fetch_locally(
                self.local_connection.cursor(), quals, columns, pathkeys)
        if local_results is not None:
            return local_results

        with self.remote_connection, self.local_connection:
            remote_results = self.sampling_strategy.fetch_remotely(
                self.remote_connection.cursor(), quals, columns, pathkeys)
            remote_results, remote_results_copy = itertools.tee(remote_results)
            self.rows_stored_locally += (
                self.sampling_strategy.store_results_locally(
                    self.local_connection.cursor(), remote_results))
        return remote_results_copy

    @property
    def rowid_column(self):  # type: () -> str
        """Primary key column of the remote database."""
        return self.sampling_strategy.rowid_column

    def insert(self, values):  # type: (Dict[str, Any]) -> Dict[str, Any]
        """This function will insert the supplied values into both the local
        and remote database using the user-defined sampling strategy.
        """
        with self.local_connection, self.remote_connection:
            self.rows_stored_locally += self.sampling_strategy.insert_locally(
                self.local_connection.cursor(), values)

            return self.sampling_strategy.insert_remotely(
                self.remote_connection.cursor(), values)

    def update(self, oldvalues, newvalues):
        # type: (Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
        """This function will update the supplied values in both the local and
        remote database using the user-defined sampling strategy.
        """
        with self.local_connection, self.remote_connection:
            self.rows_stored_locally += self.sampling_strategy.update_locally(
                self.local_connection.cursor(), oldvalues, newvalues)

            return self.sampling_strategy.update_remotely(
                self.remote_connection.cursor(), oldvalues, newvalues)

    def delete(self, oldvalues):  # type: (Dict[str, Any]) -> None
        """This function will delete the supplied values in both the local and
        remote database using the user-defined sampling strategy.
        """
        with self.local_connection, self.remote_connection:
            self.rows_stored_locally -= self.sampling_strategy.delete_locally(
                self.local_connection.cursor(), oldvalues)

            self.sampling_strategy.delete_remotely(
                self.remote_connection.cursor(), oldvalues)

    @property
    def local_connection(self):  # type: () -> psycopg2.connection
        """Returns a connection to the local database."""
        if self._local_connection is None:
            self._local_connection = psycopg2.connect(**self.local_options)
        return self._local_connection

    @property
    def remote_connection(self):  # type: () -> psycopg2.connection
        """Returns a connection to the remote database."""
        if self._remote_connection is None:
            self._remote_connection = psycopg2.connect(**self.remote_options)
        return self._remote_connection


class MetadataFdw(ForeignDataWrapper):
    """A foreign data wrapper that stores metadata about all active SamplingFdws.

    Available columns are:
        name                -- the name supplied when opening the SamplingFdw
        table_name          -- the name of the table supplied when opening the SamplingFdw
        rows_stored_locally -- the number of rows stored in the local database for the SamplingFdw
    """

    def execute(self, quals, columns, sortkeys=None):
        # type: (List[Qual], List[str], List[SortKey]) -> Iterable[Any]
        """Fetches metadata about all of the active SamplingFdws"""
        for name, sampling_fdw in SamplingFdw.registry.items():
            yield {
                "name": name,
                "table_name": sampling_fdw.table_name,
                "rows_stored_locally": sampling_fdw.rows_stored_locally
            }

    @property
    def rowid_column(self):  # type: () -> str
        """The primary key column of the FDW."""
        return "name"

    def insert(self, values):  # type: (Dict[str, Any]) -> Dict[str, Any]
        raise NotImplementedError(
            "{} does not support insertion".format(self.__class__.__name__))

    def delete(self, oldvalues):  # type: (Dict[str, Any]) -> None
        raise NotImplementedError(
            "{} does not support deletion".format(self.__class__.__name__))

    def update(self, oldvalues, newvalues):
        # type: (Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
        """This function allows users to update the value of
        rows_stored_locally, which notifies the sampling strategy that it
        should request more rows from the remote database and store them in the
        local database.
        """
        for key in oldvalues:
            if (oldvalues[key] != newvalues[key] and
                    key != "rows_stored_locally"):
                log_to_postgres(
                    "The only column that can be modified in this FDW is rows_stored_locally",
                    logging.ERROR)

        sampling_fdw = SamplingFdw.registry[oldvalues["name"]]
        oldcount = oldvalues["rows_stored_locally"]
        newcount = newvalues["rows_stored_locally"]
        with sampling_fdw.remote_connection, sampling_fdw.local_connection:
            remote_cursor = sampling_fdw.remote_connection.cursor()
            local_cursor = sampling_fdw.local_connection.cursor()
            sampling_fdw.rows_stored_locally = (
                sampling_fdw.sampling_strategy.fetch_more_rows(
                    remote_cursor, local_cursor, oldcount, newcount))

        newvalues["rows_stored_locally"] = sampling_fdw.rows_stored_locally
        return newvalues
