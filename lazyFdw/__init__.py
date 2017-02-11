import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
import pkg_resources
import psycopg2

EXECUTION_STRATEGY = pkg_resources.resource_string(__name__,
                                                   "execution_strategy.sql")


class LazyForeignDataWrapper(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(LazyForeignDataWrapper, self).__init__(options, columns)
        self.options = options
        self.columns = columns
        self._connection = None

    def execute(self, quals, columns):
        cur = self.connection.cursor()
        for command in EXECUTION_STRATEGY.split(";"):
            if command.strip() != "":
                cur.execute(command)
                for record in cur:
                    yield dict(zip(self.columns, record))

    @property
    def connection(self):
        if self._connection is None:
            self._connection = psycopg2.connect(**self.options)
        return self._connection
