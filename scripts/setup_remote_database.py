#!/usr/bin/env python2
"""
Script to set up a remote database.

This script must be run on the remote server under a user with permissions to
create tables.
When this is run, it will create a table named TABLE_NAME with the following command:

.. code-block:: sql
  CREATE TABLE table_name(
    id INTEGER PRIMARY KEY,
    str_column VARCHAR(3),
    int_column INTEGER
  );

This table will then be populated with 200000 rows, where 50% of the rows will
have str_column equal to foo, and 50% of the rows will have str_column equal to
bar. The values in int_column will be random values in the range [0, 10000].
"""

import psycopg2
import random

DBNAME = "postgres"
TABLE_NAME = "remote_table"
NUM_VALUES = 200000


def main():
    conn = psycopg2.connect(dbname=DBNAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE {}(
          id SERIAL PRIMARY KEY,
          str_column VARCHAR(3),
          int_column INTEGER);
        """.format(TABLE_NAME))
    for i in range(NUM_VALUES):
        str_column = "foo" if i % 2 == 0 else "bar"
        int_column = random.randint(0, 10000)
        cursor.execute(
            "INSERT INTO {} (str_column, int_column) VALUES (%s, %s)".format(
                TABLE_NAME), (str_column, int_column))
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
