#!/usr/bin/env python2
"""
Sets up a SamplingFdw with a RemoteSamplingStrategy against a table in a remote server.
"""

import psycopg2
import sys

DBNAME = "postgres"
REMOTE_TABLE_NAME = "remote_table"
LOCAL_TABLE_NAME = "local_table"


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
            "usage: {} sampling_fdw_name remote_host\n".format(sys.argv[0]))
        sys.exit(1)

    conn = psycopg2.connect(dbname=DBNAME)
    cursor = conn.cursor()

    cursor.execute("SELECT extname FROM pg_extension")
    for row in cursor:
        if row[0] == "multicorn":
            break
    else:
        cursor.execute("CREATE EXTENSION multicorn")

    cursor.execute("SELECT srvname FROM pg_foreign_server")
    for row in cursor:
        if row[0] == "sampling_srv":
            break
    else:
        cursor.execute("""
                CREATE SERVER sampling_srv FOREIGN DATA WRAPPER multicorn options(
                  wrapper 'samplingfdw.SamplingFdw'
                );
                """)

    cursor.execute("""
            CREATE FOREIGN TABLE {} (
              id INTEGER,
              str_column VARCHAR(3),
              int_column INTEGER
            ) SERVER sampling_srv OPTIONS (
              sampling_strategy 'remote_sampling_strategy',
              primary_key 'id',
              table_name '{}',
              local_dbname '{}',
              remote_dbname '{}',
              name '{}',
              remote_host '{}'
            );
    """.format(LOCAL_TABLE_NAME, REMOTE_TABLE_NAME, DBNAME, DBNAME, sys.argv[
        1], sys.argv[2]))
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
