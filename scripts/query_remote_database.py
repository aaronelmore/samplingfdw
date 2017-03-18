#!/usr/bin/env python2
"""
Runs a sequence of queries against a table and logs the time taken for each query.

The amount of read vs. write queries can be changed with a command line option.
Read queries are made up of equality queries on str_column and range queries on
int_column.  Equality queries happen 90% of the time, and range queries happen
10% of the time. Write queries insert a single row into the database. By
default, 100 queries are run, and the total time taken for all queries is
measured.
"""

import psycopg2
import random
import sys
import time

DBNAME = "postgres"
TABLE_NAME = "local_table"
NUM_QUERIES = 100


def randbool():
    return random.choice([True, False])


def main():
    if len(sys.argv) < 1:
        sys.stderr.write("usage: {} read_percentage\n".format(sys.argv[0]))
        sys.exit(1)

    read_percentage = float(sys.argv[1]) * 100.0
    conn = psycopg2.connect(dbname=DBNAME)
    cursor = conn.cursor()

    iters = range(NUM_QUERIES)
    random.shuffle(iters)
    start_time = time.time()
    for i in iters:
        if i < read_percentage:
            # read query
            if random.random < 0.75:
                # equality query
                if randbool():
                    # str_column = 'foo'
                    cursor.execute("SELECT * FROM {} WHERE str_column = 'foo'")
                    for _ in cursor:
                        pass
                else:
                    # str_column = 'bar'
                    cursor.execute("SELECT * FROM {} WHERE str_column = 'bar'")
                    for _ in cursor:
                        pass
            else:
                #range query
                int_column_min = random.randint(0, 10000)
                cursor.execute("SELECT * FROM {} WHERE int_column > %s",
                               int_column_min)
                for _ in cursor:
                    pass
        else:
            # write query
            str_column = "foo" if randbool() else "bar"
            int_column = random.randint(0, 10000)
            cursor.execute(
                "INSERT INTO {} (str_column, int_column) VALUES (%s, %s)".
                format(TABLE_NAME), (str_column, int_column))
    print(time.time() - start_time)


if __name__ == "__main__":
    main()
