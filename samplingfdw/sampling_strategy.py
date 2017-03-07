from multicorn import Qual, SortKey, ColumnDefinition
import psycopg2
from typing import List, Iterable, Any, Optional, Dict


class SamplingStrategy:
    def __init__(self,
                 table_name: str,
                 options: Dict[str, str],
                 columns: Dict[str, ColumnDefinition]) -> None:
        self.table_name = table_name
        self.options = options
        self.columns = columns

    def execute_fetch_statement(self,
                                cursor: psycopg2.cursor,
                                quals: List[Qual],
                                columns: List[str],
                                pathkeys: List[SortKey]) -> None:
        select_clause = ", ".join(columns) if len(columns) > 0 else "*"
        statement = "SELECT {} FROM {}".format(select_clause, self.table_name)
        if len(quals) > 0:
            statement += " " + " AND ".join(str(qual) for qual in quals)
        cursor.execute(statement + ";")

    def execute_insert_statement(self,
                                 cursor: psycopg2.cursor,
                                 values: Dict[str, Any]) -> None:
        statement = ("INSERT INTO {} {} VALUES {}".format(
            self.table_name,
            tuple(values.keys()), tuple(["%s" for _ in values])))
        cursor.execute(statement, values.values())

    def execute_update_statement(self,
                                 cursor: psycopg2.cursor,
                                 oldvalues: Dict[str, Any],
                                 newvalues: Dict[str, Any]) -> None:
        newvalue_clause = ", ".join("{} = %s".format(value)
                                    for value in newvalues.keys())
        where_clause = " AND ".join("{} = %s".format(value)
                                    for value in oldvalues.keys())
        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name, newvalue_clause, where_clause)
        cursor.execute(statement,
                       list(newvalues.values()) + list(oldvalues.values()))

    def execute_delete_statement(self,
                                 cursor: psycopg2.cursor,
                                 oldvalues: Dict[str, Any]) -> None:
        where_clause = " AND ".join("{} = %s".format(value)
                                    for value in oldvalues.keys())
        statement = "DELETE FROM {} WHERE {}".format(self.table_name,
                                                     where_clause)
        cursor.execute(statement, oldvalues.values())

    def on_open(self,
                remote_cursor: psycopg2.cursor,
                local_cursor: psycopg2.cursor) -> int:
        return 0

    def fetch_locally(self,
                      local_cursor: psycopg2.cursor,
                      quals: List[Qual],
                      columns: List[str],
                      pathkeys: List[SortKey]=[]) -> Optional[Iterable[Any]]:
        return None

    def fetch_remotely(self,
                       remote_cursor: psycopg2.cursor,
                       quals: List[Qual],
                       columns: List[str],
                       pathkeys: List[SortKey]=[]) -> Optional[Iterable[Any]]:
        return None

    def store_results_locally(self,
                              local_cursor: psycopg2.cursor,
                              fetch_results: Optional[Iterable[Any]]) -> int:
        return 0

    @property
    def rowid_column(self) -> str:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def insert_locally(self,
                       local_cursor: psycopg2.cursor,
                       values: Dict[str, Any]) -> int:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def insert_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        values: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def update_locally(self,
                       local_cursor: psycopg2.cursor,
                       oldvalues: Dict[str, Any],
                       newvalues: Dict[str, Any]) -> int:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def update_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        oldvalues: Dict[str, Any],
                        newvalues: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def delete_locally(self,
                       local_cursor: psycopg2.cursor,
                       oldvalues: Dict[str, Any]) -> int:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def delete_remotely(self,
                        remote_cursor: psycopg2.cursor,
                        oldvalues: Dict[str, Any]) -> None:
        raise NotImplementedError("{} does not support the writable API".
                                  format(self.__class__.__name__))

    def fetch_more_rows(self,
                        remote_cursor: psycopg2.cursor,
                        local_cursor: psycopg2.cursor,
                        oldvalue: int,
                        newvalue: int) -> int:
        return oldvalue
