import abc
from multicorn import Qual, SortKey
import psycopg2
from typing import List, Iterable, Sequence, Any, Optional


class SamplingStragegy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def fetch_locally(
            self,
            cursor: psycopg2.cursor,
            quals: List[Qual],
            columns: List[str],
            pathkeys: List[SortKey]=[]) -> Optional[Iterable[Sequence[Any]]]:
        pass

    @abc.abstractmethod
    def fetch_remotely(
            self,
            cursor: psycopg2.cursor,
            quals: List[Qual],
            columns: List[str],
            pathkeys: List[SortKey]=[]) -> Optional[Iterable[Sequence[Any]]]:
        pass
