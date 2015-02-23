# -*- coding: UTF-8 -*-

from uuid import UUID

from cassandra.query import SimpleStatement, dict_factory

from driver import *
from util import *

class CassandraDriver(BaseDriver):
    _session = None
    _fetch_size = None
    _prep_insert = None
    _prep_update = None

    def __init__(self, cluster, fetch_size=DEFAULT_FETCH_SIZE):
        self._session = cluster.connect(get_conf("CASSANDRA_KEYSPACE"))
        self._session.row_factory = dict_factory

        self._fetch_size = fetch_size

        self._prep_insert = self._session.prepare(
            """
            INSERT INTO films (
                id,
                title,
                description,
                release_year,
                length,
                rating,
                last_modified
            )
            VALUES (
                :id,
                :title,
                :description,
                :release_year,
                :length,
                :rating,
                :last_modified
            )
            """
        )

        self._prep_update = self._session.prepare(
            """
            UPDATE films
            SET
                title = :title, 
                description = :description,
                release_year = :release_year,
                length = :length,
                rating = :rating,
                last_modified = :last_modified
            WHERE id = :id
            """
        )

    def get_queryset(self):
        q = """
            SELECT *
            FROM films
        """

        stmt = SimpleStatement(q, fetch_size=self._fetch_size)

        result = self._session.execute(stmt)

        return CassandraQuerySet(result)

    def get_record(self, id):
        q = """
            SELECT *
            FROM films
            WHERE id = %s
        """

        result = self._session.execute(q, [UUID(id)])

        if not result:
            return None

        row = result[0]

        row["id"] = str(row["id"])
        row["last_modified"] = row["last_modified"].replace(microsecond=0)

        return row

    def execute_operation(self, operation):
        if operation.oper_type == Operation.OPER_INSERT:
            self._session.execute(self._prep_insert, operation.data)

        elif operation.oper_type == Operation.OPER_UPDATE:
            self._session.execute(self._prep_update, operation.data)

class CassandraQuerySet(BaseQuerySet):
    """
    This is basically a wrapper, as Cassandra 2.0+ paginates the result
    automatically.
    """

    _result = None

    def __init__(self, result):
        self._result = iter(result)

    def _fetch_row(self):
        """
        Return the next available row, taking care to convert to a neutral
        structure.
        """

        row = next(self._result)

        row["id"] = str(row["id"])
        row["last_modified"] = row["last_modified"].replace(microsecond=0)

        return row
