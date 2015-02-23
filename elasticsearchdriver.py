# -*- coding: UTF-8 -*-

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch.exceptions import NotFoundError

from driver import *

class ElasticsearchDriver(BaseDriver):
    _client = None
    _index_name = None
    _doc_type = None
    _fetch_size = None
    _scroll_expiry_time = None

    def __init__(self, client, index_name, doc_type, \
            fetch_size=DEFAULT_FETCH_SIZE, scroll_expiry_time="5m"):
        self._client = client
        self._index_name = index_name
        self._doc_type = doc_type
        self._fetch_size = fetch_size
        self._scroll_expiry_time = scroll_expiry_time

    def get_queryset(self):
        body = {"query": {"match_all": {}}}

        return ElasticsearchQuerySet(self._client, body, \
                self._scroll_expiry_time, self._index_name, self._doc_type)

    def get_record(self, id):
        try:
            data = self._client.get(
                index=self._index_name,
                id=id,
                doc_type=self._doc_type,
            )

        except NotFoundError:
            return None

        record = data["_source"]

        record["id"] = data["_id"]
        record["last_modified"] = datetime.strptime(record["last_modified"], \
                "%Y-%m-%d %H:%M:%S")

        return record

    def execute_operation(self, operation):
        if operation.oper_type == Operation.OPER_INSERT:
            self._client.index(
                index=self._index_name,
                doc_type=self._doc_type,
                body={
                    "title": operation.data["title"],
                    "description": operation.data["description"],
                    "release_year": operation.data["release_year"],
                    "length": operation.data["length"],
                    "rating": operation.data["rating"],
                    "last_modified": datetime.strftime( \
                            operation.data["last_modified"], \
                            "%Y-%m-%d %H:%M:%S"),
                },
                id=operation.data["id"],
            )

        elif operation.oper_type == Operation.OPER_UPDATE:
            self._client.update(
                index=self._index_name,
                doc_type=self._doc_type,
                id=operation.data["id"],
                body={
                    "doc": {
                        "title": operation.data["title"],
                        "description": operation.data["description"],
                        "release_year": operation.data["release_year"],
                        "length": operation.data["length"],
                        "rating": operation.data["rating"],
                        "last_modified": datetime.strftime( \
                                operation.data["last_modified"], \
                                "%Y-%m-%d %H:%M:%S"),
                    }
                },
            )

class ElasticsearchQuerySet(BaseQuerySet):
    """
    Implementation of Elasticsearch Driver QuerySet logic.
    """

    _scan_iter = None

    def __init__(self, client, query, scroll_expiry_time, index_name, doc_type):
        """
        Thanks to "scan and scroll" API python helper, our job is simple here :)
        """

        self._scan_iter = scan(client, query, scroll_expiry_time, \
                index=index_name, doc_type=doc_type)

    def _fetch_row(self):
        """
        Return the next available row, taking care to convert to a neutral
        structure.
        """

        data = next(self._scan_iter)
        row = data["_source"]

        row["id"] = data["_id"]
        row["last_modified"] = datetime.strptime(row["last_modified"], \
                "%Y-%m-%d %H:%M:%S")

        return row
