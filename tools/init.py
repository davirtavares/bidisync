# -*- coding: UTF-8 -*-

import os

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

lib_dir = os.path.dirname(os.path.abspath(__file__))
os.sys.path.append(os.path.join(lib_dir, ".."))

from util import *
from importsakila import import_sakila_films

def init_cassandra():
    cass_cluster = Cluster(get_conf("CASSANDRA_CONTACT_POINTS"), \
            get_conf("CASSANDRA_PORT"))
    cass_session = cass_cluster.connect()

    cass_session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS simbiose
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
        """
    )

    cass_session.set_keyspace("simbiose")

    cass_session.execute(
        """
        DROP TABLE IF EXISTS films
        """
    )

    cass_session.execute(
        """
        CREATE TABLE films (
            id uuid,
            title text,
            description text,
            release_year int,
            length int,
            rating text,
            last_modified timestamp,
            PRIMARY KEY (id)
        )
        """
    )

def init_elasticsearh():
    es_client = Elasticsearch(hosts=get_conf("ELASTICSEARCH_NODES"))

    try:
        es_client.indices.delete(get_conf("ELASTICSEARCH_INDEX_NAME"))

    except NotFoundError:
        pass

    es_client.indices.create(get_conf("ELASTICSEARCH_INDEX_NAME"), {
        "mappings": {
            get_conf("ELASTICSEARCH_DOC_TYPE"): {
                "properties": {
                    "title": {
                        "type": "string",
                    },

                    "description": {
                        "type": "string",
                    },

                    "release_year": {
                        "type": "long",
                    },

                    "length": {
                        "type": "long",
                    },

                    "rating": {
                        "type": "string",
                        "index": "not_analyzed",
                    },

                    "last_modified": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss",
                    },
                },
            },
        },
    })

if __name__ == "__main__":
    print "Initializing Cassandra structure..."

    init_cassandra()

    print "Initializing Elasticsearch structure..."

    init_elasticsearh()

    print "Importing initial data..."

    import_sakila_films()
