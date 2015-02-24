# -*- coding: UTF-8 -*-

import os
from uuid import uuid4
from datetime import datetime
import cPickle as pickle

lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
os.sys.path.append(lib_dir)

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

from util import *

def import_films():
    films = pickle.load(open(os.path.join(lib_dir, "films.pickle")))

    cass_cluster = Cluster(get_conf("CASSANDRA_CONTACT_POINTS"), \
            get_conf("CASSANDRA_PORT"))
    cass_session = cass_cluster.connect("simbiose")

    es_client = Elasticsearch(hosts=get_conf("ELASTICSEARCH_NODES"))

    insert_film = cass_session.prepare(
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
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """
    )

    for row in films[:1]:
        id = uuid4()
        last_modified = datetime.utcnow()

        cass_session.execute(insert_film, (
            id,
            row["title"],
            row["description"],
            row["release_year"],
            row["length"],
            row["rating"],
            last_modified,
        ))

        es_client.index(
            index=get_conf("ELASTICSEARCH_INDEX_NAME"),
            doc_type=get_conf("ELASTICSEARCH_DOC_TYPE"),
            body={
                "title": row["title"],
                "description": row["description"],
                "release_year": row["release_year"],
                "length": row["length"],
                "rating": row["rating"],
                "last_modified": datetime.strftime(last_modified, \
                        "%Y-%m-%d %H:%M:%S"),
            },
            id=id,
        )

if __name__ == "__main__":
    import_films()
