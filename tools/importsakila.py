# -*- coding: UTF-8 -*-

import os
from uuid import uuid4
from datetime import datetime

lib_dir = os.path.dirname(os.path.abspath(__file__))
os.sys.path.append(os.path.join(lib_dir, ".."))

import MySQLdb as mysql
from MySQLdb import cursors
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

from util import *

def import_sakila_films():
    mysql_conn = mysql.connect(
            host="127.0.0.1",
            user="root",
            passwd="",
            db="sakila",
            cursorclass=cursors.DictCursor,
            use_unicode=True,
            charset="utf8"
    )

    cass_cluster = Cluster(get_conf("CASSANDRA_CONTACT_POINTS"), \
            get_conf("CASSANDRA_PORT"))
    cass_session = cass_cluster.connect("simbiose")

    es_client = Elasticsearch(hosts=get_conf("ELASTICSEARCH_NODES"))

    sel_cursor = mysql_conn.cursor()

    sel_cursor.execute(
        """
        SELECT title, description, release_year, length, rating
        FROM film
        ORDER BY film_id
        LIMIT 500
        """
    )

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

    row = sel_cursor.fetchone()

    while row is not None:
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

        row = sel_cursor.fetchone()

if __name__ == "__main__":
    import_sakila_films()
