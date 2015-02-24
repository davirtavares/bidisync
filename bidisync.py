# -*- coding: UTF-8 -*-

import argparse
from time import sleep

from daemon import DaemonContext
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

from util import *
from cassandradriver import CassandraDriver
from elasticsearchdriver import ElasticsearchDriver

def run():
    while True:
        cas_cluster = Cluster(get_conf("CASSANDRA_CONTACT_POINTS"), \
                get_conf("CASSANDRA_PORT"))
        es_conn = Elasticsearch(get_conf("ELASTICSEARCH_NODES"))

        cd = CassandraDriver(cas_cluster)
        ed = ElasticsearchDriver(es_conn, \
                get_conf("ELASTICSEARCH_INDEX_NAME"), \
                get_conf("ELASTICSEARCH_DOC_TYPE"))

        es_delta = cd.build_delta(ed)
        es.apply()

        cas_delta = ed.build_delta(cd)
        cas_delta.apply()

        sleep(get_conf("SYNC_POLLING_INTERVAL"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--foreground", "-f", dest="foreground", \
            help="run in foreground", action="store_true")
    parser.set_defaults(foreground=False)

    args = parser.parse_args()

    if not args.foreground:
        with DaemonContext():
            run()

    else:
        run()
