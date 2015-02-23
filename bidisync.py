# -*- coding: UTF-8 -*-

from uuid import uuid4

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

import driver
from util import *
from cassandradriver import CassandraDriver
from elasticsearchdriver import ElasticsearchDriver

if __name__ == "__main__":
    cas_cluster = Cluster(get_conf("CASSANDRA_CONTACT_POINTS"), \
            get_conf("CASSANDRA_PORT"))
    es_conn = Elasticsearch(get_conf("ELASTICSEARCH_NODES"))

    cd = CassandraDriver(cas_cluster)
    ed = ElasticsearchDriver(es_conn, get_conf("ELASTICSEARCH_INDEX_NAME"), \
            get_conf("ELASTICSEARCH_DOC_TYPE"))

    qs = ed.get_queryset()
    a = next(qs)

    qs = cd.get_queryset()
    b = next(qs)

    r = cd.get_record(a["id"])

    import pdb; pdb.set_trace()

    op = driver.Operation(driver.Operation.OPER_UPDATE, a)

    a["title"] = "Novo Teste"

    ed.execute_operation(op)
