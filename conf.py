# -*- coding: UTF-8 -*-

################################################################################
# General configuration                                                        #
################################################################################

SYNC_POLLING_INTERVAL = 300 # in seconds

################################################################################
# Cassandra configuration                                                      #
################################################################################

CASSANDRA_CONTACT_POINTS = ["127.0.0.1"]
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "simbiose"

################################################################################
# Elasticsearch configuration                                                  #
################################################################################

ELASTICSEARCH_NODES = ["127.0.0.1"]
ELASTICSEARCH_INDEX_NAME = "simbiose"
ELASTICSEARCH_DOC_TYPE = "films"
