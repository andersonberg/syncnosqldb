# -*- coding: utf8 -*-

from datetime import datetime
from elasticsearch import Elasticsearch
import uuid
import logging
import six

from elasticsearch.helpers import bulk, scan
from cassandra.cluster import Cluster
from cassandra.query import BatchType, BatchStatement, dict_factory

log = logging.getLogger()


class Sincronizador():

    def __init__(self, intervalo):
        self.id = "id"
        self.intervalo = intervalo
        self.cassandra_table = "songs"
        self.elasticsearch_index = "songs"
        self.elasticsearch_type = "song"

        self.cluster = Cluster()
        self.ca_session = self.cluster.connect("music")
        self.ca_session.row_factory = dict_factory

        self.es_session = Elasticsearch()

    def bulk_es_insert(self, docs):
        """
        Insere documentos (docs) no index do Elasticsearch
        :param docs: lista de documentos a inserir
        :return: tupla (successful, failed) contendo o número de inserções corretas e falhas, respectivamente
        """
        successful, failed = bulk(self.es_session,
            ({"_index": "songs",
                "_type": "song",
                "_source": doc}
                for doc in docs),
            stats_only=True)

        return successful, failed

    def scan_es(self, query):
        """
        Retorna os documentos como resultado da query inserida.
        :return: resultado da query inserida
        """

        docs = []
        cursor = scan(self.es_session, index="songs", doc_type="song", query=query)

        for d in cursor:
            d["_source"].update({"id": d["_id"]})
            docs.append(d["_source"])

        return docs

    def insert_ca(self, docs):
        insert_stat = self.ca_session.prepare("INSERT INTO songs (id, title, artist, timestamp) VALUES (?, ?, ?, ?);")
        batch = BatchStatement()
        for doc in docs:
            doc['timestamp'] = datetime.strptime(doc["timestamp"], "%Y-%m-%d %H:%M:%S+%f")
            batch.add(insert_stat, doc)

    def search_ca(self):
        query_ca = "SELECT * FROM music.songs;"
        ca_cursor = self.ca_session.execute(query_ca)

        return ca_cursor


def patch_cql_types():

    from cassandra.cqltypes import UUIDType

    saved_serialize = UUIDType.serialize

    @staticmethod
    def serialize_ex(obj, protocol_version):
        if isinstance(obj, six.string_types):
            obj = uuid.UUID(hex=obj)
        return saved_serialize(obj, protocol_version)

    UUIDType.serialize = serialize_ex

# query example: {"filter": {"range": {"timestamp": {"gte":"2015-02-24T17:36:23.632650", "lte": "2015-02-25T17:36:23.645119"}}}}
