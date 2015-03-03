# -*- coding: utf8 -*-

from datetime import datetime
from elasticsearch import Elasticsearch
import uuid
import logging
import six

from elasticsearch.helpers import bulk, scan
from cassandra.cluster import Cluster
from cassandra.query import BatchType, BatchStatement, dict_factory
import time

log = logging.getLogger()


class Sincronizador():
    """
    Classe que implementa a sincronização de dados entre os dois bancos
    """

    def __init__(self):
        self.id = "id"
        self.cassandra_table = "songs"
        self.elasticsearch_index = "songs"
        self.elasticsearch_type = "song"

        self.cluster = Cluster()
        self.ca_session = self.cluster.connect("music")
        self.ca_session.row_factory = dict_factory

        self.es_session = Elasticsearch()

        self.marker = datetime.now()

        patch_cql_types()

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
        """
        Realiza a inserção de dados no Cassandra a partir de uma lista de documentos
        :param docs: Lista de documentos a serem inseridos no Cassandra
        """
        insert_stat = self.ca_session.prepare("INSERT INTO songs (id, title, artist, timestamp) VALUES (?, ?, ?, ?);")
        batch = BatchStatement()
        for doc in docs:
            doc['timestamp'] = datetime.strptime(doc["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            batch.add(insert_stat, doc)

        self.ca_session.execute(batch)

    def search_ca(self):
        """
        Realiza uma busca por todos os documentos no Cassandra
        :return: cursor contendo o resultado da query
        """
        query_ca = "SELECT * FROM music.songs;"
        ca_cursor = self.ca_session.execute(query_ca)

        return ca_cursor

    def sync(self):
        """
        Realiza a sincronização dos dados
        """

        last_marker = self.marker
        current_marker = datetime.now()

        # sincroniza do Elasticsearch para o Cassandra
        docs = []
        query = {"filter": {"range": {"timestamp": {"gte": last_marker, "lte": current_marker}}}}
        docs = self.scan_es(query)

        if docs:
            self.insert_ca(docs)
            print("Sincronização do Elasticsearch para o Cassandra")

        # sincroniza do Cassandra para o Elasticsearch
        docs = []
        ca_cursor = self.search_ca()
        for doc in ca_cursor:
            if doc["timestamp"] < last_marker or doc["timestamp"] > current_marker:
                continue
            else:
                doc["id"] = str(doc["id"])
                docs.append(doc)

        if docs:
            self.bulk_es_insert(docs)
            print("Sincronização do Cassandra para o Elasticsearch")

        self.marker = current_marker


def patch_cql_types():
    """
    Permite o Cassandra aceitar string de hexadecimais como id de um documento
    """

    from cassandra.cqltypes import UUIDType

    saved_serialize = UUIDType.serialize

    @staticmethod
    def serialize_ex(obj, protocol_version):
        if isinstance(obj, six.string_types):
            obj = uuid.UUID(hex=obj)
        return saved_serialize(obj, protocol_version)

    UUIDType.serialize = serialize_ex
