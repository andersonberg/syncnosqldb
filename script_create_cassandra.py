# -*- coding: utf8 -*-

from datetime import datetime
from cassandra.cluster import Cluster
import logging
import uuid


log = logging.getLogger()
log.setLevel('INFO')


class SimpleClient():
    session = None

    def connect(self, nodes):
        cluster = Cluster()
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Conectado ao cluster: ' + metadata.cluster_name)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Conexão encerrada.')

    def create_schema(self):
        self.session.execute("""
            CREATE KEYSPACE music WITH replication
            = {'class':'SimpleStrategy','replication_factor':3};
            """)

        self.session.execute("""
            CREATE TABLE music.songs (
                id uuid PRIMARY KEY,
                title text,
                artist text,
                timestamp timestamp);
        """)

        log.info('Keyspace Music e schema criados.')

    def load_data(self):
        songs = [('Rockstar','Third Day'), ('Corcovado','Tom Jobim'), ('Vertigo','U2'),('A Sky Full Of Stars','Coldplay'),('Coração','João Alexandre')]
        for title, artist in songs:
            id = uuid.uuid4()

            self.session.execute("""
                INSERT INTO music.songs (id, title, artist, timestamp)
                VALUES (
                    %s,
                    '%s',
                    '%s',
                    '%s'
                    );
                """ % (str(id), title, artist, datetime.now().strftime("%Y-%m-%d %H:%M:%S+%f")))

        log.info("Dados carregados.")

    def query_schema(self):
        results = self.session.execute("""
            SELECT * FROM music.songs
            """)

        print("%-30s\t%-20s\t%-20s\n%s" % ("title", "artist", "timestamp",
        "-------------------------------+-----------------------+--------------------"))
        for row in results:
            print("%-30s\t%-20s\t%-20s" % (row.title, row.artist, row.timestamp))
        log.info('Schema queried.')


def main():
    logging.basicConfig()
    client = SimpleClient()
    client.connect(['127.0.0.1'])
    # client.create_schema()
    # time.sleep(10)
    # client.load_data()
    client.query_schema()
    client.close()

if __name__ == "__main__":
    main()
