# -*- coding: utf8 -*-

from datetime import datetime
from elasticsearch import Elasticsearch
import uuid

es = Elasticsearch()

songs = [('Rockstar','Third Day'),('Corcovado','Tom Jobim'),('Vertigo','U2'),('A Sky Full Of Stars','Coldplay'),('Coração','João Alexandre')]

for title, artist in songs:
    doc = {
        'title': title,
        'artist': artist,
        'timestamp': datetime.now()
    }

    id = uuid.uuid4()
    res = es.index(index="songs", doc_type='song', id=id, body=doc)
