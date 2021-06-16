import pymongo
import os
import logging
import time
import ssl

import routing

URL = 'mongodb+srv://admin:admin123@cluster0.*******.mongodb.net/myFirstDatabase?retryWrites=true&w=1'
client = pymongo.MongoReplicaSetClient(URL,ssl=True,ssl_cert_reqs=ssl.CERT_NONE)
db = client['logistics']

pipeline = [{'$match': {'operationType': 'insert'}}]
with db['cargos'].watch(pipeline=pipeline) as stream:
    while stream.alive:
        change = stream.try_next()
        # Note that the ChangeStream's resume token may be updated
        # even when no changes are returned.
        print("Current resume token: %r" % (stream.resume_token,))
        if change is not None:
            print("Change document: %r" % (change,))
            document = change['fullDocument']
            print(document['_id'])
            res = routing.add_route(document['_id'],document['location'],document['destination'])
            continue
        # We end up here when there are no recent changes.
        # Sleep for a while before trying again to avoid flooding
        # the server with getMore requests when no changes are
        # available.
        time.sleep(0.5)
        
