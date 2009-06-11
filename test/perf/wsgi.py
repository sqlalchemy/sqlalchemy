#!/usr/bin/python
"""Uses ``wsgiref``, standard in Python 2.5 and also in the cheeseshop."""

from sqlalchemy import *
from sqlalchemy.orm import *
import thread
from sqlalchemy.test import *

port = 8000

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)

threadids = set()
meta = MetaData(testing.db)
foo = Table('foo', meta,
    Column('id', Integer, primary_key=True),
    Column('data', String(30)))
class Foo(object):
    pass
mapper(Foo, foo)

def prep():
    meta.drop_all()
    meta.create_all()

    data = []
    for x in range(1,500):
        data.append({'id':x,'data':"this is x value %d" % x})
    foo.insert().execute(data)

def serve(environ, start_response):
    start_response("200 OK", [('Content-type', 'text/plain')])
    sess = create_session()
    l = sess.query(Foo).select()
    threadids.add(thread.get_ident())

    print ("sending response on thread", thread.get_ident(),
           " total threads ", len(threadids))
    return [str("\n".join([x.data for x in l]))]


if __name__ == '__main__':
    from wsgiref import simple_server
    try:
        prep()
        server = simple_server.make_server('localhost', port, serve)
        print "Server listening on port %d" % port
        server.serve_forever()
    finally:
        meta.drop_all()
