#!/usr/bin/python

from sqlalchemy import *
import sqlalchemy.pool as pool
import thread
from sqlalchemy import exceptions

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)

threadids = set()
#meta = MetaData('postgres://scott:tiger@127.0.0.1/test')

#meta = MetaData('mysql://scott:tiger@localhost/test', poolclass=pool.SingletonThreadPool)
meta = MetaData('mysql://scott:tiger@localhost/test')
foo = Table('foo', meta, 
    Column('id', Integer, primary_key=True),
    Column('data', String(30)))

meta.drop_all()
meta.create_all()

data = []
for x in range(1,500):
    data.append({'id':x,'data':"this is x value %d" % x})
foo.insert().execute(data)

class Foo(object):
    pass

mapper(Foo, foo)

root = './'
port = 8000

def serve(environ, start_response):
    sess = create_session()
    l = sess.query(Foo).select()
            
    start_response("200 OK", [('Content-type','text/plain')])
    threadids.add(thread.get_ident())
    print "sending response on thread", thread.get_ident(), " total threads ", len(threadids)
    return ["\n".join([x.data for x in l])]

        
if __name__ == '__main__':
    from wsgiutils import wsgiServer
    server = wsgiServer.WSGIServer (('localhost', port), {'/': serve})
    print "Server listening on port %d" % port
    server.serve_forever()


