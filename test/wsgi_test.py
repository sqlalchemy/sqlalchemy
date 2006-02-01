"""Interactive wsgi test

Small WSGI application that uses a table and mapper defined at the module
level, with per-application uris enabled by the ProxyEngine.

Requires the wsgiutils package from:

http://www.owlfish.com/software/wsgiutils/

Run the script with python wsgi_test.py, then visit http://localhost:8080/a
and http://localhost:8080/b with a browser. You should see two distinct lists
of colors.
"""

from sqlalchemy import *
from sqlalchemy.ext.proxy import ProxyEngine
from wsgiutils import wsgiServer

engine = ProxyEngine()

colors = Table('colors', engine,
               Column('id', Integer, primary_key=True),
               Column('name', String(32)),
               Column('hex', String(6)))

class Color(object):
    pass

assign_mapper(Color, colors)

data = { 'a': (('fff','white'), ('aaa','gray'), ('000','black'),
               ('f00', 'red'), ('0f0', 'green')),
         'b': (('00f','blue'), ('ff0', 'yellow'), ('0ff','purple')) }

db_uri = { 'a': 'sqlite://filename=wsgi_db_a.db',
           'b': 'sqlite://filename=wsgi_db_b.db' }

def app(dataset):
    print '... connecting to database %s: %s' % (dataset, db_uri[dataset])
    engine.connect(db_uri[dataset], echo=True, echo_pool=True)
    colors.create()

    print '... populating data into %s' % db_uri[dataset]
    for hex, name in data[dataset]:
        c = Color()
        c.hex = hex
        c.name = name
    objectstore.commit()
    objectstore.clear()
    
    def call(environ, start_response):
        engine.connect(db_uri[dataset], echo=True, echo_pool=True)

        # NOTE: must clear objectstore on each request, or you'll see
        # objects from another thread here
        objectstore.clear()
        objectstore.begin()
        
        c = Color.select()

        start_response('200 OK', [('content-type','text/html')])
        yield '<html><head><title>Test dataset %s</title></head>' % dataset
        yield '<body>'
        yield '<p>uri: %s</p>' % db_uri[dataset]
        yield '<p>engine: <xmp>%s</xmp></p>' % engine.engine
        yield '<p>Colors!</p>'
        for color in c:
            yield '<div style="background: #%s">%s</div>' % (color.hex,
                                                             color.name)
        yield '</body></html>'
    return call

def cleanup():
    for uri in db_uri.values():
        print "Cleaning db %s" % uri
        engine.connect(uri)
        colors.drop()

def run_server(apps, host='localhost', port=8080):
    print "Serving test app at http://%s:%s/" % (host, port)
    print "Visit http://%(host)s:%(port)s/a and " \
        "http://%(host)s:%(port)s/b to test apps" % {'host': host,
                                                     'port': port}
    
    server = wsgiServer.WSGIServer((host, port), apps, serveFiles=False)
    try:
        server.serve_forever()
    except:
        cleanup()
        raise
    
if __name__ == '__main__':
    run_server({'/a':app('a'), '/b':app('b')})





























