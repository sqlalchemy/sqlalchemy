"""Serializer/Deserializer objects for usage with SQLAlchemy structures.

Any SQLAlchemy structure, including Tables, Columns, expressions, mappers,
Query objects etc. can be serialized in a minimally-sized format,
and deserialized when given a Metadata and optional ScopedSession object
to use as context on the way out.

Usage is nearly the same as that of the standard Python pickle module::

    from sqlalchemy.ext.serializer import loads, dumps
    metadata = MetaData(bind=some_engine)
    Session = scoped_session(sessionmaker())
    
    # ... define mappers
    
    query = Session.query(MyClass).filter(MyClass.somedata=='foo').order_by(MyClass.sortkey)
    
    # pickle the query
    serialized = dumps(query)
    
    # unpickle.  Pass in metadata + scoped_session
    query2 = loads(serialized, metadata, Session)
    
    print query2.all()

Similar restrictions as when using raw pickle apply; mapped classes must be 
themselves be pickleable, meaning they are importable from a module-level
namespace.

Note that instances of user-defined classes do not require this extension
in order to be pickled; these contain no references to engines, sessions
or expression constructs in the typical case and can be serialized directly.
This module is specifically for ORM and expression constructs.

"""

from sqlalchemy.orm import class_mapper, Query
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy import Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.util import pickle
import re
import base64
from cStringIO import StringIO

__all__ = ['Serializer', 'Deserializer', 'dumps', 'loads']

def Serializer(*args, **kw):
    pickler = pickle.Pickler(*args, **kw)
        
    def persistent_id(obj):
        #print "serializing:", repr(obj)
        if isinstance(obj, QueryableAttribute):
            cls = obj.impl.class_
            key = obj.impl.key
            id = "attribute:" + key + ":" + base64.b64encode(pickle.dumps(cls))
        elif isinstance(obj, Mapper) and not obj.non_primary:
            id = "mapper:" + base64.b64encode(pickle.dumps(obj.class_))
        elif isinstance(obj, Table):
            id = "table:" + str(obj)
        elif isinstance(obj, Column) and isinstance(obj.table, Table):
            id = "column:" + str(obj.table) + ":" + obj.key
        elif isinstance(obj, Session):
            id = "session:"
        elif isinstance(obj, Engine):
            id = "engine:"
        else:
            return None
        return id
        
    pickler.persistent_id = persistent_id
    return pickler
    
our_ids = re.compile(r'(mapper|table|column|session|attribute|engine):(.*)')

def Deserializer(file, metadata=None, scoped_session=None, engine=None):
    unpickler = pickle.Unpickler(file)
    
    def get_engine():
        if engine:
            return engine
        elif scoped_session and scoped_session().bind:
            return scoped_session().bind
        elif metadata and metadata.bind:
            return metadata.bind
        else:
            return None
            
    def persistent_load(id):
        m = our_ids.match(id)
        if not m:
            return None
        else:
            type_, args = m.group(1, 2)
            if type_ == 'attribute':
                key, clsarg = args.split(":")
                cls = pickle.loads(base64.b64decode(clsarg))
                return getattr(cls, key)
            elif type_ == "mapper":
                cls = pickle.loads(base64.b64decode(args))
                return class_mapper(cls)
            elif type_ == "table":
                return metadata.tables[args]
            elif type_ == "column":
                table, colname = args.split(':')
                return metadata.tables[table].c[colname]
            elif type_ == "session":
                return scoped_session()
            elif type_ == "engine":
                return get_engine()
            else:
                raise Exception("Unknown token: %s" % type_)
    unpickler.persistent_load = persistent_load
    return unpickler

def dumps(obj):
    buf = StringIO()
    pickler = Serializer(buf)
    pickler.dump(obj)
    return buf.getvalue()
    
def loads(data, metadata=None, scoped_session=None, engine=None):
    buf = StringIO(data)
    unpickler = Deserializer(buf, metadata, scoped_session, engine)
    return unpickler.load()
    
    