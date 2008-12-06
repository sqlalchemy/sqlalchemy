serializer
==========

:author: Mike Bayer

Serializer/Deserializer objects for usage with SQLAlchemy structures.

Any SQLAlchemy structure, including Tables, Columns, expressions, mappers,
Query objects etc. can be serialized in a minimally-sized format,
and deserialized when given a Metadata and optional ScopedSession object
to use as context on the way out.

Usage is nearly the same as that of the standard Python pickle module:

.. sourcecode:: python+sql

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

.. automodule:: sqlalchemy.ext.serializer
   :members:
   :undoc-members:
