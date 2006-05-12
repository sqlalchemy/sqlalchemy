import cPickle as pickle
import sys, os

sys.path = ['../../lib', './lib/'] + sys.path

import docstring

import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.sql as sql
import sqlalchemy.pool as pool
import sqlalchemy.mapping as mapping
import sqlalchemy.exceptions as exceptions
import sqlalchemy.ext.proxy as proxy

objects = []
def make_doc(obj, classes=None, functions=None):
    objects.append(docstring.ObjectDoc(obj, classes=classes, functions=functions))
    
make_doc(obj=schema)
make_doc(obj=engine, classes=[engine.SQLSession, engine.SQLEngine, engine.ResultProxy, engine.RowProxy])
make_doc(obj=sql, classes=[sql.ClauseParameters, sql.Compiled, sql.ClauseElement, sql.TableClause, sql.ColumnClause])
make_doc(obj=pool, classes=[pool.DBProxy, pool.Pool, pool.QueuePool, pool.SingletonThreadPool])
make_doc(obj=mapping, classes=[mapping.Mapper, mapping.MapperExtension])
make_doc(obj=mapping.query, classes=[mapping.query.Query])
make_doc(obj=mapping.objectstore, classes=[mapping.objectstore.Session, mapping.objectstore.Session.SessionTrans])
make_doc(obj=exceptions)
make_doc(obj=proxy)

output = os.path.join(os.getcwd(), 'content', "compiled_docstrings.pickle")
pickle.dump(objects, file(output, 'w'))