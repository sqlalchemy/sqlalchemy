import cPickle as pickle
import sys, os

sys.path = ['../../lib', './lib/'] + sys.path

import docstring

import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.engine.strategies as strategies
import sqlalchemy.sql as sql
import sqlalchemy.pool as pool
import sqlalchemy.orm as orm
import sqlalchemy.exceptions as exceptions
import sqlalchemy.ext.proxy as proxy
import sqlalchemy.ext.sessioncontext as sessioncontext
import sqlalchemy.mods.threadlocal as threadlocal
import sqlalchemy.ext.selectresults as selectresults

objects = []
def make_doc(obj, classes=None, functions=None):
    objects.append(docstring.ObjectDoc(obj, classes=classes, functions=functions))
    
make_doc(obj=sql, classes=[sql.Engine, sql.AbstractDialect, sql.ClauseParameters, sql.Compiled, sql.ClauseElement, sql.TableClause, sql.ColumnClause])
make_doc(obj=schema)
make_doc(obj=engine, classes=[engine.Connectable, engine.ComposedSQLEngine, engine.Connection, engine.Transaction, engine.Dialect, engine.ConnectionProvider, engine.ExecutionContext, engine.ResultProxy, engine.RowProxy])
make_doc(obj=strategies)
make_doc(obj=orm, classes=[orm.Mapper, orm.MapperExtension])
make_doc(obj=orm.query, classes=[orm.query.Query])
make_doc(obj=orm.session, classes=[orm.session.Session, orm.session.SessionTransaction])
make_doc(obj=pool, classes=[pool.DBProxy, pool.Pool, pool.QueuePool, pool.SingletonThreadPool])
make_doc(obj=sessioncontext)
make_doc(obj=threadlocal)
make_doc(obj=selectresults)
make_doc(obj=exceptions)
make_doc(obj=proxy)


output = os.path.join(os.getcwd(), 'content', "compiled_docstrings.pickle")
pickle.dump(objects, file(output, 'w'))
