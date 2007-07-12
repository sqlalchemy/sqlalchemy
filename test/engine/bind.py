"""tests the "bind" attribute/argument across schema, SQL, and ORM sessions,
including the deprecated versions of these arguments"""

import testbase
import unittest, sys, datetime
import tables
db = testbase.db
from sqlalchemy import *

class BindTest(testbase.PersistTest):
    def test_create_drop_explicit(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer))
        for bind in (
            testbase.db,
            testbase.db.connect()
        ):
            for args in [
                ([], {'connectable':bind}),
                ([], {'bind':bind}),
                ([bind], {})
            ]:
                metadata.create_all(*args[0], **args[1])
                assert table.exists(*args[0], **args[1])
                metadata.drop_all(*args[0], **args[1])
                table.create(*args[0], **args[1])
                table.drop(*args[0], **args[1])
                assert not table.exists(*args[0], **args[1])
    
    def test_create_drop_err(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer))

        for meth in [
            metadata.create_all,
            table.exists,
            metadata.drop_all,
            table.create,
            table.drop,
        ]:
            try:
                meth()
                assert False
            except exceptions.InvalidRequestError, e:
                assert str(e)  == "This SchemaItem is not connected to any Engine or Connection."
        
    def test_create_drop_bound(self):
        
        for meta in (MetaData,ThreadLocalMetaData):
            for bind in (
                testbase.db,
                testbase.db.connect()
            ):
                metadata = meta()
                table = Table('test_table', metadata,   
                Column('foo', Integer))
                metadata.bind = bind
                assert metadata.bind is metadata.engine is table.bind is table.engine is bind
                metadata.create_all()
                assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                assert not table.exists()

                metadata = meta()
                table = Table('test_table', metadata,   
                    Column('foo', Integer))

                metadata.connect(bind)
                assert metadata.bind is metadata.engine is table.bind is table.engine is bind
                metadata.create_all()
                assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                assert not table.exists()

    def test_create_drop_constructor_bound(self):
        for bind in (
            testbase.db,
            testbase.db.connect()
        ):
            for args in (
                ([bind], {}),
                ([], {'engine_or_url':bind}),
                ([], {'bind':bind}),
                ([], {'engine':bind})
            ):
                metadata = MetaData(*args[0], **args[1])
                table = Table('test_table', metadata,   
                    Column('foo', Integer))

                assert metadata.bind is metadata.engine is table.bind is table.engine is bind
                metadata.create_all()
                assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                assert not table.exists()


    def test_clauseelement(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer))
        metadata.create_all(bind=testbase.db)
        try:
            for elem in [
                table.select,
                lambda **kwargs:func.current_timestamp(**kwargs).select(),
#                func.current_timestamp().select,
                lambda **kwargs:text("select * from test_table", **kwargs)
            ]:
                for bind in (
                    testbase.db,
                    testbase.db.connect()
                ):
                    e = elem(bind=bind)
                    assert e.bind is e.engine is bind
                    e.execute()
                    e = elem(engine=bind)
                    assert e.bind is e.engine is bind
                    e.execute()

                try:
                    e = elem()
                    assert e.bind is e.engine is None
                    e.execute()
                    assert False
                except exceptions.InvalidRequestError, e:
                    assert str(e) == "This Compiled object is not bound to any Engine or Connection."
                
        finally:
            metadata.drop_all(bind=testbase.db)
    
    def test_session(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer, primary_key=True),
            Column('data', String(30)))
        class Foo(object):
            pass
        mapper(Foo, table)
        metadata.create_all(bind=testbase.db)
        try:
            for bind in (testbase.db, testbase.db.connect()):
                for args in ({'bind':bind}, {'bind_to':bind}):
                    sess = create_session(**args)
                    assert sess.bind is sess.bind_to is bind
                    f = Foo()
                    sess.save(f)
                    sess.flush()
                    assert sess.get(Foo, f.foo) is f
                    
            sess = create_session()
            f = Foo()
            sess.save(f)
            try:
                sess.flush()
                assert False
            except exceptions.InvalidRequestError, e:
                assert str(e).startswith("Could not locate any Engine or Connection bound to mapper")
                
        finally:
            metadata.drop_all(bind=testbase.db)
        
               
if __name__ == '__main__':
    testbase.main()