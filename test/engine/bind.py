"""tests the "bind" attribute/argument across schema, SQL, and ORM sessions,
including the deprecated versions of these arguments"""

import testbase
from sqlalchemy import *
from sqlalchemy import engine, exceptions
from testlib import *


class BindTest(PersistTest):
    def test_create_drop_explicit(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer))
        for bind in (
            testbase.db,
            testbase.db.connect()
        ):
            for args in [
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
                assert metadata.bind is table.bind is bind
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
                assert metadata.bind is table.bind is bind
                metadata.create_all()
                assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                assert not table.exists()
                if isinstance(bind, engine.Connection):
                    bind.close()

    def test_create_drop_constructor_bound(self):
        for bind in (
            testbase.db,
            testbase.db.connect()
        ):
            try:
                for args in (
                    ([bind], {}),
                    ([], {'bind':bind}),
                ):
                    metadata = MetaData(*args[0], **args[1])
                    table = Table('test_table', metadata,   
                        Column('foo', Integer))
                    assert metadata.bind is table.bind is bind
                    metadata.create_all()
                    assert table.exists()
                    metadata.drop_all()
                    table.create()
                    table.drop()
                    assert not table.exists()
            finally:
                if isinstance(bind, engine.Connection):
                    bind.close()

    def test_implicit_execution(self):
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer),
            test_needs_acid=True,
            )
        conn = testbase.db.connect()
        metadata.create_all(bind=conn)
        try:
            trans = conn.begin()
            metadata.bind = conn
            t = table.insert()
            assert t.bind is conn
            table.insert().execute(foo=5)
            table.insert().execute(foo=6)
            table.insert().execute(foo=7)
            trans.rollback()
            metadata.bind = None
            assert conn.execute("select count(1) from test_table").scalar() == 0
        finally:
            metadata.drop_all(bind=conn)
            

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
                    try:
                        e = elem(bind=bind)
                        assert e.bind is bind
                        e.execute()
                    finally:
                        if isinstance(bind, engine.Connection):
                            bind.close()

                try:
                    e = elem()
                    assert e.bind is None
                    e.execute()
                    assert False
                except exceptions.InvalidRequestError, e:
                    assert str(e) == "This Compiled object is not bound to any Engine or Connection."

        finally:
            if isinstance(bind, engine.Connection):
                bind.close()
            metadata.drop_all(bind=testbase.db)
    
    def test_session(self):
        from sqlalchemy.orm import create_session, mapper
        metadata = MetaData()
        table = Table('test_table', metadata,   
            Column('foo', Integer, primary_key=True),
            Column('data', String(30)))
        class Foo(object):
            pass
        mapper(Foo, table)
        metadata.create_all(bind=testbase.db)
        try:
            for bind in (testbase.db, 
                testbase.db.connect()
                ):
                try:
                    for args in ({'bind':bind},):
                        sess = create_session(**args)
                        assert sess.bind is bind
                        f = Foo()
                        sess.save(f)
                        sess.flush()
                        assert sess.get(Foo, f.foo) is f
                finally:
                    if isinstance(bind, engine.Connection):
                        bind.close()

                if isinstance(bind, engine.Connection):
                    bind.close()
                    
            sess = create_session()
            f = Foo()
            sess.save(f)
            try:
                sess.flush()
                assert False
            except exceptions.InvalidRequestError, e:
                assert str(e).startswith("Could not locate any Engine or Connection bound to mapper")
        finally:
            if isinstance(bind, engine.Connection):
                bind.close()
            metadata.drop_all(bind=testbase.db)
        
               
if __name__ == '__main__':
    testbase.main()
