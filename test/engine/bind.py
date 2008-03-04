"""tests the "bind" attribute/argument across schema, SQL, and ORM sessions,
including the deprecated versions of these arguments"""

import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import engine, exceptions
from testlib import *


class BindTest(TestBase):
    def test_create_drop_explicit(self):
        metadata = MetaData()
        table = Table('test_table', metadata,
            Column('foo', Integer))
        for bind in (
            testing.db,
            testing.db.connect()
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
            metadata.drop_all,
            table.create,
            table.drop,
        ]:
            try:
                meth()
                assert False
            except exceptions.UnboundExecutionError, e:
                self.assertEquals(
                    str(e),
                    "The MetaData "
                    "is not bound to an Engine or Connection.  "
                    "Execution can not proceed without a database to execute "
                    "against.  Either execute with an explicit connection or "
                    "assign the MetaData's .bind to enable implicit execution.")

        for meth in [
            table.exists,
            # future:
            #table.create,
            #table.drop,
        ]:
            try:
                meth()
                assert False
            except exceptions.UnboundExecutionError, e:
                self.assertEquals(
                    str(e),
                    "The Table 'test_table' "
                    "is not bound to an Engine or Connection.  "
                    "Execution can not proceed without a database to execute "
                    "against.  Either execute with an explicit connection or "
                    "assign this Table's .metadata.bind to enable implicit "
                    "execution.")

    @testing.future
    def test_create_drop_err2(self):
        for meth in [
            table.exists,
            table.create,
            table.drop,
        ]:
            try:
                meth()
                assert False
            except exceptions.UnboundExecutionError, e:
                self.assertEquals(
                    str(e),
                    "The Table 'test_table' "
                    "is not bound to an Engine or Connection.  "
                    "Execution can not proceed without a database to execute "
                    "against.  Either execute with an explicit connection or "
                    "assign this Table's .metadata.bind to enable implicit "
                    "execution.")

    @testing.uses_deprecated('//connect')
    def test_create_drop_bound(self):

        for meta in (MetaData,ThreadLocalMetaData):
            for bind in (
                testing.db,
                testing.db.connect()
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
            testing.db,
            testing.db.connect()
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
        conn = testing.db.connect()
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
        metadata.create_all(bind=testing.db)
        try:
            for elem in [
                table.select,
                lambda **kwargs:func.current_timestamp(**kwargs).select(),
#                func.current_timestamp().select,
                lambda **kwargs:text("select * from test_table", **kwargs)
            ]:
                for bind in (
                    testing.db,
                    testing.db.connect()
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
                except exceptions.UnboundExecutionError, e:
                    assert str(e).endswith(
                        'is not bound and does not support direct '
                        'execution. Supply this statement to a Connection or '
                        'Engine for execution. Or, assign a bind to the '
                        'statement or the Metadata of its underlying tables to '
                        'enable implicit execution via this method.')
        finally:
            if isinstance(bind, engine.Connection):
                bind.close()
            metadata.drop_all(bind=testing.db)

    def test_session(self):
        from sqlalchemy.orm import create_session, mapper
        metadata = MetaData()
        table = Table('test_table', metadata,
            Column('foo', Integer, Sequence('foo_seq', optional=True), primary_key=True),
            Column('data', String(30)))
        class Foo(object):
            pass
        mapper(Foo, table)
        metadata.create_all(bind=testing.db)
        try:
            for bind in (testing.db,
                testing.db.connect()
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
            metadata.drop_all(bind=testing.db)


if __name__ == '__main__':
    testenv.main()
