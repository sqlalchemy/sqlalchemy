"""tests the "bind" attribute/argument across schema and SQL,
including the deprecated versions of these arguments"""

from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy import engine, exc
from sqlalchemy import MetaData, ThreadLocalMetaData
from sqlalchemy import Integer, text
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing import fixtures


class BindTest(fixtures.TestBase):
    def test_bind_close_engine(self):
        e = testing.db
        with e.connect() as conn:
            assert not conn.closed
        assert conn.closed

        with e.contextual_connect() as conn:
            assert not conn.closed
        assert conn.closed

    def test_bind_close_conn(self):
        e = testing.db
        conn = e.connect()
        with conn.connect() as c2:
            assert not c2.closed
        assert not conn.closed
        assert c2.closed

        with conn.contextual_connect() as c2:
            assert not c2.closed
        assert not conn.closed
        assert c2.closed

    def test_create_drop_explicit(self):
        metadata = MetaData()
        table = Table('test_table', metadata, Column('foo', Integer))
        for bind in (
            testing.db,
            testing.db.connect()
        ):
            for args in [
                ([], {'bind': bind}),
                ([bind], {})
            ]:
                metadata.create_all(*args[0], **args[1])
                assert table.exists(*args[0], **args[1])
                metadata.drop_all(*args[0], **args[1])
                table.create(*args[0], **args[1])
                table.drop(*args[0], **args[1])
                assert not table.exists(*args[0], **args[1])

    def test_create_drop_err_metadata(self):
        metadata = MetaData()
        Table('test_table', metadata, Column('foo', Integer))
        for meth in [metadata.create_all, metadata.drop_all]:
            assert_raises_message(
                exc.UnboundExecutionError,
                "MetaData object is not bound to an Engine or Connection.",
                meth
            )

    def test_create_drop_err_table(self):
        metadata = MetaData()
        table = Table('test_table', metadata, Column('foo', Integer))

        for meth in [
            table.exists,
            table.create,
            table.drop,
        ]:
            assert_raises_message(
                exc.UnboundExecutionError,
                ("Table object 'test_table' is not bound to an Engine or "
                 "Connection."),
                meth
            )

    @testing.uses_deprecated()
    def test_create_drop_bound(self):

        for meta in (MetaData, ThreadLocalMetaData):
            for bind in (
                testing.db,
                testing.db.connect()
            ):
                metadata = meta()
                table = Table('test_table', metadata, Column('foo', Integer))
                metadata.bind = bind
                assert metadata.bind is table.bind is bind
                metadata.create_all()
                assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                assert not table.exists()

                metadata = meta()
                table = Table('test_table', metadata, Column('foo', Integer))

                metadata.bind = bind

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
                    ([], {'bind': bind}),
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
                      test_needs_acid=True)
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
            assert conn.execute('select count(*) from test_table'
                                ).scalar() == 0
        finally:
            metadata.drop_all(bind=conn)

    def test_clauseelement(self):
        metadata = MetaData()
        table = Table('test_table', metadata, Column('foo', Integer))
        metadata.create_all(bind=testing.db)
        try:
            for elem in [
                table.select,
                lambda **kwargs: sa.func.current_timestamp(**kwargs).select(),
                # func.current_timestamp().select,
                lambda **kwargs:text("select * from test_table", **kwargs)
            ]:
                for bind in (
                    testing.db,
                    testing.db.connect()
                ):
                    try:
                        e = elem(bind=bind)
                        assert e.bind is bind
                        e.execute().close()
                    finally:
                        if isinstance(bind, engine.Connection):
                            bind.close()

                e = elem()
                assert e.bind is None
                assert_raises(
                    exc.UnboundExecutionError,
                    e.execute
                )
        finally:
            if isinstance(bind, engine.Connection):
                bind.close()
            metadata.drop_all(bind=testing.db)
