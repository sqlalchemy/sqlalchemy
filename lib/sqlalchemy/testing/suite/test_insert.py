from .. import fixtures, config
from ..config import requirements
from .. import exclusions
from ..assertions import eq_
from .. import engines

from sqlalchemy import Integer, String, select, util

from ..schema import Table, Column


class LastrowidTest(fixtures.TablesTest):
    run_deletes = 'each'

    __requires__ = 'implements_get_lastrowid', 'autoincrement_insert'

    __engine_options__ = {"implicit_returning": False}

    @classmethod
    def define_tables(cls, metadata):
        Table('autoinc_pk', metadata,
                Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
                Column('data', String(50))
            )

        Table('manual_pk', metadata,
                Column('id', Integer, primary_key=True, autoincrement=False),
                Column('data', String(50))
            )

    def _assert_round_trip(self, table, conn):
        row = conn.execute(table.select()).first()
        eq_(
            row,
            (1, "some data")
        )

    def test_autoincrement_on_insert(self):

        config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        self._assert_round_trip(self.tables.autoinc_pk, config.db)

    def test_last_inserted_id(self):

        r = config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        pk = config.db.scalar(select([self.tables.autoinc_pk.c.id]))
        eq_(
            r.inserted_primary_key,
            [pk]
        )

    @exclusions.fails_if(lambda: util.pypy, "lastrowid not maintained after "
                            "connection close")
    @requirements.dbapi_lastrowid
    def test_native_lastrowid_autoinc(self):
        r = config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        lastrowid = r.lastrowid
        pk = config.db.scalar(select([self.tables.autoinc_pk.c.id]))
        eq_(
            lastrowid, pk
        )


class InsertBehaviorTest(fixtures.TablesTest):
    run_deletes = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('autoinc_pk', metadata,
                Column('id', Integer, primary_key=True, \
                                test_needs_autoincrement=True),
                Column('data', String(50))
            )

    def test_autoclose_on_insert(self):
        if requirements.returning.enabled:
            engine = engines.testing_engine(
                            options={'implicit_returning': False})
        else:
            engine = config.db

        r = engine.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        assert r.closed
        assert r.is_insert
        assert not r.returns_rows

    @requirements.returning
    def test_autoclose_on_insert_implicit_returning(self):
        r = config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        assert r.closed
        assert r.is_insert
        assert not r.returns_rows


class ReturningTest(fixtures.TablesTest):
    run_deletes = 'each'
    __requires__ = 'returning', 'autoincrement_insert'

    __engine_options__ = {"implicit_returning": True}

    def _assert_round_trip(self, table, conn):
        row = conn.execute(table.select()).first()
        eq_(
            row,
            (1, "some data")
        )

    @classmethod
    def define_tables(cls, metadata):
        Table('autoinc_pk', metadata,
                Column('id', Integer, primary_key=True, \
                                test_needs_autoincrement=True),
                Column('data', String(50))
            )

    def test_explicit_returning_pk(self):
        engine = config.db
        table = self.tables.autoinc_pk
        r = engine.execute(
            table.insert().returning(
                            table.c.id),
            data="some data"
        )
        pk = r.first()[0]
        fetched_pk = config.db.scalar(select([table.c.id]))
        eq_(fetched_pk, pk)

    def test_autoincrement_on_insert_implcit_returning(self):

        config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        self._assert_round_trip(self.tables.autoinc_pk, config.db)

    def test_last_inserted_id_implicit_returning(self):

        r = config.db.execute(
            self.tables.autoinc_pk.insert(),
            data="some data"
        )
        pk = config.db.scalar(select([self.tables.autoinc_pk.c.id]))
        eq_(
            r.inserted_primary_key,
            [pk]
        )


__all__ = ('LastrowidTest', 'InsertBehaviorTest', 'ReturningTest')
