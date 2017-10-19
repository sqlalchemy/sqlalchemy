from .. import fixtures, config
from ..config import requirements
from sqlalchemy import exc
from sqlalchemy import Integer, String, select, literal_column
from .. import assert_raises
from ..schema import Table, Column
from .. import provide_metadata
from .. import eq_


class ExceptionTest(fixtures.TablesTest):
    """Test basic exception wrapping.

    DBAPIs vary a lot in exception behavior so to actually anticipate
    specific exceptions from real round trips, we need to be conservative.

    """
    run_deletes = 'each'

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('manual_pk', metadata,
              Column('id', Integer, primary_key=True, autoincrement=False),
              Column('data', String(50))
              )

    @requirements.duplicate_key_raises_integrity_error
    def test_integrity_error(self):

        with config.db.connect() as conn:

            trans = conn.begin()
            conn.execute(
                self.tables.manual_pk.insert(),
                {'id': 1, 'data': 'd1'}
            )

            assert_raises(
                exc.IntegrityError,
                conn.execute,
                self.tables.manual_pk.insert(),
                {'id': 1, 'data': 'd1'}
            )

            trans.rollback()


class AutocommitTest(fixtures.TablesTest):

    run_deletes = 'each'

    __requires__ = 'autocommit',

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('some_table', metadata,
              Column('id', Integer, primary_key=True, autoincrement=False),
              Column('data', String(50)),
              test_needs_acid=True
              )

    def _test_conn_autocommits(self, conn, autocommit):
        trans = conn.begin()
        conn.execute(
            self.tables.some_table.insert(),
            {"id": 1, "data": "some data"}
        )
        trans.rollback()

        eq_(
            conn.scalar(select([self.tables.some_table.c.id])),
            1 if autocommit else None
        )

        conn.execute(self.tables.some_table.delete())

    def test_autocommit_on(self):
        conn = config.db.connect()
        c2 = conn.execution_options(isolation_level='AUTOCOMMIT')
        self._test_conn_autocommits(c2, True)
        conn.invalidate()
        self._test_conn_autocommits(conn, False)

    def test_autocommit_off(self):
        conn = config.db.connect()
        self._test_conn_autocommits(conn, False)


class EscapingTest(fixtures.TestBase):
    @provide_metadata
    def test_percent_sign_round_trip(self):
        """test that the DBAPI accommodates for escaped / nonescaped
        percent signs in a way that matches the compiler

        """
        m = self.metadata
        t = Table('t', m, Column('data', String(50)))
        t.create(config.db)
        with config.db.begin() as conn:
            conn.execute(t.insert(), dict(data="some % value"))
            conn.execute(t.insert(), dict(data="some %% other value"))

            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some % value'"))
                ),
                "some % value"
            )

            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some %% other value'"))
                ), "some %% other value"
            )
