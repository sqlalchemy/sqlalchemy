#! coding: utf-8

from .. import assert_raises
from .. import config
from .. import eq_
from .. import fixtures
from .. import provide_metadata
from ..config import requirements
from ..schema import Column
from ..schema import Table
from ... import exc
from ... import Integer
from ... import literal_column
from ... import select
from ... import String
from ...util import compat


class ExceptionTest(fixtures.TablesTest):
    """Test basic exception wrapping.

    DBAPIs vary a lot in exception behavior so to actually anticipate
    specific exceptions from real round trips, we need to be conservative.

    """

    run_deletes = "each"

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "manual_pk",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )

    @requirements.duplicate_key_raises_integrity_error
    def test_integrity_error(self):

        with config.db.connect() as conn:

            trans = conn.begin()
            conn.execute(
                self.tables.manual_pk.insert(), {"id": 1, "data": "d1"}
            )

            assert_raises(
                exc.IntegrityError,
                conn.execute,
                self.tables.manual_pk.insert(),
                {"id": 1, "data": "d1"},
            )

            trans.rollback()

    def test_exception_with_non_ascii(self):
        with config.db.connect() as conn:
            try:
                # try to create an error message that likely has non-ascii
                # characters in the DBAPI's message string.  unfortunately
                # there's no way to make this happen with some drivers like
                # mysqlclient, pymysql.  this at least does produce a non-
                # ascii error message for cx_oracle, psycopg2
                conn.execute(select([literal_column(u"méil")]))
                assert False
            except exc.DBAPIError as err:
                err_str = str(err)

                assert str(err.orig) in str(err)

            # test that we are actually getting string on Py2k, unicode
            # on Py3k.
            if compat.py2k:
                assert isinstance(err_str, str)
            else:
                assert isinstance(err_str, str)


class AutocommitTest(fixtures.TablesTest):

    run_deletes = "each"

    __requires__ = ("autocommit",)

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
            test_needs_acid=True,
        )

    def _test_conn_autocommits(self, conn, autocommit):
        trans = conn.begin()
        conn.execute(
            self.tables.some_table.insert(), {"id": 1, "data": "some data"}
        )
        trans.rollback()

        eq_(
            conn.scalar(select([self.tables.some_table.c.id])),
            1 if autocommit else None,
        )

        conn.execute(self.tables.some_table.delete())

    def test_autocommit_on(self):
        conn = config.db.connect()
        c2 = conn.execution_options(isolation_level="AUTOCOMMIT")
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
        t = Table("t", m, Column("data", String(50)))
        t.create(config.db)
        with config.db.begin() as conn:
            conn.execute(t.insert(), dict(data="some % value"))
            conn.execute(t.insert(), dict(data="some %% other value"))

            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some % value'")
                    )
                ),
                "some % value",
            )

            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some %% other value'")
                    )
                ),
                "some %% other value",
            )
