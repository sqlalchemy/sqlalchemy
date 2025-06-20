import sqlalchemy as sa
from sqlalchemy import BigInteger
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.dialects import sqlite
from sqlalchemy.schema import CreateSequence
from sqlalchemy.schema import DropSequence
from sqlalchemy.sql import select
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import EachOf
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class SequenceDDLTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"
    __backend__ = True

    @testing.combinations(
        (Sequence("foo_seq"), ""),
        (Sequence("foo_seq", start=5), "START WITH 5"),
        (Sequence("foo_seq", increment=2), "INCREMENT BY 2"),
        (
            Sequence("foo_seq", increment=2, start=5),
            "INCREMENT BY 2 START WITH 5",
        ),
        (
            Sequence("foo_seq", increment=2, start=0, minvalue=0),
            "INCREMENT BY 2 START WITH 0 MINVALUE 0",
        ),
        (
            Sequence("foo_seq", increment=2, start=1, maxvalue=5),
            "INCREMENT BY 2 START WITH 1 MAXVALUE 5",
        ),
        (
            Sequence("foo_seq", increment=2, start=1, nomaxvalue=True),
            "INCREMENT BY 2 START WITH 1 NO MAXVALUE",
        ),
        (
            Sequence("foo_seq", increment=2, start=0, nominvalue=True),
            "INCREMENT BY 2 START WITH 0 NO MINVALUE",
        ),
        (
            Sequence("foo_seq", start=1, maxvalue=10, cycle=True),
            "START WITH 1 MAXVALUE 10 CYCLE",
        ),
        (
            Sequence("foo_seq", cache=1000),
            "CACHE 1000",
        ),
        (Sequence("foo_seq", minvalue=42), "MINVALUE 42"),
        (Sequence("foo_seq", minvalue=-42), "MINVALUE -42"),
        (
            Sequence("foo_seq", minvalue=42, increment=2),
            "INCREMENT BY 2 MINVALUE 42",
        ),
        (
            Sequence("foo_seq", minvalue=-42, increment=2),
            "INCREMENT BY 2 MINVALUE -42",
        ),
        (
            Sequence("foo_seq", minvalue=42, increment=-2),
            "INCREMENT BY -2 MINVALUE 42",
        ),
        (
            Sequence("foo_seq", minvalue=-42, increment=-2),
            "INCREMENT BY -2 MINVALUE -42",
        ),
        (Sequence("foo_seq", maxvalue=99), "MAXVALUE 99"),
        (Sequence("foo_seq", maxvalue=-99), "MAXVALUE -99"),
        (
            Sequence("foo_seq", maxvalue=99, increment=2),
            "INCREMENT BY 2 MAXVALUE 99",
        ),
        (
            Sequence("foo_seq", maxvalue=99, increment=-2),
            "INCREMENT BY -2 MAXVALUE 99",
        ),
        (
            Sequence("foo_seq", maxvalue=-99, increment=-2),
            "INCREMENT BY -2 MAXVALUE -99",
        ),
        (
            Sequence("foo_seq", minvalue=42, maxvalue=99),
            "MINVALUE 42 MAXVALUE 99",
        ),
        (
            Sequence("foo_seq", minvalue=42, maxvalue=99, increment=2),
            "INCREMENT BY 2 MINVALUE 42 MAXVALUE 99",
        ),
        (
            Sequence("foo_seq", minvalue=-42, maxvalue=-9, increment=2),
            "INCREMENT BY 2 MINVALUE -42 MAXVALUE -9",
        ),
        (
            Sequence("foo_seq", minvalue=42, maxvalue=99, increment=-2),
            "INCREMENT BY -2 MINVALUE 42 MAXVALUE 99",
        ),
        (
            Sequence("foo_seq", minvalue=-42, maxvalue=-9, increment=-2),
            "INCREMENT BY -2 MINVALUE -42 MAXVALUE -9",
        ),
    )
    def test_create_ddl(self, sequence, sql):
        before = sequence.start
        self.assert_compile(
            CreateSequence(sequence),
            ("CREATE SEQUENCE foo_seq " + sql).strip(),
        )
        eq_(sequence.start, before)

    def test_drop_ddl(self):
        self.assert_compile(
            CreateSequence(Sequence("foo_seq"), if_not_exists=True),
            "CREATE SEQUENCE IF NOT EXISTS foo_seq",
        )

        self.assert_compile(
            DropSequence(Sequence("foo_seq")), "DROP SEQUENCE foo_seq"
        )

        self.assert_compile(
            DropSequence(Sequence("foo_seq"), if_exists=True),
            "DROP SEQUENCE IF EXISTS foo_seq",
        )


class SequenceExecTest(fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        cls.seq = normalize_sequence(config, Sequence("my_sequence"))
        cls.seq.create(testing.db)

    @classmethod
    def teardown_test_class(cls):
        cls.seq.drop(testing.db)

    def _assert_seq_result(self, ret):
        """asserts return of next_value is an int"""

        assert isinstance(ret, int)
        assert ret >= testing.db.dialect.default_sequence_base

    def test_execute(self, connection):
        s = normalize_sequence(config, Sequence("my_sequence"))
        self._assert_seq_result(connection.scalar(s))

    def test_execute_deprecated(self, connection):
        s = normalize_sequence(config, Sequence("my_sequence", optional=True))

        with expect_deprecated(
            r"Using the .execute\(\) method to invoke a "
            r"DefaultGenerator object is deprecated; please use "
            r"the .scalar\(\) method."
        ):
            self._assert_seq_result(connection.execute(s))

    def test_scalar_optional(self, connection):
        """test dialect executes a Sequence, returns nextval, whether
        or not "optional" is set"""

        s = normalize_sequence(config, Sequence("my_sequence", optional=True))
        self._assert_seq_result(connection.scalar(s))

    def test_execute_next_value(self, connection):
        """test func.next_value().execute()/.scalar() works
        with connectionless execution."""

        s = normalize_sequence(config, Sequence("my_sequence"))
        self._assert_seq_result(connection.scalar(s.next_value()))

    def test_execute_optional_next_value(self, connection):
        """test func.next_value().execute()/.scalar() works
        with connectionless execution."""

        s = normalize_sequence(config, Sequence("my_sequence", optional=True))
        self._assert_seq_result(connection.scalar(s.next_value()))

    def test_func_embedded_select(self, connection):
        """test can use next_value() in select column expr"""

        s = normalize_sequence(config, Sequence("my_sequence"))
        self._assert_seq_result(connection.scalar(select(s.next_value())))

    @testing.requires.sequences_in_other_clauses
    @testing.provide_metadata
    def test_func_embedded_whereclause(self, connection):
        """test can use next_value() in whereclause"""

        metadata = self.metadata
        t1 = Table("t", metadata, Column("x", Integer))
        t1.create(testing.db)
        connection.execute(t1.insert(), [{"x": 1}, {"x": 300}, {"x": 301}])
        s = normalize_sequence(config, Sequence("my_sequence"))
        eq_(
            list(
                connection.execute(t1.select().where(t1.c.x > s.next_value()))
            ),
            [(300,), (301,)],
        )

    @testing.provide_metadata
    def test_func_embedded_valuesbase(self, connection):
        """test can use next_value() in values() of _ValuesBase"""

        metadata = self.metadata
        t1 = Table(
            "t",
            metadata,
            Column("x", Integer),
        )
        t1.create(testing.db)
        s = normalize_sequence(config, Sequence("my_sequence"))
        connection.execute(t1.insert().values(x=s.next_value()))
        self._assert_seq_result(connection.scalar(t1.select()))

    def test_inserted_pk_no_returning(self, metadata, connection):
        """test inserted_primary_key contains [None] when
        pk_col=next_value(), implicit returning is not used."""

        # I'm not really sure what this test wants to accomlish.

        t1 = Table(
            "t",
            metadata,
            Column("x", Integer, primary_key=True),
            implicit_returning=False,
        )
        s = normalize_sequence(
            config, Sequence("my_sequence_here", metadata=metadata)
        )

        conn = connection
        t1.create(conn)
        s.create(conn)

        r = conn.execute(t1.insert().values(x=s.next_value()))

        if testing.requires.emulated_lastrowid_even_with_sequences.enabled:
            eq_(r.inserted_primary_key, (1,))
        else:
            eq_(r.inserted_primary_key, (None,))

    @testing.combinations(
        ("implicit_returning",),
        ("no_implicit_returning",),
        ("explicit_returning", testing.requires.insert_returning),
        (
            "return_defaults_no_implicit_returning",
            testing.requires.insert_returning,
        ),
        (
            "return_defaults_implicit_returning",
            testing.requires.insert_returning,
        ),
        argnames="returning",
    )
    @testing.requires.multivalues_inserts
    def test_seq_multivalues_inline(self, metadata, connection, returning):
        _implicit_returning = "no_implicit_returning" not in returning
        t1 = Table(
            "t",
            metadata,
            Column(
                "x",
                Integer,
                normalize_sequence(config, Sequence("my_seq")),
                primary_key=True,
            ),
            Column("data", String(50)),
            implicit_returning=_implicit_returning,
        )

        metadata.create_all(connection)
        conn = connection

        stmt = t1.insert().values(
            [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}]
        )
        if returning == "explicit_returning":
            stmt = stmt.returning(t1.c.x)
        elif "return_defaults" in returning:
            stmt = stmt.return_defaults()

        r = conn.execute(stmt)
        if returning == "explicit_returning":
            eq_(r.all(), [(1,), (2,), (3,)])
        elif "return_defaults" in returning:
            eq_(r.returned_defaults_rows, None)

            # TODO: not sure what this is
            eq_(r.inserted_primary_key_rows, [(None,)])

        eq_(
            conn.execute(t1.select().order_by(t1.c.x)).all(),
            [(1, "d1"), (2, "d2"), (3, "d3")],
        )

    @testing.combinations(
        ("implicit_returning",),
        ("no_implicit_returning",),
        (
            "explicit_returning",
            testing.requires.insert_returning
            + testing.requires.insert_executemany_returning,
        ),
        (
            "return_defaults_no_implicit_returning",
            testing.requires.insert_returning
            + testing.requires.insert_executemany_returning,
        ),
        (
            "return_defaults_implicit_returning",
            testing.requires.insert_returning
            + testing.requires.insert_executemany_returning,
        ),
        argnames="returning",
    )
    def test_seq_multivalues_executemany(
        self, connection, metadata, returning
    ):
        _implicit_returning = "no_implicit_returning" not in returning
        t1 = Table(
            "t",
            metadata,
            Column(
                "x",
                Integer,
                normalize_sequence(config, Sequence("my_seq")),
                primary_key=True,
            ),
            Column("data", String(50)),
            implicit_returning=_implicit_returning,
        )

        metadata.create_all(connection)
        conn = connection

        stmt = t1.insert()
        if returning == "explicit_returning":
            stmt = stmt.returning(t1.c.x)
        elif "return_defaults" in returning:
            stmt = stmt.return_defaults()

        r = conn.execute(
            stmt, [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}]
        )
        if returning == "explicit_returning":
            eq_(r.all(), [(1,), (2,), (3,)])
        elif "return_defaults" in returning:
            if "no_implicit_returning" in returning:
                eq_(r.returned_defaults_rows, None)
                eq_(r.inserted_primary_key_rows, [(1,), (2,), (3,)])
            else:
                eq_(r.returned_defaults_rows, [(1,), (2,), (3,)])
                eq_(r.inserted_primary_key_rows, [(1,), (2,), (3,)])

        eq_(
            conn.execute(t1.select().order_by(t1.c.x)).all(),
            [(1, "d1"), (2, "d2"), (3, "d3")],
        )

    @testing.requires.insert_returning
    def test_inserted_pk_implicit_returning(self, connection, metadata):
        """test inserted_primary_key contains the result when
        pk_col=next_value(), when implicit returning is used."""

        s = normalize_sequence(config, Sequence("my_sequence"))
        t1 = Table(
            "t",
            metadata,
            Column(
                "x",
                Integer,
                primary_key=True,
            ),
            implicit_returning=True,
        )
        t1.create(connection)

        r = connection.execute(t1.insert().values(x=s.next_value()))
        self._assert_seq_result(r.inserted_primary_key[0])


class SequenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __requires__ = ("sequences",)
    __backend__ = True

    @testing.combinations(
        (Sequence("foo_seq"),),
        (Sequence("foo_seq", start=8),),
        (Sequence("foo_seq", increment=5),),
    )
    def test_start_increment(self, seq):
        seq = normalize_sequence(config, seq)
        seq.create(testing.db)
        try:
            with testing.db.connect() as conn:
                values = [conn.scalar(seq) for i in range(3)]
                start = seq.start or testing.db.dialect.default_sequence_base
                inc = seq.increment or 1
                eq_(values, list(range(start, start + inc * 3, inc)))

        finally:
            seq.drop(testing.db)

    def _has_sequence(self, connection, name):
        return testing.db.dialect.has_sequence(connection, name)

    def test_nextval_unsupported(self):
        """test next_value() used on non-sequence platform
        raises NotImplementedError."""

        s = normalize_sequence(config, Sequence("my_seq"))
        d = sqlite.dialect()
        assert_raises_message(
            NotImplementedError,
            "Dialect 'sqlite' does not support sequence increments.",
            s.next_value().compile,
            dialect=d,
        )

    def test_checkfirst_sequence(self, connection):
        s = normalize_sequence(config, Sequence("my_sequence"))
        s.create(connection, checkfirst=False)
        assert self._has_sequence(connection, "my_sequence")
        s.create(connection, checkfirst=True)
        s.drop(connection, checkfirst=False)
        assert not self._has_sequence(connection, "my_sequence")
        s.drop(connection, checkfirst=True)

    def test_checkfirst_metadata(self, connection):
        m = MetaData()
        Sequence("my_sequence", metadata=m)
        m.create_all(connection, checkfirst=False)
        assert self._has_sequence(connection, "my_sequence")
        m.create_all(connection, checkfirst=True)
        m.drop_all(connection, checkfirst=False)
        assert not self._has_sequence(connection, "my_sequence")
        m.drop_all(connection, checkfirst=True)

    def test_checkfirst_table(self, connection):
        m = MetaData()
        s = normalize_sequence(config, Sequence("my_sequence"))
        t = Table("t", m, Column("c", Integer, s, primary_key=True))
        t.create(connection, checkfirst=False)
        assert self._has_sequence(connection, "my_sequence")
        t.create(connection, checkfirst=True)
        t.drop(connection, checkfirst=False)
        assert not self._has_sequence(connection, "my_sequence")
        t.drop(connection, checkfirst=True)

    @testing.provide_metadata
    def test_table_overrides_metadata_create(self, connection):
        metadata = self.metadata
        normalize_sequence(config, Sequence("s1", metadata=metadata))
        s2 = normalize_sequence(config, Sequence("s2", metadata=metadata))
        s3 = normalize_sequence(config, Sequence("s3"))
        t = Table("t", metadata, Column("c", Integer, s3, primary_key=True))
        assert s3.metadata is metadata

        t.create(connection, checkfirst=True)
        s3.drop(connection)

        # 't' is created, and 's3' won't be
        # re-created since it's linked to 't'.
        # 's1' and 's2' are, however.
        metadata.create_all(connection)
        assert self._has_sequence(connection, "s1")
        assert self._has_sequence(connection, "s2")
        assert not self._has_sequence(connection, "s3")

        s2.drop(connection)
        assert self._has_sequence(connection, "s1")
        assert not self._has_sequence(connection, "s2")

        metadata.drop_all(connection)
        assert not self._has_sequence(connection, "s1")
        assert not self._has_sequence(connection, "s2")

    @testing.requires.insert_returning
    @testing.requires.supports_sequence_for_autoincrement_column
    @testing.provide_metadata
    def test_freestanding_sequence_via_autoinc(self, connection):
        t = Table(
            "some_table",
            self.metadata,
            Column(
                "id",
                Integer,
                autoincrement=True,
                primary_key=True,
                default=normalize_sequence(
                    config, Sequence("my_sequence", metadata=self.metadata)
                ).next_value(),
            ),
        )
        self.metadata.create_all(connection)

        result = connection.execute(t.insert())
        eq_(result.inserted_primary_key, (1,))

    @testing.requires.sequences_as_server_defaults
    @testing.provide_metadata
    def test_shared_sequence(self, connection):
        # test case for #6071
        common_seq = normalize_sequence(
            config, Sequence("common_sequence", metadata=self.metadata)
        )
        Table(
            "table_1",
            self.metadata,
            Column(
                "id",
                Integer,
                common_seq,
                server_default=common_seq.next_value(),
                primary_key=True,
            ),
        )
        Table(
            "table_2",
            self.metadata,
            Column(
                "id",
                Integer,
                common_seq,
                server_default=common_seq.next_value(),
                primary_key=True,
            ),
        )

        self.metadata.create_all(connection)
        is_true(self._has_sequence(connection, "common_sequence"))
        is_true(testing.db.dialect.has_table(connection, "table_1"))
        is_true(testing.db.dialect.has_table(connection, "table_2"))
        self.metadata.drop_all(connection)
        is_false(self._has_sequence(connection, "common_sequence"))
        is_false(testing.db.dialect.has_table(connection, "table_1"))
        is_false(testing.db.dialect.has_table(connection, "table_2"))

    def test_next_value_type(self):
        seq = normalize_sequence(
            config, Sequence("my_sequence", data_type=BigInteger)
        )
        assert isinstance(seq.next_value().type, BigInteger)


class TableBoundSequenceTest(fixtures.TablesTest):
    __requires__ = ("sequences",)
    __backend__ = True

    @testing.fixture
    def table_fixture(self, metadata, connection, implicit_returning):
        def go(implicit_returning):
            cartitems = Table(
                "cartitems",
                metadata,
                Column(
                    "cart_id",
                    Integer,
                    normalize_sequence(config, Sequence("cart_id_seq")),
                    primary_key=True,
                    autoincrement=False,
                ),
                Column("description", String(40)),
                Column("createdate", sa.DateTime()),
                implicit_returning=implicit_returning,
            )

            # a little bit of implicit case sensitive naming test going on here
            Manager = Table(
                "Manager",
                metadata,
                Column(
                    "obj_id",
                    Integer,
                    normalize_sequence(config, Sequence("obj_id_seq")),
                ),
                Column("name", String(128)),
                Column(
                    "id",
                    Integer,
                    normalize_sequence(
                        config, Sequence("Manager_id_seq", optional=True)
                    ),
                    primary_key=True,
                ),
                implicit_returning=implicit_returning,
            )
            metadata.create_all(connection)
            return Manager, cartitems

        return go

    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_insert_via_seq(
        self, table_fixture, connection, implicit_returning
    ):
        Manager, cartitems = table_fixture(implicit_returning)

        connection.execute(cartitems.insert(), dict(description="hi"))
        connection.execute(cartitems.insert(), dict(description="there"))
        r = connection.execute(cartitems.insert(), dict(description="lala"))

        expected = 2 + testing.db.dialect.default_sequence_base
        eq_(r.inserted_primary_key[0], expected)

        eq_(
            connection.scalar(
                sa.select(cartitems.c.cart_id).where(
                    cartitems.c.description == "lala"
                ),
            ),
            expected,
        )

    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_seq_nonpk(self, connection, table_fixture, implicit_returning):
        """test sequences fire off as defaults on non-pk columns"""

        sometable, cartitems = table_fixture(implicit_returning)

        conn = connection
        result = conn.execute(sometable.insert(), dict(name="somename"))

        eq_(result.postfetch_cols(), [sometable.c.obj_id])

        result = conn.execute(sometable.insert(), dict(name="someother"))

        conn.execute(
            sometable.insert(), [{"name": "name3"}, {"name": "name4"}]
        )

        dsb = testing.db.dialect.default_sequence_base
        eq_(
            list(conn.execute(sometable.select().order_by(sometable.c.id))),
            [
                (
                    dsb,
                    "somename",
                    dsb,
                ),
                (
                    dsb + 1,
                    "someother",
                    dsb + 1,
                ),
                (
                    dsb + 2,
                    "name3",
                    dsb + 2,
                ),
                (
                    dsb + 3,
                    "name4",
                    dsb + 3,
                ),
            ],
        )


class SequenceAsServerDefaultTest(
    testing.AssertsExecutionResults, fixtures.TablesTest
):
    __requires__ = ("sequences_as_server_defaults",)
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        m = metadata

        s = normalize_sequence(config, Sequence("t_seq", metadata=m))
        Table(
            "t_seq_test",
            m,
            Column("id", Integer, s, server_default=s.next_value()),
            Column("data", String(50)),
        )

        s2 = normalize_sequence(config, Sequence("t_seq_2", metadata=m))
        Table(
            "t_seq_test_2",
            m,
            Column("id", Integer, server_default=s2.next_value()),
            Column("data", String(50)),
        )

    def test_default_textual_w_default(self, connection):
        connection.exec_driver_sql(
            "insert into t_seq_test (data) values ('some data')"
        )

        eq_(
            connection.exec_driver_sql("select id from t_seq_test").scalar(), 1
        )

    def test_default_core_w_default(self, connection):
        t_seq_test = self.tables.t_seq_test
        connection.execute(t_seq_test.insert().values(data="some data"))

        eq_(connection.scalar(select(t_seq_test.c.id)), 1)

    def test_default_textual_server_only(self, connection):
        connection.exec_driver_sql(
            "insert into t_seq_test_2 (data) values ('some data')"
        )

        eq_(
            connection.exec_driver_sql("select id from t_seq_test_2").scalar(),
            1,
        )

    def test_default_core_server_only(self, connection):
        t_seq_test = self.tables.t_seq_test_2
        connection.execute(t_seq_test.insert().values(data="some data"))

        eq_(connection.scalar(select(t_seq_test.c.id)), 1)

    def test_drop_ordering(self):
        with self.sql_execution_asserter(testing.db) as asserter:
            self.tables_test_metadata.drop_all(testing.db, checkfirst=False)

        asserter.assert_(
            AllOf(
                CompiledSQL("DROP TABLE t_seq_test_2", {}),
                CompiledSQL("DROP TABLE t_seq_test", {}),
            ),
            AllOf(
                # dropped as part of metadata level
                CompiledSQL("DROP SEQUENCE t_seq", {}),
                CompiledSQL("DROP SEQUENCE t_seq_2", {}),
            ),
        )

    def test_drop_ordering_single_table(self):
        with self.sql_execution_asserter(testing.db) as asserter:
            for table in self.tables_test_metadata.tables.values():
                table.drop(testing.db, checkfirst=False)

        asserter.assert_(
            AllOf(
                CompiledSQL("DROP TABLE t_seq_test_2", {}),
                EachOf(
                    CompiledSQL("DROP TABLE t_seq_test", {}),
                    CompiledSQL("DROP SEQUENCE t_seq", {}),
                ),
            )
        )
