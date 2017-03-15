from sqlalchemy.testing import eq_, assert_raises_message, is_
from sqlalchemy import testing
from sqlalchemy.testing import fixtures, engines
from sqlalchemy import (
    exc, sql, String, Integer, MetaData, and_, ForeignKey,
    VARCHAR, INT, Sequence, func)
from sqlalchemy.testing.schema import Table, Column


class InsertExecTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'users', metadata,
            Column(
                'user_id', INT, primary_key=True,
                test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )

    @testing.requires.multivalues_inserts
    def test_multivalues_insert(self):
        users = self.tables.users
        users.insert(
            values=[
                {'user_id': 7, 'user_name': 'jack'},
                {'user_id': 8, 'user_name': 'ed'}]).execute()
        rows = users.select().order_by(users.c.user_id).execute().fetchall()
        eq_(rows[0], (7, 'jack'))
        eq_(rows[1], (8, 'ed'))
        users.insert(values=[(9, 'jack'), (10, 'ed')]).execute()
        rows = users.select().order_by(users.c.user_id).execute().fetchall()
        eq_(rows[2], (9, 'jack'))
        eq_(rows[3], (10, 'ed'))

    def test_insert_heterogeneous_params(self):
        """test that executemany parameters are asserted to match the
        parameter set of the first."""
        users = self.tables.users

        assert_raises_message(
            exc.StatementError,
            r"\(sqlalchemy.exc.InvalidRequestError\) A value is required for "
            "bind parameter 'user_name', in "
            "parameter group 2 "
            r"\[SQL: u?'INSERT INTO users",
            users.insert().execute,
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9}
        )

        # this succeeds however.   We aren't yet doing
        # a length check on all subsequent parameters.
        users.insert().execute(
            {'user_id': 7},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9}
        )

    def _test_lastrow_accessor(self, table_, values, assertvalues):
        """Tests the inserted_primary_key and lastrow_has_id() functions."""

        def insert_values(engine, table_, values):
            """
            Inserts a row into a table, returns the full list of values
            INSERTed including defaults that fired off on the DB side and
            detects rows that had defaults and post-fetches.
            """

            # verify implicit_returning is working
            if engine.dialect.implicit_returning:
                ins = table_.insert()
                comp = ins.compile(engine, column_keys=list(values))
                if not set(values).issuperset(
                        c.key for c in table_.primary_key):
                    is_(bool(comp.returning), True)

            result = engine.execute(table_.insert(), **values)
            ret = values.copy()

            for col, id in zip(
                    table_.primary_key, result.inserted_primary_key):
                ret[col.key] = id

            if result.lastrow_has_defaults():
                criterion = and_(
                    *[
                        col == id for col, id in
                        zip(table_.primary_key, result.inserted_primary_key)])
                row = engine.execute(table_.select(criterion)).first()
                for c in table_.c:
                    ret[c.key] = row[c]
            return ret

        if testing.against('firebird', 'postgresql', 'oracle', 'mssql'):
            assert testing.db.dialect.implicit_returning

        if testing.db.dialect.implicit_returning:
            test_engines = [
                engines.testing_engine(options={'implicit_returning': False}),
                engines.testing_engine(options={'implicit_returning': True}),
            ]
        else:
            test_engines = [testing.db]

        for engine in test_engines:
            try:
                table_.create(bind=engine, checkfirst=True)
                i = insert_values(engine, table_, values)
                eq_(i, assertvalues)
            finally:
                table_.drop(bind=engine)

    @testing.skip_if('sqlite')
    def test_lastrow_accessor_one(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t1", metadata,
                Column(
                    'id', Integer, primary_key=True,
                    test_needs_autoincrement=True),
                Column('foo', String(30), primary_key=True)),
            {'foo': 'hi'},
            {'id': 1, 'foo': 'hi'}
        )

    @testing.skip_if('sqlite')
    def test_lastrow_accessor_two(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t2", metadata,
                Column(
                    'id', Integer, primary_key=True,
                    test_needs_autoincrement=True),
                Column('foo', String(30), primary_key=True),
                Column('bar', String(30), server_default='hi')
            ),
            {'foo': 'hi'},
            {'id': 1, 'foo': 'hi', 'bar': 'hi'}
        )

    def test_lastrow_accessor_three(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t3", metadata,
                Column("id", String(40), primary_key=True),
                Column('foo', String(30), primary_key=True),
                Column("bar", String(30))
            ),
            {'id': 'hi', 'foo': 'thisisfoo', 'bar': "thisisbar"},
            {'id': 'hi', 'foo': 'thisisfoo', 'bar': "thisisbar"}
        )

    def test_lastrow_accessor_four(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t4", metadata,
                Column(
                    'id', Integer,
                    Sequence('t4_id_seq', optional=True),
                    primary_key=True),
                Column('foo', String(30), primary_key=True),
                Column('bar', String(30), server_default='hi')
            ),
            {'foo': 'hi', 'id': 1},
            {'id': 1, 'foo': 'hi', 'bar': 'hi'}
        )

    def test_lastrow_accessor_five(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t5", metadata,
                Column('id', String(10), primary_key=True),
                Column('bar', String(30), server_default='hi')
            ),
            {'id': 'id1'},
            {'id': 'id1', 'bar': 'hi'},
        )

    @testing.skip_if('sqlite')
    def test_lastrow_accessor_six(self):
        metadata = MetaData()
        self._test_lastrow_accessor(
            Table(
                "t6", metadata,
                Column(
                    'id', Integer, primary_key=True,
                    test_needs_autoincrement=True),
                Column('bar', Integer, primary_key=True)
            ),
            {'bar': 0},
            {'id': 1, 'bar': 0},
        )

    # TODO: why not in the sqlite suite?
    @testing.only_on('sqlite+pysqlite')
    @testing.provide_metadata
    def test_lastrowid_zero(self):
        from sqlalchemy.dialects import sqlite
        eng = engines.testing_engine()

        class ExcCtx(sqlite.base.SQLiteExecutionContext):

            def get_lastrowid(self):
                return 0
        eng.dialect.execution_ctx_cls = ExcCtx
        t = Table(
            't', self.metadata, Column('x', Integer, primary_key=True),
            Column('y', Integer))
        t.create(eng)
        r = eng.execute(t.insert().values(y=5))
        eq_(r.inserted_primary_key, [0])

    @testing.fails_on(
        'sqlite', "sqlite autoincremnt doesn't work with composite pks")
    @testing.provide_metadata
    def test_misordered_lastrow(self):
        metadata = self.metadata

        related = Table(
            'related', metadata,
            Column('id', Integer, primary_key=True),
            mysql_engine='MyISAM'
        )
        t6 = Table(
            "t6", metadata,
            Column(
                'manual_id', Integer, ForeignKey('related.id'),
                primary_key=True),
            Column(
                'auto_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            mysql_engine='MyISAM'
        )

        metadata.create_all()
        r = related.insert().values(id=12).execute()
        id_ = r.inserted_primary_key[0]
        eq_(id_, 12)

        r = t6.insert().values(manual_id=id_).execute()
        eq_(r.inserted_primary_key, [12, 1])

    def test_implicit_id_insert_select_columns(self):
        users = self.tables.users
        stmt = users.insert().from_select(
            (users.c.user_id, users.c.user_name),
            users.select().where(users.c.user_id == 20))

        testing.db.execute(stmt)

    def test_implicit_id_insert_select_keys(self):
        users = self.tables.users
        stmt = users.insert().from_select(
            ["user_id", "user_name"],
            users.select().where(users.c.user_id == 20))

        testing.db.execute(stmt)

    @testing.requires.empty_inserts
    @testing.requires.returning
    def test_no_inserted_pk_on_returning(self):
        users = self.tables.users
        result = testing.db.execute(users.insert().returning(
            users.c.user_id, users.c.user_name))
        assert_raises_message(
            exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr, result, 'inserted_primary_key'
        )


class TableInsertTest(fixtures.TablesTest):

    """test for consistent insert behavior across dialects
    regarding the inline=True flag, lower-case 't' tables.

    """
    run_create_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'foo', metadata,
            Column('id', Integer, Sequence('t_id_seq'), primary_key=True),
            Column('data', String(50)),
            Column('x', Integer)
        )

    def _fixture(self, types=True):
        if types:
            t = sql.table(
                'foo', sql.column('id', Integer),
                sql.column('data', String),
                sql.column('x', Integer))
        else:
            t = sql.table(
                'foo', sql.column('id'), sql.column('data'), sql.column('x'))
        return t

    def _test(self, stmt, row, returning=None, inserted_primary_key=False):
        r = testing.db.execute(stmt)

        if returning:
            returned = r.first()
            eq_(returned, returning)
        elif inserted_primary_key is not False:
            eq_(r.inserted_primary_key, inserted_primary_key)

        eq_(testing.db.execute(self.tables.foo.select()).first(), row)

    def _test_multi(self, stmt, rows, data):
        testing.db.execute(stmt, rows)
        eq_(
            testing.db.execute(
                self.tables.foo.select().
                order_by(self.tables.foo.c.id)).fetchall(),
            data)

    @testing.requires.sequences
    def test_explicit_sequence(self):
        t = self._fixture()
        self._test(
            t.insert().values(
                id=func.next_value(Sequence('t_id_seq')), data='data', x=5),
            (1, 'data', 5)
        )

    def test_uppercase(self):
        t = self.tables.foo
        self._test(
            t.insert().values(id=1, data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[1]
        )

    def test_uppercase_inline(self):
        t = self.tables.foo
        self._test(
            t.insert(inline=True).values(id=1, data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[1]
        )

    @testing.crashes(
        "mssql+pyodbc",
        "Pyodbc + SQL Server + Py3K, some decimal handling issue")
    def test_uppercase_inline_implicit(self):
        t = self.tables.foo
        self._test(
            t.insert(inline=True).values(data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[None]
        )

    def test_uppercase_implicit(self):
        t = self.tables.foo
        self._test(
            t.insert().values(data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[1]
        )

    def test_uppercase_direct_params(self):
        t = self.tables.foo
        self._test(
            t.insert().values(id=1, data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[1]
        )

    @testing.requires.returning
    def test_uppercase_direct_params_returning(self):
        t = self.tables.foo
        self._test(
            t.insert().values(id=1, data='data', x=5).returning(t.c.id, t.c.x),
            (1, 'data', 5),
            returning=(1, 5)
        )

    @testing.fails_on(
        'mssql', "lowercase table doesn't support identity insert disable")
    def test_direct_params(self):
        t = self._fixture()
        self._test(
            t.insert().values(id=1, data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[]
        )

    @testing.fails_on(
        'mssql', "lowercase table doesn't support identity insert disable")
    @testing.requires.returning
    def test_direct_params_returning(self):
        t = self._fixture()
        self._test(
            t.insert().values(id=1, data='data', x=5).returning(t.c.id, t.c.x),
            (1, 'data', 5),
            returning=(1, 5)
        )

    @testing.requires.emulated_lastrowid
    def test_implicit_pk(self):
        t = self._fixture()
        self._test(
            t.insert().values(data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[]
        )

    @testing.requires.emulated_lastrowid
    def test_implicit_pk_multi_rows(self):
        t = self._fixture()
        self._test_multi(
            t.insert(),
            [
                {'data': 'd1', 'x': 5},
                {'data': 'd2', 'x': 6},
                {'data': 'd3', 'x': 7},
            ],
            [
                (1, 'd1', 5),
                (2, 'd2', 6),
                (3, 'd3', 7)
            ],
        )

    @testing.requires.emulated_lastrowid
    def test_implicit_pk_inline(self):
        t = self._fixture()
        self._test(
            t.insert(inline=True).values(data='data', x=5),
            (1, 'data', 5),
            inserted_primary_key=[]
        )
