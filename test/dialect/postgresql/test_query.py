# coding: utf-8

from sqlalchemy.testing import AssertsExecutionResults, eq_, \
    assert_raises_message, AssertsCompiledSQL, expect_warnings, assert_raises
from sqlalchemy import Table, Column, MetaData, Integer, String, bindparam, \
    Sequence, ForeignKey, text, select, func, extract, literal_column, \
    tuple_, DateTime, Time, literal, and_, Date, or_
from sqlalchemy.testing import engines, fixtures
from sqlalchemy.testing.assertsql import DialectSQL, CursorSQL
from sqlalchemy import testing
from sqlalchemy import exc
from sqlalchemy.dialects import postgresql
import datetime

matchtable = cattable = None


class InsertTest(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'postgresql'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        cls.metadata = MetaData(testing.db)

    def teardown(self):
        self.metadata.drop_all()
        self.metadata.clear()

    def test_compiled_insert(self):
        table = Table(
            'testtable', self.metadata, Column(
                'id', Integer, primary_key=True),
            Column(
                'data', String(30)))
        self.metadata.create_all()
        ins = table.insert(
            inline=True,
            values={'data': bindparam('x')}).compile()
        ins.execute({'x': 'five'}, {'x': 'seven'})
        eq_(
            table.select().execute().fetchall(),
            [(1, 'five'), (2, 'seven')]
        )

    def test_foreignkey_missing_insert(self):
        Table(
            't1', self.metadata,
            Column('id', Integer, primary_key=True))
        t2 = Table(
            't2',
            self.metadata,
            Column(
                'id',
                Integer,
                ForeignKey('t1.id'),
                primary_key=True))
        self.metadata.create_all()

        # want to ensure that "null value in column "id" violates not-
        # null constraint" is raised (IntegrityError on psycoopg2, but
        # ProgrammingError on pg8000), and not "ProgrammingError:
        # (ProgrammingError) relationship "t2_id_seq" does not exist".
        # the latter corresponds to autoincrement behavior, which is not
        # the case here due to the foreign key.

        for eng in [
            engines.testing_engine(options={'implicit_returning': False}),
            engines.testing_engine(options={'implicit_returning': True})
        ]:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    eng.execute, t2.insert()
                )

    def test_sequence_insert(self):
        table = Table(
            'testtable',
            self.metadata,
            Column(
                'id',
                Integer,
                Sequence('my_seq'),
                primary_key=True),
            Column(
                'data',
                String(30)))
        self.metadata.create_all()
        self._assert_data_with_sequence(table, 'my_seq')

    @testing.requires.returning
    def test_sequence_returning_insert(self):
        table = Table(
            'testtable',
            self.metadata,
            Column(
                'id',
                Integer,
                Sequence('my_seq'),
                primary_key=True),
            Column(
                'data',
                String(30)))
        self.metadata.create_all()
        self._assert_data_with_sequence_returning(table, 'my_seq')

    def test_opt_sequence_insert(self):
        table = Table(
            'testtable', self.metadata,
            Column(
                'id', Integer, Sequence(
                    'my_seq', optional=True), primary_key=True),
            Column(
                'data', String(30)))
        self.metadata.create_all()
        self._assert_data_autoincrement(table)

    @testing.requires.returning
    def test_opt_sequence_returning_insert(self):
        table = Table(
            'testtable', self.metadata,
            Column(
                'id', Integer, Sequence(
                    'my_seq', optional=True), primary_key=True),
            Column(
                'data', String(30)))
        self.metadata.create_all()
        self._assert_data_autoincrement_returning(table)

    def test_autoincrement_insert(self):
        table = Table(
            'testtable', self.metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'data', String(30)))
        self.metadata.create_all()
        self._assert_data_autoincrement(table)

    @testing.requires.returning
    def test_autoincrement_returning_insert(self):
        table = Table(
            'testtable', self.metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'data', String(30)))
        self.metadata.create_all()
        self._assert_data_autoincrement_returning(table)

    def test_noautoincrement_insert(self):
        table = Table(
            'testtable',
            self.metadata,
            Column(
                'id',
                Integer,
                primary_key=True,
                autoincrement=False),
            Column(
                'data',
                String(30)))
        self.metadata.create_all()
        self._assert_data_noautoincrement(table)

    def _assert_data_autoincrement(self, table):
        engine = \
            engines.testing_engine(options={'implicit_returning': False})

        with self.sql_execution_asserter(engine) as asserter:

            with engine.connect() as conn:
                # execute with explicit id

                r = conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                eq_(r.inserted_primary_key, [30])

                # execute with prefetch id

                r = conn.execute(table.insert(), {'data': 'd2'})
                eq_(r.inserted_primary_key, [1])

                # executemany with explicit ids

                conn.execute(
                    table.insert(),
                    {'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'})

                # executemany, uses SERIAL

                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})

                # single execute, explicit id, inline

                conn.execute(
                    table.insert(inline=True),
                    {'id': 33, 'data': 'd7'})

                # single execute, inline, uses SERIAL

                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                {'id': 30, 'data': 'd1'}),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                {'id': 1, 'data': 'd2'}),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL(
                'INSERT INTO testtable (data) VALUES (:data)',
                [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                [{'id': 33, 'data': 'd7'}]),
            DialectSQL(
                'INSERT INTO testtable (data) VALUES (:data)',
                [{'data': 'd8'}]),
        )

        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (1, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (2, 'd5'),
                    (3, 'd6'),
                    (33, 'd7'),
                    (4, 'd8'),
                ]
            )

            conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData(engine)
        table = Table(table.name, m2, autoload=True)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.connect() as conn:
                conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                r = conn.execute(table.insert(), {'data': 'd2'})
                eq_(r.inserted_primary_key, [5])
                conn.execute(
                    table.insert(),
                    {'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'})
                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})
                conn.execute(
                    table.insert(inline=True), {'id': 33, 'data': 'd7'})
                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                {'id': 30, 'data': 'd1'}),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                {'id': 5, 'data': 'd2'}),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL(
                'INSERT INTO testtable (data) VALUES (:data)',
                [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL(
                'INSERT INTO testtable (id, data) VALUES (:id, :data)',
                [{'id': 33, 'data': 'd7'}]),
            DialectSQL(
                'INSERT INTO testtable (data) VALUES (:data)',
                [{'data': 'd8'}]),
        )
        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (5, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (6, 'd5'),
                    (7, 'd6'),
                    (33, 'd7'),
                    (8, 'd8'),
                ]
            )
            conn.execute(table.delete())

    def _assert_data_autoincrement_returning(self, table):
        engine = \
            engines.testing_engine(options={'implicit_returning': True})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.connect() as conn:

                # execute with explicit id

                r = conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                eq_(r.inserted_primary_key, [30])

                # execute with prefetch id

                r = conn.execute(table.insert(), {'data': 'd2'})
                eq_(r.inserted_primary_key, [1])

                # executemany with explicit ids

                conn.execute(
                    table.insert(),
                    {'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'})

                # executemany, uses SERIAL

                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})

                # single execute, explicit id, inline

                conn.execute(
                    table.insert(inline=True), {'id': 33, 'data': 'd7'})

                # single execute, inline, uses SERIAL

                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       {'id': 30, 'data': 'd1'}),
            DialectSQL('INSERT INTO testtable (data) VALUES (:data) RETURNING '
                       'testtable.id', {'data': 'd2'}),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL('INSERT INTO testtable (data) VALUES (:data)',
                       [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 33, 'data': 'd7'}]),
            DialectSQL('INSERT INTO testtable (data) VALUES (:data)',
                       [{'data': 'd8'}]),
        )

        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (1, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (2, 'd5'),
                    (3, 'd6'),
                    (33, 'd7'),
                    (4, 'd8'),
                ]
            )
            conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData(engine)
        table = Table(table.name, m2, autoload=True)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.connect() as conn:
                conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                r = conn.execute(table.insert(), {'data': 'd2'})
                eq_(r.inserted_primary_key, [5])
                conn.execute(
                    table.insert(),
                    {'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'})
                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})
                conn.execute(
                    table.insert(inline=True), {'id': 33, 'data': 'd7'})
                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       {'id': 30, 'data': 'd1'}),
            DialectSQL('INSERT INTO testtable (data) VALUES (:data) RETURNING '
                       'testtable.id', {'data': 'd2'}),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL('INSERT INTO testtable (data) VALUES (:data)',
                       [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 33, 'data': 'd7'}]),
            DialectSQL(
                'INSERT INTO testtable (data) VALUES (:data)',
                [{'data': 'd8'}]),
        )

        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (5, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (6, 'd5'),
                    (7, 'd6'),
                    (33, 'd7'),
                    (8, 'd8'),
                ]
            )
            conn.execute(table.delete())

    def _assert_data_with_sequence(self, table, seqname):
        engine = \
            engines.testing_engine(options={'implicit_returning': False})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.connect() as conn:
                conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                conn.execute(table.insert(), {'data': 'd2'})
                conn.execute(table.insert(),
                             {'id': 31, 'data': 'd3'},
                             {'id': 32, 'data': 'd4'})
                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})
                conn.execute(table.insert(inline=True),
                             {'id': 33, 'data': 'd7'})
                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       {'id': 30, 'data': 'd1'}),
            CursorSQL("select nextval('my_seq')"),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       {'id': 1, 'data': 'd2'}),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL(
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                ":data)" % seqname, [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 33, 'data': 'd7'}]),
            DialectSQL(
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                ":data)" % seqname, [{'data': 'd8'}]),
        )
        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (1, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (2, 'd5'),
                    (3, 'd6'),
                    (33, 'd7'),
                    (4, 'd8'),
                ]
            )

        # cant test reflection here since the Sequence must be
        # explicitly specified

    def _assert_data_with_sequence_returning(self, table, seqname):
        engine = \
            engines.testing_engine(options={'implicit_returning': True})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.connect() as conn:
                conn.execute(table.insert(), {'id': 30, 'data': 'd1'})
                conn.execute(table.insert(), {'data': 'd2'})
                conn.execute(table.insert(),
                             {'id': 31, 'data': 'd3'},
                             {'id': 32, 'data': 'd4'})
                conn.execute(table.insert(), {'data': 'd5'}, {'data': 'd6'})
                conn.execute(
                    table.insert(inline=True), {'id': 33, 'data': 'd7'})
                conn.execute(table.insert(inline=True), {'data': 'd8'})

        asserter.assert_(
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       {'id': 30, 'data': 'd1'}),
            DialectSQL("INSERT INTO testtable (id, data) VALUES "
                       "(nextval('my_seq'), :data) RETURNING testtable.id",
                       {'data': 'd2'}),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 31, 'data': 'd3'}, {'id': 32, 'data': 'd4'}]),
            DialectSQL(
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                ":data)" % seqname, [{'data': 'd5'}, {'data': 'd6'}]),
            DialectSQL('INSERT INTO testtable (id, data) VALUES (:id, :data)',
                       [{'id': 33, 'data': 'd7'}]),
            DialectSQL(
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                ":data)" % seqname, [{'data': 'd8'}]),
        )

        with engine.connect() as conn:
            eq_(
                conn.execute(table.select()).fetchall(),
                [
                    (30, 'd1'),
                    (1, 'd2'),
                    (31, 'd3'),
                    (32, 'd4'),
                    (2, 'd5'),
                    (3, 'd6'),
                    (33, 'd7'),
                    (4, 'd8'),
                ]
            )

            # cant test reflection here since the Sequence must be
            # explicitly specified

    def _assert_data_noautoincrement(self, table):
        engine = \
            engines.testing_engine(options={'implicit_returning': False})

        with engine.connect() as conn:
            conn.execute(table.insert(), {'id': 30, 'data': 'd1'})

            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'})
            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'},
                    {'data': 'd3'})
            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'})
            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'},
                    {'data': 'd3'})

            conn.execute(
                table.insert(),
                {'id': 31, 'data': 'd2'}, {'id': 32, 'data': 'd3'})
            conn.execute(table.insert(inline=True), {'id': 33, 'data': 'd4'})
            eq_(conn.execute(table.select()).fetchall(), [
                (30, 'd1'),
                (31, 'd2'),
                (32, 'd3'),
                (33, 'd4')])
            conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData(engine)
        table = Table(table.name, m2, autoload=True)
        with engine.connect() as conn:
            conn.execute(table.insert(), {'id': 30, 'data': 'd1'})

            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'})
            with expect_warnings(
                    ".*has no Python-side or server-side default.*",
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute, table.insert(), {'data': 'd2'},
                    {'data': 'd3'})
            conn.execute(
                table.insert(),
                {'id': 31, 'data': 'd2'}, {'id': 32, 'data': 'd3'})
            conn.execute(table.insert(inline=True), {'id': 33, 'data': 'd4'})
            eq_(conn.execute(table.select()).fetchall(), [
                (30, 'd1'),
                (31, 'd2'),
                (32, 'd3'),
                (33, 'd4')])


class ServerSideCursorsTest(fixtures.TestBase, AssertsExecutionResults):

    __requires__ = 'psycopg2_compatibility',

    def _fixture(self, server_side_cursors):
        self.engine = engines.testing_engine(
            options={'server_side_cursors': server_side_cursors}
        )
        return self.engine

    def tearDown(self):
        engines.testing_reaper.close_all()
        self.engine.dispose()

    def test_global_string(self):
        engine = self._fixture(True)
        result = engine.execute('select 1')
        assert result.cursor.name

    def test_global_text(self):
        engine = self._fixture(True)
        result = engine.execute(text('select 1'))
        assert result.cursor.name

    def test_global_expr(self):
        engine = self._fixture(True)
        result = engine.execute(select([1]))
        assert result.cursor.name

    def test_global_off_explicit(self):
        engine = self._fixture(False)
        result = engine.execute(text('select 1'))

        # It should be off globally ...

        assert not result.cursor.name

    def test_stmt_option(self):
        engine = self._fixture(False)

        s = select([1]).execution_options(stream_results=True)
        result = engine.execute(s)

        # ... but enabled for this one.

        assert result.cursor.name

    def test_conn_option(self):
        engine = self._fixture(False)

        # and this one
        result = \
            engine.connect().execution_options(stream_results=True).\
            execute('select 1'
                    )
        assert result.cursor.name

    def test_stmt_enabled_conn_option_disabled(self):
        engine = self._fixture(False)

        s = select([1]).execution_options(stream_results=True)

        # not this one
        result = \
            engine.connect().execution_options(stream_results=False).\
            execute(s)
        assert not result.cursor.name

    def test_stmt_option_disabled(self):
        engine = self._fixture(True)
        s = select([1]).execution_options(stream_results=False)
        result = engine.execute(s)
        assert not result.cursor.name

    def test_aliases_and_ss(self):
        engine = self._fixture(False)
        s1 = select([1]).execution_options(stream_results=True).alias()
        result = engine.execute(s1)
        assert result.cursor.name

        # s1's options shouldn't affect s2 when s2 is used as a
        # from_obj.
        s2 = select([1], from_obj=s1)
        result = engine.execute(s2)
        assert not result.cursor.name

    def test_for_update_expr(self):
        engine = self._fixture(True)
        s1 = select([1], for_update=True)
        result = engine.execute(s1)
        assert result.cursor.name

    def test_for_update_string(self):
        engine = self._fixture(True)
        result = engine.execute('SELECT 1 FOR UPDATE')
        assert result.cursor.name

    def test_text_no_ss(self):
        engine = self._fixture(False)
        s = text('select 42')
        result = engine.execute(s)
        assert not result.cursor.name

    def test_text_ss_option(self):
        engine = self._fixture(False)
        s = text('select 42').execution_options(stream_results=True)
        result = engine.execute(s)
        assert result.cursor.name

    @testing.provide_metadata
    def test_roundtrip(self):
        md = self.metadata

        engine = self._fixture(True)
        test_table = Table('test_table', md,
                           Column('id', Integer, primary_key=True),
                           Column('data', String(50)))
        test_table.create(checkfirst=True)
        test_table.insert().execute(data='data1')
        nextid = engine.execute(Sequence('test_table_id_seq'))
        test_table.insert().execute(id=nextid, data='data2')
        eq_(test_table.select().execute().fetchall(), [(1, 'data1'
                                                        ), (2, 'data2')])
        test_table.update().where(
            test_table.c.id == 2).values(
            data=test_table.c.data +
            ' updated').execute()
        eq_(test_table.select().execute().fetchall(),
            [(1, 'data1'), (2, 'data2 updated')])
        test_table.delete().execute()
        eq_(select([func.count('*')]).select_from(test_table).scalar(), 0)


class MatchTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = 'postgresql >= 8.3'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)
        cattable = Table(
            'cattable', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'description', String(50)))
        matchtable = Table(
            'matchtable', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'title', String(200)),
            Column(
                'category_id', Integer, ForeignKey('cattable.id')))
        metadata.create_all()
        cattable.insert().execute([{'id': 1, 'description': 'Python'},
                                   {'id': 2, 'description': 'Ruby'}])
        matchtable.insert().execute(
            [{'id': 1, 'title': 'Agile Web Development with Rails',
              'category_id': 2},
             {'id': 2, 'title': 'Dive Into Python', 'category_id': 1},
             {'id': 3, 'title': "Programming Matz's Ruby", 'category_id': 2},
             {'id': 4, 'title': 'The Definitive Guide to Django',
              'category_id': 1},
             {'id': 5, 'title': 'Python in a Nutshell', 'category_id': 1}])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on('postgresql+pg8000', 'uses positional')
    @testing.fails_on('postgresql+zxjdbc', 'uses qmark')
    def test_expression_pyformat(self):
        self.assert_compile(matchtable.c.title.match('somstr'),
                            'matchtable.title @@ to_tsquery(%(title_1)s'
                            ')')

    @testing.fails_on('postgresql+psycopg2', 'uses pyformat')
    @testing.fails_on('postgresql+pypostgresql', 'uses pyformat')
    @testing.fails_on('postgresql+pygresql', 'uses pyformat')
    @testing.fails_on('postgresql+zxjdbc', 'uses qmark')
    @testing.fails_on('postgresql+psycopg2cffi', 'uses pyformat')
    def test_expression_positional(self):
        self.assert_compile(matchtable.c.title.match('somstr'),
                            'matchtable.title @@ to_tsquery(%s)')

    def test_simple_match(self):
        results = matchtable.select().where(
            matchtable.c.title.match('python')).order_by(
            matchtable.c.id).execute().fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_not_match(self):
        results = matchtable.select().where(
            ~matchtable.c.title.match('python')).order_by(
            matchtable.c.id).execute().fetchall()
        eq_([1, 3, 4], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = matchtable.select().where(
            matchtable.c.title.match("Matz's")).execute().fetchall()
        eq_([3], [r.id for r in results])

    def test_simple_derivative_match(self):
        results = matchtable.select().where(
            matchtable.c.title.match('nutshells')).execute().fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self):
        results1 = matchtable.select().where(
            or_(
                matchtable.c.title.match('nutshells'),
                matchtable.c.title.match('rubies'))).order_by(
            matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results1])
        results2 = matchtable.select().where(
            matchtable.c.title.match('nutshells | rubies')).order_by(
            matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self):
        results1 = matchtable.select().where(
            and_(
                matchtable.c.title.match('python'),
                matchtable.c.title.match('nutshells'))).execute().fetchall()
        eq_([5], [r.id for r in results1])
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('python & nutshells'
                                         )).execute().fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(
            and_(
                cattable.c.id == matchtable.c.category_id, or_(
                    cattable.c.description.match('Ruby'),
                    matchtable.c.title.match('nutshells')))).order_by(
            matchtable.c.id).execute().fetchall()
        eq_([1, 3, 5], [r.id for r in results])


class TupleTest(fixtures.TestBase):
    __only_on__ = 'postgresql'
    __backend__ = True

    def test_tuple_containment(self):

        for test, exp in [
            ([('a', 'b')], True),
            ([('a', 'c')], False),
            ([('f', 'q'), ('a', 'b')], True),
            ([('f', 'q'), ('a', 'c')], False)
        ]:
            eq_(
                testing.db.execute(
                    select([
                        tuple_(
                            literal_column("'a'"),
                            literal_column("'b'")
                        ).
                        in_([
                            tuple_(*[
                                literal_column("'%s'" % letter)
                                for letter in elem
                            ]) for elem in test
                        ])
                    ])
                ).scalar(),
                exp
            )


class ExtractTest(fixtures.TablesTest):

    """The rationale behind this test is that for many years we've had a system
    of embedding type casts into the expressions rendered by visit_extract()
    on the postgreql platform.  The reason for this cast is not clear.
    So here we try to produce a wide range of cases to ensure that these casts
    are not needed; see [ticket:2740].

    """
    __only_on__ = 'postgresql'
    __backend__ = True

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_bind(cls):
        from sqlalchemy import event
        eng = engines.testing_engine()

        @event.listens_for(eng, "connect")
        def connect(dbapi_conn, rec):
            cursor = dbapi_conn.cursor()
            cursor.execute("SET SESSION TIME ZONE 0")
            cursor.close()

        return eng

    @classmethod
    def define_tables(cls, metadata):
        Table('t', metadata,
              Column('id', Integer, primary_key=True),
              Column('dtme', DateTime),
              Column('dt', Date),
              Column('tm', Time),
              Column('intv', postgresql.INTERVAL),
              Column('dttz', DateTime(timezone=True))
              )

    @classmethod
    def insert_data(cls):
        # TODO: why does setting hours to anything
        # not affect the TZ in the DB col ?
        class TZ(datetime.tzinfo):

            def utcoffset(self, dt):
                return datetime.timedelta(hours=4)

        cls.bind.execute(
            cls.tables.t.insert(),
            {
                'dtme': datetime.datetime(2012, 5, 10, 12, 15, 25),
                'dt': datetime.date(2012, 5, 10),
                'tm': datetime.time(12, 15, 25),
                'intv': datetime.timedelta(seconds=570),
                'dttz': datetime.datetime(2012, 5, 10, 12, 15, 25,
                                          tzinfo=TZ())
            },
        )

    def _test(self, expr, field="all", overrides=None):
        t = self.tables.t

        if field == "all":
            fields = {"year": 2012, "month": 5, "day": 10,
                      "epoch": 1336652125.0,
                      "hour": 12, "minute": 15}
        elif field == "time":
            fields = {"hour": 12, "minute": 15, "second": 25}
        elif field == 'date':
            fields = {"year": 2012, "month": 5, "day": 10}
        elif field == 'all+tz':
            fields = {"year": 2012, "month": 5, "day": 10,
                      "epoch": 1336637725.0,
                      "hour": 8,
                      "timezone": 0
                      }
        else:
            fields = field

        if overrides:
            fields.update(overrides)

        for field in fields:
            result = self.bind.scalar(
                select([extract(field, expr)]).select_from(t))
            eq_(result, fields[field])

    def test_one(self):
        t = self.tables.t
        self._test(t.c.dtme, "all")

    def test_two(self):
        t = self.tables.t
        self._test(t.c.dtme + t.c.intv,
                   overrides={"epoch": 1336652695.0, "minute": 24})

    def test_three(self):
        self.tables.t

        actual_ts = self.bind.scalar(func.current_timestamp()) - \
            datetime.timedelta(days=5)
        self._test(func.current_timestamp() - datetime.timedelta(days=5),
                   {"hour": actual_ts.hour, "year": actual_ts.year,
                    "month": actual_ts.month}
                   )

    def test_four(self):
        t = self.tables.t
        self._test(datetime.timedelta(days=5) + t.c.dt,
                   overrides={"day": 15, "epoch": 1337040000.0, "hour": 0,
                              "minute": 0}
                   )

    def test_five(self):
        t = self.tables.t
        self._test(func.coalesce(t.c.dtme, func.current_timestamp()),
                   overrides={"epoch": 1336652125.0})

    def test_six(self):
        t = self.tables.t
        self._test(t.c.tm + datetime.timedelta(seconds=30), "time",
                   overrides={"second": 55})

    def test_seven(self):
        self._test(literal(datetime.timedelta(seconds=10))
                   - literal(datetime.timedelta(seconds=10)), "all",
                   overrides={"hour": 0, "minute": 0, "month": 0,
                              "year": 0, "day": 0, "epoch": 0})

    def test_eight(self):
        t = self.tables.t
        self._test(t.c.tm + datetime.timedelta(seconds=30),
                   {"hour": 12, "minute": 15, "second": 55})

    def test_nine(self):
        self._test(text("t.dt + t.tm"))

    def test_ten(self):
        t = self.tables.t
        self._test(t.c.dt + t.c.tm)

    def test_eleven(self):
        self._test(func.current_timestamp() - func.current_timestamp(),
                   {"year": 0, "month": 0, "day": 0, "hour": 0}
                   )

    def test_twelve(self):
        t = self.tables.t
        actual_ts = self.bind.scalar(
            func.current_timestamp()).replace(tzinfo=None) - \
            datetime.datetime(2012, 5, 10, 12, 15, 25)

        self._test(
            func.current_timestamp() - func.coalesce(
                t.c.dtme,
                func.current_timestamp()
            ),
            {"day": actual_ts.days})

    def test_thirteen(self):
        t = self.tables.t
        self._test(t.c.dttz, "all+tz")

    def test_fourteen(self):
        t = self.tables.t
        self._test(t.c.tm, "time")

    def test_fifteen(self):
        t = self.tables.t
        self._test(datetime.timedelta(days=5) + t.c.dtme,
                   overrides={"day": 15, "epoch": 1337084125.0}
                   )
