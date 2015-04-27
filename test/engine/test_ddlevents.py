
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.schema import DDL, CheckConstraint, AddConstraint, \
    DropConstraint
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Integer, String, event, exc, text
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
import sqlalchemy as tsa
from sqlalchemy import testing
from sqlalchemy.testing import engines
from sqlalchemy.testing import AssertsCompiledSQL, eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock


class DDLEventTest(fixtures.TestBase):

    def setup(self):
        self.bind = engines.mock_engine()
        self.metadata = MetaData()
        self.table = Table('t', self.metadata, Column('id', Integer))

    def test_table_create_before(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()
        event.listen(table, 'before_create', canary.before_create)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY)
            ]
        )

    def test_table_create_after(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()
        event.listen(table, 'after_create', canary.after_create)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.after_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY)
            ]
        )

    def test_table_create_both(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()
        event.listen(table, 'before_create', canary.before_create)
        event.listen(table, 'after_create', canary.after_create)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
                mock.call.after_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY)
            ]
        )

    def test_table_drop_before(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()
        event.listen(table, 'before_drop', canary.before_drop)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
            ]
        )

    def test_table_drop_after(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()
        event.listen(table, 'after_drop', canary.after_drop)

        table.create(bind)
        canary.state = 'skipped'
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.after_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
            ]
        )

    def test_table_drop_both(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()

        event.listen(table, 'before_drop', canary.before_drop)
        event.listen(table, 'after_drop', canary.after_drop)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
                mock.call.after_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
            ]
        )

    def test_table_all(self):
        table, bind = self.table, self.bind
        canary = mock.Mock()

        event.listen(table, 'before_create', canary.before_create)
        event.listen(table, 'after_create', canary.after_create)
        event.listen(table, 'before_drop', canary.before_drop)
        event.listen(table, 'after_drop', canary.after_drop)

        table.create(bind)
        table.drop(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
                mock.call.after_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
                mock.call.before_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
                mock.call.after_drop(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
            ]
        )

    def test_metadata_create_before(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()
        event.listen(metadata, 'before_create', canary.before_create)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_create(
                    # checkfirst is False because of the MockConnection
                    # used in the current testing strategy.
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_create_after(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()
        event.listen(metadata, 'after_create', canary.after_create)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.after_create(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_create_both(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()

        event.listen(metadata, 'before_create', canary.before_create)
        event.listen(metadata, 'after_create', canary.after_create)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_create(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
                mock.call.after_create(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_drop_before(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()
        event.listen(metadata, 'before_drop', canary.before_drop)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_drop(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_drop_after(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()
        event.listen(metadata, 'after_drop', canary.after_drop)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.after_drop(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_drop_both(self):
        metadata, bind = self.metadata, self.bind
        canary = mock.Mock()

        event.listen(metadata, 'before_drop', canary.before_drop)
        event.listen(metadata, 'after_drop', canary.after_drop)

        metadata.create_all(bind)
        metadata.drop_all(bind)
        eq_(
            canary.mock_calls,
            [
                mock.call.before_drop(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
                mock.call.after_drop(
                    metadata, self.bind, checkfirst=False,
                    tables=list(metadata.tables.values()),
                    _ddl_runner=mock.ANY),
            ]
        )

    def test_metadata_table_isolation(self):
        metadata, table = self.metadata, self.table
        table_canary = mock.Mock()
        metadata_canary = mock.Mock()

        event.listen(table, 'before_create', table_canary.before_create)

        event.listen(metadata, 'before_create', metadata_canary.before_create)
        self.table.create(self.bind)
        eq_(
            table_canary.mock_calls,
            [
                mock.call.before_create(
                    table, self.bind, checkfirst=False,
                    _ddl_runner=mock.ANY, _is_metadata_operation=mock.ANY),
            ]
        )
        eq_(
            metadata_canary.mock_calls,
            []
        )

    def test_append_listener(self):
        metadata, table, bind = self.metadata, self.table, self.bind

        fn = lambda *a: None

        table.append_ddl_listener('before-create', fn)
        assert_raises(exc.InvalidRequestError, table.append_ddl_listener,
                                        'blah', fn)

        metadata.append_ddl_listener('before-create', fn)
        assert_raises(exc.InvalidRequestError, metadata.append_ddl_listener,
                                        'blah', fn)


class DDLExecutionTest(fixtures.TestBase):
    def setup(self):
        self.engine = engines.mock_engine()
        self.metadata = MetaData(self.engine)
        self.users = Table('users', self.metadata,
                           Column('user_id', Integer, primary_key=True),
                           Column('user_name', String(40)),
                           )

    def test_table_standalone(self):
        users, engine = self.users, self.engine
        event.listen(users, 'before_create', DDL('mxyzptlk'))
        event.listen(users, 'after_create', DDL('klptzyxm'))
        event.listen(users, 'before_drop', DDL('xyzzy'))
        event.listen(users, 'after_drop', DDL('fnord'))

        users.create()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        users.drop()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_table_by_metadata(self):
        metadata, users, engine = self.metadata, self.users, self.engine

        event.listen(users, 'before_create', DDL('mxyzptlk'))
        event.listen(users, 'after_create', DDL('klptzyxm'))
        event.listen(users, 'before_drop', DDL('xyzzy'))
        event.listen(users, 'after_drop', DDL('fnord'))

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    @testing.uses_deprecated(r'See DDLEvents')
    def test_table_by_metadata_deprecated(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        DDL('mxyzptlk').execute_at('before-create', users)
        DDL('klptzyxm').execute_at('after-create', users)
        DDL('xyzzy').execute_at('before-drop', users)
        DDL('fnord').execute_at('after-drop', users)

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_deprecated_append_ddl_listener_table(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        canary = []
        users.append_ddl_listener('before-create',
                            lambda e, t, b: canary.append('mxyzptlk')
                        )
        users.append_ddl_listener('after-create',
                            lambda e, t, b: canary.append('klptzyxm')
                        )
        users.append_ddl_listener('before-drop',
                            lambda e, t, b: canary.append('xyzzy')
                        )
        users.append_ddl_listener('after-drop',
                            lambda e, t, b: canary.append('fnord')
                        )

        metadata.create_all()
        assert 'mxyzptlk' in canary
        assert 'klptzyxm' in canary
        assert 'xyzzy' not in canary
        assert 'fnord' not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert 'mxyzptlk' not in canary
        assert 'klptzyxm' not in canary
        assert 'xyzzy' in canary
        assert 'fnord' in canary

    def test_deprecated_append_ddl_listener_metadata(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        canary = []
        metadata.append_ddl_listener('before-create',
                            lambda e, t, b, tables=None: canary.append('mxyzptlk')
                        )
        metadata.append_ddl_listener('after-create',
                            lambda e, t, b, tables=None: canary.append('klptzyxm')
                        )
        metadata.append_ddl_listener('before-drop',
                            lambda e, t, b, tables=None: canary.append('xyzzy')
                        )
        metadata.append_ddl_listener('after-drop',
                            lambda e, t, b, tables=None: canary.append('fnord')
                        )

        metadata.create_all()
        assert 'mxyzptlk' in canary
        assert 'klptzyxm' in canary
        assert 'xyzzy' not in canary
        assert 'fnord' not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert 'mxyzptlk' not in canary
        assert 'klptzyxm' not in canary
        assert 'xyzzy' in canary
        assert 'fnord' in canary

    def test_metadata(self):
        metadata, engine = self.metadata, self.engine

        event.listen(metadata, 'before_create', DDL('mxyzptlk'))
        event.listen(metadata, 'after_create', DDL('klptzyxm'))
        event.listen(metadata, 'before_drop', DDL('xyzzy'))
        event.listen(metadata, 'after_drop', DDL('fnord'))

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    @testing.uses_deprecated(r'See DDLEvents')
    def test_metadata_deprecated(self):
        metadata, engine = self.metadata, self.engine

        DDL('mxyzptlk').execute_at('before-create', metadata)
        DDL('klptzyxm').execute_at('after-create', metadata)
        DDL('xyzzy').execute_at('before-drop', metadata)
        DDL('fnord').execute_at('after-drop', metadata)

        metadata.create_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' in strings
        assert 'klptzyxm' in strings
        assert 'xyzzy' not in strings
        assert 'fnord' not in strings
        del engine.mock[:]
        metadata.drop_all()
        strings = [str(x) for x in engine.mock]
        assert 'mxyzptlk' not in strings
        assert 'klptzyxm' not in strings
        assert 'xyzzy' in strings
        assert 'fnord' in strings

    def test_conditional_constraint(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        nonpg_mock = engines.mock_engine(dialect_name='sqlite')
        pg_mock = engines.mock_engine(dialect_name='postgresql')
        constraint = CheckConstraint('a < b', name='my_test_constraint',
                                    table=users)

        # by placing the constraint in an Add/Drop construct, the
        # 'inline_ddl' flag is set to False

        event.listen(
            users,
            'after_create',
            AddConstraint(constraint).execute_if(dialect='postgresql'),
        )

        event.listen(
            users,
            'before_drop',
            DropConstraint(constraint).execute_if(dialect='postgresql'),
        )

        metadata.create_all(bind=nonpg_mock)
        strings = ' '.join(str(x) for x in nonpg_mock.mock)
        assert 'my_test_constraint' not in strings
        metadata.drop_all(bind=nonpg_mock)
        strings = ' '.join(str(x) for x in nonpg_mock.mock)
        assert 'my_test_constraint' not in strings
        metadata.create_all(bind=pg_mock)
        strings = ' '.join(str(x) for x in pg_mock.mock)
        assert 'my_test_constraint' in strings
        metadata.drop_all(bind=pg_mock)
        strings = ' '.join(str(x) for x in pg_mock.mock)
        assert 'my_test_constraint' in strings

    @testing.uses_deprecated(r'See DDLEvents')
    def test_conditional_constraint_deprecated(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        nonpg_mock = engines.mock_engine(dialect_name='sqlite')
        pg_mock = engines.mock_engine(dialect_name='postgresql')
        constraint = CheckConstraint('a < b', name='my_test_constraint',
                                    table=users)

        # by placing the constraint in an Add/Drop construct, the
        # 'inline_ddl' flag is set to False

        AddConstraint(constraint, on='postgresql'
                      ).execute_at('after-create', users)
        DropConstraint(constraint, on='postgresql'
                       ).execute_at('before-drop', users)
        metadata.create_all(bind=nonpg_mock)
        strings = ' '.join(str(x) for x in nonpg_mock.mock)
        assert 'my_test_constraint' not in strings
        metadata.drop_all(bind=nonpg_mock)
        strings = ' '.join(str(x) for x in nonpg_mock.mock)
        assert 'my_test_constraint' not in strings
        metadata.create_all(bind=pg_mock)
        strings = ' '.join(str(x) for x in pg_mock.mock)
        assert 'my_test_constraint' in strings
        metadata.drop_all(bind=pg_mock)
        strings = ' '.join(str(x) for x in pg_mock.mock)
        assert 'my_test_constraint' in strings

    @testing.requires.sqlite
    def test_ddl_execute(self):
        engine = create_engine('sqlite:///')
        cx = engine.connect()
        table = self.users
        ddl = DDL('SELECT 1')

        for py in ('engine.execute(ddl)',
                   'engine.execute(ddl, table)',
                   'cx.execute(ddl)',
                   'cx.execute(ddl, table)',
                   'ddl.execute(engine)',
                   'ddl.execute(engine, table)',
                   'ddl.execute(cx)',
                   'ddl.execute(cx, table)'):
            r = eval(py)
            assert list(r) == [(1,)], py

        for py in ('ddl.execute()',
                   'ddl.execute(target=table)'):
            try:
                r = eval(py)
                assert False
            except tsa.exc.UnboundExecutionError:
                pass

        for bind in engine, cx:
            ddl.bind = bind
            for py in ('ddl.execute()',
                       'ddl.execute(target=table)'):
                r = eval(py)
                assert list(r) == [(1,)], py

    def test_platform_escape(self):
        """test the escaping of % characters in the DDL construct."""

        default_from = testing.db.dialect.statement_compiler(
            testing.db.dialect, None).default_from()

        # We're abusing the DDL()
        # construct here by pushing a SELECT through it
        # so that we can verify the round trip.
        # the DDL() will trigger autocommit, which prohibits
        # some DBAPIs from returning results (pyodbc), so we
        # run in an explicit transaction.
        with testing.db.begin() as conn:
            eq_(
                conn.execute(
                    text("select 'foo%something'" + default_from)
                ).scalar(),
                'foo%something'
            )

            eq_(
                conn.execute(
                    DDL("select 'foo%%something'" + default_from)
                ).scalar(),
                'foo%something'
            )


class DDLTest(fixtures.TestBase, AssertsCompiledSQL):
    def mock_engine(self):
        executor = lambda *a, **kw: None
        engine = create_engine(testing.db.name + '://',
                               strategy='mock', executor=executor)
        engine.dialect.identifier_preparer = \
           tsa.sql.compiler.IdentifierPreparer(engine.dialect)
        return engine

    def test_tokens(self):
        m = MetaData()
        sane_alone = Table('t', m, Column('id', Integer))
        sane_schema = Table('t', m, Column('id', Integer), schema='s')
        insane_alone = Table('t t', m, Column('id', Integer))
        insane_schema = Table('t t', m, Column('id', Integer),
                              schema='s s')
        ddl = DDL('%(schema)s-%(table)s-%(fullname)s')
        dialect = self.mock_engine().dialect
        self.assert_compile(ddl.against(sane_alone), '-t-t',
                            dialect=dialect)
        self.assert_compile(ddl.against(sane_schema), 's-t-s.t',
                            dialect=dialect)
        self.assert_compile(ddl.against(insane_alone), '-"t t"-"t t"',
                            dialect=dialect)
        self.assert_compile(ddl.against(insane_schema),
                            '"s s"-"t t"-"s s"."t t"', dialect=dialect)

        # overrides are used piece-meal and verbatim.

        ddl = DDL('%(schema)s-%(table)s-%(fullname)s-%(bonus)s',
                  context={'schema': 'S S', 'table': 'T T', 'bonus': 'b'
                  })
        self.assert_compile(ddl.against(sane_alone), 'S S-T T-t-b',
                            dialect=dialect)
        self.assert_compile(ddl.against(sane_schema), 'S S-T T-s.t-b',
                            dialect=dialect)
        self.assert_compile(ddl.against(insane_alone), 'S S-T T-"t t"-b',
                            dialect=dialect)
        self.assert_compile(ddl.against(insane_schema),
                            'S S-T T-"s s"."t t"-b', dialect=dialect)

    def test_filter(self):
        cx = self.mock_engine()

        tbl = Table('t', MetaData(), Column('id', Integer))
        target = cx.name

        assert DDL('')._should_execute(tbl, cx)
        assert DDL('').execute_if(dialect=target)._should_execute(tbl, cx)
        assert not DDL('').execute_if(dialect='bogus').\
                        _should_execute(tbl, cx)
        assert DDL('').execute_if(callable_=lambda d, y, z, **kw: True).\
                        _should_execute(tbl, cx)
        assert(DDL('').execute_if(
                        callable_=lambda d, y, z, **kw: z.engine.name
                        != 'bogus').
               _should_execute(tbl, cx))

    @testing.uses_deprecated(r'See DDLEvents')
    def test_filter_deprecated(self):
        cx = self.mock_engine()

        tbl = Table('t', MetaData(), Column('id', Integer))
        target = cx.name

        assert DDL('')._should_execute_deprecated('x', tbl, cx)
        assert DDL('', on=target)._should_execute_deprecated('x', tbl, cx)
        assert not DDL('', on='bogus').\
                        _should_execute_deprecated('x', tbl, cx)
        assert DDL('', on=lambda d, x, y, z: True).\
                        _should_execute_deprecated('x', tbl, cx)
        assert(DDL('', on=lambda d, x, y, z: z.engine.name != 'bogus').
               _should_execute_deprecated('x', tbl, cx))

    def test_repr(self):
        assert repr(DDL('s'))
        assert repr(DDL('s', on='engine'))
        assert repr(DDL('s', on=lambda x: 1))
        assert repr(DDL('s', context={'a': 1}))
        assert repr(DDL('s', on='engine', context={'a': 1}))
