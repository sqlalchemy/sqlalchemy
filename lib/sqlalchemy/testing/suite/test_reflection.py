

import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import types as sql_types
from sqlalchemy import inspect
from sqlalchemy import MetaData, Integer, String, func
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.testing import engines, fixtures
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing import eq_, is_, assert_raises_message
from sqlalchemy import testing
from .. import config
import operator
from sqlalchemy.schema import DDL, Index
from sqlalchemy import event
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy import ForeignKey
import re

metadata, users = None, None


class HasTableTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('test_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(50))
              )

    def test_has_table(self):
        with config.db.begin() as conn:
            assert config.db.dialect.has_table(conn, "test_table")
            assert not config.db.dialect.has_table(conn, "nonexistent_table")


class ComponentReflectionTest(fixtures.TablesTest):
    run_inserts = run_deletes = None

    __backend__ = True

    @classmethod
    def setup_bind(cls):
        if config.requirements.independent_connections.enabled:
            from sqlalchemy import pool
            return engines.testing_engine(
                options=dict(poolclass=pool.StaticPool))
        else:
            return config.db

    @classmethod
    def define_tables(cls, metadata):
        cls.define_reflected_tables(metadata, None)
        if testing.requires.schemas.enabled:
            cls.define_reflected_tables(metadata, testing.config.test_schema)

    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        if schema:
            schema_prefix = schema + "."
        else:
            schema_prefix = ""

        if testing.requires.self_referential_foreign_keys.enabled:
            users = Table('users', metadata,
                          Column('user_id', sa.INT, primary_key=True),
                          Column('test1', sa.CHAR(5), nullable=False),
                          Column('test2', sa.Float(5), nullable=False),
                          Column('parent_user_id', sa.Integer,
                                 sa.ForeignKey('%susers.user_id' %
                                               schema_prefix,
                                               name='user_id_fk')),
                          schema=schema,
                          test_needs_fk=True,
                          )
        else:
            users = Table('users', metadata,
                          Column('user_id', sa.INT, primary_key=True),
                          Column('test1', sa.CHAR(5), nullable=False),
                          Column('test2', sa.Float(5), nullable=False),
                          schema=schema,
                          test_needs_fk=True,
                          )

        Table("dingalings", metadata,
              Column('dingaling_id', sa.Integer, primary_key=True),
              Column('address_id', sa.Integer,
                     sa.ForeignKey('%semail_addresses.address_id' %
                                   schema_prefix)),
              Column('data', sa.String(30)),
              schema=schema,
              test_needs_fk=True,
              )
        Table('email_addresses', metadata,
              Column('address_id', sa.Integer),
              Column('remote_user_id', sa.Integer,
                     sa.ForeignKey(users.c.user_id)),
              Column('email_address', sa.String(20)),
              sa.PrimaryKeyConstraint('address_id', name='email_ad_pk'),
              schema=schema,
              test_needs_fk=True,
              )
        Table('comment_test', metadata,
              Column('id', sa.Integer, primary_key=True, comment='id comment'),
              Column('data', sa.String(20), comment='data % comment'),
              Column(
                  'd2', sa.String(20),
                  comment=r"""Comment types type speedily ' " \ '' Fun!"""),
              schema=schema,
              comment=r"""the test % ' " \ table comment""")

        if testing.requires.cross_schema_fk_reflection.enabled:
            if schema is None:
                Table(
                    'local_table', metadata,
                    Column('id', sa.Integer, primary_key=True),
                    Column('data', sa.String(20)),
                    Column(
                        'remote_id',
                        ForeignKey(
                            '%s.remote_table_2.id' %
                            testing.config.test_schema)
                    ),
                    test_needs_fk=True,
                    schema=config.db.dialect.default_schema_name
                )
            else:
                Table(
                    'remote_table', metadata,
                    Column('id', sa.Integer, primary_key=True),
                    Column(
                        'local_id',
                        ForeignKey(
                            '%s.local_table.id' %
                            config.db.dialect.default_schema_name)
                    ),
                    Column('data', sa.String(20)),
                    schema=schema,
                    test_needs_fk=True,
                )
                Table(
                    'remote_table_2', metadata,
                    Column('id', sa.Integer, primary_key=True),
                    Column('data', sa.String(20)),
                    schema=schema,
                    test_needs_fk=True,
                )

        if testing.requires.index_reflection.enabled:
            cls.define_index(metadata, users)

            if not schema:
                noncol_idx_test_nopk = Table(
                    'noncol_idx_test_nopk', metadata,
                    Column('q', sa.String(5)),
                )
                noncol_idx_test_pk = Table(
                    'noncol_idx_test_pk', metadata,
                    Column('id', sa.Integer, primary_key=True),
                    Column('q', sa.String(5)),
                )
                Index('noncol_idx_nopk', noncol_idx_test_nopk.c.q.desc())
                Index('noncol_idx_pk', noncol_idx_test_pk.c.q.desc())

        if testing.requires.view_column_reflection.enabled:
            cls.define_views(metadata, schema)
        if not schema and testing.requires.temp_table_reflection.enabled:
            cls.define_temp_tables(metadata)

    @classmethod
    def define_temp_tables(cls, metadata):
        # cheat a bit, we should fix this with some dialect-level
        # temp table fixture
        if testing.against("oracle"):
            kw = {
                'prefixes': ["GLOBAL TEMPORARY"],
                'oracle_on_commit': 'PRESERVE ROWS'
            }
        else:
            kw = {
                'prefixes': ["TEMPORARY"],
            }

        user_tmp = Table(
            "user_tmp", metadata,
            Column("id", sa.INT, primary_key=True),
            Column('name', sa.VARCHAR(50)),
            Column('foo', sa.INT),
            sa.UniqueConstraint('name', name='user_tmp_uq'),
            sa.Index("user_tmp_ix", "foo"),
            **kw
        )
        if testing.requires.view_reflection.enabled and \
                testing.requires.temporary_views.enabled:
            event.listen(
                user_tmp, "after_create",
                DDL("create temporary view user_tmp_v as "
                    "select * from user_tmp")
            )
            event.listen(
                user_tmp, "before_drop",
                DDL("drop view user_tmp_v")
            )

    @classmethod
    def define_index(cls, metadata, users):
        Index("users_t_idx", users.c.test1, users.c.test2)
        Index("users_all_idx", users.c.user_id, users.c.test2, users.c.test1)

    @classmethod
    def define_views(cls, metadata, schema):
        for table_name in ('users', 'email_addresses'):
            fullname = table_name
            if schema:
                fullname = "%s.%s" % (schema, table_name)
            view_name = fullname + '_v'
            query = "CREATE VIEW %s AS SELECT * FROM %s" % (
                view_name, fullname)

            event.listen(
                metadata,
                "after_create",
                DDL(query)
            )
            event.listen(
                metadata,
                "before_drop",
                DDL("DROP VIEW %s" % view_name)
            )

    @testing.requires.schema_reflection
    def test_get_schema_names(self):
        insp = inspect(testing.db)

        self.assert_(testing.config.test_schema in insp.get_schema_names())

    @testing.requires.schema_reflection
    def test_dialect_initialize(self):
        engine = engines.testing_engine()
        assert not hasattr(engine.dialect, 'default_schema_name')
        inspect(engine)
        assert hasattr(engine.dialect, 'default_schema_name')

    @testing.requires.schema_reflection
    def test_get_default_schema_name(self):
        insp = inspect(testing.db)
        eq_(insp.default_schema_name, testing.db.dialect.default_schema_name)

    @testing.provide_metadata
    def _test_get_table_names(self, schema=None, table_type='table',
                              order_by=None):
        _ignore_tables = [
            'comment_test', 'noncol_idx_test_pk', 'noncol_idx_test_nopk',
            'local_table', 'remote_table', 'remote_table_2'
        ]
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        insp = inspect(meta.bind)

        if table_type == 'view':
            table_names = insp.get_view_names(schema)
            table_names.sort()
            answer = ['email_addresses_v', 'users_v']
            eq_(sorted(table_names), answer)
        else:
            table_names = [
                t for t in insp.get_table_names(
                    schema,
                    order_by=order_by) if t not in _ignore_tables]

            if order_by == 'foreign_key':
                answer = ['users', 'email_addresses', 'dingalings']
                eq_(table_names, answer)
            else:
                answer = ['dingalings', 'email_addresses', 'users']
                eq_(sorted(table_names), answer)

    @testing.requires.temp_table_names
    def test_get_temp_table_names(self):
        insp = inspect(self.bind)
        temp_table_names = insp.get_temp_table_names()
        eq_(sorted(temp_table_names), ['user_tmp'])

    @testing.requires.view_reflection
    @testing.requires.temp_table_names
    @testing.requires.temporary_views
    def test_get_temp_view_names(self):
        insp = inspect(self.bind)
        temp_table_names = insp.get_temp_view_names()
        eq_(sorted(temp_table_names), ['user_tmp_v'])

    @testing.requires.table_reflection
    def test_get_table_names(self):
        self._test_get_table_names()

    @testing.requires.table_reflection
    @testing.requires.foreign_key_constraint_reflection
    def test_get_table_names_fks(self):
        self._test_get_table_names(order_by='foreign_key')

    @testing.requires.comment_reflection
    def test_get_comments(self):
        self._test_get_comments()

    @testing.requires.comment_reflection
    @testing.requires.schemas
    def test_get_comments_with_schema(self):
        self._test_get_comments(testing.config.test_schema)

    def _test_get_comments(self, schema=None):
        insp = inspect(testing.db)

        eq_(
            insp.get_table_comment("comment_test", schema=schema),
            {"text": r"""the test % ' " \ table comment"""}
        )

        eq_(
            insp.get_table_comment("users", schema=schema),
            {"text": None}
        )

        eq_(
            [
                {"name": rec['name'], "comment": rec['comment']}
                for rec in
                insp.get_columns("comment_test", schema=schema)
            ],
            [
                {'comment': 'id comment', 'name': 'id'},
                {'comment': 'data % comment', 'name': 'data'},
                {'comment': r"""Comment types type speedily ' " \ '' Fun!""",
                 'name': 'd2'}
            ]
        )

    @testing.requires.table_reflection
    @testing.requires.schemas
    def test_get_table_names_with_schema(self):
        self._test_get_table_names(testing.config.test_schema)

    @testing.requires.view_column_reflection
    def test_get_view_names(self):
        self._test_get_table_names(table_type='view')

    @testing.requires.view_column_reflection
    @testing.requires.schemas
    def test_get_view_names_with_schema(self):
        self._test_get_table_names(
            testing.config.test_schema, table_type='view')

    @testing.requires.table_reflection
    @testing.requires.view_column_reflection
    def test_get_tables_and_views(self):
        self._test_get_table_names()
        self._test_get_table_names(table_type='view')

    def _test_get_columns(self, schema=None, table_type='table'):
        meta = MetaData(testing.db)
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        table_names = ['users', 'email_addresses']
        if table_type == 'view':
            table_names = ['users_v', 'email_addresses_v']
        insp = inspect(meta.bind)
        for table_name, table in zip(table_names, (users,
                                                   addresses)):
            schema_name = schema
            cols = insp.get_columns(table_name, schema=schema_name)
            self.assert_(len(cols) > 0, len(cols))

            # should be in order

            for i, col in enumerate(table.columns):
                eq_(col.name, cols[i]['name'])
                ctype = cols[i]['type'].__class__
                ctype_def = col.type
                if isinstance(ctype_def, sa.types.TypeEngine):
                    ctype_def = ctype_def.__class__

                # Oracle returns Date for DateTime.

                if testing.against('oracle') and ctype_def \
                        in (sql_types.Date, sql_types.DateTime):
                    ctype_def = sql_types.Date

                # assert that the desired type and return type share
                # a base within one of the generic types.

                self.assert_(len(set(ctype.__mro__).
                                 intersection(ctype_def.__mro__).
                                 intersection([
                                     sql_types.Integer,
                                     sql_types.Numeric,
                                     sql_types.DateTime,
                                     sql_types.Date,
                                     sql_types.Time,
                                     sql_types.String,
                                     sql_types._Binary,
                                 ])) > 0, '%s(%s), %s(%s)' %
                             (col.name, col.type, cols[i]['name'], ctype))

                if not col.primary_key:
                    assert cols[i]['default'] is None

    @testing.requires.table_reflection
    def test_get_columns(self):
        self._test_get_columns()

    @testing.provide_metadata
    def _type_round_trip(self, *types):
        t = Table('t', self.metadata,
                  *[
                      Column('t%d' % i, type_)
                      for i, type_ in enumerate(types)
                  ]
                  )
        t.create()

        return [
            c['type'] for c in
            inspect(self.metadata.bind).get_columns('t')
        ]

    @testing.requires.table_reflection
    def test_numeric_reflection(self):
        for typ in self._type_round_trip(
            sql_types.Numeric(18, 5),
        ):
            assert isinstance(typ, sql_types.Numeric)
            eq_(typ.precision, 18)
            eq_(typ.scale, 5)

    @testing.requires.table_reflection
    def test_varchar_reflection(self):
        typ = self._type_round_trip(sql_types.String(52))[0]
        assert isinstance(typ, sql_types.String)
        eq_(typ.length, 52)

    @testing.requires.table_reflection
    @testing.provide_metadata
    def test_nullable_reflection(self):
        t = Table('t', self.metadata,
                  Column('a', Integer, nullable=True),
                  Column('b', Integer, nullable=False))
        t.create()
        eq_(
            dict(
                (col['name'], col['nullable'])
                for col in inspect(self.metadata.bind).get_columns('t')
            ),
            {"a": True, "b": False}
        )

    @testing.requires.table_reflection
    @testing.requires.schemas
    def test_get_columns_with_schema(self):
        self._test_get_columns(schema=testing.config.test_schema)

    @testing.requires.temp_table_reflection
    def test_get_temp_table_columns(self):
        meta = MetaData(self.bind)
        user_tmp = self.tables.user_tmp
        insp = inspect(meta.bind)
        cols = insp.get_columns('user_tmp')
        self.assert_(len(cols) > 0, len(cols))

        for i, col in enumerate(user_tmp.columns):
            eq_(col.name, cols[i]['name'])

    @testing.requires.temp_table_reflection
    @testing.requires.view_column_reflection
    @testing.requires.temporary_views
    def test_get_temp_view_columns(self):
        insp = inspect(self.bind)
        cols = insp.get_columns('user_tmp_v')
        eq_(
            [col['name'] for col in cols],
            ['id', 'name', 'foo']
        )

    @testing.requires.view_column_reflection
    def test_get_view_columns(self):
        self._test_get_columns(table_type='view')

    @testing.requires.view_column_reflection
    @testing.requires.schemas
    def test_get_view_columns_with_schema(self):
        self._test_get_columns(
            schema=testing.config.test_schema, table_type='view')

    @testing.provide_metadata
    def _test_get_pk_constraint(self, schema=None):
        meta = self.metadata
        users, addresses = self.tables.users, self.tables.email_addresses
        insp = inspect(meta.bind)

        users_cons = insp.get_pk_constraint(users.name, schema=schema)
        users_pkeys = users_cons['constrained_columns']
        eq_(users_pkeys, ['user_id'])

        addr_cons = insp.get_pk_constraint(addresses.name, schema=schema)
        addr_pkeys = addr_cons['constrained_columns']
        eq_(addr_pkeys, ['address_id'])

        with testing.requires.reflects_pk_names.fail_if():
            eq_(addr_cons['name'], 'email_ad_pk')

    @testing.requires.primary_key_constraint_reflection
    def test_get_pk_constraint(self):
        self._test_get_pk_constraint()

    @testing.requires.table_reflection
    @testing.requires.primary_key_constraint_reflection
    @testing.requires.schemas
    def test_get_pk_constraint_with_schema(self):
        self._test_get_pk_constraint(schema=testing.config.test_schema)

    @testing.requires.table_reflection
    @testing.provide_metadata
    def test_deprecated_get_primary_keys(self):
        meta = self.metadata
        users = self.tables.users
        insp = Inspector(meta.bind)
        assert_raises_message(
            sa_exc.SADeprecationWarning,
            "Call to deprecated method get_primary_keys."
            "  Use get_pk_constraint instead.",
            insp.get_primary_keys, users.name
        )

    @testing.provide_metadata
    def _test_get_foreign_keys(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        insp = inspect(meta.bind)
        expected_schema = schema
        # users

        if testing.requires.self_referential_foreign_keys.enabled:
            users_fkeys = insp.get_foreign_keys(users.name,
                                                schema=schema)
            fkey1 = users_fkeys[0]

            with testing.requires.named_constraints.fail_if():
                eq_(fkey1['name'], "user_id_fk")

            eq_(fkey1['referred_schema'], expected_schema)
            eq_(fkey1['referred_table'], users.name)
            eq_(fkey1['referred_columns'], ['user_id', ])
            if testing.requires.self_referential_foreign_keys.enabled:
                eq_(fkey1['constrained_columns'], ['parent_user_id'])

        # addresses
        addr_fkeys = insp.get_foreign_keys(addresses.name,
                                           schema=schema)
        fkey1 = addr_fkeys[0]

        with testing.requires.implicitly_named_constraints.fail_if():
            self.assert_(fkey1['name'] is not None)

        eq_(fkey1['referred_schema'], expected_schema)
        eq_(fkey1['referred_table'], users.name)
        eq_(fkey1['referred_columns'], ['user_id', ])
        eq_(fkey1['constrained_columns'], ['remote_user_id'])

    @testing.requires.foreign_key_constraint_reflection
    def test_get_foreign_keys(self):
        self._test_get_foreign_keys()

    @testing.requires.foreign_key_constraint_reflection
    @testing.requires.schemas
    def test_get_foreign_keys_with_schema(self):
        self._test_get_foreign_keys(schema=testing.config.test_schema)

    @testing.requires.cross_schema_fk_reflection
    @testing.requires.schemas
    def test_get_inter_schema_foreign_keys(self):
        local_table, remote_table, remote_table_2 = self.tables(
            '%s.local_table' % testing.db.dialect.default_schema_name,
            '%s.remote_table' % testing.config.test_schema,
            '%s.remote_table_2' % testing.config.test_schema
        )

        insp = inspect(config.db)

        local_fkeys = insp.get_foreign_keys(local_table.name)
        eq_(len(local_fkeys), 1)

        fkey1 = local_fkeys[0]
        eq_(fkey1['referred_schema'], testing.config.test_schema)
        eq_(fkey1['referred_table'], remote_table_2.name)
        eq_(fkey1['referred_columns'], ['id', ])
        eq_(fkey1['constrained_columns'], ['remote_id'])

        remote_fkeys = insp.get_foreign_keys(
            remote_table.name, schema=testing.config.test_schema)
        eq_(len(remote_fkeys), 1)

        fkey2 = remote_fkeys[0]

        assert fkey2['referred_schema'] in (
            None,
            testing.db.dialect.default_schema_name
        )
        eq_(fkey2['referred_table'], local_table.name)
        eq_(fkey2['referred_columns'], ['id', ])
        eq_(fkey2['constrained_columns'], ['local_id'])


    @testing.requires.foreign_key_constraint_option_reflection_ondelete
    def test_get_foreign_key_options_ondelete(self):
        self._test_get_foreign_key_options(ondelete="CASCADE")

    @testing.requires.foreign_key_constraint_option_reflection_onupdate
    def test_get_foreign_key_options_onupdate(self):
        self._test_get_foreign_key_options(onupdate="SET NULL")

    @testing.provide_metadata
    def _test_get_foreign_key_options(self, **options):
        meta = self.metadata

        Table(
            'x', meta,
            Column('id', Integer, primary_key=True),
            test_needs_fk=True
        )

        Table('table', meta,
              Column('id', Integer, primary_key=True),
              Column('x_id', Integer, sa.ForeignKey('x.id', name='xid')),
              Column('test', String(10)),
              test_needs_fk=True)

        Table('user', meta,
              Column('id', Integer, primary_key=True),
              Column('name', String(50), nullable=False),
              Column('tid', Integer),
              sa.ForeignKeyConstraint(
                  ['tid'], ['table.id'],
                  name='myfk',
                  **options),
              test_needs_fk=True)

        meta.create_all()

        insp = inspect(meta.bind)

        # test 'options' is always present for a backend
        # that can reflect these, since alembic looks for this
        opts = insp.get_foreign_keys('table')[0]['options']

        eq_(
            dict(
                (k, opts[k])
                for k in opts if opts[k]
            ),
            {}
        )

        opts = insp.get_foreign_keys('user')[0]['options']
        eq_(
            dict(
                (k, opts[k])
                for k in opts if opts[k]
            ),
            options
        )

    def _assert_insp_indexes(self, indexes, expected_indexes):
        index_names = [d['name'] for d in indexes]
        for e_index in expected_indexes:
            assert e_index['name'] in index_names
            index = indexes[index_names.index(e_index['name'])]
            for key in e_index:
                eq_(e_index[key], index[key])

    @testing.provide_metadata
    def _test_get_indexes(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        # The database may decide to create indexes for foreign keys, etc.
        # so there may be more indexes than expected.
        insp = inspect(meta.bind)
        indexes = insp.get_indexes('users', schema=schema)
        expected_indexes = [
            {'unique': False,
             'column_names': ['test1', 'test2'],
             'name': 'users_t_idx'},
            {'unique': False,
             'column_names': ['user_id', 'test2', 'test1'],
             'name': 'users_all_idx'}
        ]
        self._assert_insp_indexes(indexes, expected_indexes)

    @testing.requires.index_reflection
    def test_get_indexes(self):
        self._test_get_indexes()

    @testing.requires.index_reflection
    @testing.requires.schemas
    def test_get_indexes_with_schema(self):
        self._test_get_indexes(schema=testing.config.test_schema)

    @testing.provide_metadata
    def _test_get_noncol_index(self, tname, ixname):
        meta = self.metadata
        insp = inspect(meta.bind)
        indexes = insp.get_indexes(tname)

        # reflecting an index that has "x DESC" in it as the column.
        # the DB may or may not give us "x", but make sure we get the index
        # back, it has a name, it's connected to the table.
        expected_indexes = [
            {'unique': False,
             'name': ixname}
        ]
        self._assert_insp_indexes(indexes, expected_indexes)

        t = Table(tname, meta, autoload_with=meta.bind)
        eq_(len(t.indexes), 1)
        is_(list(t.indexes)[0].table, t)
        eq_(list(t.indexes)[0].name, ixname)

    @testing.requires.index_reflection
    def test_get_noncol_index_no_pk(self):
        self._test_get_noncol_index("noncol_idx_test_nopk", "noncol_idx_nopk")

    @testing.requires.index_reflection
    def test_get_noncol_index_pk(self):
        self._test_get_noncol_index("noncol_idx_test_pk", "noncol_idx_pk")

    @testing.requires.unique_constraint_reflection
    def test_get_unique_constraints(self):
        self._test_get_unique_constraints()

    @testing.requires.temp_table_reflection
    @testing.requires.unique_constraint_reflection
    def test_get_temp_table_unique_constraints(self):
        insp = inspect(self.bind)
        reflected = insp.get_unique_constraints('user_tmp')
        for refl in reflected:
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            refl.pop('duplicates_index', None)
        eq_(reflected, [{'column_names': ['name'], 'name': 'user_tmp_uq'}])

    @testing.requires.temp_table_reflection
    def test_get_temp_table_indexes(self):
        insp = inspect(self.bind)
        indexes = insp.get_indexes('user_tmp')
        for ind in indexes:
            ind.pop('dialect_options', None)
        eq_(
            # TODO: we need to add better filtering for indexes/uq constraints
            # that are doubled up
            [idx for idx in indexes if idx['name'] == 'user_tmp_ix'],
            [{'unique': False, 'column_names': ['foo'], 'name': 'user_tmp_ix'}]
        )

    @testing.requires.unique_constraint_reflection
    @testing.requires.schemas
    def test_get_unique_constraints_with_schema(self):
        self._test_get_unique_constraints(schema=testing.config.test_schema)

    @testing.provide_metadata
    def _test_get_unique_constraints(self, schema=None):
        # SQLite dialect needs to parse the names of the constraints
        # separately from what it gets from PRAGMA index_list(), and
        # then matches them up.  so same set of column_names in two
        # constraints will confuse it.    Perhaps we should no longer
        # bother with index_list() here since we have the whole
        # CREATE TABLE?
        uniques = sorted(
            [
                {'name': 'unique_a', 'column_names': ['a']},
                {'name': 'unique_a_b_c', 'column_names': ['a', 'b', 'c']},
                {'name': 'unique_c_a_b', 'column_names': ['c', 'a', 'b']},
                {'name': 'unique_asc_key', 'column_names': ['asc', 'key']},
                {'name': 'i.have.dots', 'column_names': ['b']},
                {'name': 'i have spaces', 'column_names': ['c']},
            ],
            key=operator.itemgetter('name')
        )
        orig_meta = self.metadata
        table = Table(
            'testtbl', orig_meta,
            Column('a', sa.String(20)),
            Column('b', sa.String(30)),
            Column('c', sa.Integer),
            # reserved identifiers
            Column('asc', sa.String(30)),
            Column('key', sa.String(30)),
            schema=schema
        )
        for uc in uniques:
            table.append_constraint(
                sa.UniqueConstraint(*uc['column_names'], name=uc['name'])
            )
        orig_meta.create_all()

        inspector = inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_unique_constraints('testtbl', schema=schema),
            key=operator.itemgetter('name')
        )

        names_that_duplicate_index = set()

        for orig, refl in zip(uniques, reflected):
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            dupe = refl.pop('duplicates_index', None)
            if dupe:
                names_that_duplicate_index.add(dupe)
            eq_(orig, refl)

        reflected_metadata = MetaData()
        reflected = Table(
            'testtbl', reflected_metadata, autoload_with=orig_meta.bind,
            schema=schema)

        # test "deduplicates for index" logic.   MySQL and Oracle
        # "unique constraints" are actually unique indexes (with possible
        # exception of a unique that is a dupe of another one in the case
        # of Oracle).  make sure # they aren't duplicated.
        idx_names = set([idx.name for idx in reflected.indexes])
        uq_names = set([
            uq.name for uq in reflected.constraints
            if isinstance(uq, sa.UniqueConstraint)]).difference(
            ['unique_c_a_b'])

        assert not idx_names.intersection(uq_names)
        if names_that_duplicate_index:
            eq_(names_that_duplicate_index, idx_names)
            eq_(uq_names, set())

    @testing.requires.check_constraint_reflection
    def test_get_check_constraints(self):
        self._test_get_check_constraints()

    @testing.requires.check_constraint_reflection
    @testing.requires.schemas
    def test_get_check_constraints_schema(self):
        self._test_get_check_constraints(schema=testing.config.test_schema)

    @testing.provide_metadata
    def _test_get_check_constraints(self, schema=None):
        orig_meta = self.metadata
        Table(
            'sa_cc', orig_meta,
            Column('a', Integer()),
            sa.CheckConstraint('a > 1 AND a < 5', name='cc1'),
            sa.CheckConstraint('a = 1 OR (a > 2 AND a < 5)', name='cc2'),
            schema=schema
        )

        orig_meta.create_all()

        inspector = inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_check_constraints('sa_cc', schema=schema),
            key=operator.itemgetter('name')
        )

        # trying to minimize effect of quoting, parenthesis, etc.
        # may need to add more to this as new dialects get CHECK
        # constraint reflection support
        def normalize(sqltext):
            return " ".join(re.findall(r"and|\d|=|a|or|<|>", sqltext.lower(), re.I))

        reflected = [
            {"name": item["name"],
             "sqltext": normalize(item["sqltext"])}
            for item in reflected
        ]
        eq_(
            reflected,
            [
                {'name': 'cc1', 'sqltext': 'a > 1 and a < 5'},
                {'name': 'cc2', 'sqltext': 'a = 1 or a > 2 and a < 5'}
            ]
        )

    @testing.provide_metadata
    def _test_get_view_definition(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        view_name1 = 'users_v'
        view_name2 = 'email_addresses_v'
        insp = inspect(meta.bind)
        v1 = insp.get_view_definition(view_name1, schema=schema)
        self.assert_(v1)
        v2 = insp.get_view_definition(view_name2, schema=schema)
        self.assert_(v2)

    @testing.requires.view_reflection
    def test_get_view_definition(self):
        self._test_get_view_definition()

    @testing.requires.view_reflection
    @testing.requires.schemas
    def test_get_view_definition_with_schema(self):
        self._test_get_view_definition(schema=testing.config.test_schema)

    @testing.only_on("postgresql", "PG specific feature")
    @testing.provide_metadata
    def _test_get_table_oid(self, table_name, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        insp = inspect(meta.bind)
        oid = insp.get_table_oid(table_name, schema)
        self.assert_(isinstance(oid, int))

    def test_get_table_oid(self):
        self._test_get_table_oid('users')

    @testing.requires.schemas
    def test_get_table_oid_with_schema(self):
        self._test_get_table_oid('users', schema=testing.config.test_schema)

    @testing.requires.table_reflection
    @testing.provide_metadata
    def test_autoincrement_col(self):
        """test that 'autoincrement' is reflected according to sqla's policy.

        Don't mark this test as unsupported for any backend !

        (technically it fails with MySQL InnoDB since "id" comes before "id2")

        A backend is better off not returning "autoincrement" at all,
        instead of potentially returning "False" for an auto-incrementing
        primary key column.

        """

        meta = self.metadata
        insp = inspect(meta.bind)

        for tname, cname in [
            ('users', 'user_id'),
            ('email_addresses', 'address_id'),
            ('dingalings', 'dingaling_id'),
        ]:
            cols = insp.get_columns(tname)
            id_ = {c['name']: c for c in cols}[cname]
            assert id_.get('autoincrement', True)


class NormalizedNameTest(fixtures.TablesTest):
    __requires__ = 'denormalized_names',
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            quoted_name('t1', quote=True), metadata,
            Column('id', Integer, primary_key=True),
        )
        Table(
            quoted_name('t2', quote=True), metadata,
            Column('id', Integer, primary_key=True),
            Column('t1id', ForeignKey('t1.id'))
        )

    def test_reflect_lowercase_forced_tables(self):

        m2 = MetaData(testing.db)
        t2_ref = Table(quoted_name('t2', quote=True), m2, autoload=True)
        t1_ref = m2.tables['t1']
        assert t2_ref.c.t1id.references(t1_ref.c.id)

        m3 = MetaData(testing.db)
        m3.reflect(only=lambda name, m: name.lower() in ('t1', 't2'))
        assert m3.tables['t2'].c.t1id.references(m3.tables['t1'].c.id)

    def test_get_table_names(self):
        tablenames = [
            t for t in inspect(testing.db).get_table_names()
            if t.lower() in ("t1", "t2")]

        eq_(tablenames[0].upper(), tablenames[0].lower())
        eq_(tablenames[1].upper(), tablenames[1].lower())


__all__ = ('ComponentReflectionTest', 'HasTableTest', 'NormalizedNameTest')
