import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import types as sql_types
from sqlalchemy import schema
from sqlalchemy import inspect
from sqlalchemy import MetaData, Integer, String
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.testing import engines, fixtures
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing import eq_, assert_raises_message
from sqlalchemy import testing
from .. import config

metadata, users = None, None


class HasTableTest(fixtures.TablesTest):
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

class HasSequenceTest(fixtures.TestBase):
    __requires__ = 'sequences',

    def test_has_sequence(self):
        metadata = MetaData()
        Table('users', metadata, Column('user_id', sa.Integer,
                      sa.Sequence('user_id_seq'), primary_key=True),
                      Column('user_name', sa.String(40)))
        metadata.create_all(bind=testing.db)
        try:
            eq_(testing.db.dialect.has_sequence(testing.db,
                'user_id_seq'), True)
        finally:
            metadata.drop_all(bind=testing.db)
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'),
            False)

    @testing.requires.schemas
    def test_has_sequence_schema(self):
        test_schema = 'test_schema'
        s1 = sa.Sequence('user_id_seq', schema=test_schema)
        s2 = sa.Sequence('user_id_seq')
        testing.db.execute(schema.CreateSequence(s1))
        testing.db.execute(schema.CreateSequence(s2))
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq',
            schema=test_schema), True)
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'),
            True)
        testing.db.execute(schema.DropSequence(s1))
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq',
            schema=test_schema), False)
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'),
            True)
        testing.db.execute(schema.DropSequence(s2))
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq',
            schema=test_schema), False)
        eq_(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'),
            False)


def createTables(meta, schema=None):
    if schema:
        schema_prefix = schema + "."
    else:
        schema_prefix = ""

    users = Table('users', meta,
        Column('user_id', sa.INT, primary_key=True),
        Column('user_name', sa.VARCHAR(20), nullable=False),
        Column('test1', sa.CHAR(5), nullable=False),
        Column('test2', sa.Float(5), nullable=False),
        Column('test3', sa.Text),
        Column('test4', sa.Numeric(10, 2), nullable=False),
        Column('test5', sa.Date),
        Column('test5_1', sa.TIMESTAMP),
        Column('parent_user_id', sa.Integer,
                    sa.ForeignKey('%susers.user_id' % schema_prefix)),
        Column('test6', sa.Date, nullable=False),
        Column('test7', sa.Text),
        Column('test8', sa.LargeBinary),
        Column('test_passivedefault2', sa.Integer, server_default='5'),
        Column('test9', sa.LargeBinary(100)),
        Column('test10', sa.Numeric(10, 2)),
        schema=schema,
        test_needs_fk=True,
    )
    dingalings = Table("dingalings", meta,
              Column('dingaling_id', sa.Integer, primary_key=True),
              Column('address_id', sa.Integer,
                    sa.ForeignKey('%semail_addresses.address_id' % schema_prefix)),
              Column('data', sa.String(30)),
              schema=schema,
              test_needs_fk=True,
        )
    addresses = Table('email_addresses', meta,
        Column('address_id', sa.Integer),
        Column('remote_user_id', sa.Integer,
               sa.ForeignKey(users.c.user_id)),
        Column('email_address', sa.String(20)),
        sa.PrimaryKeyConstraint('address_id', name='email_ad_pk'),
        schema=schema,
        test_needs_fk=True,
    )

    return (users, addresses, dingalings)

def createIndexes(con, schema=None):
    fullname = 'users'
    if schema:
        fullname = "%s.%s" % (schema, 'users')
    query = "CREATE INDEX users_t_idx ON %s (test1, test2)" % fullname
    con.execute(sa.sql.text(query))

@testing.requires.views
def _create_views(con, schema=None):
    for table_name in ('users', 'email_addresses'):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + '_v'
        query = "CREATE VIEW %s AS SELECT * FROM %s" % (view_name,
                                                                   fullname)
        con.execute(sa.sql.text(query))

@testing.requires.views
def _drop_views(con, schema=None):
    for table_name in ('email_addresses', 'users'):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + '_v'
        query = "DROP VIEW %s" % view_name
        con.execute(sa.sql.text(query))

class ComponentReflectionTest(fixtures.TestBase):

    @testing.requires.schemas
    def test_get_schema_names(self):
        insp = inspect(testing.db)

        self.assert_('test_schema' in insp.get_schema_names())

    def test_dialect_initialize(self):
        engine = engines.testing_engine()
        assert not hasattr(engine.dialect, 'default_schema_name')
        inspect(engine)
        assert hasattr(engine.dialect, 'default_schema_name')

    def test_get_default_schema_name(self):
        insp = inspect(testing.db)
        eq_(insp.default_schema_name, testing.db.dialect.default_schema_name)

    @testing.provide_metadata
    def _test_get_table_names(self, schema=None, table_type='table',
                              order_by=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        _create_views(meta.bind, schema)
        try:
            insp = inspect(meta.bind)
            if table_type == 'view':
                table_names = insp.get_view_names(schema)
                table_names.sort()
                answer = ['email_addresses_v', 'users_v']
            else:
                table_names = insp.get_table_names(schema,
                                                   order_by=order_by)
                if order_by == 'foreign_key':
                    answer = ['dingalings', 'email_addresses', 'users']
                    eq_(table_names, answer)
                else:
                    answer = ['dingalings', 'email_addresses', 'users']
                    eq_(sorted(table_names), answer)
        finally:
            _drop_views(meta.bind, schema)

    def test_get_table_names(self):
        self._test_get_table_names()

    def test_get_table_names_fks(self):
        self._test_get_table_names(order_by='foreign_key')

    @testing.requires.schemas
    def test_get_table_names_with_schema(self):
        self._test_get_table_names('test_schema')

    @testing.requires.views
    def test_get_view_names(self):
        self._test_get_table_names(table_type='view')

    @testing.requires.schemas
    def test_get_view_names_with_schema(self):
        self._test_get_table_names('test_schema', table_type='view')

    def _test_get_columns(self, schema=None, table_type='table'):
        meta = MetaData(testing.db)
        users, addresses, dingalings = createTables(meta, schema)
        table_names = ['users', 'email_addresses']
        meta.create_all()
        if table_type == 'view':
            _create_views(meta.bind, schema)
            table_names = ['users_v', 'email_addresses_v']
        try:
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
                        intersection(ctype_def.__mro__).intersection([
                        sql_types.Integer,
                        sql_types.Numeric,
                        sql_types.DateTime,
                        sql_types.Date,
                        sql_types.Time,
                        sql_types.String,
                        sql_types._Binary,
                        ])) > 0, '%s(%s), %s(%s)' % (col.name,
                                col.type, cols[i]['name'], ctype))
        finally:
            if table_type == 'view':
                _drop_views(meta.bind, schema)
            meta.drop_all()

    def test_get_columns(self):
        self._test_get_columns()

    @testing.requires.schemas
    def test_get_columns_with_schema(self):
        self._test_get_columns(schema='test_schema')

    @testing.requires.views
    def test_get_view_columns(self):
        self._test_get_columns(table_type='view')

    @testing.requires.views
    @testing.requires.schemas
    def test_get_view_columns_with_schema(self):
        self._test_get_columns(schema='test_schema', table_type='view')

    @testing.provide_metadata
    def _test_get_pk_constraint(self, schema=None):
        meta = self.metadata
        users, addresses, _ = createTables(meta, schema)
        meta.create_all()
        insp = inspect(meta.bind)

        users_cons = insp.get_pk_constraint(users.name, schema=schema)
        users_pkeys = users_cons['constrained_columns']
        eq_(users_pkeys,  ['user_id'])

        addr_cons = insp.get_pk_constraint(addresses.name, schema=schema)
        addr_pkeys = addr_cons['constrained_columns']
        eq_(addr_pkeys,  ['address_id'])

        @testing.requires.reflects_pk_names
        def go():
            eq_(addr_cons['name'], 'email_ad_pk')
        go()

    def test_get_pk_constraint(self):
        self._test_get_pk_constraint()

    @testing.fails_on('sqlite', 'no schemas')
    def test_get_pk_constraint_with_schema(self):
        self._test_get_pk_constraint(schema='test_schema')

    @testing.provide_metadata
    def test_deprecated_get_primary_keys(self):
        meta = self.metadata
        users, _, _ = createTables(meta, schema=None)
        meta.create_all()
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
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        insp = inspect(meta.bind)
        expected_schema = schema
        # users
        users_fkeys = insp.get_foreign_keys(users.name,
                                            schema=schema)
        fkey1 = users_fkeys[0]

        @testing.fails_on('sqlite', 'no support for constraint names')
        def go():
            self.assert_(fkey1['name'] is not None)
        go()

        eq_(fkey1['referred_schema'], expected_schema)
        eq_(fkey1['referred_table'], users.name)
        eq_(fkey1['referred_columns'], ['user_id', ])
        eq_(fkey1['constrained_columns'], ['parent_user_id'])
        #addresses
        addr_fkeys = insp.get_foreign_keys(addresses.name,
                                           schema=schema)
        fkey1 = addr_fkeys[0]
        @testing.fails_on('sqlite', 'no support for constraint names')
        def go():
            self.assert_(fkey1['name'] is not None)
        go()
        eq_(fkey1['referred_schema'], expected_schema)
        eq_(fkey1['referred_table'], users.name)
        eq_(fkey1['referred_columns'], ['user_id', ])
        eq_(fkey1['constrained_columns'], ['remote_user_id'])

    def test_get_foreign_keys(self):
        self._test_get_foreign_keys()

    @testing.requires.schemas
    def test_get_foreign_keys_with_schema(self):
        self._test_get_foreign_keys(schema='test_schema')

    @testing.provide_metadata
    def _test_get_indexes(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        createIndexes(meta.bind, schema)
        # The database may decide to create indexes for foreign keys, etc.
        # so there may be more indexes than expected.
        insp = inspect(meta.bind)
        indexes = insp.get_indexes('users', schema=schema)
        expected_indexes = [
            {'unique': False,
             'column_names': ['test1', 'test2'],
             'name': 'users_t_idx'}]
        index_names = [d['name'] for d in indexes]
        for e_index in expected_indexes:
            assert e_index['name'] in index_names
            index = indexes[index_names.index(e_index['name'])]
            for key in e_index:
                eq_(e_index[key], index[key])

    def test_get_indexes(self):
        self._test_get_indexes()

    @testing.requires.schemas
    def test_get_indexes_with_schema(self):
        self._test_get_indexes(schema='test_schema')

    @testing.provide_metadata
    def _test_get_view_definition(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        _create_views(meta.bind, schema)
        view_name1 = 'users_v'
        view_name2 = 'email_addresses_v'
        try:
            insp = inspect(meta.bind)
            v1 = insp.get_view_definition(view_name1, schema=schema)
            self.assert_(v1)
            v2 = insp.get_view_definition(view_name2, schema=schema)
            self.assert_(v2)
        finally:
            _drop_views(meta.bind, schema)

    @testing.requires.views
    def test_get_view_definition(self):
        self._test_get_view_definition()

    @testing.requires.views
    @testing.requires.schemas
    def test_get_view_definition_with_schema(self):
        self._test_get_view_definition(schema='test_schema')

    @testing.only_on("postgresql", "PG specific feature")
    @testing.provide_metadata
    def _test_get_table_oid(self, table_name, schema=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        insp = inspect(meta.bind)
        oid = insp.get_table_oid(table_name, schema)
        self.assert_(isinstance(oid, (int, long)))

    def test_get_table_oid(self):
        self._test_get_table_oid('users')

    @testing.requires.schemas
    def test_get_table_oid_with_schema(self):
        self._test_get_table_oid('users', schema='test_schema')


__all__ = ('ComponentReflectionTest', 'HasSequenceTest', 'HasTableTest')