from test.lib.testing import eq_, assert_raises, assert_raises_message
import StringIO, unicodedata
from sqlalchemy import types as sql_types
from sqlalchemy import schema, events, event
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import MetaData, Integer, String
from test.lib.schema import Table, Column
import sqlalchemy as sa
from test.lib import ComparesTables, \
                            testing, engines, AssertsCompiledSQL
from test.lib import fixtures

create_inspector = Inspector.from_engine

metadata, users = None, None

class ReflectionTest(fixtures.TestBase, ComparesTables):

    @testing.exclude('mssql', '<', (10, 0, 0),
                     'Date is only supported on MSSQL 2008+')
    @testing.exclude('mysql', '<', (4, 1, 1),
                     'early types are squirrely')
    @testing.provide_metadata
    def test_basic_reflection(self):
        meta = self.metadata

        users = Table('engine_users', meta,
            Column('user_id', sa.INT, primary_key=True),
            Column('user_name', sa.VARCHAR(20), nullable=False),
            Column('test1', sa.CHAR(5), nullable=False),
            Column('test2', sa.Float(5), nullable=False),
            Column('test3', sa.Text),
            Column('test4', sa.Numeric(10, 2), nullable = False),
            Column('test5', sa.Date),
            Column('parent_user_id', sa.Integer,
                   sa.ForeignKey('engine_users.user_id')),
            Column('test6', sa.Date, nullable=False),
            Column('test7', sa.Text),
            Column('test8', sa.LargeBinary),
            Column('test_passivedefault2', sa.Integer, server_default='5'),
            Column('test9', sa.LargeBinary(100)),
            Column('test10', sa.Numeric(10, 2)),
            test_needs_fk=True,
        )

        addresses = Table(
            'engine_email_addresses',
            meta,
            Column('address_id', sa.Integer, primary_key=True),
            Column('remote_user_id', sa.Integer,
                   sa.ForeignKey(users.c.user_id)),
            Column('email_address', sa.String(20)),
            test_needs_fk=True,
            )
        meta.create_all()

        meta2 = MetaData()
        reflected_users = Table('engine_users', meta2,
                                autoload=True,
                                autoload_with=testing.db)
        reflected_addresses = Table('engine_email_addresses',
                meta2, autoload=True, autoload_with=testing.db)
        self.assert_tables_equal(users, reflected_users)
        self.assert_tables_equal(addresses, reflected_addresses)

    @testing.provide_metadata
    def test_two_foreign_keys(self):
        meta = self.metadata
        t1 = Table(
            't1',
            meta,
            Column('id', sa.Integer, primary_key=True),
            Column('t2id', sa.Integer, sa.ForeignKey('t2.id')),
            Column('t3id', sa.Integer, sa.ForeignKey('t3.id')),
            test_needs_fk=True,
            )
        t2 = Table('t2', meta, Column('id', sa.Integer,
                   primary_key=True), test_needs_fk=True)
        t3 = Table('t3', meta, Column('id', sa.Integer,
                   primary_key=True), test_needs_fk=True)
        meta.create_all()
        meta2 = MetaData()
        t1r, t2r, t3r = [Table(x, meta2, autoload=True,
                         autoload_with=testing.db) for x in ('t1',
                         't2', 't3')]
        assert t1r.c.t2id.references(t2r.c.id)
        assert t1r.c.t3id.references(t3r.c.id)

    def test_nonexistent(self):
        meta = MetaData(testing.db)
        assert_raises(sa.exc.NoSuchTableError, Table, 'nonexistent',
                      meta, autoload=True)

    @testing.provide_metadata
    def test_include_columns(self):
        meta = self.metadata
        foo = Table('foo', meta, *[Column(n, sa.String(30))
                                   for n in ['a', 'b', 'c', 'd', 'e', 'f']])
        meta.create_all()
        meta2 = MetaData(testing.db)
        foo = Table('foo', meta2, autoload=True,
                    include_columns=['b', 'f', 'e'])
        # test that cols come back in original order
        eq_([c.name for c in foo.c], ['b', 'e', 'f'])
        for c in ('b', 'f', 'e'):
            assert c in foo.c
        for c in ('a', 'c', 'd'):
            assert c not in foo.c

        # test against a table which is already reflected
        meta3 = MetaData(testing.db)
        foo = Table('foo', meta3, autoload=True)
        foo = Table('foo', meta3, include_columns=['b', 'f', 'e'],
                    extend_existing=True)
        eq_([c.name for c in foo.c], ['b', 'e', 'f'])
        for c in ('b', 'f', 'e'):
            assert c in foo.c
        for c in ('a', 'c', 'd'):
            assert c not in foo.c

    @testing.provide_metadata
    def test_extend_existing(self):
        meta = self.metadata

        t1 = Table('t', meta,
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('y', Integer),
            Column('z', Integer, server_default="5"),
        )
        meta.create_all()

        m2 = MetaData()
        old_z = Column('z', String, primary_key=True)
        old_y = Column('y', String)
        old_q = Column('q', Integer)
        t2 = Table('t', m2, old_z, old_q)
        eq_(t2.primary_key.columns, (t2.c.z, ))
        t2 = Table('t', m2, old_y,
                        extend_existing=True, 
                        autoload=True, 
                        autoload_with=testing.db)
        eq_(
            set(t2.columns.keys()), 
            set(['x', 'y', 'z', 'q', 'id'])
        )
        eq_(t2.primary_key.columns, (t2.c.id, ))
        assert t2.c.z is not old_z
        assert t2.c.y is old_y
        assert t2.c.z.type._type_affinity is Integer
        assert t2.c.q is old_q

        m3 = MetaData()
        t3 = Table('t', m3, Column('z', Integer))
        t3 = Table('t', m3, extend_existing=False, 
                        autoload=True, 
                        autoload_with=testing.db)
        eq_(
            set(t3.columns.keys()), 
            set(['z'])
        )

        m4 = MetaData()
        old_z = Column('z', String, primary_key=True)
        old_y = Column('y', String)
        old_q = Column('q', Integer)
        t4 = Table('t', m4, old_z, old_q)
        eq_(t4.primary_key.columns, (t4.c.z, ))
        t4 = Table('t', m4, old_y,
                        extend_existing=True, 
                        autoload=True, 
                        autoload_replace=False,
                        autoload_with=testing.db)
        eq_(
            set(t4.columns.keys()), 
            set(['x', 'y', 'z', 'q', 'id'])
        )
        eq_(t4.primary_key.columns, (t4.c.id, ))
        assert t4.c.z is old_z
        assert t4.c.y is old_y
        assert t4.c.z.type._type_affinity is String
        assert t4.c.q is old_q

    @testing.emits_warning(r".*omitted columns")
    @testing.provide_metadata
    def test_include_columns_indexes(self):
        m = self.metadata

        t1 = Table('t1', m, Column('a', sa.Integer), Column('b', sa.Integer))
        sa.Index('foobar', t1.c.a, t1.c.b)
        sa.Index('bat', t1.c.a)
        m.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)
        assert len(t2.indexes) == 2

        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True, include_columns=['a'])
        assert len(t2.indexes) == 1

        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True, include_columns=['a', 'b'])
        assert len(t2.indexes) == 2

    @testing.provide_metadata
    def test_autoload_replace(self):
        a = Table('a', self.metadata, Column('id', Integer, primary_key=True))
        b = Table('b', self.metadata, Column('id', Integer, primary_key=True),
                                    Column('a_id', Integer))
        self.metadata.create_all()

        m2 = MetaData()
        b2 = Table('b', m2, Column('a_id', Integer, sa.ForeignKey('a.id')))
        a2 = Table('a', m2, autoload=True, autoload_with=testing.db)
        b2 = Table('b', m2, extend_existing=True, autoload=True, 
                                autoload_with=testing.db, 
                                autoload_replace=False)

        assert b2.c.id is not None
        assert b2.c.a_id.references(a2.c.id)
        eq_(len(b2.constraints), 2)

    def test_autoload_replace_arg(self):
        Table('t', MetaData(), autoload_replace=False)

    @testing.provide_metadata
    def test_autoincrement_col(self):
        """test that 'autoincrement' is reflected according to sqla's policy.

        Don't mark this test as unsupported for any backend !

        (technically it fails with MySQL InnoDB since "id" comes before "id2")

        """

        meta = self.metadata
        t1 = Table('test', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('data', sa.String(50)),
            mysql_engine='MyISAM'
        )
        t2 = Table('test2', meta,
            Column('id', sa.Integer, sa.ForeignKey('test.id'),
                                        primary_key=True),
            Column('id2', sa.Integer, primary_key=True),
            Column('data', sa.String(50)),
            mysql_engine='MyISAM'
        )
        meta.create_all()
        m2 = MetaData(testing.db)
        t1a = Table('test', m2, autoload=True)
        assert t1a._autoincrement_column is t1a.c.id

        t2a = Table('test2', m2, autoload=True)
        assert t2a._autoincrement_column is t2a.c.id2

    @testing.provide_metadata
    def test_unknown_types(self):
        meta = self.metadata
        t = Table("test", meta,
            Column('foo', sa.DateTime))

        ischema_names = testing.db.dialect.ischema_names
        t.create()
        testing.db.dialect.ischema_names = {}
        try:
            m2 = MetaData(testing.db)
            assert_raises(sa.exc.SAWarning, Table, "test", m2, autoload=True)

            @testing.emits_warning('Did not recognize type')
            def warns():
                m3 = MetaData(testing.db)
                t3 = Table("test", m3, autoload=True)
                assert t3.c.foo.type.__class__ == sa.types.NullType

        finally:
            testing.db.dialect.ischema_names = ischema_names

    @testing.provide_metadata
    def test_basic_override(self):
        meta = self.metadata
        table = Table(
            'override_test', meta,
            Column('col1', sa.Integer, primary_key=True),
            Column('col2', sa.String(20)),
            Column('col3', sa.Numeric)
        )
        table.create()

        meta2 = MetaData(testing.db)
        table = Table(
            'override_test', meta2,
            Column('col2', sa.Unicode()),
            Column('col4', sa.String(30)), autoload=True)

        self.assert_(isinstance(table.c.col1.type, sa.Integer))
        self.assert_(isinstance(table.c.col2.type, sa.Unicode))
        self.assert_(isinstance(table.c.col4.type, sa.String))

    @testing.provide_metadata
    def test_override_pkfk(self):
        """test that you can override columns which contain foreign keys
        to other reflected tables, where the foreign key column is also
        a primary key column"""

        meta = self.metadata
        users = Table('users', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('name', sa.String(30)))
        addresses = Table('addresses', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('street', sa.String(30)))


        meta.create_all()
        meta2 = MetaData(testing.db)
        a2 = Table('addresses', meta2,
            Column('id', sa.Integer,
                   sa.ForeignKey('users.id'), primary_key=True),
            autoload=True)
        u2 = Table('users', meta2, autoload=True)

        assert list(a2.primary_key) == [a2.c.id]
        assert list(u2.primary_key) == [u2.c.id]
        assert u2.join(a2).onclause.compare(u2.c.id==a2.c.id)

        meta3 = MetaData(testing.db)
        u3 = Table('users', meta3, autoload=True)
        a3 = Table('addresses', meta3,
            Column('id', sa.Integer, sa.ForeignKey('users.id'),
                   primary_key=True),
            autoload=True)

        assert list(a3.primary_key) == [a3.c.id]
        assert list(u3.primary_key) == [u3.c.id]
        assert u3.join(a3).onclause.compare(u3.c.id==a3.c.id)

    @testing.provide_metadata
    def test_override_nonexistent_fk(self):
        """test that you can override columns and create new foreign
        keys to other reflected tables which have no foreign keys.  this
        is common with MySQL MyISAM tables."""

        meta = self.metadata
        users = Table('users', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('name', sa.String(30)))
        addresses = Table('addresses', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('street', sa.String(30)),
            Column('user_id', sa.Integer))

        meta.create_all()
        meta2 = MetaData(testing.db)
        a2 = Table('addresses', meta2,
                Column('user_id',sa.Integer, sa.ForeignKey('users.id')),
               autoload=True)
        u2 = Table('users', meta2, autoload=True)
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.foreign_keys) == 1
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] \
            == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent \
            is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)
        meta3 = MetaData(testing.db)

        u3 = Table('users', meta3, autoload=True)

        a3 = Table('addresses', meta3, Column('user_id',
                   sa.Integer, sa.ForeignKey('users.id')),
                   autoload=True)
        assert u3.join(a3).onclause.compare(u3.c.id == a3.c.user_id)

        meta4 = MetaData(testing.db)

        u4 = Table('users', meta4, 
                Column('id', sa.Integer, key='u_id', primary_key=True),
                autoload=True)

        a4 = Table(
            'addresses',
            meta4,
            Column('id', sa.Integer, key='street',
                   primary_key=True),
            Column('street', sa.String(30), key='user_id'),
            Column('user_id', sa.Integer, sa.ForeignKey('users.u_id'
                   ), key='id'),
            autoload=True,
            )
        assert u4.join(a4).onclause.compare(u4.c.u_id == a4.c.id)
        assert list(u4.primary_key) == [u4.c.u_id]
        assert len(u4.columns) == 2
        assert len(u4.constraints) == 1
        assert len(a4.columns) == 3
        assert len(a4.constraints) == 2

    @testing.provide_metadata
    def test_override_composite_fk(self):
        """Test double-remove of composite foreign key, when replaced."""

        metadata = self.metadata

        a = Table('a',
            metadata,
            Column('x', sa.Integer, primary_key=True),
            Column('y', sa.Integer, primary_key=True),
        )

        b = Table('b',
            metadata,
            Column('x', sa.Integer, primary_key=True),
            Column('y', sa.Integer, primary_key=True),
            sa.ForeignKeyConstraint(['x', 'y'], ['a.x', 'a.y'])
        )

        metadata.create_all()

        meta2 = MetaData()

        c1 = Column('x', sa.Integer, primary_key=True)
        c2 = Column('y', sa.Integer, primary_key=True)
        f1 = sa.ForeignKeyConstraint(['x', 'y'], ['a.x', 'a.y'])
        b1 = Table('b',
            meta2, c1, c2, f1,
            autoload=True,
            autoload_with=testing.db
        )

        assert b1.c.x is c1
        assert b1.c.y is c2
        assert f1 in b1.constraints
        assert len(b1.constraints) == 2



    @testing.provide_metadata
    def test_override_keys(self):
        """test that columns can be overridden with a 'key', 
        and that ForeignKey targeting during reflection still works."""

        meta = self.metadata
        a1 = Table('a', meta,
            Column('x', sa.Integer, primary_key=True),
            Column('z', sa.Integer),
            test_needs_fk=True
        )
        b1 = Table('b', meta,
            Column('y', sa.Integer, sa.ForeignKey('a.x')),
            test_needs_fk=True
        )
        meta.create_all()
        m2 = MetaData(testing.db)
        a2 = Table('a', m2, 
                Column('x', sa.Integer, primary_key=True, key='x1'),
                autoload=True)
        b2 = Table('b', m2, autoload=True)
        assert a2.join(b2).onclause.compare(a2.c.x1 == b2.c.y)
        assert b2.c.y.references(a2.c.x1)

    @testing.provide_metadata
    def test_nonreflected_fk_raises(self):
        """test that a NoReferencedColumnError is raised when reflecting
        a table with an FK to another table which has not included the target
        column in its reflection.

        """

        meta = self.metadata
        a1 = Table('a', meta,
            Column('x', sa.Integer, primary_key=True),
            Column('z', sa.Integer),
            test_needs_fk=True
        )
        b1 = Table('b', meta,
            Column('y', sa.Integer, sa.ForeignKey('a.x')),
            test_needs_fk=True
        )
        meta.create_all()
        m2 = MetaData(testing.db)
        a2 = Table('a', m2, include_columns=['z'], autoload=True)
        b2 = Table('b', m2, autoload=True)

        assert_raises(sa.exc.NoReferencedColumnError, a2.join, b2)

    @testing.exclude('mysql', '<', (4, 1, 1), 'innodb funkiness')
    @testing.provide_metadata
    def test_override_existing_fk(self):
        """test that you can override columns and specify new foreign
        keys to other reflected tables, on columns which *do* already
        have that foreign key, and that the FK is not duped. """

        meta = self.metadata
        users = Table('users', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('name', sa.String(30)),
            test_needs_fk=True)
        addresses = Table('addresses', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('user_id', sa.Integer, sa.ForeignKey('users.id')),
            test_needs_fk=True)

        meta.create_all()
        meta2 = MetaData(testing.db)
        a2 = Table('addresses', meta2, 
                Column('user_id',sa.Integer, sa.ForeignKey('users.id')),
                autoload=True)
        u2 = Table('users', meta2, autoload=True)
        s = sa.select([a2])

        assert s.c.user_id is not None
        assert len(a2.foreign_keys) == 1
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.constraints) == 2
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] \
            == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent \
            is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)

        meta2 = MetaData(testing.db)
        u2 = Table('users', meta2, Column('id', sa.Integer,
                   primary_key=True), autoload=True)
        a2 = Table('addresses', meta2, Column('id', sa.Integer,
                   primary_key=True), Column('user_id', sa.Integer,
                   sa.ForeignKey('users.id')), autoload=True)
        s = sa.select([a2])

        assert s.c.user_id is not None
        assert len(a2.foreign_keys) == 1
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.constraints) == 2
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] \
            == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent \
            is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)

    def test_pks_not_uniques(self):
        """test that primary key reflection not tripped up by unique
        indexes"""

        testing.db.execute("""
        CREATE TABLE book (
            id INTEGER NOT NULL,
            title VARCHAR(100) NOT NULL,
            series INTEGER,
            series_id INTEGER,
            UNIQUE(series, series_id),
            PRIMARY KEY(id)
        )""")
        try:
            metadata = MetaData(bind=testing.db)
            book = Table('book', metadata, autoload=True)
            assert book.primary_key.contains_column(book.c.id)
            assert not book.primary_key.contains_column(book.c.series)
            assert len(book.primary_key) == 1
        finally:
            testing.db.execute("drop table book")

    def test_fk_error(self):
        metadata = MetaData(testing.db)
        slots_table = Table('slots', metadata,
            Column('slot_id', sa.Integer, primary_key=True),
            Column('pkg_id', sa.Integer, sa.ForeignKey('pkgs.pkg_id')),
            Column('slot', sa.String(128)),
            )

        assert_raises_message(sa.exc.InvalidRequestError,
                            "Foreign key associated with column 'slots.pkg_id' "
                            "could not find table 'pkgs' with which to generate "
                            "a foreign key to target column 'pkg_id'",
                              metadata.create_all)

    def test_composite_pks(self):
        """test reflection of a composite primary key"""

        testing.db.execute("""
        CREATE TABLE book (
            id INTEGER NOT NULL,
            isbn VARCHAR(50) NOT NULL,
            title VARCHAR(100) NOT NULL,
            series INTEGER,
            series_id INTEGER,
            UNIQUE(series, series_id),
            PRIMARY KEY(id, isbn)
        )""")
        try:
            metadata = MetaData(bind=testing.db)
            book = Table('book', metadata, autoload=True)
            assert book.primary_key.contains_column(book.c.id)
            assert book.primary_key.contains_column(book.c.isbn)
            assert not book.primary_key.contains_column(book.c.series)
            assert len(book.primary_key) == 2
        finally:
            testing.db.execute("drop table book")

    @testing.exclude('mysql', '<', (4, 1, 1), 'innodb funkiness')
    @testing.provide_metadata
    def test_composite_fk(self):
        """test reflection of composite foreign keys"""

        meta = self.metadata
        multi = Table(
            'multi', meta,
            Column('multi_id', sa.Integer, primary_key=True),
            Column('multi_rev', sa.Integer, primary_key=True),
            Column('multi_hoho', sa.Integer, primary_key=True),
            Column('name', sa.String(50), nullable=False),
            Column('val', sa.String(100)),
            test_needs_fk=True,
        )
        multi2 = Table('multi2', meta,
            Column('id', sa.Integer, primary_key=True),
            Column('foo', sa.Integer),
            Column('bar', sa.Integer),
            Column('lala', sa.Integer),
            Column('data', sa.String(50)),
            sa.ForeignKeyConstraint(['foo', 'bar', 'lala'],
             ['multi.multi_id', 'multi.multi_rev', 'multi.multi_hoho'
             ]),
            test_needs_fk=True,
        )
        meta.create_all()

        meta2 = MetaData()
        table = Table('multi', meta2, autoload=True,
                      autoload_with=testing.db)
        table2 = Table('multi2', meta2, autoload=True,
                       autoload_with=testing.db)
        self.assert_tables_equal(multi, table)
        self.assert_tables_equal(multi2, table2)
        j = sa.join(table, table2)

        self.assert_(sa.and_(table.c.multi_id == table2.c.foo,
                     table.c.multi_rev == table2.c.bar,
                     table.c.multi_hoho
                     == table2.c.lala).compare(j.onclause))


    @testing.crashes('oracle', 'FIXME: unknown, confirm not fails_on')
    @testing.fails_on('+informixdb', 
                        "FIXME: should be supported via the "
                        "DELIMITED env var but that breaks "
                        "everything else for now")
    @testing.provide_metadata
    def test_reserved(self):

        # check a table that uses an SQL reserved name doesn't cause an
        # error

        meta = self.metadata
        table_a = Table('select', meta, Column('not', sa.Integer,
                        primary_key=True), Column('from',
                        sa.String(12), nullable=False),
                        sa.UniqueConstraint('from', name='when'))
        sa.Index('where', table_a.c['from'])

        # There's currently no way to calculate identifier case
        # normalization in isolation, so...

        if testing.against('firebird', 'oracle', 'maxdb'):
            check_col = 'TRUE'
        else:
            check_col = 'true'
        quoter = meta.bind.dialect.identifier_preparer.quote_identifier

        table_b = Table('false', meta, 
                    Column('create', sa.Integer, primary_key=True), 
                    Column('true', sa.Integer,sa.ForeignKey('select.not')),
                    sa.CheckConstraint('%s <> 1'
                        % quoter(check_col), name='limit')
                    )

        table_c = Table('is', meta, 
                Column('or', sa.Integer, nullable=False, primary_key=True), 
                Column('join', sa.Integer, nullable=False, primary_key=True),
                sa.PrimaryKeyConstraint('or', 'join', name='to')
                )
        index_c = sa.Index('else', table_c.c.join)
        meta.create_all()
        index_c.drop()
        meta2 = MetaData(testing.db)
        table_a2 = Table('select', meta2, autoload=True)
        table_b2 = Table('false', meta2, autoload=True)
        table_c2 = Table('is', meta2, autoload=True)

    @testing.provide_metadata
    def test_reflect_all(self):
        existing = testing.db.table_names()

        names = ['rt_%s' % name for name in ('a','b','c','d','e')]
        nameset = set(names)
        for name in names:
            # be sure our starting environment is sane
            self.assert_(name not in existing)
        self.assert_('rt_f' not in existing)

        baseline = self.metadata
        for name in names:
            Table(name, baseline, Column('id', sa.Integer, primary_key=True))
        baseline.create_all()

        m1 = MetaData(testing.db)
        self.assert_(not m1.tables)
        m1.reflect()
        self.assert_(nameset.issubset(set(m1.tables.keys())))

        m2 = MetaData()
        m2.reflect(testing.db, only=['rt_a', 'rt_b'])
        self.assert_(set(m2.tables.keys()) == set(['rt_a', 'rt_b']))

        m3 = MetaData()
        c = testing.db.connect()
        m3.reflect(bind=c, only=lambda name, meta: name == 'rt_c')
        self.assert_(set(m3.tables.keys()) == set(['rt_c']))

        m4 = MetaData(testing.db)
        try:
            m4.reflect(only=['rt_a', 'rt_f'])
            self.assert_(False)
        except sa.exc.InvalidRequestError, e:
            self.assert_(e.args[0].endswith('(rt_f)'))

        m5 = MetaData(testing.db)
        m5.reflect(only=[])
        self.assert_(not m5.tables)

        m6 = MetaData(testing.db)
        m6.reflect(only=lambda n, m: False)
        self.assert_(not m6.tables)

        m7 = MetaData(testing.db, reflect=True)
        self.assert_(nameset.issubset(set(m7.tables.keys())))

        try:
            m8 = MetaData(reflect=True)
            self.assert_(False)
        except sa.exc.ArgumentError, e:
            self.assert_(e.args[0]
                         == 'A bind must be supplied in '
                         'conjunction with reflect=True')

        if existing:
            print "Other tables present in database, skipping some checks."
        else:
            baseline.drop_all()
            m9 = MetaData(testing.db)
            m9.reflect()
            self.assert_(not m9.tables)

    def test_reflect_all_conn_closing(self):
        m1 = MetaData()
        c = testing.db.connect()
        m1.reflect(bind=c)
        assert not c.closed

    def test_inspector_conn_closing(self):
        m1 = MetaData()
        c = testing.db.connect()
        i = Inspector.from_engine(testing.db)
        assert not c.closed

    @testing.provide_metadata
    def test_index_reflection(self):
        m1 = self.metadata
        t1 = Table('party', m1,
            Column('id', sa.Integer, nullable=False),
            Column('name', sa.String(20), index=True)
            )
        i1 = sa.Index('idx1', t1.c.id, unique=True)
        i2 = sa.Index('idx2', t1.c.name, t1.c.id, unique=False)
        m1.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('party', m2, autoload=True)

        assert len(t2.indexes) == 3
        # Make sure indexes are in the order we expect them in
        tmp = [(idx.name, idx) for idx in t2.indexes]
        tmp.sort()
        r1, r2, r3 = [idx[1] for idx in tmp]

        assert r1.name == 'idx1'
        assert r2.name == 'idx2'
        assert r1.unique == True
        assert r2.unique == False
        assert r3.unique == False
        assert set([t2.c.id]) == set(r1.columns)
        assert set([t2.c.name, t2.c.id]) == set(r2.columns)
        assert set([t2.c.name]) == set(r3.columns)

    @testing.requires.views
    @testing.provide_metadata
    def test_views(self):
        metadata = self.metadata
        users, addresses, dingalings = createTables(metadata)
        try:
            metadata.create_all()
            _create_views(metadata.bind, None)
            m2 = MetaData(testing.db)
            users_v = Table("users_v", m2, autoload=True)
            addresses_v = Table("email_addresses_v", m2, autoload=True)

            for c1, c2 in zip(users.c, users_v.c):
                eq_(c1.name, c2.name)
                self.assert_types_base(c1, c2)

            for c1, c2 in zip(addresses.c, addresses_v.c):
                eq_(c1.name, c2.name)
                self.assert_types_base(c1, c2)
        finally:
            _drop_views(metadata.bind)

    @testing.requires.views
    @testing.provide_metadata
    def test_reflect_all_with_views(self):
        metadata = self.metadata
        users, addresses, dingalings = createTables(metadata, None)
        try:
            metadata.create_all()
            _create_views(metadata.bind, None)
            m2 = MetaData(testing.db)

            m2.reflect(views=False)
            eq_(
                set(m2.tables), 
                set(['users', 'email_addresses', 'dingalings'])
            )

            m2 = MetaData(testing.db)
            m2.reflect(views=True)
            eq_(
                set(m2.tables), 
                set(['email_addresses_v', 'users_v', 
                            'users', 'dingalings', 'email_addresses'])
            )
        finally:
            _drop_views(metadata.bind)

class CreateDropTest(fixtures.TestBase):

    @classmethod
    def setup_class(cls):
        global metadata, users
        metadata = MetaData()
        users = Table('users', metadata, 
                    Column('user_id', sa.Integer,
                      sa.Sequence('user_id_seq', optional=True),
                      primary_key=True), 
                    Column('user_name',sa.String(40)))

        addresses = Table('email_addresses', metadata,
                      Column('address_id', sa.Integer,
                          sa.Sequence('address_id_seq', optional=True),
                          primary_key=True), 
                      Column('user_id',
                          sa.Integer, sa.ForeignKey(users.c.user_id)),
                      Column('email_address', sa.String(40)))

        orders = Table(
            'orders',
            metadata,
            Column('order_id', sa.Integer, sa.Sequence('order_id_seq',
                   optional=True), primary_key=True),
            Column('user_id', sa.Integer,
                   sa.ForeignKey(users.c.user_id)),
            Column('description', sa.String(50)),
            Column('isopen', sa.Integer),
            )
        orderitems = Table('items', metadata, Column('item_id', sa.INT,
                           sa.Sequence('items_id_seq', optional=True),
                           primary_key=True), Column('order_id',
                           sa.INT, sa.ForeignKey('orders')),
                           Column('item_name', sa.VARCHAR(50)))

    def test_sorter( self ):
        tables = metadata.sorted_tables
        table_names = [t.name for t in tables]
        ua = [n for n in table_names if n in ('users', 'email_addresses')]
        oi = [n for n in table_names if n in ('orders', 'items')]

        eq_(ua, ['users', 'email_addresses'])
        eq_(oi, ['orders', 'items'])


    def testcheckfirst(self):
        try:
            assert not users.exists(testing.db)
            users.create(bind=testing.db)
            assert users.exists(testing.db)
            users.create(bind=testing.db, checkfirst=True)
            users.drop(bind=testing.db)
            users.drop(bind=testing.db, checkfirst=True)
            assert not users.exists(bind=testing.db)
            users.create(bind=testing.db, checkfirst=True)
            users.drop(bind=testing.db)
        finally:
            metadata.drop_all(bind=testing.db)

    def test_createdrop(self):
        metadata.create_all(bind=testing.db)
        eq_( testing.db.has_table('items'), True )
        eq_( testing.db.has_table('email_addresses'), True )
        metadata.create_all(bind=testing.db)
        eq_( testing.db.has_table('items'), True )

        metadata.drop_all(bind=testing.db)
        eq_( testing.db.has_table('items'), False )
        eq_( testing.db.has_table('email_addresses'), False )
        metadata.drop_all(bind=testing.db)
        eq_( testing.db.has_table('items'), False )

    def test_tablenames(self):
        metadata.create_all(bind=testing.db)

        # we only check to see if all the explicitly created tables are
        # there, rather than assertEqual -- the test db could have
        # "extra" tables if there is a misconfigured template.  (*cough*
        # tsearch2 w/ the pg windows installer.)

        self.assert_(not set(metadata.tables)
                     - set(testing.db.table_names()))
        metadata.drop_all(bind=testing.db)

class SchemaManipulationTest(fixtures.TestBase):
    def test_append_constraint_unique(self):
        meta = MetaData()

        users = Table('users', meta, Column('id', sa.Integer))
        addresses = Table('addresses', meta, 
                        Column('id', sa.Integer), 
                        Column('user_id', sa.Integer))

        fk = sa.ForeignKeyConstraint(['user_id'],[users.c.id])

        addresses.append_constraint(fk)
        addresses.append_constraint(fk)
        assert len(addresses.c.user_id.foreign_keys) == 1
        assert addresses.constraints == set([addresses.primary_key, fk])

class UnicodeReflectionTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        # trigger mysql _server_casing check...
        testing.db.connect().close()

        cls.bind = bind = engines.utf8_engine(
                            options={'convert_unicode' : True})

        cls.metadata = metadata = MetaData()

        no_multibyte_period = set([
            (u'plain', u'col_plain', u'ix_plain')
        ])
        no_has_table = [
            (u'no_has_table_1', u'col_Unit\u00e9ble', u'ix_Unit\u00e9ble'),
            (u'no_has_table_2', u'col_\u6e2c\u8a66', u'ix_\u6e2c\u8a66'),
        ]
        no_case_sensitivity = [
            (u'\u6e2c\u8a66', u'col_\u6e2c\u8a66', u'ix_\u6e2c\u8a66'),
            (u'unit\u00e9ble', u'col_unit\u00e9ble', u'ix_unit\u00e9ble'),
        ]
        full = [
            (u'Unit\u00e9ble', u'col_Unit\u00e9ble', u'ix_Unit\u00e9ble'),
            (u'\u6e2c\u8a66', u'col_\u6e2c\u8a66', u'ix_\u6e2c\u8a66'),
        ]

        # as you can see, our options for this kind of thing 
        # are really limited unless you're on PG or SQLite

        # forget about it on these backends
        if testing.against('sybase', 'maxdb', 'oracle'):
            names = no_multibyte_period
        # mysql can't handle casing usually
        elif testing.against("mysql") and \
            not testing.requires._has_mysql_fully_case_sensitive():
            names = no_multibyte_period.union(no_case_sensitivity)
        # mssql + pyodbc + freetds can't compare multibyte names to 
        # information_schema.tables.table_name
        elif testing.against("mssql"):
            names = no_multibyte_period.union(no_has_table)
        else:
            names = no_multibyte_period.union(full)

        for tname, cname, ixname in names:
            t = Table(tname, metadata, Column('id', sa.Integer,
                  sa.Sequence(cname + '_id_seq'), primary_key=True),
                  Column(cname, Integer)
                  )
            schema.Index(ixname, t.c[cname])

        metadata.create_all(bind)
        cls.names = names

    @classmethod
    def teardown_class(cls):
        cls.metadata.drop_all(cls.bind, checkfirst=False)
        cls.bind.dispose()

    @testing.requires.unicode_connections
    def test_has_table(self):
        for tname, cname, ixname in self.names:
            assert self.bind.has_table(tname), "Can't detect name %s" % tname

    @testing.requires.unicode_connections
    def test_basic(self):
        # the 'convert_unicode' should not get in the way of the
        # reflection process.  reflecttable for oracle, postgresql
        # (others?) expect non-unicode strings in result sets/bind
        # params

        bind = self.bind
        names = set([rec[0] for rec in self.names])

        reflected = set(bind.table_names())

        # Jython 2.5 on Java 5 lacks unicodedata.normalize

        if not names.issubset(reflected) and hasattr(unicodedata,'normalize'):

            # Python source files in the utf-8 coding seem to
            # normalize literals as NFC (and the above are
            # explicitly NFC).  Maybe this database normalizes NFD
            # on reflection.

            nfc = set([unicodedata.normalize('NFC', n) for n in names])
            self.assert_(nfc == names)

            # Yep.  But still ensure that bulk reflection and
            # create/drop work with either normalization.

        r = MetaData(bind, reflect=True)
        r.drop_all(checkfirst=False)
        r.create_all(checkfirst=False)

    @testing.requires.unicode_connections
    def test_get_names(self):
        inspector = Inspector.from_engine(self.bind)
        names = dict(
            (tname, (cname, ixname)) for tname, cname, ixname in self.names
        )
        for tname in inspector.get_table_names():
            assert tname in names
            eq_(
                [
                    (rec['name'], rec['column_names'][0])
                    for rec in inspector.get_indexes(tname)
                ],
                [(names[tname][1], names[tname][0])]
            )

class SchemaTest(fixtures.TestBase):

    @testing.requires.schemas
    @testing.fails_on_everything_except("postgresql", "unimplemented feature")
    def test_has_schema(self):
        eq_(testing.db.dialect.has_schema(testing.db, 'test_schema'), True)
        eq_(testing.db.dialect.has_schema(testing.db, 'sa_fake_schema_123'), False)

    @testing.crashes('firebird', 'No schema support')
    @testing.fails_on('sqlite', 'FIXME: unknown')
    # fixme: revisit these below.
    @testing.fails_on('access', 'FIXME: unknown')
    @testing.fails_on('sybase', 'FIXME: unknown')
    def test_explicit_default_schema(self):
        engine = testing.db
        engine.connect().close()

        if testing.against('sqlite'):
            # Works for CREATE TABLE main.foo, SELECT FROM main.foo, etc.,
            # but fails on:
            #   FOREIGN KEY(col2) REFERENCES main.table1 (col1)
            schema = 'main'
        else:
            schema = engine.dialect.default_schema_name

        assert bool(schema)

        metadata = MetaData(engine)
        table1 = Table('table1', metadata,
                       Column('col1', sa.Integer, primary_key=True),
                       test_needs_fk=True,
                       schema=schema)
        table2 = Table('table2', metadata,
                       Column('col1', sa.Integer, primary_key=True),
                       Column('col2', sa.Integer,
                              sa.ForeignKey('%s.table1.col1' % schema)),
                       test_needs_fk=True,
                       schema=schema)
        try:
            metadata.create_all()
            metadata.create_all(checkfirst=True)
            assert len(metadata.tables) == 2
            metadata.clear()

            table1 = Table('table1', metadata, autoload=True, schema=schema)
            table2 = Table('table2', metadata, autoload=True, schema=schema)
            assert len(metadata.tables) == 2
        finally:
            metadata.drop_all()

    @testing.crashes('firebird', 'No schema support')
    # fixme: revisit these below.
    @testing.fails_on('access', 'FIXME: unknown')
    @testing.fails_on('sybase', 'FIXME: unknown')
    def test_explicit_default_schema_metadata(self):
        engine = testing.db

        if testing.against('sqlite'):
            # Works for CREATE TABLE main.foo, SELECT FROM main.foo, etc.,
            # but fails on:
            #   FOREIGN KEY(col2) REFERENCES main.table1 (col1)
            schema = 'main'
        else:
            schema = engine.dialect.default_schema_name

        assert bool(schema)

        metadata = MetaData(engine, schema=schema)
        table1 = Table('table1', metadata,
                       Column('col1', sa.Integer, primary_key=True),
                       test_needs_fk=True)
        table2 = Table('table2', metadata,
                       Column('col1', sa.Integer, primary_key=True),
                       Column('col2', sa.Integer,
                              sa.ForeignKey('table1.col1')),
                       test_needs_fk=True)
        try:
            metadata.create_all()
            metadata.create_all(checkfirst=True)
            assert len(metadata.tables) == 2
            metadata.clear()

            table1 = Table('table1', metadata, autoload=True)
            table2 = Table('table2', metadata, autoload=True)
            assert len(metadata.tables) == 2
        finally:
            metadata.drop_all()

    @testing.requires.schemas
    @testing.provide_metadata
    def test_metadata_reflect_schema(self):
        metadata = self.metadata
        createTables(metadata, "test_schema")
        metadata.create_all()
        m2 = MetaData(schema="test_schema", bind=testing.db)
        m2.reflect()
        eq_(
            set(m2.tables), 
            set(['test_schema.dingalings', 'test_schema.users', 
                'test_schema.email_addresses'])
        )

class HasSequenceTest(fixtures.TestBase):

    @testing.requires.sequences
    def test_has_sequence(self):
        metadata = MetaData()
        users = Table('users', metadata, Column('user_id', sa.Integer,
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
    @testing.requires.sequences
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



# Tests related to engine.reflection


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
        Column('test4', sa.Numeric(10, 2), nullable = False),
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


class ReverseCasingReflectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @testing.requires.denormalized_names
    def setup(self):
        testing.db.execute("""
        CREATE TABLE weird_casing(
                col1 char(20),
                "Col2" char(20),
                "col3" char(20)
        )
        """)

    @testing.requires.denormalized_names
    def teardown(self):
        testing.db.execute("drop table weird_casing")

    @testing.requires.denormalized_names
    def test_direct_quoting(self):
        m = MetaData(testing.db)
        t = Table('weird_casing', m, autoload=True)
        self.assert_compile(t.select(),
                            'SELECT weird_casing.col1, '
                            'weird_casing."Col2", weird_casing."col3" '
                            'FROM weird_casing')

class CaseSensitiveTest(fixtures.TablesTest):
    """Nail down case sensitive behaviors, mostly on MySQL."""

    @classmethod
    def define_tables(cls, metadata):
        Table('SomeTable', metadata, 
            Column('x', Integer, primary_key=True),
            test_needs_fk=True
        )
        Table('SomeOtherTable', metadata, 
            Column('x', Integer, primary_key=True),
            Column('y', Integer, sa.ForeignKey("SomeTable.x")),
            test_needs_fk=True
        )

    @testing.fails_if(testing.requires._has_mysql_on_windows)
    def test_table_names(self):
        x = testing.db.run_callable(
            testing.db.dialect.get_table_names
        )
        assert set(["SomeTable", "SomeOtherTable"]).issubset(x)

    def test_reflect_exact_name(self):
        m = MetaData()
        t1 = Table("SomeTable", m, autoload=True, autoload_with=testing.db)
        eq_(t1.name, "SomeTable")
        assert t1.c.x is not None

    @testing.fails_if(lambda: 
            testing.against(('mysql', '<', (5, 5))) and 
            not testing.requires._has_mysql_fully_case_sensitive()
            )
    def test_reflect_via_fk(self):
        m = MetaData()
        t2 = Table("SomeOtherTable", m, autoload=True, autoload_with=testing.db)
        eq_(t2.name, "SomeOtherTable")
        assert "SomeTable" in m.tables

    @testing.fails_if(testing.requires._has_mysql_fully_case_sensitive)
    @testing.fails_on_everything_except('sqlite', 'mysql', 'mssql')
    def test_reflect_case_insensitive(self):
        m = MetaData()
        t2 = Table("sOmEtAbLe", m, autoload=True, autoload_with=testing.db)
        eq_(t2.name, "sOmEtAbLe")


class ComponentReflectionTest(fixtures.TestBase):

    @testing.requires.schemas
    def test_get_schema_names(self):
        insp = Inspector(testing.db)

        self.assert_('test_schema' in insp.get_schema_names())

    def test_dialect_initialize(self):
        engine = engines.testing_engine()
        assert not hasattr(engine.dialect, 'default_schema_name')
        insp = Inspector(engine)
        assert hasattr(engine.dialect, 'default_schema_name')

    def test_get_default_schema_name(self):
        insp = Inspector(testing.db)
        eq_(insp.default_schema_name, testing.db.dialect.default_schema_name)

    @testing.provide_metadata
    def _test_get_table_names(self, schema=None, table_type='table',
                              order_by=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        _create_views(meta.bind, schema)
        try:
            insp = Inspector(meta.bind)
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
            insp = Inspector(meta.bind)
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
    def _test_get_primary_keys(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        insp = Inspector(meta.bind)
        users_pkeys = insp.get_primary_keys(users.name,
                                            schema=schema)
        eq_(users_pkeys,  ['user_id'])
        addr_cons = insp.get_pk_constraint(addresses.name,
                                            schema=schema)

        addr_pkeys = addr_cons['constrained_columns']
        eq_(addr_pkeys,  ['address_id'])

        @testing.requires.reflects_pk_names
        def go():
            eq_(addr_cons['name'], 'email_ad_pk')
        go()

    def test_get_primary_keys(self):
        self._test_get_primary_keys()

    @testing.fails_on('sqlite', 'no schemas')
    def test_get_primary_keys_with_schema(self):
        self._test_get_primary_keys(schema='test_schema')

    @testing.provide_metadata
    def _test_get_foreign_keys(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = createTables(meta, schema)
        meta.create_all()
        insp = Inspector(meta.bind)
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
        insp = Inspector(meta.bind)
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
            insp = Inspector(meta.bind)
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
        insp = create_inspector(meta.bind)
        oid = insp.get_table_oid(table_name, schema)
        self.assert_(isinstance(oid, (int, long)))

    def test_get_table_oid(self):
        self._test_get_table_oid('users')

    @testing.requires.schemas
    def test_get_table_oid_with_schema(self):
        self._test_get_table_oid('users', schema='test_schema')

class ColumnEventsTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        cls.metadata = MetaData()
        cls.to_reflect = Table(
            'to_reflect',
            cls.metadata,
            Column('x', sa.Integer, primary_key=True),
        )
        cls.metadata.create_all(testing.db)

    @classmethod
    def teardown_class(cls):
        cls.metadata.drop_all(testing.db)

    def teardown(self):
        events.SchemaEventTarget.dispatch._clear()

    def _do_test(self, col, update, assert_):
        # load the actual Table class, not the test
        # wrapper
        from sqlalchemy.schema import Table

        m = MetaData(testing.db)
        def column_reflect(table, column_info):
            if column_info['name'] == col:
                column_info.update(update)

        t = Table('to_reflect', m, autoload=True, listeners=[
            ('column_reflect', column_reflect),
        ])
        assert_(t)

        m = MetaData(testing.db)
        event.listen(Table, 'column_reflect', column_reflect)
        t2 = Table('to_reflect', m, autoload=True)
        assert_(t2)

    def test_override_key(self):
        self._do_test(
            "x", {"key":"YXZ"},
            lambda table: eq_(table.c.YXZ.name, "x")
        )

    def test_override_type(self):
        def assert_(table):
            assert isinstance(table.c.x.type, sa.String)
        self._do_test(
            "x", {"type":sa.String},
            assert_
        )

    def test_override_info(self):
        self._do_test(
            "x", {"info":{"a":"b"}},
            lambda table: eq_(table.c.x.info, {"a":"b"})
        )
