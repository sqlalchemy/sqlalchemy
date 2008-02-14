import testenv; testenv.configure_for_tests()
import StringIO, unicodedata
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy import types as sqltypes
from testlib import *
from testlib import engines


class ReflectionTest(TestBase, ComparesTables):

    @testing.exclude('mysql', '<', (4, 1, 1))
    def test_basic_reflection(self):
        meta = MetaData(testing.db)

        users = Table('engine_users', meta,
            Column('user_id', INT, primary_key=True),
            Column('user_name', VARCHAR(20), nullable=False),
            Column('test1', CHAR(5), nullable=False),
            Column('test2', Float(5), nullable=False),
            Column('test3', Text),
            Column('test4', Numeric, nullable = False),
            Column('test5', DateTime),
            Column('parent_user_id', Integer, ForeignKey('engine_users.user_id')),
            Column('test6', DateTime, nullable=False),
            Column('test7', Text),
            Column('test8', Binary),
            Column('test_passivedefault2', Integer, PassiveDefault("5")),
            Column('test9', Binary(100)),
            Column('test_numeric', Numeric()),
            test_needs_fk=True,
        )

        addresses = Table('engine_email_addresses', meta,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            test_needs_fk=True,
        )
        meta.create_all()

        try:
            meta2 = MetaData()
            reflected_users = Table('engine_users', meta2, autoload=True, autoload_with=testing.db)
            reflected_addresses = Table('engine_email_addresses', meta2, autoload=True, autoload_with=testing.db)
            self.assert_tables_equal(users, reflected_users)
            self.assert_tables_equal(addresses, reflected_addresses)
        finally:
            addresses.drop()
            users.drop()

    def test_include_columns(self):
        meta = MetaData(testing.db)
        foo = Table('foo', meta, *[Column(n, String(30)) for n in ['a', 'b', 'c', 'd', 'e', 'f']])
        meta.create_all()
        try:
            meta2 = MetaData(testing.db)
            foo = Table('foo', meta2, autoload=True, include_columns=['b', 'f', 'e'])
            # test that cols come back in original order
            self.assertEquals([c.name for c in foo.c], ['b', 'e', 'f'])
            for c in ('b', 'f', 'e'):
                assert c in foo.c
            for c in ('a', 'c', 'd'):
                assert c not in foo.c
                
            # test against a table which is already reflected
            meta3 = MetaData(testing.db)
            foo = Table('foo', meta3, autoload=True)
            foo = Table('foo', meta3, include_columns=['b', 'f', 'e'], useexisting=True)
            self.assertEquals([c.name for c in foo.c], ['b', 'e', 'f'])
            for c in ('b', 'f', 'e'):
                assert c in foo.c
            for c in ('a', 'c', 'd'):
                assert c not in foo.c
        finally:
            meta.drop_all()


    def test_unknown_types(self):
        meta = MetaData(testing.db)
        t = Table("test", meta,
            Column('foo', DateTime))

        import sys
        dialect_module = sys.modules[testing.db.dialect.__module__]

        # we're relying on the presence of "ischema_names" in the
        # dialect module, else we can't test this.  we need to be able
        # to get the dialect to not be aware of some type so we temporarily
        # monkeypatch.  not sure what a better way for this could be,
        # except for an established dialect hook or dialect-specific tests
        if not hasattr(dialect_module, 'ischema_names'):
            return

        ischema_names = dialect_module.ischema_names
        t.create()
        dialect_module.ischema_names = {}
        try:
            try:
                m2 = MetaData(testing.db)
                t2 = Table("test", m2, autoload=True)
                assert False
            except exceptions.SAWarning:
                assert True

            @testing.emits_warning('Did not recognize type')
            def warns():
                m3 = MetaData(testing.db)
                t3 = Table("test", m3, autoload=True)
                assert t3.c.foo.type.__class__ == sqltypes.NullType

        finally:
            dialect_module.ischema_names = ischema_names
            t.drop()

    def test_basic_override(self):
        meta = MetaData(testing.db)
        table = Table(
            'override_test', meta,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(20)),
            Column('col3', Numeric)
        )
        table.create()

        meta2 = MetaData(testing.db)
        try:
            table = Table(
                'override_test', meta2,
                Column('col2', Unicode()),
                Column('col4', String(30)), autoload=True)

            self.assert_(isinstance(table.c.col1.type, Integer))
            self.assert_(isinstance(table.c.col2.type, Unicode))
            self.assert_(isinstance(table.c.col4.type, String))
        finally:
            table.drop()

    def test_override_pkfk(self):
        """test that you can override columns which contain foreign keys to other reflected tables,
        where the foreign key column is also a primary key column"""

        meta = MetaData(testing.db)
        users = Table('users', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)))
        addresses = Table('addresses', meta,
            Column('id', Integer, primary_key=True),
            Column('street', String(30)))


        meta.create_all()
        try:
            meta2 = MetaData(testing.db)
            a2 = Table('addresses', meta2,
                Column('id', Integer, ForeignKey('users.id'), primary_key=True),
                autoload=True)
            u2 = Table('users', meta2, autoload=True)

            assert list(a2.primary_key) == [a2.c.id]
            assert list(u2.primary_key) == [u2.c.id]
            assert u2.join(a2).onclause == u2.c.id==a2.c.id

            meta3 = MetaData(testing.db)
            u3 = Table('users', meta3, autoload=True)
            a3 = Table('addresses', meta3,
                Column('id', Integer, ForeignKey('users.id'), primary_key=True),
                autoload=True)

            assert list(a3.primary_key) == [a3.c.id]
            assert list(u3.primary_key) == [u3.c.id]
            assert u3.join(a3).onclause == u3.c.id==a3.c.id

        finally:
            meta.drop_all()

    def test_override_nonexistent_fk(self):
        """test that you can override columns and create new foreign keys to other reflected tables
        which have no foreign keys.  this is common with MySQL MyISAM tables."""

        meta = MetaData(testing.db)
        users = Table('users', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)))
        addresses = Table('addresses', meta,
            Column('id', Integer, primary_key=True),
            Column('street', String(30)),
            Column('user_id', Integer))

        meta.create_all()
        try:
            meta2 = MetaData(testing.db)
            a2 = Table('addresses', meta2,
                Column('user_id', Integer, ForeignKey('users.id')),
                autoload=True)
            u2 = Table('users', meta2, autoload=True)

            assert len(a2.c.user_id.foreign_keys) == 1
            assert len(a2.foreign_keys) == 1
            assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
            assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
            assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
            assert u2.join(a2).onclause == u2.c.id==a2.c.user_id

            meta3 = MetaData(testing.db)
            u3 = Table('users', meta3, autoload=True)
            a3 = Table('addresses', meta3,
                Column('user_id', Integer, ForeignKey('users.id')),
                autoload=True)

            assert u3.join(a3).onclause == u3.c.id==a3.c.user_id

            meta4 = MetaData(testing.db)
            u4 = Table('users', meta4,
                       Column('id', Integer, key='u_id', primary_key=True),
                       autoload=True)
            a4 = Table('addresses', meta4,
                       Column('id', Integer, key='street', primary_key=True),
                       Column('street', String(30), key='user_id'),
                       Column('user_id', Integer, ForeignKey('users.u_id'),
                              key='id'),
                       autoload=True)

            assert u4.join(a4).onclause.compare(u4.c.u_id==a4.c.id)
            assert list(u4.primary_key) == [u4.c.u_id]
            assert len(u4.columns) == 2
            assert len(u4.constraints) == 1
            assert len(a4.columns) == 3
            assert len(a4.constraints) == 2
        finally:
            meta.drop_all()

    def test_override_existing_fk(self):
        """test that you can override columns and specify new foreign keys to other reflected tables,
        on columns which *do* already have that foreign key, and that the FK is not duped.
        """

        meta = MetaData(testing.db)
        users = Table('users', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            test_needs_fk=True)
        addresses = Table('addresses', meta,
            Column('id', Integer,primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id')),
            test_needs_fk=True)

        meta.create_all()
        try:
            meta2 = MetaData(testing.db)
            a2 = Table('addresses', meta2,
                Column('user_id',Integer, ForeignKey('users.id')),
                autoload=True)
            u2 = Table('users', meta2, autoload=True)

            assert len(a2.foreign_keys) == 1
            assert len(a2.c.user_id.foreign_keys) == 1
            assert len(a2.constraints) == 2
            assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
            assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
            assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
            assert u2.join(a2).onclause == u2.c.id==a2.c.user_id

            meta2 = MetaData(testing.db)
            u2 = Table('users', meta2, 
                Column('id', Integer, primary_key=True),
                autoload=True)
            a2 = Table('addresses', meta2,
                Column('id', Integer, primary_key=True),
                Column('user_id',Integer, ForeignKey('users.id')),
                autoload=True)

            assert len(a2.foreign_keys) == 1
            assert len(a2.c.user_id.foreign_keys) == 1
            assert len(a2.constraints) == 2
            assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
            assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
            assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
            assert u2.join(a2).onclause == u2.c.id==a2.c.user_id
        finally:
            meta.drop_all()
    
    def test_use_existing(self):
        meta = MetaData(testing.db)
        users = Table('users', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            test_needs_fk=True)
        addresses = Table('addresses', meta,
            Column('id', Integer,primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id')),
            Column('data', String(100)),
            test_needs_fk=True)

        meta.create_all()
        try:
            meta2 = MetaData(testing.db)
            addresses = Table('addresses', meta2, Column('data', Unicode), autoload=True)
            try:
                users = Table('users', meta2, Column('name', Unicode), autoload=True)
                assert False
            except exceptions.InvalidRequestError, err:
                assert str(err) == "Table 'users' is already defined for this MetaData instance.  Specify 'useexisting=True' to redefine options and columns on an existing Table object."
            
            users = Table('users', meta2, Column('name', Unicode), autoload=True, useexisting=True)
            assert isinstance(users.c.name.type, Unicode)

            assert not users.quote
            
            users = Table('users', meta2, quote=True, autoload=True, useexisting=True)
            assert users.quote
            
        finally:
            meta.drop_all()
            
    def test_pks_not_uniques(self):
        """test that primary key reflection not tripped up by unique indexes"""

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
            assert book.c.id  in book.primary_key
            assert book.c.series not in book.primary_key
            assert len(book.primary_key) == 1
        finally:
            testing.db.execute("drop table book")

    def test_fk_error(self):
        metadata = MetaData(testing.db)
        slots_table = Table('slots', metadata,
            Column('slot_id', Integer, primary_key=True),
            Column('pkg_id', Integer, ForeignKey('pkgs.pkg_id')),
            Column('slot', String(128)),
            )
        try:
            metadata.create_all()
            assert False
        except exceptions.InvalidRequestError, err:
            assert str(err) == "Could not find table 'pkgs' with which to generate a foreign key"

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
            assert book.c.id  in book.primary_key
            assert book.c.isbn  in book.primary_key
            assert book.c.series not in book.primary_key
            assert len(book.primary_key) == 2
        finally:
            testing.db.execute("drop table book")

    @testing.exclude('mysql', '<', (4, 1, 1))
    def test_composite_fk(self):
        """test reflection of composite foreign keys"""

        meta = MetaData(testing.db)
        multi = Table(
            'multi', meta,
            Column('multi_id', Integer, primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('multi_hoho', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('val', String(100)),
            test_needs_fk=True,
        )
        multi2 = Table('multi2', meta,
            Column('id', Integer, primary_key=True),
            Column('foo', Integer),
            Column('bar', Integer),
            Column('lala', Integer),
            Column('data', String(50)),
            ForeignKeyConstraint(['foo', 'bar', 'lala'], ['multi.multi_id', 'multi.multi_rev', 'multi.multi_hoho']),
            test_needs_fk=True,
        )
        meta.create_all()

        try:
            meta2 = MetaData()
            table = Table('multi', meta2, autoload=True, autoload_with=testing.db)
            table2 = Table('multi2', meta2, autoload=True, autoload_with=testing.db)
            self.assert_tables_equal(multi, table)
            self.assert_tables_equal(multi2, table2)
            j = join(table, table2)
            self.assert_(and_(table.c.multi_id==table2.c.foo, table.c.multi_rev==table2.c.bar, table.c.multi_hoho==table2.c.lala).compare(j.onclause))
        finally:
            meta.drop_all()


    @testing.unsupported('oracle')
    def testreserved(self):
        # check a table that uses an SQL reserved name doesn't cause an error
        meta = MetaData(testing.db)
        table_a = Table('select', meta,
                       Column('not', Integer, primary_key=True),
                       Column('from', String(12), nullable=False),
                       UniqueConstraint('from', name='when'))
        Index('where', table_a.c['from'])

        # There's currently no way to calculate identifier case normalization
        # in isolation, so...
        if testing.against('firebird', 'oracle', 'maxdb'):
            check_col = 'TRUE'
        else:
            check_col = 'true'
        quoter = meta.bind.dialect.identifier_preparer.quote_identifier

        table_b = Table('false', meta,
                        Column('create', Integer, primary_key=True),
                        Column('true', Integer, ForeignKey('select.not')),
                        CheckConstraint('%s <> 1' % quoter(check_col),
                                        name='limit'))

        table_c = Table('is', meta,
                        Column('or', Integer, nullable=False, primary_key=True),
                        Column('join', Integer, nullable=False, primary_key=True),
                        PrimaryKeyConstraint('or', 'join', name='to'))

        index_c = Index('else', table_c.c.join)

        meta.create_all()

        index_c.drop()

        meta2 = MetaData(testing.db)
        try:
            table_a2 = Table('select', meta2, autoload=True)
            table_b2 = Table('false', meta2, autoload=True)
            table_c2 = Table('is', meta2, autoload=True)
        finally:
            meta.drop_all()

    def test_reflect_all(self):
        existing = testing.db.table_names()

        names = ['rt_%s' % name for name in ('a','b','c','d','e')]
        nameset = set(names)
        for name in names:
            # be sure our starting environment is sane
            self.assert_(name not in existing)
        self.assert_('rt_f' not in existing)

        baseline = MetaData(testing.db)
        for name in names:
            Table(name, baseline, Column('id', Integer, primary_key=True))
        baseline.create_all()

        try:
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
            except exceptions.InvalidRequestError, e:
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
            except exceptions.ArgumentError, e:
                self.assert_(
                    e.args[0] ==
                    "A bind must be supplied in conjunction with reflect=True")
        finally:
            baseline.drop_all()

        if existing:
            print "Other tables present in database, skipping some checks."
        else:
            m9 = MetaData(testing.db)
            m9.reflect()
            self.assert_(not m9.tables)


class CreateDropTest(TestBase):
    def setUpAll(self):
        global metadata, users
        metadata = MetaData()
        users = Table('users', metadata,
                      Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key=True),
                      Column('user_name', String(40)),
                      )

        addresses = Table('email_addresses', metadata,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(40)),
        )

        orders = Table('orders', metadata,
            Column('order_id', Integer, Sequence('order_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('description', String(50)),
            Column('isopen', Integer),
        )

        orderitems = Table('items', metadata,
            Column('item_id', INT, Sequence('items_id_seq', optional=True), primary_key = True),
            Column('order_id', INT, ForeignKey("orders")),
            Column('item_name', VARCHAR(50)),
        )

    def test_sorter( self ):
        tables = metadata.table_iterator(reverse=False)
        table_names = [t.name for t in tables]
        self.assert_( table_names == ['users', 'orders', 'items', 'email_addresses'] or table_names ==  ['users', 'email_addresses', 'orders', 'items'])

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

    @testing.exclude('mysql', '<', (4, 1, 1))
    def test_createdrop(self):
        metadata.create_all(bind=testing.db)
        self.assertEqual( testing.db.has_table('items'), True )
        self.assertEqual( testing.db.has_table('email_addresses'), True )
        metadata.create_all(bind=testing.db)
        self.assertEqual( testing.db.has_table('items'), True )

        metadata.drop_all(bind=testing.db)
        self.assertEqual( testing.db.has_table('items'), False )
        self.assertEqual( testing.db.has_table('email_addresses'), False )
        metadata.drop_all(bind=testing.db)
        self.assertEqual( testing.db.has_table('items'), False )

    def test_tablenames(self):
        from sqlalchemy.util import Set
        metadata.create_all(bind=testing.db)
        # we only check to see if all the explicitly created tables are there, rather than
        # assertEqual -- the test db could have "extra" tables if there is a misconfigured
        # template.  (*cough* tsearch2 w/ the pg windows installer.)
        self.assert_(not Set(metadata.tables) - Set(testing.db.table_names()))
        metadata.drop_all(bind=testing.db)

class SchemaManipulationTest(TestBase):
    def test_append_constraint_unique(self):
        meta = MetaData()
        
        users = Table('users', meta, Column('id', Integer))
        addresses = Table('addresses', meta, Column('id', Integer), Column('user_id', Integer))
        
        fk = ForeignKeyConstraint(['user_id'],[users.c.id])
        
        addresses.append_constraint(fk)
        addresses.append_constraint(fk)
        assert len(addresses.c.user_id.foreign_keys) == 1
        assert addresses.constraints == set([addresses.primary_key, fk])
        
class UnicodeReflectionTest(TestBase):

    def test_basic(self):
        try:
            # the 'convert_unicode' should not get in the way of the reflection
            # process.  reflecttable for oracle, postgres (others?) expect non-unicode
            # strings in result sets/bind params
            bind = engines.utf8_engine(options={'convert_unicode':True})
            metadata = MetaData(bind)

            if testing.against('sybase', 'maxdb', 'oracle'):
                names = set(['plain'])
            else:
                names = set([u'plain', u'Unit\u00e9ble', u'\u6e2c\u8a66'])

            for name in names:
                Table(name, metadata, Column('id', Integer, Sequence(name + "_id_seq"), primary_key=True))
            metadata.create_all()

            reflected = set(bind.table_names())
            if not names.issubset(reflected):
                # Python source files in the utf-8 coding seem to normalize
                # literals as NFC (and the above are explicitly NFC).  Maybe
                # this database normalizes NFD on reflection.
                nfc = set([unicodedata.normalize('NFC', n) for n in names])
                self.assert_(nfc == names)
                # Yep.  But still ensure that bulk reflection and create/drop
                # work with either normalization.

            r = MetaData(bind, reflect=True)
            r.drop_all()
            r.create_all()
        finally:
            metadata.drop_all()
            bind.dispose()


class SchemaTest(TestBase):

    def test_iteration(self):
        metadata = MetaData()
        table1 = Table('table1', metadata,
            Column('col1', Integer, primary_key=True),
            schema='someschema')
        table2 = Table('table2', metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', Integer, ForeignKey('someschema.table1.col1')),
            schema='someschema')
        # ensure this doesnt crash
        print [t for t in metadata.table_iterator()]
        buf = StringIO.StringIO()
        def foo(s, p=None):
            buf.write(s)
        gen = create_engine(testing.db.name + "://", strategy="mock", executor=foo)
        gen = gen.dialect.schemagenerator(gen.dialect, gen)
        gen.traverse(table1)
        gen.traverse(table2)
        buf = buf.getvalue()
        print buf
        if testing.db.dialect.preparer(testing.db.dialect).omit_schema:
            assert buf.index("CREATE TABLE table1") > -1
            assert buf.index("CREATE TABLE table2") > -1
        else:
            assert buf.index("CREATE TABLE someschema.table1") > -1
            assert buf.index("CREATE TABLE someschema.table2") > -1

    @testing.unsupported('sqlite', 'firebird')
    # fixme: revisit these below.
    @testing.fails_on('oracle', 'mssql', 'sybase', 'access')
    def test_explicit_default_schema(self):
        engine = testing.db

        if testing.against('mysql'):
            schema = testing.db.url.database
        elif testing.against('postgres'):
            schema = 'public'
        else:
            schema = engine.dialect.get_default_schema_name(engine)

        metadata = MetaData(engine)
        table1 = Table('table1', metadata,
                       Column('col1', Integer, primary_key=True),
                       schema=schema)
        table2 = Table('table2', metadata,
                       Column('col1', Integer, primary_key=True),
                       Column('col2', Integer,
                              ForeignKey('%s.table1.col1' % schema)),
                       schema=schema)
        try:
            metadata.create_all()
            metadata.create_all(checkfirst=True)
            metadata.clear()

            table1 = Table('table1', metadata, autoload=True, schema=schema)
            table2 = Table('table2', metadata, autoload=True, schema=schema)
        finally:
            metadata.drop_all()


class HasSequenceTest(TestBase):
    def setUpAll(self):
        global metadata, users
        metadata = MetaData()
        users = Table('users', metadata,
                      Column('user_id', Integer, Sequence('user_id_seq'), primary_key=True),
                      Column('user_name', String(40)),
                      )

    @testing.unsupported('sqlite', 'mysql', 'mssql', 'access', 'sybase')
    def test_hassequence(self):
        metadata.create_all(bind=testing.db)
        self.assertEqual(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'), True)
        metadata.drop_all(bind=testing.db)
        self.assertEqual(testing.db.dialect.has_sequence(testing.db, 'user_id_seq'), False)


if __name__ == "__main__":
    testenv.main()
