from testbase import PersistTest, AssertMixin
from sqlalchemy import *
import testbase
import pickleable
from sqlalchemy.orm.mapper import global_extensions
from sqlalchemy.orm import util as ormutil
from sqlalchemy.ext.sessioncontext import SessionContext
import sqlalchemy.ext.assignmapper as assignmapper
from tables import *
import tables

"""tests unitofwork operations"""

class UnitOfWorkTest(AssertMixin):
    def setUpAll(self):
        global ctx, assign_mapper
        ctx = SessionContext(create_session)
        def assign_mapper(*args, **kwargs):
            return assignmapper.assign_mapper(ctx, *args, **kwargs)
        global_extensions.append(ctx.mapper_extension)
    def tearDownAll(self):
        global_extensions.remove(ctx.mapper_extension)
    def tearDown(self):
        ctx.current.clear()
        clear_mappers()

class HistoryTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        users.create()
        addresses.create()
    def tearDownAll(self):
        addresses.drop()
        users.drop()
        UnitOfWorkTest.tearDownAll(self)
        
    def testbackref(self):
        s = create_session()
        class User(object):pass
        class Address(object):pass
        am = mapper(Address, addresses)
        m = mapper(User, users, properties = dict(
            addresses = relation(am, backref='user', lazy=False))
        )
        
        u = User(_sa_session=s)
        a = Address(_sa_session=s)
        a.user = u
        #print repr(a.__class__._attribute_manager.get_history(a, 'user').added_items())
        #print repr(u.addresses.added_items())
        self.assert_(u.addresses == [a])
        s.flush()

        s.clear()
        u = s.query(m).select()[0]
        print u.addresses[0].user

            
class VersioningTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        ctx.current.clear()
        global version_table
        version_table = Table('version_test', db,
        Column('id', Integer, Sequence('version_test_seq'), primary_key=True ),
        Column('version_id', Integer, nullable=False),
        Column('value', String(40), nullable=False)
        )
        version_table.create()
    def tearDownAll(self):
        version_table.drop()
        UnitOfWorkTest.tearDownAll(self)
    def tearDown(self):
        version_table.delete().execute()
        UnitOfWorkTest.tearDown(self)
    
    def testbasic(self):
        s = create_session()
        class Foo(object):pass
        assign_mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        f1 =Foo(value='f1', _sa_session=s)
        f2 = Foo(value='f2', _sa_session=s)
        s.flush()
        
        f1.value='f1rev2'
        s.flush()
        s2 = create_session()
        f1_s = s2.query(Foo).get(f1.id)
        f1_s.value='f1rev3'
        s2.flush()

        f1.value='f1rev3mine'
        success = False
        try:
            # a concurrent session has modified this, should throw
            # an exception
            s.flush()
        except exceptions.ConcurrentModificationError, e:
            #print e
            success = True

        # Only dialects with a sane rowcount can detect the ConcurrentModificationError
        if testbase.db.dialect.supports_sane_rowcount():
            assert success
        
        s.clear()
        f1 = s.query(Foo).get(f1.id)
        f2 = s.query(Foo).get(f2.id)
        
        f1_s.value='f1rev4'
        s2.flush()
    
        s.delete(f1)
        s.delete(f2)
        success = False
        try:
            s.flush()
        except exceptions.ConcurrentModificationError, e:
            #print e
            success = True
        if testbase.db.dialect.supports_sane_rowcount():
            assert success

    def testversioncheck(self):
        """test that query.with_lockmode performs a 'version check' on an already loaded instance"""
        s1 = create_session()
        class Foo(object):pass
        assign_mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        f1s1 =Foo(value='f1', _sa_session=s1)
        s1.flush()
        s2 = create_session()
        f1s2 = s2.query(Foo).get(f1s1.id)
        f1s2.value='f1 new value'
        s2.flush()
        try:
            # load, version is wrong
            s1.query(Foo).with_lockmode('read').get(f1s1.id)
            assert False
        except exceptions.ConcurrentModificationError, e:
            assert True
        # reload it
        s1.query(Foo).load(f1s1.id)
        # now assert version OK
        s1.query(Foo).with_lockmode('read').get(f1s1.id)
        
        # assert brand new load is OK too
        s1.clear()
        s1.query(Foo).with_lockmode('read').get(f1s1.id)
        
    def testnoversioncheck(self):
        """test that query.with_lockmode works OK when the mapper has no version id col"""
        s1 = create_session()
        class Foo(object):pass
        assign_mapper(Foo, version_table)
        f1s1 =Foo(value='f1', _sa_session=s1)
        f1s1.version_id=0
        s1.flush()
        s2 = create_session()
        f1s2 = s2.query(Foo).with_lockmode('read').get(f1s1.id)
        assert f1s2.id == f1s1.id
        assert f1s2.value == f1s1.value
        
class UnicodeTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global metadata, uni_table, uni_table2
        metadata = MetaData(testbase.db)
        uni_table = Table('uni_test', metadata,
            Column('id',  Integer, Sequence("uni_test_id_seq", optional=True), primary_key=True),
            Column('txt', Unicode(50), unique=True))
        uni_table2 = Table('uni2', metadata,
            Column('id',  Integer, Sequence("uni2_test_id_seq", optional=True), primary_key=True),
            Column('txt', Unicode(50), ForeignKey(uni_table.c.txt)))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        UnitOfWorkTest.tearDownAll(self)
    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    def testbasic(self):
        class Test(object):
            def __init__(self, id, txt):
                self.id = id
                self.txt = txt
        mapper(Test, uni_table)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(id=1, txt = txt)
        self.assert_(t1.txt == txt)
        ctx.current.flush()
        self.assert_(t1.txt == txt)
    def testrelation(self):
        class Test(object):
            def __init__(self, txt):
                self.txt = txt
        class Test2(object):pass
            
        mapper(Test, uni_table, properties={
            't2s':relation(Test2)
        })
        mapper(Test2, uni_table2)
            
        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(txt=txt)
        t1.t2s.append(Test2())
        t1.t2s.append(Test2())
        ctx.current.flush()
        ctx.current.clear()
        t1 = ctx.current.query(Test).get_by(id=t1.id)
        assert len(t1.t2s) == 2

class MutableTypesTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global metadata, table
        metadata = MetaData(testbase.db)
        table = Table('mutabletest', metadata,
            Column('id', Integer, Sequence('mutableidseq', optional=True), primary_key=True),
            Column('data', PickleType),
            Column('value', Unicode(30)))
        table.create()
    def tearDownAll(self):
        table.drop()
        UnitOfWorkTest.tearDownAll(self)

    def testbasic(self):
        """test that types marked as MutableType get changes detected on them"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        ctx.current.flush()
        ctx.current.clear()
        f2 = ctx.current.query(Foo).get_by(id=f1.id)
        assert f2.data == f1.data
        f2.data.y = 19
        ctx.current.flush()
        ctx.current.clear()
        f3 = ctx.current.query(Foo).get_by(id=f1.id)
        print f2.data, f3.data
        assert f3.data != f1.data
        assert f3.data == pickleable.Bar(4, 19)

    def testmutablechanges(self):
        """test that mutable changes are detected or not detected correctly"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        f1.value = unicode('hi')
        ctx.current.flush()
        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)
        f1.value = unicode('someothervalue')
        self.assert_sql(db, lambda: ctx.current.flush(), [
            (
                "UPDATE mutabletest SET value=:value WHERE mutabletest.id = :mutabletest_id",
                {'mutabletest_id': f1.id, 'value': u'someothervalue'}
            ),
        ])
        f1.value = unicode('hi')
        f1.data.x = 9
        self.assert_sql(db, lambda: ctx.current.flush(), [
            (
                "UPDATE mutabletest SET data=:data, value=:value WHERE mutabletest.id = :mutabletest_id",
                {'mutabletest_id': f1.id, 'value': u'hi', 'data':f1.data}
            ),
        ])
        
        
    def testnocomparison(self):
        """test that types marked as MutableType get changes detected on them when the type has no __eq__ method"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.BarWithoutCompare(4,5)
        ctx.current.flush()
        
        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)
        
        ctx.current.clear()

        f2 = ctx.current.query(Foo).get_by(id=f1.id)

        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)

        f2.data.y = 19
        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 1)
        
        ctx.current.clear()
        f3 = ctx.current.query(Foo).get_by(id=f1.id)
        print f2.data, f3.data
        assert (f3.data.x, f3.data.y) == (4,19)

        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)
        
    def testunicode(self):
        """test that two equivalent unicode values dont get flagged as changed.
        
        apparently two equal unicode objects dont compare via "is" in all cases, so this
        tests the compare_values() call on types.String and its usage via types.Unicode."""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.value = u'hi'
        ctx.current.flush()
        ctx.current.clear()
        f1 = ctx.current.get(Foo, f1.id)
        f1.value = u'hi'
        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)
        
        
class PKTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global table
        global table2
        global table3
        table = Table(
            'multipk', db, 
            Column('multi_id', Integer, Sequence("multi_id_seq", optional=True), primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('value', String(100))
        )
        
        table2 = Table('multipk2', db,
            Column('pk_col_1', String(30), primary_key=True),
            Column('pk_col_2', String(30), primary_key=True),
            Column('data', String(30), )
            )
        table3 = Table('multipk3', db,
            Column('pri_code', String(30), key='primary', primary_key=True),
            Column('sec_code', String(30), key='secondary', primary_key=True),
            Column('date_assigned', Date, key='assigned', primary_key=True),
            Column('data', String(30), )
            )
        table.create()
        table2.create()
        table3.create()

    def tearDownAll(self):
        table.drop()
        table2.drop()
        table3.drop()
        UnitOfWorkTest.tearDownAll(self)
        
    # not support on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys    
    @testbase.unsupported('sqlite')
    def testprimarykey(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table)
        e = Entry()
        e.name = 'entry1'
        e.value = 'this is entry 1'
        e.multi_rev = 2
        ctx.current.flush()
        ctx.current.clear()
        e2 = Query(Entry).get((e.multi_id, 2))
        self.assert_(e is not e2 and e._instance_key == e2._instance_key)
        
    # this one works with sqlite since we are manually setting up pk values
    def testmanualpk(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table2)
        e = Entry()
        e.pk_col_1 = 'pk1'
        e.pk_col_2 = 'pk1_related'
        e.data = 'im the data'
        ctx.current.flush()
        
    def testkeypks(self):
        import datetime
        class Entity(object):
            pass
        Entity.mapper = mapper(Entity, table3)
        e = Entity()
        e.primary = 'pk1'
        e.secondary = 'pk2'
        e.assigned = datetime.date.today()
        e.data = 'some more data'
        ctx.current.flush()

    def testpksimmutable(self):
        class Entry(object):
            pass
        mapper(Entry, table)
        e = Entry()
        e.multi_id=5
        e.multi_rev=5
        e.name='somename'
        ctx.current.flush()
        e.multi_rev=6
        e.name = 'someothername'
        try:
            ctx.current.flush()
            assert False
        except exceptions.FlushError, fe:
            assert str(fe) == "Can't change the identity of instance Entry@%s in session (existing identity: (%s, (5, 5), None); new identity: (%s, (5, 6), None))" % (hex(id(e)), repr(e.__class__), repr(e.__class__))
            
            
class ForeignPKTest(UnitOfWorkTest):
    """tests mapper detection of the relationship direction when parent/child tables are joined on their
    primary keys"""
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global metadata, people, peoplesites
        metadata = MetaData(testbase.db)
        people = Table("people", metadata,
           Column('person', String(10), primary_key=True),
           Column('firstname', String(10)),
           Column('lastname', String(10)),
        )
        
        peoplesites = Table("peoplesites", metadata,
            Column('person', String(10), ForeignKey("people.person"),  
        primary_key=True),
            Column('site', String(10)),
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        UnitOfWorkTest.tearDownAll(self)
    def testbasic(self):
        class PersonSite(object):pass
        class Person(object):pass
        m1 = mapper(PersonSite, peoplesites)

        m2 = mapper(Person, people,
              properties = {
                      'sites' : relation(PersonSite), 
              },
            )

        assert list(m2.props['sites'].foreign_keys) == [peoplesites.c.person]
        p = Person()
        p.person = 'im the key'
        p.firstname = 'asdf'
        ps = PersonSite()
        ps.site = 'asdf'
        p.sites.append(ps)
        ctx.current.flush()
        assert people.count(people.c.person=='im the key').scalar() == peoplesites.count(peoplesites.c.person=='im the key').scalar() == 1

class PassiveDeletesTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global metadata, mytable,myothertable
        metadata = MetaData(testbase.db)
        mytable = Table('mytable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            mysql_engine='InnoDB'
            )

        myothertable = Table('myothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer),
            Column('data', String(30)),
            ForeignKeyConstraint(['parent_id'],['mytable.id'], ondelete="CASCADE"),
            mysql_engine='InnoDB'
            )

        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        UnitOfWorkTest.tearDownAll(self)

    @testbase.unsupported('sqlite')
    def testbasic(self):
        class MyClass(object):
            pass
        class MyOtherClass(object):
            pass
        
        mapper(MyOtherClass, myothertable)

        mapper(MyClass, mytable, properties={
            'children':relation(MyOtherClass, passive_deletes=True, cascade="all")
        })

        sess = ctx.current
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        sess.save(mc)
        sess.flush()
        sess.clear()
        assert myothertable.count().scalar() == 4
        mc = sess.query(MyClass).get(mc.id)
        sess.delete(mc)
        sess.flush()
        assert mytable.count().scalar() == 0
        assert myothertable.count().scalar() == 0
        

        
class DefaultTest(UnitOfWorkTest):
    """tests that when saving objects whose table contains DefaultGenerators, either python-side, preexec or database-side,
    the newly saved instances receive all the default values either through a post-fetch or getting the pre-exec'ed 
    defaults back from the engine."""
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        use_string_defaults = db.engine.__module__.endswith('postgres') or db.engine.__module__.endswith('oracle') or db.engine.__module__.endswith('sqlite')

        if use_string_defaults:
            hohotype = String(30)
            self.hohoval = "im hoho"
            self.althohoval = "im different hoho"
        else:
            hohotype = Integer
            self.hohoval = 9
            self.althohoval = 15
        self.table = Table('default_test', db,
        Column('id', Integer, Sequence("dt_seq", optional=True), primary_key=True),
        Column('hoho', hohotype, PassiveDefault(str(self.hohoval))),
        Column('counter', Integer, PassiveDefault("7")),
        Column('foober', String(30), default="im foober", onupdate="im the update")
        )
        self.table.create()
    def tearDownAll(self):
        self.table.drop()
        UnitOfWorkTest.tearDownAll(self)
    def setUp(self):
        self.table = Table('default_test', db)
    def testinsert(self):
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho(hoho=self.althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=self.althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober='im the new foober')
        ctx.current.flush()
        self.assert_(h1.hoho==self.althohoval)
        self.assert_(h3.hoho==self.althohoval)
        self.assert_(h2.hoho==h4.hoho==h5.hoho==self.hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        self.assert_(h5.foober=='im the new foober')
        ctx.current.clear()
        l = Query(Hoho).select()
        (h1, h2, h3, h4, h5) = l
        self.assert_(h1.hoho==self.althohoval)
        self.assert_(h3.hoho==self.althohoval)
        self.assert_(h2.hoho==h4.hoho==h5.hoho==self.hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        self.assert_(h5.foober=='im the new foober')
    
    def testinsertnopostfetch(self):
        # populates the PassiveDefaults explicitly so there is no "post-update"
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho(hoho="15", counter="15")
        ctx.current.flush()
        self.assert_(h1.hoho=="15")
        self.assert_(h1.counter=="15")
        self.assert_(h1.foober=="im foober")
        
    def testupdate(self):
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho()
        ctx.current.flush()
        self.assert_(h1.foober == 'im foober')
        h1.counter = 19
        ctx.current.flush()
        self.assert_(h1.foober == 'im the update')

class OneToManyTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        tables.create()
    def tearDownAll(self):
        tables.drop()
        UnitOfWorkTest.tearDownAll(self)
    def tearDown(self):
        tables.delete()
        UnitOfWorkTest.tearDown(self)

    def testonetomany_1(self):
        """test basic save of one to many."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u = User()
        u.user_name = 'one2manytester'
        u.addresses = []
        a = Address()
        a.email_address = 'one2many@test.org'
        u.addresses.append(a)
        a2 = Address()
        a2.email_address = 'lala@test.org'
        u.addresses.append(a2)
        self.echo( repr(u.addresses))
        self.echo( repr(u.addresses.added_items()))
        ctx.current.flush()

        usertable = users.select(users.c.user_id.in_(u.user_id)).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.user_id, 'one2manytester'])
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id), order_by=[addresses.c.email_address]).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [a2.address_id, u.user_id, 'lala@test.org'])
        self.assertEqual(addresstable[1].values(), [a.address_id, u.user_id, 'one2many@test.org'])

        userid = u.user_id
        addressid = a2.address_id

        a2.email_address = 'somethingnew@foo.com'

        ctx.current.flush()

        addresstable = addresses.select(addresses.c.address_id == addressid).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [addressid, userid, 'somethingnew@foo.com'])
        self.assert_(u.user_id == userid and a2.address_id == addressid)

    def testonetomany_2(self):
        """digs deeper into modifying the child items of an object to insure the correct
        updates take place"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u1.addresses = []
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1.addresses.append(a1)
        u2 = User()
        u2.user_name = 'user2'
        u2.addresses = []
        a2 = Address()
        a2.email_address = 'emailaddress2'
        u2.addresses.append(a2)

        a3 = Address()
        a3.email_address = 'emailaddress3'

        ctx.current.flush()

        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.user_name = 'user2modified'
        u1.addresses.append(a3)
        del u1.addresses[0]
        self.assert_sql(db, lambda: ctx.current.flush(), 
                [
                    (
                        "UPDATE users SET user_name=:user_name WHERE users.user_id = :users_user_id",
                        {'users_user_id': u2.user_id, 'user_name': 'user2modified'}
                    ),
                    ("UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        {'user_id': None, 'email_addresses_address_id': a1.address_id}
                    ),
                    (
                        "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        {'user_id': u1.user_id, 'email_addresses_address_id': a3.address_id}
                    ),
                ])

    def testchildmove(self):
        """tests moving a child from one parent to the other, then deleting the first parent, properly
        updates the child with the new parent.  this tests the 'trackparent' option in the attributes module."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u2 = User()
        u2.user_name = 'user2'
        a = Address()
        a.email_address = 'address1'
        u1.addresses.append(a)
        ctx.current.flush()
        del u1.addresses[0]
        u2.addresses.append(a)
        ctx.current.delete(u1)
        ctx.current.flush()
        ctx.current.clear()
        u2 = ctx.current.get(User, u2.user_id)
        assert len(u2.addresses) == 1

    def testchildmove_2(self):
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u2 = User()
        u2.user_name = 'user2'
        a = Address()
        a.email_address = 'address1'
        u1.addresses.append(a)
        ctx.current.flush()
        del u1.addresses[0]
        u2.addresses.append(a)
        ctx.current.flush()
        ctx.current.clear()
        u2 = ctx.current.get(User, u2.user_id)
        assert len(u2.addresses) == 1

    def testo2mdeleteparent(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False, private = False)
        ))
        u = User()
        a = Address()
        u.user_name = 'one2onetester'
        u.address = a
        u.address.email_address = 'myonlyaddress@foo.com'
        ctx.current.flush()
        ctx.current.delete(u)
        ctx.current.flush()
        self.assert_(a.address_id is not None and a.user_id is None and not ctx.current.identity_map.has_key(u._instance_key) and ctx.current.identity_map.has_key(a._instance_key))

    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False)
        ))
        u = User()
        u.user_name = 'one2onetester'
        u.address = Address()
        u.address.email_address = 'myonlyaddress@foo.com'
        ctx.current.flush()
        u.user_name = 'imnew'
        ctx.current.flush()
        u.address.email_address = 'imnew@foo.com'
        ctx.current.flush()

    def testbidirectional(self):
        m1 = mapper(User, users)

        m2 = mapper(Address, addresses, properties = dict(
            user = relation(m1, lazy = False, backref='addresses')
        ))


        u = User()
        print repr(u.addresses)
        u.user_name = 'test'
        a = Address()
        a.email_address = 'testaddress'
        a.user = u
        ctx.current.flush()
        print repr(u.addresses)
        x = False
        try:
            u.addresses.append('hi')
            x = True
        except:
            pass

        if x:
            self.assert_(False, "User addresses element should be scalar based")

        ctx.current.delete(u)
        ctx.current.flush()
    
    def testdoublerelation(self):
        m2 = mapper(Address, addresses)
        m = mapper(User, users, properties={
            'boston_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Address.c.email_address.like('%boston%'))),
            'newyork_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Address.c.email_address.like('%newyork%'))),
        })
        u = User()
        a = Address()
        a.email_address = 'foo@boston.com'
        b = Address()
        b.email_address = 'bar@newyork.com'

        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)
        ctx.current.flush()

class SaveTest(UnitOfWorkTest):

    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        tables.create()
    def tearDownAll(self):
        tables.drop()
        UnitOfWorkTest.tearDownAll(self)
        
    def setUp(self):
        keywords.insert().execute(
            dict(name='blue'),
            dict(name='red'),
            dict(name='green'),
            dict(name='big'),
            dict(name='small'),
            dict(name='round'),
            dict(name='square')
        )

    def tearDown(self):
        tables.delete()
        UnitOfWorkTest.tearDown(self)

    def testbasic(self):
        # save two users
        u = User()
        u.user_name = 'savetester'
        m = mapper(User, users)
        u2 = User()
        u2.user_name = 'savetester2'

        ctx.current.save(u)
        
        ctx.current.flush([u])
        ctx.current.flush()

        # assert the first one retreives the same from the identity map
        nu = ctx.current.get(m, u.user_id)
        self.echo( "U: " + repr(u) + "NU: " + repr(nu))
        self.assert_(u is nu)
        
        # clear out the identity map, so next get forces a SELECT
        ctx.current.clear()

        # check it again, identity should be different but ids the same
        nu = ctx.current.get(m, u.user_id)
        self.assert_(u is not nu and u.user_id == nu.user_id and nu.user_name == 'savetester')

        # change first users name and save
        ctx.current.update(u)
        u.user_name = 'modifiedname'
        assert u in ctx.current.dirty
        ctx.current.flush()

        # select both
        #ctx.current.clear()
        userlist = Query(m).select(users.c.user_id.in_(u.user_id, u2.user_id), order_by=[users.c.user_name])
        print repr(u.user_id), repr(userlist[0].user_id), repr(userlist[0].user_name)
        self.assert_(u.user_id == userlist[0].user_id and userlist[0].user_name == 'modifiedname')
        self.assert_(u2.user_id == userlist[1].user_id and userlist[1].user_name == 'savetester2')

    def testlazyattrcommit(self):
        """tests that when a lazy-loaded list is unloaded, and a commit occurs, that the
        'passive' call on that list does not blow away its value"""
        m1 = mapper(User, users, properties = {
            'addresses': relation(mapper(Address, addresses))
        })
        
        u = User()
        u.addresses.append(Address())
        u.addresses.append(Address())
        u.addresses.append(Address())
        u.addresses.append(Address())
        ctx.current.flush()
        ctx.current.clear()
        ulist = ctx.current.query(m1).select()
        u1 = ulist[0]
        u1.user_name = 'newname'
        ctx.current.flush()
        self.assert_(len(u1.addresses) == 4)
        
    def testinherits(self):
        m1 = mapper(User, users)
        
        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and joins on the user_id column
        AddressUser.mapper = mapper(
                AddressUser,
                addresses, inherits=m1
                )
        
        au = AddressUser()
        ctx.current.flush()
        ctx.current.clear()
        l = ctx.current.query(AddressUser).selectone()
        self.assert_(l.user_id == au.user_id and l.address_id == au.address_id)
    
    def testdeferred(self):
        """test that a deferred load within a flush() doesnt screw up the connection"""
        mapper(User, users, properties={
            'user_name':deferred(users.c.user_name)
        })
        u = User()
        u.user_id=42
        ctx.current.flush()
    
    def test_dont_update_blanks(self):
        mapper(User, users)
        u = User()
        u.user_name = ""
        ctx.current.flush()
        ctx.current.clear()
        u = ctx.current.query(User).get(u.user_id)
        u.user_name = ""
        def go():
            ctx.current.flush()
        self.assert_sql_count(db, go, 0)

    def testmultitable(self):
        """tests a save of an object where each instance spans two tables. also tests
        redefinition of the keynames for the column properties."""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, 
            properties = dict(
                email = addresses.c.email_address, 
                foo_id = [users.c.user_id, addresses.c.user_id],
                )
            )
            
        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'

        ctx.current.flush()
        id = m.identity(u)
        print id

        ctx.current.clear()
        
        u = ctx.current.get(User, id)
        assert u.user_name == 'multitester'
        
        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'multitester'])
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'multi@test.org'])

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        ctx.current.flush()

        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'imnew'])
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'lala@hey.com'])

        ctx.current.clear()
        u = ctx.current.get(User, id)
        assert u.user_name == 'imnew'
    
    def testhistoryget(self):
        """tests that the history properly lazy-fetches data when it wasnt otherwise loaded"""
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all, delete-orphan")
        })
        mapper(Address, addresses)
        
        u = User()
        u.addresses.append(Address())
        u.addresses.append(Address())
        ctx.current.flush()
        ctx.current.clear()
        u = ctx.current.query(User).get(u.user_id)
        ctx.current.delete(u)
        ctx.current.flush()
        assert users.count().scalar() == 0
        assert addresses.count().scalar() == 0
        
            
    
    def testbatchmode(self):
        class TestExtension(MapperExtension):
            def before_insert(self, mapper, connection, instance):
                self.current_instance = instance
            def after_insert(self, mapper, connection, instance):
                assert instance is self.current_instance
        m = mapper(User, users, extension=TestExtension(), batch=False)
        u1 = User()
        u1.username = 'user1'
        u2 = User()
        u2.username = 'user2'
        ctx.current.flush()
        
        clear_mappers()
        
        m = mapper(User, users, extension=TestExtension())
        u1 = User()
        u1.username = 'user1'
        u2 = User()
        u2.username = 'user2'
        try:
            ctx.current.flush()
            assert False
        except AssertionError:
            assert True
        
    
class ManyToOneTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        tables.create()
    def tearDownAll(self):
        tables.drop()
        UnitOfWorkTest.tearDownAll(self)
    def tearDown(self):
        tables.delete()
        UnitOfWorkTest.tearDown(self)
    
    def testm2oonetoone(self):
        # TODO: put assertion in here !!!
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
            {'user_name' : 'n4knd' , 'email_address' : 'asf3@bar.org'},
            {'user_name' : 'v88f4' , 'email_address' : 'adsd5@llala.net'},
            {'user_name' : 'asdf8d' , 'email_address' : 'theater@foo.com'}
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)
            
        ctx.current.flush()
        objects[2].email_address = 'imnew@foo.bar'
        objects[3].user = User()
        objects[3].user.user_name = 'imnewlyadded'
        self.assert_sql(db, lambda: ctx.current.flush(), [
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'imnewlyadded'}
                ),
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}
                ,
                
                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}
                },
                
        ],
        with_sequences=[
                (
                    "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                    lambda ctx:{'user_name': 'imnewlyadded', 'user_id':ctx.last_inserted_ids()[0]}
                ),
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}
                ,
                
                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}
                },
                
        ])
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.user_id, addresses.c.address_id==a.address_id)).execute()
        assert l.fetchone().values() == [a.user.user_id, 'asdf8d', a.address_id, a.user_id, 'theater@foo.com']


    def testmanytoone_1(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'
        
        a1.user = u1
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        assert a1.user is u1

        a1.user = None
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        assert a1.user is None

    def testmanytoone_2(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        a2 = Address()
        a2.email_address = 'emailaddress2'
        u1 = User()
        u1.user_name='user1'

        a1.user = u1
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        a2 = ctx.current.query(Address).get(a2.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        assert a1.user is u1
        a1.user = None
        a2.user = u1
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        a2 = ctx.current.query(Address).get(a2.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        assert a1.user is None
        assert a2.user is u1

    def testmanytoone_3(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'
        u2 = User()
        u2.user_name='user2'

        a1.user = u1
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        u2 = ctx.current.query(User).get(u2.user_id)
        assert a1.user is u1
        
        a1.user = u2
        ctx.current.flush()
        ctx.current.clear()
        a1 = ctx.current.query(Address).get(a1.address_id)
        u1 = ctx.current.query(User).get(u1.user_id)
        u2 = ctx.current.query(User).get(u2.user_id)
        assert a1.user is u2

    def testbidirectional_noload(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=None)
        })
        mapper(Address, addresses)

        sess = ctx.current

        # try it on unsaved objects
        u1 = User()
        a1 = Address()
        a1.user = u1
        sess.save(u1)
        sess.flush()
        sess.clear()

        a1 = sess.query(Address).get(a1.address_id)

        a1.user = None
        sess.flush()
        sess.clear()
        assert sess.query(Address).get(a1.address_id).user is None
        assert sess.query(User).get(u1.user_id).addresses == []

        
class ManyToManyTest(UnitOfWorkTest):
    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        tables.create()
    def tearDownAll(self):
        tables.drop()
        UnitOfWorkTest.tearDownAll(self)
    def tearDown(self):
        tables.delete()
        UnitOfWorkTest.tearDown(self)

    def testmanytomany(self):
        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, items, properties = dict(
                keywords = relation(keywordmapper, itemkeywords, lazy = False, order_by=keywords.c.name),
            ))

        data = [Item,
            {'item_name': 'mm_item1', 'keywords' : (Keyword,[{'name': 'big'},{'name': 'green'}, {'name': 'purple'},{'name': 'round'}])},
            {'item_name': 'mm_item2', 'keywords' : (Keyword,[{'name':'blue'}, {'name':'imnew'},{'name':'round'}, {'name':'small'}])},
            {'item_name': 'mm_item3', 'keywords' : (Keyword,[])},
            {'item_name': 'mm_item4', 'keywords' : (Keyword,[{'name':'big'}, {'name':'blue'},])},
            {'item_name': 'mm_item5', 'keywords' : (Keyword,[{'name':'big'},{'name':'exacting'},{'name':'green'}])},
            {'item_name': 'mm_item6', 'keywords' : (Keyword,[{'name':'red'},{'name':'round'},{'name':'small'}])},
        ]
        objects = []
        for elem in data[1:]:
            item = Item()
            objects.append(item)
            item.item_name = elem['item_name']
            item.keywords = []
            if len(elem['keywords'][1]):
                klist = ctx.current.query(keywordmapper).select(keywords.c.name.in_(*[e['name'] for e in elem['keywords'][1]]))
            else:
                klist = []
            khash = {}
            for k in klist:
                khash[k.name] = k
            for kname in [e['name'] for e in elem['keywords'][1]]:
                try:
                    k = khash[kname]
                except KeyError:
                    k = Keyword()
                    k.name = kname
                item.keywords.append(k)

        ctx.current.flush()
        
        l = ctx.current.query(m).select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name])
        self.assert_result(l, *data)

        objects[4].item_name = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql(db, lambda:ctx.current.flush(), [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                {'item_name': 'item4updated', 'items_item_id': objects[4].item_id}
            ,
                "INSERT INTO keywords (name) VALUES (:name)":
                {'name': 'yellow'}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda ctx: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ],
        
        with_sequences = [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                {'item_name': 'item4updated', 'items_item_id': objects[4].item_id}
            ,
                "INSERT INTO keywords (keyword_id, name) VALUES (:keyword_id, :name)":
                lambda ctx: {'name': 'yellow', 'keyword_id':ctx.last_inserted_ids()[0]}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda ctx: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ]
        )
        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].keyword_id
        del objects[5].keywords[1]
        self.assert_sql(db, lambda:ctx.current.flush(), [
                (
                    "DELETE FROM itemkeywords WHERE itemkeywords.item_id = :item_id AND itemkeywords.keyword_id = :keyword_id",
                    [{'item_id': objects[5].item_id, 'keyword_id': dkid}]
                ),
                (   
                    "INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
                    lambda ctx: [{'item_id': objects[2].item_id, 'keyword_id': k.keyword_id}]
                )
        ])
        
        ctx.current.delete(objects[3])
        ctx.current.flush()

    def testmanytomanyremove(self):
        """tests that setting a list-based attribute to '[]' properly affects the history and allows
        the many-to-many rows to be deleted"""
        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, orderitems, properties = dict(
                keywords = relation(keywordmapper, itemkeywords, lazy = False),
            ))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)
        ctx.current.flush()
        
        assert itemkeywords.count().scalar() == 2
        i.keywords = []
        ctx.current.flush()
        assert itemkeywords.count().scalar() == 0

    def testscalar(self):
        """test that dependency.py doesnt try to delete an m2m relation referencing None."""
        
        mapper(Keyword, keywords)

        mapper(Item, orderitems, properties = dict(
                keyword = relation(Keyword, secondary=itemkeywords, uselist=False),
            ))
        
        i = Item()
        ctx.current.flush()
        ctx.current.delete(i)
        ctx.current.flush()
        
        

    def testmanytomanyupdate(self):
        """tests some history operations on a many to many"""
        class Keyword(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.__class__ == Keyword and other.name == self.name
            def __repr__(self):
                return "Keyword(%s, %s)" % (getattr(self, 'keyword_id', 'None'), self.name)
                
        mapper(Keyword, keywords)
        mapper(Item, orderitems, properties = dict(
                keywords = relation(Keyword, secondary=itemkeywords, lazy=False, order_by=keywords.c.name),
            ))

        (k1, k2, k3) = (Keyword('keyword 1'), Keyword('keyword 2'), Keyword('keyword 3'))
        item = Item()
        item.item_name = 'item 1'
        item.keywords.append(k1)
        item.keywords.append(k2)
        item.keywords.append(k3)
        ctx.current.flush()
        
        item.keywords = []
        item.keywords.append(k1)
        item.keywords.append(k2)
        ctx.current.flush()
        
        ctx.current.clear()
        item = ctx.current.query(Item).get(item.item_id)
        print [k1, k2]
        assert item.keywords == [k1, k2]
        
    def testassociation(self):
        """basic test of an association object"""
        class IKAssociation(object):
            def __repr__(self):
                return "\nIKAssociation " + repr(self.item_id) + " " + repr(self.keyword)

        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        # note that we are breaking a rule here, making a second mapper(Keyword, keywords)
        # the reorganization of mapper construction affected this, but was fixed again
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(IKAssociation, itemkeywords, properties = dict(
                    keyword = relation(mapper(Keyword, keywords, non_primary=True), lazy = False, uselist = False, order_by=keywords.c.name)
                ), primary_key = [itemkeywords.c.item_id, itemkeywords.c.keyword_id]),
                lazy = False)
            ))

        data = [Item,
            {'item_name': 'a_item1', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'green'})}, 
                                                        {'keyword' : (Keyword, {'name': 'purple'})},
                                                        {'keyword' : (Keyword, {'name': 'round'})}
                                                    ]
                                                 ) 
            },
            {'item_name': 'a_item2', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'huge'})},
                                                        {'keyword' : (Keyword, {'name': 'violet'})}, 
                                                        {'keyword' : (Keyword, {'name': 'yellow'})}
                                                    ]
                                                 ) 
            },
            {'item_name': 'a_item3', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'blue'})}, 
                                                    ]
                                                 ) 
            }
        ]
        for elem in data[1:]:
            item = Item()
            item.item_name = elem['item_name']
            item.keywords = []
            for kname in [e['keyword'][1]['name'] for e in elem['keywords'][1]]:
                try:
                    k = Query(keywordmapper).select(keywords.c.name == kname)[0]
                except IndexError:
                    k = Keyword()
                    k.name= kname
                ik = IKAssociation()
                ik.keyword = k
                item.keywords.append(ik)

        ctx.current.flush()
        ctx.current.clear()
        l = Query(m).select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name])
        self.assert_result(l, *data)
    
class SaveTest2(UnitOfWorkTest):

    def setUp(self):
        ctx.current.clear()
        clear_mappers()
        global meta, users, addresses
        meta = MetaData(db)
        users = Table('users', meta,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(20)),
        )

        addresses = Table('email_addresses', meta,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('rel_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
        )
        meta.create_all()

    def tearDown(self):
        meta.drop_all()
        UnitOfWorkTest.tearDown(self)
    
    def testbackwardsnonmatch(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)
        self.assert_sql(db, lambda: ctx.current.flush(), [
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'thesub'}
                ),
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'assdkfj'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 1, 'email_address': 'bar@foo.com'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 2, 'email_address': 'thesdf@asdf.com'}
                )
                ],
                
                with_sequences = [
                        (
                            "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda ctx: {'user_name': 'thesub', 'user_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda ctx: {'user_name': 'assdkfj', 'user_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda ctx:{'rel_user_id': 1, 'email_address': 'bar@foo.com', 'address_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda ctx:{'rel_user_id': 2, 'email_address': 'thesdf@asdf.com', 'address_id':ctx.last_inserted_ids()[0]}
                        )
                        ]
        )

class SaveTest3(UnitOfWorkTest):

    def setUpAll(self):
        UnitOfWorkTest.setUpAll(self)
        global metadata, t1, t2, t3
        metadata = testbase.metadata
        t1 = Table('items', metadata,
            Column('item_id', INT, Sequence('items_id_seq', optional=True), primary_key = True),
            Column('item_name', VARCHAR(50)),
        )

        t3 = Table('keywords', metadata,
            Column('keyword_id', Integer, Sequence('keyword_id_seq', optional=True), primary_key = True),
            Column('name', VARCHAR(50)),

        )
        t2 = Table('assoc', metadata,
            Column('item_id', INT, ForeignKey("items")),
            Column('keyword_id', INT, ForeignKey("keywords")),
            Column('foo', Boolean, default=True)
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        UnitOfWorkTest.tearDownAll(self)

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testmanytomanyxtracolremove(self):
        """test that a many-to-many on a table that has an extra column can properly delete rows from the table
        without referencing the extra column"""
        mapper(Keyword, t3)

        mapper(Item, t1, properties = dict(
                keywords = relation(Keyword, secondary=t2, lazy = False),
            ))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)
        ctx.current.flush()

        assert t2.count().scalar() == 2
        i.keywords = []
        print i.keywords
        ctx.current.flush()
        assert t2.count().scalar() == 0


if __name__ == "__main__":
    testbase.main()        
