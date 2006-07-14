from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase
from sqlalchemy.orm.mapper import global_extensions
from sqlalchemy.ext.sessioncontext import SessionContext
import sqlalchemy.ext.assignmapper as assignmapper
from tables import *
import tables

class SessionTest(AssertMixin):
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

class HistoryTest(SessionTest):
    def setUpAll(self):
        SessionTest.setUpAll(self)
        db.echo = False
        users.create()
        addresses.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        addresses.drop()
        users.drop()
        db.echo = testbase.echo
        SessionTest.tearDownAll(self)
        
    def testattr(self):
        """tests the rolling back of scalar and list attributes.  this kind of thing
        should be tested mostly in attributes.py which tests independently of the ORM 
        objects, but I think here we are going for
        the Mapper not interfering with it."""
        m = mapper(User, users, properties = dict(addresses = relation(mapper(Address, addresses))))
        u = User()
        u.user_id = 7
        u.user_name = 'afdas'
        u.addresses.append(Address())
        u.addresses[0].email_address = 'hi'
        u.addresses.append(Address())
        u.addresses[1].email_address = 'there'
        data = [User,
            {'user_name' : 'afdas',
             'addresses' : (Address, [{'email_address':'hi'}, {'email_address':'there'}])
            },
        ]
        self.assert_result([u], data[0], *data[1:])

        self.echo(repr(u.addresses))
        ctx.current.uow.rollback_object(u)
        
        # depending on the setting in the get() method of InstrumentedAttribute in attributes.py, 
        # username is either None or is a non-present attribute.
        assert u.user_name is None
        #assert not hasattr(u, 'user_name')
        
        assert u.addresses == []

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

class CustomAttrTest(SessionTest):
    def setUpAll(self):
        SessionTest.setUpAll(self)
        global sometable, metadata, someothertable
        metadata = BoundMetaData(testbase.db)
        sometable = Table('sometable', metadata,
            Column('col1',Integer, primary_key=True))
        someothertable = Table('someothertable', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('scol1', Integer, ForeignKey(sometable.c.col1)),
            Column('data', String(20))
        )
    def testbasic(self):
        class MyList(list):
            pass
        class Foo(object):
            bars = MyList
        class Bar(object):
            pass
        mapper(Foo, sometable, properties={
            'bars':relation(Bar)
        })
        mapper(Bar, someothertable)
        f = Foo()
        assert isinstance(f.bars.data, MyList)
    def tearDownAll(self):
        SessionTest.tearDownAll(self)
            
class VersioningTest(SessionTest):
    def setUpAll(self):
        SessionTest.setUpAll(self)
        ctx.current.clear()
        global version_table
        version_table = Table('version_test', db,
        Column('id', Integer, Sequence('version_test_seq'), primary_key=True ),
        Column('version_id', Integer, nullable=False),
        Column('value', String(40), nullable=False)
        ).create()
    def tearDownAll(self):
        version_table.drop()
        SessionTest.tearDownAll(self)
    def tearDown(self):
        version_table.delete().execute()
        SessionTest.tearDown(self)
    
    @testbase.unsupported('mysql', 'mssql')
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
        f1_s = Foo.mapper.using(s2).get(f1.id)
        f1_s.value='f1rev3'
        s2.flush()

        f1.value='f1rev3mine'
        success = False
        try:
            # a concurrent session has modified this, should throw
            # an exception
            s.flush()
        except exceptions.SQLAlchemyError, e:
            #print e
            success = True
        assert success
        
        s.clear()
        f1 = s.query(Foo).get(f1.id)
        f2 = s.query(Foo).get(f2.id)
        
        f1_s.value='f1rev4'
        s2.flush()
    
        s.delete(f1, f2)
        success = False
        try:
            s.flush()
        except exceptions.SQLAlchemyError, e:
            #print e
            success = True
        assert success
        
class UnicodeTest(SessionTest):
    def setUpAll(self):
        SessionTest.setUpAll(self)
        global metadata, uni_table, uni_table2
        metadata = BoundMetaData(testbase.db)
        uni_table = Table('uni_test', metadata,
            Column('id',  Integer, primary_key=True),
            Column('txt', Unicode(50), unique=True))
        uni_table2 = Table('uni2', metadata,
            Column('id',  Integer, primary_key=True),
            Column('txt', Unicode(50), ForeignKey(uni_table.c.txt)))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        SessionTest.tearDownAll(self)
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
        
class PKTest(SessionTest):
    @testbase.unsupported('mssql')
    def setUpAll(self):
        SessionTest.setUpAll(self)
        #db.echo = False
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
        db.echo = testbase.echo
    @testbase.unsupported('mssql')
    def tearDownAll(self):
        db.echo = False
        table.drop()
        table2.drop()
        table3.drop()
        db.echo = testbase.echo
        SessionTest.tearDownAll(self)
        
    # not support on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys    
    @testbase.unsupported('sqlite', 'mssql')
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
        e2 = Entry.mapper.get((e.multi_id, 2))
        self.assert_(e is not e2 and e._instance_key == e2._instance_key)
        
    # this one works with sqlite since we are manually setting up pk values
    @testbase.unsupported('mssql')
    def testmanualpk(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table2)
        e = Entry()
        e.pk_col_1 = 'pk1'
        e.pk_col_2 = 'pk1_related'
        e.data = 'im the data'
        ctx.current.flush()
        
    @testbase.unsupported('mssql')
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

class ForeignPKTest(SessionTest):
    """tests mapper detection of the relationship direction when parent/child tables are joined on their
    primary keys"""
    def setUpAll(self):
        SessionTest.setUpAll(self)
        global metadata, people, peoplesites
        metadata = BoundMetaData(testbase.db)
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
        SessionTest.tearDownAll(self)
    def testbasic(self):
        class PersonSite(object):pass
        class Person(object):pass
        m1 = mapper(PersonSite, peoplesites)

        m2 = mapper(Person, people,
              properties = {
                      'sites' : relation(PersonSite), 
              },
            )

        assert list(m2.props['sites'].foreignkey) == [peoplesites.c.person]
        p = Person()
        p.person = 'im the key'
        p.firstname = 'asdf'
        ps = PersonSite()
        ps.site = 'asdf'
        p.sites.append(ps)
        ctx.current.flush()
        assert people.count(people.c.person=='im the key').scalar() == peoplesites.count(peoplesites.c.person=='im the key').scalar() == 1
        
class PrivateAttrTest(SessionTest):
    """tests various things to do with private=True mappers"""
    def setUpAll(self):
        SessionTest.setUpAll(self)
        global a_table, b_table
        a_table = Table('a',testbase.db,
            Column('a_id', Integer, Sequence('next_a_id'), primary_key=True),
            Column('data', String(10)),
            ).create()
    
        b_table = Table('b',testbase.db,
            Column('b_id',Integer,Sequence('next_b_id'),primary_key=True),
            Column('a_id',Integer,ForeignKey('a.a_id')),
            Column('data',String(10))).create()
    def tearDownAll(self):
        b_table.drop()
        a_table.drop()
        SessionTest.tearDownAll(self)
    
    def testsinglecommit(self):
        """tests that a commit of a single object deletes private relationships"""
        class A(object):pass
        class B(object):pass
    
        assign_mapper(B,b_table)
        assign_mapper(A,a_table,properties= {'bs' : relation(B.mapper,private=True)})
    
        # create some objects
        a = A(data='a1')
        a.bs = []
    
        # add a 'B' instance
        b1 = B(data='1111')
        a.bs.append(b1)
    
        # add another one
        b2 = B(data='2222')
        a.bs.append(b2)
    
        # inserts both A and Bs
        ctx.current.flush([a])
    
        ctx.current.delete(a)
        print ctx.current.deleted
        ctx.current.flush([a])
#        ctx.current.flush()
        
        assert b_table.count().scalar() == 0

    def testswitchparent(self):
        """tests that you can switch the parent of an object in a backref scenario"""
        class A(object):pass
        class B(object):pass
    
        assign_mapper(B,b_table)
        assign_mapper(A,a_table,properties= {
            'bs' : relation (B.mapper,private=True, backref='a')}
        )
        a1 = A(data='testa1')
        a2 = A(data='testa2')
        b = B(data='testb')
        b.a = a1
        ctx.current.flush()
        ctx.current.clear()
        sess = ctx.current
        a1 = A.mapper.get(a1.a_id)
        a2 = A.mapper.get(a2.a_id)
        assert a1.bs[0].a is a1
        b = a1.bs[0]
        b.a = a2
        assert b not in sess.deleted
        ctx.current.flush()
        assert b in sess.identity_map.values()
                
class DefaultTest(SessionTest):
    """tests that when saving objects whose table contains DefaultGenerators, either python-side, preexec or database-side,
    the newly saved instances receive all the default values either through a post-fetch or getting the pre-exec'ed 
    defaults back from the engine."""
    def setUpAll(self):
        SessionTest.setUpAll(self)
        #db.echo = 'debug'
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
        SessionTest.tearDownAll(self)
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
        l = Hoho.mapper.select()
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
        
class SaveTest(SessionTest):

    def setUpAll(self):
        SessionTest.setUpAll(self)
        db.echo = False
        tables.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        tables.drop()
        db.echo = testbase.echo
        SessionTest.tearDownAll(self)
        
    def setUp(self):
        db.echo = False
        keywords.insert().execute(
            dict(name='blue'),
            dict(name='red'),
            dict(name='green'),
            dict(name='big'),
            dict(name='small'),
            dict(name='round'),
            dict(name='square')
        )
        db.echo = testbase.echo

    def tearDown(self):
        db.echo = False
        tables.delete()
        db.echo = testbase.echo

        #self.assert_(len(ctx.current.new) == 0)
        #self.assert_(len(ctx.current.dirty) == 0)
        SessionTest.tearDown(self)

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
        nu = m.get(u.user_id)
        self.echo( "U: " + repr(u) + "NU: " + repr(nu))
        self.assert_(u is nu)
        
        # clear out the identity map, so next get forces a SELECT
        ctx.current.clear()

        # check it again, identity should be different but ids the same
        nu = m.get(u.user_id)
        self.assert_(u is not nu and u.user_id == nu.user_id and nu.user_name == 'savetester')

        # change first users name and save
        ctx.current.update(u)
        u.user_name = 'modifiedname'
        assert u in ctx.current.dirty
        ctx.current.flush()

        # select both
        #ctx.current.clear()
        userlist = m.select(users.c.user_id.in_(u.user_id, u2.user_id), order_by=[users.c.user_name])
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
        ulist = m1.select()
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
        l = AddressUser.mapper.selectone()
        self.assert_(l.user_id == au.user_id and l.address_id == au.address_id)
    
    def testdeferred(self):
        """test that a deferred load within a flush() doesnt screw up the connection"""
        mapper(User, users, properties={
            'user_name':deferred(users.c.user_name)
        })
        u = User()
        u.user_id=42
        ctx.current.flush()
        
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
        
        u = m.get(id)
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
        u = m.get(id)
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
        
            
    def testm2mmultitable(self):
        # many-to-many join on an association table
        j = join(users, userkeywords, 
                users.c.user_id==userkeywords.c.user_id).join(keywords, 
                   userkeywords.c.keyword_id==keywords.c.keyword_id)

        # a class 
        class KeywordUser(object):
            pass

        # map to it - the identity of a KeywordUser object will be
        # (user_id, keyword_id) since those are the primary keys involved
        m = mapper(KeywordUser, j, properties={
            'user_id':[users.c.user_id, userkeywords.c.user_id],
            'keyword_id':[userkeywords.c.keyword_id, keywords.c.keyword_id],
            'keyword_name':keywords.c.name
            
        })
        
        k = KeywordUser()
        k.user_name = 'keyworduser'
        k.keyword_name = 'a keyword'
        ctx.current.flush()
        print m.instance_key(k)
        id = (k.user_id, k.keyword_id)
        ctx.current.clear()
        k = ctx.current.query(KeywordUser).get(id)
        assert k.user_name == 'keyworduser'
        assert k.keyword_name == 'a keyword'
        
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

    def testchildmove(self):
        """tests moving a child from one parent to the other, then deleting the first parent, properly
        updates the child with the new parent.  this tests the 'trackparent' option in the attributes module."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True, private = False)
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
        u2 = m.get(u2.user_id)
        assert len(u2.addresses) == 1
    
    def testdelete(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False, private = False)
        ))
        u = User()
        a = Address()
        u.user_name = 'one2onetester'
        u.address = a
        u.address.email_address = 'myonlyaddress@foo.com'
        ctx.current.flush()
        self.echo("\n\n\n")
        ctx.current.delete(u)
        ctx.current.flush()
        self.assert_(a.address_id is not None and a.user_id is None and not ctx.current.identity_map.has_key(u._instance_key) and ctx.current.identity_map.has_key(a._instance_key))
        
    def testbackwardsonetoone(self):
        # test 'backwards'
#        m = mapper(Address, addresses, properties = dict(
#            user = relation(User, users, foreignkey = addresses.c.user_id, primaryjoin = users.c.user_id == addresses.c.user_id, lazy = True, uselist = False)
#        ))
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
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.address_id, addresses.c.address_id==a.address_id)).execute()
        self.echo( repr(l.fetchone().values()))

        

    def testonetomany(self):
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

    def testmapperswitch(self):
        """test that, if we change mappers, the new one gets used fully. """
        users.insert().execute(
            dict(user_id = 7, user_name = 'jack'),
            dict(user_id = 8, user_name = 'ed'),
            dict(user_id = 9, user_name = 'fred')
        )

        # mapper with just users table
        assign_mapper(User, users)
        User.mapper.select()
        oldmapper = User.mapper
        # now a mapper with the users table plus a relation to the addresses
        assign_mapper(User, users, is_primary=True, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        self.assert_(oldmapper is not User.mapper)
        u = User.mapper.select()
        u[0].addresses.append(Address())
        u[0].addresses[0].email_address='hi'
        
        # insure that upon commit, the new mapper with the address relation is used
        ctx.current.echo_uow=True
        self.assert_sql(db, lambda: ctx.current.flush(), 
                [
                    (
                    "INSERT INTO email_addresses (user_id, email_address) VALUES (:user_id, :email_address)",
                    {'email_address': 'hi', 'user_id': 7}
                    ),
                ],
                with_sequences=[
                    (
                    "INSERT INTO email_addresses (address_id, user_id, email_address) VALUES (:address_id, :user_id, :email_address)",
                    lambda ctx:{'email_address': 'hi', 'user_id': 7, 'address_id':ctx.last_inserted_ids()[0]}
                    ),
                ]
        )

    def testchildmanipulations(self):
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
        
        self.echo("\n\n\n")
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

    def testbackwardsmanipulations(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'
        
        a1.user = u1
        ctx.current.flush()

        self.echo("\n\n\n")
        ctx.current.delete(u1)
        a1.user = None
        ctx.current.flush()

    def testmanytomany(self):
        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, items, properties = dict(
                keywords = relation(keywordmapper, itemkeywords, lazy = False),
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
                klist = keywordmapper.select(keywords.c.name.in_(*[e['name'] for e in elem['keywords'][1]]))
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
        
        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
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


    def testmanytomanyupdate(self):
        """tests some history operations on a many to many"""
        class Keyword(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name
            def __repr__(self):
                return "Keyword(%s, %s)" % (getattr(self, 'keyword_id', 'None'), self.name)
                
        mapper(Keyword, keywords)
        mapper(Item, orderitems, properties = dict(
                keywords = relation(Keyword, secondary=itemkeywords, lazy=False),
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
                    keyword = relation(mapper(Keyword, keywords, non_primary=True), lazy = False, uselist = False)
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
                    k = keywordmapper.select(keywords.c.name == kname)[0]
                except IndexError:
                    k = Keyword()
                    k.name= kname
                ik = IKAssociation()
                ik.keyword = k
                item.keywords.append(ik)

        ctx.current.flush()
        ctx.current.clear()
        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
        self.assert_result(l, *data)

    def testbidirectional(self):
        m1 = mapper(User, users, is_primary=True)
        
        m2 = mapper(Address, addresses, properties = dict(
            user = relation(m1, lazy = False, backref='addresses')
        ), is_primary=True)
        
 
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
    
class SaveTest2(SessionTest):

    def setUp(self):
        db.echo = False
        ctx.current.clear()
        clear_mappers()
        self.users = Table('users', db,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(20)),
            redefine=True
        )

        self.addresses = Table('email_addresses', db,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('rel_user_id', Integer, ForeignKey(self.users.c.user_id)),
            Column('email_address', String(20)),
            redefine=True
        )
        x = sql.Join(self.users, self.addresses)
#        raise repr(self.users) + repr(self.users.primary_key)
#        raise repr(self.addresses) + repr(self.addresses.foreign_keys)
        self.users.create()
        self.addresses.create()
        db.echo = testbase.echo

    def tearDown(self):
        db.echo = False
        self.addresses.drop()
        self.users.drop()
        db.echo = testbase.echo
        SessionTest.tearDown(self)
    
    def testbackwardsnonmatch(self):
        m = mapper(Address, self.addresses, properties = dict(
            user = relation(mapper(User, self.users), lazy = True, uselist = False)
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

class SaveTest3(SessionTest):

    def setUpAll(self):
        SessionTest.setUpAll(self)
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
        SessionTest.tearDownAll(self)

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testmanytomanyxtracolremove(self):
        """tests that a many-to-many on a table that has an extra column can properly delete rows from the table
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
