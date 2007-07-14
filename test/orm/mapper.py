from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
import sqlalchemy.exceptions as exceptions
from sqlalchemy.ext.sessioncontext import SessionContext
from tables import *
import tables

"""tests general mapper operations with an emphasis on selecting/loading"""

class MapperSuperTest(AssertMixin):
    def setUpAll(self):
        tables.create()
        tables.data()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass
    
class MapperTest(MapperSuperTest):
    # TODO: MapperTest has grown much larger than it originally was and needs
    # to be broken up among various functions, including querying, session operations,
    # mapper configurational issues
    def testget(self):
        s = create_session()
        mapper(User, users)
        self.assert_(s.get(User, 19) is None)
        u = s.get(User, 7)
        u2 = s.get(User, 7)
        self.assert_(u is u2)
        s.clear()
        u2 = s.get(User, 7)
        self.assert_(u is not u2)

    def testunicodeget(self):
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail 
        on postgres, mysql and oracle unless it is converted to an encoded string"""
        metadata = MetaData(db)
        table = Table('foo', metadata, 
            Column('id', Unicode(10), primary_key=True),
            Column('data', Unicode(40)))
        try:
            table.create()
            class LocalFoo(object):pass
            mapper(LocalFoo, table)
            crit = 'petit voix m\xe2\x80\x99a '.decode('utf-8')
            print repr(crit)
            create_session().query(LocalFoo).get(crit)
        finally:
            table.drop()

    def testpropconflict(self):
        """test that a backref created against an existing mapper with a property name
        conflict raises a decent error message"""
        mapper(Address, addresses)
        mapper(User, users,
            properties={
            'addresses':relation(Address, backref='email_address')
        })
        try:
            class_mapper(Address)
            class_mapper(User)
            assert False
        except exceptions.ArgumentError:
            pass

    def testbadcascade(self):
        mapper(Address, addresses)
        try:
            mapper(User, users, properties={'addresses':relation(Address, cascade="fake, all, delete-orphan")})
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Invalid cascade option 'fake'"
        
    def testcolumnprefix(self):
        mapper(User, users, column_prefix='_', properties={
            'user_name':synonym('_user_name')
        })
        s = create_session()
        u = s.get(User, 7)
        assert u._user_name=='jack'
    	assert u._user_id ==7
        assert not hasattr(u, 'user_name')
        u2 = s.query(User).filter_by(user_name='jack').one()
        assert u is u2
          
    def testrefresh(self):
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        s = create_session()
        u = s.get(User, 7)
        u.user_name = 'foo'
        a = Address()
        import sqlalchemy.orm.session
        assert sqlalchemy.orm.session.object_session(a) is None
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.refresh(u)
        
        # its refreshed, so not dirty
        self.assert_(u not in s.dirty)
        
        # username is back to the DB
        self.assert_(u.user_name == 'jack')
        
        self.assert_(a not in u.addresses)
        
        u.user_name = 'foo'
        u.addresses.append(a)
        # now its dirty
        self.assert_(u in s.dirty)
        self.assert_(u.user_name == 'foo')
        self.assert_(a in u.addresses)
        s.expire(u)

        # get the attribute, it refreshes
        self.assert_(u.user_name == 'jack')
        self.assert_(a not in u.addresses)
    
    def testexpirecascade(self):
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), cascade="all, refresh-expire")})
        s = create_session()
        u = s.get(User, 8)
        u.addresses[0].email_address = 'someotheraddress'
        s.expire(u)
        assert u.addresses[0].email_address == 'ed@wood.com'
        
    def testrefreshwitheager(self):
        """test that a refresh/expire operation loads rows properly and sends correct "isnew" state to eager loaders"""
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), lazy=False)})
        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.refresh(u)
        assert len(u.addresses) == 3

        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.expire(u)
        assert len(u.addresses) == 3
        
    def testbadconstructor(self):
        """test that if the construction of a mapped class fails, the instnace does not get placed in the session"""
        class Foo(object):
            def __init__(self, one, two):
                pass
        mapper(Foo, users)
        sess = create_session()
        try:
            Foo('one', _sa_session=sess)
            assert False
        except:
            assert len(list(sess)) == 0
        try:
            Foo('one')
            assert False
        except TypeError, e:
            pass

    def testconstructorexceptions(self):
        """test that exceptions raised in the mapped class are not masked by sa decorations""" 
        ex = AssertionError('oops')
        sess = create_session()

        class Foo(object):
            def __init__(self):
                raise ex
        mapper(Foo, users)
        
        try:
            Foo()
            assert False
        except Exception, e:
            assert e is ex

        def bad_expunge(foo):
            raise Exception("this exception should be stated as a warning")

        import warnings
        warnings.filterwarnings("always", r".*this exception should be stated as a warning")
        sess.expunge = bad_expunge
        try:
            Foo(_sa_session=sess)
            assert False
        except Exception, e:
            assert e is ex
        
    def testrefresh_lazy(self):
        """test that when a lazy loader is set as a trigger on an object's attribute (at the attribute level, not the class level), a refresh() operation doesnt fire the lazy loader or create any problems"""
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        q2 = s.query(User).options(lazyload('addresses'))
        u = q2.selectfirst(users.c.user_id==8)
        def go():
            s.refresh(u)
        self.assert_sql_count(db, go, 1)

    def testexpire(self):
        """test the expire function"""
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), lazy=False)})
        u = s.get(User, 7)
        assert(len(u.addresses) == 1)
        u.user_name = 'foo'
        del u.addresses[0]
        s.expire(u)
        # test plain expire
        self.assert_(u.user_name =='jack')
        self.assert_(len(u.addresses) == 1)
        
        # we're changing the database here, so if this test fails in the middle,
        # it'll screw up the other tests which are hardcoded to 7/'jack'
        u.user_name = 'foo'
        s.flush()
        # change the value in the DB
        users.update(users.c.user_id==7, values=dict(user_name='jack')).execute()
        s.expire(u)
        # object isnt refreshed yet, using dict to bypass trigger
        self.assert_(u.__dict__.get('user_name') != 'jack')
        # do a select
        s.query(User).select()
        # test that it refreshed
        self.assert_(u.__dict__['user_name'] == 'jack')
        
        # object should be back to normal now, 
        # this should *not* produce a SELECT statement (not tested here though....)
        self.assert_(u.user_name =='jack')
        
    def testrefresh2(self):
        """test a hang condition that was occuring on expire/refresh"""
        s = create_session()
        mapper(Address, addresses)

        mapper(User, users, properties = dict(addresses=relation(Address,private=True,lazy=False)) )

        u=User()
        u.user_name='Justin'
        a = Address()
        a.address_id=17  # to work around the hardcoded IDs in this test suite....
        u.addresses.append(a)
        s.flush()
        s.clear()
        u = s.query(User).selectfirst()
        print u.user_name

        #ok so far
        s.expire(u)        #hangs when
        print u.user_name #this line runs

        s.refresh(u) #hangs
        
    def testprops(self):
        """tests the various attributes of the properties attached to classes"""
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        }).compile()
        self.assert_(User.addresses.property is m.props['addresses'])
        
    def testquery(self):
        """test a basic Query.select() operation."""
        mapper(User, users)
        l = create_session().query(User).select()
        self.assert_result(l, User, *user_result)
        l = create_session().query(User).select(users.c.user_name.endswith('ed'))
        self.assert_result(l, User, *user_result[1:3])

    def testrecursiveselectby(self):
        """test that no endless loop occurs when traversing for select_by"""
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders), backref='user'),
            'addresses':relation(mapper(Address, addresses), backref='user'),
        })
        q = create_session().query(m)
        q.select_by(email_address='foo')

    def testmappingtojoin(self):
        """test mapping to a join"""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, primary_key=[users.c.user_id])
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *user_result[0:2])
        
    def testmappingtoouterjoin(self):
        """test mapping to an outer join, with a composite primary key that allows nulls"""
        result = [
        {'user_id' : 7, 'address_id' : 1},
        {'user_id' : 8, 'address_id' : 2},
        {'user_id' : 8, 'address_id' : 3},
        {'user_id' : 8, 'address_id' : 4},
        {'user_id' : 9, 'address_id':None}
        ]
        
        j = join(users, addresses, isouter=True)
        m = mapper(User, j, allow_null_pks=True, primary_key=[users.c.user_id, addresses.c.address_id])
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *result)

    def testwithparent(self):
        """test the with_parent()) method and one-to-many relationships"""
        
        m = mapper(User, users, properties={
            'user_name_syn':synonym('user_name'),
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems)),
                'items_syn':synonym('items')
            })),
            'orders_syn':synonym('orders')
        })

        sess = create_session()
        q = sess.query(m)
        u1 = q.get_by(user_name='jack')

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).list()
        self.assert_result(o, Order, *user_all_result[0]['orders'][1])

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property='orders').list()
        self.assert_result(o, Order, *user_all_result[0]['orders'][1])

        # test static method
        o = Query.query_from_parent(u1, property='orders', session=sess).list()
        self.assert_result(o, Order, *user_all_result[0]['orders'][1])

        # test generative criterion
        o = sess.query(Order).with_parent(u1).select_by(orders.c.order_id>2)
        self.assert_result(o, Order, *user_all_result[0]['orders'][1][1:])

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Could not locate a property which relates instances of class 'Item' to instances of class 'User'"


        for nameprop, orderprop in (
            ('user_name', 'orders'),
            ('user_name_syn', 'orders'),
            ('user_name', 'orders_syn'),
            ('user_name_syn', 'orders_syn'),
        ):
            sess = create_session()
            q = sess.query(User)

            u1 = q.filter_by(**{nameprop:'jack'}).one()

            o = sess.query(Order).with_parent(u1, property=orderprop).list()
            self.assert_result(o, Order, *user_all_result[0]['orders'][1])
            
    def testwithparentm2m(self):
        """test the with_parent() method and many-to-many relationships"""
        
        m = mapper(Item, orderitems, properties = {
                'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
        })
        sess = create_session()
        i1 = sess.query(Item).get_by(item_id=2)
        k = sess.query(Keyword).with_parent(i1)
        self.assert_result(k, Keyword, *item_keyword_result[1]['keywords'][1])

        
    def test_join(self):
        """test functions derived from Query's _join_to function."""
        
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems)),
                'items_syn':synonym('items')
            })),
            
            'orders_syn':synonym('orders'),
        })

        sess = create_session()
        q = sess.query(m)

        for j in (
            ['orders', 'items'],
            ['orders', 'items_syn'],
            ['orders_syn', 'items'],
            ['orders_syn', 'items_syn'],
        ):
            for q in (
                q.filter(orderitems.c.item_name=='item 4').join(j),
                q.filter(orderitems.c.item_name=='item 4').join(j[-1]),
                q.filter(orderitems.c.item_name=='item 4').filter(q.join_via(j)),
                q.filter(orderitems.c.item_name=='item 4').filter(q.join_to(j[-1])),
            ):
                l = q.all()
                self.assert_result(l, User, user_result[0])

        l = q.select_by(item_name='item 4')
        self.assert_result(l, User, user_result[0])

        l = q.filter(orderitems.c.item_name=='item 4').join('item_name').list()
        self.assert_result(l, User, user_result[0])

        l = q.filter(orderitems.c.item_name=='item 4').join('items').list()
        self.assert_result(l, User, user_result[0])

        # test comparing to an object instance
        item = sess.query(Item).get_by(item_name='item 4')

        l = sess.query(Order).select_by(items=item)
        self.assert_result(l, Order, user_all_result[0]['orders'][1][1])

        l = q.select_by(items=item)
        self.assert_result(l, User, user_result[0])
        
        # TODO: this works differently from:
        #q = sess.query(User).join(['orders', 'items']).select_by(order_id=3)
        # because select_by() doesnt respect query._joinpoint, whereas filter_by does
        q = sess.query(User).join(['orders', 'items']).filter_by(order_id=3).list()
        self.assert_result(l, User, user_result[0])
        
        try:
            # this should raise AttributeError
            l = q.select_by(items=5)
            assert False
        except AttributeError:
            assert True
        
    def testautojoinm2m(self):
        """test functions derived from Query's _join_to function."""
        
        m = mapper(Order, orders, properties = {
            'items' : relation(mapper(Item, orderitems, properties = {
                'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
            }))
        })
        
        sess = create_session()
        q = sess.query(m)

        l = q.filter(keywords.c.name=='square').join(['items', 'keywords']).list()
        self.assert_result(l, Order, order_result[1])

        # test comparing to an object instance
        item = sess.query(Item).selectfirst()
        l = sess.query(Item).select_by(keywords=item.keywords[0])
        assert item == l[0]
        
    def testcustomjoin(self):
        """test that the from_obj parameter to query.select() can be used
        to totally replace the FROM parameters of the generated query."""

        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems))
            }))
        })

        q = create_session().query(m)
        l = q.select((orderitems.c.item_name=='item 4'), from_obj=[users.join(orders).join(orderitems)])
        self.assert_result(l, User, user_result[0])
            
    def testorderby(self):
        """test ordering at the mapper and query level"""
        # TODO: make a unit test out of these various combinations
#        m = mapper(User, users, order_by=desc(users.c.user_name))
        mapper(User, users, order_by=None)
#        mapper(User, users)
        
#        l = create_session().query(User).select(order_by=[desc(users.c.user_name), asc(users.c.user_id)])
        l = create_session().query(User).select()
#        l = create_session().query(User).select(order_by=[])
#        l = create_session().query(User).select(order_by=None)
        
        
    @testbase.unsupported('firebird') 
    def testfunction(self):
        """test mapping to a SELECT statement that has functions in it."""
        s = select([users, (users.c.user_id * 2).label('concat'), func.count(addresses.c.address_id).label('count')],
        users.c.user_id==addresses.c.user_id, group_by=[c for c in users.c]).alias('myselect')
        mapper(User, s)
        sess = create_session()
        l = sess.query(User).select()
        for u in l:
            print "User", u.user_id, u.user_name, u.concat, u.count
        assert l[0].concat == l[0].user_id * 2 == 14
        assert l[1].concat == l[1].user_id * 2 == 16

    def testexternalcolumns(self):
        """test creating mappings that reference external columns or functions"""

        f = (users.c.user_id *2).label('concat')
        try:
            mapper(User, users, properties={
                'concat': f,
            })
            class_mapper(User)
        except exceptions.ArgumentError, e:
            assert str(e) == "Column '%s' is not represented in mapper's table.  Use the `column_property()` function to force this column to be mapped as a read-only attribute." % str(f)
        clear_mappers()
        
        mapper(User, users, properties={
            'concat': column_property(f),
            'count': column_property(select([func.count(addresses.c.address_id)], users.c.user_id==addresses.c.user_id, scalar=True).label('count'))
        })
        
        sess = create_session()
        l = sess.query(User).select()
        for u in l:
            print "User", u.user_id, u.user_name, u.concat, u.count
        assert l[0].concat == l[0].user_id * 2 == 14
        assert l[1].concat == l[1].user_id * 2 == 16
        
        ### eager loads, not really working across all DBs, no column aliasing in place so
        # results still wont be good for larger situations
        clear_mappers()
        mapper(Address, addresses, properties={
            'user':relation(User, lazy=False)
        })    
        
        mapper(User, users, properties={
            'concat': column_property(f),
        })

        for x in range(0, 2):
            sess.clear()
            l = sess.query(Address).select()
            for a in l:
                print "User", a.user.user_id, a.user.user_name, a.user.concat
            assert l[0].user.concat == l[0].user.user_id * 2 == 14
            assert l[1].user.concat == l[1].user.user_id * 2 == 16
            
        
    @testbase.unsupported('firebird') 
    def testcount(self):
        """test the count function on Query.
        
        (why doesnt this work on firebird?)"""
        mapper(User, users)
        q = create_session().query(User)
        self.assert_(q.count()==3)
        self.assert_(q.count(users.c.user_id.in_(8,9))==2)
        self.assert_(q.count_by(user_name='fred')==1)

    def testmanytomany_count(self):
        mapper(Item, orderitems, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        q = create_session().query(Item)
        assert q.join('keywords').distinct().count(Keyword.c.name=="red") == 2

    def testoverride(self):
        # assert that overriding a column raises an error
        try:
            m = mapper(User, users, properties = {
                    'user_name' : relation(mapper(Address, addresses)),
                }).compile()
            self.assert_(False, "should have raised ArgumentError")
        except exceptions.ArgumentError, e:
            self.assert_(True)
        
        clear_mappers()
        # assert that allow_column_override cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses))
            }, allow_column_override=True)
            
        clear_mappers()
        # assert that the column being named else where also cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses)),
                'foo' : users.c.user_name,
            })

    def testsynonym(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            uname = synonym('user_name', proxy=True),
            adlist = synonym('addresses', proxy=True),
            adname = synonym('addresses')
        ))
        
        u = sess.query(User).get_by(uname='jack')
        self.assert_result(u.adlist, Address, *(user_address_result[0]['addresses'][1]))

        assert hasattr(u, 'adlist')
        assert not hasattr(u, 'adname')
        
        addr = sess.query(Address).get_by(address_id=user_address_result[0]['addresses'][1][0]['address_id'])
        u = sess.query(User).get_by(adname=addr)
        u2 = sess.query(User).get_by(adlist=addr)
        assert u is u2
        
        assert u not in sess.dirty
        u.uname = "some user name"
        assert u.uname == "some user name"
        assert u.user_name == "some user name"
        assert u in sess.dirty

    def testsynonymoptions(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            adlist = synonym('addresses', proxy=True)
        ))
        
        def go():
            u = sess.query(User).options(eagerload('adlist')).get_by(user_name='jack')
            self.assert_result(u.adlist, Address, *(user_address_result[0]['addresses'][1]))
        self.assert_sql_count(db, go, 1)
        
    def testextensionoptions(self):
        sess  = create_session()
        class ext1(MapperExtension):
            def populate_instance(self, mapper, selectcontext, row, instance, identitykey, isnew):
                """test options at the Mapper._instance level"""
                instance.TEST = "hello world"
                return EXT_PASS
        mapper(User, users, extension=ext1(), properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })
        class testext(MapperExtension):
            def select_by(self, *args, **kwargs):
                """test options at the Query level"""
                return "HI"
            def populate_instance(self, mapper, selectcontext, row, instance, identitykey, isnew):
                """test options at the Mapper._instance level"""
                instance.TEST_2 = "also hello world"
                return EXT_PASS
        l = sess.query(User).options(extension(testext())).select_by(x=5)
        assert l == "HI"
        l = sess.query(User).options(extension(testext())).get(7)
        assert l.user_id == 7
        assert l.TEST == "hello world"
        assert l.TEST_2 == "also hello world"
        assert not hasattr(l.addresses[0], 'TEST')
        assert not hasattr(l.addresses[0], 'TEST2')
        
    def testeageroptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        l = sess.query(User).options(eagerload('addresses')).select()

        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 0)

    def testeageroptionswithlimit(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u = sess.query(User).options(eagerload('addresses')).get_by(user_id=8)

        def go():
            assert u.user_id == 8
            assert len(u.addresses) == 3
        self.assert_sql_count(db, go, 0)

        sess.clear()
        
        # test that eager loading doesnt modify parent mapper
        def go():
            u = sess.query(User).get_by(user_id=8)
            assert u.user_id == 8
            assert len(u.addresses) == 3
        assert "tbl_row_count" not in self.capture_sql(db, go)
        
    def testlazyoptionswithlimit(self):
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        u = sess.query(User).options(lazyload('addresses')).get_by(user_id=8)

        def go():
            assert u.user_id == 8
            assert len(u.addresses) == 3
        self.assert_sql_count(db, go, 1)

    def testeagerdegrade(self):
        """tests that an eager relation automatically degrades to a lazy relation if eager columns are not available"""
        sess = create_session()
        usermapper = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        )).compile()

        # first test straight eager load, 1 statement
        def go():
            l = sess.query(usermapper).select()
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 1)

        sess.clear()
        
        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        # (previous users in session fell out of scope and were removed from session's identity map)
        def go():
            r = users.select().execute()
            l = usermapper.instances(r, sess)
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 4)
        
        clear_mappers()

        sess.clear()
        
        # test with a deeper set of eager loads.  when we first load the three
        # users, they will have no addresses or orders.  the number of lazy loads when
        # traversing the whole thing will be three for the addresses and three for the 
        # orders.
        # (previous users in session fell out of scope and were removed from session's identity map)
        usermapper = mapper(User, users,
            properties = {
                'addresses':relation(mapper(Address, addresses), lazy=False),
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords, lazy=False)
                    }), lazy=False)
                }), lazy=False)
            })

        sess.clear()

        # first test straight eager load, 1 statement
        def go():
            l = sess.query(usermapper).select()
            self.assert_result(l, User, *user_all_result)
        self.assert_sql_count(db, go, 1)

        sess.clear()
        
        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        def go():
            r = users.select().execute()
            l = usermapper.instances(r, sess)
            self.assert_result(l, User, *user_all_result)
        self.assert_sql_count(db, go, 7)
        
        
    def testlazyoptions(self):
        """tests that an eager relation can be upgraded to a lazy relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        l = sess.query(User).options(lazyload('addresses')).select()
        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 3)

    def testlatecompile(self):
        """tests mappers compiling late in the game"""
        
        mapper(User, users, properties = {'orders': relation(Order)})
        mapper(Item, orderitems, properties={'keywords':relation(Keyword, secondary=itemkeywords)})
        mapper(Keyword, keywords)
        mapper(Order, orders, properties={'items':relation(Item)})
        
        sess = create_session()
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(db, go, 3)

    def testdeepoptions(self):
        mapper(User, users,
            properties = {
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
                    }))
                }))
            })
            
        sess = create_session()
        
        # eagerload nothing.
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(db, go, 3)
        sess.clear()
        
        
        print "-------MARK----------"
        # eagerload orders, orders.items, orders.items.keywords
        q2 = sess.query(User).options(eagerload('orders'), eagerload('orders.items'), eagerload('orders.items.keywords'))
        u = q2.select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        print "-------MARK2----------"
        self.assert_sql_count(db, go, 0)

        sess.clear()

        # same thing, with separate options calls
        q2 = sess.query(User).options(eagerload('orders')).options(eagerload('orders.items')).options(eagerload('orders.items.keywords'))
        u = q2.select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        print "-------MARK3----------"
        self.assert_sql_count(db, go, 0)
        print "-------MARK4----------"

        sess.clear()
        
        # eagerload "keywords" on items.  it will lazy load "orders", then lazy load
        # the "items" on the order, but on "items" it will eager load the "keywords"
        print "-------MARK5----------"
        q3 = sess.query(User).options(eagerload('orders.items.keywords'))
        u = q3.select()
        self.assert_sql_count(db, go, 2)
            
    
class DeferredTest(MapperSuperTest):

    def testbasic(self):
        """tests a basic "deferred" load"""
        
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        o = Order()
        self.assert_(o.description is None)

        q = create_session().query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.description

        orderby = str(orders.default_order_by()[0].compile(engine=db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.description AS orders_description FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])

    def testunsaved(self):
        """test that deferred loading doesnt kick in when just PK cols are set"""
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        sess = create_session()
        o = Order()
        sess.save(o)
        o.order_id = 7
        def go():
            o.description = "some description"
        self.assert_sql_count(testbase.db, go, 0)

    def testunsavedgroup(self):
        """test that deferred loading doesnt kick in when just PK cols are set"""
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })

        sess = create_session()
        o = Order()
        sess.save(o)
        o.order_id = 7
        def go():
            o.description = "some description"
        self.assert_sql_count(testbase.db, go, 0)
        
    def testsave(self):
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        sess = create_session()
        q = sess.query(m)
        l = q.select()
        o2 = l[2]
        o2.isopen = 1
        sess.flush()
        
    def testgroup(self):
        """tests deferred load with a group"""
        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })
        q = create_session().query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.opened, o2.description, o2.userident
            assert o2.opened == 1
            assert o2.userident == 7
            assert o2.description == 'order 3'
        orderby = str(orders.default_order_by()[0].compile(db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        
    def testoptions(self):
        """tests using options on a mapper to create deferred and undeferred columns"""
        m = mapper(Order, orders)
        sess = create_session()
        q = sess.query(m)
        q2 = q.options(defer('user_id'))
        def go():
            l = q2.select()
            print l[2].user_id
            
        orderby = str(orders.default_order_by()[0].compile(db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        sess.clear()
        q3 = q2.options(undefer('user_id'))
        def go():
            l = q3.select()
            print l[3].user_id
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
        ])

        
    def testdeepoptions(self):
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems, properties={
                    'item_name':deferred(orderitems.c.item_name)
                }))
            }))
        })
        sess = create_session()
        q = sess.query(m)
        l = q.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(db, go, 1)
        self.assert_(item.item_name == 'item 4')
        sess.clear()
        q2 = q.options(undefer('orders.items.item_name'))
        l = q2.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(db, go, 0)
        self.assert_(item.item_name == 'item 4')
    

class NoLoadTest(MapperSuperTest):
    def testbasic(self):
        """tests a basic one-to-many lazy load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=None)
        ))
        q = create_session().query(m)
        l = [None]
        def go():
            x = q.select(users.c.user_id == 7)
            x[0].addresses
            l[0] = x
        self.assert_sql_count(testbase.db, go, 1)
            
        self.assert_result(l[0], User,
            {'user_id' : 7, 'addresses' : (Address, [])},
            )
    def testoptions(self):
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=None)
        ))
        q = create_session().query(m).options(lazyload('addresses'))
        l = [None]
        def go():
            x = q.select(users.c.user_id == 7)
            x[0].addresses
            l[0] = x
        self.assert_sql_count(testbase.db, go, 2)
            
        self.assert_result(l[0], User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            )




if __name__ == "__main__":    
    testbase.main()
