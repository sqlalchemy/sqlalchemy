import testbase
import operator
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *

class QueryTest(FixtureTest):
    keep_mappers = True
    keep_data = True
    
    def setUpAll(self):
        super(QueryTest, self).setUpAll()
        install_fixture_data()
        self.setup_mappers()
    
    def tearDownAll(self):
        clear_mappers()
        super(QueryTest, self).tearDownAll()
          
    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, order_by=items.c.id),  #m2m
            'address':relation(Address),  # m2o
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

class UnicodeSchemaTest(QueryTest):
    keep_mappers = False
    
    def setup_mappers(self):
        pass
        
    def define_tables(self, metadata):
        super(UnicodeSchemaTest, self).define_tables(metadata)
        global uni_meta, uni_users
        uni_meta = MetaData()
        uni_users = Table(u'users', uni_meta,
            Column(u'id', Integer, primary_key=True),
            Column(u'name', String(30), nullable=False))
            
    def test_get(self):
        mapper(User, uni_users)
        assert User(id=7) == create_session(bind=testbase.db).query(User).get(7)
        
class GetTest(QueryTest):
    def test_get(self):
        s = create_session()
        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2
        s.clear()
        u2 = s.query(User).get(7)
        assert u is not u2

    def test_unique_param_names(self):
        class SomeUser(object):
            pass
        s = users.select(users.c.id!=12).alias('users')
        m = mapper(SomeUser, s)
        print s.primary_key
        print m.primary_key
        assert s.primary_key == m.primary_key
        
        row = s.select(use_labels=True).execute().fetchone()
        print row[s.primary_key[0]]
         
        sess = create_session()
        assert sess.query(SomeUser).get(7).name == 'jack'

    def test_load(self):
        s = create_session()
        
        try:
            assert s.query(User).load(19) is None
            assert False
        except exceptions.InvalidRequestError:
            assert True
            
        u = s.query(User).load(7)
        u2 = s.query(User).load(7)
        assert u is u2
        s.clear()
        u2 = s.query(User).load(7)
        assert u is not u2
        
        u2.name = 'some name'
        a = Address(email_address='some other name')
        u2.addresses.append(a)
        assert u2 in s.dirty
        assert a in u2.addresses
        
        s.query(User).load(7)
        assert u2 not in s.dirty
        assert u2.name =='jack'
        assert a not in u2.addresses
        
    @testing.exclude('mysql', '<', (5, 0))  # fixme
    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail 
        on postgres, mysql and oracle unless it is converted to an encoded string"""
        
        table = Table('unicode_data', users.metadata, 
            Column('id', Unicode(40), primary_key=True),
            Column('data', Unicode(40)))
        table.create()
        ustring = 'petit voix m\xe2\x80\x99a '.decode('utf-8')
        table.insert().execute(id=ustring, data=ustring)
        class LocalFoo(Base):pass
        mapper(LocalFoo, table)
        assert create_session().query(LocalFoo).get(ustring) == LocalFoo(id=ustring, data=ustring)

    def test_populate_existing(self):
        s = create_session()

        userlist = s.query(User).all()

        u = userlist[0]
        u.name = 'foo'
        a = Address(name='ed')
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.query(User).populate_existing().all()

        self.assert_(u not in s.dirty)

        self.assert_(u.name == 'jack')

        self.assert_(a not in u.addresses)

        u.addresses[0].email_address = 'lala'
        u.orders[1].items[2].description = 'item 12'
        # test that lazy load doesnt change child items
        s.query(User).populate_existing().all()
        assert u.addresses[0].email_address == 'lala'
        assert u.orders[1].items[2].description == 'item 12'
        
        # eager load does
        s.query(User).options(eagerload('addresses'), eagerload_all('orders.items')).populate_existing().all()
        assert u.addresses[0].email_address == 'jack@bean.com'
        assert u.orders[1].items[2].description == 'item 5'
        
class OperatorTest(QueryTest):
    """test sql.Comparator implementation for MapperProperties"""
    
    def _test(self, clause, expected):
        c = str(clause.compile(dialect = default.DefaultDialect()))
        assert c == expected, "%s != %s" % (c, expected)
        
    def test_arithmetic(self):
        create_session().query(User)
        for (py_op, sql_op) in ((operator.add, '+'), (operator.mul, '*'),
                                (operator.sub, '-'), (operator.div, '/'),
                                ):
            for (lhs, rhs, res) in (
                (5, User.id, ':users_id %s users.id'),
                (5, literal(6), ':literal %s :literal_1'),
                (User.id, 5, 'users.id %s :users_id'),
                (User.id, literal('b'), 'users.id %s :literal'),
                (User.id, User.id, 'users.id %s users.id'),
                (literal(5), 'b', ':literal %s :literal_1'),
                (literal(5), User.id, ':literal %s users.id'),
                (literal(5), literal(6), ':literal %s :literal_1'),
                ):
                self._test(py_op(lhs, rhs), res % sql_op)

    def test_comparison(self):
        create_session().query(User)
        for (py_op, fwd_op, rev_op) in ((operator.lt, '<', '>'),
                                        (operator.gt, '>', '<'),
                                        (operator.eq, '=', '='),
                                        (operator.ne, '!=', '!='),
                                        (operator.le, '<=', '>='),
                                        (operator.ge, '>=', '<=')):
            for (lhs, rhs, l_sql, r_sql) in (
                ('a', User.id, ':users_id', 'users.id'),
                ('a', literal('b'), ':literal_1', ':literal'), # note swap!
                (User.id, 'b', 'users.id', ':users_id'),
                (User.id, literal('b'), 'users.id', ':literal'),
                (User.id, User.id, 'users.id', 'users.id'),
                (literal('a'), 'b', ':literal', ':literal_1'),
                (literal('a'), User.id, ':literal', 'users.id'),
                (literal('a'), literal('b'), ':literal', ':literal_1'),
                ):

                # the compiled clause should match either (e.g.):
                # 'a' < 'b' -or- 'b' > 'a'.
                compiled = str(py_op(lhs, rhs).compile(dialect=default.DefaultDialect()))
                fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
                rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

                self.assert_(compiled == fwd_sql or compiled == rev_sql,
                             "\n'" + compiled + "'\n does not match\n'" +
                             fwd_sql + "'\n or\n'" + rev_sql + "'")
    
    def test_in(self):
         self._test(User.id.in_(['a', 'b']),
                    "users.id IN (:users_id, :users_id_1)")

    def test_between(self):
        self._test(User.id.between('a', 'b'),
                   "users.id BETWEEN :users_id AND :users_id_1")

    def test_clauses(self):
        for (expr, compare) in (
            (func.max(User.id), "max(users.id)"),
            (User.id.desc(), "users.id DESC"),
            (between(5, User.id, Address.id), ":literal BETWEEN users.id AND addresses.id"),
            # this one would require adding compile() to InstrumentedScalarAttribute.  do we want this ?
            #(User.id, "users.id")
        ):
            c = expr.compile(dialect=default.DefaultDialect())
            assert str(c) == compare, "%s != %s" % (str(c), compare)
            
            
class CompileTest(QueryTest):
    def test_deferred(self):
        session = create_session()
        s = session.query(User).filter(and_(addresses.c.email_address == bindparam('emailad'), Address.user_id==User.id)).compile()
        
        l = session.query(User).instances(s.execute(emailad = 'jack@bean.com'))
        assert [User(id=7)] == l
    
class SliceTest(QueryTest):
    def test_first(self):
        assert  User(id=7) == create_session().query(User).first()
        
        assert create_session().query(User).filter(User.id==27).first() is None
        
        # more slice tests are available in test/orm/generative.py
        
class TextTest(QueryTest):
    def test_fulltext(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).from_statement("select * from users").all()

    def test_fragment(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (8, 9)").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter("id=9").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter(User.id==9).all()

    def test_binds(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (:id1, :id2)").params(id1=8, id2=9).all()
        
class FilterTest(QueryTest):
    def test_basic(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).all()

    def test_limit(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).limit(2).offset(1).all()

        assert [User(id=8), User(id=9)] == list(create_session().query(User)[1:3])

        assert User(id=8) == create_session().query(User)[1]
        
    def test_onefilter(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter(User.name.endswith('ed')).all()

    def test_contains(self):
        """test comparing a collection to an object instance."""
        
        sess = create_session()
        address = sess.query(Address).get(3)
        assert [User(id=8)] == sess.query(User).filter(User.addresses.contains(address)).all()

        try:
            sess.query(User).filter(User.addresses == address)
            assert False
        except exceptions.InvalidRequestError:
            assert True

        assert [User(id=10)] == sess.query(User).filter(User.addresses==None).all()

        try:
            assert [User(id=7), User(id=9), User(id=10)] == sess.query(User).filter(User.addresses!=address).all()
            assert False
        except exceptions.InvalidRequestError:
            assert True
            
        #assert [User(id=7), User(id=9), User(id=10)] == sess.query(User).filter(User.addresses!=address).all()
    
    def test_any(self):
        sess = create_session()

        assert [User(id=8), User(id=9)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'))).all()

        assert [User(id=8)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'), id=4)).all()

        assert [User(id=8)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'))).\
            filter(User.addresses.any(id=4)).all()

        assert [User(id=9)] == sess.query(User).filter(User.addresses.any(email_address='fred@fred.com')).all()
    
    def test_has(self):
        sess = create_session()
        assert [Address(id=5)] == sess.query(Address).filter(Address.user.has(name='fred')).all()
        
        assert [Address(id=2), Address(id=3), Address(id=4), Address(id=5)] == sess.query(Address).filter(Address.user.has(User.name.like('%ed%'))).all()
        
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter(Address.user.has(User.name.like('%ed%'), id=8)).all()
            
    def test_contains_m2m(self):
        sess = create_session()
        item = sess.query(Item).get(3)
        assert [Order(id=1), Order(id=2), Order(id=3)] == sess.query(Order).filter(Order.items.contains(item)).all()

        assert [Order(id=4), Order(id=5)] == sess.query(Order).filter(~Order.items.contains(item)).all()

    def test_comparison(self):
        """test scalar comparison to an object instance"""
        
        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter(Address.user==user).all()

        assert [Address(id=1), Address(id=5)] == sess.query(Address).filter(Address.user!=user).all()

class AggregateTest(QueryTest):
    def test_sum(self):
        sess = create_session()
        orders = sess.query(Order).filter(Order.id.in_([2, 3, 4]))
        assert orders.sum(Order.user_id * Order.address_id) == 79

    def test_apply(self):
        sess = create_session()
        assert sess.query(Order).apply_sum(Order.user_id * Order.address_id).filter(Order.id.in_([2, 3, 4])).one() == 79
        
                
class CountTest(QueryTest):
    def test_basic(self):
        assert 4 == create_session().query(User).count()

        assert 2 == create_session().query(User).filter(users.c.name.endswith('ed')).count()

class TextTest(QueryTest):
    def test_fulltext(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).from_statement("select * from users").all()

    def test_fragment(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (8, 9)").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter("id=9").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter(User.id==9).all()

    def test_binds(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (:id1, :id2)").params(id1=8, id2=9).all()
        
        
class ParentTest(QueryTest):
    def test_o2m(self):
        sess = create_session()
        q = sess.query(User)
        
        u1 = q.filter_by(name='jack').one()

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property='orders').all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test static method
        o = Query.query_from_parent(u1, property='orders', session=sess).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test generative criterion
        o = sess.query(Order).with_parent(u1).filter(orders.c.id>2).all()
        assert [Order(description="order 3"), Order(description="order 5")] == o

    def test_noparent(self):
        sess = create_session()
        q = sess.query(User)
        
        u1 = q.filter_by(name='jack').one()

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Could not locate a property which relates instances of class 'Item' to instances of class 'User'"

    def test_m2m(self):
        sess = create_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [Keyword(name='red'), Keyword(name='small'), Keyword(name='square')] == k
        
    
class JoinTest(QueryTest):

    def test_overlapping_paths(self):
        for aliased in (True,False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join(['orders', 'items'], aliased=aliased).filter_by(id=3).join(['orders','address'], aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_outerjoin(self):
        result = create_session().query(User).outerjoin(['orders', 'items']).filter_by(id=3).outerjoin(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result

    def test_reset_joinpoint(self):
        for aliased in (True, False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join(['orders', 'items'], aliased=aliased).filter_by(id=3).reset_joinpoint().join(['orders','address'], aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

            result = create_session().query(User).outerjoin(['orders', 'items'], aliased=aliased).filter_by(id=3).reset_joinpoint().outerjoin(['orders','address'], aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result
    
    def test_overlap_with_aliases(self):
        oalias = orders.alias('oalias')

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join(['orders', 'items']).all()
        assert [User(id=7, name='jack'), User(id=9, name='fred')] == result
        
        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join(['orders', 'items']).filter_by(id=4).all()
        assert [User(id=7, name='jack')] == result

    def test_aliased(self):
        """test automatic generation of aliased joins."""

        sess = create_session()

        # test a basic aliasized path
        q = sess.query(User).join('addresses', aliased=True).filter_by(email_address='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(Address.email_address=='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(or_(Address.email_address=='jack@bean.com', Address.email_address=='fred@fred.com'))
        assert [User(id=7), User(id=9)] == q.all()

        # test two aliasized paths, one to 'orders' and the other to 'orders','items'.
        # one row is returned because user 7 has order 3 and also has order 1 which has item 1
        # this tests a o2m join and a m2m join.
        q = sess.query(User).join('orders', aliased=True).filter(Order.description=="order 3").join(['orders', 'items'], aliased=True).filter(Item.description=="item 1")
        assert q.count() == 1
        assert [User(id=7)] == q.all()

        # test the control version - same joins but not aliased.  rows are not returned because order 3 does not have item 1
        # addtionally by placing this test after the previous one, test that the "aliasing" step does not corrupt the
        # join clauses that are cached by the relationship.
        q = sess.query(User).join('orders').filter(Order.description=="order 3").join(['orders', 'items']).filter(Order.description=="item 1")
        assert [] == q.all()
        assert q.count() == 0
        
        q = sess.query(User).join('orders', aliased=True).filter(Order.items.any(Item.description=='item 4'))
        assert [User(id=7)] == q.all()

    def test_aliased_add_entity(self):
        """test the usage of aliased joins with add_entity()"""
        sess = create_session()
        q = sess.query(User).join('orders', aliased=True, id='order1').filter(Order.description=="order 3").join(['orders', 'items'], aliased=True, id='item1').filter(Item.description=="item 1")

        try:
            q.add_entity(Order, id='fakeid').compile()
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Query has no alias identified by 'fakeid'"

        try:
            q.add_entity(Order, id='fakeid').instances(None)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Query has no alias identified by 'fakeid'"

        q = q.add_entity(Order, id='order1').add_entity(Item, id='item1')
        assert q.count() == 1
        assert [(User(id=7), Order(description='order 3'), Item(description='item 1'))] == q.all()

        q = sess.query(User).add_entity(Order).join('orders', aliased=True).filter(Order.description=="order 3").join('orders', aliased=True).filter(Order.description=='order 4')
        try:
            q.compile()
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Ambiguous join for entity 'Mapper|Order|orders'; specify id=<someid> to query.join()/query.add_entity()"

class MultiplePathTest(ORMTest):
    def define_tables(self, metadata):
        global t1, t2, t1t2_1, t1t2_2
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30))
            )
        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30))
            )

        t1t2_1 = Table('t1t2_1', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

        t1t2_2 = Table('t1t2_2', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

    def test_basic(self):
        class T1(object):pass
        class T2(object):pass

        mapper(T1, t1, properties={
            't2s_1':relation(T2, secondary=t1t2_1),
            't2s_2':relation(T2, secondary=t1t2_2),
        })
        mapper(T2, t2)

        try:
            create_session().query(T1).join('t2s_1').filter(t2.c.id==5).reset_joinpoint().join('t2s_2')
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't join to property 't2s_2'; a path to this table along a different secondary table already exists.  Use the `alias=True` argument to `join()`."

        create_session().query(T1).join('t2s_1', aliased=True).filter(t2.c.id==5).reset_joinpoint().join('t2s_2').all()
        create_session().query(T1).join('t2s_1').filter(t2.c.id==5).reset_joinpoint().join('t2s_2', aliased=True).all()
        
        

class SynonymTest(QueryTest):
    keep_mappers = True
    keep_data = True

    def setup_mappers(self):
        mapper(User, users, properties={
            'name_syn':synonym('name'),
            'addresses':relation(Address),
            'orders':relation(Order, backref='user'), # o2m, m2o
            'orders_syn':synonym('orders')
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items),  #m2m
            'address':relation(Address),  # m2o
            'items_syn':synonym('items')
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

    def test_joins(self):
        for j in (
            ['orders', 'items'],
            ['orders_syn', 'items'],
            ['orders', 'items_syn'],
            ['orders_syn', 'items_syn'],
        ):
            result = create_session().query(User).join(j).filter_by(id=3).all()
            assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

    def test_with_parent(self):
        for nameprop, orderprop in (
            ('name', 'orders'),
            ('name_syn', 'orders'),
            ('name', 'orders_syn'),
            ('name_syn', 'orders_syn'),
        ):
            sess = create_session()
            q = sess.query(User)
        
            u1 = q.filter_by(**{nameprop:'jack'}).one()

            o = sess.query(Order).with_parent(u1, property=orderprop).all()
            assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o
        
class InstancesTest(QueryTest):

    def test_from_alias(self):

        query = users.select(users.c.id==7).union(users.select(users.c.id>7)).alias('ulist').outerjoin(addresses).select(use_labels=True,order_by=['ulist.id', addresses.c.id])
        q = create_session().query(User)

        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).instances(query.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)


        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).from_statement(query).all()
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_contains_eager(self):

        selectquery = users.outerjoin(addresses).select(users.c.id<10, use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = create_session().query(User)

        def go():
            l = q.options(contains_eager('addresses')).instances(selectquery.execute())
            assert fixtures.user_address_result[0:3] == l
        self.assert_sql_count(testbase.db, go, 1)

        def go():
            l = q.options(contains_eager('addresses')).from_statement(selectquery).all()
            assert fixtures.user_address_result[0:3] == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_contains_eager_alias(self):
        adalias = addresses.alias('adalias')
        selectquery = users.outerjoin(adalias).select(use_labels=True, order_by=[users.c.id, adalias.c.id])
        q = create_session().query(User)

        def go():
            # test using a string alias name
            l = q.options(contains_eager('addresses', alias="adalias")).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

        def go():
            # test using the Alias object itself
            l = q.options(contains_eager('addresses', alias=adalias)).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

        def decorate(row):
            d = {}
            for c in addresses.columns:
                d[c] = row[adalias.corresponding_column(c)]
            return d

        def go():
            # test using a custom 'decorate' function
            l = q.options(contains_eager('addresses', decorator=decorate)).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_multi_mappers(self):
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()

        # note the result is a cartesian product
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]
        
        selectquery = users.outerjoin(addresses).select(use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = sess.query(User)
        l = q.instances(selectquery.execute(), Address)
        assert l == expected

        for aliased in (False, True):
            q = sess.query(User)
            q = q.add_entity(Address).outerjoin('addresses', aliased=aliased)
            l = q.all()
            assert l == expected

            q = sess.query(User).add_entity(Address)
            l = q.join('addresses', aliased=aliased).filter_by(email_address='ed@bettyboop.com').all()
            assert l == [(user8, address3)]
        
            q = sess.query(User, Address).join('addresses', aliased=aliased).filter_by(email_address='ed@bettyboop.com')
            assert q.all() == [(user8, address3)]

            q = sess.query(User, Address).join('addresses', aliased=aliased).options(eagerload('addresses')).filter_by(email_address='ed@bettyboop.com')
            assert q.all() == [(user8, address3)]

    def test_aliased_multi_mappers(self):
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()

        # note the result is a cartesian product
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]
        
        q = sess.query(User)
        adalias = addresses.alias('adalias')
        q = q.add_entity(Address, alias=adalias).select_from(users.outerjoin(adalias))
        l = q.all()
        assert l == expected

        q = sess.query(User).add_entity(Address, alias=adalias)
        l = q.select_from(users.outerjoin(adalias)).filter(adalias.c.email_address=='ed@bettyboop.com').all()
        assert l == [(user8, address3)]
        
    def test_multi_columns(self):
        """test aliased/nonalised joins with the usage of add_column()"""
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [(user7, 1),
            (user8, 3),
            (user9, 1),
            (user10, 0)
            ]
        
        for aliased in (False, True):
            q = sess.query(User)
            q = q.group_by([c for c in users.c]).order_by(User.id).outerjoin('addresses', aliased=aliased).add_column(func.count(Address.id).label('count'))
            l = q.all()
            assert l == expected

        s = select([users, func.count(addresses.c.id).label('count')]).select_from(users.outerjoin(addresses)).group_by(*[c for c in users.c]).order_by(User.id)
        q = sess.query(User)
        l = q.add_column("count").from_statement(s).all()
        assert l == expected

    def test_two_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck")]

        # test with a straight statement
        s = select([users, func.count(addresses.c.id).label('count'), ("Name:" + users.c.name).label('concat')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = create_session().query(User)
        l = q.add_column("count").add_column("concat").from_statement(s).all()
        assert l == expected
        
        # test with select_from()
        q = create_session().query(User).add_column(func.count(addresses.c.id))\
            .add_column(("Name:" + users.c.name)).select_from(users.outerjoin(addresses))\
            .group_by([c for c in users.c]).order_by(users.c.id)
            
        assert q.all() == expected

        # test with outerjoin() both aliased and non
        for aliased in (False, True):
            q = create_session().query(User).add_column(func.count(addresses.c.id))\
                .add_column(("Name:" + users.c.name)).outerjoin('addresses', aliased=aliased)\
                .group_by([c for c in users.c]).order_by(users.c.id)
            
            assert q.all() == expected

class CustomJoinTest(QueryTest):
    keep_mappers = False

    def setup_mappers(self):
        pass

    def test_double_same_mappers(self):
        """test aliasing of joins with a custom join condition"""
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=True, order_by=items.c.id),
        })
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=True),
            open_orders = relation(Order, primaryjoin = and_(orders.c.isopen == 1, users.c.id==orders.c.user_id), lazy=True),
            closed_orders = relation(Order, primaryjoin = and_(orders.c.isopen == 0, users.c.id==orders.c.user_id), lazy=True)
        ))
        q = create_session().query(User)

        assert [User(id=7)] == q.join(['open_orders', 'items'], aliased=True).filter(Item.id==4).join(['closed_orders', 'items'], aliased=True).filter(Item.id==3).all()

class SelfReferentialJoinTest(ORMTest):
    def define_tables(self, metadata):
        global nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    def test_join(self):
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=True, join_depth=3, 
                backref=backref('parent', remote_side=[nodes.c.id])
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.save(n1)
        sess.flush()
        sess.clear()
        
        # TODO: the aliasing of the join in query._join_to has to limit the aliasing
        # among local_side / remote_side (add local_side as an attribute on PropertyLoader)
        # also implement this idea in EagerLoader
        node = sess.query(Node).join('children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n12'

        node = sess.query(Node).join(['children', 'children'], aliased=True).filter_by(data='n122').first()
        assert node.data=='n1'
        
        node = sess.query(Node).filter_by(data='n122').join('parent', aliased=True).filter_by(data='n12').\
            join('parent', aliased=True, from_joinpoint=True).filter_by(data='n1').first()
        assert node.data == 'n122'

class ExternalColumnsTest(QueryTest):
    keep_mappers = False

    def setup_mappers(self):
        pass

    def test_external_columns_bad(self):
        """test that SA catches some common mis-configurations of external columns."""
        f = (users.c.id * 2)
        try:
            mapper(User, users, properties={
                'concat': f,
            })
            class_mapper(User)
        except exceptions.ArgumentError, e:
            assert str(e) == "Column '%s' is not represented in mapper's table.  Use the `column_property()` function to force this column to be mapped as a read-only attribute." % str(f)
        else:
            raise 'expected ArgumentError'
        clear_mappers()
        try:
            mapper(User, users, properties={
                'concat': column_property(users.c.id * 2),
            })
        except exceptions.ArgumentError, e:
            assert str(e) == 'ColumnProperties must be named for the mapper to work with them.  Try .label() to fix this'
        else:
            raise 'expected ArgumentError'

    def test_external_columns_good(self):
        """test querying mappings that reference external columns or selectables."""
        mapper(User, users, properties={
            'concat': column_property((users.c.id * 2).label('concat')),
            'count': column_property(select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).correlate(users).label('count'))
        })

        mapper(Address, addresses, properties={
            'user':relation(User, lazy=True)
        })    

        sess = create_session()
        l = sess.query(User).select()
        assert [
            User(id=7, concat=14, count=1),
            User(id=8, concat=16, count=3),
            User(id=9, concat=18, count=1),
            User(id=10, concat=20, count=0),
        ] == l

        address_result = [
            Address(id=1, user=User(id=7, concat=14, count=1)),
            Address(id=2, user=User(id=8, concat=16, count=3)),
            Address(id=3, user=User(id=8, concat=16, count=3)),
            Address(id=4, user=User(id=8, concat=16, count=3)),
            Address(id=5, user=User(id=9, concat=18, count=1))
        ]
        
        assert address_result == sess.query(Address).all()
        
        # run the eager version twice to test caching of aliased clauses
        for x in range(2):
            sess.clear()
            def go():
                assert address_result == sess.query(Address).options(eagerload('user')).all()
            self.assert_sql_count(testbase.db, go, 1)

        tuple_address_result = [(address, address.user) for address in address_result]
        
        tuple_address_result == sess.query(Address).join('user').add_entity(User).all()

        assert tuple_address_result == sess.query(Address).join('user', aliased=True, id='ualias').add_entity(User, id='ualias').all()

if __name__ == '__main__':
    testbase.main()


