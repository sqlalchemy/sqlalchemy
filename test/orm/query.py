import testenv; testenv.configure_for_tests()
import operator
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy.orm import *
from testlib import *
from testlib import engines
from testlib.fixtures import *

class QueryTest(FixtureTest):
    keep_mappers = True
    keep_data = True

    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Address, addresses, properties={
            'dingaling':relation(Dingaling, uselist=False, backref="address")  #o2o
        })
        mapper(Dingaling, dingalings)
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
        assert User(id=7) == create_session(bind=testing.db).query(User).get(7)

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

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc. criterion"""

        s = create_session()
        try:
            s.query(User).join('addresses').filter(Address.user_id==8).get(7)
            assert False
        except exceptions.SAWarning, e:
            assert str(e) == "Query.get() being called on a Query with existing criterion; criterion is being ignored."

        @testing.emits_warning('Query.*')
        def warns():
            assert s.query(User).filter(User.id==7).get(19) is None

            u = s.query(User).get(7)
            assert s.query(User).filter(User.id==9).get(7) is u
            s.clear()
            assert s.query(User).filter(User.id==9).get(7).id == u.id

            # user 10 has no addresses
            u = s.query(User).get(10)
            assert s.query(User).join('addresses').get(10) is u
            s.clear()
            assert s.query(User).join('addresses').get(10).id == u.id

            u = s.query(User).get(7)
            assert s.query(User).join('addresses').filter(Address.user_id==8).filter(User.id==7).first() is None
            assert s.query(User).join('addresses').filter(Address.user_id==8).get(7) is u
            s.clear()
            assert s.query(User).join('addresses').filter(Address.user_id==8).get(7).id == u.id

            assert s.query(User).join('addresses').filter(Address.user_id==8).load(7).id == u.id
        warns()

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

    @testing.exclude('mysql', '<', (4, 1))
    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail
        on postgres, mysql and oracle unless it is converted to an encoded string"""

        metadata = MetaData(engines.utf8_engine())
        table = Table('unicode_data', metadata,
            Column('id', Unicode(40), primary_key=True),
            Column('data', Unicode(40)))
        try:
            metadata.create_all()
            ustring = 'petit voix m\xe2\x80\x99a'.decode('utf-8')
            table.insert().execute(id=ustring, data=ustring)
            class LocalFoo(Base):
                pass
            mapper(LocalFoo, table)
            self.assertEquals(create_session().query(LocalFoo).get(ustring),
                              LocalFoo(id=ustring, data=ustring))
        finally:
            metadata.drop_all()

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
                (5, User.id, ':users_id_1 %s users.id'),
                (5, literal(6), ':param_1 %s :param_2'),
                (User.id, 5, 'users.id %s :users_id_1'),
                (User.id, literal('b'), 'users.id %s :param_1'),
                (User.id, User.id, 'users.id %s users.id'),
                (literal(5), 'b', ':param_1 %s :param_2'),
                (literal(5), User.id, ':param_1 %s users.id'),
                (literal(5), literal(6), ':param_1 %s :param_2'),
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
                ('a', User.id, ':users_id_1', 'users.id'),
                ('a', literal('b'), ':param_2', ':param_1'), # note swap!
                (User.id, 'b', 'users.id', ':users_id_1'),
                (User.id, literal('b'), 'users.id', ':param_1'),
                (User.id, User.id, 'users.id', 'users.id'),
                (literal('a'), 'b', ':param_1', ':param_2'),
                (literal('a'), User.id, ':param_1', 'users.id'),
                (literal('a'), literal('b'), ':param_1', ':param_2'),
                ):

                # the compiled clause should match either (e.g.):
                # 'a' < 'b' -or- 'b' > 'a'.
                compiled = str(py_op(lhs, rhs).compile(dialect=default.DefaultDialect()))
                fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
                rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

                self.assert_(compiled == fwd_sql or compiled == rev_sql,
                             "\n'" + compiled + "'\n does not match\n'" +
                             fwd_sql + "'\n or\n'" + rev_sql + "'")

    def test_op(self):
        assert str(User.name.op('ilike')('17').compile(dialect=default.DefaultDialect())) == "users.name ilike :users_name_1"

    def test_in(self):
         self._test(User.id.in_(['a', 'b']),
                    "users.id IN (:users_id_1, :users_id_2)")

    def test_between(self):
        self._test(User.id.between('a', 'b'),
                   "users.id BETWEEN :users_id_1 AND :users_id_2")

    def test_clauses(self):
        for (expr, compare) in (
            (func.max(User.id), "max(users.id)"),
            (User.id.desc(), "users.id DESC"),
            (between(5, User.id, Address.id), ":param_1 BETWEEN users.id AND addresses.id"),
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

    @testing.fails_on('maxdb')
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

    @testing.unsupported('maxdb') # can core
    def test_has(self):
        sess = create_session()
        assert [Address(id=5)] == sess.query(Address).filter(Address.user.has(name='fred')).all()

        assert [Address(id=2), Address(id=3), Address(id=4), Address(id=5)] == sess.query(Address).filter(Address.user.has(User.name.like('%ed%'))).all()

        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter(Address.user.has(User.name.like('%ed%'), id=8)).all()

        dingaling = sess.query(Dingaling).get(2)
        assert [User(id=9)] == sess.query(User).filter(User.addresses.any(Address.dingaling==dingaling)).all()
        
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

        # generates an IS NULL
        assert [] == sess.query(Address).filter(Address.user == None).all()

        assert [Order(id=5)] == sess.query(Order).filter(Order.address == None).all()

        # o2o
        dingaling = sess.query(Dingaling).get(2)
        assert [Address(id=5)] == sess.query(Address).filter(Address.dingaling==dingaling).all()

    def test_filter_by(self):
        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter_by(user=user).all()

        # many to one generates IS NULL
        assert [] == sess.query(Address).filter_by(user = None).all()

        # one to many generates WHERE NOT EXISTS
        assert [User(name='chuck')] == sess.query(User).filter_by(addresses = None).all()
    
    def test_none_comparison(self):
        sess = create_session()
        
        # o2o
        self.assertEquals([Address(id=1), Address(id=3), Address(id=4)], sess.query(Address).filter(Address.dingaling==None).all())
        self.assertEquals([Address(id=2), Address(id=5)], sess.query(Address).filter(Address.dingaling != None).all())
        
        # m2o
        self.assertEquals([Order(id=5)], sess.query(Order).filter(Order.address==None).all())
        self.assertEquals([Order(id=1), Order(id=2), Order(id=3), Order(id=4)], sess.query(Order).filter(Order.address!=None).all())
        
        # o2m
        self.assertEquals([User(id=10)], sess.query(User).filter(User.addresses==None).all())
        self.assertEquals([User(id=7),User(id=8),User(id=9)], sess.query(User).filter(User.addresses!=None).all())
        
class AggregateTest(QueryTest):
    def test_sum(self):
        sess = create_session()
        orders = sess.query(Order).filter(Order.id.in_([2, 3, 4]))
        assert orders.sum(Order.user_id * Order.address_id) == 79

    @testing.uses_deprecated('Call to deprecated function apply_sum')
    def test_apply(self):
        sess = create_session()
        assert sess.query(Order).apply_sum(Order.user_id * Order.address_id).filter(Order.id.in_([2, 3, 4])).one() == 79

    def test_having(self):
        sess = create_session()
        assert [User(name=u'ed',id=8)] == sess.query(User).group_by([c for c in User.c]).join('addresses').having(func.count(Address.c.id)> 2).all()

        assert [User(name=u'jack',id=7), User(name=u'fred',id=9)] == sess.query(User).group_by([c for c in User.c]).join('addresses').having(func.count(Address.c.id)< 2).all()

class CountTest(QueryTest):
    def test_basic(self):
        assert 4 == create_session().query(User).count()

        assert 2 == create_session().query(User).filter(users.c.name.endswith('ed')).count()

class DistinctTest(QueryTest):
    def test_basic(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).distinct().all()
        assert [User(id=7), User(id=9), User(id=8),User(id=10)] == create_session().query(User).distinct().order_by(desc(User.name)).all()

    def test_joined(self):
        """test that orderbys from a joined table get placed into the columns clause when DISTINCT is used"""

        sess = create_session()
        q = sess.query(User).join('addresses').distinct().order_by(desc(Address.email_address))

        assert [User(id=7), User(id=9), User(id=8)] == q.all()

        sess.clear()

        # test that it works on embedded eagerload/LIMIT subquery
        q = sess.query(User).join('addresses').distinct().options(eagerload('addresses')).order_by(desc(Address.email_address)).limit(2)

        def go():
            assert [
                User(id=7, addresses=[
                    Address(id=1)
                ]),
                User(id=9, addresses=[
                    Address(id=5)
                ]),
            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)


class YieldTest(QueryTest):
    def test_basic(self):
        import gc
        sess = create_session()
        q = iter(sess.query(User).yield_per(1).from_statement("select * from users"))

        ret = []
        self.assertEquals(len(sess.identity_map), 0)
        ret.append(q.next())
        ret.append(q.next())
        self.assertEquals(len(sess.identity_map), 2)
        ret.append(q.next())
        ret.append(q.next())
        self.assertEquals(len(sess.identity_map), 4)
        try:
            q.next()
            assert False
        except StopIteration:
            pass

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

        # test against None for parent? this can't be done with the current API since we don't know
        # what mapper to use
        #assert sess.query(Order).with_parent(None, property='addresses').all() == [Order(description="order 5")]

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

    def test_getjoinable_tables(self):
        sess = create_session()

        sel1 = select([users]).alias()
        sel2 = select([users], from_obj=users.join(addresses)).alias()

        j1 = sel1.join(users, sel1.c.id==users.c.id)
        j2 = j1.join(addresses)

        for from_obj, assert_cond in (
            (users, [users]),
            (users.join(addresses), [users, addresses]),
            (sel1, [sel1]),
            (sel2, [sel2]),
            (sel1.join(users, sel1.c.id==users.c.id), [sel1, users]),
            (sel2.join(users, sel2.c.id==users.c.id), [sel2, users]),
            (j2, [j1, j2, sel1, users, addresses])

        ):
            ret = set(sess.query(User).select_from(from_obj)._get_joinable_tables())
            self.assertEquals(ret, set(assert_cond).union([from_obj]), [x.description for x in ret])

    def test_overlapping_paths(self):
        for aliased in (True,False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join(['orders', 'items'], aliased=aliased).filter_by(id=3).join(['orders','address'], aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_outerjoin(self):
        result = create_session().query(User).outerjoin(['orders', 'items']).filter_by(id=3).outerjoin(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result

    def test_generative_join(self):
        # test that alised_ids is copied
        sess = create_session()
        q = sess.query(User).add_entity(Address)
        q1 = q.join('addresses', aliased=True)
        q2 = q.join('addresses', aliased=True)
        q3 = q2.join('addresses', aliased=True)
        q4 = q2.join('addresses', aliased=True, id='someid')
        q5 = q2.join('addresses', aliased=True, id='someid')
        q6 = q5.join('addresses', aliased=True, id='someid')
        assert q1._alias_ids[class_mapper(Address)] != q2._alias_ids[class_mapper(Address)]
        assert q2._alias_ids[class_mapper(Address)] != q3._alias_ids[class_mapper(Address)]
        assert q4._alias_ids['someid'] != q5._alias_ids['someid']
        
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
        sess =create_session()
        q = sess.query(User)

        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).instances(query.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()

        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).from_statement(query).all()
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):

        selectquery = users.outerjoin(addresses).select(users.c.id<10, use_labels=True, order_by=[users.c.id, addresses.c.id])
        sess = create_session()
        q = sess.query(User)

        def go():
            l = q.options(contains_eager('addresses')).instances(selectquery.execute())
            assert fixtures.user_address_result[0:3] == l
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()

        def go():
            l = q.options(contains_eager('addresses')).from_statement(selectquery).all()
            assert fixtures.user_address_result[0:3] == l
        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_alias(self):
        adalias = addresses.alias('adalias')
        selectquery = users.outerjoin(adalias).select(use_labels=True, order_by=[users.c.id, adalias.c.id])
        sess = create_session()
        q = sess.query(User)

        def go():
            # test using a string alias name
            l = q.options(contains_eager('addresses', alias="adalias")).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()

        def go():
            # test using the Alias object itself
            l = q.options(contains_eager('addresses', alias=adalias)).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()

        def decorate(row):
            d = {}
            for c in addresses.columns:
                d[c] = row[adalias.corresponding_column(c)]
            return d

        def go():
            # test using a custom 'decorate' function
            l = q.options(contains_eager('addresses', decorator=decorate)).instances(selectquery.execute())
            assert fixtures.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()

        oalias = orders.alias('o1')
        ialias = items.alias('i1')
        query = users.outerjoin(oalias).outerjoin(order_items).outerjoin(ialias).select(use_labels=True).order_by(users.c.id).order_by(oalias.c.id).order_by(ialias.c.id)
        q = create_session().query(User)
        # test using string alias with more than one level deep
        def go():
            l = q.options(contains_eager('orders', alias='o1'), contains_eager('orders.items', alias='i1')).instances(query.execute())
            assert fixtures.user_order_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()

        # test using Alias with more than one level deep
        def go():
            l = q.options(contains_eager('orders', alias=oalias), contains_eager('orders.items', alias=ialias)).instances(query.execute())
            assert fixtures.user_order_result == l
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()


    def test_multi_mappers(self):

        test_session = create_session()

        (user7, user8, user9, user10) = test_session.query(User).all()
        (address1, address2, address3, address4, address5) = test_session.query(Address).all()

        # note the result is a cartesian product
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]

        sess = create_session()

        selectquery = users.outerjoin(addresses).select(use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = sess.query(User)
        l = q.instances(selectquery.execute(), Address)
        assert l == expected

        sess.clear()

        for aliased in (False, True):
            q = sess.query(User)

            q = q.add_entity(Address).outerjoin('addresses', aliased=aliased)
            l = q.all()
            assert l == expected
            sess.clear()

            q = sess.query(User).add_entity(Address)
            l = q.join('addresses', aliased=aliased).filter_by(email_address='ed@bettyboop.com').all()
            assert l == [(user8, address3)]
            sess.clear()

            q = sess.query(User, Address).join('addresses', aliased=aliased).filter_by(email_address='ed@bettyboop.com')
            assert q.all() == [(user8, address3)]
            sess.clear()

            q = sess.query(User, Address).join('addresses', aliased=aliased).options(eagerload('addresses')).filter_by(email_address='ed@bettyboop.com')
            assert q.all() == [(user8, address3)]
            sess.clear()

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

        sess.clear()

        q = sess.query(User).add_entity(Address, alias=adalias)
        l = q.select_from(users.outerjoin(adalias)).filter(adalias.c.email_address=='ed@bettyboop.com').all()
        assert l == [(user8, address3)]

    def test_multi_columns(self):
        sess = create_session()

        expected = [(u, u.name) for u in sess.query(User).all()]

        for add_col in (User.name, users.c.name, User.c.name):
            assert sess.query(User).add_column(add_col).all() == expected
            sess.clear()

        try:
            sess.query(User).add_column(object()).all()
            assert False
        except exceptions.InvalidRequestError, e:
            assert "Invalid column expression" in str(e)


    def test_multi_columns_2(self):
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
            sess.clear()

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

        sess.clear()

        # test with select_from()
        q = create_session().query(User).add_column(func.count(addresses.c.id))\
            .add_column(("Name:" + users.c.name)).select_from(users.outerjoin(addresses))\
            .group_by([c for c in users.c]).order_by(users.c.id)

        assert q.all() == expected
        sess.clear()

        # test with outerjoin() both aliased and non
        for aliased in (False, True):
            q = create_session().query(User).add_column(func.count(addresses.c.id))\
                .add_column(("Name:" + users.c.name)).outerjoin('addresses', aliased=aliased)\
                .group_by([c for c in users.c]).order_by(users.c.id)

            assert q.all() == expected
            sess.clear()


class SelectFromTest(QueryTest):
    keep_mappers = False

    def setup_mappers(self):
        pass

    def test_replace_with_select(self):
        mapper(User, users, properties = {
            'addresses':relation(Address)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8])).alias()
        sess = create_session()

        self.assertEquals(sess.query(User).select_from(sel).all(), [User(id=7), User(id=8)])

        self.assertEquals(sess.query(User).select_from(sel).filter(User.c.id==8).all(), [User(id=8)])

        self.assertEquals(sess.query(User).select_from(sel).order_by(desc(User.name)).all(), [
            User(name='jack',id=7), User(name='ed',id=8)
        ])

        self.assertEquals(sess.query(User).select_from(sel).order_by(asc(User.name)).all(), [
            User(name='ed',id=8), User(name='jack',id=7)
        ])

        self.assertEquals(sess.query(User).select_from(sel).options(eagerload('addresses')).first(),
            User(name='jack', addresses=[Address(id=1)])
        )

    def test_join(self):
        mapper(User, users, properties = {
            'addresses':relation(Address)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        self.assertEquals(sess.query(User).select_from(sel).join('addresses').add_entity(Address).order_by(User.id).order_by(Address.id).all(),
            [
                (User(name='jack',id=7), Address(user_id=7,email_address='jack@bean.com',id=1)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@wood.com',id=2)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@bettyboop.com',id=3)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@lala.com',id=4))
            ]
        )

        self.assertEquals(sess.query(User).select_from(sel).join('addresses', aliased=True).add_entity(Address).order_by(User.id).order_by(Address.id).all(),
            [
                (User(name='jack',id=7), Address(user_id=7,email_address='jack@bean.com',id=1)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@wood.com',id=2)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@bettyboop.com',id=3)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@lala.com',id=4))
            ]
        )
    
    def test_more_joins(self):
        mapper(User, users, properties={
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, order_by=items.c.id),  #m2m
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords, order_by=keywords.c.id) #m2m
        })
        mapper(Keyword, keywords)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        self.assertEquals(sess.query(User).select_from(sel).join(['orders', 'items', 'keywords']).filter(Keyword.name.in_(['red', 'big', 'round'])).all(), [
            User(name=u'jack',id=7)
        ])

        self.assertEquals(sess.query(User).select_from(sel).join(['orders', 'items', 'keywords'], aliased=True).filter(Keyword.name.in_(['red', 'big', 'round'])).all(), [
            User(name=u'jack',id=7)
        ])

        def go():
            self.assertEquals(sess.query(User).select_from(sel).options(eagerload_all('orders.items.keywords')).join(['orders', 'items', 'keywords'], aliased=True).filter(Keyword.name.in_(['red', 'big', 'round'])).all(), [
                User(name=u'jack',orders=[
                    Order(description=u'order 1',items=[
                        Item(description=u'item 1',keywords=[Keyword(name=u'red'), Keyword(name=u'big'), Keyword(name=u'round')]),
                        Item(description=u'item 2',keywords=[Keyword(name=u'red',id=2), Keyword(name=u'small',id=5), Keyword(name=u'square')]),
                        Item(description=u'item 3',keywords=[Keyword(name=u'green',id=3), Keyword(name=u'big',id=4), Keyword(name=u'round',id=6)])
                    ]),
                    Order(description=u'order 3',items=[
                        Item(description=u'item 3',keywords=[Keyword(name=u'green',id=3), Keyword(name=u'big',id=4), Keyword(name=u'round',id=6)]),
                        Item(description=u'item 4',keywords=[],id=4),
                        Item(description=u'item 5',keywords=[],id=5)
                        ]),
                    Order(description=u'order 5',items=[Item(description=u'item 5',keywords=[])])])
                ])
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()
        sel2 = orders.select(orders.c.id.in_([1,2,3]))
        self.assertEquals(sess.query(Order).select_from(sel2).join(['items', 'keywords']).filter(Keyword.name == 'red').all(), [
            Order(description=u'order 1',id=1),
            Order(description=u'order 2',id=2),
        ])
        self.assertEquals(sess.query(Order).select_from(sel2).join(['items', 'keywords'], aliased=True).filter(Keyword.name == 'red').all(), [
            Order(description=u'order 1',id=1),
            Order(description=u'order 2',id=2),
        ])


    def test_replace_with_eager(self):
        mapper(User, users, properties = {
            'addresses':relation(Address)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        def go():
            self.assertEquals(sess.query(User).options(eagerload('addresses')).select_from(sel).all(),
                [
                    User(id=7, addresses=[Address(id=1)]),
                    User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)])
                ]
            )
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()

        def go():
            self.assertEquals(sess.query(User).options(eagerload('addresses')).select_from(sel).filter(User.c.id==8).all(),
                [User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)])]
            )
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()

        def go():
            self.assertEquals(sess.query(User).options(eagerload('addresses')).select_from(sel)[1], User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)]))
        self.assert_sql_count(testing.db, go, 1)

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

class SelfReferentialTest(ORMTest):
    keep_mappers = True
    keep_data = True
    
    def define_tables(self, metadata):
        global nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))
    
    def insert_data(self):
        global Node
        
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
        sess.close()
        
    def test_join(self):
        sess = create_session()

        node = sess.query(Node).join('children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n12'

        node = sess.query(Node).join(['children', 'children'], aliased=True).filter_by(data='n122').first()
        assert node.data=='n1'

        node = sess.query(Node).filter_by(data='n122').join('parent', aliased=True).filter_by(data='n12').\
            join('parent', aliased=True, from_joinpoint=True).filter_by(data='n1').first()
        assert node.data == 'n122'

    def test_any(self):
        sess = create_session()
        
        self.assertEquals(sess.query(Node).filter(Node.children.any(Node.data=='n1')).all(), [])
        self.assertEquals(sess.query(Node).filter(Node.children.any(Node.data=='n12')).all(), [Node(data='n1')])
        self.assertEquals(sess.query(Node).filter(~Node.children.any()).all(), [Node(data='n11'), Node(data='n13'),Node(data='n121'),Node(data='n122'),Node(data='n123'),])

    def test_has(self):
        sess = create_session()
        
        self.assertEquals(sess.query(Node).filter(Node.parent.has(Node.data=='n12')).all(), [Node(data='n121'),Node(data='n122'),Node(data='n123')])
        self.assertEquals(sess.query(Node).filter(Node.parent.has(Node.data=='n122')).all(), [])
        self.assertEquals(sess.query(Node).filter(~Node.parent.has()).all(), [Node(data='n1')])
    
    def test_contains(self):
        sess = create_session()
        
        n122 = sess.query(Node).filter(Node.data=='n122').one()
        self.assertEquals(sess.query(Node).filter(Node.children.contains(n122)).all(), [Node(data='n12')])

        n13 = sess.query(Node).filter(Node.data=='n13').one()
        self.assertEquals(sess.query(Node).filter(Node.children.contains(n13)).all(), [Node(data='n1')])
    
    def test_eq_ne(self):
        sess = create_session()
        
        n12 = sess.query(Node).filter(Node.data=='n12').one()
        self.assertEquals(sess.query(Node).filter(Node.parent==n12).all(), [Node(data='n121'),Node(data='n122'),Node(data='n123')])
        
        self.assertEquals(sess.query(Node).filter(Node.parent != n12).all(), [Node(data='n1'), Node(data='n11'), Node(data='n12'), Node(data='n13')])

class SelfReferentialM2MTest(ORMTest):
    keep_mappers = True
    keep_data = True
    
    def define_tables(self, metadata):
        global nodes, node_to_nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
            
        node_to_nodes =Table('node_to_nodes', metadata,
            Column('left_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            Column('right_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            )
    
    def insert_data(self):
        global Node
        
        class Node(Base):
            pass

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=True, secondary=node_to_nodes,
                primaryjoin=nodes.c.id==node_to_nodes.c.left_node_id,
                secondaryjoin=nodes.c.id==node_to_nodes.c.right_node_id,
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n2 = Node(data='n2')
        n3 = Node(data='n3')
        n4 = Node(data='n4')
        n5 = Node(data='n5')
        n6 = Node(data='n6')
        n7 = Node(data='n7')
        
        n1.children = [n2, n3, n4]
        n2.children = [n3, n6, n7]
        n3.children = [n5, n4]

        sess.save(n1)
        sess.save(n2)
        sess.save(n3)
        sess.save(n4)
        sess.flush()
        sess.close()

    def test_any(self):
        sess = create_session()
        self.assertEquals(sess.query(Node).filter(Node.children.any(Node.data=='n3')).all(), [Node(data='n1'), Node(data='n2')])

    def test_contains(self):
        sess = create_session()
        n4 = sess.query(Node).filter_by(data='n4').one()

        self.assertEquals(sess.query(Node).filter(Node.children.contains(n4)).order_by(Node.data).all(), [Node(data='n1'), Node(data='n3')])
        self.assertEquals(sess.query(Node).filter(not_(Node.children.contains(n4))).order_by(Node.data).all(), [Node(data='n2'), Node(data='n4'), Node(data='n5'), Node(data='n6'), Node(data='n7')])
        
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
                'count': column_property(select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).correlate(users))
            })
        except exceptions.ArgumentError, e:
            assert str(e) == 'column_property() must be given a ColumnElement as its argument.  Try .label() or .as_scalar() for Selectables to fix this.'
        else:
            raise 'expected ArgumentError'

    def test_external_columns_good(self):
        """test querying mappings that reference external columns or selectables."""
        mapper(User, users, properties={
            'concat': column_property((users.c.id * 2)),
            'count': column_property(select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).correlate(users).as_scalar())
        })

        mapper(Address, addresses, properties={
            'user':relation(User, lazy=True)
        })

        sess = create_session()
        l = sess.query(User).all()
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
            self.assert_sql_count(testing.db, go, 1)

        tuple_address_result = [(address, address.user) for address in address_result]

        tuple_address_result == sess.query(Address).join('user').add_entity(User).all()

        assert tuple_address_result == sess.query(Address).join('user', aliased=True, id='ualias').add_entity(User, id='ualias').all()



if __name__ == '__main__':
    testenv.main()
