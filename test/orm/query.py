import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from fixtures import *

class Base(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __eq__(self, other):
        """'passively' compare this object to another.
        
        only look at attributes that are present on the source object.
        
        """
        # use __dict__ to avoid instrumented properties
        for attr in self.__dict__.keys():
            if attr[0] == '_':
                continue
            value = getattr(self, attr)
            if hasattr(value, '__iter__') and not isinstance(value, basestring):
                if len(value) == 0:
                    continue
                for (us, them) in zip(value, getattr(other, attr)):
                    if us != them:
                        return False
                else:
                    continue
            else:
                if value is not None:
                    if value != getattr(other, attr):
                        return False
        else:
            return True

class QueryTest(testbase.ORMTest):
    keep_mappers = True
    keep_data = True
    
    def setUpAll(self):
        super(QueryTest, self).setUpAll()
        install_fixture_data()
        self.setup_mappers()
    
    def tearDownAll(self):
        clear_mappers()
        super(QueryTest, self).tearDownAll()
          
    def define_tables(self, meta):
        # a slight dirty trick here. 
        meta.tables = metadata.tables
        metadata.connect(meta.engine)
        
    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address),
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items),  #m2m
            'address':relation(Address),  # m2o
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

            
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

    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail 
        on postgres, mysql and oracle unless it is converted to an encoded string"""
        
        table = Table('unicode_data', users.metadata, 
            Column('id', Unicode(40), primary_key=True),
            Column('data', Unicode(40)))
        table.create()
        ustring = 'petit voix m\xe2\x80\x99a'.decode('utf-8')
        table.insert().execute(id=ustring, data=ustring)
        class LocalFoo(Base):pass
        mapper(LocalFoo, table)
        assert create_session().query(LocalFoo).get(ustring) == LocalFoo(id=ustring, data=ustring)

class CompileTest(QueryTest):
    def test_deferred(self):
        session = create_session()
        s = session.query(User).filter(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.id)).compile()
        
        l = session.query(User).instances(s.execute(emailad = 'jack@bean.com'))
        assert [User(id=7)] == l
    
class SliceTest(QueryTest):
    def test_first(self):
        assert  User(id=7) == create_session().query(User).first()
        
        assert create_session().query(User).filter(users.c.id==27).first() is None
        
class FilterTest(QueryTest):
    def test_basic(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).all()

    @testbase.unsupported('mssql')
    def test_limit(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).limit(2).offset(1).all()

        assert [User(id=8), User(id=9)] == list(create_session().query(User)[1:3])

        assert User(id=8) == create_session().query(User)[1]
        
    def test_onefilter(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter(users.c.name.endswith('ed')).all()

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

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter(users.c.id==9).all()

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
        # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
        result = create_session().query(User).join(['orders', 'items']).filter_by(id=3).reset_joinpoint().join(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_outerjoin(self):
        result = create_session().query(User).outerjoin(['orders', 'items']).filter_by(id=3).reset_joinpoint().outerjoin(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result
    
    def test_overlap_with_aliases(self):
        oalias = orders.alias('oalias')

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_("order 1", "order 2", "order 3")).join(['orders', 'items']).all()
        assert [User(id=7, name='jack'), User(id=9, name='fred')] == result
        
        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_("order 1", "order 2", "order 3")).join(['orders', 'items']).filter_by(id=4).all()
        assert [User(id=7, name='jack')] == result

class MultiplePathTest(testbase.ORMTest):
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
            create_session().query(T1).join('t2s_1').filter_by(t2.c.id==5).reset_joinpoint().join('t2s_2')
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't join to property 't2s_2'; a path to this table along a different secondary table already exists.  Use explicit `Alias` objects."
        
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

        q = sess.query(User)
        q = q.add_entity(Address).outerjoin('addresses')
        l = q.all()
        assert l == expected

        q = sess.query(User).add_entity(Address)
        l = q.join('addresses').filter_by(email_address='ed@bettyboop.com').all()
        assert l == [(user8, address3)]
        
        q = sess.query(User, Address).join('addresses').filter_by(email_address='ed@bettyboop.com')
        assert q.all() == [(user8, address3)]

        q = sess.query(User, Address).join('addresses').options(eagerload('addresses')).filter_by(email_address='ed@bettyboop.com')
        assert q.all() == [(user8, address3)]

    def test_multi_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [(user7, 1),
            (user8, 3),
            (user9, 1),
            (user10, 0)
            ]
            
        q = sess.query(User)
        q = q.group_by([c for c in users.c]).order_by(User.c.id).outerjoin('addresses').add_column(func.count(addresses.c.id).label('count'))
        l = q.all()
        assert l == expected

        s = select([users, func.count(addresses.c.id).label('count')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=users.c.id)
        q = sess.query(User)
        l = q.add_column("count").from_statement(s).all()
        assert l == expected

    @testbase.unsupported('mysql') # only because of "+" operator requiring "concat" in mysql (fix #475)
    def test_two_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck")]

        s = select([users, func.count(addresses.c.id).label('count'), ("Name:" + users.c.name).label('concat')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = create_session().query(User)
        l = q.add_column("count").add_column("concat").from_statement(s).all()
        assert l == expected
        
        q = create_session().query(User).add_column(func.count(addresses.c.id))\
            .add_column(("Name:" + users.c.name)).select_from(users.outerjoin(addresses))\
            .group_by([c for c in users.c]).order_by(users.c.id)
            
        assert q.all() == expected


if __name__ == '__main__':
    testbase.main()


