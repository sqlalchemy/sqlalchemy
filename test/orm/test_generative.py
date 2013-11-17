from sqlalchemy.testing import eq_
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey, MetaData, func
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from test.orm import _fixtures


class GenerativeQueryTest(fixtures.MappedTest):
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
              Column('id', Integer, sa.Sequence('foo_id_seq'), primary_key=True),
              Column('bar', Integer),
              Column('range', Integer))

    @classmethod
    def fixtures(cls):
        rows = tuple([(i, i % 10) for i in range(100)])
        foo_data = (('bar', 'range'),) + rows
        return dict(foo=foo_data)

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        class Foo(cls.Basic):
            pass

        mapper(Foo, foo)

    def test_selectby(self):
        Foo = self.classes.Foo

        res = create_session().query(Foo).filter_by(range=5)
        assert res.order_by(Foo.bar)[0].bar == 5
        assert res.order_by(sa.desc(Foo.bar))[0].bar == 95

    def test_slice(self):
        Foo = self.classes.Foo

        sess = create_session()
        query = sess.query(Foo).order_by(Foo.id)
        orig = query.all()

        assert query[1] == orig[1]
        assert query[-4] == orig[-4]
        assert query[-1] == orig[-1]

        assert list(query[10:20]) == orig[10:20]
        assert list(query[10:]) == orig[10:]
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]
        assert list(query[5:5]) == orig[5:5]
        assert list(query[10:40:3]) == orig[10:40:3]
        assert list(query[-5:]) == orig[-5:]
        assert list(query[-2:-5]) == orig[-2:-5]
        assert list(query[-5:-2]) == orig[-5:-2]
        assert list(query[:-2]) == orig[:-2]

        assert query[10:20][5] == orig[10:20][5]

    @testing.uses_deprecated('Call to deprecated function apply_max')
    def test_aggregate(self):
        foo, Foo = self.tables.foo, self.classes.Foo

        sess = create_session()
        query = sess.query(Foo)
        assert query.count() == 100
        assert sess.query(func.min(foo.c.bar)).filter(foo.c.bar<30).one() == (0,)

        assert sess.query(func.max(foo.c.bar)).filter(foo.c.bar<30).one() == (29,)
        assert next(query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)))[0] == 29
        assert next(query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)))[0] == 29

    @testing.fails_if(lambda:testing.against('mysql+mysqldb') and
            testing.db.dialect.dbapi.version_info[:4] == (1, 2, 1, 'gamma'),
            "unknown incompatibility")
    def test_aggregate_1(self):
        foo = self.tables.foo


        query = create_session().query(func.sum(foo.c.bar))
        assert query.filter(foo.c.bar<30).one() == (435,)

    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.fails_on('mssql', 'AVG produces an average as the original column type on mssql.')
    def test_aggregate_2(self):
        foo = self.tables.foo

        query = create_session().query(func.avg(foo.c.bar))
        avg = query.filter(foo.c.bar < 30).one()[0]
        eq_(float(round(avg, 1)), 14.5)

    @testing.fails_on('mssql', 'AVG produces an average as the original column type on mssql.')
    def test_aggregate_3(self):
        foo, Foo = self.tables.foo, self.classes.Foo

        query = create_session().query(Foo)

        avg_f = next(query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)))[0]
        assert float(round(avg_f, 1)) == 14.5

        avg_o = next(query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)))[0]
        assert float(round(avg_o, 1)) == 14.5

    def test_filter(self):
        Foo = self.classes.Foo

        query = create_session().query(Foo)
        assert query.count() == 100
        assert query.filter(Foo.bar < 30).count() == 30
        res2 = query.filter(Foo.bar < 30).filter(Foo.bar > 10)
        assert res2.count() == 19

    def test_order_by(self):
        Foo = self.classes.Foo

        query = create_session().query(Foo)
        assert query.order_by(Foo.bar)[0].bar == 0
        assert query.order_by(sa.desc(Foo.bar))[0].bar == 99

    def test_offset(self):
        Foo = self.classes.Foo

        query = create_session().query(Foo)
        assert list(query.order_by(Foo.bar).offset(10))[0].bar == 10

    def test_offset(self):
        Foo = self.classes.Foo

        query = create_session().query(Foo)
        assert len(list(query.limit(10))) == 10


class GenerativeTest2(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata,
              Column('id', Integer, primary_key=True))
        Table('table2', metadata,
              Column('t1id', Integer, ForeignKey("table1.id"),
                     primary_key=True),
              Column('num', Integer, primary_key=True))

    @classmethod
    def setup_mappers(cls):
        table2, table1 = cls.tables.table2, cls.tables.table1

        class Obj1(cls.Basic):
            pass
        class Obj2(cls.Basic):
            pass

        mapper(Obj1, table1)
        mapper(Obj2, table2)

    @classmethod
    def fixtures(cls):
        return dict(
            table1=(('id',),
                    (1,),
                    (2,),
                    (3,),
                    (4,)),
            table2=(('num', 't1id'),
                    (1, 1),
                    (2, 1),
                    (3, 1),
                    (4, 2),
                    (5, 2),
                    (6, 3)))

    def test_distinct_count(self):
        table2, Obj1, table1 = (self.tables.table2,
                                self.classes.Obj1,
                                self.tables.table1)

        query = create_session().query(Obj1)
        eq_(query.count(), 4)

        res = query.filter(sa.and_(table1.c.id == table2.c.t1id,
                                   table2.c.t1id == 1))
        eq_(res.count(), 3)
        res = query.filter(sa.and_(table1.c.id == table2.c.t1id,
                                   table2.c.t1id == 1)).distinct()
        eq_(res.count(), 1)


class RelationshipsTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        addresses, Order, User, Address, orders, users = (cls.tables.addresses,
                                cls.classes.Order,
                                cls.classes.User,
                                cls.classes.Address,
                                cls.tables.orders,
                                cls.tables.users)

        mapper(User, users, properties={
            'orders':relationship(mapper(Order, orders, properties={
                'addresses':relationship(mapper(Address, addresses))}))})


    def test_join(self):
        """Query.join"""

        User, Address = self.classes.User, self.classes.Address


        session = create_session()
        q = (session.query(User).join('orders', 'addresses').
             filter(Address.id == 1))
        eq_([User(id=7)], q.all())

    def test_outer_join(self):
        """Query.outerjoin"""

        Order, User, Address = (self.classes.Order,
                                self.classes.User,
                                self.classes.Address)


        session = create_session()
        q = (session.query(User).outerjoin('orders', 'addresses').
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(set([User(id=7), User(id=8), User(id=10)]),
            set(q.all()))

    def test_outer_join_count(self):
        """test the join and outerjoin functions on Query"""

        Order, User, Address = (self.classes.Order,
                                self.classes.User,
                                self.classes.Address)


        session = create_session()

        q = (session.query(User).outerjoin('orders', 'addresses').
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(q.count(), 4)

    def test_from(self):
        users, Order, User, Address, orders, addresses = (self.tables.users,
                                self.classes.Order,
                                self.classes.User,
                                self.classes.Address,
                                self.tables.orders,
                                self.tables.addresses)

        session = create_session()

        sel = users.outerjoin(orders).outerjoin(
            addresses, orders.c.address_id == addresses.c.id)
        q = (session.query(User).select_from(sel).
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(set([User(id=7), User(id=8), User(id=10)]),
            set(q.all()))


class CaseSensitiveTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('Table1', metadata,
            Column('ID', Integer, primary_key=True))
        Table('Table2', metadata,
              Column('T1ID', Integer, ForeignKey("Table1.ID"),
                     primary_key=True),
              Column('NUM', Integer, primary_key=True))

    @classmethod
    def setup_mappers(cls):
        Table2, Table1 = cls.tables.Table2, cls.tables.Table1

        class Obj1(cls.Basic):
            pass
        class Obj2(cls.Basic):
            pass

        mapper(Obj1, Table1)
        mapper(Obj2, Table2)

    @classmethod
    def fixtures(cls):
        return dict(
            Table1=(('ID',),
                    (1,),
                    (2,),
                    (3,),
                    (4,)),
            Table2=(('NUM', 'T1ID'),
                    (1, 1),
                    (2, 1),
                    (3, 1),
                    (4, 2),
                    (5, 2),
                    (6, 3)))

    def test_distinct_count(self):
        Table2, Obj1, Table1 = (self.tables.Table2,
                                self.classes.Obj1,
                                self.tables.Table1)

        q = create_session(bind=testing.db).query(Obj1)
        assert q.count() == 4
        res = q.filter(sa.and_(Table1.c.ID==Table2.c.T1ID,Table2.c.T1ID==1))
        assert res.count() == 3
        res = q.filter(sa.and_(Table1.c.ID==Table2.c.T1ID,Table2.c.T1ID==1)).distinct()
        eq_(res.count(), 1)


