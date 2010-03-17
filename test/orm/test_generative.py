from sqlalchemy.test.testing import eq_
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import Integer, String, ForeignKey, MetaData, func
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy.test.testing import eq_
from test.orm import _base, _fixtures


class GenerativeQueryTest(_base.MappedTest):
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
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Foo(_base.BasicEntity):
            pass

        mapper(Foo, foo)

    @testing.resolve_artifact_names
    def test_selectby(self):
        res = create_session().query(Foo).filter_by(range=5)
        assert res.order_by(Foo.bar)[0].bar == 5
        assert res.order_by(sa.desc(Foo.bar))[0].bar == 95

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_slice(self):
        sess = create_session()
        query = sess.query(Foo).order_by(Foo.id)
        orig = query.all()
        
        assert query[1] == orig[1]
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
    @testing.resolve_artifact_names
    def test_aggregate(self):
        sess = create_session()
        query = sess.query(Foo)
        assert query.count() == 100
        assert sess.query(func.min(foo.c.bar)).filter(foo.c.bar<30).one() == (0,)
        
        assert sess.query(func.max(foo.c.bar)).filter(foo.c.bar<30).one() == (29,)
        # Py3K
        #assert query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)).__next__()[0] == 29
        #assert query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)).__next__()[0] == 29
        # Py2K
        assert query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)).next()[0] == 29
        assert query.filter(foo.c.bar<30).values(sa.func.max(foo.c.bar)).next()[0] == 29
        # end Py2K
        
    @testing.resolve_artifact_names
    def test_aggregate_1(self):
        if (testing.against('mysql+mysqldb') and
            testing.db.dialect.dbapi.version_info[:4] == (1, 2, 1, 'gamma')):
            return

        query = create_session().query(func.sum(foo.c.bar))
        assert query.filter(foo.c.bar<30).one() == (435,)

    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.fails_on('mssql', 'AVG produces an average as the original column type on mssql.')
    @testing.resolve_artifact_names
    def test_aggregate_2(self):
        query = create_session().query(func.avg(foo.c.bar))
        avg = query.filter(foo.c.bar < 30).one()[0]
        eq_(float(round(avg, 1)), 14.5)

    @testing.fails_on('mssql', 'AVG produces an average as the original column type on mssql.')
    @testing.resolve_artifact_names
    def test_aggregate_3(self):
        query = create_session().query(Foo)

        # Py3K
        #avg_f = query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)).__next__()[0]
        # Py2K
        avg_f = query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)).next()[0]
        # end Py2K
        assert float(round(avg_f, 1)) == 14.5

        # Py3K
        #avg_o = query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)).__next__()[0]
        # Py2K
        avg_o = query.filter(foo.c.bar<30).values(sa.func.avg(foo.c.bar)).next()[0]
        # end Py2K
        assert float(round(avg_o, 1)) == 14.5

    @testing.resolve_artifact_names
    def test_filter(self):
        query = create_session().query(Foo)
        assert query.count() == 100
        assert query.filter(Foo.bar < 30).count() == 30
        res2 = query.filter(Foo.bar < 30).filter(Foo.bar > 10)
        assert res2.count() == 19

    @testing.resolve_artifact_names
    def test_options(self):
        query = create_session().query(Foo)
        class ext1(sa.orm.MapperExtension):
            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                instance.TEST = "hello world"
                return sa.orm.EXT_CONTINUE
        assert query.options(sa.orm.extension(ext1()))[0].TEST == "hello world"

    @testing.resolve_artifact_names
    def test_order_by(self):
        query = create_session().query(Foo)
        assert query.order_by(Foo.bar)[0].bar == 0
        assert query.order_by(sa.desc(Foo.bar))[0].bar == 99

    @testing.resolve_artifact_names
    def test_offset(self):
        query = create_session().query(Foo)
        assert list(query.order_by(Foo.bar).offset(10))[0].bar == 10

    @testing.resolve_artifact_names
    def test_offset(self):
        query = create_session().query(Foo)
        assert len(list(query.limit(10))) == 10


class GenerativeTest2(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('Table1', metadata,
              Column('id', Integer, primary_key=True))
        Table('Table2', metadata,
              Column('t1id', Integer, ForeignKey("Table1.id"),
                     primary_key=True),
              Column('num', Integer, primary_key=True))

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Obj1(_base.BasicEntity):
            pass
        class Obj2(_base.BasicEntity):
            pass

        mapper(Obj1, Table1)
        mapper(Obj2, Table2)

    @classmethod
    def fixtures(cls):
        return dict(
            Table1=(('id',),
                    (1,),
                    (2,),
                    (3,),
                    (4,)),
            Table2=(('num', 't1id'),
                    (1, 1),
                    (2, 1),
                    (3, 1),
                    (4, 2),
                    (5, 2),
                    (6, 3)))

    @testing.resolve_artifact_names
    def test_distinct_count(self):
        query = create_session().query(Obj1)
        eq_(query.count(), 4)

        res = query.filter(sa.and_(Table1.c.id == Table2.c.t1id,
                                   Table2.c.t1id == 1))
        eq_(res.count(), 3)
        res = query.filter(sa.and_(Table1.c.id == Table2.c.t1id,
                                   Table2.c.t1id == 1)).distinct()
        eq_(res.count(), 1)


class RelationshipsTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users, properties={
            'orders':relationship(mapper(Order, orders, properties={
                'addresses':relationship(mapper(Address, addresses))}))})


    @testing.resolve_artifact_names
    def test_join(self):
        """Query.join"""

        session = create_session()
        q = (session.query(User).join('orders', 'addresses').
             filter(Address.id == 1))
        eq_([User(id=7)], q.all())

    @testing.resolve_artifact_names
    def test_outer_join(self):
        """Query.outerjoin"""

        session = create_session()
        q = (session.query(User).outerjoin('orders', 'addresses').
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(set([User(id=7), User(id=8), User(id=10)]),
            set(q.all()))

    @testing.resolve_artifact_names
    def test_outer_join_count(self):
        """test the join and outerjoin functions on Query"""

        session = create_session()

        q = (session.query(User).outerjoin('orders', 'addresses').
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(q.count(), 4)

    @testing.resolve_artifact_names
    def test_from(self):
        session = create_session()

        sel = users.outerjoin(orders).outerjoin(
            addresses, orders.c.address_id == addresses.c.id)
        q = (session.query(User).select_from(sel).
             filter(sa.or_(Order.id == None, Address.id == 1)))
        eq_(set([User(id=7), User(id=8), User(id=10)]),
            set(q.all()))


class CaseSensitiveTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('Table1', metadata,
            Column('ID', Integer, primary_key=True))
        Table('Table2', metadata,
              Column('T1ID', Integer, ForeignKey("Table1.ID"),
                     primary_key=True),
              Column('NUM', Integer, primary_key=True))

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Obj1(_base.BasicEntity):
            pass
        class Obj2(_base.BasicEntity):
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

    @testing.resolve_artifact_names
    def test_distinct_count(self):
        q = create_session(bind=testing.db).query(Obj1)
        assert q.count() == 4
        res = q.filter(sa.and_(Table1.c.ID==Table2.c.T1ID,Table2.c.T1ID==1))
        assert res.count() == 3
        res = q.filter(sa.and_(Table1.c.ID==Table2.c.T1ID,Table2.c.T1ID==1)).distinct()
        eq_(res.count(), 1)


