from test.lib.testing import eq_
from sqlalchemy.orm import mapper, relationship, create_session, \
    clear_mappers, sessionmaker, class_mapper
from sqlalchemy.orm.mapper import _mapper_registry
from sqlalchemy.orm.session import _sessions
import operator
from test.lib import testing, engines
from sqlalchemy import MetaData, Integer, String, ForeignKey, \
    PickleType, create_engine, Unicode
from test.lib.schema import Table, Column
import sqlalchemy as sa
from sqlalchemy.sql import column
from sqlalchemy.processors import to_decimal_processor_factory, \
    to_unicode_processor_factory
from test.lib.util import gc_collect
from sqlalchemy.util.compat import decimal
import gc
import weakref
from test.lib import fixtures

class A(fixtures.ComparableEntity):
    pass
class B(fixtures.ComparableEntity):
    pass

def profile_memory(func):
    # run the test 50 times.  if length of gc.get_objects()
    # keeps growing, assert false

    def profile(*args):
        gc_collect()
        samples = [0 for x in range(0, 50)]
        for x in range(0, 50):
            func(*args)
            gc_collect()
            samples[x] = len(gc.get_objects())

        print "sample gc sizes:", samples

        assert len(_sessions) == 0

        for x in samples[-4:]:
            if x != samples[-5]:
                flatline = False
                break
        else:
            flatline = True

        # object count is bigger than when it started
        if not flatline and samples[-1] > samples[0]:
            for x in samples[1:-2]:
                # see if a spike bigger than the endpoint exists
                if x > samples[-1]:
                    break
            else:
                assert False, repr(samples) + " " + repr(flatline)

    return profile

def assert_no_mappers():
    clear_mappers()
    gc_collect()
    assert len(_mapper_registry) == 0

class EnsureZeroed(fixtures.ORMTest):
    def setup(self):
        _sessions.clear()
        _mapper_registry.clear()

class MemUsageTest(EnsureZeroed):

    __requires__ = 'cpython',

    # ensure a pure growing test trips the assertion
    @testing.fails_if(lambda: True)
    def test_fixture(self):
        class Foo(object):
            pass

        x = []
        @profile_memory
        def go():
            x[-1:] = [Foo(), Foo(), Foo(), Foo(), Foo(), Foo()]
        go()

    def test_session(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('col2', String(30)))

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1")))

        metadata.create_all()

        m1 = mapper(A, table1, properties={
            "bs":relationship(B, cascade="all, delete",
                                    order_by=table2.c.col1)},
            order_by=table1.c.col1)
        m2 = mapper(B, table2)

        m3 = mapper(A, table1, non_primary=True)

        @profile_memory
        def go():
            sess = create_session()
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1,a2,a3]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).all()
            eq_(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()
        go()

        metadata.drop_all()
        del m1, m2, m3
        assert_no_mappers()

    @testing.crashes('sqlite', ':memory: connection not suitable here')
    def test_orm_many_engines(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('col2', String(30)))

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1")))

        metadata.create_all()

        m1 = mapper(A, table1, properties={
            "bs":relationship(B, cascade="all, delete",
                                    order_by=table2.c.col1)},
            order_by=table1.c.col1,
            _compiled_cache_size=10
            )
        m2 = mapper(B, table2,
            _compiled_cache_size=10
        )

        m3 = mapper(A, table1, non_primary=True)

        @profile_memory
        def go():
            engine = engines.testing_engine(
                                options={'logging_name':'FOO',
                                        'pool_logging_name':'BAR',
                                        'use_reaper':False}
                                    )
            sess = create_session(bind=engine)

            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1,a2,a3]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).all()
            eq_(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()
            sess.close()
            engine.dispose()
        go()

        metadata.drop_all()
        del m1, m2, m3
        assert_no_mappers()

    def test_ad_hoc_types(self):
        """test storage of bind processors, result processors
        in dialect-wide registry."""

        from sqlalchemy.dialects import mysql, postgresql, sqlite
        from sqlalchemy import types

        eng = engines.testing_engine()
        for args in (
            (types.Integer, ),
            (types.String, ),
            (types.PickleType, ),
            (types.Enum, 'a', 'b', 'c'),
            (sqlite.DATETIME, ),
            (postgresql.ENUM, 'a', 'b', 'c'),
            (types.Interval, ),
            (postgresql.INTERVAL, ),
            (mysql.VARCHAR, ),
        ):
            @profile_memory
            def go():
                type_ = args[0](*args[1:])
                bp = type_._cached_bind_processor(eng.dialect)
                rp = type_._cached_result_processor(eng.dialect, 0)
            go()

        assert not eng.dialect._type_memos


    def test_many_updates(self):
        metadata = MetaData(testing.db)

        wide_table = Table('t', metadata,
            Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
            *[Column('col%d' % i, Integer) for i in range(10)]
        )

        class Wide(object):
            pass

        mapper(Wide, wide_table, _compiled_cache_size=10)

        metadata.create_all()
        session = create_session()
        w1 = Wide()
        session.add(w1)
        session.flush()
        session.close()
        del session
        counter = [1]

        @profile_memory
        def go():
            session = create_session()
            w1 = session.query(Wide).first()
            x = counter[0]
            dec = 10
            while dec > 0:
                # trying to count in binary here, 
                # works enough to trip the test case
                if pow(2, dec) < x:
                    setattr(w1, 'col%d' % dec, counter[0])
                    x -= pow(2, dec)
                dec -= 1
            session.flush()
            session.close()
            counter[0] += 1

        try:
            go()
        finally:
            metadata.drop_all()

    @testing.fails_if(lambda : testing.db.dialect.name == 'sqlite' \
                      and testing.db.dialect.dbapi.version_info >= (2,
                      5),
                      'Newer pysqlites generate warnings here too and '
                      'have similar issues.')
    def test_unicode_warnings(self):
        metadata = MetaData(testing.db)
        table1 = Table('mytable', metadata, Column('col1', Integer,
                       primary_key=True,
                       test_needs_autoincrement=True), Column('col2',
                       Unicode(30)))
        metadata.create_all()
        i = [1]

        @testing.emits_warning()
        @profile_memory
        def go():

            # execute with a non-unicode object. a warning is emitted,
            # this warning shouldn't clog up memory.

            testing.db.execute(table1.select().where(table1.c.col2
                               == 'foo%d' % i[0]))
            i[0] += 1
        try:
            go()
        finally:
            metadata.drop_all()

    def test_mapper_reset(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                                test_needs_autoincrement=True),
            Column('col2', String(30)))

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True,
                                test_needs_autoincrement=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1")))

        @profile_memory
        def go():
            m1 = mapper(A, table1, properties={
                "bs":relationship(B, order_by=table2.c.col1)
            })
            m2 = mapper(B, table2)

            m3 = mapper(A, table1, non_primary=True)

            sess = create_session()
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1,a2,a3]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()
            sess.close()
            clear_mappers()

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()

    def test_with_inheritance(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, ForeignKey('mytable.col1'),
                   primary_key=True, test_needs_autoincrement=True),
            Column('col3', String(30)),
            )

        @profile_memory
        def go():
            class A(fixtures.ComparableEntity):
                pass
            class B(A):
                pass

            mapper(A, table1,
                   polymorphic_on=table1.c.col2,
                   polymorphic_identity='a')
            mapper(B, table2,
                   inherits=A,
                   polymorphic_identity='b')

            sess = create_session()
            a1 = A()
            a2 = A()
            b1 = B(col3='b1')
            b2 = B(col3='b2')
            for x in [a1,a2,b1, b2]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_(
                [
                    A(), A(), B(col3='b1'), B(col3='b2')
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # dont need to clear_mappers()
            del B
            del A

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()

    def test_with_manytomany(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('col2', String(30)),
            )

        table3 = Table('t1tot2', metadata,
            Column('t1', Integer, ForeignKey('mytable.col1')),
            Column('t2', Integer, ForeignKey('mytable2.col1')),
            )

        @profile_memory
        def go():
            class A(fixtures.ComparableEntity):
                pass
            class B(fixtures.ComparableEntity):
                pass

            mapper(A, table1, properties={
                'bs':relationship(B, secondary=table3, 
                                    backref='as', order_by=table3.c.t1)
            })
            mapper(B, table2)

            sess = create_session()
            a1 = A(col2='a1')
            a2 = A(col2='a2')
            b1 = B(col2='b1')
            b2 = B(col2='b2')
            a1.bs.append(b1)
            a2.bs.append(b2)
            for x in [a1,a2]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_(
                [
                    A(bs=[B(col2='b1')]), A(bs=[B(col2='b2')])
                ],
                alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # dont need to clear_mappers()
            del B
            del A

        metadata.create_all()
        try:
            go()
        finally:
            metadata.drop_all()
        assert_no_mappers()

    # fails on newer versions of pysqlite due to unusual memory behvior
    # in pysqlite itself. background at:
    # http://thread.gmane.org/gmane.comp.python.db.pysqlite.user/2290

    @testing.fails_if(lambda : testing.db.dialect.name == 'sqlite' \
                      and testing.db.dialect.dbapi.version > '2.5')
    def test_join_cache(self):
        metadata = MetaData(testing.db)
        table1 = Table('table1', metadata, Column('id', Integer,
                       primary_key=True,
                       test_needs_autoincrement=True), Column('data',
                       String(30)))
        table2 = Table('table2', metadata, Column('id', Integer,
                       primary_key=True,
                       test_needs_autoincrement=True), Column('data',
                       String(30)), Column('t1id', Integer,
                       ForeignKey('table1.id')))

        class Foo(object):
            pass

        class Bar(object):
            pass

        mapper(Foo, table1, properties={'bars'
               : relationship(mapper(Bar, table2))})
        metadata.create_all()
        session = sessionmaker()

        @profile_memory
        def go():
            s = table2.select()
            sess = session()
            sess.query(Foo).join((s, Foo.bars)).all()
            sess.rollback()
        try:
            go()
        finally:
            metadata.drop_all()


    def test_mutable_identity(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True,
                                test_needs_autoincrement=True),
            Column('col2', PickleType(comparator=operator.eq, mutable=True))
            )

        class Foo(object):
            def __init__(self, col2):
                self.col2 = col2

        mapper(Foo, table1)
        metadata.create_all()

        session = sessionmaker()()

        def go():
            obj = [
                Foo({'a':1}),
                Foo({'b':1}),
                Foo({'c':1}),
                Foo({'d':1}),
                Foo({'e':1}),
                Foo({'f':1}),
                Foo({'g':1}),
                Foo({'h':1}),
                Foo({'i':1}),
                Foo({'j':1}),
                Foo({'k':1}),
                Foo({'l':1}),
            ]

            session.add_all(obj)
            session.commit()

            testing.eq_(len(session.identity_map._mutable_attrs), 12)
            testing.eq_(len(session.identity_map), 12)
            obj = None
            gc_collect()
            testing.eq_(len(session.identity_map._mutable_attrs), 0)
            testing.eq_(len(session.identity_map), 0)

        try:
            go()
        finally:
            metadata.drop_all()

    def test_type_compile(self):
        from sqlalchemy.dialects.sqlite.base import dialect as SQLiteDialect
        cast = sa.cast(column('x'), sa.Integer)
        @profile_memory
        def go():
            dialect = SQLiteDialect()
            cast.compile(dialect=dialect)
        go()

    @testing.requires.cextensions
    def test_DecimalResultProcessor_init(self):
        @profile_memory
        def go():
            to_decimal_processor_factory({}, 10)
        go()

    @testing.requires.cextensions
    def test_DecimalResultProcessor_process(self):
        @profile_memory
        def go():
            to_decimal_processor_factory(decimal.Decimal, 10)(1.2)
        go()

    @testing.requires.cextensions
    def test_UnicodeResultProcessor_init(self):
        @profile_memory
        def go():
            to_unicode_processor_factory('utf8')
        go()


