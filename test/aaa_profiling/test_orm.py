from test.lib.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy import exc as sa_exc, util, Integer, String, ForeignKey
from sqlalchemy.orm import exc as orm_exc, mapper, relationship, \
    sessionmaker, Session
from test.lib import testing, profiling
from test.lib import fixtures
from test.lib.schema import Table, Column
import sys

class MergeTest(fixtures.MappedTest):

    __requires__ = 'cpython',

    @classmethod
    def define_tables(cls, metadata):
        parent = Table('parent', metadata, Column('id', Integer,
                       primary_key=True,
                       test_needs_autoincrement=True), Column('data',
                       String(20)))
        child = Table('child', metadata, Column('id', Integer,
                      primary_key=True, test_needs_autoincrement=True),
                      Column('data', String(20)), Column('parent_id',
                      Integer, ForeignKey('parent.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                cls.classes.Parent,
                                cls.tables.parent,
                                cls.tables.child)

        mapper(Parent, parent, properties={'children'
               : relationship(Child, backref='parent')})
        mapper(Child, child)

    @classmethod
    def insert_data(cls):
        parent, child = cls.tables.parent, cls.tables.child

        parent.insert().execute({'id': 1, 'data': 'p1'})
        child.insert().execute({'id': 1, 'data': 'p1c1', 'parent_id'
                               : 1})

    def test_merge_no_load(self):
        Parent = self.classes.Parent

        sess = sessionmaker()()
        sess2 = sessionmaker()()
        p1 = sess.query(Parent).get(1)
        p1.children

        # down from 185 on this this is a small slice of a usually
        # bigger operation so using a small variance

        @profiling.function_call_count(variance=0.05,
                versions={'2.7':80, '2.6':80, '2.5':94, '3': 83})
        def go():
            return sess2.merge(p1, load=False)
        p2 = go()

        # third call, merge object already present. almost no calls.

        @profiling.function_call_count(variance=0.05,
                versions={'2.7':11, '2.6':11, '2.5':15, '3': 12})
        def go():
            return sess2.merge(p2, load=False)
        p3 = go()

    @testing.only_on('sqlite', 'Call counts tailored to pysqlite')
    def test_merge_load(self):
        Parent = self.classes.Parent

        sess = sessionmaker()()
        sess2 = sessionmaker()()
        p1 = sess.query(Parent).get(1)
        p1.children

        # preloading of collection took this down from 1728 to 1192
        # using sqlite3 the C extension took it back up to approx. 1257
        # (py2.6)

        @profiling.function_call_count(variance=0.10,
                                versions={'2.5':1050, '2.6':1050,
                                        '2.6+cextension':988, 
                                        '2.7':1005,
                                        '3':1050}
                            )
        def go():
            p2 = sess2.merge(p1)
        go()

        # one more time, count the SQL

        sess2 = sessionmaker(testing.db)()
        self.assert_sql_count(testing.db, go, 2)

class LoadManyToOneFromIdentityTest(fixtures.MappedTest):
    """test overhead associated with many-to-one fetches.

    Prior to the refactor of LoadLazyAttribute and 
    query._get(), the load from identity map took 2x
    as many calls (65K calls here instead of around 33K)
    to load 1000 related objects from the identity map.

    """

    # only need to test for unexpected variance in a large call 
    # count here,
    # so remove some platforms that have wildly divergent
    # callcounts.
    __requires__ = 'python25', 'cpython'
    __unsupported_on__ = 'postgresql+pg8000', 'mysql+pymysql'

    @classmethod
    def define_tables(cls, metadata):
        parent = Table('parent', metadata, 
                        Column('id', Integer, primary_key=True), 
                       Column('data', String(20)),
                       Column('child_id', Integer, ForeignKey('child.id'))
                       )

        child = Table('child', metadata, 
                    Column('id', Integer,primary_key=True),
                  Column('data', String(20))
                 )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                cls.classes.Parent,
                                cls.tables.parent,
                                cls.tables.child)

        mapper(Parent, parent, properties={
            'child': relationship(Child)})
        mapper(Child, child)

    @classmethod
    def insert_data(cls):
        parent, child = cls.tables.parent, cls.tables.child

        child.insert().execute([
            {'id':i, 'data':'c%d' % i}
            for i in xrange(1, 251)
        ])
        parent.insert().execute([
            {
                'id':i, 
                'data':'p%dc%d' % (i, (i % 250) + 1), 
                'child_id':(i % 250) + 1
            } 
            for i in xrange(1, 1000)
        ])

    def test_many_to_one_load_no_identity(self):
        Parent = self.classes.Parent

        sess = Session()
        parents = sess.query(Parent).all()


        @profiling.function_call_count(108019, variance=.2)
        def go():
            for p in parents:
                p.child
        go()

    def test_many_to_one_load_identity(self):
        Parent, Child = self.classes.Parent, self.classes.Child

        sess = Session()
        parents = sess.query(Parent).all()
        children = sess.query(Child).all()

        @profiling.function_call_count(17987, {'3':18987})
        def go():
            for p in parents:
                p.child
        go()

class MergeBackrefsTest(fixtures.MappedTest):
    __only_on__ = 'sqlite'  # keep things simple

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
            Column('id', Integer, primary_key=True), 
            Column('c_id', Integer, ForeignKey('c.id'))
        )
        Table('b', metadata,
            Column('id', Integer, primary_key=True), 
            Column('a_id', Integer, ForeignKey('a.id'))
        )
        Table('c', metadata,
            Column('id', Integer, primary_key=True), 
        )
        Table('d', metadata,
            Column('id', Integer, primary_key=True), 
            Column('a_id', Integer, ForeignKey('a.id'))
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass
        class B(cls.Basic):
            pass
        class C(cls.Basic):
            pass
        class D(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, \
                    cls.classes.C, cls.classes.D
        a, b, c, d= cls.tables.a, cls.tables.b, \
                    cls.tables.c, cls.tables.d
        mapper(A, a, properties={
            'bs':relationship(B, backref='a'),
            'c':relationship(C, backref='as'),
            'ds':relationship(D, backref='a'),
        })
        mapper(B, b)
        mapper(C, c)
        mapper(D, d)

    @classmethod
    def insert_data(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, \
                    cls.classes.C, cls.classes.D
        s = Session()
        s.add_all([
            A(id=i, 
                bs=[B(id=(i * 50) + j) for j in xrange(1, 50)],
                c=C(id=i),
                ds=[D(id=(i * 50) + j) for j in xrange(1, 50)]
            )
            for i in xrange(1, 50)
        ])
        s.commit()

    @profiling.function_call_count(1092497, variance=.10)
    def test_merge_pending_with_all_pks(self):
        A, B, C, D = self.classes.A, self.classes.B, \
                    self.classes.C, self.classes.D
        s = Session()
        for a in [
            A(id=i, 
                bs=[B(id=(i * 50) + j) for j in xrange(1, 50)],
                c=C(id=i),
                ds=[D(id=(i * 50) + j) for j in xrange(1, 50)]
            )
            for i in xrange(1, 50)
        ]:
            s.merge(a)


