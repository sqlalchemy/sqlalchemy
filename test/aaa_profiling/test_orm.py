from test.lib.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy import exc as sa_exc, util, Integer, String, ForeignKey
from sqlalchemy.orm import exc as orm_exc, mapper, relationship, \
    sessionmaker, Session
from test.lib import testing, profiling
from test.orm import _base
from test.lib.schema import Table, Column
import sys

class MergeTest(_base.MappedTest):

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
        class Parent(_base.BasicEntity):
            pass

        class Child(_base.BasicEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Parent, parent, properties={'children'
               : relationship(Child, backref='parent')})
        mapper(Child, child)

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        parent.insert().execute({'id': 1, 'data': 'p1'})
        child.insert().execute({'id': 1, 'data': 'p1c1', 'parent_id'
                               : 1})

    @testing.resolve_artifact_names
    def test_merge_no_load(self):
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
    @testing.resolve_artifact_names
    def test_merge_load(self):
        sess = sessionmaker()()
        sess2 = sessionmaker()()
        p1 = sess.query(Parent).get(1)
        p1.children

        # preloading of collection took this down from 1728 to 1192
        # using sqlite3 the C extension took it back up to approx. 1257
        # (py2.6)

        @profiling.function_call_count(
                                versions={'2.5':1050, '2.6':1050,
                                        '2.6+cextension':1041, 
                                        '2.7':1005,
                                        '3':1005}
                            )
        def go():
            p2 = sess2.merge(p1)
        go()

        # one more time, count the SQL

        sess2 = sessionmaker()()
        self.assert_sql_count(testing.db, go, 2)

class LoadManyToOneFromIdentityTest(_base.MappedTest):
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
    __requires__ = 'python25',
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
        class Parent(_base.BasicEntity):
            pass

        class Child(_base.BasicEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Parent, parent, properties={
            'child': relationship(Child)})
        mapper(Child, child)

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
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

    @testing.resolve_artifact_names
    def test_many_to_one_load_no_identity(self):
        sess = Session()
        parents = sess.query(Parent).all()


        @profiling.function_call_count(108019, variance=.2)
        def go():
            for p in parents:
                p.child
        go()

    @testing.resolve_artifact_names
    def test_many_to_one_load_identity(self):
        sess = Session()
        parents = sess.query(Parent).all()
        children = sess.query(Child).all()

        @profiling.function_call_count(17987, {'3':18987})
        def go():
            for p in parents:
                p.child
        go()


