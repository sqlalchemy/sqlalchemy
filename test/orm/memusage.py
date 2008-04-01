import testenv; testenv.configure_for_tests()
import gc
from sqlalchemy import MetaData, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relation, clear_mappers, create_session
from sqlalchemy.orm.mapper import Mapper, _mapper_registry
from sqlalchemy.orm.session import _sessions 
from testlib import *
from testlib.fixtures import Base

class A(Base):pass
class B(Base):pass

def profile_memory(func):
    # run the test 50 times.  if length of gc.get_objects()
    # keeps growing, assert false
    def profile(*args):
        samples = []
        for x in range(0, 50):
            func(*args)
            gc.collect()
            samples.append(len(gc.get_objects()))
        print "sample gc sizes:", samples

        assert len(_sessions) == 0

        # TODO: this test only finds pure "growing" tests.
        # if a drop is detected, it's assumed that GC is able
        # to reduce memory.  better methodology would
        # make this more accurate.
        for i, x in enumerate(samples):
            if i < len(samples) - 1 and x < samples[i+1]:
                continue
        else:
            return
        assert False, repr(samples)
    return profile

def assert_no_mappers():
    clear_mappers()
    gc.collect()
    assert len(_mapper_registry) == 0

class EnsureZeroed(TestBase, AssertsExecutionResults):
    def setUp(self):
        _sessions.clear()
        _mapper_registry.clear()
        
class MemUsageTest(EnsureZeroed):

    def test_session(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1"))
            )

        metadata.create_all()

        m1 = mapper(A, table1, properties={
            "bs":relation(B, cascade="all, delete")
        })
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
                sess.save(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).all()
            self.assertEquals(
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

    def test_mapper_reset(self):
        metadata = MetaData(testing.db)

        table1 = Table("mytable", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1"))
            )

        @profile_memory
        def go():
            m1 = mapper(A, table1, properties={
                "bs":relation(B)
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
                sess.save(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).all()
            self.assertEquals(
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
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, ForeignKey('mytable.col1'), primary_key=True),
            Column('col3', String(30)),
            )

        @profile_memory
        def go():
            class A(Base):
                pass
            class B(A):
                pass

            mapper(A, table1, polymorphic_on=table1.c.col2, polymorphic_identity='a')
            mapper(B, table2, inherits=A, polymorphic_identity='b')

            sess = create_session()
            a1 = A()
            a2 = A()
            b1 = B(col3='b1')
            b2 = B(col3='b2')
            for x in [a1,a2,b1, b2]:
                sess.save(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).all()
            self.assertEquals(
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
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata,
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            )

        table3 = Table('t1tot2', metadata,
            Column('t1', Integer, ForeignKey('mytable.col1')),
            Column('t2', Integer, ForeignKey('mytable2.col1')),
            )

        @profile_memory
        def go():
            class A(Base):
                pass
            class B(Base):
                pass

            mapper(A, table1, properties={
                'bs':relation(B, secondary=table3, backref='as')
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
                sess.save(x)
            sess.flush()
            sess.clear()

            alist = sess.query(A).all()
            self.assertEquals(
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


if __name__ == '__main__':
    testenv.main()
