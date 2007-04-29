# coding: utf-8
import testbase

from sqlalchemy import *

"""verrrrry basic unicode column name testing"""

class UnicodeSchemaTest(testbase.PersistTest):
    def setUpAll(self):
        global metadata, t1, t2
        metadata = MetaData(engine=testbase.db)
        t1 = Table('unitable1', metadata,
            Column(u'méil', Integer, primary_key=True),
            Column(u'éXXm', Integer),

            )
        t2 = Table(u'unitéble2', metadata,
            Column(u'méil', Integer, primary_key=True, key="a"),
            Column(u'éXXm', Integer, ForeignKey(u'unitable1.méil'), key="b"),

            )
        metadata.create_all()

    def tearDown(self):
        t2.delete().execute()
        t1.delete().execute()
        
    def tearDownAll(self):
        metadata.drop_all()
        
    def test_insert(self):
        t1.insert().execute({u'méil':1, u'éXXm':5})
        t2.insert().execute({'a':1, 'b':1})
        
        assert t1.select().execute().fetchall() == [(1, 5)]
        assert t2.select().execute().fetchall() == [(1, 1)]
    
    def test_reflect(self):
        t1.insert().execute({u'méil':2, u'éXXm':7})
        t2.insert().execute({'a':2, 'b':2})

        meta = BoundMetaData(testbase.db)
        tt1 = Table(t1.name, meta, autoload=True)
        tt2 = Table(t2.name, meta, autoload=True)
        tt1.insert().execute({u'méil':1, u'éXXm':5})
        tt2.insert().execute({u'méil':1, u'éXXm':1})

        assert tt1.select(order_by=desc(u'méil')).execute().fetchall() == [(2, 7), (1, 5)]
        assert tt2.select(order_by=desc(u'méil')).execute().fetchall() == [(2, 2), (1, 1)]
        
    def test_mapping(self):
        # TODO: this test should be moved to the ORM tests, tests should be
        # added to this module testing SQL syntax and joins, etc.
        class A(object):pass
        class B(object):pass
        
        mapper(A, t1, properties={
            't2s':relation(B),
            'a':t1.c[u'méil'],
            'b':t1.c[u'éXXm']
        })
        mapper(B, t2)
        sess = create_session()
        a1 = A()
        b1 = B()
        a1.t2s.append(b1)
        sess.save(a1)
        sess.flush()
        sess.clear()
        new_a1 = sess.query(A).selectone(t1.c[u'méil'] == a1.a)
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].a == b1.a
        sess.clear()
        new_a1 = sess.query(A).options(eagerload('t2s')).selectone(t1.c[u'méil'] == a1.a)
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].a == b1.a
        
if __name__ == '__main__':
    testbase.main()