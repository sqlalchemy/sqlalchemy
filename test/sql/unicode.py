# coding: utf-8
"""verrrrry basic unicode column name testing"""

import testbase
from sqlalchemy import *
from sqlalchemy.orm import mapper, relation, create_session, eagerload
from testbase import Table, Column


class UnicodeSchemaTest(testbase.PersistTest):
    def setUpAll(self):
        global unicode_bind, metadata, t1, t2

        unicode_bind = self._unicode_bind()

        metadata = MetaData(unicode_bind)
        t1 = Table('unitable1', metadata,
            Column(u'méil', Integer, primary_key=True),
            Column(u'\u6e2c\u8a66', Integer),

            )
        t2 = Table(u'Unitéble2', metadata,
            Column(u'méil', Integer, primary_key=True, key="a"),
            Column(u'\u6e2c\u8a66', Integer, ForeignKey(u'unitable1.méil'), key="b"),
            )
        metadata.create_all()

    def tearDown(self):
        t2.delete().execute()
        t1.delete().execute()
        
    def tearDownAll(self):
        global unicode_bind
        metadata.drop_all()
        del unicode_bind

    def _unicode_bind(self):
        if testbase.db.name != 'mysql':
            return testbase.db
        else:
            # most mysql installations don't default to utf8 connections
            version = testbase.db.dialect.get_version_info(testbase.db)
            if version < (4, 1):
                raise AssertionError("Unicode not supported on MySQL < 4.1")

            c = testbase.db.connect()
            if not hasattr(c.connection.connection, 'set_character_set'):
                raise AssertionError(
                    "Unicode not supported on this MySQL-python version")
            else: 
                c.connection.set_character_set('utf8')
                c.detach()

            return c
        
    def test_insert(self):
        t1.insert().execute({u'méil':1, u'\u6e2c\u8a66':5})
        t2.insert().execute({'a':1, 'b':1})
        
        assert t1.select().execute().fetchall() == [(1, 5)]
        assert t2.select().execute().fetchall() == [(1, 1)]
    
    def test_reflect(self):
        t1.insert().execute({u'méil':2, u'\u6e2c\u8a66':7})
        t2.insert().execute({'a':2, 'b':2})

        meta = MetaData(unicode_bind)
        tt1 = Table(t1.name, meta, autoload=True)
        tt2 = Table(t2.name, meta, autoload=True)

        tt1.insert().execute({u'méil':1, u'\u6e2c\u8a66':5})
        tt2.insert().execute({u'méil':1, u'\u6e2c\u8a66':1})

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
            'b':t1.c[u'\u6e2c\u8a66']
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
