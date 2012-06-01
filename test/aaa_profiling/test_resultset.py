from sqlalchemy import *
from test.lib import *
from test.lib.testing import eq_
NUM_FIELDS = 10
NUM_RECORDS = 1000


class ResultSetTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = 'cpython',
    __only_on__ = 'sqlite'

    @classmethod
    def setup_class(cls):
        global t, t2, metadata
        metadata = MetaData(testing.db)
        t = Table('table', metadata, *[Column('field%d' % fnum, String)
                  for fnum in range(NUM_FIELDS)])
        t2 = Table('table2', metadata, *[Column('field%d' % fnum,
                   Unicode) for fnum in range(NUM_FIELDS)])

    def setup(self):
        metadata.create_all()
        t.insert().execute([dict(('field%d' % fnum, u'value%d' % fnum)
                           for fnum in range(NUM_FIELDS)) for r_num in
                           range(NUM_RECORDS)])
        t2.insert().execute([dict(('field%d' % fnum, u'value%d' % fnum)
                            for fnum in range(NUM_FIELDS)) for r_num in
                            range(NUM_RECORDS)])

        # warm up type caches
        t.select().execute().fetchall()
        t2.select().execute().fetchall()

    def teardown(self):
        metadata.drop_all()

    @profiling.function_call_count(versions={
                                    '2.4': 13214,
                                    '2.6':14416,
                                    '2.7':14416,
                                   '2.6+cextension': 365,
                                   '2.7+cextension':365})
    def test_string(self):
        [tuple(row) for row in t.select().execute().fetchall()]

    # sqlite3 returns native unicode.  so shouldn't be an increase here.

    @profiling.function_call_count(versions={
                                    '2.7':14396,
                                    '2.6':14396,
                                   '2.6+cextension': 365, 
                                   '2.7+cextension':365})
    def test_unicode(self):
        [tuple(row) for row in t2.select().execute().fetchall()]

    def test_contains_doesnt_compile(self):
        row = t.select().execute().first()
        c1 = Column('some column', Integer) + Column("some other column", Integer)
        @profiling.function_call_count(9, variance=.15)
        def go():
            c1 in row
        go()

class ExecutionTest(fixtures.TestBase):
    __requires__ = 'cpython',
    __only_on__ = 'sqlite'

    def test_minimal_connection_execute(self):
        # create an engine without any instrumentation.
        e = create_engine('sqlite://')
        c = e.connect()
        # ensure initial connect activities complete
        c.execute("select 1")

        @profiling.function_call_count(versions={'2.7':40, '2.6':40, '2.5':35, 
                                                    '2.4':21, '3':40}, 
                                            variance=.10)
        def go():
            c.execute("select 1")
        go()

    def test_minimal_engine_execute(self):
        # create an engine without any instrumentation.
        e = create_engine('sqlite://')
        # ensure initial connect activities complete
        e.execute("select 1")

        @profiling.function_call_count(versions={'2.4':41, '2.5':65, 
                                                    '2.6':65, '3':61,
                                                    '2.7':65,
                                                    '2.6+cextension':65}, 
                                            variance=.05)
        def go():
            e.execute("select 1")
        go()


class RowProxyTest(fixtures.TestBase):
    def _rowproxy_fixture(self, keys, processors, row):
        from sqlalchemy.engine.base import RowProxy
        class MockMeta(object):
            def __init__(self):
                pass

        metadata = MockMeta()

        keymap = {}
        for index, (keyobjs, processor, values) in \
            enumerate(zip(keys, processors, row)):
            for key in keyobjs:
                keymap[key] = (processor, key, index)
            keymap[index] = (processor, key, index)
        return RowProxy(metadata, row, processors, keymap)

    def _test_getitem_value_refcounts(self, seq_factory):
        import sys
        col1, col2 = object(), object()
        def proc1(value):
            return value
        value1, value2 = "x", "y"
        row = self._rowproxy_fixture(
            [(col1, "a"),(col2, "b")],
            [proc1, None],
            seq_factory([value1, value2])
        )

        v1_refcount = sys.getrefcount(value1)
        v2_refcount = sys.getrefcount(value2)
        for i in range(10):
            row[col1]
            row["a"]
            row[col2]
            row["b"]
            row[0]
            row[1]
            row[0:2]
        eq_(sys.getrefcount(value1), v1_refcount)
        eq_(sys.getrefcount(value2), v2_refcount)

    def test_value_refcounts_pure_tuple(self):
        self._test_getitem_value_refcounts(tuple)

    def test_value_refcounts_custom_seq(self):
        class CustomSeq(object):
            def __init__(self, data):
                self.data = data

            def __getitem__(self, item):
                return self.data[item]

            def __iter__(self):
                return iter(self.data)
        self._test_getitem_value_refcounts(CustomSeq)
