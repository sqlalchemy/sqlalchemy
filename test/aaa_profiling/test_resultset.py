from sqlalchemy import *
from sqlalchemy.test import *
NUM_FIELDS = 10
NUM_RECORDS = 1000


class ResultSetTest(TestBase, AssertsExecutionResults):

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

    def teardown(self):
        metadata.drop_all()

    @profiling.function_call_count(14416, versions={'2.4': 13214,
                                   '2.6+cextension': 409})
    def test_string(self):
        [tuple(row) for row in t.select().execute().fetchall()]

    # sqlite3 returns native unicode.  so shouldn't be an increase here.

    @profiling.function_call_count(14396, versions={'2.4': 13214,
                                   '2.6+cextension': 409})
    def test_unicode(self):
        [tuple(row) for row in t2.select().execute().fetchall()]
