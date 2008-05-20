from testlib import sa, testing
from testlib.testing import adict


class TablesTest(testing.TestBase):
    """An integration test that creates and uses tables."""

    # 'once', 'each', None
    run_setup_bind = 'once'

    # 'once', 'each', None
    run_define_tables = 'once'

    # 'once', 'each', None
    run_inserts = 'each'

    # 'foreach', None
    run_deletes = 'each'

    # 'once', 'each', None
    run_dispose_bind = None

    _artifact_registries = ('tables', 'other_artifacts')

    bind = None
    metadata = None
    tables = None
    other_artifacts = None

    def setUpAll(self):
        if self.run_setup_bind is None:
            assert self.bind is not None
        assert self.run_deletes in (None, 'each')
        if self.run_inserts == 'once':
            assert self.run_deletes is None

        cls = self.__class__
        if cls.tables is None:
            cls.tables = adict()
        if cls.other_artifacts is None:
            cls.other_artifacts = adict()

        if self.bind is None:
            setattr(type(self), 'bind', self.setup_bind())

        if self.metadata is None:
            setattr(type(self), 'metadata', sa.MetaData())

        if self.metadata.bind is None:
            self.metadata.bind = self.bind

        if self.run_define_tables:
            self.define_tables(self.metadata)
            self.metadata.create_all()
            self.tables.update(self.metadata.tables)

        if self.run_inserts:
            self._load_fixtures()
            self.insert_data()

    def setUp(self):
        if self._sa_first_test:
            return

        cls = self.__class__

        if self.setup_bind == 'each':
            setattr(cls, 'bind', self.setup_bind())

        if self.run_define_tables == 'each':
            self.tables.clear()
            self.metadata.drop_all()
            self.metadata.clear()
            self.define_tables(self.metadata)
            self.metadata.create_all()
            self.tables.update(self.metadata.tables)

        if self.run_inserts == 'each':
            self._load_fixtures()
            self.insert_data()

    def tearDown(self):
        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != 'each' and self.run_deletes:
            for table in self.metadata.table_iterator(reverse=True):
                try:
                    table.delete().execute().close()
                except sa.exc.DBAPIError, ex:
                    print >> sys.stderr, "Error emptying table %s: %r" % (
                        table, ex)

        if self.run_dispose_bind == 'each':
            self.dispose_bind(self.bind)

    def tearDownAll(self):
        self.metadata.drop_all()

        if self.dispose_bind:
            self.dispose_bind(self.bind)

        self.metadata.bind = None

        if self.run_setup_bind is not None:
            self.bind = None

    def setup_bind(self):
        return testing.db

    def dispose_bind(self, bind):
        if hasattr(bind, 'dispose'):
            bind.dispose()
        elif hasattr(bind, 'close'):
            bind.close()

    def define_tables(self, metadata):
        raise NotImplementedError()

    def fixtures(self):
        return {}

    def insert_data(self):
        pass

    def sql_count_(self, count, fn):
        self.assert_sql_count(self.bind, fn, count)

    def sql_eq_(self, callable_, statements, with_sequences=None):
        self.assert_sql(self.bind,
                        callable_, statements, with_sequences)

    def _load_fixtures(self):
        headers, rows = {}, {}
        for table, data in self.fixtures().iteritems():
            if isinstance(table, basestring):
                table = self.tables[table]
            headers[table] = data[0]
            rows[table] = data[1:]
        for table in self.metadata.table_iterator(reverse=False):
            if table not in headers:
                continue
            table.bind.execute(
                table.insert(),
                [dict(zip(headers[table], column_values))
                 for column_values in rows[table]])


class AltEngineTest(testing.TestBase):
    engine = None

    def setUpAll(self):
        type(self).engine = self.create_engine()
        testing.TestBase.setUpAll(self)

    def tearDownAll(self):
        testing.TestBase.tearDownAll(self)
        self.engine.dispose()
        type(self).engine = None

    def create_engine(self):
        raise NotImplementedError
