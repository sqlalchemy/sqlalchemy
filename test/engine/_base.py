import sqlalchemy as sa
from test.lib import testing
from test.lib.testing import adict
from test.lib.engines import drop_all_tables


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

    bind = None
    metadata = None
    tables = None
    other = None

    @classmethod
    def setup_class(cls):
        cls._init_class()

        cls._setup_once_tables()

        cls._setup_once_inserts()

    @classmethod
    def _init_class(cls):
        if cls.other is None:
            cls.other = adict()

        if cls.tables is None:
            cls.tables = adict()

        if cls.bind is None:
            setattr(cls, 'bind', cls.setup_bind())

        if cls.metadata is None:
            setattr(cls, 'metadata', sa.MetaData())

        if cls.metadata.bind is None:
            cls.metadata.bind = cls.bind

    @classmethod
    def _setup_once_inserts(cls):
        if cls.run_inserts == 'once':
            cls._load_fixtures()
            cls.insert_data()

    @classmethod
    def _setup_once_tables(cls):
        if cls.run_define_tables == 'once':
            cls.define_tables(cls.metadata)
            cls.metadata.create_all()
            cls.tables.update(cls.metadata.tables)

    def _setup_each_tables(self):
        if self.run_define_tables == 'each':
            self.tables.clear()
            drop_all_tables(self.metadata)
            self.metadata.clear()
            self.define_tables(self.metadata)
            self.metadata.create_all()
            self.tables.update(self.metadata.tables)

    def _setup_each_inserts(self):
        if self.run_inserts == 'each':
            self._load_fixtures()
            self.insert_data()

    def _teardown_each_tables(self):
        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != 'each' and self.run_deletes:
            for table in reversed(self.metadata.sorted_tables):
                try:
                    table.delete().execute().close()
                except sa.exc.DBAPIError, ex:
                    print >> sys.stderr, "Error emptying table %s: %r" % (
                        table, ex)

    def _setup_each_bind(self):
        if self.setup_bind == 'each':
            setattr(cls, 'bind', self.setup_bind())

    def _teardown_each_bind(self):
        if self.run_dispose_bind == 'each':
            self.dispose_bind(self.bind)

    def setup(self):
        self._setup_each_bind()
        self._setup_each_tables()
        self._setup_each_inserts()

    def teardown(self):
        self._teardown_each_tables()
        self._teardown_each_bind()

    @classmethod
    def _teardown_once_metadata_bind(cls):
        cls.metadata.drop_all()

        if cls.run_dispose_bind == 'once':
            cls.dispose_bind(cls.bind)

        cls.metadata.bind = None

        if cls.run_setup_bind is not None:
            cls.bind = None

    @classmethod
    def teardown_class(cls):
        cls._teardown_once_metadata_bind()

    @classmethod
    def setup_bind(cls):
        return testing.db

    @classmethod
    def dispose_bind(cls, bind):
        if hasattr(bind, 'dispose'):
            bind.dispose()
        elif hasattr(bind, 'close'):
            bind.close()

    @classmethod
    def define_tables(cls, metadata):
        pass

    @classmethod
    def fixtures(cls):
        return {}

    @classmethod
    def insert_data(cls):
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
        for table in self.metadata.sorted_tables:
            if table not in headers:
                continue
            table.bind.execute(
                table.insert(),
                [dict(zip(headers[table], column_values))
                 for column_values in rows[table]])


