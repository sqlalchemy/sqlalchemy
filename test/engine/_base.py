import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy.test.testing import adict


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

    @classmethod
    def setup_class(cls):
        if cls.run_setup_bind is None:
            assert cls.bind is not None
        assert cls.run_deletes in (None, 'each')
        if cls.run_inserts == 'once':
            assert cls.run_deletes is None

        if cls.tables is None:
            cls.tables = adict()
        if cls.other_artifacts is None:
            cls.other_artifacts = adict()

        if cls.bind is None:
            setattr(cls, 'bind', cls.setup_bind())

        if cls.metadata is None:
            setattr(cls, 'metadata', sa.MetaData())

        if cls.metadata.bind is None:
            cls.metadata.bind = cls.bind

        if cls.run_define_tables == 'once':
            cls.define_tables(cls.metadata)
            cls.metadata.create_all()
            cls.tables.update(cls.metadata.tables)

        if cls.run_inserts == 'once':
            cls._load_fixtures()
            cls.insert_data()

    def setup(self):
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

    def teardown(self):
        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != 'each' and self.run_deletes:
            for table in reversed(self.metadata.sorted_tables):
                try:
                    table.delete().execute().close()
                except sa.exc.DBAPIError, ex:
                    print >> sys.stderr, "Error emptying table %s: %r" % (
                        table, ex)

        if self.run_dispose_bind == 'each':
            self.dispose_bind(self.bind)

    @classmethod
    def teardown_class(cls):
        cls.metadata.drop_all()

        if cls.dispose_bind:
            cls.dispose_bind(cls.bind)

        cls.metadata.bind = None

        if cls.run_setup_bind is not None:
            cls.bind = None

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
        raise NotImplementedError()

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


class AltEngineTest(testing.TestBase):
    engine = None

    @classmethod
    def setup_class(cls):
        cls.engine = cls.create_engine()
        super(AltEngineTest, cls).setup_class()
        
    @classmethod
    def teardown_class(cls):
        cls.engine.dispose()
        cls.engine = None
        super(AltEngineTest, cls).teardown_class()
        
    @classmethod
    def create_engine(cls):
        raise NotImplementedError
