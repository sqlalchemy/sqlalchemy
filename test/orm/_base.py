import inspect
import sys
import types
import sqlalchemy as sa
import sqlalchemy.exceptions as sa_exc
from sqlalchemy.test import config, testing
from sqlalchemy.test.testing import resolve_artifact_names, adict
from sqlalchemy.test.engines import drop_all_tables
from sqlalchemy.util import function_named
from sqlalchemy.test.entities import BasicEntity, ComparableEntity

Entity = BasicEntity


class ORMTest(testing.TestBase, testing.AssertsExecutionResults):
    __requires__ = ('subqueries',)

    @classmethod
    def teardown_class(cls):
        sa.orm.session.Session.close_all()
        sa.orm.clear_mappers()
        # TODO: ensure mapper registry is empty
        # TODO: ensure instrumentation registry is empty

class MappedTest(ORMTest):
    # 'once', 'each', None
    run_define_tables = 'once'

    # 'once', 'each', None
    run_setup_classes = 'once'

    # 'once', 'each', None
    run_setup_mappers = 'each'

    # 'once', 'each', None
    run_inserts = 'each'

    # 'each', None
    run_deletes = 'each'

    metadata = None

    _artifact_registries = ('tables', 'classes', 'other_artifacts')
    tables = None
    classes = None
    other_artifacts = None

    @classmethod
    def setup_class(cls):
        if cls.run_setup_classes == 'each':
            assert cls.run_setup_mappers != 'once'

        assert cls.run_deletes in (None, 'each')
        if cls.run_inserts == 'once':
            assert cls.run_deletes is None

        assert not hasattr(cls, 'keep_mappers')
        assert not hasattr(cls, 'keep_data')

        if cls.tables is None:
            cls.tables = adict()
        if cls.classes is None:
            cls.classes = adict()
        if cls.other_artifacts is None:
            cls.other_artifacts = adict()

        if cls.metadata is None:
            setattr(cls, 'metadata', sa.MetaData())

        if cls.metadata.bind is None:
            cls.metadata.bind = getattr(cls, 'engine', config.db)

        if cls.run_define_tables == 'once':
            cls.define_tables(cls.metadata)
            cls.metadata.create_all()
            cls.tables.update(cls.metadata.tables)

        if cls.run_setup_classes == 'once':
            baseline = subclasses(BasicEntity)
            cls.setup_classes()
            cls._register_new_class_artifacts(baseline)

        if cls.run_setup_mappers == 'once':
            baseline = subclasses(BasicEntity)
            cls.setup_mappers()
            cls._register_new_class_artifacts(baseline)

        if cls.run_inserts == 'once':
            cls._load_fixtures()
            cls.insert_data()

    def setup(self):
        if self.run_define_tables == 'each':
            self.tables.clear()
            drop_all_tables(self.metadata)
            self.metadata.clear()
            self.define_tables(self.metadata)
            self.metadata.create_all()
            self.tables.update(self.metadata.tables)

        if self.run_setup_classes == 'each':
            self.classes.clear()
            baseline = subclasses(BasicEntity)
            self.setup_classes()
            self._register_new_class_artifacts(baseline)

        if self.run_setup_mappers == 'each':
            baseline = subclasses(BasicEntity)
            self.setup_mappers()
            self._register_new_class_artifacts(baseline)

        if self.run_inserts == 'each':
            self._load_fixtures()
            self.insert_data()

    def teardown(self):
        sa.orm.session.Session.close_all()

        # some tests create mappers in the test bodies
        # and will define setup_mappers as None - 
        # clear mappers in any case
        if self.run_setup_mappers != 'once':
            sa.orm.clear_mappers()

        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != 'each' and self.run_deletes:
            for table in reversed(self.metadata.sorted_tables):
                try:
                    table.delete().execute().close()
                except sa.exc.DBAPIError, ex:
                    print >> sys.stderr, "Error emptying table %s: %r" % (
                        table, ex)

    @classmethod
    def teardown_class(cls):
        for cl in cls.classes.values():
            cls.unregister_class(cl)
        ORMTest.teardown_class()
        drop_all_tables(cls.metadata)
        cls.metadata.bind = None

    @classmethod
    def define_tables(cls, metadata):
        raise NotImplementedError()

    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        pass

    @classmethod
    def fixtures(cls):
        return {}

    @classmethod
    def insert_data(cls):
        pass

    def sql_count_(self, count, fn):
        self.assert_sql_count(self.metadata.bind, fn, count)

    def sql_eq_(self, callable_, statements, with_sequences=None):
        self.assert_sql(self.metadata.bind,
                        callable_, statements, with_sequences)

    @classmethod
    def _register_new_class_artifacts(cls, baseline):
        for class_ in subclasses(BasicEntity) - baseline:
            cls.register_class(class_)

    @classmethod
    def register_class(cls, class_):
        name = class_.__name__
        if name[0].isupper:
            setattr(cls, name, class_)
        cls.classes[name] = class_

    @classmethod
    def unregister_class(cls, class_):
        name = class_.__name__
        if name[0].isupper:
            delattr(cls, name)
        del cls.classes[name]
    
    @classmethod
    def _load_fixtures(cls):
        headers, rows = {}, {}
        for table, data in cls.fixtures().iteritems():
            if isinstance(table, basestring):
                table = cls.tables[table]
            headers[table] = data[0]
            rows[table] = data[1:]
        for table in cls.metadata.sorted_tables:
            if table not in headers:
                continue
            table.bind.execute(
                table.insert(),
                [dict(zip(headers[table], column_values))
                 for column_values in rows[table]])


def subclasses(cls):
    subs, process = set(), set(cls.__subclasses__())
    while process:
        cls = process.pop()
        if cls not in subs:
            subs.add(cls)
            process |= set(cls.__subclasses__())
    return subs

