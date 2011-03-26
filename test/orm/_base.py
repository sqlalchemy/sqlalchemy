import inspect
import sys
import types
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from test.lib import config, testing
from test.lib.testing import adict
from test.lib.entities import BasicEntity, ComparableEntity
from test.engine._base import TablesTest

Entity = BasicEntity

class _ORMTest(object):
    __requires__ = ('subqueries',)

    @classmethod
    def teardown_class(cls):
        sa.orm.session.Session.close_all()
        sa.orm.clear_mappers()
        # TODO: ensure mapper registry is empty
        # TODO: ensure instrumentation registry is empty

class ORMTest(_ORMTest, testing.TestBase):
    pass

class MappedTest(_ORMTest, TablesTest, testing.AssertsExecutionResults):
    # 'once', 'each', None
    run_setup_classes = 'once'

    # 'once', 'each', None
    run_setup_mappers = 'each'

    classes = None

    @classmethod
    def setup_class(cls):
        cls._init_class()

        if cls.classes is None:
            cls.classes = adict()

        cls._setup_once_tables()

        cls._setup_once_classes()

        cls._setup_once_mappers()

        cls._setup_once_inserts()

    @classmethod
    def _setup_once_classes(cls):
        if cls.run_setup_classes == 'once':
            baseline = subclasses(BasicEntity)
            cls.setup_classes()
            cls._register_new_class_artifacts(baseline)

    @classmethod
    def _setup_once_mappers(cls):
        if cls.run_setup_mappers == 'once':
            baseline = subclasses(BasicEntity)
            cls.setup_mappers()
            cls._register_new_class_artifacts(baseline)

    def _setup_each_classes(self):
        if self.run_setup_classes == 'each':
            self.classes.clear()
            baseline = subclasses(BasicEntity)
            self.setup_classes()
            self._register_new_class_artifacts(baseline)

    def _setup_each_mappers(self):
        if self.run_setup_mappers == 'each':
            baseline = subclasses(BasicEntity)
            self.setup_mappers()
            self._register_new_class_artifacts(baseline)

    def _teardown_each_mappers(self):
        # some tests create mappers in the test bodies
        # and will define setup_mappers as None - 
        # clear mappers in any case
        if self.run_setup_mappers != 'once':
            sa.orm.clear_mappers()

    def setup(self):
        self._setup_each_tables()
        self._setup_each_classes()

        self._setup_each_mappers()
        self._setup_each_inserts()

    def teardown(self):
        sa.orm.session.Session.close_all()
        self._teardown_each_mappers()
        self._teardown_each_tables()
        self._teardown_each_bind()

    @classmethod
    def teardown_class(cls):
        for cl in cls.classes.values():
            cls.unregister_class(cl)
        _ORMTest.teardown_class()
        cls._teardown_once_metadata_bind()

    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        pass

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

