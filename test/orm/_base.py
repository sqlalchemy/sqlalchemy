import inspect
import sys
import types
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from test.lib import config, testing
from test.lib.testing import adict
from test.lib.entities import BasicEntity, ComparableEntity
from test.engine._base import TablesTest

class _ORMTest(object):
    __requires__ = ('subqueries',)

    @classmethod
    def teardown_class(cls):
        sa.orm.session.Session.close_all()
        sa.orm.clear_mappers()

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
    def teardown_class(cls):
        cls.classes.clear()
        _ORMTest.teardown_class()
        cls._teardown_once_metadata_bind()

    def setup(self):
        self._setup_each_tables()
        self._setup_each_mappers()
        self._setup_each_inserts()

    def teardown(self):
        sa.orm.session.Session.close_all()
        self._teardown_each_mappers()
        self._teardown_each_tables()
        self._teardown_each_bind()

    @classmethod
    def _setup_once_classes(cls):
        if cls.run_setup_classes == 'once':
            cls._with_register_classes(cls.setup_classes)

    @classmethod
    def _setup_once_mappers(cls):
        if cls.run_setup_mappers == 'once':
            cls._with_register_classes(cls.setup_mappers)

    def _setup_each_mappers(self):
        if self.run_setup_mappers == 'each':
            self._with_register_classes(self.setup_mappers)

    @classmethod
    def _with_register_classes(cls, fn):
        """Run a setup method, framing the operation with a Base class
        that will catch new subclasses to be established within
        the "classes" registry.
        
        """
        class Base(object):
            pass
        class Basic(BasicEntity, Base):
            pass
        class Comparable(ComparableEntity, Base):
            pass
        cls.Basic = Basic
        cls.Comparable = Comparable
        fn()
        for class_ in subclasses(Base):
            cls.classes[class_.__name__] = class_

    def _teardown_each_mappers(self):
        # some tests create mappers in the test bodies
        # and will define setup_mappers as None - 
        # clear mappers in any case
        if self.run_setup_mappers != 'once':
            sa.orm.clear_mappers()
        if self.run_setup_classes == 'each':
            cls.classes.clear()

    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        pass

    @classmethod
    def _load_fixtures(cls):
        """Insert rows as represented by the fixtures() method."""

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

