import gc
import inspect
import sys
import types
from testlib import config, sa, testing
from testlib.testing import resolve_artifact_names, adict
from testlib.compat import set, sorted, _function_named


_repr_stack = set()
class BasicEntity(object):
    def __init__(self, **kw):
        for key, value in kw.iteritems():
            setattr(self, key, value)

    def __repr__(self):
        if id(self) in _repr_stack:
            return object.__repr__(self)
        _repr_stack.add(id(self))
        try:
            return "%s(%s)" % (
                (self.__class__.__name__),
                ', '.join(["%s=%r" % (key, getattr(self, key))
                           for key in sorted(self.__dict__.keys())
                           if not key.startswith('_')]))
        finally:
            _repr_stack.remove(id(self))

Entity = BasicEntity

_recursion_stack = set()
class ComparableEntity(BasicEntity):
    def __hash__(self):
        return hash(self.__class__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        """'Deep, sparse compare.

        Deeply compare two entities, following the non-None attributes of the
        non-persisted object, if possible.

        """
        if other is self:
            return True
        elif not self.__class__ == other.__class__:
            return False

        if id(self) in _recursion_stack:
            return True
        _recursion_stack.add(id(self))

        try:
            # pick the entity thats not SA persisted as the source
            try:
                self_key = sa.orm.attributes.instance_state(self).key
            except sa.orm.exc.NO_STATE:
                self_key = None
            try:
                other_key = sa.orm.attributes.instance_state(other).key
            except sa.orm.exc.NO_STATE:
                other_key = None

            if other_key is None and self_key is not None:
                a, b = other, self
            else:
                a, b = self, other

            for attr in a.__dict__.keys():
                if attr.startswith('_'):
                    continue
                value = getattr(a, attr)
                if (hasattr(value, '__iter__') and
                    not isinstance(value, basestring)):
                    try:
                        # catch AttributeError so that lazy loaders trigger
                        battr = getattr(b, attr)
                    except AttributeError:
                        return False

                    if list(value) != list(battr):
                        return False
                else:
                    if value is not None:
                        if value != getattr(b, attr, None):
                            return False
            return True
        finally:
            _recursion_stack.remove(id(self))


class ORMTest(testing.TestBase, testing.AssertsExecutionResults):
    __requires__ = ('subqueries',)

    def tearDownAll(self):
        sa.orm.session.Session.close_all()
        sa.orm.clear_mappers()
        # TODO: ensure mapper registry is empty
        # TODO: ensure instrumentation registry is empty

class MappedTest(ORMTest):
    # 'once', 'foreach', None
    run_define_tables = 'once'

    # 'once', 'foreach', None
    run_setup_classes = 'once'

    # 'once', 'foreach', None
    run_setup_mappers = 'each'

    # 'once', 'foreach', None
    run_inserts = 'each'

    # 'foreach', None
    run_deletes = 'each'

    metadata = None

    _artifact_registries = ('tables', 'classes', 'other_artifacts')
    tables = None
    classes = None
    other_artifacts = None

    def setUpAll(self):
        if self.run_setup_classes == 'each':
            assert self.run_setup_mappers != 'once'

        assert self.run_deletes in (None, 'each')
        if self.run_inserts == 'once':
            assert self.run_deletes is None

        assert not hasattr(self, 'keep_mappers')
        assert not hasattr(self, 'keep_data')

        cls = self.__class__
        if cls.tables is None:
            cls.tables = adict()
        if cls.classes is None:
            cls.classes = adict()
        if cls.other_artifacts is None:
            cls.other_artifacts = adict()

        if self.metadata is None:
            setattr(type(self), 'metadata', sa.MetaData())

        if self.metadata.bind is None:
            self.metadata.bind = getattr(self, 'engine', config.db)

        if self.run_define_tables:
            self.define_tables(self.metadata)
            self.metadata.create_all()
            self.tables.update(self.metadata.tables)

        if self.run_setup_classes:
            baseline = subclasses(BasicEntity)
            self.setup_classes()
            self._register_new_class_artifacts(baseline)

        if self.run_setup_mappers:
            baseline = subclasses(BasicEntity)
            self.setup_mappers()
            self._register_new_class_artifacts(baseline)

        if self.run_inserts:
            self._load_fixtures()
            self.insert_data()

    def setUp(self):
        if self._sa_first_test:
            return

        if self.run_define_tables == 'each':
            self.tables.clear()
            self.metadata.drop_all()
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

    def tearDown(self):
        sa.orm.session.Session.close_all()

        if self.run_setup_mappers == 'each':
            sa.orm.clear_mappers()

        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != 'each' and self.run_deletes:
            for table in self.metadata.table_iterator(reverse=True):
                try:
                    table.delete().execute().close()
                except sa.exc.DBAPIError, ex:
                    print >> sys.stderr, "Error emptying table %s: %r" % (
                        table, ex)

    def tearDownAll(self):
        for cls in self.classes.values():
            self.unregister_class(cls)
        ORMTest.tearDownAll(self)
        self.metadata.drop_all()
        self.metadata.bind = None

    def define_tables(self, metadata):
        raise NotImplementedError()

    def setup_classes(self):
        pass

    def setup_mappers(self):
        pass

    def fixtures(self):
        return {}

    def insert_data(self):
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


def subclasses(cls):
    subs, process = set(), set(cls.__subclasses__())
    while process:
        cls = process.pop()
        if cls not in subs:
            subs.add(cls)
            process |= set(cls.__subclasses__())
    return subs

