from test.lib import fixtures
from test.lib.util import gc_collect
from test.lib.testing import assert_raises_message, is_, eq_
from sqlalchemy import exc, MetaData
from sqlalchemy.ext.declarative import clsregistry
import weakref

class MockClass(object):
    def __init__(self, base, name):
        self._decl_class_registry = base
        tokens = name.split(".")
        self.__module__ = ".".join(tokens[0:-1])
        self.name = tokens[-1]
        self.metadata = MetaData()


class MockProp(object):
    parent = "some_parent"


class ClsRegistryTest(fixtures.TestBase):
    def test_same_module_same_name(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1)
        gc_collect()

        assert_raises_message(
            exc.SAWarning,
            "This declarative base already contains a class ",
            clsregistry.add_class, "Foo", f2
        )

    def test_resolve(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1)
        clsregistry.add_class("Foo", f2)
        resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        is_(resolver("foo.bar.Foo")(), f1)
        is_(resolver("foo.alt.Foo")(), f2)

    def test_resolve_dupe_by_name(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1)
        clsregistry.add_class("Foo", f2)

        gc_collect()

        resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            "Multiple classes with the classname 'Foo' "
            "are in the registry of this declarative "
            "base. Please use a fully module-qualified path.",
            resolver
        )

    def test_dupe_classes_back_to_one(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1)
        clsregistry.add_class("Foo", f2)

        del f2
        gc_collect()

        # registry restores itself to just the one class
        resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("Foo")
        is_(resolver(), f1)

    def test_dupe_classes_cleanout(self):
        # force this to maintain isolation between tests
        clsregistry._registries.clear()

        base = weakref.WeakValueDictionary()

        for i in xrange(3):
            f1 = MockClass(base, "foo.bar.Foo")
            f2 = MockClass(base, "foo.alt.Foo")
            clsregistry.add_class("Foo", f1)
            clsregistry.add_class("Foo", f2)

            eq_(len(clsregistry._registries), 5)

            del f1
            del f2
            gc_collect()

            eq_(len(clsregistry._registries), 1)

    def test_dupe_classes_name_race(self):
        """test the race condition that the class was garbage "
        "collected while being resolved from a dupe class."""
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1)
        clsregistry.add_class("Foo", f2)

        dupe_reg = base['Foo']
        dupe_reg.contents = [lambda: None]
        resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            "When initializing mapper some_parent, expression "
            "'Foo' failed to locate a name \('Foo'\).",
            resolver
        )

    def test_module_reg_cleanout_race(self):
        """test the race condition that a class was gc'ed as we tried
        to look it up by module name."""

        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1)
        reg = base['_sa_module_registry']

        mod_entry = reg['foo']['bar']
        resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("foo")
        mod_entry.contents.update({"Foo": lambda: None})
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Foo'",
            lambda: resolver().bar.Foo
        )

    def test_module_reg_no_class(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1)
        reg = base['_sa_module_registry']
        mod_entry = reg['foo']['bar']
        resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("foo")
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Bat'",
            lambda: resolver().bar.Bat
        )

    def test_module_reg_cleanout_two_sub(self):
        base = weakref.WeakValueDictionary()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1)
        reg = base['_sa_module_registry']

        f2 = MockClass(base, "foo.alt.Bar")
        clsregistry.add_class("Bar", f2)
        assert reg['foo']['bar']
        del f1
        gc_collect()
        assert 'bar' not in \
            reg['foo']
        assert 'alt' in reg['foo']

        del f2
        gc_collect()
        assert 'foo' not in reg.contents

    def test_module_reg_cleanout_sub_to_base(self):
        base = weakref.WeakValueDictionary()
        f3 = MockClass(base, "bat.bar.Hoho")
        clsregistry.add_class("Hoho", f3)
        reg = base['_sa_module_registry']

        assert reg['bat']['bar']
        del f3
        gc_collect()
        assert 'bat' not in reg

    def test_module_reg_cleanout_cls_to_base(self):
        base = weakref.WeakValueDictionary()
        f4 = MockClass(base, "single.Blat")
        clsregistry.add_class("Blat", f4)
        reg = base['_sa_module_registry']
        assert reg['single']
        del f4
        gc_collect()
        assert 'single' not in reg

