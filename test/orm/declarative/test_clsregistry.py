from sqlalchemy import exc
from sqlalchemy import MetaData
from sqlalchemy.orm import clsregistry
from sqlalchemy.orm import registry
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.util import gc_collect


class MockClass(object):
    def __init__(self, base, name):
        self._sa_class_manager = mock.Mock(registry=base)
        tokens = name.split(".")
        self.__module__ = ".".join(tokens[0:-1])
        self.name = self.__name__ = tokens[-1]
        self.metadata = MetaData()


class MockProp(object):
    parent = "some_parent"


class ClsRegistryTest(fixtures.TestBase):
    __requires__ = ("predictable_gc",)

    def test_same_module_same_name(self):

        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        gc_collect()

        assert_raises_message(
            exc.SAWarning,
            "This declarative base already contains a class with the "
            "same class name and module name as foo.bar.Foo, and "
            "will be replaced in the string-lookup table.",
            clsregistry.add_class,
            "Foo",
            f2,
            base._class_registry,
        )

    def test_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        is_(resolver("foo.bar.Foo")(), f1)
        is_(resolver("foo.alt.Foo")(), f2)

        is_(name_resolver("foo.bar.Foo")(), f1)
        is_(name_resolver("foo.alt.Foo")(), f2)

    def test_fragment_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        f3 = MockClass(base, "bat.alt.Hoho")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)
        clsregistry.add_class("HoHo", f3, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        is_(resolver("bar.Foo")(), f1)
        is_(resolver("alt.Foo")(), f2)

        is_(name_resolver("bar.Foo")(), f1)
        is_(name_resolver("alt.Foo")(), f2)

    def test_fragment_ambiguous(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        f3 = MockClass(base, "bat.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)
        clsregistry.add_class("Foo", f3, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "alt.Foo" in the registry '
            "of this declarative base. Please use a fully "
            "module-qualified path.",
            resolver("alt.Foo"),
        )

        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "alt.Foo" in the registry '
            "of this declarative base. Please use a fully "
            "module-qualified path.",
            name_resolver("alt.Foo"),
        )

    def test_no_fns_in_name_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        import sqlalchemy

        is_(
            resolver("__import__('sqlalchemy.util').util.EMPTY_SET")(),
            sqlalchemy.util.EMPTY_SET,
        )

        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"\"__import__\('sqlalchemy.util'\).util.EMPTY_SET\" "
            "failed to locate a name",
            name_resolver("__import__('sqlalchemy.util').util.EMPTY_SET"),
        )

    def test_resolve_dupe_by_name(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)

        gc_collect()

        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "Foo" in the '
            "registry of this declarative base. Please use a "
            "fully module-qualified path.",
            resolver,
        )

        resolver = name_resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "Foo" in the '
            "registry of this declarative base. Please use a "
            "fully module-qualified path.",
            resolver,
        )

    def test_dupe_classes_back_to_one(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)

        del f2
        gc_collect()

        # registry restores itself to just the one class
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("Foo")
        is_(f_resolver(), f1)

        f_resolver = name_resolver("Foo")
        is_(f_resolver(), f1)

    def test_dupe_classes_cleanout(self):
        # force this to maintain isolation between tests
        clsregistry._registries.clear()

        base = registry()

        for i in range(3):
            f1 = MockClass(base, "foo.bar.Foo")
            f2 = MockClass(base, "foo.alt.Foo")
            clsregistry.add_class("Foo", f1, base._class_registry)
            clsregistry.add_class("Foo", f2, base._class_registry)

            eq_(len(clsregistry._registries), 11)

            del f1
            del f2
            gc_collect()

            eq_(len(clsregistry._registries), 1)

    def test_dupe_classes_name_race(self):
        """test the race condition that the class was garbage "
        "collected while being resolved from a dupe class."""
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        clsregistry.add_class("Foo", f2, base._class_registry)

        dupe_reg = base._class_registry["Foo"]
        dupe_reg.contents = [lambda: None]
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"'Foo' failed to locate a name \('Foo'\).",
            f_resolver,
        )

        f_resolver = name_resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"'Foo' failed to locate a name \('Foo'\).",
            f_resolver,
        )

    def test_module_reg_cleanout_race(self):
        """test the race condition that a class was gc'ed as we tried
        to look it up by module name."""

        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        mod_entry = reg["foo"]["bar"]
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("foo")
        del mod_entry.contents["Foo"]
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Foo'",
            lambda: f_resolver().bar.Foo,
        )

        f_resolver = name_resolver("foo")
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Foo'",
            lambda: f_resolver().bar.Foo,
        )

    def test_module_reg_no_class(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]
        mod_entry = reg["foo"]["bar"]  # noqa
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("foo")
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Bat'",
            lambda: f_resolver().bar.Bat,
        )

        f_resolver = name_resolver("foo")
        assert_raises_message(
            AttributeError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Bat'",
            lambda: f_resolver().bar.Bat,
        )

    def test_module_reg_cleanout_two_sub(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry.add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        f2 = MockClass(base, "foo.alt.Bar")
        clsregistry.add_class("Bar", f2, base._class_registry)
        assert reg["foo"]["bar"]
        del f1
        gc_collect()
        assert "bar" not in reg["foo"]
        assert "alt" in reg["foo"]

        del f2
        gc_collect()
        assert "foo" not in reg.contents

    def test_module_reg_cleanout_sub_to_base(self):
        base = registry()
        f3 = MockClass(base, "bat.bar.Hoho")
        clsregistry.add_class("Hoho", f3, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        assert reg["bat"]["bar"]
        del f3
        gc_collect()
        assert "bat" not in reg

    def test_module_reg_cleanout_cls_to_base(self):
        base = registry()
        f4 = MockClass(base, "single.Blat")
        clsregistry.add_class("Blat", f4, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]
        assert reg["single"]
        del f4
        gc_collect()
        assert "single" not in reg
