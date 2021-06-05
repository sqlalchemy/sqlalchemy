import copy
import pickle

from sqlalchemy import cast
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.engine import default
from sqlalchemy.ext.associationproxy import _AssociationList
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import aliased
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import collections
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.collections import collection
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class DictCollection(dict):
    @collection.appender
    def append(self, obj):
        self[obj.foo] = obj

    @collection.remover
    def remove(self, obj):
        del self[obj.foo]


class SetCollection(set):
    pass


class ListCollection(list):
    pass


class ObjectCollection(object):
    def __init__(self):
        self.values = list()

    @collection.appender
    def append(self, obj):
        self.values.append(obj)

    @collection.remover
    def remove(self, obj):
        self.values.remove(obj)

    def __iter__(self):
        return iter(self.values)


class AutoFlushTest(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "association",
            metadata,
            Column("parent_id", ForeignKey("parent.id"), primary_key=True),
            Column("child_id", ForeignKey("child.id"), primary_key=True),
            Column("name", String(50)),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )

    def teardown_test(self):
        clear_mappers()

    def _fixture(self, collection_class, is_dict=False):
        class Parent(object):
            collection = association_proxy("_collection", "child")

        class Child(object):
            def __init__(self, name):
                self.name = name

        class Association(object):
            if is_dict:

                def __init__(self, key, child):
                    self.child = child

            else:

                def __init__(self, child):
                    self.child = child

        mapper(
            Parent,
            self.tables.parent,
            properties={
                "_collection": relationship(
                    Association,
                    collection_class=collection_class,
                    backref="parent",
                )
            },
        )
        mapper(
            Association,
            self.tables.association,
            properties={"child": relationship(Child, backref="association")},
        )
        mapper(Child, self.tables.child)

        return Parent, Child, Association

    def _test_premature_flush(self, collection_class, fn, is_dict=False):
        Parent, Child, Association = self._fixture(
            collection_class, is_dict=is_dict
        )

        session = Session(testing.db, autoflush=True, expire_on_commit=True)

        p1 = Parent()
        c1 = Child("c1")
        c2 = Child("c2")
        session.add(p1)
        session.add(c1)
        session.add(c2)

        fn(p1.collection, c1)
        session.commit()

        fn(p1.collection, c2)
        session.commit()

        is_(c1.association[0].parent, p1)
        is_(c2.association[0].parent, p1)

        session.close()

    def test_list_append(self):
        self._test_premature_flush(
            list, lambda collection, obj: collection.append(obj)
        )

    def test_list_extend(self):
        self._test_premature_flush(
            list, lambda collection, obj: collection.extend([obj])
        )

    def test_set_add(self):
        self._test_premature_flush(
            set, lambda collection, obj: collection.add(obj)
        )

    def test_set_extend(self):
        self._test_premature_flush(
            set, lambda collection, obj: collection.update([obj])
        )

    def test_dict_set(self):
        def set_(collection, obj):
            collection[obj.name] = obj

        self._test_premature_flush(
            collections.attribute_mapped_collection("name"), set_, is_dict=True
        )


class _CollectionOperations(fixtures.TestBase):
    def setup_test(self):
        collection_class = self.collection_class

        metadata = MetaData()

        parents_table = Table(
            "Parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        children_table = Table(
            "Children",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("Parent.id")),
            Column("foo", String(128)),
            Column("name", String(128)),
        )

        class Parent(object):
            children = association_proxy("_children", "name")

            def __init__(self, name):
                self.name = name

        class Child(object):
            if collection_class and issubclass(collection_class, dict):

                def __init__(self, foo, name):
                    self.foo = foo
                    self.name = name

            else:

                def __init__(self, name):
                    self.name = name

        mapper(
            Parent,
            parents_table,
            properties={
                "_children": relationship(
                    Child,
                    lazy="joined",
                    backref="parent",
                    collection_class=collection_class,
                )
            },
        )
        mapper(Child, children_table)

        metadata.create_all(testing.db)

        self.metadata = metadata
        self.session = fixture_session()
        self.Parent, self.Child = Parent, Child

    def teardown_test(self):
        self.metadata.drop_all(testing.db)

    def roundtrip(self, obj):
        if obj not in self.session:
            self.session.add(obj)
        self.session.flush()
        id_, type_ = obj.id, type(obj)
        self.session.expunge_all()
        return self.session.query(type_).get(id_)

    def _test_sequence_ops(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent("P1")

        def assert_index(expected, value, *args):
            """Assert index of child value is equal to expected.

            If expected is None, assert that index raises ValueError.
            """
            try:
                index = p1.children.index(value, *args)
            except ValueError:
                self.assert_(expected is None)
            else:
                self.assert_(expected is not None)
                self.assert_(index == expected)

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch = Child("regular")
        p1._children.append(ch)

        self.assert_(ch in p1._children)
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch not in p1.children)
        self.assert_("regular" in p1.children)

        assert_index(0, "regular")
        assert_index(None, "regular", 1)

        p1.children.append("proxied")

        self.assert_("proxied" in p1.children)
        self.assert_("proxied" not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(p1._children[0].name == "regular")
        self.assert_(p1._children[1].name == "proxied")

        assert_index(0, "regular")
        assert_index(1, "proxied")
        assert_index(1, "proxied", 1)
        assert_index(None, "proxied", 0, 1)

        del p1._children[1]

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children[0] == ch)

        assert_index(None, "proxied")

        del p1.children[0]

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        assert_index(None, "regular")

        p1.children = ["a", "b", "c"]
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        assert_index(0, "a")
        assert_index(1, "b")
        assert_index(2, "c")

        del ch
        p1 = self.roundtrip(p1)

        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        assert_index(0, "a")
        assert_index(1, "b")
        assert_index(2, "c")

        popped = p1.children.pop()
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)
        assert_index(None, popped)

        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)
        assert_index(None, popped)

        p1.children[1] = "changed-in-place"
        self.assert_(p1.children[1] == "changed-in-place")
        assert_index(1, "changed-in-place")
        assert_index(None, "b")

        inplace_id = p1._children[1].id
        p1 = self.roundtrip(p1)
        self.assert_(p1.children[1] == "changed-in-place")
        assert p1._children[1].id == inplace_id

        p1.children.append("changed-in-place")
        self.assert_(p1.children.count("changed-in-place") == 2)
        assert_index(1, "changed-in-place")

        p1.children.remove("changed-in-place")
        self.assert_(p1.children.count("changed-in-place") == 1)
        assert_index(1, "changed-in-place")

        p1 = self.roundtrip(p1)
        self.assert_(p1.children.count("changed-in-place") == 1)
        assert_index(1, "changed-in-place")

        p1._children = []
        self.assert_(len(p1.children) == 0)
        assert_index(None, "changed-in-place")

        after = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        p1.children = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        self.assert_(len(p1.children) == 10)
        self.assert_([c.name for c in p1._children] == after)
        for i, val in enumerate(after):
            assert_index(i, val)

        p1.children[2:6] = ["x"] * 4
        after = ["a", "b", "x", "x", "x", "x", "g", "h", "i", "j"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)
        assert_index(2, "x")
        assert_index(3, "x", 3)
        assert_index(None, "x", 6)

        p1.children[2:6] = ["y"]
        after = ["a", "b", "y", "g", "h", "i", "j"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)
        assert_index(2, "y")
        assert_index(None, "y", 3)

        p1.children[2:3] = ["z"] * 4
        after = ["a", "b", "z", "z", "z", "z", "g", "h", "i", "j"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[2::2] = ["O"] * 4
        after = ["a", "b", "O", "z", "O", "z", "O", "h", "O", "j"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        assert_raises(TypeError, set, [p1.children])

        p1.children *= 0
        after = []
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children += ["a", "b"]
        after = ["a", "b"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[:] = ["d", "e"]
        after = ["d", "e"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[:] = ["a", "b"]

        p1.children += ["c"]
        after = ["a", "b", "c"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children *= 1
        after = ["a", "b", "c"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children *= 2
        after = ["a", "b", "c", "a", "b", "c"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children = ["a"]
        after = ["a"]
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        self.assert_((p1.children * 2) == ["a", "a"])
        self.assert_((2 * p1.children) == ["a", "a"])
        self.assert_((p1.children * 0) == [])
        self.assert_((0 * p1.children) == [])

        self.assert_((p1.children + ["b"]) == ["a", "b"])
        self.assert_((["b"] + p1.children) == ["b", "a"])

        try:
            p1.children + 123
            assert False
        except TypeError:
            assert True


class DefaultTest(_CollectionOperations):
    collection_class = None

    def test_sequence_ops(self):
        self._test_sequence_ops()


class ListTest(_CollectionOperations):
    collection_class = list

    def test_sequence_ops(self):
        self._test_sequence_ops()


class CustomDictTest(_CollectionOperations):
    collection_class = DictCollection

    def test_mapping_ops(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent("P1")

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch = Child("a", "regular")
        p1._children.append(ch)

        self.assert_(ch in list(p1._children.values()))
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch not in p1.children)
        self.assert_("a" in p1.children)
        self.assert_(p1.children["a"] == "regular")
        self.assert_(p1._children["a"] == ch)

        p1.children["b"] = "proxied"

        self.assert_("proxied" in list(p1.children.values()))
        self.assert_("b" in p1.children)
        self.assert_("proxied" not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(p1._children["a"].name == "regular")
        self.assert_(p1._children["b"].name == "proxied")

        del p1._children["b"]

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children["a"] == ch)

        del p1.children["a"]

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        p1.children = {"d": "v d", "e": "v e", "f": "v f"}
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        self.assert_(set(p1.children) == set(["d", "e", "f"]))

        del ch
        p1 = self.roundtrip(p1)
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        p1.children["e"] = "changed-in-place"
        self.assert_(p1.children["e"] == "changed-in-place")
        inplace_id = p1._children["e"].id
        p1 = self.roundtrip(p1)
        self.assert_(p1.children["e"] == "changed-in-place")
        self.assert_(p1._children["e"].id == inplace_id)

        p1._children = {}
        self.assert_(len(p1.children) == 0)

        try:
            p1._children = []
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        assert_raises(TypeError, set, [p1.children])

    def test_bulk_replace(self):
        Parent = self.Parent

        p1 = Parent("foo")
        p1.children = {"a": "v a", "b": "v b", "c": "v c"}
        assocs = set(p1._children.values())
        keep_assocs = {a for a in assocs if a.foo in ("a", "c")}
        eq_(len(keep_assocs), 2)
        remove_assocs = {a for a in assocs if a.foo == "b"}

        p1.children = {"a": "v a", "d": "v d", "c": "v c"}
        eq_(
            {a for a in p1._children.values() if a.foo in ("a", "c")},
            keep_assocs,
        )
        assert not remove_assocs.intersection(p1._children.values())

        eq_(p1.children, {"a": "v a", "d": "v d", "c": "v c"})


class SetTest(_CollectionOperations):
    collection_class = set

    def test_set_operations(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent("P1")

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch1 = Child("regular")
        p1._children.add(ch1)

        self.assert_(ch1 in p1._children)
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch1 not in p1.children)
        self.assert_("regular" in p1.children)

        p1.children.add("proxied")

        self.assert_("proxied" in p1.children)
        self.assert_("proxied" not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(
            set([o.name for o in p1._children]) == set(["regular", "proxied"])
        )

        ch2 = None
        for o in p1._children:
            if o.name == "proxied":
                ch2 = o
                break

        p1._children.remove(ch2)

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children == set([ch1]))

        p1.children.remove("regular")

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        p1.children = ["a", "b", "c"]
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        del ch1
        p1 = self.roundtrip(p1)

        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        self.assert_("a" in p1.children)
        self.assert_("b" in p1.children)
        self.assert_("d" not in p1.children)

        self.assert_(p1.children == set(["a", "b", "c"]))

        assert_raises(KeyError, p1.children.remove, "d")

        self.assert_(len(p1.children) == 3)
        p1.children.discard("d")
        self.assert_(len(p1.children) == 3)
        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 3)

        popped = p1.children.pop()
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)
        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)

        p1.children = ["a", "b", "c"]
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(["a", "b", "c"]))

        p1.children.discard("b")
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(["a", "c"]))

        p1.children.remove("a")
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(["c"]))

        p1._children = set()
        self.assert_(len(p1.children) == 0)

        try:
            p1._children = []
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        assert_raises(TypeError, set, [p1.children])

    def test_set_comparisons(self):
        Parent = self.Parent

        p1 = Parent("P1")
        p1.children = ["a", "b", "c"]
        control = set(["a", "b", "c"])

        for other in (
            set(["a", "b", "c"]),
            set(["a", "b", "c", "d"]),
            set(["a"]),
            set(["a", "b"]),
            set(["c", "d"]),
            set(["e", "f", "g"]),
            set(),
        ):

            eq_(p1.children.union(other), control.union(other))
            eq_(p1.children.difference(other), control.difference(other))
            eq_((p1.children - other), (control - other))
            eq_(p1.children.intersection(other), control.intersection(other))
            eq_(
                p1.children.symmetric_difference(other),
                control.symmetric_difference(other),
            )
            eq_(p1.children.issubset(other), control.issubset(other))
            eq_(p1.children.issuperset(other), control.issuperset(other))

            self.assert_((p1.children == other) == (control == other))
            self.assert_((p1.children != other) == (control != other))
            self.assert_((p1.children < other) == (control < other))
            self.assert_((p1.children <= other) == (control <= other))
            self.assert_((p1.children > other) == (control > other))
            self.assert_((p1.children >= other) == (control >= other))

    @testing.requires.python_fixed_issue_8743
    def test_set_comparison_empty_to_empty(self):
        # test issue #3265 which was fixed in Python version 2.7.8
        Parent = self.Parent

        p1 = Parent("P1")
        p1.children = []

        p2 = Parent("P2")
        p2.children = []

        set_0 = set()
        set_a = p1.children
        set_b = p2.children

        is_(set_a == set_a, True)
        is_(set_a == set_b, True)
        is_(set_a == set_0, True)
        is_(set_0 == set_a, True)

        is_(set_a != set_a, False)
        is_(set_a != set_b, False)
        is_(set_a != set_0, False)
        is_(set_0 != set_a, False)

    def test_set_mutation(self):
        Parent = self.Parent

        # mutations
        for op in (
            "update",
            "intersection_update",
            "difference_update",
            "symmetric_difference_update",
        ):
            for base in (["a", "b", "c"], []):
                for other in (
                    set(["a", "b", "c"]),
                    set(["a", "b", "c", "d"]),
                    set(["a"]),
                    set(["a", "b"]),
                    set(["c", "d"]),
                    set(["e", "f", "g"]),
                    set(),
                ):
                    p = Parent("p")
                    p.children = base[:]
                    control = set(base[:])

                    getattr(p.children, op)(other)
                    getattr(control, op)(other)
                    try:
                        self.assert_(p.children == control)
                    except Exception:
                        print("Test %s.%s(%s):" % (set(base), op, other))
                        print("want", repr(control))
                        print("got", repr(p.children))
                        raise

                    p = self.roundtrip(p)

                    try:
                        self.assert_(p.children == control)
                    except Exception:
                        print("Test %s.%s(%s):" % (base, op, other))
                        print("want", repr(control))
                        print("got", repr(p.children))
                        raise

        # in-place mutations
        for op in ("|=", "-=", "&=", "^="):
            for base in (["a", "b", "c"], []):
                for other in (
                    set(["a", "b", "c"]),
                    set(["a", "b", "c", "d"]),
                    set(["a"]),
                    set(["a", "b"]),
                    set(["c", "d"]),
                    set(["e", "f", "g"]),
                    frozenset(["e", "f", "g"]),
                    set(),
                ):
                    p = Parent("p")
                    p.children = base[:]
                    control = set(base[:])

                    exec("p.children %s other" % op)
                    exec("control %s other" % op)

                    try:
                        self.assert_(p.children == control)
                    except Exception:
                        print("Test %s %s %s:" % (set(base), op, other))
                        print("want", repr(control))
                        print("got", repr(p.children))
                        raise

                    p = self.roundtrip(p)

                    try:
                        self.assert_(p.children == control)
                    except Exception:
                        print("Test %s %s %s:" % (base, op, other))
                        print("want", repr(control))
                        print("got", repr(p.children))
                        raise

    def test_bulk_replace(self):
        Parent = self.Parent

        p1 = Parent("foo")
        p1.children = {"a", "b", "c"}
        assocs = set(p1._children)
        keep_assocs = {a for a in assocs if a.name in ("a", "c")}
        eq_(len(keep_assocs), 2)
        remove_assocs = {a for a in assocs if a.name == "b"}

        p1.children = {"a", "c", "d"}
        eq_({a for a in p1._children if a.name in ("a", "c")}, keep_assocs)
        assert not remove_assocs.intersection(p1._children)

        eq_(p1.children, {"a", "c", "d"})


class CustomSetTest(SetTest):
    collection_class = SetCollection


class CustomObjectTest(_CollectionOperations):
    collection_class = ObjectCollection

    def test_basic(self):
        Parent = self.Parent

        p = Parent("p1")
        self.assert_(len(list(p.children)) == 0)

        p.children.append("child")
        self.assert_(len(list(p.children)) == 1)

        p = self.roundtrip(p)
        self.assert_(len(list(p.children)) == 1)

        # We didn't provide an alternate _AssociationList implementation
        # for our ObjectCollection, so indexing will fail.
        assert_raises(TypeError, p.children.__getitem__, 1)


class ProxyFactoryTest(ListTest):
    def setup_test(self):
        metadata = MetaData()

        parents_table = Table(
            "Parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        children_table = Table(
            "Children",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("Parent.id")),
            Column("foo", String(128)),
            Column("name", String(128)),
        )

        class CustomProxy(_AssociationList):
            def __init__(self, lazy_collection, creator, value_attr, parent):
                getter, setter = parent._default_getset(lazy_collection)
                _AssociationList.__init__(
                    self, lazy_collection, creator, getter, setter, parent
                )

        class Parent(object):
            children = association_proxy(
                "_children",
                "name",
                proxy_factory=CustomProxy,
                proxy_bulk_set=CustomProxy.extend,
            )

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, name):
                self.name = name

        mapper(
            Parent,
            parents_table,
            properties={
                "_children": relationship(
                    Child, lazy="joined", collection_class=list
                )
            },
        )
        mapper(Child, children_table)

        metadata.create_all(testing.db)

        self.metadata = metadata
        self.session = fixture_session()
        self.Parent, self.Child = Parent, Child

    def test_sequence_ops(self):
        self._test_sequence_ops()


class ScalarTest(fixtures.TestBase):
    @testing.provide_metadata
    def test_scalar_proxy(self):
        metadata = self.metadata

        parents_table = Table(
            "Parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        children_table = Table(
            "Children",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("Parent.id")),
            Column("foo", String(128)),
            Column("bar", String(128)),
            Column("baz", String(128)),
        )

        class Parent(object):
            foo = association_proxy("child", "foo")
            bar = association_proxy(
                "child", "bar", creator=lambda v: Child(bar=v)
            )
            baz = association_proxy(
                "child", "baz", creator=lambda v: Child(baz=v)
            )

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, **kw):
                for attr in kw:
                    setattr(self, attr, kw[attr])

        mapper(
            Parent,
            parents_table,
            properties={
                "child": relationship(
                    Child, lazy="joined", backref="parent", uselist=False
                )
            },
        )
        mapper(Child, children_table)

        metadata.create_all(testing.db)
        session = fixture_session()

        def roundtrip(obj):
            if obj not in session:
                session.add(obj)
            session.flush()
            id_, type_ = obj.id, type(obj)
            session.expunge_all()
            return session.query(type_).get(id_)

        p = Parent("p")

        eq_(p.child, None)
        eq_(p.foo, None)

        p.child = Child(foo="a", bar="b", baz="c")

        self.assert_(p.foo == "a")
        self.assert_(p.bar == "b")
        self.assert_(p.baz == "c")

        p.bar = "x"
        self.assert_(p.foo == "a")
        self.assert_(p.bar == "x")
        self.assert_(p.baz == "c")

        p = roundtrip(p)

        self.assert_(p.foo == "a")
        self.assert_(p.bar == "x")
        self.assert_(p.baz == "c")

        p.child = None

        eq_(p.foo, None)

        # Bogus creator for this scalar type
        assert_raises(TypeError, setattr, p, "foo", "zzz")

        p.bar = "yyy"

        self.assert_(p.foo is None)
        self.assert_(p.bar == "yyy")
        self.assert_(p.baz is None)

        del p.child

        p = roundtrip(p)

        self.assert_(p.child is None)

        p.baz = "xxx"

        self.assert_(p.foo is None)
        self.assert_(p.bar is None)
        self.assert_(p.baz == "xxx")

        p = roundtrip(p)

        self.assert_(p.foo is None)
        self.assert_(p.bar is None)
        self.assert_(p.baz == "xxx")

        # Ensure an immediate __set__ works.
        p2 = Parent("p2")
        p2.bar = "quux"

    @testing.provide_metadata
    def test_empty_scalars(self):
        metadata = self.metadata

        a = Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        a2b = Table(
            "a2b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("id_a", Integer, ForeignKey("a.id")),
            Column("id_b", Integer, ForeignKey("b.id")),
            Column("name", String(50)),
        )
        b = Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        class A(object):
            a2b_name = association_proxy("a2b_single", "name")
            b_single = association_proxy("a2b_single", "b")

        class A2B(object):
            pass

        class B(object):
            pass

        mapper(
            A, a, properties=dict(a2b_single=relationship(A2B, uselist=False))
        )

        mapper(A2B, a2b, properties=dict(b=relationship(B)))
        mapper(B, b)

        a1 = A()
        assert a1.a2b_name is None
        assert a1.b_single is None

    def test_custom_getset(self):
        metadata = MetaData()
        p = Table(
            "p",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("cid", Integer, ForeignKey("c.id")),
        )
        c = Table(
            "c",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", String(128)),
        )

        get = Mock()
        set_ = Mock()

        class Parent(object):
            foo = association_proxy(
                "child", "foo", getset_factory=lambda cc, parent: (get, set_)
            )

        class Child(object):
            def __init__(self, foo):
                self.foo = foo

        mapper(Parent, p, properties={"child": relationship(Child)})
        mapper(Child, c)

        p1 = Parent()

        eq_(p1.foo, get(None))
        p1.child = child = Child(foo="x")
        eq_(p1.foo, get(child))
        p1.foo = "y"
        eq_(set_.mock_calls, [call(child, "y")])


class LazyLoadTest(fixtures.TestBase):
    def setup_test(self):
        metadata = MetaData()

        parents_table = Table(
            "Parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        children_table = Table(
            "Children",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("Parent.id")),
            Column("foo", String(128)),
            Column("name", String(128)),
        )

        class Parent(object):
            children = association_proxy("_children", "name")

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, name):
                self.name = name

        mapper(Child, children_table)
        metadata.create_all(testing.db)

        self.metadata = metadata
        self.session = fixture_session()
        self.Parent, self.Child = Parent, Child
        self.table = parents_table

    def teardown_test(self):
        self.metadata.drop_all(testing.db)

    def roundtrip(self, obj):
        self.session.add(obj)
        self.session.flush()
        id_, type_ = obj.id, type(obj)
        self.session.expunge_all()
        return self.session.query(type_).get(id_)

    def test_lazy_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(
            Parent,
            self.table,
            properties={
                "_children": relationship(
                    Child, lazy="select", collection_class=list
                )
            },
        )

        p = Parent("p")
        p.children = ["a", "b", "c"]

        p = self.roundtrip(p)

        # Is there a better way to ensure that the association_proxy
        # didn't convert a lazy load to an eager load?  This does work though.
        self.assert_("_children" not in p.__dict__)
        self.assert_(len(p._children) == 3)
        self.assert_("_children" in p.__dict__)

    def test_eager_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(
            Parent,
            self.table,
            properties={
                "_children": relationship(
                    Child, lazy="joined", collection_class=list
                )
            },
        )

        p = Parent("p")
        p.children = ["a", "b", "c"]

        p = self.roundtrip(p)

        self.assert_("_children" in p.__dict__)
        self.assert_(len(p._children) == 3)

    def test_slicing_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(
            Parent,
            self.table,
            properties={
                "_children": relationship(
                    Child, lazy="select", collection_class=list
                )
            },
        )

        p = Parent("p")
        p.children = ["a", "b", "c"]

        p = self.roundtrip(p)

        self.assert_(len(p._children) == 3)
        eq_("b", p.children[1])
        eq_(["b", "c"], p.children[-2:])

    def test_lazy_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(
            Parent,
            self.table,
            properties={
                "_children": relationship(Child, lazy="select", uselist=False)
            },
        )

        p = Parent("p")
        p.children = "value"

        p = self.roundtrip(p)

        self.assert_("_children" not in p.__dict__)
        self.assert_(p._children is not None)

    def test_eager_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(
            Parent,
            self.table,
            properties={
                "_children": relationship(Child, lazy="joined", uselist=False)
            },
        )

        p = Parent("p")
        p.children = "value"

        p = self.roundtrip(p)

        self.assert_("_children" in p.__dict__)
        self.assert_(p._children is not None)


class Parent(object):
    def __init__(self, name):
        self.name = name


class Child(object):
    def __init__(self, name):
        self.name = name


class KVChild(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value


class ReconstitutionTest(fixtures.MappedTest):
    run_setup_mappers = "each"
    run_setup_classes = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parents",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
        )
        Table(
            "children",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("parents.id")),
            Column("name", String(30)),
        )

    @classmethod
    def insert_data(cls, connection):
        parents = cls.tables.parents
        connection.execute(parents.insert(), dict(name="p1"))

    @classmethod
    def setup_classes(cls):
        Parent.kids = association_proxy("children", "name")

    def test_weak_identity_map(self):
        mapper(
            Parent,
            self.tables.parents,
            properties=dict(children=relationship(Child)),
        )
        mapper(Child, self.tables.children)
        session = fixture_session()

        def add_child(parent_name, child_name):
            parent = session.query(Parent).filter_by(name=parent_name).one()
            parent.kids.append(child_name)

        add_child("p1", "c1")
        gc_collect()
        add_child("p1", "c2")
        session.flush()
        p = session.query(Parent).filter_by(name="p1").one()
        assert set(p.kids) == set(["c1", "c2"]), p.kids

    def test_copy(self):
        mapper(
            Parent,
            self.tables.parents,
            properties=dict(children=relationship(Child)),
        )
        mapper(Child, self.tables.children)
        p = Parent("p1")
        p.kids.extend(["c1", "c2"])
        p_copy = copy.copy(p)
        del p
        gc_collect()
        assert set(p_copy.kids) == set(["c1", "c2"]), p_copy.kids

    def test_pickle_list(self):
        mapper(
            Parent,
            self.tables.parents,
            properties=dict(children=relationship(Child)),
        )
        mapper(Child, self.tables.children)
        p = Parent("p1")
        p.kids.extend(["c1", "c2"])
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == ["c1", "c2"]

        # can't do this without parent having a cycle
        # r2 = pickle.loads(pickle.dumps(p.kids))
        # assert r2 == ['c1', 'c2']

    def test_pickle_set(self):
        mapper(
            Parent,
            self.tables.parents,
            properties=dict(
                children=relationship(Child, collection_class=set)
            ),
        )
        mapper(Child, self.tables.children)
        p = Parent("p1")
        p.kids.update(["c1", "c2"])
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == set(["c1", "c2"])

        # can't do this without parent having a cycle
        # r2 = pickle.loads(pickle.dumps(p.kids))
        # assert r2 == set(['c1', 'c2'])

    def test_pickle_dict(self):
        mapper(
            Parent,
            self.tables.parents,
            properties=dict(
                children=relationship(
                    KVChild,
                    collection_class=collections.mapped_collection(
                        PickleKeyFunc("name")
                    ),
                )
            ),
        )
        mapper(KVChild, self.tables.children)
        p = Parent("p1")
        p.kids.update({"c1": "v1", "c2": "v2"})
        assert p.kids == {"c1": "c1", "c2": "c2"}
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == {"c1": "c1", "c2": "c2"}

        # can't do this without parent having a cycle
        # r2 = pickle.loads(pickle.dumps(p.kids))
        # assert r2 == {'c1': 'c1', 'c2': 'c2'}


class PickleKeyFunc(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, obj):
        return getattr(obj, self.name)


class ComparatorTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_inserts = "once"
    run_deletes = None
    run_setup_mappers = "once"
    run_setup_classes = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "userkeywords",
            metadata,
            Column(
                "keyword_id",
                Integer,
                ForeignKey("keywords.id"),
                primary_key=True,
            ),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("value", String(50)),
        )
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(64)),
            Column("singular_id", Integer, ForeignKey("singular.id")),
        )
        Table(
            "keywords",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("keyword", String(64)),
            Column("singular_id", Integer, ForeignKey("singular.id")),
        )
        Table(
            "singular",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            def __init__(self, name):
                self.name = name

            # o2m -> m2o
            # uselist -> nonuselist
            keywords = association_proxy(
                "user_keywords",
                "keyword",
                creator=lambda k: UserKeyword(keyword=k),
            )

            # m2o -> o2m
            # nonuselist -> uselist
            singular_keywords = association_proxy("singular", "keywords")

            # m2o -> scalar
            # nonuselist
            singular_value = association_proxy("singular", "value")

            # o2m -> scalar
            singular_collection = association_proxy("user_keywords", "value")

            # uselist assoc_proxy -> assoc_proxy -> obj
            common_users = association_proxy("user_keywords", "common_users")

            # non uselist assoc_proxy -> assoc_proxy -> obj
            common_singular = association_proxy("singular", "keyword")

            # non uselist assoc_proxy -> assoc_proxy -> scalar
            singular_keyword = association_proxy("singular", "keyword")

            # uselist assoc_proxy -> assoc_proxy -> scalar
            common_keyword_name = association_proxy(
                "user_keywords", "keyword_name"
            )

        class Keyword(cls.Comparable):
            def __init__(self, keyword):
                self.keyword = keyword

            # o2o -> m2o
            # nonuselist -> nonuselist
            user = association_proxy("user_keyword", "user")

            # uselist assoc_proxy -> collection -> assoc_proxy -> scalar object
            # (o2m relationship,
            #  associationproxy(m2o relationship, m2o relationship))
            singulars = association_proxy("user_keywords", "singular")

        class UserKeyword(cls.Comparable):
            def __init__(self, user=None, keyword=None):
                self.user = user
                self.keyword = keyword

            common_users = association_proxy("keyword", "user")
            keyword_name = association_proxy("keyword", "keyword")

            singular = association_proxy("user", "singular")

        class Singular(cls.Comparable):
            def __init__(self, value=None):
                self.value = value

            keyword = association_proxy("keywords", "keyword")

    @classmethod
    def setup_mappers(cls):
        (
            users,
            Keyword,
            UserKeyword,
            singular,
            userkeywords,
            User,
            keywords,
            Singular,
        ) = (
            cls.tables.users,
            cls.classes.Keyword,
            cls.classes.UserKeyword,
            cls.tables.singular,
            cls.tables.userkeywords,
            cls.classes.User,
            cls.tables.keywords,
            cls.classes.Singular,
        )

        mapper(User, users, properties={"singular": relationship(Singular)})
        mapper(
            Keyword,
            keywords,
            properties={
                "user_keyword": relationship(
                    UserKeyword, uselist=False, back_populates="keyword"
                ),
                "user_keywords": relationship(UserKeyword, viewonly=True),
            },
        )

        mapper(
            UserKeyword,
            userkeywords,
            properties={
                "user": relationship(User, backref="user_keywords"),
                "keyword": relationship(
                    Keyword, back_populates="user_keyword"
                ),
            },
        )
        mapper(
            Singular, singular, properties={"keywords": relationship(Keyword)}
        )

    @classmethod
    def insert_data(cls, connection):
        UserKeyword, User, Keyword, Singular = (
            cls.classes.UserKeyword,
            cls.classes.User,
            cls.classes.Keyword,
            cls.classes.Singular,
        )

        session = Session(connection)
        words = ("quick", "brown", "fox", "jumped", "over", "the", "lazy")
        for ii in range(16):
            user = User("user%d" % ii)

            if ii % 2 == 0:
                user.singular = Singular(
                    value=("singular%d" % ii) if ii % 4 == 0 else None
                )
            session.add(user)
            for jj in words[(ii % len(words)) : ((ii + 3) % len(words))]:
                k = Keyword(jj)
                user.keywords.append(k)
                if ii % 2 == 0:
                    user.singular.keywords.append(k)
                    user.user_keywords[-1].value = "singular%d" % ii

        orphan = Keyword("orphan")
        orphan.user_keyword = UserKeyword(keyword=orphan, user=None)
        session.add(orphan)

        keyword_with_nothing = Keyword("kwnothing")
        session.add(keyword_with_nothing)

        session.commit()
        cls.u = user
        cls.kw = user.keywords[0]

        # TODO: this is not the correct pattern, use session per test
        cls.session = Session(testing.db)

    def _equivalent(self, q_proxy, q_direct):
        proxy_sql = q_proxy.statement.compile(dialect=default.DefaultDialect())
        direct_sql = q_direct.statement.compile(
            dialect=default.DefaultDialect()
        )
        eq_(str(proxy_sql), str(direct_sql))
        eq_(q_proxy.all(), q_direct.all())

    def test_no_straight_expr(self):
        User = self.classes.User

        assert_raises_message(
            NotImplementedError,
            "The association proxy can't be used as a plain column expression",
            func.foo,
            User.singular_value,
        )

        assert_raises_message(
            NotImplementedError,
            "The association proxy can't be used as a plain column expression",
            self.session.query,
            User.singular_value,
        )

    def test_filter_any_criterion_ul_scalar(self):
        UserKeyword, User = self.classes.UserKeyword, self.classes.User

        q1 = self.session.query(User).filter(
            User.singular_collection.any(UserKeyword.value == "singular8")
        )
        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM userkeywords "
            "WHERE users.id = userkeywords.user_id AND "
            "userkeywords.value = :value_1)",
            checkparams={"value_1": "singular8"},
        )

        q2 = self.session.query(User).filter(
            User.user_keywords.any(UserKeyword.value == "singular8")
        )
        self._equivalent(q1, q2)

    def test_filter_any_kwarg_ul_nul(self):
        UserKeyword, User = self.classes.UserKeyword, self.classes.User

        self._equivalent(
            self.session.query(User).filter(
                User.keywords.any(keyword="jumped")
            ),
            self.session.query(User).filter(
                User.user_keywords.any(
                    UserKeyword.keyword.has(keyword="jumped")
                )
            ),
        )

    def test_filter_has_kwarg_nul_nul(self):
        UserKeyword, Keyword = self.classes.UserKeyword, self.classes.Keyword

        self._equivalent(
            self.session.query(Keyword).filter(Keyword.user.has(name="user2")),
            self.session.query(Keyword).filter(
                Keyword.user_keyword.has(UserKeyword.user.has(name="user2"))
            ),
        )

    def test_filter_has_kwarg_nul_ul(self):
        User, Singular = self.classes.User, self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_keywords.any(keyword="jumped")
            ),
            self.session.query(User).filter(
                User.singular.has(Singular.keywords.any(keyword="jumped"))
            ),
        )

    def test_filter_any_criterion_ul_nul(self):
        UserKeyword, User, Keyword = (
            self.classes.UserKeyword,
            self.classes.User,
            self.classes.Keyword,
        )

        self._equivalent(
            self.session.query(User).filter(
                User.keywords.any(Keyword.keyword == "jumped")
            ),
            self.session.query(User).filter(
                User.user_keywords.any(
                    UserKeyword.keyword.has(Keyword.keyword == "jumped")
                )
            ),
        )

    def test_filter_has_criterion_nul_nul(self):
        UserKeyword, User, Keyword = (
            self.classes.UserKeyword,
            self.classes.User,
            self.classes.Keyword,
        )

        self._equivalent(
            self.session.query(Keyword).filter(
                Keyword.user.has(User.name == "user2")
            ),
            self.session.query(Keyword).filter(
                Keyword.user_keyword.has(
                    UserKeyword.user.has(User.name == "user2")
                )
            ),
        )

    def test_filter_any_criterion_nul_ul(self):
        User, Keyword, Singular = (
            self.classes.User,
            self.classes.Keyword,
            self.classes.Singular,
        )

        self._equivalent(
            self.session.query(User).filter(
                User.singular_keywords.any(Keyword.keyword == "jumped")
            ),
            self.session.query(User).filter(
                User.singular.has(
                    Singular.keywords.any(Keyword.keyword == "jumped")
                )
            ),
        )

    def test_filter_contains_ul_nul(self):
        User = self.classes.User

        self._equivalent(
            self.session.query(User).filter(User.keywords.contains(self.kw)),
            self.session.query(User).filter(
                User.user_keywords.any(keyword=self.kw)
            ),
        )

    def test_filter_contains_nul_ul(self):
        User, Singular = self.classes.User, self.classes.Singular

        with expect_warnings(
            "Got None for value of column keywords.singular_id;"
        ):
            self._equivalent(
                self.session.query(User).filter(
                    User.singular_keywords.contains(self.kw)
                ),
                self.session.query(User).filter(
                    User.singular.has(Singular.keywords.contains(self.kw))
                ),
            )

    def test_filter_eq_nul_nul(self):
        Keyword = self.classes.Keyword

        self._equivalent(
            self.session.query(Keyword).filter(Keyword.user == self.u),
            self.session.query(Keyword).filter(
                Keyword.user_keyword.has(user=self.u)
            ),
        )

    def test_filter_ne_nul_nul(self):
        Keyword = self.classes.Keyword
        UserKeyword = self.classes.UserKeyword

        self._equivalent(
            self.session.query(Keyword).filter(Keyword.user != self.u),
            self.session.query(Keyword).filter(
                Keyword.user_keyword.has(UserKeyword.user != self.u)
            ),
        )

    def test_filter_eq_null_nul_nul(self):
        UserKeyword, Keyword = self.classes.UserKeyword, self.classes.Keyword

        self._equivalent(
            self.session.query(Keyword).filter(Keyword.user == None),  # noqa
            self.session.query(Keyword).filter(
                or_(
                    Keyword.user_keyword.has(UserKeyword.user == None),
                    Keyword.user_keyword == None,
                )
            ),
        )

    def test_filter_ne_null_nul_nul(self):
        UserKeyword, Keyword = self.classes.UserKeyword, self.classes.Keyword

        self._equivalent(
            self.session.query(Keyword).filter(Keyword.user != None),  # noqa
            self.session.query(Keyword).filter(
                Keyword.user_keyword.has(UserKeyword.user != None)
            ),
        )

    def test_filter_object_eq_None_nul(self):
        UserKeyword = self.classes.UserKeyword
        User = self.classes.User

        self._equivalent(
            self.session.query(UserKeyword).filter(
                UserKeyword.singular == None
            ),  # noqa
            self.session.query(UserKeyword).filter(
                or_(
                    UserKeyword.user.has(User.singular == None),
                    UserKeyword.user_id == None,
                )
            ),
        )

    def test_filter_column_eq_None_nul(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_value == None
            ),  # noqa
            self.session.query(User).filter(
                or_(
                    User.singular.has(Singular.value == None),
                    User.singular == None,
                )
            ),
        )

    def test_filter_object_ne_value_nul(self):
        UserKeyword = self.classes.UserKeyword
        User = self.classes.User
        Singular = self.classes.Singular

        s4 = self.session.query(Singular).filter_by(value="singular4").one()
        self._equivalent(
            self.session.query(UserKeyword).filter(UserKeyword.singular != s4),
            self.session.query(UserKeyword).filter(
                UserKeyword.user.has(User.singular != s4)
            ),
        )

    def test_filter_column_ne_value_nul(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_value != "singular4"
            ),
            self.session.query(User).filter(
                User.singular.has(Singular.value != "singular4")
            ),
        )

    def test_filter_eq_value_nul(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_value == "singular4"
            ),
            self.session.query(User).filter(
                User.singular.has(Singular.value == "singular4")
            ),
        )

    def test_filter_ne_None_nul(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_value != None
            ),  # noqa
            self.session.query(User).filter(
                User.singular.has(Singular.value != None)
            ),
        )

    def test_has_nul(self):
        # a special case where we provide an empty has() on a
        # non-object-targeted association proxy.
        User = self.classes.User
        self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(User.singular_value.has()),
            self.session.query(User).filter(User.singular.has()),
        )

    def test_nothas_nul(self):
        # a special case where we provide an empty has() on a
        # non-object-targeted association proxy.
        User = self.classes.User
        self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(~User.singular_value.has()),
            self.session.query(User).filter(~User.singular.has()),
        )

    def test_filter_any_chained(self):
        User = self.classes.User

        UserKeyword, User = self.classes.UserKeyword, self.classes.User
        Keyword = self.classes.Keyword

        q1 = self.session.query(User).filter(
            User.common_users.any(User.name == "user7")
        )
        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM userkeywords "
            "WHERE users.id = userkeywords.user_id AND (EXISTS (SELECT 1 "
            "FROM keywords "
            "WHERE keywords.id = userkeywords.keyword_id AND "
            "(EXISTS (SELECT 1 "
            "FROM userkeywords "
            "WHERE keywords.id = userkeywords.keyword_id AND "
            "(EXISTS (SELECT 1 "
            "FROM users "
            "WHERE users.id = userkeywords.user_id AND users.name = :name_1)"
            "))))))",
            checkparams={"name_1": "user7"},
        )

        q2 = self.session.query(User).filter(
            User.user_keywords.any(
                UserKeyword.keyword.has(
                    Keyword.user_keyword.has(
                        UserKeyword.user.has(User.name == "user7")
                    )
                )
            )
        )
        self._equivalent(q1, q2)

    def test_filter_has_chained_has_to_any(self):
        User = self.classes.User
        Singular = self.classes.Singular
        Keyword = self.classes.Keyword

        q1 = self.session.query(User).filter(
            User.common_singular.has(Keyword.keyword == "brown")
        )
        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM singular "
            "WHERE singular.id = users.singular_id AND (EXISTS (SELECT 1 "
            "FROM keywords "
            "WHERE singular.id = keywords.singular_id AND "
            "keywords.keyword = :keyword_1)))",
            checkparams={"keyword_1": "brown"},
        )

        q2 = self.session.query(User).filter(
            User.singular.has(
                Singular.keywords.any(Keyword.keyword == "brown")
            )
        )
        self._equivalent(q1, q2)

    def test_filter_has_scalar_raises(self):
        User = self.classes.User
        assert_raises_message(
            exc.ArgumentError,
            r"Can't apply keyword arguments to column-targeted",
            User.singular_keyword.has,
            keyword="brown",
        )

    def test_filter_eq_chained_has_to_any(self):
        User = self.classes.User
        Keyword = self.classes.Keyword
        Singular = self.classes.Singular

        q1 = self.session.query(User).filter(User.singular_keyword == "brown")
        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM singular "
            "WHERE singular.id = users.singular_id AND (EXISTS (SELECT 1 "
            "FROM keywords "
            "WHERE singular.id = keywords.singular_id "
            "AND keywords.keyword = :keyword_1)))",
            checkparams={"keyword_1": "brown"},
        )
        q2 = self.session.query(User).filter(
            User.singular.has(
                Singular.keywords.any(Keyword.keyword == "brown")
            )
        )

        self._equivalent(q1, q2)

    def test_filter_contains_chained_any_to_has(self):
        User = self.classes.User
        Keyword = self.classes.Keyword
        UserKeyword = self.classes.UserKeyword

        q1 = self.session.query(User).filter(
            User.common_keyword_name.contains("brown")
        )
        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM userkeywords "
            "WHERE users.id = userkeywords.user_id AND (EXISTS (SELECT 1 "
            "FROM keywords "
            "WHERE keywords.id = userkeywords.keyword_id AND "
            "keywords.keyword = :keyword_1)))",
            checkparams={"keyword_1": "brown"},
        )

        q2 = self.session.query(User).filter(
            User.user_keywords.any(
                UserKeyword.keyword.has(Keyword.keyword == "brown")
            )
        )
        self._equivalent(q1, q2)

    def test_filter_contains_chained_any_to_has_to_eq(self):
        User = self.classes.User
        Keyword = self.classes.Keyword
        UserKeyword = self.classes.UserKeyword
        Singular = self.classes.Singular

        singular = self.session.query(Singular).order_by(Singular.id).first()

        q1 = self.session.query(Keyword).filter(
            Keyword.singulars.contains(singular)
        )
        self.assert_compile(
            q1,
            "SELECT keywords.id AS keywords_id, "
            "keywords.keyword AS keywords_keyword, "
            "keywords.singular_id AS keywords_singular_id "
            "FROM keywords "
            "WHERE EXISTS (SELECT 1 "
            "FROM userkeywords "
            "WHERE keywords.id = userkeywords.keyword_id AND "
            "(EXISTS (SELECT 1 "
            "FROM users "
            "WHERE users.id = userkeywords.user_id AND "
            ":param_1 = users.singular_id)))",
            checkparams={"param_1": singular.id},
        )

        q2 = self.session.query(Keyword).filter(
            Keyword.user_keywords.any(
                UserKeyword.user.has(User.singular == singular)
            )
        )
        self._equivalent(q1, q2)

    def test_has_criterion_nul(self):
        # but we don't allow that with any criterion...
        User = self.classes.User
        self.classes.Singular

        assert_raises_message(
            exc.ArgumentError,
            r"Non-empty has\(\) not allowed",
            User.singular_value.has,
            User.singular_value == "singular4",
        )

    def test_has_kwargs_nul(self):
        # ... or kwargs
        User = self.classes.User
        self.classes.Singular

        assert_raises_message(
            exc.ArgumentError,
            r"Can't apply keyword arguments to column-targeted",
            User.singular_value.has,
            singular_value="singular4",
        )

    def test_filter_scalar_object_contains_fails_nul_nul(self):
        Keyword = self.classes.Keyword

        assert_raises(
            exc.InvalidRequestError, lambda: Keyword.user.contains(self.u)
        )

    def test_filter_scalar_object_any_fails_nul_nul(self):
        Keyword = self.classes.Keyword

        assert_raises(
            exc.InvalidRequestError, lambda: Keyword.user.any(name="user2")
        )

    def test_filter_scalar_column_like(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(User.singular_value.like("foo")),
            self.session.query(User).filter(
                User.singular.has(Singular.value.like("foo"))
            ),
        )

    def test_filter_scalar_column_contains(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                User.singular_value.contains("foo")
            ),
            self.session.query(User).filter(
                User.singular.has(Singular.value.contains("foo"))
            ),
        )

    def test_filter_scalar_column_eq(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(User.singular_value == "foo"),
            self.session.query(User).filter(
                User.singular.has(Singular.value == "foo")
            ),
        )

    def test_filter_scalar_column_ne(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(User.singular_value != "foo"),
            self.session.query(User).filter(
                User.singular.has(Singular.value != "foo")
            ),
        )

    def test_filter_scalar_column_eq_nul(self):
        User = self.classes.User
        Singular = self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(User.singular_value == None),
            self.session.query(User).filter(
                or_(
                    User.singular.has(Singular.value == None),
                    User.singular == None,
                )
            ),
        )

    def test_filter_collection_has_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(
            exc.InvalidRequestError, lambda: User.keywords.has(keyword="quick")
        )

    def test_filter_collection_eq_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(
            exc.InvalidRequestError, lambda: User.keywords == self.kw
        )

    def test_filter_collection_ne_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(
            exc.InvalidRequestError, lambda: User.keywords != self.kw
        )

    def test_join_separate_attr(self):
        User = self.classes.User
        self.assert_compile(
            self.session.query(User).join(
                User.keywords.local_attr, User.keywords.remote_attr
            ),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users JOIN userkeywords ON users.id = "
            "userkeywords.user_id JOIN keywords ON keywords.id = "
            "userkeywords.keyword_id",
        )

    def test_join_single_attr(self):
        User = self.classes.User
        self.assert_compile(
            self.session.query(User).join(*User.keywords.attr),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users JOIN userkeywords ON users.id = "
            "userkeywords.user_id JOIN keywords ON keywords.id = "
            "userkeywords.keyword_id",
        )


class DictOfTupleUpdateTest(fixtures.TestBase):
    def setup_test(self):
        class B(object):
            def __init__(self, key, elem):
                self.key = key
                self.elem = elem

        class A(object):
            elements = association_proxy("orig", "elem", creator=B)

        m = MetaData()
        a = Table("a", m, Column("id", Integer, primary_key=True))
        b = Table(
            "b",
            m,
            Column("id", Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id")),
            Column("elem", String),
        )
        mapper(
            A,
            a,
            properties={
                "orig": relationship(
                    B, collection_class=attribute_mapped_collection("key")
                )
            },
        )
        mapper(B, b)
        self.A = A
        self.B = B

    def test_update_one_elem_dict(self):
        a1 = self.A()
        a1.elements.update({("B", 3): "elem2"})
        eq_(a1.elements, {("B", 3): "elem2"})

    def test_update_multi_elem_dict(self):
        a1 = self.A()
        a1.elements.update({("B", 3): "elem2", ("C", 4): "elem3"})
        eq_(a1.elements, {("B", 3): "elem2", ("C", 4): "elem3"})

    def test_update_one_elem_list(self):
        a1 = self.A()
        a1.elements.update([(("B", 3), "elem2")])
        eq_(a1.elements, {("B", 3): "elem2"})

    def test_update_multi_elem_list(self):
        a1 = self.A()
        a1.elements.update([(("B", 3), "elem2"), (("C", 4), "elem3")])
        eq_(a1.elements, {("B", 3): "elem2", ("C", 4): "elem3"})

    def test_update_one_elem_varg(self):
        a1 = self.A()
        assert_raises_message(
            ValueError,
            "dictionary update sequence requires " "2-element tuples",
            a1.elements.update,
            (("B", 3), "elem2"),
        )

    def test_update_multi_elem_varg(self):
        a1 = self.A()
        assert_raises_message(
            TypeError,
            "update expected at most 1 arguments, got 2",
            a1.elements.update,
            (("B", 3), "elem2"),
            (("C", 4), "elem3"),
        )


class CompositeAccessTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        class Point(cls.Basic):
            def __init__(self, x, y):
                self.x = x
                self.y = y

            def __composite_values__(self):
                return [self.x, self.y]

            __hash__ = None

            def __eq__(self, other):
                return (
                    isinstance(other, Point)
                    and other.x == self.x
                    and other.y == self.y
                )

            def __ne__(self, other):
                return not isinstance(other, Point) or not self.__eq__(other)

        class Graph(cls.DeclarativeBasic):
            __tablename__ = "graph"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(30))

            point_data = relationship("PointData")

            points = association_proxy(
                "point_data",
                "point",
                creator=lambda point: PointData(point=point),
            )

        class PointData(fixtures.ComparableEntity, cls.DeclarativeBasic):
            __tablename__ = "point"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            graph_id = Column(ForeignKey("graph.id"))

            x1 = Column(Integer)
            y1 = Column(Integer)

            point = composite(Point, x1, y1)

        return Point, Graph, PointData

    def test_append(self):
        Point, Graph, PointData = self.classes("Point", "Graph", "PointData")

        g1 = Graph()
        g1.points.append(Point(3, 5))
        eq_(g1.point_data, [PointData(point=Point(3, 5))])

    def test_access(self):
        Point, Graph, PointData = self.classes("Point", "Graph", "PointData")
        g1 = Graph()
        g1.point_data.append(PointData(point=Point(3, 5)))
        g1.point_data.append(PointData(point=Point(10, 7)))
        eq_(g1.points, [Point(3, 5), Point(10, 7)])


class AttributeAccessTest(fixtures.TestBase):
    def teardown_test(self):
        clear_mappers()

    def test_resolve_aliased_class(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            value = Column(String)

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(Integer, ForeignKey(A.id))
            a = relationship(A)
            a_value = association_proxy("a", "value")

        spec = aliased(B).a_value

        is_(spec.owning_class, B)

        spec = B.a_value

        is_(spec.owning_class, B)

    def test_resolved_w_subclass(self):
        # test for issue #4185, as well as several below

        Base = declarative_base()

        class Mixin(object):
            @declared_attr
            def children(cls):
                return association_proxy("_children", "value")

        # 1. build parent, Mixin.children gets invoked, we add
        # Parent.children
        class Parent(Mixin, Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)

            _children = relationship("Child")

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        # 2. declarative builds up SubParent, scans through all attributes
        # over all classes.  Hits Mixin, hits "children", accesses "children"
        # in terms of the class, e.g. SubParent.children.  SubParent isn't
        # mapped yet.  association proxy then sets up "owning_class"
        # as NoneType.
        class SubParent(Parent):
            __tablename__ = "subparent"
            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)

        configure_mappers()

        # 3. which would break here.
        p1 = Parent()
        eq_(p1.children, [])

    def test_resolved_to_correct_class_one(self):
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            _children = relationship("Child")
            children = association_proxy("_children", "value")

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        class SubParent(Parent):
            __tablename__ = "subparent"
            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)

        is_(SubParent.children.owning_class, SubParent)
        is_(Parent.children.owning_class, Parent)

    def test_resolved_to_correct_class_two(self):
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            _children = relationship("Child")

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        class SubParent(Parent):
            __tablename__ = "subparent"
            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)
            children = association_proxy("_children", "value")

        is_(SubParent.children.owning_class, SubParent)

    def test_resolved_to_correct_class_three(self):
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            _children = relationship("Child")

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        class SubParent(Parent):
            __tablename__ = "subparent"
            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)
            children = association_proxy("_children", "value")

        class SubSubParent(SubParent):
            __tablename__ = "subsubparent"
            id = Column(Integer, ForeignKey(SubParent.id), primary_key=True)

        is_(SubParent.children.owning_class, SubParent)
        is_(SubSubParent.children.owning_class, SubSubParent)

    def test_resolved_to_correct_class_four(self):
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            _children = relationship("Child")
            children = association_proxy(
                "_children", "value", creator=lambda value: Child(value=value)
            )

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        class SubParent(Parent):
            __tablename__ = "subparent"
            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)

        sp = SubParent()
        sp.children = "c"
        is_(SubParent.children.owning_class, SubParent)
        is_(Parent.children.owning_class, Parent)

    def test_resolved_to_correct_class_five(self):
        Base = declarative_base()

        class Mixin(object):
            children = association_proxy("_children", "value")

        class Parent(Mixin, Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            _children = relationship("Child")

        class Child(Base):
            __tablename__ = "child"
            parent_id = Column(
                Integer, ForeignKey(Parent.id), primary_key=True
            )
            value = Column(String)

        # this triggers the owning routine, doesn't fail
        Mixin.children

        p1 = Parent()

        c1 = Child(value="c1")
        p1._children.append(c1)
        is_(Parent.children.owning_class, Parent)
        eq_(p1.children, ["c1"])

    def _test_never_assign_nonetype(self):
        foo = association_proxy("x", "y")
        foo._calc_owner(None, None)
        is_(foo.owning_class, None)

        class Bat(object):
            foo = association_proxy("x", "y")

        Bat.foo
        is_(Bat.foo.owning_class, None)

        b1 = Bat()
        assert_raises_message(
            exc.InvalidRequestError,
            "This association proxy has no mapped owning class; "
            "can't locate a mapped property",
            getattr,
            b1,
            "foo",
        )
        is_(Bat.foo.owning_class, None)

        # after all that, we can map it
        mapper(
            Bat,
            Table("bat", MetaData(), Column("x", Integer, primary_key=True)),
        )

        # answer is correct
        is_(Bat.foo.owning_class, Bat)


class ScalarRemoveTest(object):
    useobject = None
    cascade_scalar_deletes = None
    uselist = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "test_a"
            id = Column(Integer, primary_key=True)
            ab = relationship("AB", backref="a", uselist=cls.uselist)
            b = association_proxy(
                "ab",
                "b",
                creator=lambda b: AB(b=b),
                cascade_scalar_deletes=cls.cascade_scalar_deletes,
            )

        if cls.useobject:

            class B(Base):
                __tablename__ = "test_b"
                id = Column(Integer, primary_key=True)
                ab = relationship("AB", backref="b")

            class AB(Base):
                __tablename__ = "test_ab"
                a_id = Column(Integer, ForeignKey(A.id), primary_key=True)
                b_id = Column(Integer, ForeignKey(B.id), primary_key=True)

        else:

            class AB(Base):
                __tablename__ = "test_ab"
                b = Column(Integer)
                a_id = Column(Integer, ForeignKey(A.id), primary_key=True)

    def test_set_nonnone_to_none(self):
        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        a1 = A()

        b1 = B() if self.useobject else 5

        if self.uselist:
            a1.b.append(b1)
        else:
            a1.b = b1

        if self.uselist:
            assert isinstance(a1.ab[0], AB)
        else:
            assert isinstance(a1.ab, AB)

        if self.uselist:
            a1.b.remove(b1)
        else:
            a1.b = None

        if self.uselist:
            eq_(a1.ab, [])
        else:
            if self.cascade_scalar_deletes:
                assert a1.ab is None
            else:
                assert isinstance(a1.ab, AB)
                assert a1.ab.b is None

    def test_set_none_to_none(self):
        if self.uselist:
            return

        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        a1 = A()

        a1.b = None

        assert a1.ab is None

    def test_del_already_nonpresent(self):
        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        a1 = A()

        if self.uselist:
            del a1.b

            eq_(a1.ab, [])

        else:

            def go():
                del a1.b

            assert_raises_message(
                AttributeError, "A.ab object does not have a value", go
            )

    def test_del(self):
        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        b1 = B() if self.useobject else 5

        a1 = A()
        if self.uselist:
            a1.b.append(b1)
        else:
            a1.b = b1

        if self.uselist:
            assert isinstance(a1.ab[0], AB)
        else:
            assert isinstance(a1.ab, AB)

        del a1.b

        if self.uselist:
            eq_(a1.ab, [])
        else:
            assert a1.ab is None

    def test_del_no_proxy(self):
        if not self.uselist:
            return

        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        b1 = B() if self.useobject else 5
        a1 = A()
        a1.b.append(b1)

        del a1.ab

        # this is what it does for now, so maintain that w/ assoc proxy
        eq_(a1.ab, [])

    def test_del_already_nonpresent_no_proxy(self):
        if not self.uselist:
            return

        if self.useobject:
            A, AB, B = self.classes("A", "AB", "B")
        else:
            A, AB = self.classes("A", "AB")

        a1 = A()

        del a1.ab

        # this is what it does for now, so maintain that w/ assoc proxy
        eq_(a1.ab, [])


class ScalarRemoveListObjectCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = True
    cascade_scalar_deletes = True
    uselist = True


class ScalarRemoveScalarObjectCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = True
    cascade_scalar_deletes = True
    uselist = False


class ScalarRemoveListScalarCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = False
    cascade_scalar_deletes = True
    uselist = True


class ScalarRemoveScalarScalarCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = False
    cascade_scalar_deletes = True
    uselist = False


class ScalarRemoveListObjectNoCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = True
    cascade_scalar_deletes = False
    uselist = True


class ScalarRemoveScalarObjectNoCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = True
    cascade_scalar_deletes = False
    uselist = False


class ScalarRemoveListScalarNoCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = False
    cascade_scalar_deletes = False
    uselist = True


class ScalarRemoveScalarScalarNoCascade(
    ScalarRemoveTest, fixtures.DeclarativeMappedTest
):

    useobject = False
    cascade_scalar_deletes = False
    uselist = False


class InfoTest(fixtures.TestBase):
    def test_constructor(self):
        assoc = association_proxy("a", "b", info={"some_assoc": "some_value"})
        eq_(assoc.info, {"some_assoc": "some_value"})

    def test_empty(self):
        assoc = association_proxy("a", "b")
        eq_(assoc.info, {})

    def test_via_cls(self):
        class Foob(object):
            assoc = association_proxy("a", "b")

        eq_(Foob.assoc.info, {})

        Foob.assoc.info["foo"] = "bar"

        eq_(Foob.assoc.info, {"foo": "bar"})


class OnlyRelationshipTest(fixtures.DeclarativeMappedTest):
    run_define_tables = None
    run_create_tables = None
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Foo(Base):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)
            foo = Column(String)  # assume some composite datatype

            bar = association_proxy("foo", "attr")

    def test_setattr(self):
        Foo = self.classes.Foo

        f1 = Foo()

        assert_raises_message(
            NotImplementedError,
            "association proxy to a non-relationship "
            "intermediary is not supported",
            setattr,
            f1,
            "bar",
            "asdf",
        )

    def test_getattr(self):
        Foo = self.classes.Foo

        f1 = Foo()

        assert_raises_message(
            NotImplementedError,
            "association proxy to a non-relationship "
            "intermediary is not supported",
            getattr,
            f1,
            "bar",
        )

    def test_get_class_attr(self):
        Foo = self.classes.Foo

        assert_raises_message(
            NotImplementedError,
            "association proxy to a non-relationship "
            "intermediary is not supported",
            getattr,
            Foo,
            "bar",
        )


class MultiOwnerTest(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    run_define_tables = "each"
    run_create_tables = None
    run_inserts = None
    run_deletes = None
    run_setup_classes = "each"
    run_setup_mappers = "each"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            type = Column(String(5), nullable=False)
            d_values = association_proxy("ds", "value")

            __mapper_args__ = {"polymorphic_on": type}

        class B(A):
            __tablename__ = "b"
            id = Column(ForeignKey("a.id"), primary_key=True)

            c1_id = Column(ForeignKey("c1.id"))

            ds = relationship("D", primaryjoin="D.b_id == B.id")

            __mapper_args__ = {"polymorphic_identity": "b"}

        class C(A):
            __tablename__ = "c"
            id = Column(ForeignKey("a.id"), primary_key=True)

            ds = relationship(
                "D", primaryjoin="D.c_id == C.id", back_populates="c"
            )

            __mapper_args__ = {"polymorphic_identity": "c"}

        class C1(C):
            __tablename__ = "c1"
            id = Column(ForeignKey("c.id"), primary_key=True)

            csub_only_data = relationship("B")  # uselist=True relationship

            ds = relationship(
                "D", primaryjoin="D.c1_id == C1.id", back_populates="c"
            )

            __mapper_args__ = {"polymorphic_identity": "c1"}

        class C2(C):
            __tablename__ = "c2"
            id = Column(ForeignKey("c.id"), primary_key=True)

            csub_only_data = Column(String(50))  # scalar Column

            ds = relationship(
                "D", primaryjoin="D.c2_id == C2.id", back_populates="c"
            )

            __mapper_args__ = {"polymorphic_identity": "c2"}

        class D(Base):
            __tablename__ = "d"
            id = Column(Integer, primary_key=True)
            value = Column(String(50))
            b_id = Column(ForeignKey("b.id"))
            c_id = Column(ForeignKey("c.id"))
            c1_id = Column(ForeignKey("c1.id"))
            c2_id = Column(ForeignKey("c2.id"))

            c = relationship("C", primaryjoin="D.c_id == C.id")

            c_data = association_proxy("c", "csub_only_data")

    def _assert_raises_ambiguous(self, fn, *arg, **kw):
        assert_raises_message(
            AttributeError,
            "Association proxy D.c refers to an attribute 'csub_only_data'",
            fn,
            *arg,
            **kw
        )

    def _assert_raises_attribute(self, message, fn, *arg, **kw):
        assert_raises_message(AttributeError, message, fn, *arg, **kw)

    def test_column_collection_expressions(self):
        B, C, C2 = self.classes("B", "C", "C2")

        self.assert_compile(
            B.d_values.contains("b1"),
            "EXISTS (SELECT 1 FROM d, b WHERE d.b_id = b.id "
            "AND (d.value LIKE '%' || :value_1 || '%'))",
        )

        self.assert_compile(
            C2.d_values.contains("c2"),
            "EXISTS (SELECT 1 FROM d, c2 WHERE d.c2_id = c2.id "
            "AND (d.value LIKE '%' || :value_1 || '%'))",
        )

        self.assert_compile(
            C.d_values.contains("c1"),
            "EXISTS (SELECT 1 FROM d, c WHERE d.c_id = c.id "
            "AND (d.value LIKE '%' || :value_1 || '%'))",
        )

    def test_subclass_only_owner_none(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D()
        eq_(d1.c_data, None)

    def test_subclass_only_owner_assign(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C2())
        d1.c_data = "some c2"
        eq_(d1.c_data, "some c2")

    def test_subclass_only_owner_get(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C2(csub_only_data="some c2"))
        eq_(d1.c_data, "some c2")

    def test_subclass_only_owner_none_raise(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D()
        eq_(d1.c_data, None)

    def test_subclass_only_owner_delete(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C2(csub_only_data="some c2"))
        eq_(d1.c.csub_only_data, "some c2")
        del d1.c_data
        assert not hasattr(d1.c, "csub_only_data")

    def test_subclass_only_owner_assign_passes(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C())
        d1.c_data = "some c1"

        # not mapped, but we set it
        eq_(d1.c.csub_only_data, "some c1")

    def test_subclass_only_owner_get_raises(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C())
        self._assert_raises_attribute(
            "'C' object has no attribute 'csub_only_data'",
            getattr,
            d1,
            "c_data",
        )

    def test_subclass_only_owner_delete_raises(self):
        D, C, C2 = self.classes("D", "C", "C2")

        d1 = D(c=C2(csub_only_data="some c2"))
        eq_(d1.c_data, "some c2")

        # now switch
        d1.c = C()

        self._assert_raises_attribute("csub_only_data", delattr, d1, "c_data")

    def test_subclasses_conflicting_types(self):
        B, D, C, C1, C2 = self.classes("B", "D", "C", "C1", "C2")

        bs = [B(), B()]
        d1 = D(c=C1(csub_only_data=bs))
        d2 = D(c=C2(csub_only_data="some c2"))

        association_proxy_object = inspect(D).all_orm_descriptors["c_data"]
        inst1 = association_proxy_object.for_class(D, d1)
        inst2 = association_proxy_object.for_class(D, d2)

        eq_(inst1._target_is_object, True)
        eq_(inst2._target_is_object, False)

        # both instances are cached
        inst0 = association_proxy_object.for_class(D)
        eq_(inst0._lookup_cache, {C1: inst1, C2: inst2})

        # cache works
        is_(association_proxy_object.for_class(D, d1), inst1)
        is_(association_proxy_object.for_class(D, d2), inst2)

    def test_col_expressions_not_available(self):
        (D,) = self.classes("D")

        self._assert_raises_ambiguous(lambda: D.c_data == 5)

    def test_rel_expressions_not_available(self):
        (
            B,
            D,
        ) = self.classes("B", "D")

        self._assert_raises_ambiguous(lambda: D.c_data.any(B.id == 5))


class ProxyOfSynonymTest(AssertsCompiledSQL, fixtures.DeclarativeMappedTest):
    __dialect__ = "default"

    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.orm import synonym

        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)
            bs = relationship("B", backref="a")
            data_syn = synonym("data")

            b_data = association_proxy("bs", "data_syn")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            data = Column(String)
            data_syn = synonym("data")

            a_data = association_proxy("a", "data_syn")

    def test_o2m_instance_getter(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B(data="bdata1"), B(data="bdata2")])
        eq_(a1.b_data, ["bdata1", "bdata2"])

    def test_m2o_instance_getter(self):
        A, B = self.classes("A", "B")

        b1 = B(a=A(data="adata"))
        eq_(b1.a_data, "adata")

    def test_o2m_expr(self):
        A, B = self.classes("A", "B")

        self.assert_compile(
            A.b_data == "foo",
            "EXISTS (SELECT 1 FROM a, b WHERE a.id = b.a_id "
            "AND b.data = :data_1)",
        )


class SynonymOfProxyTest(AssertsCompiledSQL, fixtures.DeclarativeMappedTest):
    __dialect__ = "default"

    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.orm import synonym

        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)
            bs = relationship("B", backref="a")

            b_data = association_proxy("bs", "data")

            b_data_syn = synonym("b_data")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            data = Column(String)

    def test_hasattr(self):
        A, B = self.classes("A", "B")
        is_false(hasattr(A.b_data_syn, "nonexistent"))

    def test_o2m_instance_getter(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B(data="bdata1"), B(data="bdata2")])
        eq_(a1.b_data_syn, ["bdata1", "bdata2"])

    def test_o2m_expr(self):
        A, B = self.classes("A", "B")

        self.assert_compile(
            A.b_data_syn == "foo",
            "EXISTS (SELECT 1 FROM a, b WHERE a.id = b.a_id "
            "AND b.data = :data_1)",
        )


class ProxyHybridTest(fixtures.DeclarativeMappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.ext.hybrid import hybrid_property
        from sqlalchemy.orm.interfaces import PropComparator

        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            bs = relationship("B")

            b_data = association_proxy("bs", "value")
            well_behaved_b_data = association_proxy("bs", "well_behaved_value")

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)
            aid = Column(ForeignKey("a.id"))
            data = Column(String(50))

            @hybrid_property
            def well_behaved_value(self):
                return self.data

            @well_behaved_value.setter
            def well_behaved_value(self, value):
                self.data = value

            @hybrid_property
            def value(self):
                return self.data

            @value.setter
            def value(self, value):
                self.data = value

            @value.comparator
            class value(PropComparator):
                # comparator has no proxy __getattr__, so we can't
                # get to impl to see what we ar proxying towards.
                # as of #4690 we assume column-oriented proxying
                def __init__(self, cls):
                    self.cls = cls

            @hybrid_property
            def well_behaved_w_expr(self):
                return self.data

            @well_behaved_w_expr.setter
            def well_behaved_w_expr(self, value):
                self.data = value

            @well_behaved_w_expr.expression
            def well_behaved_w_expr(cls):
                return cast(cls.data, Integer)

        class C(Base):
            __tablename__ = "c"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))
            _b = relationship("B")
            attr = association_proxy("_b", "well_behaved_w_expr")

    def test_get_ambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B(data="b1")])
        eq_(a1.b_data[0], "b1")

    def test_get_nonambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B(data="b1")])
        eq_(a1.well_behaved_b_data[0], "b1")

    def test_set_ambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B()])

        a1.b_data[0] = "b1"
        eq_(a1.b_data[0], "b1")

    def test_set_nonambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B()])

        a1.b_data[0] = "b1"
        eq_(a1.well_behaved_b_data[0], "b1")

    def test_expr_nonambiguous(self):
        A, B = self.classes("A", "B")

        eq_(
            str(A.well_behaved_b_data == 5),
            "EXISTS (SELECT 1 \nFROM a, b \nWHERE "
            "a.id = b.aid AND b.data = :data_1)",
        )

    def test_get_classlevel_ambiguous(self):
        A, B = self.classes("A", "B")

        eq_(
            str(A.b_data),
            "ColumnAssociationProxyInstance"
            "(AssociationProxy('bs', 'value'))",
        )

    def test_comparator_ambiguous(self):
        A, B = self.classes("A", "B")

        s = fixture_session()
        self.assert_compile(
            s.query(A).filter(A.b_data.any()),
            "SELECT a.id AS a_id FROM a WHERE EXISTS "
            "(SELECT 1 FROM b WHERE a.id = b.aid)",
        )

    def test_explicit_expr(self):
        (C,) = self.classes("C")

        s = fixture_session()
        self.assert_compile(
            s.query(C).filter_by(attr=5),
            "SELECT c.id AS c_id, c.b_id AS c_b_id FROM c WHERE EXISTS "
            "(SELECT 1 FROM b WHERE b.id = c.b_id AND "
            "CAST(b.data AS INTEGER) = :param_1)",
        )


class ProxyPlainPropertyTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):

        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            bs = relationship("B")

            b_data = association_proxy("bs", "value")

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)
            aid = Column(ForeignKey("a.id"))
            data = Column(String(50))

            @property
            def value(self):
                return self.data

            @value.setter
            def value(self, value):
                self.data = value

    def test_get_ambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B(data="b1")])
        eq_(a1.b_data[0], "b1")

    def test_set_ambiguous(self):
        A, B = self.classes("A", "B")

        a1 = A(bs=[B()])

        a1.b_data[0] = "b1"
        eq_(a1.b_data[0], "b1")

    def test_get_classlevel_ambiguous(self):
        A, B = self.classes("A", "B")

        eq_(
            str(A.b_data),
            "AmbiguousAssociationProxyInstance"
            "(AssociationProxy('bs', 'value'))",
        )

    def test_expr_ambiguous(self):
        A, B = self.classes("A", "B")

        assert_raises_message(
            AttributeError,
            "Association proxy A.bs refers to an attribute "
            "'value' that is not directly mapped",
            lambda: A.b_data == 5,
        )


class ScopeBehaviorTest(fixtures.DeclarativeMappedTest):
    # test some GC scenarios, including issue #4268

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String(50))
            bs = relationship("B")

            b_dyn = relationship("B", lazy="dynamic", viewonly=True)

            b_data = association_proxy("bs", "data")

            b_dynamic_data = association_proxy("bs", "data")

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)
            aid = Column(ForeignKey("a.id"))
            data = Column(String(50))

    @classmethod
    def insert_data(cls, connection):
        A, B = cls.classes("A", "B")

        s = Session(connection)
        s.add_all(
            [
                A(id=1, bs=[B(data="b1"), B(data="b2")]),
                A(id=2, bs=[B(data="b3"), B(data="b4")]),
            ]
        )
        s.commit()
        s.close()

    def test_plain_collection_gc(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)
        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.bs  # noqa

        del a1

        gc_collect()

        assert (A, (1,), None) not in s.identity_map

    @testing.fails("dynamic relationship strong references parent")
    def test_dynamic_collection_gc(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_dyn  # noqa

        del a1

        gc_collect()

        # also fails, AppenderQuery holds onto parent
        assert (A, (1,), None) not in s.identity_map

    @testing.fails("association proxy strong references parent")
    def test_associated_collection_gc(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_data  # noqa

        del a1

        gc_collect()

        assert (A, (1,), None) not in s.identity_map

    @testing.fails("association proxy strong references parent")
    def test_associated_dynamic_gc(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_dynamic_data  # noqa

        del a1

        gc_collect()

        assert (A, (1,), None) not in s.identity_map

    def test_plain_collection_iterate(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.bs

        del a1

        gc_collect()

        assert len(a1bs) == 2

    def test_dynamic_collection_iterate(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_dyn  # noqa

        del a1

        gc_collect()

        assert len(list(a1bs)) == 2

    def test_associated_collection_iterate(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_data

        del a1

        gc_collect()

        assert len(a1bs) == 2

    def test_associated_dynamic_iterate(self):
        A, B = self.classes("A", "B")

        s = Session(testing.db)

        a1 = s.query(A).filter_by(id=1).one()

        a1bs = a1.b_dynamic_data

        del a1

        gc_collect()

        assert len(a1bs) == 2
