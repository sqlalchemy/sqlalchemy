from __future__ import annotations

import copy
import dataclasses
import pickle
from typing import Any
from typing import Dict

from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.ext.mutable import MutableSet
from sqlalchemy.orm import attributes
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.orm.instrumentation import ClassManager
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.entities import BasicEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import picklers
from sqlalchemy.types import PickleType
from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import VARCHAR


class Foo(BasicEntity):
    pass


class SubFoo(Foo):
    pass


class Foo2(BasicEntity):
    pass


class FooWithEq:
    def __init__(self, **kw):
        for k in kw:
            setattr(self, k, kw[k])

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


class FooWNoHash(BasicEntity):
    __hash__ = None


class Point(MutableComposite):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __composite_values__(self):
        return self.x, self.y

    def __getstate__(self):
        return self.x, self.y

    def __setstate__(self, state):
        self.x, self.y = state

    def __eq__(self, other):
        return (
            isinstance(other, Point)
            and other.x == self.x
            and other.y == self.y
        )


class MyPoint(Point):
    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, tuple):
            value = Point(*value)
        return value


@dataclasses.dataclass
class DCPoint(MutableComposite):
    x: int
    y: int

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __getstate__(self):
        return self.x, self.y

    def __setstate__(self, state):
        self.x, self.y = state


@dataclasses.dataclass
class MyDCPoint(MutableComposite):
    x: int
    y: int

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __getstate__(self):
        return self.x, self.y

    def __setstate__(self, state):
        self.x, self.y = state

    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, tuple):
            value = MyDCPoint(*value)
        return value


class _MutableDictTestFixture:
    @classmethod
    def _type_fixture(cls):
        return MutableDict

    def teardown_test(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()


class MiscTest(fixtures.TestBase):
    @testing.combinations(True, False, argnames="pickleit")
    def test_pickle_parent_multi_attrs(self, registry, connection, pickleit):
        """test #8133"""

        local_foo = Table(
            "lf",
            registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("j1", MutableDict.as_mutable(PickleType)),
            Column("j2", MutableDict.as_mutable(PickleType)),
            Column("j3", MutableDict.as_mutable(PickleType)),
            Column("j4", MutableDict.as_mutable(PickleType)),
        )

        registry.map_imperatively(Foo2, local_foo)
        registry.metadata.create_all(connection)

        with Session(connection) as sess:
            data = dict(
                j1={"a": 1},
                j2={"b": 2},
                j3={"c": 3},
                j4={"d": 4},
            )
            lf = Foo2(**data)
            sess.add(lf)
            sess.commit()

        all_attrs = {"j1", "j2", "j3", "j4"}
        for attr in all_attrs:
            for loads, dumps in picklers():
                with Session(connection) as sess:
                    f1 = sess.scalars(select(Foo2)).first()
                    if pickleit:
                        f2 = loads(dumps(f1))
                    else:
                        f2 = f1

                existing_dict = getattr(f2, attr)
                existing_dict["q"] = "c"
                eq_(
                    inspect(f2).attrs[attr].history,
                    ([existing_dict], (), ()),
                )
                for other_attr in all_attrs.difference([attr]):
                    a = inspect(f2).attrs[other_attr].history
                    b = ((), [data[other_attr]], ())
                    eq_(a, b)

    @testing.combinations("key_present", "key_non_present", argnames="present")
    @testing.combinations(
        ("transient", True),
        ("detached", True),
        ("detached", False),
        argnames="merge_subject, load",
    )
    @testing.requires.json_type
    def test_session_merge(
        self, decl_base, connection, present, load, merge_subject
    ):
        """test #8446"""

        class Thing(decl_base):
            __tablename__ = "thing"
            id = Column(Integer, primary_key=True)
            data = Column(MutableDict.as_mutable(JSON))

        decl_base.metadata.create_all(connection)

        with Session(connection) as sess:
            sess.add(Thing(id=1, data={"foo": "bar"}))
            sess.commit()

        if merge_subject == "transient":
            t1_to_merge = Thing(id=1, data={"foo": "bar"})
        elif merge_subject == "detached":
            with Session(connection) as sess:
                t1_to_merge = sess.get(Thing, 1)

        with Session(connection) as sess:
            already_present = None
            if present == "key_present":
                already_present = sess.get(Thing, 1)

            t1_merged = sess.merge(t1_to_merge, load=load)

            t1_merged.data["foo"] = "bat"
            if present == "key_present":
                is_(t1_merged, already_present)

            is_true(inspect(t1_merged).attrs.data.history.added)

    def test_no_duplicate_reg_w_inheritance(self, decl_base):
        """test #9676"""

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)

            json: Mapped[Dict[str, Any]] = mapped_column(
                MutableDict.as_mutable(JSON())
            )

        class B(A):
            pass

        class C(B):
            pass

        decl_base.registry.configure()

        # the event hook itself doesnt do anything for repeated calls
        # already, so there's really nothing else to assert other than there's
        # only one "set" event listener

        eq_(len(A.json.dispatch.set), 1)
        eq_(len(B.json.dispatch.set), 1)
        eq_(len(C.json.dispatch.set), 1)


class _MutableDictTestBase(_MutableDictTestFixture):
    run_define_tables = "each"

    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.mapper_registry.map_imperatively(Foo, foo)

    def test_coerce_none(self):
        sess = fixture_session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, None)

    def test_coerce_raise(self):
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects of type",
            Foo,
            data={1, 2, 3},
        )

    def test_in_place_mutation(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.commit()

        f1.data["a"] = "c"
        sess.commit()

        eq_(f1.data, {"a": "c"})

    def test_modified_event(self):
        canary = mock.Mock()
        event.listen(Foo.data, "modified", canary)

        f1 = Foo(data={"a": "b"})
        f1.data["a"] = "c"

        eq_(
            canary.mock_calls,
            [
                mock.call(
                    f1,
                    attributes.AttributeEventToken(
                        Foo.data.impl, attributes.OP_MODIFIED
                    ),
                )
            ],
        )

    def test_clear(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.commit()

        f1.data.clear()
        sess.commit()

        eq_(f1.data, {})

    def test_update(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.commit()

        f1.data.update({"a": "z"})
        sess.commit()

        eq_(f1.data, {"a": "z"})

    def test_pop(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b", "c": "d"})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.pop("a"), "b")
        sess.commit()

        assert_raises(KeyError, f1.data.pop, "g")

        eq_(f1.data, {"c": "d"})

    def test_pop_default(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b", "c": "d"})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.pop("a", "q"), "b")
        eq_(f1.data.pop("a", "q"), "q")
        sess.commit()

        eq_(f1.data, {"c": "d"})

    def test_pop_default_none(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b", "c": "d"})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.pop("a", None), "b")
        eq_(f1.data.pop("a", None), None)
        sess.commit()

        eq_(f1.data, {"c": "d"})

    def test_popitem(self):
        sess = fixture_session()

        orig = {"a": "b", "c": "d"}

        # the orig dict remains unchanged when we assign,
        # but just making this future-proof
        data = dict(orig)
        f1 = Foo(data=data)
        sess.add(f1)
        sess.commit()

        k, v = f1.data.popitem()
        assert k in ("a", "c")
        orig.pop(k)

        sess.commit()

        eq_(f1.data, orig)

    def test_setdefault(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.setdefault("c", "d"), "d")
        sess.commit()

        eq_(f1.data, {"a": "b", "c": "d"})

        eq_(f1.data.setdefault("c", "q"), "d")
        sess.commit()

        eq_(f1.data, {"a": "b", "c": "d"})

        eq_(f1.data.setdefault("w", None), None)
        sess.commit()
        eq_(f1.data, {"a": "b", "c": "d", "w": None})

    def test_replace(self):
        sess = fixture_session()
        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.flush()

        f1.data = {"b": "c"}
        sess.commit()
        eq_(f1.data, {"b": "c"})

    def test_replace_itself_still_ok(self):
        sess = fixture_session()
        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.flush()

        f1.data = f1.data
        f1.data["b"] = "c"
        sess.commit()
        eq_(f1.data, {"a": "b", "b": "c"})

    def test_pickle_parent(self):
        sess = fixture_session()

        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.commit()
        f1.data
        sess.close()

        for loads, dumps in picklers():
            sess = fixture_session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data["a"] = "c"
            assert f2 in sess.dirty

    def test_unrelated_flush(self):
        sess = fixture_session()
        f1 = Foo(data={"a": "b"}, unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data["a"] = "c"
        sess.commit()
        eq_(f1.data["a"], "c")

    def _test_non_mutable(self):
        sess = fixture_session()

        f1 = Foo(non_mutable_data={"a": "b"})
        sess.add(f1)
        sess.commit()

        f1.non_mutable_data["a"] = "c"
        sess.commit()

        eq_(f1.non_mutable_data, {"a": "b"})

    def test_copy(self):
        f1 = Foo(data={"a": "b"})
        f1.data = copy.copy(f1.data)
        eq_(f1.data, {"a": "b"})

    def test_deepcopy(self):
        f1 = Foo(data={"a": "b"})
        f1.data = copy.deepcopy(f1.data)
        eq_(f1.data, {"a": "b"})


class _MutableListTestFixture:
    @classmethod
    def _type_fixture(cls):
        return MutableList

    def teardown_test(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()


class _MutableListTestBase(_MutableListTestFixture):
    run_define_tables = "each"

    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.mapper_registry.map_imperatively(Foo, foo)

    def test_coerce_none(self):
        sess = fixture_session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, None)

    def test_coerce_raise(self):
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects of type",
            Foo,
            data={1, 2, 3},
        )

    def test_in_place_mutation_int(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data[0] = 3
        sess.commit()

        eq_(f1.data, [3, 2])

    def test_in_place_mutation_str(self):
        sess = fixture_session()

        f1 = Foo(data=["one", "two"])
        sess.add(f1)
        sess.commit()

        f1.data[0] = "three"
        sess.commit()

        eq_(f1.data, ["three", "two"])

    def test_in_place_slice_mutation_int(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2, 3, 4])
        sess.add(f1)
        sess.commit()

        f1.data[1:3] = 5, 6
        sess.commit()

        eq_(f1.data, [1, 5, 6, 4])

    def test_in_place_slice_mutation_str(self):
        sess = fixture_session()

        f1 = Foo(data=["one", "two", "three", "four"])
        sess.add(f1)
        sess.commit()

        f1.data[1:3] = "five", "six"
        sess.commit()

        eq_(f1.data, ["one", "five", "six", "four"])

    def test_del_slice(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2, 3, 4])
        sess.add(f1)
        sess.commit()

        del f1.data[1:3]
        sess.commit()

        eq_(f1.data, [1, 4])

    def test_clear(self):
        if not hasattr(list, "clear"):
            # py2 list doesn't have 'clear'
            return
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data.clear()
        sess.commit()

        eq_(f1.data, [])

    def test_pop(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2, 3])
        sess.add(f1)
        sess.commit()

        eq_(f1.data.pop(), 3)
        eq_(f1.data.pop(0), 1)
        sess.commit()

        assert_raises(IndexError, f1.data.pop, 5)

        eq_(f1.data, [2])

    def test_append(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data.append(5)
        sess.commit()

        eq_(f1.data, [1, 2, 5])

    def test_extend(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data.extend([5])
        sess.commit()

        eq_(f1.data, [1, 2, 5])

    def test_operator_extend(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data += [5]
        sess.commit()

        eq_(f1.data, [1, 2, 5])

    def test_insert(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()

        f1.data.insert(1, 5)
        sess.commit()

        eq_(f1.data, [1, 5, 2])

    def test_remove(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2, 3])
        sess.add(f1)
        sess.commit()

        f1.data.remove(2)
        sess.commit()

        eq_(f1.data, [1, 3])

    def test_sort(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 3, 2])
        sess.add(f1)
        sess.commit()

        f1.data.sort()
        sess.commit()

        eq_(f1.data, [1, 2, 3])

    def test_sort_w_key(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 3, 2])
        sess.add(f1)
        sess.commit()

        f1.data.sort(key=lambda elem: -1 * elem)
        sess.commit()

        eq_(f1.data, [3, 2, 1])

    def test_sort_w_reverse_kwarg(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 3, 2])
        sess.add(f1)
        sess.commit()

        f1.data.sort(reverse=True)
        sess.commit()

        eq_(f1.data, [3, 2, 1])

    def test_reverse(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 3, 2])
        sess.add(f1)
        sess.commit()

        f1.data.reverse()
        sess.commit()

        eq_(f1.data, [2, 3, 1])

    def test_pickle_parent(self):
        sess = fixture_session()

        f1 = Foo(data=[1, 2])
        sess.add(f1)
        sess.commit()
        f1.data
        sess.close()

        for loads, dumps in picklers():
            sess = fixture_session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data[0] = 3
            assert f2 in sess.dirty

    def test_unrelated_flush(self):
        sess = fixture_session()
        f1 = Foo(data=[1, 2], unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data[0] = 3
        sess.commit()
        eq_(f1.data[0], 3)

    def test_copy(self):
        f1 = Foo(data=[1, 2])
        f1.data = copy.copy(f1.data)
        eq_(f1.data, [1, 2])

    def test_deepcopy(self):
        f1 = Foo(data=[1, 2])
        f1.data = copy.deepcopy(f1.data)
        eq_(f1.data, [1, 2])

    def test_legacy_pickle_loads(self):
        # due to an inconsistency between pickle and copy, we have to change
        # MutableList to implement a __reduce_ex__ method.   Which means we
        # have to make sure all the old pickle formats are still
        # deserializable since these can be used for persistence. these pickles
        # were all generated using a MutableList that has only __getstate__ and
        # __setstate__.

        # f1 = Foo(data=[1, 2])
        # pickles = [
        #    dumps(f1.data)
        #    for loads, dumps in picklers()
        # ]
        # print(repr(pickles))
        # return

        pickles = [
            b"\x80\x04\x95<\x00\x00\x00\x00\x00\x00\x00\x8c\x16"
            b"sqlalchemy.ext.mutable\x94\x8c\x0bMutableList\x94\x93\x94)"
            b"\x81\x94(K\x01K\x02e]\x94(K\x01K\x02eb.",
            b"ccopy_reg\n_reconstructor\np0\n(csqlalchemy.ext.mutable\n"
            b"MutableList\np1\nc__builtin__\nlist\np2\n(lp3\nI1\naI2\n"
            b"atp4\nRp5\n(lp6\nI1\naI2\nab.",
            b"ccopy_reg\n_reconstructor\nq\x00(csqlalchemy.ext.mutable\n"
            b"MutableList\nq\x01c__builtin__\nlist\nq\x02]q\x03(K\x01K"
            b"\x02etq\x04Rq\x05]q\x06(K\x01K\x02eb.",
            b"\x80\x02csqlalchemy.ext.mutable\nMutableList\nq\x00)\x81q"
            b"\x01(K\x01K\x02e]q\x02(K\x01K\x02eb.",
            b"\x80\x03csqlalchemy.ext.mutable\nMutableList\nq\x00)\x81q"
            b"\x01(K\x01K\x02e]q\x02(K\x01K\x02eb.",
            b"\x80\x04\x95<\x00\x00\x00\x00\x00\x00\x00\x8c\x16"
            b"sqlalchemy.ext.mutable\x94\x8c\x0bMutableList\x94\x93\x94)"
            b"\x81\x94(K\x01K\x02e]\x94(K\x01K\x02eb.",
        ]

        for pickle_ in pickles:
            obj = pickle.loads(pickle_)
            eq_(obj, [1, 2])
            assert isinstance(obj, MutableList)


class _MutableSetTestFixture:
    @classmethod
    def _type_fixture(cls):
        return MutableSet

    def teardown_test(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()


class _MutableSetTestBase(_MutableSetTestFixture):
    run_define_tables = "each"

    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.mapper_registry.map_imperatively(Foo, foo)

    def test_coerce_none(self):
        sess = fixture_session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, None)

    def test_coerce_raise(self):
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects of type",
            Foo,
            data=[1, 2, 3],
        )

    def test_clear(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.clear()
        sess.commit()

        eq_(f1.data, set())

    def test_pop(self):
        sess = fixture_session()

        f1 = Foo(data={1})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.pop(), 1)
        sess.commit()

        assert_raises(KeyError, f1.data.pop)

        eq_(f1.data, set())

    def test_add(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.add(5)
        sess.commit()

        eq_(f1.data, {1, 2, 5})

    def test_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.update({2, 5})
        sess.commit()

        eq_(f1.data, {1, 2, 5})

    def test_binary_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data |= {2, 5}
        sess.commit()

        eq_(f1.data, {1, 2, 5})

    def test_intersection_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.intersection_update({2, 5})
        sess.commit()

        eq_(f1.data, {2})

    def test_binary_intersection_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data &= {2, 5}
        sess.commit()

        eq_(f1.data, {2})

    def test_difference_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.difference_update({2, 5})
        sess.commit()

        eq_(f1.data, {1})

    def test_operator_difference_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data -= {2, 5}
        sess.commit()

        eq_(f1.data, {1})

    def test_symmetric_difference_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data.symmetric_difference_update({2, 5})
        sess.commit()

        eq_(f1.data, {1, 5})

    def test_binary_symmetric_difference_update(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()

        f1.data ^= {2, 5}
        sess.commit()

        eq_(f1.data, {1, 5})

    def test_remove(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2, 3})
        sess.add(f1)
        sess.commit()

        f1.data.remove(2)
        sess.commit()

        eq_(f1.data, {1, 3})

    def test_discard(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2, 3})
        sess.add(f1)
        sess.commit()

        f1.data.discard(2)
        sess.commit()

        eq_(f1.data, {1, 3})

        f1.data.discard(2)
        sess.commit()

        eq_(f1.data, {1, 3})

    def test_pickle_parent(self):
        sess = fixture_session()

        f1 = Foo(data={1, 2})
        sess.add(f1)
        sess.commit()
        f1.data
        sess.close()

        for loads, dumps in picklers():
            sess = fixture_session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data.add(3)
            assert f2 in sess.dirty

    def test_unrelated_flush(self):
        sess = fixture_session()
        f1 = Foo(data={1, 2}, unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data.add(3)
        sess.commit()
        eq_(f1.data, {1, 2, 3})

    def test_copy(self):
        f1 = Foo(data={1, 2})
        f1.data = copy.copy(f1.data)
        eq_(f1.data, {1, 2})

    def test_deepcopy(self):
        f1 = Foo(data={1, 2})
        f1.data = copy.deepcopy(f1.data)
        eq_(f1.data, {1, 2})


class _MutableNoHashFixture:
    @testing.fixture(autouse=True, scope="class")
    def set_class(self):
        global Foo

        _replace_foo = Foo
        Foo = FooWNoHash

        yield
        Foo = _replace_foo

    def test_ensure_not_hashable(self):
        d = {}
        obj = Foo()
        with testing.expect_raises(TypeError):
            d[obj] = True


class MutableListNoHashTest(
    _MutableNoHashFixture, _MutableListTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        MutableList = cls._type_fixture()

        mutable_pickle = MutableList.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", mutable_pickle),
        )


class MutableDictNoHashTest(
    _MutableNoHashFixture, _MutableDictTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()

        mutable_pickle = MutableDict.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", mutable_pickle),
        )


class MutableColumnDefaultTest(_MutableDictTestFixture, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()

        mutable_pickle = MutableDict.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", mutable_pickle, default={}),
        )

    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.mapper_registry.map_imperatively(Foo, foo)

    def test_evt_on_flush_refresh(self):
        # test for #3427

        sess = fixture_session()

        f1 = Foo()
        sess.add(f1)
        sess.flush()
        assert isinstance(f1.data, self._type_fixture())
        assert f1 not in sess.dirty
        f1.data["foo"] = "bar"
        assert f1 in sess.dirty


class MutableWithScalarPickleTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()

        mutable_pickle = MutableDict.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("skip", mutable_pickle),
            Column("data", mutable_pickle),
            Column("non_mutable_data", PickleType),
            Column("unrelated_data", String(50)),
        )

    def test_non_mutable(self):
        self._test_non_mutable()


class MutableWithScalarJSONTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)
            cache_ok = True

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        MutableDict = cls._type_fixture()

        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", MutableDict.as_mutable(JSONEncodedDict)),
            Column("non_mutable_data", JSONEncodedDict),
            Column("unrelated_data", String(50)),
        )

    def test_non_mutable(self):
        self._test_non_mutable()


class MutableColumnCopyJSONTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)
            cache_ok = True

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        MutableDict = cls._type_fixture()

        Base = declarative_base(metadata=metadata)

        class AbstractFoo(Base):
            __abstract__ = True

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            data = Column(MutableDict.as_mutable(JSONEncodedDict))
            non_mutable_data = Column(JSONEncodedDict)
            unrelated_data = Column(String(50))

        class Foo(AbstractFoo):
            __tablename__ = "foo"
            column_prop = column_property(
                func.lower(AbstractFoo.unrelated_data)
            )

        assert Foo.data.property.columns[0].type is not AbstractFoo.data.type

    def test_non_mutable(self):
        self._test_non_mutable()


class MutableColumnCopyArrayTest(_MutableListTestBase, fixtures.MappedTest):
    __requires__ = ("array_type",)

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.sql.sqltypes import ARRAY

        MutableList = cls._type_fixture()

        Base = declarative_base(metadata=metadata)

        class Mixin:
            data = Column(MutableList.as_mutable(ARRAY(Integer)))

        class Foo(Mixin, Base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)

    def test_in_place_mutation_str(self):
        """this test is hardcoded to integer, skip strings"""

    def test_in_place_slice_mutation_str(self):
        """this test is hardcoded to integer, skip strings"""


class MutableListWithScalarPickleTest(
    _MutableListTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        MutableList = cls._type_fixture()

        mutable_pickle = MutableList.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("skip", mutable_pickle),
            Column("data", mutable_pickle),
            Column("non_mutable_data", PickleType),
            Column("unrelated_data", String(50)),
        )


class MutableSetWithScalarPickleTest(_MutableSetTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        MutableSet = cls._type_fixture()

        mutable_pickle = MutableSet.as_mutable(PickleType)
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("skip", mutable_pickle),
            Column("data", mutable_pickle),
            Column("non_mutable_data", PickleType),
            Column("unrelated_data", String(50)),
        )


class MutableAssocWithAttrInheritTest(
    _MutableDictTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", PickleType),
            Column("non_mutable_data", PickleType),
            Column("unrelated_data", String(50)),
        )

        Table(
            "subfoo",
            metadata,
            Column("id", Integer, ForeignKey("foo.id"), primary_key=True),
        )

    def setup_mappers(cls):
        foo = cls.tables.foo
        subfoo = cls.tables.subfoo

        cls.mapper_registry.map_imperatively(Foo, foo)
        cls.mapper_registry.map_imperatively(SubFoo, subfoo, inherits=Foo)
        MutableDict.associate_with_attribute(Foo.data)

    def test_in_place_mutation(self):
        sess = fixture_session()

        f1 = SubFoo(data={"a": "b"})
        sess.add(f1)
        sess.commit()

        f1.data["a"] = "c"
        sess.commit()

        eq_(f1.data, {"a": "c"})

    def test_replace(self):
        sess = fixture_session()
        f1 = SubFoo(data={"a": "b"})
        sess.add(f1)
        sess.flush()

        f1.data = {"b": "c"}
        sess.commit()
        eq_(f1.data, {"b": "c"})


class MutableAssociationScalarPickleTest(
    _MutableDictTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()
        MutableDict.associate_with(PickleType)

        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("skip", PickleType),
            Column("data", PickleType),
            Column("unrelated_data", String(50)),
        )


class MutableAssociationScalarJSONTest(
    _MutableDictTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)
            cache_ok = True

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        MutableDict = cls._type_fixture()
        MutableDict.associate_with(JSONEncodedDict)

        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", JSONEncodedDict),
            Column("unrelated_data", String(50)),
        )


class CustomMutableAssociationScalarJSONTest(
    _MutableDictTestBase, fixtures.MappedTest
):
    CustomMutableDict = None

    @classmethod
    def _type_fixture(cls):
        if not (getattr(cls, "CustomMutableDict")):
            MutableDict = super()._type_fixture()

            class CustomMutableDict(MutableDict):
                pass

            cls.CustomMutableDict = CustomMutableDict
        return cls.CustomMutableDict

    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)
            cache_ok = True

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        CustomMutableDict = cls._type_fixture()
        CustomMutableDict.associate_with(JSONEncodedDict)

        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", JSONEncodedDict),
            Column("unrelated_data", String(50)),
        )

    def test_pickle_parent(self):
        # Picklers don't know how to pickle CustomMutableDict,
        # but we aren't testing that here
        pass

    def test_coerce(self):
        sess = fixture_session()
        f1 = Foo(data={"a": "b"})
        sess.add(f1)
        sess.flush()
        eq_(type(f1.data), self._type_fixture())


class _CompositeTestBase:
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer),
            Column("y", Integer),
            Column("unrelated_data", String(50)),
        )

    def setup_test(self):
        from sqlalchemy.ext import mutable

        mutable._setup_composite_listener()

    def teardown_test(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()

    @classmethod
    def _type_fixture(cls):
        return Point


class MutableCompositeColumnDefaultTest(
    _CompositeTestBase, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer, default=5),
            Column("y", Integer, default=9),
            Column("unrelated_data", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.Point = cls._type_fixture()

        cls.mapper_registry.map_imperatively(
            Foo,
            foo,
            properties={"data": composite(cls.Point, foo.c.x, foo.c.y)},
        )

    def test_evt_on_flush_refresh(self):
        # this still worked prior to #3427 being fixed in any case

        sess = fixture_session()

        f1 = Foo(data=self.Point(None, None))
        sess.add(f1)
        sess.flush()
        eq_(f1.data, self.Point(5, 9))
        assert f1 not in sess.dirty
        f1.data.x = 10
        assert f1 in sess.dirty


class MutableDCCompositeColumnDefaultTest(MutableCompositeColumnDefaultTest):
    @classmethod
    def _type_fixture(cls):
        return DCPoint


class MutableCompositesUnpickleTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.Point = cls._type_fixture()

        cls.mapper_registry.map_imperatively(
            FooWithEq,
            foo,
            properties={"data": composite(cls.Point, foo.c.x, foo.c.y)},
        )

    def test_unpickle_modified_eq(self):
        u1 = FooWithEq(data=self.Point(3, 5))
        for loads, dumps in picklers():
            loads(dumps(u1))


class MutableDCCompositesUnpickleTest(MutableCompositesUnpickleTest):
    @classmethod
    def _type_fixture(cls):
        return DCPoint


class MutableCompositesTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.Point = cls._type_fixture()

        cls.mapper_registry.map_imperatively(
            Foo,
            foo,
            properties={"data": composite(cls.Point, foo.c.x, foo.c.y)},
        )

    def test_in_place_mutation(self):
        sess = fixture_session()
        d = self.Point(3, 4)
        f1 = Foo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data.y = 5
        sess.commit()

        eq_(f1.data, self.Point(3, 5))

    def test_pickle_of_parent(self):
        sess = fixture_session()
        d = self.Point(3, 4)
        f1 = Foo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data
        assert "data" in f1.__dict__
        sess.close()

        for loads, dumps in picklers():
            sess = fixture_session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data.y = 12
            assert f2 in sess.dirty

    def test_set_none(self):
        sess = fixture_session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, self.Point(None, None))

        f1.data.y = 5
        sess.commit()
        eq_(f1.data, self.Point(None, 5))

    def test_set_illegal(self):
        f1 = Foo()
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects",
            setattr,
            f1,
            "data",
            "foo",
        )

    def test_unrelated_flush(self):
        sess = fixture_session()
        f1 = Foo(data=self.Point(3, 4), unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data.x = 5
        sess.commit()

        eq_(f1.data.x, 5)

    def test_dont_reset_on_attr_refresh(self):
        sess = fixture_session()
        f1 = Foo(data=self.Point(3, 4), unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()

        f1.data.x = 5

        # issue 6001, this would reload a new Point() that would be missed
        # by the mutable composite, and tracking would be lost
        sess.refresh(f1, ["unrelated_data"])

        is_(list(f1.data._parents.keys())[0], f1._sa_instance_state)

        f1.data.y = 9

        sess.commit()

        eq_(f1.data.x, 5)
        eq_(f1.data.y, 9)

        f1.data.x = 12

        sess.refresh(f1, ["unrelated_data", "y"])

        is_(list(f1.data._parents.keys())[0], f1._sa_instance_state)

        f1.data.y = 15
        sess.commit()

        eq_(f1.data.x, 12)
        eq_(f1.data.y, 15)


class MutableDCCompositesTest(MutableCompositesTest):
    @classmethod
    def _type_fixture(cls):
        return DCPoint


class MutableCompositeCallableTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        Point = cls._type_fixture()

        # in this case, this is not actually a MutableComposite.
        # so we don't expect it to track changes
        cls.mapper_registry.map_imperatively(
            Foo,
            foo,
            properties={
                "data": composite(lambda x, y: Point(x, y), foo.c.x, foo.c.y)
            },
        )

    def test_basic(self):
        sess = fixture_session()
        f1 = Foo(data=Point(3, 4))
        sess.add(f1)
        sess.flush()
        f1.data.x = 5
        sess.commit()

        # we didn't get the change.
        eq_(f1.data.x, 3)


class MutableCompositeCustomCoerceTest(
    _CompositeTestBase, fixtures.MappedTest
):
    @classmethod
    def _type_fixture(cls):
        return MyPoint

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.Point = cls._type_fixture()

        cls.mapper_registry.map_imperatively(
            Foo,
            foo,
            properties={"data": composite(cls.Point, foo.c.x, foo.c.y)},
        )

    def test_custom_coerce(self):
        f = Foo()
        f.data = (3, 4)
        eq_(f.data, self.Point(3, 4))

    def test_round_trip_ok(self):
        sess = fixture_session()
        f = Foo()
        f.data = (3, 4)

        sess.add(f)
        sess.commit()

        eq_(f.data, self.Point(3, 4))


class MutableDCCompositeCustomCoerceTest(MutableCompositeCustomCoerceTest):
    @classmethod
    def _type_fixture(cls):
        return MyDCPoint


class MutableInheritedCompositesTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer),
            Column("y", Integer),
        )
        Table(
            "subfoo",
            metadata,
            Column("id", Integer, ForeignKey("foo.id"), primary_key=True),
        )

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo
        subfoo = cls.tables.subfoo

        cls.Point = cls._type_fixture()

        cls.mapper_registry.map_imperatively(
            Foo,
            foo,
            properties={"data": composite(cls.Point, foo.c.x, foo.c.y)},
        )
        cls.mapper_registry.map_imperatively(SubFoo, subfoo, inherits=Foo)

    def test_in_place_mutation_subclass(self):
        sess = fixture_session()
        d = self.Point(3, 4)
        f1 = SubFoo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data.y = 5
        sess.commit()

        eq_(f1.data, self.Point(3, 5))

    def test_pickle_of_parent_subclass(self):
        sess = fixture_session()
        d = self.Point(3, 4)
        f1 = SubFoo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data
        assert "data" in f1.__dict__
        sess.close()

        for loads, dumps in picklers():
            sess = fixture_session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data.y = 12
            assert f2 in sess.dirty


class MutableInheritedDCCompositesTest(MutableInheritedCompositesTest):
    @classmethod
    def _type_fixture(cls):
        return DCPoint
