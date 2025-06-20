import pickle
from unittest.mock import call
from unittest.mock import Mock

from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import NO_KEY
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import not_in
from sqlalchemy.testing.assertions import assert_warns
from sqlalchemy.testing.entities import BasicEntity
from sqlalchemy.testing.util import all_partial_orderings
from sqlalchemy.testing.util import gc_collect

# global for pickling tests
MyTest = None
MyTest2 = None


def _set_callable(state, dict_, key, callable_):
    fn = InstanceState._instance_level_callable_processor(
        state.manager, callable_, key
    )
    fn(state, dict_, None)


def _register_attribute(class_, key, **kw):
    kw.setdefault("comparator", object())
    kw.setdefault("parententity", object())

    attributes._register_attribute(class_, key, **kw)


class AttributeImplAPITest(fixtures.MappedTest):
    def _scalar_obj_fixture(self):
        class A:
            pass

        class B:
            pass

        instrumentation.register_class(A)
        instrumentation.register_class(B)
        _register_attribute(A, "b", uselist=False, useobject=True)
        return A, B

    def _collection_obj_fixture(self):
        class A:
            pass

        class B:
            pass

        instrumentation.register_class(A)
        instrumentation.register_class(B)
        _register_attribute(A, "b", uselist=True, useobject=True)
        return A, B

    def test_scalar_obj_remove_invalid(self):
        A, B = self._scalar_obj_fixture()

        a1 = A()
        b1 = B()
        b2 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b is b1

        assert_raises_message(
            ValueError,
            "Object <B at .*?> not "
            "associated with <A at .*?> on attribute 'b'",
            A.b.impl.remove,
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b2,
            None,
        )

    def test_scalar_obj_pop_invalid(self):
        A, B = self._scalar_obj_fixture()

        a1 = A()
        b1 = B()
        b2 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b is b1

        A.b.impl.pop(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b2,
            None,
        )
        assert a1.b is b1

    def test_scalar_obj_pop_valid(self):
        A, B = self._scalar_obj_fixture()

        a1 = A()
        b1 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b is b1

        A.b.impl.pop(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )
        assert a1.b is None

    def test_collection_obj_remove_invalid(self):
        A, B = self._collection_obj_fixture()

        a1 = A()
        b1 = B()
        b2 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b == [b1]

        assert_raises_message(
            ValueError,
            r"list.remove\(.*?\): .* not in list",
            A.b.impl.remove,
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b2,
            None,
        )

    def test_collection_obj_pop_invalid(self):
        A, B = self._collection_obj_fixture()

        a1 = A()
        b1 = B()
        b2 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b == [b1]

        A.b.impl.pop(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b2,
            None,
        )
        assert a1.b == [b1]

    def test_collection_obj_pop_valid(self):
        A, B = self._collection_obj_fixture()

        a1 = A()
        b1 = B()

        A.b.impl.append(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )

        assert a1.b == [b1]

        A.b.impl.pop(
            attributes.instance_state(a1),
            attributes.instance_dict(a1),
            b1,
            None,
        )
        assert a1.b == []


class AttributesTest(fixtures.ORMTest):
    def setup_test(self):
        global MyTest, MyTest2

        class MyTest:
            pass

        class MyTest2:
            pass

    def teardown_test(self):
        global MyTest, MyTest2
        MyTest, MyTest2 = None, None

    def test_basic(self):
        class User:
            pass

        instrumentation.register_class(User)
        _register_attribute(User, "user_id", uselist=False, useobject=False)
        _register_attribute(User, "user_name", uselist=False, useobject=False)
        _register_attribute(
            User, "email_address", uselist=False, useobject=False
        )
        u = User()
        u.user_id = 7
        u.user_name = "john"
        u.email_address = "lala@123.com"
        self.assert_(
            u.user_id == 7
            and u.user_name == "john"
            and u.email_address == "lala@123.com"
        )
        attributes.instance_state(u)._commit_all(attributes.instance_dict(u))
        self.assert_(
            u.user_id == 7
            and u.user_name == "john"
            and u.email_address == "lala@123.com"
        )
        u.user_name = "heythere"
        u.email_address = "foo@bar.com"
        self.assert_(
            u.user_id == 7
            and u.user_name == "heythere"
            and u.email_address == "foo@bar.com"
        )

    def test_pickleness(self):
        instrumentation.register_class(MyTest)
        instrumentation.register_class(MyTest2)
        _register_attribute(MyTest, "user_id", uselist=False, useobject=False)
        _register_attribute(
            MyTest, "user_name", uselist=False, useobject=False
        )
        _register_attribute(
            MyTest, "email_address", uselist=False, useobject=False
        )
        _register_attribute(MyTest2, "a", uselist=False, useobject=False)
        _register_attribute(MyTest2, "b", uselist=False, useobject=False)

        # shouldn't be pickling callables at the class level

        def somecallable(state, passive):
            return None

        _register_attribute(
            MyTest,
            "mt2",
            uselist=True,
            trackparent=True,
            callable_=somecallable,
            useobject=True,
        )

        o = MyTest()
        o.mt2.append(MyTest2())
        o.user_id = 7
        o.mt2[0].a = "abcde"
        pk_o = pickle.dumps(o)

        o2 = pickle.loads(pk_o)
        pk_o2 = pickle.dumps(o2)

        # the above is kind of distrurbing, so let's do it again a little
        # differently.  the string-id in serialization thing is just an
        # artifact of pickling that comes up in the first round-trip.
        # a -> b differs in pickle memoization of 'mt2', but b -> c will
        # serialize identically.

        o3 = pickle.loads(pk_o2)
        pk_o3 = pickle.dumps(o3)
        o4 = pickle.loads(pk_o3)

        # and lastly make sure we still have our data after all that.
        # identical serialzation is great, *if* it's complete :)
        self.assert_(o4.user_id == 7)
        self.assert_(o4.user_name is None)
        self.assert_(o4.email_address is None)
        self.assert_(len(o4.mt2) == 1)
        self.assert_(o4.mt2[0].a == "abcde")
        self.assert_(o4.mt2[0].b is None)

    @testing.requires.predictable_gc
    def test_state_gc(self):
        """test that InstanceState always has a dict, even after host
        object gc'ed."""

        class Foo:
            pass

        instrumentation.register_class(Foo)
        f = Foo()
        state = attributes.instance_state(f)
        f.bar = "foo"
        eq_(state.dict, {"bar": "foo", state.manager.STATE_ATTR: state})
        del f
        gc_collect()
        assert state.obj() is None
        assert state.dict == {}

    @testing.requires.predictable_gc
    def test_object_dereferenced_error(self):
        class Foo:
            pass

        class Bar:
            def __init__(self):
                gc_collect()

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "bars", uselist=True, useobject=True)

        assert_raises_message(
            orm_exc.ObjectDereferencedError,
            "Can't emit change event for attribute "
            "'Foo.bars' - parent object of type <Foo> "
            "has been garbage collected.",
            lambda: Foo().bars.append(Bar()),
        )

    def test_unmapped_instance_raises(self):
        class User:
            pass

        instrumentation.register_class(User)
        _register_attribute(User, "user_name", uselist=False, useobject=False)

        class Blog:
            name = User.user_name

        def go():
            b = Blog()
            return b.name

        assert_raises(
            orm_exc.UnmappedInstanceError,
            go,
        )

    def test_del_scalar_nonobject(self):
        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "b", uselist=False, useobject=False)

        f1 = Foo()

        is_(f1.b, None)

        f1.b = 5

        del f1.b
        is_(f1.b, None)

        f1 = Foo()

        def go():
            del f1.b

        assert_raises_message(
            AttributeError, "Foo.b object does not have a value", go
        )

    def test_del_scalar_object(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "b", uselist=False, useobject=True)

        f1 = Foo()

        is_(f1.b, None)

        f1.b = Bar()

        del f1.b
        is_(f1.b, None)

        def go():
            del f1.b

        assert_raises_message(
            AttributeError, "Foo.b object does not have a value", go
        )

    def test_del_collection_object(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "b", uselist=True, useobject=True)

        f1 = Foo()

        eq_(f1.b, [])

        f1.b = [Bar()]

        del f1.b
        eq_(f1.b, [])

        del f1.b
        eq_(f1.b, [])

    def test_deferred(self):
        class Foo:
            pass

        data = {"a": "this is a", "b": 12}

        def loader(state, keys, passive):
            for k in keys:
                state.dict[k] = data[k]
            return attributes.ATTR_WAS_SET

        instrumentation.register_class(Foo)
        manager = attributes.manager_of_class(Foo)
        manager.expired_attribute_loader = loader
        _register_attribute(Foo, "a", uselist=False, useobject=False)
        _register_attribute(Foo, "b", uselist=False, useobject=False)

        f = Foo()
        attributes.instance_state(f)._expire(
            attributes.instance_dict(f), set()
        )
        eq_(f.a, "this is a")
        eq_(f.b, 12)
        f.a = "this is some new a"
        attributes.instance_state(f)._expire(
            attributes.instance_dict(f), set()
        )
        eq_(f.a, "this is a")
        eq_(f.b, 12)
        attributes.instance_state(f)._expire(
            attributes.instance_dict(f), set()
        )
        f.a = "this is another new a"
        eq_(f.a, "this is another new a")
        eq_(f.b, 12)
        attributes.instance_state(f)._expire(
            attributes.instance_dict(f), set()
        )
        eq_(f.a, "this is a")
        eq_(f.b, 12)
        del f.a
        eq_(f.a, None)
        eq_(f.b, 12)
        attributes.instance_state(f)._commit_all(
            attributes.instance_dict(f), set()
        )
        eq_(f.a, None)
        eq_(f.b, 12)

    def test_deferred_pickleable(self):
        data = {"a": "this is a", "b": 12}

        def loader(state, keys, passive):
            for k in keys:
                state.dict[k] = data[k]
            return attributes.ATTR_WAS_SET

        instrumentation.register_class(MyTest)
        manager = attributes.manager_of_class(MyTest)
        manager.expired_attribute_loader = loader
        _register_attribute(MyTest, "a", uselist=False, useobject=False)
        _register_attribute(MyTest, "b", uselist=False, useobject=False)

        m = MyTest()
        attributes.instance_state(m)._expire(
            attributes.instance_dict(m), set()
        )
        assert "a" not in m.__dict__
        m2 = pickle.loads(pickle.dumps(m))
        assert "a" not in m2.__dict__
        eq_(m2.a, "this is a")
        eq_(m2.b, 12)

    def test_list(self):
        class User:
            pass

        class Address:
            pass

        instrumentation.register_class(User)
        instrumentation.register_class(Address)
        _register_attribute(User, "user_id", uselist=False, useobject=False)
        _register_attribute(User, "user_name", uselist=False, useobject=False)
        _register_attribute(User, "addresses", uselist=True, useobject=True)
        _register_attribute(
            Address, "address_id", uselist=False, useobject=False
        )
        _register_attribute(
            Address, "email_address", uselist=False, useobject=False
        )

        u = User()
        u.user_id = 7
        u.user_name = "john"
        u.addresses = []
        a = Address()
        a.address_id = 10
        a.email_address = "lala@123.com"
        u.addresses.append(a)

        self.assert_(
            u.user_id == 7
            and u.user_name == "john"
            and u.addresses[0].email_address == "lala@123.com"
        )
        (
            u,
            attributes.instance_state(a)._commit_all(
                attributes.instance_dict(a)
            ),
        )
        self.assert_(
            u.user_id == 7
            and u.user_name == "john"
            and u.addresses[0].email_address == "lala@123.com"
        )

        u.user_name = "heythere"
        a = Address()
        a.address_id = 11
        a.email_address = "foo@bar.com"
        u.addresses.append(a)

        eq_(u.user_id, 7)
        eq_(u.user_name, "heythere")
        eq_(u.addresses[0].email_address, "lala@123.com")
        eq_(u.addresses[1].email_address, "foo@bar.com")

    def test_lazytrackparent(self):
        """test that the "hasparent" flag works properly
        when lazy loaders and backrefs are used

        """

        class Post:
            pass

        class Blog:
            pass

        instrumentation.register_class(Post)
        instrumentation.register_class(Blog)

        # set up instrumented attributes with backrefs
        _register_attribute(
            Post,
            "blog",
            uselist=False,
            backref="posts",
            trackparent=True,
            useobject=True,
        )
        _register_attribute(
            Blog,
            "posts",
            uselist=True,
            backref="blog",
            trackparent=True,
            useobject=True,
        )

        # create objects as if they'd been freshly loaded from the database
        # (without history)
        b = Blog()
        p1 = Post()
        _set_callable(
            attributes.instance_state(b),
            attributes.instance_dict(b),
            "posts",
            lambda state, passive: [p1],
        )
        _set_callable(
            attributes.instance_state(p1),
            attributes.instance_dict(p1),
            "blog",
            lambda state, passive: b,
        )
        p1, attributes.instance_state(b)._commit_all(
            attributes.instance_dict(b)
        )

        # no orphans (called before the lazy loaders fire off)
        assert attributes.has_parent(Blog, p1, "posts", optimistic=True)
        assert attributes.has_parent(Post, b, "blog", optimistic=True)

        # assert connections
        assert p1.blog is b
        assert p1 in b.posts

        # manual connections
        b2 = Blog()
        p2 = Post()
        b2.posts.append(p2)
        assert attributes.has_parent(Blog, p2, "posts")
        assert attributes.has_parent(Post, b2, "blog")

    def test_illegal_trackparent(self):
        class Post:
            pass

        class Blog:
            pass

        instrumentation.register_class(Post)
        instrumentation.register_class(Blog)

        _register_attribute(Post, "blog", useobject=True)
        assert_raises_message(
            AssertionError,
            "This AttributeImpl is not configured to track parents.",
            attributes.has_parent,
            Post,
            Blog(),
            "blog",
        )
        assert_raises_message(
            AssertionError,
            "This AttributeImpl is not configured to track parents.",
            Post.blog.impl.sethasparent,
            "x",
            "x",
            True,
        )

    def test_inheritance(self):
        """tests that attributes are polymorphic"""

        class Foo:
            pass

        class Bar(Foo):
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)

        def func1(state, passive):
            return "this is the foo attr"

        def func2(state, passive):
            return "this is the bar attr"

        def func3(state, passive):
            return "this is the shared attr"

        _register_attribute(
            Foo, "element", uselist=False, callable_=func1, useobject=True
        )
        _register_attribute(
            Foo, "element2", uselist=False, callable_=func3, useobject=True
        )
        _register_attribute(
            Bar, "element", uselist=False, callable_=func2, useobject=True
        )

        x = Foo()
        y = Bar()
        assert x.element == "this is the foo attr"
        assert y.element == "this is the bar attr"
        assert x.element2 == "this is the shared attr"
        assert y.element2 == "this is the shared attr"

    def test_no_double_state(self):
        states = set()

        class Foo:
            def __init__(self):
                states.add(attributes.instance_state(self))

        class Bar(Foo):
            def __init__(self):
                states.add(attributes.instance_state(self))
                Foo.__init__(self)

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)

        b = Bar()
        eq_(len(states), 1)
        eq_(list(states)[0].obj(), b)

    def test_inheritance2(self):
        """test that the attribute manager can properly traverse the
        managed attributes of an object, if the object is of a
        descendant class with managed attributes in the parent class"""

        class Foo:
            pass

        class Bar(Foo):
            pass

        class Element:
            _state = True

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "element", uselist=False, useobject=True)
        el = Element()
        x = Bar()
        x.element = el
        eq_(
            attributes.get_state_history(
                attributes.instance_state(x), "element"
            ),
            ([el], (), ()),
        )
        attributes.instance_state(x)._commit_all(attributes.instance_dict(x))
        added, unchanged, deleted = attributes.get_state_history(
            attributes.instance_state(x), "element"
        )
        assert added == ()
        assert unchanged == [el]

    def test_lazyhistory(self):
        """tests that history functions work with lazy-loading attributes"""

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]

        def func1(state, passive):
            return "this is func 1"

        def func2(state, passive):
            return [bar1, bar2, bar3]

        _register_attribute(
            Foo, "col1", uselist=False, callable_=func1, useobject=True
        )
        _register_attribute(
            Foo, "col2", uselist=True, callable_=func2, useobject=True
        )
        _register_attribute(Bar, "id", uselist=False, useobject=True)
        x = Foo()
        attributes.instance_state(x)._commit_all(attributes.instance_dict(x))
        x.col2.append(bar4)
        eq_(
            attributes.get_state_history(attributes.instance_state(x), "col2"),
            ([bar4], [bar1, bar2, bar3], []),
        )

    def test_parenttrack(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo, "element", uselist=False, trackparent=True, useobject=True
        )
        _register_attribute(
            Bar, "element", uselist=False, trackparent=True, useobject=True
        )
        f1 = Foo()
        f2 = Foo()
        b1 = Bar()
        b2 = Bar()
        f1.element = b1
        b2.element = f2
        assert attributes.has_parent(Foo, b1, "element")
        assert not attributes.has_parent(Foo, b2, "element")
        assert not attributes.has_parent(Foo, f2, "element")
        assert attributes.has_parent(Bar, f2, "element")
        b2.element = None
        assert not attributes.has_parent(Bar, f2, "element")

        # test that double assignment doesn't accidentally reset the
        # 'parent' flag.

        b3 = Bar()
        f4 = Foo()
        b3.element = f4
        assert attributes.has_parent(Bar, f4, "element")
        b3.element = f4
        assert attributes.has_parent(Bar, f4, "element")

    def test_descriptorattributes(self):
        """changeset: 1633 broke ability to use ORM to map classes with
        unusual descriptor attributes (for example, classes that inherit
        from ones implementing zope.interface.Interface). This is a
        simple regression test to prevent that defect."""

        class des:
            def __get__(self, instance, owner):
                raise AttributeError("fake attribute")

        class Foo:
            A = des()

        instrumentation.register_class(Foo)
        instrumentation.unregister_class(Foo)

    def test_collectionclasses(self):
        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(
            Foo, "collection", uselist=True, typecallable=set, useobject=True
        )
        assert attributes.manager_of_class(Foo).is_instrumented("collection")
        assert isinstance(Foo().collection, set)
        attributes._unregister_attribute(Foo, "collection")
        assert not attributes.manager_of_class(Foo).is_instrumented(
            "collection"
        )
        try:
            _register_attribute(
                Foo,
                "collection",
                uselist=True,
                typecallable=dict,
                useobject=True,
            )
            assert False
        except sa_exc.ArgumentError as e:
            assert (
                str(e) == "Type InstrumentedDict must elect an appender "
                "method to be a collection class"
            )

        class MyDict(dict):
            @collection.appender
            def append(self, item):
                self[item.foo] = item

            @collection.remover
            def remove(self, item):
                del self[item.foo]

        _register_attribute(
            Foo,
            "collection",
            uselist=True,
            typecallable=MyDict,
            useobject=True,
        )
        assert isinstance(Foo().collection, MyDict)
        attributes._unregister_attribute(Foo, "collection")

        class MyColl:
            pass

        try:
            _register_attribute(
                Foo,
                "collection",
                uselist=True,
                typecallable=MyColl,
                useobject=True,
            )
            assert False
        except sa_exc.ArgumentError as e:
            assert (
                str(e) == "Type MyColl must elect an appender method to be a "
                "collection class"
            )

        class MyColl:
            @collection.iterator
            def __iter__(self):
                return iter([])

            @collection.appender
            def append(self, item):
                pass

            @collection.remover
            def remove(self, item):
                pass

        _register_attribute(
            Foo,
            "collection",
            uselist=True,
            typecallable=MyColl,
            useobject=True,
        )
        try:
            Foo().collection
            assert True
        except sa_exc.ArgumentError:
            assert False

    def test_last_known_tracking(self):
        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "a", useobject=False)
        _register_attribute(Foo, "b", useobject=False)
        _register_attribute(Foo, "c", useobject=False)

        f1 = Foo()
        state = attributes.instance_state(f1)

        f1.a = "a1"
        f1.b = "b1"
        f1.c = "c1"

        assert not state._last_known_values

        state._track_last_known_value("b")
        state._track_last_known_value("c")

        eq_(
            state._last_known_values,
            {"b": attributes.NO_VALUE, "c": attributes.NO_VALUE},
        )

        state._expire_attributes(state.dict, ["b"])
        eq_(state._last_known_values, {"b": "b1", "c": attributes.NO_VALUE})

        state._expire(state.dict, set())
        eq_(state._last_known_values, {"b": "b1", "c": "c1"})

        f1.b = "b2"

        eq_(state._last_known_values, {"b": attributes.NO_VALUE, "c": "c1"})

        f1.c = "c2"

        eq_(
            state._last_known_values,
            {"b": attributes.NO_VALUE, "c": attributes.NO_VALUE},
        )

        state._expire(state.dict, set())
        eq_(state._last_known_values, {"b": "b2", "c": "c2"})


class GetNoValueTest(fixtures.ORMTest):
    def _fixture(self, expected):
        class Foo:
            pass

        class Bar:
            pass

        def lazy_callable(state, passive):
            return expected

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        if expected is not None:
            _register_attribute(
                Foo,
                "attr",
                useobject=True,
                uselist=False,
                callable_=lazy_callable,
            )
        else:
            _register_attribute(Foo, "attr", useobject=True, uselist=False)

        f1 = self.f1 = Foo()
        return (
            Foo.attr.impl,
            attributes.instance_state(f1),
            attributes.instance_dict(f1),
        )

    def test_passive_no_result(self):
        attr, state, dict_ = self._fixture(attributes.PASSIVE_NO_RESULT)
        eq_(
            attr.get(state, dict_, passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.PASSIVE_NO_RESULT,
        )

    def test_passive_no_result_no_value(self):
        attr, state, dict_ = self._fixture(attributes.NO_VALUE)
        eq_(
            attr.get(state, dict_, passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.PASSIVE_NO_RESULT,
        )
        assert "attr" not in dict_

    def test_passive_ret_no_value(self):
        attr, state, dict_ = self._fixture(attributes.NO_VALUE)
        eq_(
            attr.get(state, dict_, passive=attributes.PASSIVE_RETURN_NO_VALUE),
            attributes.NO_VALUE,
        )
        assert "attr" not in dict_

    def test_passive_ret_no_value_empty(self):
        attr, state, dict_ = self._fixture(None)
        eq_(
            attr.get(state, dict_, passive=attributes.PASSIVE_RETURN_NO_VALUE),
            attributes.NO_VALUE,
        )
        assert "attr" not in dict_

    def test_off_empty(self):
        attr, state, dict_ = self._fixture(None)
        eq_(attr.get(state, dict_, passive=attributes.PASSIVE_OFF), None)
        assert "attr" not in dict_


class UtilTest(fixtures.ORMTest):
    def test_helpers(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "coll", uselist=True, useobject=True)

        f1 = Foo()
        b1 = Bar()
        b2 = Bar()
        coll = attributes.init_collection(f1, "coll")
        assert coll.data is f1.coll
        assert attributes.get_attribute(f1, "coll") is f1.coll
        attributes.set_attribute(f1, "coll", [b1])
        assert f1.coll == [b1]
        eq_(attributes.get_history(f1, "coll"), ([b1], [], []))
        attributes.set_committed_value(f1, "coll", [b2])
        eq_(attributes.get_history(f1, "coll"), ((), [b2], ()))

        attributes.del_attribute(f1, "coll")
        assert "coll" not in f1.__dict__

    def test_set_committed_value_none_uselist(self):
        """test that set_committed_value->None to a uselist generates an
        empty list"""

        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "col_list", uselist=True, useobject=True)
        _register_attribute(
            Foo, "col_set", uselist=True, useobject=True, typecallable=set
        )

        f1 = Foo()
        attributes.set_committed_value(f1, "col_list", None)
        eq_(f1.col_list, [])

        attributes.set_committed_value(f1, "col_set", None)
        eq_(f1.col_set, set())

    def test_initiator_arg(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "a", uselist=False, useobject=False)
        _register_attribute(Bar, "b", uselist=False, useobject=False)

        @event.listens_for(Foo.a, "set")
        def sync_a(target, value, oldvalue, initiator):
            parentclass = initiator.parent_token.class_
            if parentclass is Foo:
                attributes.set_attribute(target.bar, "b", value, initiator)

        @event.listens_for(Bar.b, "set")
        def sync_b(target, value, oldvalue, initiator):
            parentclass = initiator.parent_token.class_
            if parentclass is Bar:
                attributes.set_attribute(target.foo, "a", value, initiator)

        f1 = Foo()
        b1 = Bar()
        f1.bar = b1
        b1.foo = f1

        f1.a = "x"
        eq_(b1.b, "x")
        b1.b = "y"
        eq_(f1.a, "y")


class BackrefTest(fixtures.ORMTest):
    def test_m2m(self):
        class Student:
            pass

        class Course:
            pass

        instrumentation.register_class(Student)
        instrumentation.register_class(Course)
        _register_attribute(
            Student,
            "courses",
            uselist=True,
            backref="students",
            useobject=True,
        )
        _register_attribute(
            Course, "students", uselist=True, backref="courses", useobject=True
        )

        s = Student()
        c = Course()
        s.courses.append(c)
        self.assert_(c.students == [s])
        s.courses.remove(c)
        self.assert_(c.students == [])

        (s1, s2, s3) = (Student(), Student(), Student())

        c.students = [s1, s2, s3]
        self.assert_(s2.courses == [c])
        self.assert_(s1.courses == [c])
        s1.courses.remove(c)
        self.assert_(c.students == [s2, s3])

    def test_o2m(self):
        class Post:
            pass

        class Blog:
            pass

        instrumentation.register_class(Post)
        instrumentation.register_class(Blog)
        _register_attribute(
            Post,
            "blog",
            uselist=False,
            backref="posts",
            trackparent=True,
            useobject=True,
        )
        _register_attribute(
            Blog,
            "posts",
            uselist=True,
            backref="blog",
            trackparent=True,
            useobject=True,
        )
        b = Blog()
        (p1, p2, p3) = (Post(), Post(), Post())
        b.posts.append(p1)
        b.posts.append(p2)
        b.posts.append(p3)
        self.assert_(b.posts == [p1, p2, p3])
        self.assert_(p2.blog is b)

        p3.blog = None
        self.assert_(b.posts == [p1, p2])
        p4 = Post()
        p4.blog = b
        self.assert_(b.posts == [p1, p2, p4])

        p4.blog = b
        p4.blog = b
        self.assert_(b.posts == [p1, p2, p4])

        # assert no failure removing None
        p5 = Post()
        p5.blog = None
        del p5.blog

    def test_o2o(self):
        class Port:
            pass

        class Jack:
            pass

        instrumentation.register_class(Port)
        instrumentation.register_class(Jack)

        _register_attribute(
            Port, "jack", uselist=False, useobject=True, backref="port"
        )

        _register_attribute(
            Jack, "port", uselist=False, useobject=True, backref="jack"
        )

        p = Port()
        j = Jack()
        p.jack = j
        self.assert_(j.port is p)
        self.assert_(p.jack is not None)

        j.port = None
        self.assert_(p.jack is None)

    def test_symmetric_o2o_inheritance(self):
        """Test that backref 'initiator' catching goes against
        a token that is global to all InstrumentedAttribute objects
        within a particular class, not just the individual IA object
        since we use distinct objects in an inheritance scenario.

        """

        class Parent:
            pass

        class Child:
            pass

        class SubChild(Child):
            pass

        p_token = object()
        c_token = object()

        instrumentation.register_class(Parent)
        instrumentation.register_class(Child)
        instrumentation.register_class(SubChild)
        _register_attribute(
            Parent,
            "child",
            uselist=False,
            backref="parent",
            parent_token=p_token,
            useobject=True,
        )
        _register_attribute(
            Child,
            "parent",
            uselist=False,
            backref="child",
            parent_token=c_token,
            useobject=True,
        )
        _register_attribute(
            SubChild,
            "parent",
            uselist=False,
            backref="child",
            parent_token=c_token,
            useobject=True,
        )

        p1 = Parent()
        c1 = Child()
        p1.child = c1

        c2 = SubChild()
        c2.parent = p1

    def test_symmetric_o2m_inheritance(self):
        class Parent:
            pass

        class SubParent(Parent):
            pass

        class Child:
            pass

        p_token = object()
        c_token = object()

        instrumentation.register_class(Parent)
        instrumentation.register_class(SubParent)
        instrumentation.register_class(Child)
        _register_attribute(
            Parent,
            "children",
            uselist=True,
            backref="parent",
            parent_token=p_token,
            useobject=True,
        )
        _register_attribute(
            SubParent,
            "children",
            uselist=True,
            backref="parent",
            parent_token=p_token,
            useobject=True,
        )
        _register_attribute(
            Child,
            "parent",
            uselist=False,
            backref="children",
            parent_token=c_token,
            useobject=True,
        )

        p1 = Parent()
        p2 = SubParent()
        c1 = Child()

        p1.children.append(c1)

        assert c1.parent is p1
        assert c1 in p1.children

        p2.children.append(c1)
        assert c1.parent is p2

        # event propagates to remove as of [ticket:2789]
        assert c1 not in p1.children


class CyclicBackrefAssertionTest(fixtures.TestBase):
    """test that infinite recursion due to incorrect backref assignments
    is blocked.

    """

    def test_scalar_set_type_assertion(self):
        A, B, C = self._scalar_fixture()
        c1 = C()
        b1 = B()
        assert_raises_message(
            ValueError,
            "Bidirectional attribute conflict detected: "
            'Passing object <B at .*> to attribute "C.a" '
            'triggers a modify event on attribute "C.b" '
            'via the backref "B.c".',
            setattr,
            c1,
            "a",
            b1,
        )

    def test_collection_append_type_assertion(self):
        A, B, C = self._collection_fixture()
        c1 = C()
        b1 = B()
        assert_raises_message(
            ValueError,
            "Bidirectional attribute conflict detected: "
            'Passing object <B at .*> to attribute "C.a" '
            'triggers a modify event on attribute "C.b" '
            'via the backref "B.c".',
            c1.a.append,
            b1,
        )

    def _scalar_fixture(self):
        class A:
            pass

        class B:
            pass

        class C:
            pass

        instrumentation.register_class(A)
        instrumentation.register_class(B)
        instrumentation.register_class(C)
        _register_attribute(C, "a", backref="c", useobject=True)
        _register_attribute(C, "b", backref="c", useobject=True)

        _register_attribute(A, "c", backref="a", useobject=True, uselist=True)
        _register_attribute(B, "c", backref="b", useobject=True, uselist=True)

        return A, B, C

    def _collection_fixture(self):
        class A:
            pass

        class B:
            pass

        class C:
            pass

        instrumentation.register_class(A)
        instrumentation.register_class(B)
        instrumentation.register_class(C)

        _register_attribute(C, "a", backref="c", useobject=True, uselist=True)
        _register_attribute(C, "b", backref="c", useobject=True, uselist=True)

        _register_attribute(A, "c", backref="a", useobject=True)
        _register_attribute(B, "c", backref="b", useobject=True)

        return A, B, C

    def _broken_collection_fixture(self):
        class A:
            pass

        class B:
            pass

        instrumentation.register_class(A)
        instrumentation.register_class(B)

        _register_attribute(A, "b", backref="a1", useobject=True)
        _register_attribute(B, "a1", backref="b", useobject=True, uselist=True)

        _register_attribute(B, "a2", backref="b", useobject=True, uselist=True)

        return A, B

    def test_broken_collection_assertion(self):
        A, B = self._broken_collection_fixture()
        b1 = B()
        a1 = A()
        assert_raises_message(
            ValueError,
            "Bidirectional attribute conflict detected: "
            'Passing object <A at .*> to attribute "B.a2" '
            'triggers a modify event on attribute "B.a1" '
            'via the backref "A.b".',
            b1.a2.append,
            a1,
        )


class PendingBackrefTest(fixtures.ORMTest):
    def _fixture(self):
        class Post:
            def __init__(self, name):
                self.name = name

            __hash__ = None

            def __eq__(self, other):
                return other is not None and other.name == self.name

        class Blog:
            def __init__(self, name):
                self.name = name

            __hash__ = None

            def __eq__(self, other):
                return other is not None and other.name == self.name

        lazy_posts = Mock()

        instrumentation.register_class(Post)
        instrumentation.register_class(Blog)
        _register_attribute(
            Post,
            "blog",
            uselist=False,
            backref="posts",
            trackparent=True,
            useobject=True,
        )
        _register_attribute(
            Blog,
            "posts",
            uselist=True,
            backref="blog",
            callable_=lazy_posts,
            trackparent=True,
            useobject=True,
        )

        return Post, Blog, lazy_posts

    def test_lazy_add(self):
        Post, Blog, lazy_posts = self._fixture()

        p1, p2, p3 = Post("post 1"), Post("post 2"), Post("post 3")
        lazy_posts.return_value = attributes.PASSIVE_NO_RESULT

        b = Blog("blog 1")
        b1_state = attributes.instance_state(b)

        p = Post("post 4")

        p.blog = b
        eq_(
            lazy_posts.mock_calls,
            [call(b1_state, attributes.PASSIVE_NO_FETCH)],
        )

        p = Post("post 5")

        # setting blog doesn't call 'posts' callable, calls with no fetch
        p.blog = b
        eq_(
            lazy_posts.mock_calls,
            [
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_NO_FETCH),
            ],
        )

        lazy_posts.return_value = [p1, p2, p3]

        # calling backref calls the callable, populates extra posts
        eq_(b.posts, [p1, p2, p3, Post("post 4"), Post("post 5")])
        eq_(
            lazy_posts.mock_calls,
            [
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_OFF),
            ],
        )

    def test_lazy_history_collection(self):
        Post, Blog, lazy_posts = self._fixture()

        p1, p2, p3 = Post("post 1"), Post("post 2"), Post("post 3")
        lazy_posts.return_value = [p1, p2, p3]

        b = Blog("blog 1")
        p = Post("post 4")
        p.blog = b

        p4 = Post("post 5")
        p4.blog = b

        eq_(lazy_posts.call_count, 1)

        eq_(
            attributes.instance_state(b).get_history(
                "posts", attributes.PASSIVE_OFF
            ),
            ([p, p4], [p1, p2, p3], []),
        )
        eq_(lazy_posts.call_count, 1)

    def test_passive_history_collection_no_value(self):
        Post, Blog, lazy_posts = self._fixture()

        lazy_posts.return_value = attributes.PASSIVE_NO_RESULT

        b = Blog("blog 1")
        p = Post("post 1")

        state, dict_ = (
            attributes.instance_state(b),
            attributes.instance_dict(b),
        )

        # this sets up NO_VALUE on b.posts
        p.blog = b

        eq_(state.committed_state, {"posts": attributes.NO_VALUE})
        assert "posts" not in dict_

        # then suppose the object was made transient again,
        # the lazy loader would return this
        lazy_posts.return_value = attributes.ATTR_EMPTY

        p2 = Post("asdf")
        p2.blog = b

        eq_(state.committed_state, {"posts": attributes.NO_VALUE})
        eq_(dict_["posts"], [p2])

        # then this would fail.
        eq_(
            Blog.posts.impl.get_history(
                state, dict_, passive=attributes.PASSIVE_NO_INITIALIZE
            ),
            ([p2], (), ()),
        )

        eq_(
            Blog.posts.impl.get_all_pending(state, dict_),
            [(attributes.instance_state(p2), p2)],
        )

    def test_state_on_add_remove(self):
        Post, Blog, lazy_posts = self._fixture()
        lazy_posts.return_value = attributes.PASSIVE_NO_RESULT

        b = Blog("blog 1")
        b1_state = attributes.instance_state(b)
        p = Post("post 1")
        p.blog = b
        eq_(
            lazy_posts.mock_calls,
            [call(b1_state, attributes.PASSIVE_NO_FETCH)],
        )
        p.blog = None
        eq_(
            lazy_posts.mock_calls,
            [
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_NO_FETCH),
            ],
        )
        lazy_posts.return_value = []
        eq_(b.posts, [])
        eq_(
            lazy_posts.mock_calls,
            [
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_NO_FETCH),
                call(b1_state, attributes.PASSIVE_OFF),
            ],
        )

    def test_pending_combines_with_lazy(self):
        Post, Blog, lazy_posts = self._fixture()

        lazy_posts.return_value = attributes.PASSIVE_NO_RESULT

        b = Blog("blog 1")
        p = Post("post 1")
        p2 = Post("post 2")
        p.blog = b
        eq_(lazy_posts.call_count, 1)

        lazy_posts.return_value = [p, p2]

        # lazy loaded + pending get added together.
        # This isn't seen often with the ORM due
        # to usual practices surrounding the
        # load/flush/load cycle.
        eq_(b.posts, [p, p2, p])
        eq_(lazy_posts.call_count, 2)

    def test_normal_load(self):
        Post, Blog, lazy_posts = self._fixture()

        lazy_posts.return_value = (p1, p2, p3) = [
            Post("post 1"),
            Post("post 2"),
            Post("post 3"),
        ]

        b = Blog("blog 1")

        # assign without using backref system
        p2.__dict__["blog"] = b

        eq_(b.posts, [Post("post 1"), Post("post 2"), Post("post 3")])

        eq_(lazy_posts.call_count, 1)
        p2.blog = None
        p4 = Post("post 4")
        p4.blog = b
        eq_(b.posts, [Post("post 1"), Post("post 3"), Post("post 4")])

        b_state = attributes.instance_state(b)

        eq_(lazy_posts.call_count, 1)
        eq_(lazy_posts.mock_calls, [call(b_state, attributes.PASSIVE_OFF)])

    def test_commit_removes_pending(self):
        Post, Blog, lazy_posts = self._fixture()

        p1 = Post("post 1")

        lazy_posts.return_value = attributes.PASSIVE_NO_RESULT
        b = Blog("blog 1")
        p1.blog = b

        b_state = attributes.instance_state(b)
        p1_state = attributes.instance_state(p1)
        b_state._commit_all(attributes.instance_dict(b))
        p1_state._commit_all(attributes.instance_dict(p1))
        lazy_posts.return_value = [p1]
        eq_(b.posts, [Post("post 1")])
        eq_(
            lazy_posts.mock_calls,
            [
                call(b_state, attributes.PASSIVE_NO_FETCH),
                call(b_state, attributes.PASSIVE_OFF),
            ],
        )


class HistoryTest(fixtures.TestBase):
    def _fixture(self, uselist, useobject, active_history, **kw):
        class Foo(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        _register_attribute(
            Foo,
            "someattr",
            uselist=uselist,
            useobject=useobject,
            active_history=active_history,
            **kw,
        )
        return Foo

    def _two_obj_fixture(self, uselist, active_history=False):
        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            def __bool__(self):
                assert False

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "someattr",
            uselist=uselist,
            useobject=True,
            active_history=active_history,
        )
        return Foo, Bar

    def _someattr_history(self, f, **kw):
        passive = kw.pop("passive", None)
        if passive is True:
            kw["passive"] = attributes.PASSIVE_NO_INITIALIZE
        elif passive is False:
            kw["passive"] = attributes.PASSIVE_OFF

        return attributes.get_state_history(
            attributes.instance_state(f), "someattr", **kw
        )

    def _commit_someattr(self, f):
        attributes.instance_state(f)._commit(
            attributes.instance_dict(f), ["someattr"]
        )

    def _someattr_committed_state(self, f):
        Foo = f.__class__
        return Foo.someattr.impl.get_committed_value(
            attributes.instance_state(f), attributes.instance_dict(f)
        )

    def test_committed_value_init(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        eq_(self._someattr_committed_state(f), None)

    def test_committed_value_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = 3
        eq_(self._someattr_committed_state(f), None)

    def test_committed_value_set_active_hist(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = 3
        eq_(self._someattr_committed_state(f), None)

    def test_committed_value_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = 3
        self._commit_someattr(f)
        eq_(self._someattr_committed_state(f), 3)

    def test_scalar_init(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        eq_(self._someattr_history(f), ((), (), ()))

    def test_object_init(self):
        Foo = self._fixture(
            uselist=False, useobject=True, active_history=False
        )
        f = Foo()
        eq_(self._someattr_history(f), ((), (), ()))

    def test_object_init_active_history(self):
        Foo = self._fixture(uselist=False, useobject=True, active_history=True)
        f = Foo()
        eq_(self._someattr_history(f), ((), (), ()))

    def test_object_replace(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        b1, b2 = Bar(), Bar()
        f.someattr = b1
        self._commit_someattr(f)

        f.someattr = b2
        eq_(self._someattr_history(f), ([b2], (), [b1]))

    def test_object_set_none(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        b1 = Bar()
        f.someattr = b1
        self._commit_someattr(f)

        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), [b1]))

    def test_object_set_none_expired(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        b1 = Bar()
        f.someattr = b1
        self._commit_someattr(f)

        attributes.instance_state(f).dict.pop("someattr", None)
        attributes.instance_state(f).expired_attributes.add("someattr")

        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_object_del(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        b1 = Bar()
        f.someattr = b1

        self._commit_someattr(f)

        del f.someattr
        eq_(self._someattr_history(f), ((), (), [b1]))

    def test_object_del_expired(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        b1 = Bar()
        f.someattr = b1
        self._commit_someattr(f)

        # the "delete" handler checks if the object
        # is db-loaded when testing if an empty "del" is valid,
        # because there's nothing else to look at for a related
        # object, there's no "expired" status
        attributes.instance_state(f).key = ("foo",)
        attributes.instance_state(f)._expire_attributes(
            attributes.instance_dict(f), ["someattr"]
        )

        del f.someattr
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_scalar_no_init_side_effect(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        self._someattr_history(f)
        # no side effects
        assert "someattr" not in f.__dict__
        assert "someattr" not in attributes.instance_state(f).committed_state

    def test_scalar_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "hi"
        eq_(self._someattr_history(f), (["hi"], (), ()))

    def test_scalar_set_None(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_scalar_del(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = 5
        attributes.instance_state(f).key = ("foo",)
        self._commit_someattr(f)

        del f.someattr
        eq_(self._someattr_history(f), ((), (), [5]))

    def test_scalar_del_expired(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = 5
        self._commit_someattr(f)

        attributes.instance_state(f)._expire_attributes(
            attributes.instance_dict(f), ["someattr"]
        )
        del f.someattr
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_scalar_get_first_set_None(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        assert f.someattr is None
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_scalar_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["hi"], ()))

    def test_scalar_set_commit_reset(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        f.someattr = "there"
        eq_(self._someattr_history(f), (["there"], (), ["hi"]))

    def test_scalar_set_commit_reset_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        f.someattr = "there"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["there"], ()))

    def test_scalar_set_commit_reset_commit_del(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "there"
        self._commit_someattr(f)
        del f.someattr
        eq_(self._someattr_history(f), ((), (), ["there"]))

    def test_scalar_set_dict(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        eq_(self._someattr_history(f), ((), ["new"], ()))

    def test_scalar_set_dict_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        self._someattr_history(f)
        f.someattr = "old"
        eq_(self._someattr_history(f), (["old"], (), ["new"]))

    def test_scalar_set_dict_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        self._someattr_history(f)
        f.someattr = "old"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["old"], ()))

    def test_scalar_set_None_from_dict_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ["new"]))

    def test_scalar_set_twice_no_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "one"
        eq_(self._someattr_history(f), (["one"], (), ()))
        f.someattr = "two"
        eq_(self._someattr_history(f), (["two"], (), ()))

    def test_scalar_active_init(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        eq_(self._someattr_history(f), ((), (), ()))

    def test_scalar_active_no_init_side_effect(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        self._someattr_history(f)
        # no side effects
        assert "someattr" not in f.__dict__
        assert "someattr" not in attributes.instance_state(f).committed_state

    def test_collection_no_value(self):
        Foo = self._fixture(uselist=True, useobject=True, active_history=True)
        f = Foo()
        eq_(self._someattr_history(f, passive=True), ((), (), ()))

    def test_scalar_obj_no_value(self):
        Foo = self._fixture(uselist=False, useobject=True, active_history=True)
        f = Foo()
        eq_(self._someattr_history(f, passive=True), ((), (), ()))

    def test_scalar_no_value(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        eq_(self._someattr_history(f, passive=True), ((), (), ()))

    def test_scalar_active_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "hi"
        eq_(self._someattr_history(f), (["hi"], (), ()))

    def test_scalar_active_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["hi"], ()))

    def test_scalar_active_set_commit_reset(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        f.someattr = "there"
        eq_(self._someattr_history(f), (["there"], (), ["hi"]))

    def test_scalar_active_set_commit_reset_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "hi"
        self._commit_someattr(f)
        f.someattr = "there"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["there"], ()))

    def test_scalar_active_set_commit_reset_commit_del(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "there"
        self._commit_someattr(f)
        del f.someattr
        eq_(self._someattr_history(f), ((), (), ["there"]))

    def test_scalar_active_set_dict(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        eq_(self._someattr_history(f), ((), ["new"], ()))

    def test_scalar_active_set_dict_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        self._someattr_history(f)
        f.someattr = "old"
        eq_(self._someattr_history(f), (["old"], (), ["new"]))

    def test_scalar_active_set_dict_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        self._someattr_history(f)
        f.someattr = "old"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["old"], ()))

    def test_scalar_active_set_None(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_scalar_active_set_None_from_dict_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.__dict__["someattr"] = "new"
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ["new"]))

    def test_scalar_active_set_twice_no_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "one"
        eq_(self._someattr_history(f), (["one"], (), ()))
        f.someattr = "two"
        eq_(self._someattr_history(f), (["two"], (), ()))

    def test_scalar_passive_flag(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=True
        )
        f = Foo()
        f.someattr = "one"
        eq_(self._someattr_history(f), (["one"], (), ()))

        self._commit_someattr(f)

        state = attributes.instance_state(f)
        # do the same thing that
        # populators.expire.append((self.key, True))
        # does in loading.py
        state.dict.pop("someattr", None)
        state.expired_attributes.add("someattr")

        def scalar_loader(state, toload, passive):
            state.dict["someattr"] = "one"

        state.manager.expired_attribute_loader = scalar_loader

        eq_(self._someattr_history(f), ((), ["one"], ()))

    def test_scalar_inplace_mutation_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        eq_(self._someattr_history(f), ([{"a": "b"}], (), ()))

    def test_scalar_inplace_mutation_set_commit(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), [{"a": "b"}], ()))

    def test_scalar_inplace_mutation_set_commit_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        f.someattr["a"] = "c"
        eq_(self._someattr_history(f), ((), [{"a": "c"}], ()))

    def test_scalar_inplace_mutation_set_commit_flag_modified(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        attributes.flag_modified(f, "someattr")
        eq_(self._someattr_history(f), ([{"a": "b"}], (), ()))

    def test_scalar_inplace_mutation_set_commit_set_flag_modified(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        f.someattr["a"] = "c"
        attributes.flag_modified(f, "someattr")
        eq_(self._someattr_history(f), ([{"a": "c"}], (), ()))

    def test_scalar_inplace_mutation_set_commit_flag_modified_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        attributes.flag_modified(f, "someattr")
        eq_(self._someattr_history(f), ([{"a": "b"}], (), ()))
        f.someattr = ["a"]
        eq_(self._someattr_history(f), ([["a"]], (), ()))

    def test_scalar_inplace_mutation_replace_self_flag_modified_set(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = {"a": "b"}
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), [{"a": "b"}], ()))

        # set the attribute to itself; this places a copy
        # in committed_state
        f.someattr = f.someattr

        attributes.flag_modified(f, "someattr")
        eq_(self._someattr_history(f), ([{"a": "b"}], (), ()))

    def test_flag_modified_but_no_value_raises(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "foo"
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), ["foo"], ()))

        attributes.instance_state(f)._expire_attributes(
            attributes.instance_dict(f), ["someattr"]
        )

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Can't flag attribute 'someattr' modified; it's "
            "not present in the object state",
            attributes.flag_modified,
            f,
            "someattr",
        )

    def test_mark_dirty_no_attr(self):
        Foo = self._fixture(
            uselist=False, useobject=False, active_history=False
        )
        f = Foo()
        f.someattr = "foo"
        attributes.instance_state(f)._commit_all(f.__dict__)
        eq_(self._someattr_history(f), ((), ["foo"], ()))

        attributes.instance_state(f)._expire_attributes(
            attributes.instance_dict(f), ["someattr"]
        )

        is_false(attributes.instance_state(f).modified)

        attributes.flag_dirty(f)

        is_true(attributes.instance_state(f).modified)

    def test_use_object_init(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        eq_(self._someattr_history(f), ((), (), ()))

    def test_use_object_no_init_side_effect(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        self._someattr_history(f)
        assert "someattr" not in f.__dict__
        assert "someattr" not in attributes.instance_state(f).committed_state

    def test_use_object_set(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.someattr = hi
        eq_(self._someattr_history(f), ([hi], (), ()))

    def test_use_object_set_commit(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.someattr = hi
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), [hi], ()))

    def test_use_object_set_commit_set(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.someattr = hi
        self._commit_someattr(f)
        there = Bar(name="there")
        f.someattr = there
        eq_(self._someattr_history(f), ([there], (), [hi]))

    def test_use_object_set_commit_set_commit(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.someattr = hi
        self._commit_someattr(f)
        there = Bar(name="there")
        f.someattr = there
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), [there], ()))

    def test_use_object_set_commit_del(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.someattr = hi
        self._commit_someattr(f)
        del f.someattr
        eq_(self._someattr_history(f), ((), (), [hi]))

    def test_use_object_set_dict(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.__dict__["someattr"] = hi
        eq_(self._someattr_history(f), ((), [hi], ()))

    def test_use_object_set_dict_set(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.__dict__["someattr"] = hi

        there = Bar(name="there")
        f.someattr = there
        eq_(self._someattr_history(f), ([there], (), [hi]))

    def test_use_object_set_dict_set_commit(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.__dict__["someattr"] = hi

        there = Bar(name="there")
        f.someattr = there
        self._commit_someattr(f)
        eq_(self._someattr_history(f), ((), [there], ()))

    def test_use_object_set_None(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_use_object_get_first_set_None(self):
        # note - compare:
        # test_scalar_set_None,
        # test_scalar_get_first_set_None,
        # test_use_object_set_None,
        # test_use_object_get_first_set_None
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        assert f.someattr is None
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), ()))

    def test_use_object_set_dict_set_None(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        f.__dict__["someattr"] = hi
        f.someattr = None
        eq_(self._someattr_history(f), ([None], (), [hi]))

    def test_use_object_set_value_twice(self):
        Foo, Bar = self._two_obj_fixture(uselist=False)
        f = Foo()
        hi = Bar(name="hi")
        there = Bar(name="there")
        f.someattr = hi
        f.someattr = there
        eq_(self._someattr_history(f), ([there], (), ()))

    def test_object_collections_set(self):
        # TODO: break into individual tests

        Foo, Bar = self._two_obj_fixture(uselist=True)
        hi = Bar(name="hi")
        there = Bar(name="there")
        old = Bar(name="old")
        new = Bar(name="new")

        # case 1.  new object

        f = Foo()
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [], ()),
        )
        f.someattr = [hi]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi], [], []),
        )
        self._commit_someattr(f)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [hi], ()),
        )
        f.someattr = [there]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([there], [], [hi]),
        )
        self._commit_someattr(f)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [there], ()),
        )
        f.someattr = [hi]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi], [], [there]),
        )
        f.someattr = [old, new]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([old, new], [], [there]),
        )

        # case 2.  object with direct settings (similar to a load
        # operation)

        f = Foo()
        collection = attributes.init_collection(f, "someattr")
        collection.append_without_event(new)
        attributes.instance_state(f)._commit_all(attributes.instance_dict(f))
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [new], ()),
        )
        f.someattr = [old]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([old], [], [new]),
        )
        self._commit_someattr(f)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [old], ()),
        )

    def test_dict_collections(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "someattr",
            uselist=True,
            useobject=True,
            typecallable=attribute_keyed_dict("name"),
        )
        hi = Bar(name="hi")
        there = Bar(name="there")
        f = Foo()
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [], ()),
        )
        f.someattr["hi"] = hi
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi], [], []),
        )
        f.someattr["there"] = there
        eq_(
            tuple(
                [
                    set(x)
                    for x in attributes.get_state_history(
                        attributes.instance_state(f), "someattr"
                    )
                ]
            ),
            ({hi, there}, set(), set()),
        )
        self._commit_someattr(f)
        eq_(
            tuple(
                [
                    set(x)
                    for x in attributes.get_state_history(
                        attributes.instance_state(f), "someattr"
                    )
                ]
            ),
            (set(), {hi, there}, set()),
        )

    def test_object_collections_mutate(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "someattr", uselist=True, useobject=True)
        _register_attribute(Foo, "id", uselist=False, useobject=False)
        instrumentation.register_class(Bar)
        hi = Bar(name="hi")
        there = Bar(name="there")
        old = Bar(name="old")
        new = Bar(name="new")

        # case 1.  new object

        f = Foo(id=1)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [], ()),
        )
        f.someattr.append(hi)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi], [], []),
        )
        self._commit_someattr(f)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [hi], ()),
        )
        f.someattr.append(there)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([there], [hi], []),
        )
        self._commit_someattr(f)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [hi, there], ()),
        )
        f.someattr.remove(there)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([], [hi], [there]),
        )
        f.someattr.append(old)
        f.someattr.append(new)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([old, new], [hi], [there]),
        )
        attributes.instance_state(f)._commit(
            attributes.instance_dict(f), ["someattr"]
        )
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [hi, old, new], ()),
        )
        f.someattr.pop(0)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([], [old, new], [hi]),
        )

        # case 2.  object with direct settings (similar to a load
        # operation)

        f = Foo()
        f.__dict__["id"] = 1
        collection = attributes.init_collection(f, "someattr")
        collection.append_without_event(new)
        attributes.instance_state(f)._commit_all(attributes.instance_dict(f))
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [new], ()),
        )
        f.someattr.append(old)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([old], [new], []),
        )
        attributes.instance_state(f)._commit(
            attributes.instance_dict(f), ["someattr"]
        )
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [new, old], ()),
        )
        f = Foo()
        collection = attributes.init_collection(f, "someattr")
        collection.append_without_event(new)
        attributes.instance_state(f)._commit_all(attributes.instance_dict(f))
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [new], ()),
        )
        f.id = 1
        f.someattr.remove(new)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([], [], [new]),
        )

        # case 3.  mixing appends with sets

        f = Foo()
        f.someattr.append(hi)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi], [], []),
        )
        f.someattr.append(there)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi, there], [], []),
        )
        f.someattr = [there]
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([there], [], []),
        )

        # case 4.  ensure duplicates show up, order is maintained

        f = Foo()
        f.someattr.append(hi)
        f.someattr.append(there)
        f.someattr.append(hi)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([hi, there, hi], [], []),
        )
        attributes.instance_state(f)._commit_all(attributes.instance_dict(f))
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ((), [hi, there, hi], ()),
        )
        f.someattr = []
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f), "someattr"
            ),
            ([], [], [hi, there, hi]),
        )

    def test_collections_via_backref(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "bars",
            uselist=True,
            backref="foo",
            trackparent=True,
            useobject=True,
        )
        _register_attribute(
            Bar,
            "foo",
            uselist=False,
            backref="bars",
            trackparent=True,
            useobject=True,
        )
        f1 = Foo()
        b1 = Bar()
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f1), "bars"
            ),
            ((), [], ()),
        )
        eq_(
            attributes.get_state_history(attributes.instance_state(b1), "foo"),
            ((), (), ()),
        )

        # b1.foo = f1

        f1.bars.append(b1)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f1), "bars"
            ),
            ([b1], [], []),
        )
        eq_(
            attributes.get_state_history(attributes.instance_state(b1), "foo"),
            ([f1], (), ()),
        )
        b2 = Bar()
        f1.bars.append(b2)
        eq_(
            attributes.get_state_history(
                attributes.instance_state(f1), "bars"
            ),
            ([b1, b2], [], []),
        )
        eq_(
            attributes.get_state_history(attributes.instance_state(b1), "foo"),
            ([f1], (), ()),
        )
        eq_(
            attributes.get_state_history(attributes.instance_state(b2), "foo"),
            ([f1], (), ()),
        )


class LazyloadHistoryTest(fixtures.TestBase):
    def test_lazy_backref_collections(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        lazy_load = []

        def lazyload(state, passive):
            return lazy_load

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "bars",
            uselist=True,
            backref="foo",
            trackparent=True,
            callable_=lazyload,
            useobject=True,
        )
        _register_attribute(
            Bar,
            "foo",
            uselist=False,
            backref="bars",
            trackparent=True,
            useobject=True,
        )
        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]
        lazy_load = [bar1, bar2, bar3]
        f = Foo()
        bar4 = Bar()
        bar4.foo = f
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([bar4], [bar1, bar2, bar3], []),
        )
        lazy_load = None
        f = Foo()
        bar4 = Bar()
        bar4.foo = f
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([bar4], [], []),
        )
        lazy_load = [bar1, bar2, bar3]
        attributes.instance_state(f)._expire_attributes(
            attributes.instance_dict(f), ["bars"]
        )
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ((), [bar1, bar2, bar3], ()),
        )

    def test_collections_via_lazyload(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        lazy_load = []

        def lazyload(state, passive):
            return lazy_load

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "bars",
            uselist=True,
            callable_=lazyload,
            trackparent=True,
            useobject=True,
        )
        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]
        lazy_load = [bar1, bar2, bar3]
        f = Foo()
        f.bars = []
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([], [], [bar1, bar2, bar3]),
        )
        f = Foo()
        f.bars.append(bar4)
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([bar4], [bar1, bar2, bar3], []),
        )
        f = Foo()
        f.bars.remove(bar2)
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([], [bar1, bar3], [bar2]),
        )
        f.bars.append(bar4)
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([bar4], [bar1, bar3], [bar2]),
        )
        f = Foo()
        del f.bars[1]
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([], [bar1, bar3], [bar2]),
        )
        lazy_load = None
        f = Foo()
        f.bars.append(bar2)
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bars"),
            ([bar2], [], []),
        )

    def test_scalar_via_lazyload(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        lazy_load = None

        def lazyload(state, passive):
            return lazy_load

        instrumentation.register_class(Foo)
        _register_attribute(
            Foo, "bar", uselist=False, callable_=lazyload, useobject=False
        )
        lazy_load = "hi"

        # with scalar non-object and active_history=False, the lazy
        # callable is only executed on gets, not history operations

        f = Foo()
        eq_(f.bar, "hi")
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), ["hi"], ()),
        )
        f = Foo()
        f.bar = None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ([None], (), ()),
        )
        f = Foo()
        f.bar = "there"
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            (["there"], (), ()),
        )
        f.bar = "hi"
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            (["hi"], (), ()),
        )
        f = Foo()
        eq_(f.bar, "hi")
        del f.bar
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), ["hi"]),
        )
        assert f.bar is None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), ["hi"]),
        )

    def test_scalar_via_lazyload_with_active(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        lazy_load = None

        def lazyload(state, passive):
            return lazy_load

        instrumentation.register_class(Foo)
        _register_attribute(
            Foo,
            "bar",
            uselist=False,
            callable_=lazyload,
            useobject=False,
            active_history=True,
        )
        lazy_load = "hi"

        # active_history=True means the lazy callable is executed on set
        # as well as get, causing the old value to appear in the history

        f = Foo()
        eq_(f.bar, "hi")
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), ["hi"], ()),
        )
        f = Foo()
        f.bar = None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ([None], (), ["hi"]),
        )
        f = Foo()
        f.bar = "there"
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            (["there"], (), ["hi"]),
        )
        f.bar = "hi"
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), ["hi"], ()),
        )
        f = Foo()
        eq_(f.bar, "hi")
        del f.bar
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), ["hi"]),
        )
        assert f.bar is None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), ["hi"]),
        )

    def test_scalar_object_via_lazyload(self):
        # TODO: break into individual tests

        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        lazy_load = None

        def lazyload(state, passive):
            return lazy_load

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "bar",
            uselist=False,
            callable_=lazyload,
            trackparent=True,
            useobject=True,
        )
        bar1, bar2 = [Bar(id=1), Bar(id=2)]
        lazy_load = bar1

        # with scalar object, the lazy callable is only executed on gets
        # and history operations

        f = Foo()
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), [bar1], ()),
        )
        f = Foo()
        f.bar = None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ([None], (), [bar1]),
        )
        f = Foo()
        f.bar = bar2
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ([bar2], (), [bar1]),
        )
        f.bar = bar1
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), [bar1], ()),
        )
        f = Foo()
        eq_(f.bar, bar1)
        del f.bar
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), [bar1]),
        )
        assert f.bar is None
        eq_(
            attributes.get_state_history(attributes.instance_state(f), "bar"),
            ((), (), [bar1]),
        )


class CollectionKeyTest(fixtures.ORMTest):
    @testing.fixture
    def dict_collection(self):
        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            def __init__(self, name):
                self.name = name

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "someattr",
            uselist=True,
            useobject=True,
            typecallable=attribute_keyed_dict("name"),
        )
        _register_attribute(
            Bar,
            "name",
            uselist=False,
            useobject=False,
        )

        return Foo, Bar

    @testing.fixture
    def list_collection(self):
        class Foo(BasicEntity):
            pass

        class Bar(BasicEntity):
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(
            Foo,
            "someattr",
            uselist=True,
            useobject=True,
        )

        return Foo, Bar

    def test_listen_w_list_key(self, list_collection):
        Foo, Bar = list_collection

        m1 = Mock()

        event.listen(Foo.someattr, "append", m1, include_key=True)
        event.listen(Foo.someattr, "remove", m1, include_key=True)

        f1 = Foo()
        b1, b2, b3 = Bar(), Bar(), Bar()
        f1.someattr.append(b1)
        f1.someattr.append(b2)
        f1.someattr[1] = b3
        del f1.someattr[0]
        append_token, remove_token = (
            Foo.someattr.impl._append_token,
            Foo.someattr.impl._remove_token,
        )

        eq_(
            m1.mock_calls,
            [
                call(
                    f1,
                    b1,
                    append_token,
                    key=NO_KEY,
                ),
                call(
                    f1,
                    b2,
                    append_token,
                    key=NO_KEY,
                ),
                call(
                    f1,
                    b2,
                    remove_token,
                    key=1,
                ),
                call(
                    f1,
                    b3,
                    append_token,
                    key=1,
                ),
                call(
                    f1,
                    b1,
                    remove_token,
                    key=0,
                ),
            ],
        )

    def test_listen_w_dict_key(self, dict_collection):
        Foo, Bar = dict_collection

        m1 = Mock()

        event.listen(Foo.someattr, "append", m1, include_key=True)
        event.listen(Foo.someattr, "remove", m1, include_key=True)

        f1 = Foo()
        b1, b2, b3 = Bar("b1"), Bar("b2"), Bar("b3")
        f1.someattr["k1"] = b1
        f1.someattr.update({"k2": b2, "k3": b3})

        del f1.someattr["k2"]

        append_token, remove_token = (
            Foo.someattr.impl._append_token,
            Foo.someattr.impl._remove_token,
        )

        eq_(
            m1.mock_calls,
            [
                call(
                    f1,
                    b1,
                    append_token,
                    key="k1",
                ),
                call(
                    f1,
                    b2,
                    append_token,
                    key="k2",
                ),
                call(
                    f1,
                    b3,
                    append_token,
                    key="k3",
                ),
                call(
                    f1,
                    b2,
                    remove_token,
                    key="k2",
                ),
            ],
        )

    def test_dict_bulk_replace_w_key(self, dict_collection):
        Foo, Bar = dict_collection

        m1 = Mock()

        event.listen(Foo.someattr, "bulk_replace", m1, include_key=True)
        event.listen(Foo.someattr, "append", m1, include_key=True)
        event.listen(Foo.someattr, "remove", m1, include_key=True)

        f1 = Foo()
        b1, b2, b3, b4 = Bar("b1"), Bar("b2"), Bar("b3"), Bar("b4")
        f1.someattr = {"b1": b1, "b3": b3}
        f1.someattr = {"b2": b2, "b3": b3, "b4": b4}

        bulk_replace_token = Foo.someattr.impl._bulk_replace_token

        eq_(
            m1.mock_calls,
            [
                call(f1, [b1, b3], bulk_replace_token, keys=["b1", "b3"]),
                call(f1, b1, bulk_replace_token, key="b1"),
                call(f1, b3, bulk_replace_token, key="b3"),
                call(
                    f1,
                    [b2, b3, b4],
                    bulk_replace_token,
                    keys=["b2", "b3", "b4"],
                ),
                call(f1, b2, bulk_replace_token, key="b2"),
                call(f1, b4, bulk_replace_token, key="b4"),
                call(f1, b1, bulk_replace_token, key=NO_KEY),
            ],
        )

    def test_listen_wo_dict_key(self, dict_collection):
        Foo, Bar = dict_collection

        m1 = Mock()

        event.listen(Foo.someattr, "append", m1)
        event.listen(Foo.someattr, "remove", m1)

        f1 = Foo()
        b1, b2, b3 = Bar("b1"), Bar("b2"), Bar("b3")
        f1.someattr["k1"] = b1
        f1.someattr.update({"k2": b2, "k3": b3})

        del f1.someattr["k2"]

        append_token, remove_token = (
            Foo.someattr.impl._append_token,
            Foo.someattr.impl._remove_token,
        )

        eq_(
            m1.mock_calls,
            [
                call(
                    f1,
                    b1,
                    append_token,
                ),
                call(
                    f1,
                    b2,
                    append_token,
                ),
                call(
                    f1,
                    b3,
                    append_token,
                ),
                call(
                    f1,
                    b2,
                    remove_token,
                ),
            ],
        )


class ListenerTest(fixtures.ORMTest):
    def test_receive_changes(self):
        """test that Listeners can mutate the given value."""

        class Foo:
            pass

        class Bar:
            pass

        def append(state, child, initiator):
            b2 = Bar()
            b2.data = b1.data + " appended"
            return b2

        def on_set(state, value, oldvalue, initiator):
            return value + " modified"

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "data", uselist=False, useobject=False)
        _register_attribute(Foo, "barlist", uselist=True, useobject=True)
        _register_attribute(
            Foo, "barset", typecallable=set, uselist=True, useobject=True
        )
        _register_attribute(Bar, "data", uselist=False, useobject=False)
        event.listen(Foo.data, "set", on_set, retval=True)
        event.listen(Foo.barlist, "append", append, retval=True)
        event.listen(Foo.barset, "append", append, retval=True)
        f1 = Foo()
        f1.data = "some data"
        eq_(f1.data, "some data modified")
        b1 = Bar()
        b1.data = "some bar"
        f1.barlist.append(b1)
        assert b1.data == "some bar"
        assert f1.barlist[0].data == "some bar appended"
        f1.barset.add(b1)
        assert f1.barset.pop().data == "some bar appended"

    def test_named(self):
        canary = Mock()

        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "data", uselist=False, useobject=False)
        _register_attribute(Foo, "barlist", uselist=True, useobject=True)

        event.listen(Foo.data, "set", canary.set, named=True)
        event.listen(Foo.barlist, "append", canary.append, named=True)
        event.listen(Foo.barlist, "remove", canary.remove, named=True)

        f1 = Foo()
        b1 = Bar()
        f1.data = 5
        f1.barlist.append(b1)
        f1.barlist.remove(b1)
        eq_(
            canary.mock_calls,
            [
                call.set(
                    oldvalue=attributes.NO_VALUE,
                    initiator=attributes.AttributeEventToken(
                        Foo.data.impl, attributes.OP_REPLACE
                    ),
                    target=f1,
                    value=5,
                ),
                call.append(
                    initiator=attributes.AttributeEventToken(
                        Foo.barlist.impl, attributes.OP_APPEND
                    ),
                    target=f1,
                    value=b1,
                ),
                call.remove(
                    initiator=attributes.AttributeEventToken(
                        Foo.barlist.impl, attributes.OP_REMOVE
                    ),
                    target=f1,
                    value=b1,
                ),
            ],
        )

    def test_collection_link_events(self):
        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "barlist", uselist=True, useobject=True)

        canary = Mock()
        event.listen(Foo.barlist, "init_collection", canary.init)
        event.listen(Foo.barlist, "dispose_collection", canary.dispose)

        f1 = Foo()
        eq_(f1.barlist, [])
        adapter_one = f1.barlist._sa_adapter
        eq_(canary.init.mock_calls, [call(f1, [], adapter_one)])

        b1 = Bar()
        f1.barlist.append(b1)

        b2 = Bar()
        f1.barlist = [b2]
        adapter_two = f1.barlist._sa_adapter
        eq_(
            canary.init.mock_calls,
            [
                call(f1, [b1], adapter_one),  # note the f1.barlist that
                # we saved earlier has been mutated
                # in place, new as of [ticket:3913]
                call(f1, [b2], adapter_two),
            ],
        )
        eq_(canary.dispose.mock_calls, [call(f1, [b1], adapter_one)])

    def test_none_on_collection_event(self):
        """test that append/remove of None in collections emits events.

        This is new behavior as of 0.8.

        """

        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "barlist", uselist=True, useobject=True)
        canary = []

        def append(state, child, initiator):
            canary.append((state, child))

        def remove(state, child, initiator):
            canary.append((state, child))

        event.listen(Foo.barlist, "append", append)
        event.listen(Foo.barlist, "remove", remove)

        b1, b2 = Bar(), Bar()
        f1 = Foo()
        f1.barlist.append(None)
        eq_(canary, [(f1, None)])

        canary[:] = []
        f1 = Foo()
        f1.barlist = [None, b2]
        eq_(canary, [(f1, None), (f1, b2)])

        canary[:] = []
        f1 = Foo()
        f1.barlist = [b1, None, b2]
        eq_(canary, [(f1, b1), (f1, None), (f1, b2)])

        f1.barlist.remove(None)
        eq_(canary, [(f1, b1), (f1, None), (f1, b2), (f1, None)])

    def test_flag_modified(self):
        canary = Mock()

        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "bar")

        event.listen(Foo.bar, "modified", canary)
        f1 = Foo()
        f1.bar = "hi"
        attributes.flag_modified(f1, "bar")
        eq_(
            canary.mock_calls,
            [
                call(
                    f1,
                    attributes.AttributeEventToken(
                        Foo.bar.impl, attributes.OP_MODIFIED
                    ),
                )
            ],
        )

    def test_none_init_scalar(self):
        canary = Mock()

        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "bar")

        event.listen(Foo.bar, "set", canary)

        f1 = Foo()
        eq_(f1.bar, None)
        # reversal of approach in #3061
        eq_(canary.mock_calls, [])

    def test_none_init_object(self):
        canary = Mock()

        class Foo:
            pass

        instrumentation.register_class(Foo)
        _register_attribute(Foo, "bar", useobject=True)

        event.listen(Foo.bar, "set", canary)

        f1 = Foo()
        eq_(f1.bar, None)
        # reversal of approach in #3061
        eq_(canary.mock_calls, [])

    def test_none_init_collection(self):
        canary = Mock()

        class Foo:
            pass

        class Bar:
            pass

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)
        _register_attribute(Foo, "bar", useobject=True, uselist=True)

        event.listen(Foo.bar, "set", canary)

        f1 = Foo()
        eq_(f1.bar, [])

        assert "bar" not in f1.__dict__

        adapter = Foo.bar.impl.get_collection(
            attributes.instance_state(f1), attributes.instance_dict(f1)
        )
        assert adapter.empty

        # reversal of approach in #3061
        eq_(canary.mock_calls, [])

        f1.bar.append(Bar())
        assert "bar" in f1.__dict__
        assert not adapter.empty


class EventPropagateTest(fixtures.TestBase):
    # tests that were expanded as of #4695
    # in particular these reveal the inconsistency we have in returning the
    # "old" value between object and non-object.
    # this inconsistency might not be a bug, since the nature of scalar
    # SQL attributes and ORM related objects is fundamentally different.

    # the inconsistency is:

    # with active_history=False if old value is not present, for scalar we
    # return NO VALUE, for object we return NEVER SET
    # with active_history=True if old value is not present, for scalar we
    # return NEVER SET, for object we return None
    # so it is basically fully inconsistent across both directions.

    def test_propagate_active_history(self):
        for (A, B, C, D), canary in self._test_propagate_fixtures(True, False):
            b = B()
            b.attrib = "foo"
            eq_(b.attrib, "foo")
            eq_(canary, [("foo", attributes.NO_VALUE)])

            c = C()
            c.attrib = "bar"
            eq_(c.attrib, "bar")
            eq_(
                canary,
                [("foo", attributes.NO_VALUE), ("bar", attributes.NO_VALUE)],
            )

    def test_propagate(self):
        for (A, B, C, D), canary in self._test_propagate_fixtures(
            False, False
        ):
            b = B()
            b.attrib = "foo"
            eq_(b.attrib, "foo")

            eq_(canary, [("foo", attributes.NO_VALUE)])

            c = C()
            c.attrib = "bar"
            eq_(c.attrib, "bar")
            eq_(
                canary,
                [("foo", attributes.NO_VALUE), ("bar", attributes.NO_VALUE)],
            )

    def test_propagate_useobject_active_history(self):
        for (A, B, C, D), canary in self._test_propagate_fixtures(True, True):
            b = B()
            d1 = D()
            b.attrib = d1
            is_(b.attrib, d1)
            eq_(canary, [(d1, None)])

            c = C()
            d2 = D()
            c.attrib = d2
            is_(c.attrib, d2)
            eq_(canary, [(d1, None), (d2, None)])

    def test_propagate_useobject(self):
        for (A, B, C, D), canary in self._test_propagate_fixtures(False, True):
            b = B()
            d1 = D()
            b.attrib = d1
            is_(b.attrib, d1)
            eq_(canary, [(d1, attributes.NO_VALUE)])

            c = C()
            d2 = D()
            c.attrib = d2
            is_(c.attrib, d2)
            eq_(canary, [(d1, attributes.NO_VALUE), (d2, attributes.NO_VALUE)])

    def _test_propagate_fixtures(self, active_history, useobject):
        classes = [None, None, None, None]
        canary = []

        def make_a():
            class A:
                pass

            classes[0] = A

        def make_b():
            class B(classes[0]):
                pass

            classes[1] = B

        def make_c():
            class C(classes[1]):
                pass

            classes[2] = C

        def make_d():
            class D:
                pass

            classes[3] = D
            return D

        def instrument_a():
            instrumentation.register_class(classes[0])

        def instrument_b():
            instrumentation.register_class(classes[1])

        def instrument_c():
            instrumentation.register_class(classes[2])

        def instrument_d():
            instrumentation.register_class(classes[3])

        def attr_a():
            _register_attribute(
                classes[0], "attrib", uselist=False, useobject=useobject
            )

        def attr_b():
            _register_attribute(
                classes[1], "attrib", uselist=False, useobject=useobject
            )

        def attr_c():
            _register_attribute(
                classes[2], "attrib", uselist=False, useobject=useobject
            )

        def set_(state, value, oldvalue, initiator):
            canary.append((value, oldvalue))

        def events_a():
            event.listen(
                classes[0].attrib,
                "set",
                set_,
                propagate=True,
                active_history=active_history,
            )

        ordering = [
            (instrument_a, instrument_b),
            (instrument_b, instrument_c),
            (attr_a, attr_b),
            (attr_b, attr_c),
            (make_a, instrument_a),
            (instrument_a, attr_a),
            (attr_a, events_a),
            (make_b, instrument_b),
            (instrument_b, attr_b),
            (make_c, instrument_c),
            (instrument_c, attr_c),
            (make_a, make_b),
            (make_b, make_c),
        ]
        elements = [
            make_a,
            make_b,
            make_c,
            instrument_a,
            instrument_b,
            instrument_c,
            attr_a,
            attr_b,
            attr_c,
            events_a,
        ]

        for i, series in enumerate(all_partial_orderings(ordering, elements)):
            for fn in series:
                fn()

            if useobject:
                make_d()
                instrument_d()

            yield classes, canary

            classes[:] = [None, None, None, None]
            canary[:] = []


class CollectionInitTest(fixtures.TestBase):
    def setup_test(self):
        class A:
            pass

        class B:
            pass

        self.A = A
        self.B = B
        instrumentation.register_class(A)
        instrumentation.register_class(B)
        _register_attribute(A, "bs", uselist=True, useobject=True)

    def test_bulk_replace_resets_empty(self):
        A = self.A
        a1 = A()
        state = attributes.instance_state(a1)

        existing = a1.bs

        is_(state._empty_collections["bs"], existing)
        is_not(existing._sa_adapter, None)

        a1.bs = []  # replaces previous "empty" collection
        not_in("bs", state._empty_collections)  # empty is replaced
        is_(existing._sa_adapter, None)

    def test_assert_false_on_default_value(self):
        A = self.A
        a1 = A()
        state = attributes.instance_state(a1)

        attributes.init_state_collection(state, state.dict, "bs")

        assert_raises(
            AssertionError, A.bs.impl._default_value, state, state.dict
        )

    def test_loader_inits_collection_already_exists(self):
        A, B = self.A, self.B
        a1 = A()
        b1, b2 = B(), B()
        a1.bs = [b1, b2]
        eq_(a1.__dict__["bs"], [b1, b2])

        old = a1.__dict__["bs"]
        is_not(old._sa_adapter, None)
        state = attributes.instance_state(a1)

        # this occurs during a load with populate_existing
        adapter = attributes.init_state_collection(state, state.dict, "bs")

        new = a1.__dict__["bs"]
        eq_(new, [])
        is_(new._sa_adapter, adapter)
        is_(old._sa_adapter, None)


class TestUnlink(fixtures.TestBase):
    def setup_test(self):
        class A:
            pass

        class B:
            pass

        self.A = A
        self.B = B
        instrumentation.register_class(A)
        instrumentation.register_class(B)
        _register_attribute(A, "bs", uselist=True, useobject=True)

    def test_expired(self):
        A, B = self.A, self.B
        a1 = A()
        coll = a1.bs
        a1.bs.append(B())
        state = attributes.instance_state(a1)
        state._expire(state.dict, set())
        assert_warns(Warning, coll.append, B())

    def test_replaced(self):
        A, B = self.A, self.B
        a1 = A()
        coll = a1.bs
        a1.bs.append(B())
        a1.bs = []
        # a bulk replace no longer empties the old collection
        # as of [ticket:3913]
        assert len(coll) == 1
        coll.append(B())
        assert len(coll) == 2

    def test_pop_existing(self):
        A, B = self.A, self.B
        a1 = A()
        coll = a1.bs
        a1.bs.append(B())
        state = attributes.instance_state(a1)
        state._reset(state.dict, "bs")
        assert_warns(Warning, coll.append, B())

    def test_ad_hoc_lazy(self):
        A, B = self.A, self.B
        a1 = A()
        coll = a1.bs
        a1.bs.append(B())
        state = attributes.instance_state(a1)
        _set_callable(state, state.dict, "bs", lambda: B())
        assert_warns(Warning, coll.append, B())
