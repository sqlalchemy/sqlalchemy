import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import util
from sqlalchemy.ext import instrumentation
from sqlalchemy.orm import attributes
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import events
from sqlalchemy.orm.attributes import del_attribute
from sqlalchemy.orm.attributes import get_attribute
from sqlalchemy.orm.attributes import set_attribute
from sqlalchemy.orm.instrumentation import is_instrumented
from sqlalchemy.orm.instrumentation import manager_of_class
from sqlalchemy.orm.instrumentation import register_class
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import ne_
from sqlalchemy.testing.util import decorator


@decorator
def modifies_instrumentation_finders(fn, *args, **kw):
    pristine = instrumentation.instrumentation_finders[:]
    try:
        fn(*args, **kw)
    finally:
        del instrumentation.instrumentation_finders[:]
        instrumentation.instrumentation_finders.extend(pristine)


class _ExtBase(object):
    @classmethod
    def teardown_test_class(cls):
        instrumentation._reinstall_default_lookups()


class MyTypesManager(instrumentation.InstrumentationManager):
    def instrument_attribute(self, class_, key, attr):
        pass

    def install_descriptor(self, class_, key, attr):
        pass

    def uninstall_descriptor(self, class_, key):
        pass

    def instrument_collection_class(self, class_, key, collection_class):
        return MyListLike

    def get_instance_dict(self, class_, instance):
        return instance._goofy_dict

    def initialize_instance_dict(self, class_, instance):
        instance.__dict__["_goofy_dict"] = {}

    def install_state(self, class_, instance, state):
        instance.__dict__["_my_state"] = state

    def state_getter(self, class_):
        return lambda instance: instance.__dict__["_my_state"]


class MyListLike(list):
    # add @appender, @remover decorators as needed
    _sa_iterator = list.__iter__
    _sa_linker = None
    _sa_converter = None

    def _sa_appender(self, item, _sa_initiator=None):
        if _sa_initiator is not False:
            self._sa_adapter.fire_append_event(item, _sa_initiator)
        list.append(self, item)

    append = _sa_appender

    def _sa_remover(self, item, _sa_initiator=None):
        self._sa_adapter.fire_pre_remove_event(_sa_initiator)
        if _sa_initiator is not False:
            self._sa_adapter.fire_remove_event(item, _sa_initiator)
        list.remove(self, item)

    remove = _sa_remover


MyBaseClass, MyClass = None, None


class UserDefinedExtensionTest(_ExtBase, fixtures.ORMTest):
    @classmethod
    def setup_test_class(cls):
        global MyBaseClass, MyClass

        class MyBaseClass(object):
            __sa_instrumentation_manager__ = (
                instrumentation.InstrumentationManager
            )

        class MyClass(object):

            # This proves that a staticmethod will work here; don't
            # flatten this back to a class assignment!
            def __sa_instrumentation_manager__(cls):
                return MyTypesManager(cls)

            __sa_instrumentation_manager__ = staticmethod(
                __sa_instrumentation_manager__
            )

            # This proves SA can handle a class with non-string dict keys
            if util.cpython:
                locals()[42] = 99  # Don't remove this line!

            def __init__(self, **kwargs):
                for k in kwargs:
                    setattr(self, k, kwargs[k])

            def __getattr__(self, key):
                if is_instrumented(self, key):
                    return get_attribute(self, key)
                else:
                    try:
                        return self._goofy_dict[key]
                    except KeyError:
                        raise AttributeError(key)

            def __setattr__(self, key, value):
                if is_instrumented(self, key):
                    set_attribute(self, key, value)
                else:
                    self._goofy_dict[key] = value

            def __hasattr__(self, key):
                if is_instrumented(self, key):
                    return True
                else:
                    return key in self._goofy_dict

            def __delattr__(self, key):
                if is_instrumented(self, key):
                    del_attribute(self, key)
                else:
                    del self._goofy_dict[key]

    def teardown_test(self):
        clear_mappers()

    def test_instance_dict(self):
        class User(MyClass):
            pass

        register_class(User)
        attributes.register_attribute(
            User, "user_id", uselist=False, useobject=False
        )
        attributes.register_attribute(
            User, "user_name", uselist=False, useobject=False
        )
        attributes.register_attribute(
            User, "email_address", uselist=False, useobject=False
        )

        u = User()
        u.user_id = 7
        u.user_name = "john"
        u.email_address = "lala@123.com"
        eq_(
            u.__dict__,
            {
                "_my_state": u._my_state,
                "_goofy_dict": {
                    "user_id": 7,
                    "user_name": "john",
                    "email_address": "lala@123.com",
                },
            },
        )

    def test_basic(self):
        for base in (object, MyBaseClass, MyClass):

            class User(base):
                pass

            register_class(User)
            attributes.register_attribute(
                User, "user_id", uselist=False, useobject=False
            )
            attributes.register_attribute(
                User, "user_name", uselist=False, useobject=False
            )
            attributes.register_attribute(
                User, "email_address", uselist=False, useobject=False
            )

            u = User()
            u.user_id = 7
            u.user_name = "john"
            u.email_address = "lala@123.com"

            eq_(u.user_id, 7)
            eq_(u.user_name, "john")
            eq_(u.email_address, "lala@123.com")
            attributes.instance_state(u)._commit_all(
                attributes.instance_dict(u)
            )
            eq_(u.user_id, 7)
            eq_(u.user_name, "john")
            eq_(u.email_address, "lala@123.com")

            u.user_name = "heythere"
            u.email_address = "foo@bar.com"
            eq_(u.user_id, 7)
            eq_(u.user_name, "heythere")
            eq_(u.email_address, "foo@bar.com")

    def test_deferred(self):
        for base in (object, MyBaseClass, MyClass):

            class Foo(base):
                pass

            data = {"a": "this is a", "b": 12}

            def loader(state, keys, passive):
                for k in keys:
                    state.dict[k] = data[k]
                return attributes.ATTR_WAS_SET

            manager = register_class(Foo)
            manager.expired_attribute_loader = loader
            attributes.register_attribute(
                Foo, "a", uselist=False, useobject=False
            )
            attributes.register_attribute(
                Foo, "b", uselist=False, useobject=False
            )

            if base is object:
                assert Foo not in (
                    instrumentation._instrumentation_factory._state_finders
                )
            else:
                assert Foo in (
                    instrumentation._instrumentation_factory._state_finders
                )

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
                attributes.instance_dict(f)
            )
            eq_(f.a, None)
            eq_(f.b, 12)

    def test_inheritance(self):
        """tests that attributes are polymorphic"""

        for base in (object, MyBaseClass, MyClass):

            class Foo(base):
                pass

            class Bar(Foo):
                pass

            register_class(Foo)
            register_class(Bar)

            def func1(state, passive):
                return "this is the foo attr"

            def func2(state, passive):
                return "this is the bar attr"

            def func3(state, passive):
                return "this is the shared attr"

            attributes.register_attribute(
                Foo, "element", uselist=False, callable_=func1, useobject=True
            )
            attributes.register_attribute(
                Foo, "element2", uselist=False, callable_=func3, useobject=True
            )
            attributes.register_attribute(
                Bar, "element", uselist=False, callable_=func2, useobject=True
            )

            x = Foo()
            y = Bar()
            assert x.element == "this is the foo attr"
            assert y.element == "this is the bar attr", y.element
            assert x.element2 == "this is the shared attr"
            assert y.element2 == "this is the shared attr"

    def test_collection_with_backref(self):
        for base in (object, MyBaseClass, MyClass):

            class Post(base):
                pass

            class Blog(base):
                pass

            register_class(Post)
            register_class(Blog)
            attributes.register_attribute(
                Post,
                "blog",
                uselist=False,
                backref="posts",
                trackparent=True,
                useobject=True,
            )
            attributes.register_attribute(
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

    def test_history(self):
        for base in (object, MyBaseClass, MyClass):

            class Foo(base):
                pass

            class Bar(base):
                pass

            register_class(Foo)
            register_class(Bar)
            attributes.register_attribute(
                Foo, "name", uselist=False, useobject=False
            )
            attributes.register_attribute(
                Foo, "bars", uselist=True, trackparent=True, useobject=True
            )
            attributes.register_attribute(
                Bar, "name", uselist=False, useobject=False
            )

            f1 = Foo()
            f1.name = "f1"

            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "name"
                ),
                (["f1"], (), ()),
            )

            b1 = Bar()
            b1.name = "b1"
            f1.bars.append(b1)
            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "bars"
                ),
                ([b1], [], []),
            )

            attributes.instance_state(f1)._commit_all(
                attributes.instance_dict(f1)
            )
            attributes.instance_state(b1)._commit_all(
                attributes.instance_dict(b1)
            )

            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "name"
                ),
                ((), ["f1"], ()),
            )
            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "bars"
                ),
                ((), [b1], ()),
            )

            f1.name = "f1mod"
            b2 = Bar()
            b2.name = "b2"
            f1.bars.append(b2)
            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "name"
                ),
                (["f1mod"], (), ["f1"]),
            )
            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "bars"
                ),
                ([b2], [b1], []),
            )
            f1.bars.remove(b1)
            eq_(
                attributes.get_state_history(
                    attributes.instance_state(f1), "bars"
                ),
                ([b2], [], [b1]),
            )

    def test_null_instrumentation(self):
        class Foo(MyBaseClass):
            pass

        register_class(Foo)
        attributes.register_attribute(
            Foo, "name", uselist=False, useobject=False
        )
        attributes.register_attribute(
            Foo, "bars", uselist=True, trackparent=True, useobject=True
        )

        assert Foo.name == attributes.manager_of_class(Foo)["name"]
        assert Foo.bars == attributes.manager_of_class(Foo)["bars"]

    def test_alternate_finders(self):
        """Ensure the generic finder front-end deals with edge cases."""

        class Unknown(object):
            pass

        class Known(MyBaseClass):
            pass

        register_class(Known)
        k, u = Known(), Unknown()

        assert instrumentation.manager_of_class(Unknown) is None
        assert instrumentation.manager_of_class(Known) is not None
        assert instrumentation.manager_of_class(None) is None

        assert attributes.instance_state(k) is not None
        assert_raises((AttributeError, KeyError), attributes.instance_state, u)
        assert_raises(
            (AttributeError, KeyError), attributes.instance_state, None
        )

    def test_unmapped_not_type_error(self):
        """extension version of the same test in test_mapper.

        fixes #3408
        """
        assert_raises_message(
            sa.exc.ArgumentError,
            "Class object expected, got '5'.",
            class_mapper,
            5,
        )

    def test_unmapped_not_type_error_iter_ok(self):
        """extension version of the same test in test_mapper.

        fixes #3408
        """
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Class object expected, got '\(5, 6\)'.",
            class_mapper,
            (5, 6),
        )


class FinderTest(_ExtBase, fixtures.ORMTest):
    def test_standard(self):
        class A(object):
            pass

        register_class(A)

        eq_(type(manager_of_class(A)), instrumentation.ClassManager)

    def test_nativeext_interfaceexact(self):
        class A(object):
            __sa_instrumentation_manager__ = (
                instrumentation.InstrumentationManager
            )

        register_class(A)
        ne_(type(manager_of_class(A)), instrumentation.ClassManager)

    def test_nativeext_submanager(self):
        class Mine(instrumentation.ClassManager):
            pass

        class A(object):
            __sa_instrumentation_manager__ = Mine

        register_class(A)
        eq_(type(manager_of_class(A)), Mine)

    @modifies_instrumentation_finders
    def test_customfinder_greedy(self):
        class Mine(instrumentation.ClassManager):
            pass

        class A(object):
            pass

        def find(cls):
            return Mine

        instrumentation.instrumentation_finders.insert(0, find)
        register_class(A)
        eq_(type(manager_of_class(A)), Mine)

    @modifies_instrumentation_finders
    def test_customfinder_pass(self):
        class A(object):
            pass

        def find(cls):
            return None

        instrumentation.instrumentation_finders.insert(0, find)
        register_class(A)

        eq_(type(manager_of_class(A)), instrumentation.ClassManager)


class InstrumentationCollisionTest(_ExtBase, fixtures.ORMTest):
    def test_none(self):
        class A(object):
            pass

        register_class(A)

        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class B(object):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        register_class(B)

        class C(object):
            __sa_instrumentation_manager__ = instrumentation.ClassManager

        register_class(C)

    def test_single_down(self):
        class A(object):
            pass

        register_class(A)

        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class B(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        assert_raises_message(
            TypeError,
            "multiple instrumentation implementations",
            register_class,
            B,
        )

    def test_single_up(self):
        class A(object):
            pass

        # delay registration

        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class B(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        register_class(B)

        assert_raises_message(
            TypeError,
            "multiple instrumentation implementations",
            register_class,
            A,
        )

    def test_diamond_b1(self):
        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class A(object):
            pass

        class B1(A):
            pass

        class B2(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        class C(object):
            pass

        assert_raises_message(
            TypeError,
            "multiple instrumentation implementations",
            register_class,
            B1,
        )

    def test_diamond_b2(self):
        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class A(object):
            pass

        class B1(A):
            pass

        class B2(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        class C(object):
            pass

        register_class(B2)
        assert_raises_message(
            TypeError,
            "multiple instrumentation implementations",
            register_class,
            B1,
        )

    def test_diamond_c_b(self):
        def mgr_factory(cls):
            return instrumentation.ClassManager(cls)

        class A(object):
            pass

        class B1(A):
            pass

        class B2(A):
            __sa_instrumentation_manager__ = staticmethod(mgr_factory)

        class C(object):
            pass

        register_class(C)

        assert_raises_message(
            TypeError,
            "multiple instrumentation implementations",
            register_class,
            B1,
        )


class ExtendedEventsTest(_ExtBase, fixtures.ORMTest):

    """Allow custom Events implementations."""

    @modifies_instrumentation_finders
    def test_subclassed(self):
        class MyEvents(events.InstanceEvents):
            pass

        class MyClassManager(instrumentation.ClassManager):
            dispatch = event.dispatcher(MyEvents)

        instrumentation.instrumentation_finders.insert(
            0, lambda cls: MyClassManager
        )

        class A(object):
            pass

        register_class(A)
        manager = instrumentation.manager_of_class(A)
        assert issubclass(manager.dispatch._events, MyEvents)
