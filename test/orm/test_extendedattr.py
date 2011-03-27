from test.lib.testing import eq_, assert_raises, assert_raises_message
import pickle
from sqlalchemy import util
from sqlalchemy.orm import attributes, instrumentation
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.attributes import set_attribute, get_attribute, del_attribute
from sqlalchemy.orm.instrumentation import is_instrumented
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import InstrumentationManager
from test.lib import *
from test.lib import fixtures

class MyTypesManager(InstrumentationManager):

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
        instance.__dict__['_goofy_dict'] = {}

    def install_state(self, class_, instance, state):
        instance.__dict__['_my_state'] = state

    def state_getter(self, class_):
        return lambda instance: instance.__dict__['_my_state']

class MyListLike(list):
    # add @appender, @remover decorators as needed
    _sa_iterator = list.__iter__
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

class MyBaseClass(object):
    __sa_instrumentation_manager__ = InstrumentationManager

class MyClass(object):

    # This proves that a staticmethod will work here; don't
    # flatten this back to a class assignment!
    def __sa_instrumentation_manager__(cls):
        return MyTypesManager(cls)

    __sa_instrumentation_manager__ = staticmethod(__sa_instrumentation_manager__)

    # This proves SA can handle a class with non-string dict keys
    if not util.pypy and not util.jython:
        locals()[42] = 99   # Don't remove this line!

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

class UserDefinedExtensionTest(fixtures.ORMTest):
    @classmethod
    def teardown_class(cls):
        clear_mappers()
        instrumentation._install_lookup_strategy(util.symbol('native'))

    def test_instance_dict(self):
        class User(MyClass):
            pass

        instrumentation.register_class(User)
        attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(User, 'email_address', uselist = False, useobject=False)

        u = User()
        u.user_id = 7
        u.user_name = 'john'
        u.email_address = 'lala@123.com'
        self.assert_(u.__dict__ == {'_my_state':u._my_state, '_goofy_dict':{'user_id':7, 'user_name':'john', 'email_address':'lala@123.com'}}, u.__dict__)

    def test_basic(self):
        for base in (object, MyBaseClass, MyClass):
            class User(base):
                pass

            instrumentation.register_class(User)
            attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
            attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
            attributes.register_attribute(User, 'email_address', uselist = False, useobject=False)

            u = User()
            u.user_id = 7
            u.user_name = 'john'
            u.email_address = 'lala@123.com'

            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')
            attributes.instance_state(u).commit_all(attributes.instance_dict(u))
            self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

            u.user_name = 'heythere'
            u.email_address = 'foo@bar.com'
            self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.email_address == 'foo@bar.com')

    def test_deferred(self):
        for base in (object, MyBaseClass, MyClass):
            class Foo(base):pass

            data = {'a':'this is a', 'b':12}
            def loader(state, keys):
                for k in keys:
                    state.dict[k] = data[k]
                return attributes.ATTR_WAS_SET

            manager = instrumentation.register_class(Foo)
            manager.deferred_scalar_loader = loader
            attributes.register_attribute(Foo, 'a', uselist=False, useobject=False)
            attributes.register_attribute(Foo, 'b', uselist=False, useobject=False)

            assert Foo in instrumentation.instrumentation_registry._state_finders
            f = Foo()
            attributes.instance_state(f).expire(attributes.instance_dict(f), set())
            eq_(f.a, "this is a")
            eq_(f.b, 12)

            f.a = "this is some new a"
            attributes.instance_state(f).expire(attributes.instance_dict(f), set())
            eq_(f.a, "this is a")
            eq_(f.b, 12)

            attributes.instance_state(f).expire(attributes.instance_dict(f), set())
            f.a = "this is another new a"
            eq_(f.a, "this is another new a")
            eq_(f.b, 12)

            attributes.instance_state(f).expire(attributes.instance_dict(f), set())
            eq_(f.a, "this is a")
            eq_(f.b, 12)

            del f.a
            eq_(f.a, None)
            eq_(f.b, 12)

            attributes.instance_state(f).commit_all(attributes.instance_dict(f))
            eq_(f.a, None)
            eq_(f.b, 12)

    def test_inheritance(self):
        """tests that attributes are polymorphic"""

        for base in (object, MyBaseClass, MyClass):
            class Foo(base):pass
            class Bar(Foo):pass

            instrumentation.register_class(Foo)
            instrumentation.register_class(Bar)

            def func1(state, passive):
                return "this is the foo attr"
            def func2(state, passive):
                return "this is the bar attr"
            def func3(state, passive):
                return "this is the shared attr"
            attributes.register_attribute(Foo, 'element',
                    uselist=False, callable_=func1,
                    useobject=True)
            attributes.register_attribute(Foo, 'element2',
                    uselist=False, callable_=func3,
                    useobject=True)
            attributes.register_attribute(Bar, 'element',
                    uselist=False, callable_=func2,
                    useobject=True)

            x = Foo()
            y = Bar()
            assert x.element == 'this is the foo attr'
            assert y.element == 'this is the bar attr', y.element
            assert x.element2 == 'this is the shared attr'
            assert y.element2 == 'this is the shared attr'

    def test_collection_with_backref(self):
        for base in (object, MyBaseClass, MyClass):
            class Post(base):pass
            class Blog(base):pass

            instrumentation.register_class(Post)
            instrumentation.register_class(Blog)
            attributes.register_attribute(Post, 'blog', uselist=False,
                    backref='posts', trackparent=True, useobject=True)
            attributes.register_attribute(Blog, 'posts', uselist=True,
                    backref='blog', trackparent=True, useobject=True)
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

            instrumentation.register_class(Foo)
            instrumentation.register_class(Bar)
            attributes.register_attribute(Foo, "name", uselist=False, useobject=False)
            attributes.register_attribute(Foo, "bars", uselist=True, trackparent=True, useobject=True)
            attributes.register_attribute(Bar, "name", uselist=False, useobject=False)


            f1 = Foo()
            f1.name = 'f1'

            eq_(attributes.get_state_history(attributes.instance_state(f1), 'name'), (['f1'], (), ()))

            b1 = Bar()
            b1.name = 'b1'
            f1.bars.append(b1)
            eq_(attributes.get_state_history(attributes.instance_state(f1), 'bars'), ([b1], [], []))

            attributes.instance_state(f1).commit_all(attributes.instance_dict(f1))
            attributes.instance_state(b1).commit_all(attributes.instance_dict(b1))

            eq_(attributes.get_state_history(attributes.instance_state(f1), 'name'), ((), ['f1'], ()))
            eq_(attributes.get_state_history(attributes.instance_state(f1), 'bars'), ((), [b1], ()))

            f1.name = 'f1mod'
            b2 = Bar()
            b2.name = 'b2'
            f1.bars.append(b2)
            eq_(attributes.get_state_history(attributes.instance_state(f1), 'name'), (['f1mod'], (), ['f1']))
            eq_(attributes.get_state_history(attributes.instance_state(f1), 'bars'), ([b2], [b1], []))
            f1.bars.remove(b1)
            eq_(attributes.get_state_history(attributes.instance_state(f1), 'bars'), ([b2], [], [b1]))

    def test_null_instrumentation(self):
        class Foo(MyBaseClass):
            pass
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, "name", uselist=False, useobject=False)
        attributes.register_attribute(Foo, "bars", uselist=True, trackparent=True, useobject=True)

        assert Foo.name == attributes.manager_of_class(Foo)['name']
        assert Foo.bars == attributes.manager_of_class(Foo)['bars']

    def test_alternate_finders(self):
        """Ensure the generic finder front-end deals with edge cases."""

        class Unknown(object): pass
        class Known(MyBaseClass): pass

        instrumentation.register_class(Known)
        k, u = Known(), Unknown()

        assert instrumentation.manager_of_class(Unknown) is None
        assert instrumentation.manager_of_class(Known) is not None
        assert instrumentation.manager_of_class(None) is None

        assert attributes.instance_state(k) is not None
        assert_raises((AttributeError, KeyError),
                          attributes.instance_state, u)
        assert_raises((AttributeError, KeyError),
                          attributes.instance_state, None)


if __name__ == '__main__':
    testing.main()
