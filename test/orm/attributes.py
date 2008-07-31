import testenv; testenv.configure_for_tests()
import pickle
import sqlalchemy.orm.attributes as attributes
from sqlalchemy.orm.collections import collection
from sqlalchemy import exceptions
from testlib import *
from testlib import fixtures

ROLLBACK_SUPPORTED=False

# these test classes defined at the module
# level to support pickling
class MyTest(object):pass
class MyTest2(object):pass

class AttributesTest(TestBase):

    def test_basic(self):
        class User(object):pass

        attributes.register_class(User)
        attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(User, 'email_address', uselist = False, useobject=False)

        u = User()
        u.user_id = 7
        u.user_name = 'john'
        u.email_address = 'lala@123.com'

        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')
        u._state.commit_all()
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

        u.user_name = 'heythere'
        u.email_address = 'foo@bar.com'
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.email_address == 'foo@bar.com')

    def test_pickleness(self):
        attributes.register_class(MyTest)
        attributes.register_class(MyTest2)
        attributes.register_attribute(MyTest, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(MyTest, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(MyTest, 'email_address', uselist = False, useobject=False)
        attributes.register_attribute(MyTest2, 'a', uselist = False, useobject=False)
        attributes.register_attribute(MyTest2, 'b', uselist = False, useobject=False)
        # shouldnt be pickling callables at the class level
        def somecallable(*args):
            return None
        attr_name = 'mt2'
        attributes.register_attribute(MyTest, attr_name, uselist = True, trackparent=True, callable_=somecallable, useobject=True)

        o = MyTest()
        o.mt2.append(MyTest2())
        o.user_id=7
        o.mt2[0].a = 'abcde'
        pk_o = pickle.dumps(o)

        o2 = pickle.loads(pk_o)
        pk_o2 = pickle.dumps(o2)

        # so... pickle is creating a new 'mt2' string after a roundtrip here,
        # so we'll brute-force set it to be id-equal to the original string
        if False:
            o_mt2_str = [ k for k in o.__dict__ if k == 'mt2'][0]
            o2_mt2_str = [ k for k in o2.__dict__ if k == 'mt2'][0]
            self.assert_(o_mt2_str == o2_mt2_str)
            self.assert_(o_mt2_str is not o2_mt2_str)
            # change the id of o2.__dict__['mt2']
            former = o2.__dict__['mt2']
            del o2.__dict__['mt2']
            o2.__dict__[o_mt2_str] = former

            self.assert_(pk_o == pk_o2)

        # the above is kind of distrurbing, so let's do it again a little
        # differently.  the string-id in serialization thing is just an
        # artifact of pickling that comes up in the first round-trip.
        # a -> b differs in pickle memoization of 'mt2', but b -> c will
        # serialize identically.

        o3 = pickle.loads(pk_o2)
        pk_o3 = pickle.dumps(o3)
        o4 = pickle.loads(pk_o3)
        pk_o4 = pickle.dumps(o4)

        self.assert_(pk_o3 == pk_o4)

        # and lastly make sure we still have our data after all that.
        # identical serialzation is great, *if* it's complete :)
        self.assert_(o4.user_id == 7)
        self.assert_(o4.user_name is None)
        self.assert_(o4.email_address is None)
        self.assert_(len(o4.mt2) == 1)
        self.assert_(o4.mt2[0].a == 'abcde')
        self.assert_(o4.mt2[0].b is None)

    def test_deferred(self):
        class Foo(object):pass

        data = {'a':'this is a', 'b':12}
        def loader(instance, keys):
            for k in keys:
                instance.__dict__[k] = data[k]
            return attributes.ATTR_WAS_SET

        attributes.register_class(Foo, deferred_scalar_loader=loader)
        attributes.register_attribute(Foo, 'a', uselist=False, useobject=False)
        attributes.register_attribute(Foo, 'b', uselist=False, useobject=False)

        f = Foo()
        f._state.expire_attributes(None)
        self.assertEquals(f.a, "this is a")
        self.assertEquals(f.b, 12)

        f.a = "this is some new a"
        f._state.expire_attributes(None)
        self.assertEquals(f.a, "this is a")
        self.assertEquals(f.b, 12)

        f._state.expire_attributes(None)
        f.a = "this is another new a"
        self.assertEquals(f.a, "this is another new a")
        self.assertEquals(f.b, 12)

        f._state.expire_attributes(None)
        self.assertEquals(f.a, "this is a")
        self.assertEquals(f.b, 12)

        del f.a
        self.assertEquals(f.a, None)
        self.assertEquals(f.b, 12)

        f._state.commit_all()
        self.assertEquals(f.a, None)
        self.assertEquals(f.b, 12)

    def test_deferred_pickleable(self):
        data = {'a':'this is a', 'b':12}
        def loader(instance, keys):
            for k in keys:
                instance.__dict__[k] = data[k]
            return attributes.ATTR_WAS_SET

        attributes.register_class(MyTest, deferred_scalar_loader=loader)
        attributes.register_attribute(MyTest, 'a', uselist=False, useobject=False)
        attributes.register_attribute(MyTest, 'b', uselist=False, useobject=False)

        m = MyTest()
        m._state.expire_attributes(None)
        assert 'a' not in m.__dict__
        m2 = pickle.loads(pickle.dumps(m))
        assert 'a' not in m2.__dict__
        self.assertEquals(m2.a, "this is a")
        self.assertEquals(m2.b, 12)

    def test_list(self):
        class User(object):pass
        class Address(object):pass

        attributes.register_class(User)
        attributes.register_class(Address)
        attributes.register_attribute(User, 'user_id', uselist = False, useobject=False)
        attributes.register_attribute(User, 'user_name', uselist = False, useobject=False)
        attributes.register_attribute(User, 'addresses', uselist = True, useobject=True)
        attributes.register_attribute(Address, 'address_id', uselist = False, useobject=False)
        attributes.register_attribute(Address, 'email_address', uselist = False, useobject=False)

        u = User()
        u.user_id = 7
        u.user_name = 'john'
        u.addresses = []
        a = Address()
        a.address_id = 10
        a.email_address = 'lala@123.com'
        u.addresses.append(a)

        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')
        u, a._state.commit_all()
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')

        u.user_name = 'heythere'
        a = Address()
        a.address_id = 11
        a.email_address = 'foo@bar.com'
        u.addresses.append(a)
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.addresses[0].email_address == 'lala@123.com' and u.addresses[1].email_address == 'foo@bar.com')

    def test_lazytrackparent(self):
        """test that the "hasparent" flag works properly when lazy loaders and backrefs are used"""

        class Post(object):pass
        class Blog(object):pass
        attributes.register_class(Post)
        attributes.register_class(Blog)

        # set up instrumented attributes with backrefs
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), trackparent=True, useobject=True)

        # create objects as if they'd been freshly loaded from the database (without history)
        b = Blog()
        p1 = Post()
        b._state.set_callable('posts', lambda:[p1])
        p1._state.set_callable('blog', lambda:b)
        p1, b._state.commit_all()

        # no orphans (called before the lazy loaders fire off)
        assert attributes.has_parent(Blog, p1, 'posts', optimistic=True)
        assert attributes.has_parent(Post, b, 'blog', optimistic=True)

        # assert connections
        assert p1.blog is b
        assert p1 in b.posts

        # manual connections
        b2 = Blog()
        p2 = Post()
        b2.posts.append(p2)
        assert attributes.has_parent(Blog, p2, 'posts')
        assert attributes.has_parent(Post, b2, 'blog')

    def test_inheritance(self):
        """tests that attributes are polymorphic"""
        class Foo(object):pass
        class Bar(Foo):pass


        attributes.register_class(Foo)
        attributes.register_class(Bar)

        def func1():
            print "func1"
            return "this is the foo attr"
        def func2():
            print "func2"
            return "this is the bar attr"
        def func3():
            print "func3"
            return "this is the shared attr"
        attributes.register_attribute(Foo, 'element', uselist=False, callable_=lambda o:func1, useobject=True)
        attributes.register_attribute(Foo, 'element2', uselist=False, callable_=lambda o:func3, useobject=True)
        attributes.register_attribute(Bar, 'element', uselist=False, callable_=lambda o:func2, useobject=True)

        x = Foo()
        y = Bar()
        assert x.element == 'this is the foo attr'
        assert y.element == 'this is the bar attr'
        assert x.element2 == 'this is the shared attr'
        assert y.element2 == 'this is the shared attr'

    def test_no_double_state(self):
        states = set()
        class Foo(object):
            def __init__(self):
                states.add(self._state)
        class Bar(Foo):
            def __init__(self):
                states.add(self._state)
                Foo.__init__(self)


        attributes.register_class(Foo)
        attributes.register_class(Bar)

        b = Bar()
        self.assertEquals(len(states), 1)
        self.assertEquals(list(states)[0].obj(), b)


    def test_inheritance2(self):
        """test that the attribute manager can properly traverse the managed attributes of an object,
        if the object is of a descendant class with managed attributes in the parent class"""
        class Foo(object):pass
        class Bar(Foo):pass

        class Element(object):
            _state = True

        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'element', uselist=False, useobject=True)
        el = Element()
        x = Bar()
        x.element = el
        self.assertEquals(attributes.get_history(x._state, 'element'), ([el],[], []))
        x._state.commit_all()

        (added, unchanged, deleted) = attributes.get_history(x._state, 'element')
        assert added == []
        assert unchanged == [el]

    def test_lazyhistory(self):
        """tests that history functions work with lazy-loading attributes"""

        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_class(Bar)

        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]
        def func1():
            return "this is func 1"
        def func2():
            return [bar1, bar2, bar3]

        attributes.register_attribute(Foo, 'col1', uselist=False, callable_=lambda o:func1, useobject=True)
        attributes.register_attribute(Foo, 'col2', uselist=True, callable_=lambda o:func2, useobject=True)
        attributes.register_attribute(Bar, 'id', uselist=False, useobject=True)

        x = Foo()
        x._state.commit_all()
        x.col2.append(bar4)
        self.assertEquals(attributes.get_history(x._state, 'col2'), ([bar4], [bar1, bar2, bar3], []))

    def test_parenttrack(self):
        class Foo(object):pass
        class Bar(object):pass

        attributes.register_class(Foo)
        attributes.register_class(Bar)

        attributes.register_attribute(Foo, 'element', uselist=False, trackparent=True, useobject=True)
        attributes.register_attribute(Bar, 'element', uselist=False, trackparent=True, useobject=True)

        f1 = Foo()
        f2 = Foo()
        b1 = Bar()
        b2 = Bar()

        f1.element = b1
        b2.element = f2

        assert attributes.has_parent(Foo, b1, 'element')
        assert not attributes.has_parent(Foo, b2, 'element')
        assert not attributes.has_parent(Foo, f2, 'element')
        assert attributes.has_parent(Bar, f2, 'element')

        b2.element = None
        assert not attributes.has_parent(Bar, f2, 'element')

        # test that double assignment doesn't accidentally reset the 'parent' flag.
        b3 = Bar()
        f4 = Foo()
        b3.element = f4
        assert attributes.has_parent(Bar, f4, 'element')
        b3.element = f4
        assert attributes.has_parent(Bar, f4, 'element')

    def test_mutablescalars(self):
        """test detection of changes on mutable scalar items"""
        class Foo(object):pass

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'element', uselist=False, copy_function=lambda x:[y for y in x], mutable_scalars=True, useobject=False)
        x = Foo()
        x.element = ['one', 'two', 'three']
        x._state.commit_all()
        x.element[1] = 'five'
        assert x._state.is_modified()

        attributes.unregister_class(Foo)

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'element', uselist=False, useobject=False)
        x = Foo()
        x.element = ['one', 'two', 'three']
        x._state.commit_all()
        x.element[1] = 'five'
        assert not x._state.is_modified()

    def test_descriptorattributes(self):
        """changeset: 1633 broke ability to use ORM to map classes with unusual
        descriptor attributes (for example, classes that inherit from ones
        implementing zope.interface.Interface).
        This is a simple regression test to prevent that defect.
        """
        class des(object):
            def __get__(self, instance, owner): raise AttributeError('fake attribute')

        class Foo(object):
            A = des()


        attributes.unregister_class(Foo)

    def test_collectionclasses(self):

        class Foo(object):pass
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=set, useobject=True)
        assert isinstance(Foo().collection, set)

        attributes.unregister_attribute(Foo, "collection")

        try:
            attributes.register_attribute(Foo, "collection", uselist=True, typecallable=dict, useobject=True)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Type InstrumentedDict must elect an appender method to be a collection class"

        class MyDict(dict):
            def append(self, item):
                self[item.foo] = item
            append = collection.appender(append)
            def remove(self, item):
                del self[item.foo]
            remove = collection.remover(remove)
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyDict, useobject=True)
        assert isinstance(Foo().collection, MyDict)

        attributes.unregister_attribute(Foo, "collection")

        class MyColl(object):pass
        try:
            attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyColl, useobject=True)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Type MyColl must elect an appender method to be a collection class"

        class MyColl(object):
            def __iter__(self):
                return iter([])
            __iter__ = collection.iterator(__iter__)
            def append(self, item):
                pass
            append = collection.appender(append)
            def remove(self, item):
                pass
            remove = collection.remover(remove)
        attributes.register_attribute(Foo, "collection", uselist=True, typecallable=MyColl, useobject=True)
        try:
            Foo().collection
            assert True
        except exceptions.ArgumentError, e:
            assert False


class BackrefTest(TestBase):

    def test_manytomany(self):
        class Student(object):pass
        class Course(object):pass

        attributes.register_class(Student)
        attributes.register_class(Course)
        attributes.register_attribute(Student, 'courses', uselist=True, extension=attributes.GenericBackrefExtension('students'), useobject=True)
        attributes.register_attribute(Course, 'students', uselist=True, extension=attributes.GenericBackrefExtension('courses'), useobject=True)

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
        self.assert_(c.students == [s2,s3])

    def test_onetomany(self):
        class Post(object):pass
        class Blog(object):pass

        attributes.register_class(Post)
        attributes.register_class(Blog)
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), trackparent=True, useobject=True)
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

    def test_onetoone(self):
        class Port(object):pass
        class Jack(object):pass
        attributes.register_class(Port)
        attributes.register_class(Jack)
        attributes.register_attribute(Port, 'jack', uselist=False, extension=attributes.GenericBackrefExtension('port'), useobject=True)
        attributes.register_attribute(Jack, 'port', uselist=False, extension=attributes.GenericBackrefExtension('jack'), useobject=True)
        p = Port()
        j = Jack()
        p.jack = j
        self.assert_(j.port is p)
        self.assert_(p.jack is not None)

        j.port = None
        self.assert_(p.jack is None)

class DeferredBackrefTest(TestBase):
    def setUp(self):
        global Post, Blog, called, lazy_load

        class Post(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name

        class Blog(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.name == self.name

        called = [0]

        lazy_load = []
        def lazy_posts(instance):
            def load():
                called[0] += 1
                return lazy_load
            return load

        attributes.register_class(Post)
        attributes.register_class(Blog)
        attributes.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'), trackparent=True, useobject=True)
        attributes.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'), callable_=lazy_posts, trackparent=True, useobject=True)

    def test_lazy_add(self):
        global lazy_load

        p1, p2, p3 = Post("post 1"), Post("post 2"), Post("post 3")
        lazy_load = [p1, p2, p3]

        b = Blog("blog 1")
        p = Post("post 4")
        p.blog = b
        p = Post("post 5")
        p.blog = b
        # setting blog doesnt call 'posts' callable
        assert called[0] == 0

        # calling backref calls the callable, populates extra posts
        assert b.posts == [p1, p2, p3, Post("post 4"), Post("post 5")]
        assert called[0] == 1

    def test_lazy_remove(self):
        global lazy_load
        called[0] = 0
        lazy_load = []

        b = Blog("blog 1")
        p = Post("post 1")
        p.blog = b
        assert called[0] == 0

        lazy_load = [p]

        p.blog = None
        p2 = Post("post 2")
        p2.blog = b
        assert called[0] == 0
        assert b.posts == [p2]
        assert called[0] == 1

    def test_normal_load(self):
        global lazy_load
        lazy_load = (p1, p2, p3) = [Post("post 1"), Post("post 2"), Post("post 3")]
        called[0] = 0

        b = Blog("blog 1")

        # assign without using backref system
        p2.__dict__['blog'] = b

        assert b.posts == [Post("post 1"), Post("post 2"), Post("post 3")]
        assert called[0] == 1
        p2.blog = None
        p4 = Post("post 4")
        p4.blog = b
        assert b.posts == [Post("post 1"), Post("post 3"), Post("post 4")]
        assert called[0] == 1

        called[0] = 0
        lazy_load = (p1, p2, p3) = [Post("post 1"), Post("post 2"), Post("post 3")]

class HistoryTest(TestBase):
    def test_get_committed_value(self):
        class Foo(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=False, useobject=False)

        f = Foo()
        self.assertEquals(Foo.someattr.impl.get_committed_value(f._state), None)

        f.someattr = 3
        self.assertEquals(Foo.someattr.impl.get_committed_value(f._state), None)

        f = Foo()
        f.someattr = 3
        self.assertEquals(Foo.someattr.impl.get_committed_value(f._state), None)
        
        f._state.commit(['someattr'])
        self.assertEquals(Foo.someattr.impl.get_committed_value(f._state), 3)

    def test_scalar(self):
        class Foo(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=False, useobject=False)

        # case 1.  new object
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))

        f.someattr = "hi"
        self.assertEquals(attributes.get_history(f._state, 'someattr'), (['hi'], [], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], ['hi'], []))

        f.someattr = 'there'

        self.assertEquals(attributes.get_history(f._state, 'someattr'), (['there'], [], ['hi']))
        f._state.commit(['someattr'])

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], ['there'], []))

        del f.someattr
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], ['there']))

        # case 2.  object with direct dictionary settings (similar to a load operation)
        f = Foo()
        f.__dict__['someattr'] = 'new'
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], ['new'], []))

        f.someattr = 'old'
        self.assertEquals(attributes.get_history(f._state, 'someattr'), (['old'], [], ['new']))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], ['old'], []))

        # setting None on uninitialized is currently a change for a scalar attribute
        # no lazyload occurs so this allows overwrite operation to proceed
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))
        f.someattr = None
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([None], [], []))

        f = Foo()
        f.__dict__['someattr'] = 'new'
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], ['new'], []))
        f.someattr = None
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([None], [], ['new']))

    def test_mutable_scalar(self):
        class Foo(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=False, useobject=False, mutable_scalars=True, copy_function=dict)

        # case 1.  new object
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))

        f.someattr = {'foo':'hi'}
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([{'foo':'hi'}], [], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [{'foo':'hi'}], []))
        self.assertEquals(f._state.committed_state['someattr'], {'foo':'hi'})

        f.someattr['foo'] = 'there'
        self.assertEquals(f._state.committed_state['someattr'], {'foo':'hi'})

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([{'foo':'there'}], [], [{'foo':'hi'}]))
        f._state.commit(['someattr'])

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [{'foo':'there'}], []))

        # case 2.  object with direct dictionary settings (similar to a load operation)
        f = Foo()
        f.__dict__['someattr'] = {'foo':'new'}
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [{'foo':'new'}], []))

        f.someattr = {'foo':'old'}
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([{'foo':'old'}], [], [{'foo':'new'}]))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [{'foo':'old'}], []))


    def test_use_object(self):
        class Foo(fixtures.Base):
            pass

        class Bar(fixtures.Base):
            _state = None
            def __nonzero__(self):
                assert False

        hi = Bar(name='hi')
        there = Bar(name='there')
        new = Bar(name='new')
        old = Bar(name='old')

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=False, useobject=True)

        # case 1.  new object
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [None], []))

        f.someattr = hi
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi], []))

        f.someattr = there

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([there], [], [hi]))
        f._state.commit(['someattr'])

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [there], []))

        del f.someattr
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([None], [], [there]))

        # case 2.  object with direct dictionary settings (similar to a load operation)
        f = Foo()
        f.__dict__['someattr'] = new
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new], []))

        f.someattr = old
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([old], [], [new]))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [old], []))

        # setting None on uninitialized is currently not a change for an object attribute
        # (this is different than scalar attribute).  a lazyload has occured so if its
        # None, its really None
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [None], []))
        f.someattr = None
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [None], []))

        f = Foo()
        f.__dict__['someattr'] = new
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new], []))
        f.someattr = None
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([None], [], [new]))

    def test_object_collections_set(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            def __nonzero__(self):
                assert False

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=True, useobject=True)

        hi = Bar(name='hi')
        there = Bar(name='there')
        old = Bar(name='old')
        new = Bar(name='new')

        # case 1.  new object
        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))

        f.someattr = [hi]
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi], []))

        f.someattr = [there]

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([there], [], [hi]))
        f._state.commit(['someattr'])

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [there], []))

        f.someattr = [hi]
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], [there]))

        f.someattr = [old, new]
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([old, new], [], [there]))

        # case 2.  object with direct settings (similar to a load operation)
        f = Foo()
        collection = attributes.init_collection(f, 'someattr')
        collection.append_without_event(new)
        f._state.commit_all()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new], []))

        f.someattr = [old]
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([old], [], [new]))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [old], []))

    def test_dict_collections(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        from sqlalchemy.orm.collections import attribute_mapped_collection

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=True, useobject=True, typecallable=attribute_mapped_collection('name'))

        hi = Bar(name='hi')
        there = Bar(name='there')
        old = Bar(name='old')
        new = Bar(name='new')

        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))

        f.someattr['hi'] = hi
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], []))

        f.someattr['there'] = there
        self.assertEquals(tuple([set(x) for x in attributes.get_history(f._state, 'someattr')]), (set([hi, there]), set([]), set([])))

        f._state.commit(['someattr'])
        self.assertEquals(tuple([set(x) for x in attributes.get_history(f._state, 'someattr')]), (set([]), set([hi, there]), set([])))

    def test_object_collections_mutate(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'someattr', uselist=True, useobject=True)
        attributes.register_attribute(Foo, 'id', uselist=False, useobject=False)

        hi = Bar(name='hi')
        there = Bar(name='there')
        old = Bar(name='old')
        new = Bar(name='new')

        # case 1.  new object
        f = Foo(id=1)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], []))

        f.someattr.append(hi)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi], []))

        f.someattr.append(there)

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([there], [hi], []))
        f._state.commit(['someattr'])

        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi, there], []))

        f.someattr.remove(there)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi], [there]))

        f.someattr.append(old)
        f.someattr.append(new)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([old, new], [hi], [there]))
        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [hi, old, new], []))

        f.someattr.pop(0)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [old, new], [hi]))

        # case 2.  object with direct settings (similar to a load operation)
        f = Foo()
        f.__dict__['id'] = 1
        collection = attributes.init_collection(f, 'someattr')
        collection.append_without_event(new)
        f._state.commit_all()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new], []))

        f.someattr.append(old)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([old], [new], []))

        f._state.commit(['someattr'])
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new, old], []))

        f = Foo()
        collection = attributes.init_collection(f, 'someattr')
        collection.append_without_event(new)
        f._state.commit_all()
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [new], []))

        f.id = 1
        f.someattr.remove(new)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([], [], [new]))

        # case 3.  mixing appends with sets
        f = Foo()
        f.someattr.append(hi)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi], [], []))
        f.someattr.append(there)
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([hi, there], [], []))
        f.someattr = [there]
        self.assertEquals(attributes.get_history(f._state, 'someattr'), ([there], [], []))

    def test_collections_via_backref(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'bars', uselist=True, extension=attributes.GenericBackrefExtension('foo'), trackparent=True, useobject=True)
        attributes.register_attribute(Bar, 'foo', uselist=False, extension=attributes.GenericBackrefExtension('bars'), trackparent=True, useobject=True)

        f1 = Foo()
        b1 = Bar()
        self.assertEquals(attributes.get_history(f1._state, 'bars'), ([], [], []))
        self.assertEquals(attributes.get_history(b1._state, 'foo'), ([], [None], []))

        #b1.foo = f1
        f1.bars.append(b1)
        self.assertEquals(attributes.get_history(f1._state, 'bars'), ([b1], [], []))
        self.assertEquals(attributes.get_history(b1._state, 'foo'), ([f1], [], []))

        b2 = Bar()
        f1.bars.append(b2)
        self.assertEquals(attributes.get_history(f1._state, 'bars'), ([b1, b2], [], []))
        self.assertEquals(attributes.get_history(b1._state, 'foo'), ([f1], [], []))
        self.assertEquals(attributes.get_history(b2._state, 'foo'), ([f1], [], []))

    def test_lazy_backref_collections(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        lazy_load = []
        def lazyload(instance):
            def load():
                return lazy_load
            return load

        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'bars', uselist=True, extension=attributes.GenericBackrefExtension('foo'), trackparent=True, callable_=lazyload, useobject=True)
        attributes.register_attribute(Bar, 'foo', uselist=False, extension=attributes.GenericBackrefExtension('bars'), trackparent=True, useobject=True)

        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]
        lazy_load = [bar1, bar2, bar3]

        f = Foo()
        bar4 = Bar()
        bar4.foo = f
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([bar4], [bar1, bar2, bar3], []))

        lazy_load = None
        f = Foo()
        bar4 = Bar()
        bar4.foo = f
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([bar4], [], []))

        lazy_load = [bar1, bar2, bar3]
        f._state.expire_attributes(['bars'])
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([], [bar1, bar2, bar3], []))

    def test_collections_via_lazyload(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        lazy_load = []
        def lazyload(instance):
            def load():
                return lazy_load
            return load

        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'bars', uselist=True, callable_=lazyload, trackparent=True, useobject=True)

        bar1, bar2, bar3, bar4 = [Bar(id=1), Bar(id=2), Bar(id=3), Bar(id=4)]
        lazy_load = [bar1, bar2, bar3]

        f = Foo()
        f.bars = []
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([], [], [bar1, bar2, bar3]))

        f = Foo()
        f.bars.append(bar4)
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([bar4], [bar1, bar2, bar3], []) )

        f = Foo()
        f.bars.remove(bar2)
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([], [bar1, bar3], [bar2]))
        f.bars.append(bar4)
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([bar4], [bar1, bar3], [bar2]))

        f = Foo()
        del f.bars[1]
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([], [bar1, bar3], [bar2]))

        lazy_load = None
        f = Foo()
        f.bars.append(bar2)
        self.assertEquals(attributes.get_history(f._state, 'bars'), ([bar2], [], []))

    def test_scalar_via_lazyload(self):
        class Foo(fixtures.Base):
            pass

        lazy_load = None
        def lazyload(instance):
            def load():
                return lazy_load
            return load

        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'bar', uselist=False, callable_=lazyload, useobject=False)
        lazy_load = "hi"

        # with scalar non-object, the lazy callable is only executed on gets, not history
        # operations

        f = Foo()
        self.assertEquals(f.bar, "hi")
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([], ["hi"], []))

        f = Foo()
        f.bar = None
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([None], [], []))

        f = Foo()
        f.bar = "there"
        self.assertEquals(attributes.get_history(f._state, 'bar'), (["there"], [], []))
        f.bar = "hi"
        self.assertEquals(attributes.get_history(f._state, 'bar'), (["hi"], [], []))

        f = Foo()
        self.assertEquals(f.bar, "hi")
        del f.bar
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([], [], ["hi"]))
        assert f.bar is None
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([None], [], ["hi"]))

    def test_scalar_object_via_lazyload(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        lazy_load = None
        def lazyload(instance):
            def load():
                return lazy_load
            return load

        attributes.register_class(Foo)
        attributes.register_class(Bar)
        attributes.register_attribute(Foo, 'bar', uselist=False, callable_=lazyload, trackparent=True, useobject=True)
        bar1, bar2 = [Bar(id=1), Bar(id=2)]
        lazy_load = bar1

        # with scalar object, the lazy callable is only executed on gets and history
        # operations

        f = Foo()
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([], [bar1], []))

        f = Foo()
        f.bar = None
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([None], [], [bar1]))

        f = Foo()
        f.bar = bar2
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([bar2], [], [bar1]))
        f.bar = bar1
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([], [bar1], []))

        f = Foo()
        self.assertEquals(f.bar, bar1)
        del f.bar
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([None], [], [bar1]))
        assert f.bar is None
        self.assertEquals(attributes.get_history(f._state, 'bar'), ([None], [], [bar1]))

if __name__ == "__main__":
    testenv.main()
