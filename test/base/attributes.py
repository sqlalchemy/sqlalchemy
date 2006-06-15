from testbase import PersistTest
import sqlalchemy.util as util
import sqlalchemy.attributes as attributes
import unittest, sys, os
import pickle


class MyTest(object):pass
    
class AttributesTest(PersistTest):
    """tests for the attributes.py module, which deals with tracking attribute changes on an object."""
    def testbasic(self):
        class User(object):pass
        manager = attributes.AttributeManager()
        manager.register_attribute(User, 'user_id', uselist = False)
        manager.register_attribute(User, 'user_name', uselist = False)
        manager.register_attribute(User, 'email_address', uselist = False)
        
        u = User()
        print repr(u.__dict__)
        
        u.user_id = 7
        u.user_name = 'john'
        u.email_address = 'lala@123.com'
        
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')
        manager.commit(u)
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

        u.user_name = 'heythere'
        u.email_address = 'foo@bar.com'
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.email_address == 'foo@bar.com')
        
        manager.rollback(u)
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.email_address == 'lala@123.com')

    def testpickleness(self):
        manager = attributes.AttributeManager()
        manager.register_attribute(MyTest, 'user_id', uselist = False)
        manager.register_attribute(MyTest, 'user_name', uselist = False)
        manager.register_attribute(MyTest, 'email_address', uselist = False)
        x = MyTest()
        x.user_id=7
        s = pickle.dumps(x)
        x2 = pickle.loads(s)
        assert s == pickle.dumps(x2)

    def testlist(self):
        class User(object):pass
        class Address(object):pass
        manager = attributes.AttributeManager()
        manager.register_attribute(User, 'user_id', uselist = False)
        manager.register_attribute(User, 'user_name', uselist = False)
        manager.register_attribute(User, 'addresses', uselist = True)
        manager.register_attribute(Address, 'address_id', uselist = False)
        manager.register_attribute(Address, 'email_address', uselist = False)
        
        u = User()
        print repr(u.__dict__)

        u.user_id = 7
        u.user_name = 'john'
        u.addresses = []
        a = Address()
        a.address_id = 10
        a.email_address = 'lala@123.com'
        u.addresses.append(a)

        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')
        manager.commit(u, a)
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')

        u.user_name = 'heythere'
        a = Address()
        a.address_id = 11
        a.email_address = 'foo@bar.com'
        u.addresses.append(a)
        print repr(u.__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'heythere' and u.addresses[0].email_address == 'lala@123.com' and u.addresses[1].email_address == 'foo@bar.com')

        manager.rollback(u, a)
        print repr(u.__dict__)
        print repr(u.addresses[0].__dict__)
        self.assert_(u.user_id == 7 and u.user_name == 'john' and u.addresses[0].email_address == 'lala@123.com')
        self.assert_(len(manager.get_history(u, 'addresses').unchanged_items()) == 1)

    def testbackref(self):
        class Student(object):pass
        class Course(object):pass
        manager = attributes.AttributeManager()
        manager.register_attribute(Student, 'courses', uselist=True, extension=attributes.GenericBackrefExtension('students'))
        manager.register_attribute(Course, 'students', uselist=True, extension=attributes.GenericBackrefExtension('courses'))
        
        s = Student()
        c = Course()
        s.courses.append(c)
        print c.students
        print [s]
        self.assert_(c.students == [s])
        s.courses.remove(c)
        self.assert_(c.students == [])
        
        (s1, s2, s3) = (Student(), Student(), Student())
        c.students = [s1, s2, s3]
        self.assert_(s2.courses == [c])
        self.assert_(s1.courses == [c])
        print "--------------------------------"
        print s1
        print s1.courses
        print c
        print c.students
        s1.courses.remove(c)
        self.assert_(c.students == [s2,s3])
        
        
        class Post(object):pass
        class Blog(object):pass
        
        manager.register_attribute(Post, 'blog', uselist=False, extension=attributes.GenericBackrefExtension('posts'))
        manager.register_attribute(Blog, 'posts', uselist=True, extension=attributes.GenericBackrefExtension('blog'))
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


        class Port(object):pass
        class Jack(object):pass
        manager.register_attribute(Port, 'jack', uselist=False, extension=attributes.GenericBackrefExtension('port'))
        manager.register_attribute(Jack, 'port', uselist=False, extension=attributes.GenericBackrefExtension('jack'))
        p = Port()
        j = Jack()
        p.jack = j
        self.assert_(j.port is p)
        self.assert_(p.jack is not None)
        
        j.port = None
        self.assert_(p.jack is None)

    def testinheritance(self):
        """tests that attributes are polymorphic"""
        class Foo(object):pass
        class Bar(Foo):pass
        
        manager = attributes.AttributeManager()
        
        def func1():
            print "func1"
            return "this is the foo attr"
        def func2():
            print "func2"
            return "this is the bar attr"
        def func3():
            print "func3"
            return "this is the shared attr"
        manager.register_attribute(Foo, 'element', uselist=False, callable_=lambda o:func1)
        manager.register_attribute(Foo, 'element2', uselist=False, callable_=lambda o:func3)
        manager.register_attribute(Bar, 'element', uselist=False, callable_=lambda o:func2)
        
        x = Foo()
        y = Bar()
        assert x.element == 'this is the foo attr'
        assert y.element == 'this is the bar attr'
        assert x.element2 == 'this is the shared attr'
        assert y.element2 == 'this is the shared attr'

    def testlazyhistory(self):
        """tests that history functions work with lazy-loading attributes"""
        class Foo(object):pass
        class Bar(object):
            def __init__(self, id):
                self.id = id
            def __repr__(self):
                return "Bar: id %d" % self.id
                
        manager = attributes.AttributeManager()

        def func1():
            return "this is func 1"
        def func2():
            return [Bar(1), Bar(2), Bar(3)]

        manager.register_attribute(Foo, 'col1', uselist=False, callable_=lambda o:func1)
        manager.register_attribute(Foo, 'col2', uselist=True, callable_=lambda o:func2)
        manager.register_attribute(Bar, 'id', uselist=False)

        x = Foo()
        manager.commit(x)
        x.col2.append(Bar(4))
        h = manager.get_history(x, 'col2')
        print h.added_items()
        print h.unchanged_items()

        
    def testparenttrack(self):    
        class Foo(object):pass
        class Bar(object):pass
        
        manager = attributes.AttributeManager()
        
        manager.register_attribute(Foo, 'element', uselist=False, trackparent=True)
        manager.register_attribute(Bar, 'element', uselist=False, trackparent=True)
        
        f1 = Foo()
        f2 = Foo()
        b1 = Bar()
        b2 = Bar()
        
        f1.element = b1
        b2.element = f2
        
        assert manager.get_history(f1, 'element').hasparent(b1)
        assert not manager.get_history(f1, 'element').hasparent(b2)
        assert not manager.get_history(f1, 'element').hasparent(f2)
        assert manager.get_history(b2, 'element').hasparent(f2)
        
        b2.element = None
        assert not manager.get_history(b2, 'element').hasparent(f2)
        
if __name__ == "__main__":
    unittest.main()
