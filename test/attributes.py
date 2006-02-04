from testbase import PersistTest
import sqlalchemy.util as util
import sqlalchemy.attributes as attributes
import unittest, sys, os


    
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
        self.assert_(len(u.addresses.unchanged_items()) == 1)

    def testbackref(self):
        class Student(object):pass
        class Course(object):pass
        manager = attributes.AttributeManager()
        manager.register_attribute(Student, 'courses', uselist=True, extension=attributes.GenericBackrefExtension('students'))
        manager.register_attribute(Course, 'students', uselist=True, extension=attributes.GenericBackrefExtension('courses'))
        
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
        
if __name__ == "__main__":
    unittest.main()
