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

        
if __name__ == "__main__":
    unittest.main()
