import sys
sys.path.insert(0, './lib/')

import gc

import random, string

from sqlalchemy.attributes import *

# with this test, run top.  make sure the Python process doenst grow in size arbitrarily.

class User(object):
    pass
        
class Address(object):
    pass

attr_manager = AttributeManager()
attr_manager.register_attribute(User, 'id', uselist=False)
attr_manager.register_attribute(User, 'name', uselist=False)
attr_manager.register_attribute(User, 'addresses', uselist=True)
attr_manager.register_attribute(Address, 'email', uselist=False)
attr_manager.register_attribute(Address, 'user', uselist=False)
        

for i in xrange(1000):
  for j in xrange(1000):
    u = User()
    u.name = str(random.randint(0, 100000000))
    for k in xrange(10):
      a = Address()
      a.email_address = str(random.randint(0, 100000000))
      u.addresses.append(a)
      a.user = u
  print "clearing"
  #managed_attributes.clear()
  gc.collect()
  
  
