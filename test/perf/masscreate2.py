import testenv; testenv.simple_setup()

import random, string

from sqlalchemy.orm import attributes
from sqlalchemy.test.util import gc_collect

# with this test, run top.  make sure the Python process doenst grow in size arbitrarily.

class User(object):
    pass

class Address(object):
    pass

attributes.register_attribute(User, 'id', False, False)
attributes.register_attribute(User, 'name', False, False)
attributes.register_attribute(User, 'addresses', True, False)
attributes.register_attribute(Address, 'email', False, False)
attributes.register_attribute(Address, 'user', False, False)


for i in xrange(1000):
  for j in xrange(1000):
    u = User()
    attributes.manage(u)
    u.name = str(random.randint(0, 100000000))
    for k in xrange(10):
      a = Address()
      a.email_address = str(random.randint(0, 100000000))
      attributes.manage(a)
      u.addresses.append(a)
      a.user = u
  print "clearing"
  #managed_attributes.clear()
  gc_collect()
