# times how long it takes to create 26000 objects
import sys
sys.path.insert(0, './lib/')

from sqlalchemy.attributes import *
import time
import gc

manage_attributes = True
init_attributes = manage_attributes and True

class User(object):
    pass
class Address(object):
    pass

attr_manager = AttributeManager()
if manage_attributes:
    attr_manager.register_attribute(User, 'id', uselist=False)
    attr_manager.register_attribute(User, 'name', uselist=False)
    attr_manager.register_attribute(User, 'addresses', uselist=True, trackparent=True)
    attr_manager.register_attribute(Address, 'email', uselist=False)

now = time.time()
for i in range(0,130):
    u = User()
    if init_attributes:
        attr_manager.init_attr(u)
    u.id = i
    u.name = "user " + str(i)
    if not manage_attributes:
        u.addresses = []
    for j in range(0,200):
        a = Address()
        if init_attributes:
            attr_manager.init_attr(a)
        a.email = 'foo@bar.com'
        u.addresses.append(a)
#    gc.collect()
#    print len(managed_attributes)
#    managed_attributes.clear()
total = time.time() - now
print "Total time", total
