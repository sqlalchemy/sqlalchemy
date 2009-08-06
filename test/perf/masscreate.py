# times how long it takes to create 26000 objects
import testenv; testenv.simple_setup()

from sqlalchemy.orm import attributes
import time

manage_attributes = True
init_attributes = manage_attributes and True

class User(object):
    pass
class Address(object):
    pass

if manage_attributes:
    attributes.register_attribute(User, 'id', False, False)
    attributes.register_attribute(User, 'name', False, False)
    attributes.register_attribute(User, 'addresses', True, False, trackparent=True)
    attributes.register_attribute(Address, 'email', False, False)

now = time.time()
for i in range(0,130):
    u = User()
    if init_attributes:
        attributes.manage(u)
    u.id = i
    u.name = "user " + str(i)
    if not manage_attributes:
        u.addresses = []
    for j in range(0,200):
        a = Address()
        if init_attributes:
            attributes.manage(a)
        a.email = 'foo@bar.com'
        u.addresses.append(a)
#    print len(managed_attributes)
#    managed_attributes.clear()
total = time.time() - now
print "Total time", total
