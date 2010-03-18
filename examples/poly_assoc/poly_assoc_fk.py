"""
"polymorphic" associations, ala SQLAlchemy.

See "poly_assoc.py" for an imitation of this functionality as implemented
in ActiveRecord.

Here, we build off the previous example, adding an association table
that allows the relationship to be expressed as a many-to-one from the
"model" object to its "association", so that each model table bears the foreign
key constraint.  This allows the same functionality via traditional
normalized form with full constraints.  It also isolates the target
associated object from its method of being associated, allowing greater
flexibility in its usage.

As in the previous example, a little bit of property magic is used
to smooth the edges.

For a more genericized version of this example, see
poly_assoc_generic.py.
"""

from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relationship, create_session, class_mapper

metadata = MetaData('sqlite://')

#######
# addresses table, class, 'addressable interface'.

addresses = Table("addresses", metadata,
    Column('id', Integer, primary_key=True),
    Column('assoc_id', None, ForeignKey('address_associations.assoc_id')),
    Column('street', String(100)),
    Column('city', String(50)),
    Column('country', String(50))
    )

## association table
address_associations = Table("address_associations", metadata,
    Column('assoc_id', Integer, primary_key=True),
    Column('type', String(50), nullable=False)
)

class Address(object):
    member = property(lambda self: getattr(self.association, '_backref_%s' % self.association.type))

class AddressAssoc(object):
    def __init__(self, name):
        self.type = name

def addressable(cls, name, uselist=True):
    """addressable 'interface'.

    we create this function here to imitate the style used in poly_assoc.py.

    """
    mapper = class_mapper(cls)
    table = mapper.local_table
    mapper.add_property('address_rel', relationship(AddressAssoc, backref='_backref_%s' % table.name))

    if uselist:
        # list based property decorator
        def get(self):
            if self.address_rel is None:
                self.address_rel = AddressAssoc(table.name)
            return self.address_rel.addresses
        setattr(cls, name, property(get))
    else:
        # scalar based property decorator
        def get(self):
            return self.address_rel.addresses[0]
        def set(self, value):
            if self.address_rel is None:
                self.address_rel = AddressAssoc(table.name)
            self.address_rel.addresses = [value]
        setattr(cls, name, property(get, set))

mapper(Address, addresses)

mapper(AddressAssoc, address_associations, properties={
    'addresses':relationship(Address, backref='association'),
})

######
# sample # 1, users

users = Table("users", metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50), nullable=False),
    # this column ties the users table into the address association
    Column('assoc_id', None, ForeignKey('address_associations.assoc_id'))
    )

class User(object):
    pass

mapper(User, users)
addressable(User, 'addresses', uselist=True)

######
# sample # 2, orders

orders = Table("orders", metadata,
    Column('id', Integer, primary_key=True),
    Column('description', String(50), nullable=False),
    # this column ties the orders table into the address association
    Column('assoc_id', None, ForeignKey('address_associations.assoc_id'))
    )

class Order(object):
    pass

mapper(Order, orders)
addressable(Order, 'address', uselist=False)

######
# use it !
metadata.create_all()

u1 = User()
u1.name = 'bob'

o1 = Order()
o1.description = 'order 1'

# note we can just create an Address object freely.
# if you want a create_address() function, just stick it on the class.
a1 = Address()
u1.addresses.append(a1)
a1.street = '123 anywhere street'

a2 = Address()
u1.addresses.append(a2)
a2.street = '345 orchard ave'

o1.address = Address()
o1.address.street = '444 park ave.'

sess = create_session()
sess.add(u1)
sess.add(o1)
sess.flush()

sess.expunge_all()

# query objects, get their addresses

bob = sess.query(User).filter_by(name='bob').one()
assert [s.street for s in bob.addresses] == ['123 anywhere street', '345 orchard ave']

order = sess.query(Order).filter_by(description='order 1').one()
assert order.address.street == '444 park ave.'

# query from Address to members

for address in sess.query(Address).all():
    print "Street", address.street, "Member", address.member
