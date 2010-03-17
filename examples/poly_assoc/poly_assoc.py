"""
"polymorphic" associations, ala ActiveRecord.

In this example, we are specifically targeting this ActiveRecord functionality:

http://wiki.rubyonrails.org/rails/pages/UnderstandingPolymorphicAssociations

The term "polymorphic" here means "object X can be referenced by objects A, B, and C,
along a common line of association".

In this example we illustrate the relationship in both directions.
A little bit of property magic is used to smooth the edges.

AR creates this relationship in such a way that disallows
any foreign key constraint from existing on the association.
For a different way of doing this,  see
poly_assoc_fks.py.  The interface is the same, the efficiency is more or less the same,
but foreign key constraints may be used.  That example also better separates
the associated target object from those which associate with it.

"""

from sqlalchemy import MetaData, Table, Column, Integer, String, and_
from sqlalchemy.orm import (mapper, relationship, create_session, class_mapper,
    backref)

metadata = MetaData('sqlite://')

#######
# addresses table, class, 'addressable interface'.

addresses = Table("addresses", metadata,
    Column('id', Integer, primary_key=True),
    Column('addressable_id', Integer),
    Column('addressable_type', String(50)),
    Column('street', String(100)),
    Column('city', String(50)),
    Column('country', String(50))
    )

class Address(object):
    def __init__(self, type):
        self.addressable_type = type
    member = property(lambda self: getattr(self, '_backref_%s' % self.addressable_type))

def addressable(cls, name, uselist=True):
    """addressable 'interface'.

    if you really wanted to make a "generic" version of this function, it's straightforward.
    """

    # create_address function, imitaes the rails example.
    # we could probably use property tricks as well to set
    # the Address object's "addressabletype" attribute.
    def create_address(self):
        a = Address(table.name)
        if uselist:
            getattr(self, name).append(a)
        else:
            setattr(self, name, a)
        return a

    mapper = class_mapper(cls)
    table = mapper.local_table
    cls.create_address = create_address
    # no constraints.  therefore define constraints in an ad-hoc fashion.
    primaryjoin = and_(
            list(table.primary_key)[0] == addresses.c.addressable_id,
            addresses.c.addressable_type == table.name
     )
    foreign_keys = [addresses.c.addressable_id]
    mapper.add_property(name, relationship(
            Address,
            primaryjoin=primaryjoin, uselist=uselist, foreign_keys=foreign_keys,
            backref=backref('_backref_%s' % table.name, primaryjoin=list(table.primary_key)[0] == addresses.c.addressable_id, foreign_keys=foreign_keys)
        )
    )

mapper(Address, addresses)

######
# sample # 1, users

users = Table("users", metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50), nullable=False)
    )

class User(object):
    pass

mapper(User, users)
addressable(User, 'addresses', uselist=True)

######
# sample # 2, orders

orders = Table("orders", metadata,
    Column('id', Integer, primary_key=True),
    Column('description', String(50), nullable=False))

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

a1 = u1.create_address()
a1.street = '123 anywhere street'
a2 = u1.create_address()
a2.street = '345 orchard ave'

a3 = o1.create_address()
a3.street = '444 park ave.'

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



