"""
"polymorphic" associations, ala SQLAlchemy.

This example generalizes the function in poly_assoc_pk.py into a
function "association" which creates a new polymorphic association
"interface".
"""

from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relationship, create_session, class_mapper

metadata = MetaData('sqlite://')

def association(cls, table):
    """create an association 'interface'."""

    interface_name = table.name
    attr_name = "%s_rel" % interface_name

    metadata = table.metadata
    association_table = Table("%s_associations" % interface_name, metadata,
        Column('assoc_id', Integer, primary_key=True),
        Column('type', String(50), nullable=False)
    )

    class GenericAssoc(object):
        def __init__(self, name):
            self.type = name

    def interface(cls, name, uselist=True):

        mapper = class_mapper(cls)
        table = mapper.local_table
        mapper.add_property(attr_name, relationship(GenericAssoc, backref='_backref_%s' % table.name))

        if uselist:
            # list based property decorator
            def get(self):
                if getattr(self, attr_name) is None:
                    setattr(self, attr_name, GenericAssoc(table.name))
                return getattr(self, attr_name).targets
            setattr(cls, name, property(get))
        else:
            # scalar based property decorator
            def get(self):
                return getattr(self, attr_name).targets[0]
            def set(self, value):
                if getattr(self, attr_name) is None:
                    setattr(self, attr_name, GenericAssoc(table.name))
                getattr(self, attr_name).targets = [value]
            setattr(cls, name, property(get, set))

    setattr(cls, 'member', property(lambda self: getattr(self.association, '_backref_%s' % self.association.type)))

    mapper(GenericAssoc, association_table, properties={
        'targets':relationship(cls, backref='association'),
    })

    return interface


#######
# addresses table

addresses = Table("addresses", metadata,
    Column('id', Integer, primary_key=True),
    Column('assoc_id', None, ForeignKey('addresses_associations.assoc_id')),
    Column('street', String(100)),
    Column('city', String(50)),
    Column('country', String(50))
    )

class Address(object):
    pass

# create "addressable" association
addressable = association(Address, addresses)

mapper(Address, addresses)


######
# sample # 1, users

users = Table("users", metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50), nullable=False),
    Column('assoc_id', None, ForeignKey('addresses_associations.assoc_id'))
    )

class User(object):
    pass

mapper(User, users)

# use the association
addressable(User, 'addresses', uselist=True)

######
# sample # 2, orders

orders = Table("orders", metadata,
    Column('id', Integer, primary_key=True),
    Column('description', String(50), nullable=False),
    Column('assoc_id', None, ForeignKey('addresses_associations.assoc_id'))
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
