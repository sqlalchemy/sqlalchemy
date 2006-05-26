# UNDER CONSTRUCTION !
# I am just pasting vladimir iliev's test cases here where they will be later assembled into unit tests.

from sqlalchemy import *
from datetime import datetime

metadata = BoundMetaData('sqlite://', echo=True)

products_table = Table('products', metadata,
   Column('product_id', Integer, primary_key=True),
   Column('product_type', String(128)),
   Column('name', String(128)),
   Column('mark', String(128)),
   Column('material', String(128), default=''),
   Column('sortament', String(128), default=''),
   Column('weight', String(128), default=''),
   )


specification_table = Table('specification', metadata,
    Column('spec_line_id', Integer, primary_key=True),
    Column('master_id', Integer, ForeignKey("products.product_id"),
        nullable=True),
    Column('slave_id', Integer, ForeignKey("products.product_id"),
        nullable=True),
    Column('quantity', Float, default=1.),
    )


class Product(object):

    def __init__(self, name, mark=''):
        self.name = name
        self.mark = mark

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)


class Detail(Product):

    def __init__(self, name, mark='', material='', sortament='', weight=''):
        self.name = name
        self.mark = mark
        self.material = material
        self.sortament = sortament
        self.weight = weight


class Assembly(Product): pass


class SpecLine(object):
    
    def __init__(self, master=None, slave=None, quantity=1):
        self.master = master
        self.slave = slave
        self.quantity = quantity
    
    def __repr__(self):
        return '<%s %.01f %s>' % (
            self.__class__.__name__,
            self.quantity or 0.,
            getattr(self.slave, 'name', None)
            )



product_mapper = mapper(Product, products_table,
    polymorphic_on=products_table.c.product_type,
    polymorphic_identity='product')

detail_mapper = mapper(Detail, inherits=product_mapper,
    polymorphic_identity='detail')

assembly_mapper = mapper(Assembly, inherits=product_mapper,
    polymorphic_identity='assembly')

specification_mapper = mapper(SpecLine, specification_table,
    properties=dict(
        master=relation(Assembly,
            foreignkey=specification_table.c.master_id,
            primaryjoin=specification_table.c.master_id==products_table.c.product_id,
            lazy=True, backref=backref('specification', primaryjoin=specification_table.c.master_id==products_table.c.product_id), uselist=False),
        slave=relation(Product, 
            foreignkey=specification_table.c.slave_id,
            primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
            lazy=True, uselist=False),
        quantity=specification_table.c.quantity,
        )
    )


metadata.create_all()
session = create_session(echo_uow=True)


a1 = Assembly(name='a1')

p1 = Product(name='p1')
a1.specification.append(SpecLine(slave=p1))

d1 = Detail(name='d1')
a1.specification.append(SpecLine(slave=d1))

session.save(a1)

session.flush()
session.clear()

a1 = session.query(Product).get_by(name='a1')
print a1
print a1.specification

# ==========================================================================================

from sqlalchemy import *

metadata = BoundMetaData('sqlite://', echo=True)

products_table = Table('products', metadata,
   Column('product_id', Integer, primary_key=True),
   Column('product_type', String(128)),
   Column('name', String(128)),
   )

specification_table = Table('specification', metadata,
    Column('spec_line_id', Integer, primary_key=True),
    Column('slave_id', Integer, ForeignKey("products.product_id"),
        nullable=True),
    )

class Product(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)

class Detail(Product):
    pass

class SpecLine(object):
    def __init__(self, slave=None):
        self.slave = slave
    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            getattr(self.slave, 'name', None)
            )

product_mapper = mapper(Product, products_table,
    polymorphic_on=products_table.c.product_type,
    polymorphic_identity='product')

detail_mapper = mapper(Detail, inherits=product_mapper,
    polymorphic_identity='detail')

specification_mapper = mapper(SpecLine, specification_table,
    properties=dict(
        slave=relation(Product, 
            foreignkey=specification_table.c.slave_id,
            primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
            lazy=True, uselist=False),
        )
    )

metadata.create_all()
session = create_session(echo_uow=True)

s = SpecLine(slave=Product(name='p1'))
s2 = SpecLine(slave=Detail(name='d1'))
session.save(s)
session.save(s2)
session.flush()
session.clear()
print session.query(SpecLine).select()


# =============================================================================================================================

from sqlalchemy import *
from datetime import datetime


metadata = BoundMetaData('sqlite:///', echo=False)


products_table = Table('products', metadata,
   Column('product_id', Integer, primary_key=True),
   Column('product_type', String(128)),
   Column('name', String(128)),
   Column('mark', String(128)),
   Column('material', String(128), default=''),
   Column('sortament', String(128), default=''),
   Column('weight', String(128), default=''),
   )


specification_table = Table('specification', metadata,
    Column('spec_line_id', Integer, primary_key=True),
    Column('master_id', Integer, ForeignKey("products.product_id"),
        nullable=True),
    Column('slave_id', Integer, ForeignKey("products.product_id"),
        nullable=True),
    Column('quantity', Float, default=1.),
    )


documents_table = Table('documents', metadata,
    Column('document_id', Integer, primary_key=True),
    Column('document_type', String(128)),
    Column('product_id', Integer, ForeignKey('products.product_id')),
    Column('create_date', DateTime, default=lambda:datetime.now()),
    Column('last_updated', DateTime, default=lambda:datetime.now(),
        onupdate=lambda:datetime.now()),
    Column('name', String(128)),
    Column('data', Binary),
    Column('size', Integer, default=0),
    )

metadata.create_all()


class Product(object):
    def __init__(self, name, mark=''):
        self.name = name
        self.mark = mark
    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)
class Detail(Product):
    def __init__(self, name, mark='', material='', sortament='', weight=''):
        self.name = name
        self.mark = mark
        self.material = material
        self.sortament = sortament
        self.weight = weight
class Assembly(Product): pass


class SpecLine(object):
    
    def __init__(self, master=None, slave=None, quantity=1):
        self.master = master
        self.slave = slave
        self.quantity = quantity
    
    def __repr__(self):
        return '<%s %.01f %s>' % (
            self.__class__.__name__,
            self.quantity or 0.,
            getattr(self.slave, 'name', None)
            )


class Document(object):
    def __init__(self, name, data=None):
        self.name = name
        self.data = data
    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)
class RasterDocument(Document): pass


product_mapper = mapper(Product, products_table,
    polymorphic_on=products_table.c.product_type,
    polymorphic_identity='product')
detail_mapper = mapper(Detail, inherits=product_mapper,
    polymorphic_identity='detail')
assembly_mapper = mapper(Assembly, inherits=product_mapper,
    polymorphic_identity='assembly')


specification_mapper = mapper(SpecLine, specification_table,
    properties=dict(
        master=relation(Assembly, lazy=False, uselist=False,
            foreignkey=specification_table.c.master_id,
            primaryjoin=specification_table.c.master_id==products_table.c.product_id,
            backref=backref('specification', primaryjoin=specification_table.c.master_id==products_table.c.product_id),
            ),
        slave=relation(Product, lazy=False,  uselist=False,
            foreignkey=specification_table.c.slave_id,
            primaryjoin=specification_table.c.slave_id==products_table.c.product_id,
            ),
        quantity=specification_table.c.quantity,
        )
    )


document_mapper = mapper(Document, documents_table,
    polymorphic_on=documents_table.c.document_type,
    polymorphic_identity='document',
    properties=dict(
        name=documents_table.c.name,
        data=deferred(documents_table.c.data),
        product=relation(Product, lazy=True, backref='documents'),
        ),
    )
raster_document_mapper = mapper(RasterDocument, inherits=document_mapper,
    polymorphic_identity='raster_document')


assembly_mapper.add_property('specification',
    relation(SpecLine, lazy=True,
        primaryjoin=specification_table.c.master_id==products_table.c.product_id,
        backref='master', cascade='all, delete-orphan',
        )
    )


# bug #1
# the property must be added to all the mapers individually, else delete-orphan doesnt work
for m in (product_mapper, assembly_mapper, detail_mapper):
    m.add_property('documents',
        relation(Document, lazy=True,
            backref='product', cascade='all, delete-orphan'),
        )


session = create_session()


a1 = Assembly(name='a1')
a1.specification.append(SpecLine(slave=Detail(name='d1')))
a1.documents.append(Document('doc1'))
a1.documents.append(RasterDocument('doc2')) # bug #2
session.save(a1)
session.flush()
session.clear()
del a1


a1 = session.query(Product).get_by(name='a1')
print a1.documents


# ==============================================================================================================================