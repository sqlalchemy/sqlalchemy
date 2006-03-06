from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
import datetime

db = testbase.db

class EagerTest(AssertMixin):
    def setUpAll(self):
        objectstore.clear()
        clear_mappers()
        testbase.db.tables.clear()
        
        global companies_table, addresses_table, invoice_table, phones_table, items_table

        companies_table = Table('companies', db,
            Column('company_id', Integer, Sequence('company_id_seq', optional=True), primary_key = True),
            Column('company_name', String(40)),

        )
        
        addresses_table = Table('addresses', db,
                                Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
                                Column('company_id', Integer, ForeignKey("companies.company_id")),
                                Column('address', String(40)),
                                )

        phones_table = Table('phone_numbers', db,
                                Column('phone_id', Integer, Sequence('phone_id_seq', optional=True), primary_key = True),
                                Column('address_id', Integer, ForeignKey('addresses.address_id')),
                                Column('type', String(20)),
                                Column('number', String(10)),
                                )

        invoice_table = Table('invoices', db,
                              Column('invoice_id', Integer, Sequence('invoice_id_seq', optional=True), primary_key = True),
                              Column('company_id', Integer, ForeignKey("companies.company_id")),
                              Column('date', DateTime),   
                              )

        items_table = Table('items', db,
                            Column('item_id', Integer, Sequence('item_id_seq', optional=True), primary_key = True),
                            Column('invoice_id', Integer, ForeignKey('invoices.invoice_id')),
                            Column('code', String(20)),
                            Column('qty', Integer),
                            )

        companies_table.create()
        addresses_table.create()
        phones_table.create()
        invoice_table.create()
        items_table.create()
        
    def tearDownAll(self):
        items_table.drop()
        invoice_table.drop()
        phones_table.drop()
        addresses_table.drop()
        companies_table.drop()

    def tearDown(self):
        objectstore.clear()
        clear_mappers()
        items_table.delete().execute()
        invoice_table.delete().execute()
        phones_table.delete().execute()
        addresses_table.delete().execute()
        companies_table.delete().execute()

    def testone(self):
        """tests eager load of a many-to-one attached to a one-to-many.  this testcase illustrated 
        the bug, which is that when the single Company is loaded, no further processing of the rows
        occurred in order to load the Company's second Address object."""
        class Company(object):
            def __init__(self):
                self.company_id = None
            def __repr__(self):
                return "Company:" + repr(getattr(self, 'company_id', None)) + " " + repr(getattr(self, 'company_name', None)) + " " + str([repr(addr) for addr in self.addresses])

        class Address(object):
            def __repr__(self):
                return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'company_id', None)) + " " + repr(self.address)

        class Invoice(object):
            def __init__(self):
                self.invoice_id = None
            def __repr__(self):
                return "Invoice:" + repr(getattr(self, 'invoice_id', None)) + " " + repr(getattr(self, 'date', None))  + " " + repr(self.company)

        Address.mapper = mapper(Address, addresses_table, properties={
            })
        Company.mapper = mapper(Company, companies_table, properties={
            'addresses' : relation(Address.mapper, lazy=False),
            })
        Invoice.mapper = mapper(Invoice, invoice_table, properties={
            'company': relation(Company.mapper, lazy=False, )
            })

        c1 = Company()
        c1.company_name = 'company 1'
        a1 = Address()
        a1.address = 'a1 address'
        c1.addresses.append(a1)
        a2 = Address()
        a2.address = 'a2 address'
        c1.addresses.append(a2)
        i1 = Invoice()
        i1.date = datetime.datetime.now()
        i1.company = c1

        
        objectstore.commit()

        company_id = c1.company_id
        invoice_id = i1.invoice_id

        objectstore.clear()

        c = Company.mapper.get(company_id)

        objectstore.clear()

        i = Invoice.mapper.get(invoice_id)

        self.echo(repr(c))
        self.echo(repr(i.company))
        self.assert_(repr(c) == repr(i.company))

    def testtwo(self):
        """this is the original testcase that includes various complicating factors"""
        class Company(object):
            def __init__(self):
                self.company_id = None
            def __repr__(self):
                return "Company:" + repr(getattr(self, 'company_id', None)) + " " + repr(getattr(self, 'company_name', None)) + " " + str([repr(addr) for addr in self.addresses])

        class Address(object):
            def __repr__(self):
                return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'company_id', None)) + " " + repr(self.address) + str([repr(ph) for ph in self.phones])

        class Phone(object):
            def __repr__(self):
                return "Phone: " + repr(getattr(self, 'phone_id', None)) + " " + repr(getattr(self, 'address_id', None)) + " " + repr(self.type) + " " + repr(self.number)

        class Invoice(object):
            def __init__(self):
                self.invoice_id = None
            def __repr__(self):
                return "Invoice:" + repr(getattr(self, 'invoice_id', None)) + " " + repr(getattr(self, 'date', None))  + " " + repr(self.company) + " " + str([repr(item) for item in self.items])

        class Item(object):
            def __repr__(self):
                return "Item: " + repr(getattr(self, 'item_id', None)) + " " + repr(getattr(self, 'invoice_id', None)) + " " + repr(self.code) + " " + repr(self.qty)

        Phone.mapper = mapper(Phone, phones_table, is_primary=True)

        Address.mapper = mapper(Address, addresses_table, properties={
            'phones': relation(Phone.mapper, lazy=False, backref='address')
            })

        Company.mapper = mapper(Company, companies_table, properties={
            'addresses' : relation(Address.mapper, lazy=False, backref='company'),
            })

        Item.mapper = mapper(Item, items_table, is_primary=True)

        Invoice.mapper = mapper(Invoice, invoice_table, properties={
            'items': relation(Item.mapper, lazy=False, backref='invoice'),
            'company': relation(Company.mapper, lazy=False, backref='invoices')
            })

        objectstore.clear()
        c1 = Company()
        c1.company_name = 'company 1'

        a1 = Address()
        a1.address = 'a1 address'

        p1 = Phone()
        p1.type = 'home'
        p1.number = '1111'

        a1.phones.append(p1)

        p2 = Phone()
        p2.type = 'work'
        p2.number = '22222'
        a1.phones.append(p2)

        c1.addresses.append(a1)

        a2 = Address()
        a2.address = 'a2 address'

        p3 = Phone()
        p3.type = 'home'
        p3.number = '3333'
        a2.phones.append(p3)

        p4 = Phone()
        p4.type = 'work'
        p4.number = '44444'
        a2.phones.append(p4)

        c1.addresses.append(a2)

        objectstore.commit()

        company_id = c1.company_id
        
        objectstore.clear()

        a = Company.mapper.get(company_id)
        self.echo(repr(a))

        # set up an invoice
        i1 = Invoice()
        i1.date = datetime.datetime.now()
        i1.company = c1

        item1 = Item()
        item1.code = 'aaaa'
        item1.qty = 1
        item1.invoice = i1

        item2 = Item()
        item2.code = 'bbbb'
        item2.qty = 2
        item2.invoice = i1

        item3 = Item()
        item3.code = 'cccc'
        item3.qty = 3
        item3.invoice = i1

        objectstore.commit()

        invoice_id = i1.invoice_id

        objectstore.clear()

        c = Company.mapper.get(company_id)
        self.echo(repr(c))

        objectstore.clear()

        i = Invoice.mapper.get(invoice_id)
        self.echo(repr(i))

        self.assert_(repr(i.company) == repr(c))
        
if __name__ == "__main__":    
    testbase.main()
