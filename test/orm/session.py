from testbase import AssertMixin
import testbase
import unittest, sys, datetime

import tables
from tables import *

db = testbase.db
from sqlalchemy import *


class SessionTest(AssertMixin):

    def setUpAll(self):
        db.echo = False
        tables.create()
        tables.data()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        tables.drop()
        db.echo = testbase.echo
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass

    def test_orphan(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()
        a = Address()
        try:
            s.save(a)
        except exceptions.InvalidRequestError, e:
            pass
        s.flush()
        assert a.address_id is None, "Error: address should not be persistent"
        
    def test_delete_new_object(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()

        u = User()
        s.save(u)
        a = Address()
        assert a not in s.new
        u.addresses.append(a)
        u.addresses.remove(a)
        s.delete(u)
        s.flush() # (erroneously) causes "a" to be persisted
        assert u.user_id is None, "Error: user should not be persistent"
        assert a.address_id is None, "Error: address should not be persistent"


if __name__ == "__main__":    
    testbase.main()
