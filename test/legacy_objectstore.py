from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables

install_mods('legacy_session')


class LegacySessionTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        users.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        users.drop()
        db.echo = testbase.echo
    def setUp(self):
        objectstore.get_session().clear()
        clear_mappers()
        tables.user_data()
        #db.echo = "debug"
    def tearDown(self):
        tables.delete_user_data()
        
    def test_nested_begin_commit(self):
        """tests that nesting objectstore transactions with multiple commits
        affects only the outermost transaction"""
        class User(object):pass
        m = mapper(User, users)
        def name_of(id):
            return users.select(users.c.user_id == id).execute().fetchone().user_name
        name1 = "Oliver Twist"
        name2 = 'Mr. Bumble'
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        s = objectstore.get_session()
        trans = s.begin()
        trans2 = s.begin()
        m.get(7).user_name = name1
        trans3 = s.begin()
        m.get(8).user_name = name2
        trans3.commit()
        s.commit() # should do nothing
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans2.commit()
        s.commit()  # should do nothing
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans.commit()
        self.assert_(name_of(7) == name1, msg="user_name should be %s" % name1)
        self.assert_(name_of(8) == name2, msg="user_name should be %s" % name2)

    def test_nested_rollback(self):
        """tests that nesting objectstore transactions with a rollback inside
        affects only the outermost transaction"""
        class User(object):pass
        m = mapper(User, users)
        def name_of(id):
            return users.select(users.c.user_id == id).execute().fetchone().user_name
        name1 = "Oliver Twist"
        name2 = 'Mr. Bumble'
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        s = objectstore.get_session()
        trans = s.begin()
        trans2 = s.begin()
        m.get(7).user_name = name1
        trans3 = s.begin()
        m.get(8).user_name = name2
        trans3.rollback()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans2.commit()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans.commit()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)

    def test_true_nested(self):
        """tests creating a new Session inside a database transaction, in 
        conjunction with an engine-level nested transaction, which uses
        a second connection in order to achieve a nested transaction that commits, inside
        of another engine session that rolls back."""
#        testbase.db.echo='debug'
        class User(object):
            pass
        testbase.db.begin()
        try:
            m = mapper(User, users)
            name1 = "Oliver Twist"
            name2 = 'Mr. Bumble'
            m.get(7).user_name = name1
            s = objectstore.Session(nest_on=testbase.db)
            m.using(s).get(8).user_name = name2
            s.commit()
            objectstore.commit()
            testbase.db.rollback()
        except:
            testbase.db.rollback()
            raise
        objectstore.clear()
        self.assert_(m.get(8).user_name == name2)
        self.assert_(m.get(7).user_name != name1)

if __name__ == "__main__":
    testbase.main()        
