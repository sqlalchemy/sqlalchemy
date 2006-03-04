from testbase import PersistTest
import sqlalchemy.util as util
import unittest, sys, os
import sqlalchemy.schema as schema
import testbase
from sqlalchemy import *
import sqlalchemy

db = testbase.db

class DefaultTest(PersistTest):

    def setUpAll(self):
        global t, f, ts
        x = {'x':50}
        def mydefault():
            x['x'] += 1
            return x['x']

        use_function_defaults = db.engine.name == 'postgres' or db.engine.name == 'oracle'
        is_oracle = db.engine.name == 'oracle'
 
        # select "count(1)" from the DB which returns different results
        # on different DBs
        if is_oracle:
            f = select([func.count(1) + 5], engine=db, from_obj=['DUAL']).scalar()
            ts = select([func.sysdate()], engine=db, from_obj=['DUAL']).scalar()
            def1 = func.sysdate()
            def2 = text("sysdate")
            deftype = Date
        elif use_function_defaults:
            f = select([func.count(1) + 5], engine=db).scalar()
            def1 = func.current_date()
            def2 = text("current_date")
            deftype = Date
            ts = select([func.current_date()], engine=db).scalar()
        else:
            f = select([func.count(1) + 5], engine=db).scalar()
            def1 = def2 = "3"
            ts = 3
            deftype = Integer
            
        t = Table('default_test1', db,
            # python function
            Column('col1', Integer, primary_key=True, default=mydefault),
            
            # python literal
            Column('col2', String(20), default="imthedefault"),
            
            # preexecute expression
            Column('col3', Integer, default=func.count(1) + 5),
            
            # SQL-side default from sql expression
            Column('col4', deftype, PassiveDefault(def1)),
            
            # SQL-side default from literal expression
            Column('col5', deftype, PassiveDefault(def2))
        )
        t.create()

    def teststandalonedefaults(self):
        x = t.c.col1.default.execute()
        y = t.c.col2.default.execute()
        z = t.c.col3.default.execute()
        self.assert_(50 <= x <= 57)
        self.assert_(y == 'imthedefault')
        self.assert_(z == 6)
        
    def testinsertdefaults(self):
        t.insert().execute()
        self.assert_(t.engine.lastrow_has_defaults())
        t.insert().execute()
        t.insert().execute()
    
        l = t.select().execute()
        self.assert_(l.fetchall() == [(51, 'imthedefault', f, ts, ts), (52, 'imthedefault', f, ts, ts), (53, 'imthedefault', f, ts, ts)])

    def tearDownAll(self):
        t.drop()

class SequenceTest(PersistTest):

    def setUpAll(self):
        if testbase.db.engine.name != 'postgres' and testbase.db.engine.name != 'oracle':
            return
        global cartitems
        cartitems = Table("cartitems", db, 
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", DateTime())
        )
        
        cartitems.create()

    def testsequence(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        cartitems.insert().execute(description='lala')
        
        cartitems.select().execute().fetchall()
   
   
    def teststandalone(self):
        s = Sequence("my_sequence", engine=db)
        s.create()
        try:
            x =s.execute()
            self.assert_(x == 1)
        finally:
            s.drop()
    
    def teststandalone2(self):
        x = cartitems.c.cart_id.sequence.execute()
        self.assert_(1 <= x <= 4)
        
    def tearDownAll(self): 
        if testbase.db.engine.name != 'postgres' and testbase.db.engine.name != 'oracle':
            return
        cartitems.drop()

if __name__ == "__main__":
    unittest.main()
