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
        global t, f, f2, ts, currenttime
        x = {'x':50}
        def mydefault():
            x['x'] += 1
            return x['x']

        use_function_defaults = db.engine.name == 'postgres' or db.engine.name == 'oracle'
        is_oracle = db.engine.name == 'oracle'
 
        # select "count(1)" returns different results on different DBs
        # also correct for "current_date" compatible as column default, value differences
        currenttime = func.current_date(type=Date, engine=db);
        if is_oracle:
            ts = db.func.trunc(func.sysdate(), column("'DAY'")).scalar()
            f = select([func.count(1) + 5], engine=db).scalar()
            f2 = select([func.count(1) + 14], engine=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(currenttime, column("'DAY'"), engine=db)
            def1 = currenttime
            def2 = func.trunc(text("sysdate"), column("'DAY'"))
            deftype = Date
        elif use_function_defaults:
            f = select([func.count(1) + 5], engine=db).scalar()
            f2 = select([func.count(1) + 14], engine=db).scalar()
            def1 = currenttime
            def2 = text("current_date")
            deftype = Date
            ts = db.func.current_date().scalar()
        else:
            f = select([func.count(1) + 5], engine=db).scalar()
            f2 = select([func.count(1) + 14], engine=db).scalar()
            def1 = def2 = "3"
            ts = 3
            deftype = Integer
            
        t = Table('default_test1', db,
            # python function
            Column('col1', Integer, primary_key=True, default=mydefault),
            
            # python literal
            Column('col2', String(20), default="imthedefault", onupdate="im the update"),
            
            # preexecute expression
            Column('col3', Integer, default=func.count(1) + 5, onupdate=func.count(1) + 14),
            
            # SQL-side default from sql expression
            Column('col4', deftype, PassiveDefault(def1)),
            
            # SQL-side default from literal expression
            Column('col5', deftype, PassiveDefault(def2)),
            
            # preexecute + update timestamp
            Column('col6', Date, default=currenttime, onupdate=currenttime),
            
            Column('boolcol1', Boolean, default=True),
            Column('boolcol2', Boolean, default=False)
        )
        t.create()

    def tearDownAll(self):
        t.drop()
    
    def tearDown(self):
        t.delete().execute()
        
    def teststandalone(self):
        c = db.engine.contextual_connect()
        x = c.execute(t.c.col1.default)
        y = t.c.col2.default.execute()
        z = c.execute(t.c.col3.default)
        self.assert_(50 <= x <= 57)
        self.assert_(y == 'imthedefault')
        self.assert_(z == f)
        # mysql/other db's return 0 or 1 for count(1)
        self.assert_(5 <= z <= 6)
        
    def testinsert(self):
        r = t.insert().execute()
        self.assert_(r.lastrow_has_defaults())
        t.insert().execute()
        t.insert().execute()

        ctexec = currenttime.scalar()
        self.echo("Currenttime "+ repr(ctexec))
        l = t.select().execute()
        self.assert_(l.fetchall() == [(51, 'imthedefault', f, ts, ts, ctexec, True, False), (52, 'imthedefault', f, ts, ts, ctexec, True, False), (53, 'imthedefault', f, ts, ts, ctexec, True, False)])

    def testinsertvalues(self):
        t.insert(values={'col3':50}).execute()
        l = t.select().execute()
        self.assert_(l.fetchone()['col3'] == 50)
        
        
    def testupdate(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        self.echo("Currenttime "+ repr(ctexec))
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l == (pk, 'im the update', f2, None, None, ctexec, True, False))
        # mysql/other db's return 0 or 1 for count(1)
        self.assert_(14 <= f2 <= 15)

    def testupdatevalues(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l['col3'] == 55)

class AutoIncrementTest(PersistTest):
    @testbase.supported('postgres', 'mysql')
    def testnonautoincrement(self):
        meta = BoundMetaData(testbase.db)
        nonai_table = Table("aitest", meta, 
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai_table.create()
        try:
            try:
                # postgres will fail on first row, mysql fails on second row
                nonai_table.insert().execute(data='row 1')
                nonai_table.insert().execute(data='row 2')
                assert False
            except exceptions.SQLError, e:
                print "Got exception", str(e)
                assert True
                
            nonai_table.insert().execute(id=1, data='row 1')
        finally:
            nonai_table.drop()    

    def testwithautoincrement(self):
        meta = BoundMetaData(testbase.db)
        table = Table("aitest", meta, 
            Column('id', Integer, primary_key=True),
            Column('data', String(20)))
        table.create()
        try:
            table.insert().execute(data='row 1')
            table.insert().execute(data='row 2')
        finally:
            table.drop()    

    def testfetchid(self):
        meta = BoundMetaData(testbase.db)
        table = Table("aitest", meta, 
            Column('id', Integer, primary_key=True),
            Column('data', String(20)))
        table.create()

        try:
            # simulate working on a table that doesn't already exist
            meta2 = BoundMetaData(testbase.db)
            table2 = Table("aitest", meta2,
                Column('id', Integer, primary_key=True),
                Column('data', String(20)))
            class AiTest(object):
                pass
            mapper(AiTest, table2)
        
            s = create_session()
            u = AiTest()
            s.save(u)
            s.flush()
            assert u.id is not None
            s.clear()
        finally:
            table.drop()
        

class SequenceTest(PersistTest):
    @testbase.supported('postgres', 'oracle')
    def setUpAll(self):
        global cartitems, sometable, metadata
        metadata = BoundMetaData(testbase.db)
        cartitems = Table("cartitems", metadata, 
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", DateTime())
        )
        sometable = Table( 'Manager', metadata,
               Column( 'obj_id', Integer, Sequence('obj_id_seq'), ),
               Column( 'name', type= String, ),
               Column( 'id', Integer, primary_key= True, ),
           )
        
        metadata.create_all()
    
    @testbase.supported('postgres', 'oracle')
    def testseqnonpk(self):
        """test sequences fire off as defaults on non-pk columns"""
        sometable.insert().execute(name="somename")
        sometable.insert().execute(name="someother")
        assert sometable.select().execute().fetchall() == [
            (1, "somename", 1),
            (2, "someother", 2),
        ]
        
    @testbase.supported('postgres', 'oracle')
    def testsequence(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        cartitems.insert().execute(description='lala')
        
        cartitems.select().execute().fetchall()
   
   
    @testbase.supported('postgres', 'oracle')
    def teststandalone(self):
        s = Sequence("my_sequence", metadata=testbase.db)
        s.create()
        try:
            x = s.execute()
            self.assert_(x == 1)
        finally:
            s.drop()
    
    @testbase.supported('postgres', 'oracle')
    def teststandalone2(self):
        x = cartitems.c.cart_id.sequence.execute()
        self.assert_(1 <= x <= 4)
        
    @testbase.supported('postgres', 'oracle')
    def tearDownAll(self): 
        metadata.drop_all()

if __name__ == "__main__":
    testbase.main()
