from testbase import PersistTest
import sqlalchemy.util as util
import unittest, sys, os
import sqlalchemy.schema as schema
import testbase
from sqlalchemy import *
import sqlalchemy


class SequenceTest(PersistTest):

    def setUp(self):
        db = sqlalchemy.engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=testbase.echo)
        #db = sqlalchemy.engine.create_engine('oracle', {'dsn':os.environ['DSN'], 'user':os.environ['USER'], 'password':os.environ['PASSWORD']}, echo=testbase.echo)

        self.table = Table("cartitems", db, 
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", DateTime())
        )
        
        self.table.create()

    def testsequence(self):
        self.table.insert().execute(description='hi')
        self.table.insert().execute(description='there')
        self.table.insert().execute(description='lala')
        
        self.table.select().execute().fetchall()
   
    def tearDown(self): 
	self.table.drop()

if __name__ == "__main__":
    unittest.main()
