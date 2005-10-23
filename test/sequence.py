from testbase import PersistTest
import sqlalchemy.util as util
import unittest, sys, os
import sqlalchemy.schema as schema
import testbase
from sqlalchemy.schema import *
import sqlalchemy

db = sqlalchemy.engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=testbase.echo)
#db = sqlalchemy.engine.create_engine('oracle', {'DSN':'test', 'user':'scott', 'password':'tiger'}, echo=testbase.echo)

class SequenceTest(PersistTest):
    def testsequence(self):
        table = Table("cartitems", db, 
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("date", DateTime())
        )
        
        table.create()

        table.insert().execute(description='hi')
        table.insert().execute(description='there')
        table.insert().execute(description='lala')
        
        table.select().execute().fetchall()
        table.drop()
if __name__ == "__main__":
    unittest.main()
