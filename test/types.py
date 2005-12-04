from sqlalchemy import *
import testbase
import string


    
class TypesTest(testbase.PersistTest):
    def setUpAll(self):
        global db
        db = testbase.db

    def testprocessing(self):
        class MyType(types.TypeEngine):
            def get_col_spec(self):
                return "VARCHAR(100)"
            def convert_bind_param(self, value):
                return "BIND_IN"+ value
            def convert_result_value(self, value):
                return value + "BIND_OUT"
            def adapt(self, typeobj):
                return typeobj()
            def adapt_args(self):
                return self
                
        users = Table('users', db, 
            Column('user_id', Integer, primary_key = True),
            Column('goofy', MyType, nullable = False)
        )
        
        users.create()
        
        users.insert().execute(user_id = 2, goofy = 'jack')
        users.insert().execute(user_id = 3, goofy = 'lala')
        users.insert().execute(user_id = 4, goofy = 'fred')
        
        l = users.select().execute().fetchall()
        print repr(l)
        self.assert_(l == [(2, u'BIND_INjackBIND_OUT'), (3, u'BIND_INlalaBIND_OUT'), (4, u'BIND_INfredBIND_OUT')])
     
        
if __name__ == "__main__":
    testbase.main()
