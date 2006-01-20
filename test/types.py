from sqlalchemy import *
import string,datetime, re
from testbase import PersistTest, AssertMixin
import testbase
    
db = testbase.db



class OverrideTest(PersistTest):

    def testprocessing(self):
        class MyType(types.TypeEngine):
            def get_col_spec(self):
                return "VARCHAR(100)"
            def convert_bind_param(self, value, engine):
                return "BIND_IN"+ value
            def convert_result_value(self, value, engine):
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

        l = users.select(use_labels=True).execute().fetchall()
        print repr(l)
        self.assert_(l == [(2, u'BIND_INjackBIND_OUT'), (3, u'BIND_INlalaBIND_OUT'), (4, u'BIND_INfredBIND_OUT')])


class ColumnsTest(AssertMixin):

    def testcolumns(self):
        expectedResults = { 'int_column': 'int_column INTEGER',
                                   'varchar_column': 'varchar_column VARCHAR(20)',
                                   'numeric_column': 'numeric_column NUMERIC(12, 3)',
                                   'float_column': 'float_column NUMERIC(25, 2)'
                                 }

        if not db.engine.__module__.endswith('sqlite'):
            expectedResults['float_column'] = 'float_column FLOAT(25)'
    
        print db.engine.__module__
        testTable = Table('testColumns', db,
            Column('int_column', Integer),
            Column('varchar_column', String(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', Float(25)),
        )

        for aCol in testTable.c:
            self.assertEquals(expectedResults[aCol.name], db.schemagenerator(None).get_column_specification(aCol))
        

class BinaryTest(AssertMixin):
    def setUpAll(self):
        global binary_table
        binary_table = Table('binary_table', db, 
        Column('primary_id', Integer, primary_key=True),
        Column('data', Binary),
        Column('data_slice', Binary(100)),
        Column('misc', String(30)))
        binary_table.create()
    def tearDownAll(self):
        binary_table.drop()
    def testbinary(self):
        stream1 =self.get_module_stream('sqlalchemy.sql')
        stream2 =self.get_module_stream('sqlalchemy.engine')
        binary_table.insert().execute(misc='sql.pyc', data=stream1, data_slice=stream1[0:100])
        binary_table.insert().execute(misc='engine.pyc', data=stream2, data_slice=stream2[0:99])
        l = binary_table.select().execute().fetchall()
        print len(stream1), len(l[0]['data']), len(l[0]['data_slice'])
        self.assert_(list(stream1) == list(l[0]['data']))
        self.assert_(list(stream1[0:100]) == list(l[0]['data_slice']))
        self.assert_(list(stream2) == list(l[1]['data']))
    def get_module_stream(self, name):
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        f = mod.__file__
        f = re.sub('\.py$', '\.pyc', f)
        return file(f).read()
        
class DateTest(AssertMixin):
    def setUpAll(self):
        global users_with_date
        users_with_date = Table('query_users_with_date', db,
        Column('user_id', INT, primary_key = True),
        Column('user_name', VARCHAR(20)),
        Column('user_date', DateTime),
        redefine = True
        )
        users_with_date.create()
        users_with_date.insert().execute(user_id = 7, user_name = 'jack', user_date=datetime.datetime(2005,11,10))
        users_with_date.insert().execute(user_id = 8, user_name = 'roy', user_date=datetime.datetime(2005,11,10, 11,52,35))
        users_with_date.insert().execute(user_id = 9, user_name = 'foo', user_date=datetime.datetime(2005,11,10, 11,52,35, 54839))
        users_with_date.insert().execute(user_id = 10, user_name = 'colber', user_date=None)
    def tearDownAll(self):
        users_with_date.drop()

    def testdate(self):
        l = users_with_date.select().execute().fetchall()
        l = [[c for c in r] for r in l]
        if db.engine.__module__.endswith('mysql'):
            x = [[7, 'jack', datetime.datetime(2005, 11, 10, 0, 0)], [8, 'roy', datetime.datetime(2005, 11, 10, 11, 52, 35)], [9, 'foo', datetime.datetime(2005, 11, 10, 11, 52, 35)], [10, 'colber', None]]
        else:
            x = [[7, 'jack', datetime.datetime(2005, 11, 10, 0, 0)], [8, 'roy', datetime.datetime(2005, 11, 10, 11, 52, 35)], [9, 'foo', datetime.datetime(2005, 11, 10, 11, 52, 35, 54839)], [10, 'colber', None]]
        print repr(l)
        print repr(x)
        self.assert_(l == x)

    def testtextdate(self):     
        x = db.text("select user_date from query_users_with_date", typemap={'user_date':DateTime}).execute().fetchall()
        
        print repr(x)
        self.assert_(isinstance(x[0][0], datetime.datetime))
        
        #x = db.text("select * from query_users_with_date where user_date=:date", bindparams=[bindparam('date', )]).execute(date=datetime.datetime(2005, 11, 10, 11, 52, 35)).fetchall()
        #print repr(x)
        
if __name__ == "__main__":
    testbase.main()
