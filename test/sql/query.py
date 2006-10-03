from testbase import PersistTest
import testbase
import unittest, sys, datetime

import sqlalchemy.databases.sqlite as sqllite

import tables
db = testbase.db
from sqlalchemy import *
from sqlalchemy.engine import ResultProxy, RowProxy

class QueryTest(PersistTest):
    
    def setUpAll(self):
        global users
        users = Table('query_users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            redefine = True
        )
        users.create()
    
    def setUp(self):
        self.users = users
    def tearDown(self):
        self.users.delete().execute()
    
    def tearDownAll(self):
        global users
        users.drop()
        
    def testinsert(self):
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        print repr(self.users.select().execute().fetchall())
        
    def testupdate(self):

        self.users.insert().execute(user_id = 7, user_name = 'jack')
        print repr(self.users.select().execute().fetchall())

        self.users.update(self.users.c.user_id == 7).execute(user_name = 'fred')
        print repr(self.users.select().execute().fetchall())

    def testrowiteration(self):
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'ed')
        self.users.insert().execute(user_id = 9, user_name = 'fred')
        r = self.users.select().execute()
        l = []
        for row in r:
            l.append(row)
        self.assert_(len(l) == 3)
    
    def test_global_metadata(self):
        t1 = Table('table1', Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
        t2 = Table('table2', Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
   
        assert t1.c.col1
        global_connect(testbase.db)
        default_metadata.create_all()
        try:
            assert t1.count().scalar() == 0
        finally:
            default_metadata.drop_all()
            default_metadata.clear()
 
    def testpassiveoverride(self):
        """primarily for postgres, tests that when we get a primary key column back 
        from reflecting a table which has a default value on it, we pre-execute
        that PassiveDefault upon insert, even though PassiveDefault says 
        "let the database execute this", because in postgres we must have all the primary
        key values in memory before insert; otherwise we cant locate the just inserted row."""
        if db.engine.name != 'postgres':
            return
        try:
            db.execute("""
             CREATE TABLE speedy_users
             (
                 speedy_user_id   SERIAL     PRIMARY KEY,
            
                 user_name        VARCHAR    NOT NULL,
                 user_password    VARCHAR    NOT NULL
             );
            """, None)
            
            t = Table("speedy_users", db, autoload=True)
            t.insert().execute(user_name='user', user_password='lala')
            l = t.select().execute().fetchall()
            print l
            self.assert_(l == [(1, 'user', 'lala')])
        finally:
            db.execute("drop table speedy_users", None)

    def testschema(self):
        if not db.engine.__module__.endswith('postgres'):
            return 
            
        test_table = Table('my_table', db,
                    Column('id', Integer, primary_key=True),
                    Column('data', String(20), nullable=False),
                    schema='alt_schema'
                 )
        test_table.create()
        try:
            # plain insert
            test_table.insert().execute(data='test')

            # try with a PassiveDefault
            test_table.deregister()
            test_table = Table('my_table', db, autoload=True, redefine=True, schema='alt_schema')
            test_table.insert().execute(data='test')

        finally:
            test_table.drop()

    def test_repeated_bindparams(self):
        """test that a BindParam can be used more than once.  
        this should be run for dbs with both positional and named paramstyles."""
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'fred')

        u = bindparam('uid')
        s = self.users.select(or_(self.users.c.user_name==u, self.users.c.user_name==u))
        r = s.execute(uid='fred').fetchall()
        assert len(r) == 1
    
    def test_bindparam_shortname(self):
        """test the 'shortname' field on BindParamClause."""
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'fred')
        u = bindparam('uid', shortname='someshortname')
        s = self.users.select(self.users.c.user_name==u)
        r = s.execute(someshortname='fred').fetchall()
        assert len(r) == 1
        
    def testdelete(self):
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'fred')
        print repr(self.users.select().execute().fetchall())

        self.users.delete(self.users.c.user_name == 'fred').execute()
        
        print repr(self.users.select().execute().fetchall())
        
    def testselectlimit(self):
        self.users.insert().execute(user_id=1, user_name='john')
        self.users.insert().execute(user_id=2, user_name='jack')
        self.users.insert().execute(user_id=3, user_name='ed')
        self.users.insert().execute(user_id=4, user_name='wendy')
        self.users.insert().execute(user_id=5, user_name='laura')
        self.users.insert().execute(user_id=6, user_name='ralph')
        self.users.insert().execute(user_id=7, user_name='fido')
        r = self.users.select(limit=3, order_by=[self.users.c.user_id]).execute().fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')], repr(r))
        r = self.users.select(limit=3, offset=2, order_by=[self.users.c.user_id]).execute().fetchall()
        self.assert_(r==[(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = self.users.select(offset=5, order_by=[self.users.c.user_id]).execute().fetchall()
        self.assert_(r==[(6, 'ralph'), (7, 'fido')])
      
  
    def test_column_accessor(self):
        self.users.insert().execute(user_id=1, user_name='john')
        self.users.insert().execute(user_id=2, user_name='jack')
        r = self.users.select(self.users.c.user_id==2).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[self.users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[self.users.c.user_name] == 'jack')

    def test_keys(self):
        self.users.insert().execute(user_id=1, user_name='foo')
        r = self.users.select().execute().fetchone()
        self.assertEqual(r.keys(), ['user_id', 'user_name'])

    def test_items(self):
        self.users.insert().execute(user_id=1, user_name='foo')
        r = self.users.select().execute().fetchone()
        self.assertEqual(r.items(), [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        self.users.insert().execute(user_id=1, user_name='foo')
        r = self.users.select().execute().fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = db.execute('select user_name from query_users', {}).fetchone()
        self.assertEqual(len(r), 1)
        r.close()
    
    def test_functions(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        assert x == y == z

    @testbase.supported('postgres')
    def test_functions_with_cols(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        w = select(['*'], from_obj=[testbase.db.func.current_date()]).scalar()
        
        # construct a column-based FROM object out of a function, like in [ticket:172]
        s = select([column('date', type=DateTime)], from_obj=[testbase.db.func.current_date()])
        q = s.execute().fetchone()[s.c.date]
        r = s.alias('datequery').select().scalar()
        
        assert x == y == z == w == q == r
        
    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        self.users.insert().execute(user_id=1, user_name='foo')
        r = self.users.select(self.users.c.user_id==1).execute().fetchone()
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 'foo')
        self.assertEqual(r.keys(), ['user_id', 'user_name'])
        self.assertEqual(r.values(), [1, 'foo'])
        
    def test_column_order_with_text_query(self):
        # should return values in query order
        self.users.insert().execute(user_id=1, user_name='foo')
        r = db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(r[0], 'foo')
        self.assertEqual(r[1], 1)
        self.assertEqual(r.keys(), ['user_name', 'user_id'])
        self.assertEqual(r.values(), ['foo', 1])
       
    @testbase.unsupported('oracle', 'firebird') 
    def test_column_accessor_shadow(self):
        shadowed = Table('test_shadowed', db,
                         Column('shadow_id', INT, primary_key = True),
                         Column('shadow_name', VARCHAR(20)),
                         Column('parent', VARCHAR(20)),
                         Column('row', VARCHAR(40)),
                         Column('__parent', VARCHAR(20)),
                         Column('__row', VARCHAR(20)),
            redefine = True
        )
        shadowed.create()
        try:
            shadowed.insert().execute(shadow_id=1, shadow_name='The Shadow', parent='The Light', row='Without light there is no shadow', __parent='Hidden parent', __row='Hidden row')
            r = shadowed.select(shadowed.c.shadow_id==1).execute().fetchone()
            self.assert_(r.shadow_id == r['shadow_id'] == r[shadowed.c.shadow_id] == 1)
            self.assert_(r.shadow_name == r['shadow_name'] == r[shadowed.c.shadow_name] == 'The Shadow')
            self.assert_(r.parent == r['parent'] == r[shadowed.c.parent] == 'The Light')
            self.assert_(r.row == r['row'] == r[shadowed.c.row] == 'Without light there is no shadow')
            self.assert_(r['__parent'] == 'Hidden parent')
            self.assert_(r['__row'] == 'Hidden row')
            try:
                print r.__parent, r.__row
                self.fail('Should not allow access to private attributes')
            except AttributeError:
                pass # expected
            r.close()
        finally:
            shadowed.drop()
        
if __name__ == "__main__":
    testbase.main()        
