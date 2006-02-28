from sqlalchemy import *
from sqlalchemy.ext.proxy import ProxyEngine

from testbase import PersistTest
import testbase
import os

#
# Define an engine, table and mapper at the module level, to show that the
# table and mapper can be used with different real engines in multiple threads
#


module_engine = ProxyEngine()
users = Table('users', module_engine, 
              Column('user_id', Integer, primary_key=True),
              Column('user_name', String(16)),
              Column('password', String(20))
              )

class User(object):
    pass


class ConstructTest(PersistTest):
    """tests that we can build SQL constructs without engine-specific parameters, particulary
    oid_column, being needed, as the proxy engine is usually not connected yet."""
    def test_join(self):
        engine = ProxyEngine()
        t = Table('table1', engine, 
            Column('col1', Integer, primary_key=True))
        t2 = Table('table2', engine, 
            Column('col2', Integer, ForeignKey('table1.col1')))
        j = join(t, t2)
        
class ProxyEngineTest1(PersistTest):

    def setUp(self):
        clear_mappers()
        objectstore.clear()
        
    def test_engine_connect(self):
        # connect to a real engine
        module_engine.connect(testbase.db_uri)
        users.create()
        assign_mapper(User, users)
        try:
            trans = objectstore.begin()

            user = User()
            user.user_name='fred'
            user.password='*'
            trans.commit()

            # select
            sqluser = User.select_by(user_name='fred')[0]
            assert sqluser.user_name == 'fred'

            # modify
            sqluser.user_name = 'fred jones'

            # commit - saves everything that changed
            objectstore.commit()
        
            allusers = [ user.user_name for user in User.select() ]
            assert allusers == [ 'fred jones' ]
        finally:
            users.drop()

class ThreadProxyTest(PersistTest):
    def setUp(self):
        assign_mapper(User, users)
    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        pass            
        os.remove('threadtesta.db')
        os.remove('threadtestb.db')
        
    def test_multi_thread(self):
        
        from threading import Thread
        from Queue import Queue
        
        # start 2 threads with different connection params
        # and perform simultaneous operations, showing that the
        # 2 threads don't share a connection
        qa = Queue()
        qb = Queue()
        def run(db_uri, uname, queue):
            def test():
                
                try:
                    module_engine.connect(db_uri)
                    users.create()
                    try:
                        trans  = objectstore.begin()

                        all = User.select()[:]
                        assert all == []

                        u = User()
                        u.user_name = uname
                        u.password = 'whatever'
                        trans.commit()

                        names = [ us.user_name for us in User.select() ]
                        assert names == [ uname ]
                    finally:
                        users.drop()
                        module_engine.dispose()
                except Exception, e:
                    import traceback
                    traceback.print_exc()
                    queue.put(e)
                else:
                    queue.put(False)
            return test

        # NOTE: I'm not sure how to give the test runner the option to
        # override these uris, or how to safely clear them after test runs
        a = Thread(target=run('sqlite://filename=threadtesta.db', 'jim', qa))
        b = Thread(target=run('sqlite://filename=threadtestb.db', 'joe', qb))
        
        a.start()
        b.start()

        # block and wait for the threads to push their results
        res = qa.get(True)
        if res != False:
            raise res

        res = qb.get(True)
        if res != False:
            raise res

class ProxyEngineTest2(PersistTest):

    def setUp(self):
        clear_mappers()
        objectstore.clear()

    def test_table_singleton_a(self):
        """set up for table singleton check
        """
        #
        # For this 'test', create a proxy engine instance, connect it
        # to a real engine, and make it do some work
        #
        engine = ProxyEngine()
        cats = Table('cats', engine,
                     Column('cat_id', Integer, primary_key=True),
                     Column('cat_name', String))

        engine.connect(testbase.db_uri)
        cats.create()
        cats.drop()

        ProxyEngineTest2.cats_table_a = cats
        assert isinstance(cats, Table)

    def test_table_singleton_b(self):
        """check that a table on a 2nd proxy engine instance gets 2nd table
        instance
        """
        #
        # Now create a new proxy engine instance and attach the same
        # table as the first test. This should result in 2 table instances,
        # since different proxy engine instances can't attach to the
        # same table instance
        #
        engine = ProxyEngine()
        cats = Table('cats', engine,
                     Column('cat_id', Integer, primary_key=True),
                     Column('cat_name', String))
        assert id(cats) != id(ProxyEngineTest2.cats_table_a)

        # the real test -- if we're still using the old engine reference,
        # this will fail because the old reference's local storage will
        # not have the default attributes
        engine.connect(testbase.db_uri)
        cats.create()
        cats.drop()

    def test_type_engine_caching(self):
        from sqlalchemy.engine import SQLEngine
        import sqlalchemy.types as sqltypes

        class EngineA(SQLEngine):
            def __init__(self):
                pass

            def hash_key(self):
                return 'a'
            
            def type_descriptor(self, typeobj):
                if typeobj == types.Integer:
                    return TypeEngineX2()
                else:
                    return TypeEngineSTR()
            
        class EngineB(SQLEngine):
            def __init__(self):
                pass

            def hash_key(self):
                return 'b'
            
            def type_descriptor(self, typeobj):
                return TypeEngineMonkey()

        class TypeEngineX2(sqltypes.TypeEngine):
            def convert_bind_param(self, value, engine):
                return value * 2

        class TypeEngineSTR(sqltypes.TypeEngine):
            def convert_bind_param(self, value, engine):
                return repr(str(value))

        class TypeEngineMonkey(sqltypes.TypeEngine):
            def convert_bind_param(self, value, engine):
                return 'monkey'
            
        engine = ProxyEngine()
        engine.storage.engine = EngineA()

        a = engine.type_descriptor(sqltypes.Integer)
        assert a.convert_bind_param(12, engine) == 24
        assert a.convert_bind_param([1,2,3], engine) == [1, 2, 3, 1, 2, 3]

        a2 = engine.type_descriptor(sqltypes.String)
        assert a2.convert_bind_param(12, engine) == "'12'"
        assert a2.convert_bind_param([1,2,3], engine) == "'[1, 2, 3]'"
        
        engine.storage.engine = EngineB()
        b = engine.type_descriptor(sqltypes.Integer)
        assert b.convert_bind_param(12, engine) == 'monkey'
        assert b.convert_bind_param([1,2,3], engine) == 'monkey'
        

    def test_type_engine_autoincrement(self):
        engine = ProxyEngine()
        dogs = Table('dogs', engine,
                     Column('dog_id', Integer, primary_key=True),
                     Column('breed', String),
                     Column('name', String))
        
        class Dog(object):
            pass
        
        assign_mapper(Dog, dogs)

        engine.connect(testbase.db_uri)
        dogs.create()
        try:
            spot = Dog()
            spot.breed = 'beagle'
            spot.name = 'Spot'

            rover = Dog()
            rover.breed = 'spaniel'
            rover.name = 'Rover'
        
            objectstore.commit()
        
            assert spot.dog_id > 0, "Spot did not get an id"
            assert rover.dog_id != spot.dog_id
        finally:
            dogs.drop()
            
    def  test_type_proxy_schema_gen(self):
        from sqlalchemy.databases.postgres import PGSchemaGenerator

        engine = ProxyEngine()
        lizards = Table('lizards', engine,
                        Column('id', Integer, primary_key=True),
                        Column('name', String))
        
        # this doesn't really CONNECT to pg, just establishes pg as the
        # actual engine so that we can determine that it gets the right
        # answer
        engine.connect('postgres://database=test&port=5432&host=127.0.0.1&user=scott&password=tiger')

        sg = PGSchemaGenerator(engine)
        id_spec = sg.get_column_specification(lizards.c.id)
        assert id_spec == 'id SERIAL NOT NULL PRIMARY KEY'
        
        
if __name__ == "__main__":
    testbase.main()





























