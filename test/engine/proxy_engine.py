from testbase import PersistTest
import testbase
import os

from sqlalchemy import *
from sqlalchemy.ext.proxy import ProxyEngine


#
# Define an engine, table and mapper at the module level, to show that the
# table and mapper can be used with different real engines in multiple threads
#


class ProxyTestBase(PersistTest):
    def setUpAll(self):

        global users, User, module_engine, module_metadata

        module_engine = ProxyEngine(echo=testbase.echo)
        module_metadata = MetaData()

        users = Table('users', module_metadata, 
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(16)),
                      Column('password', String(20))
                      )

        class User(object):
            pass

        User.mapper = mapper(User, users)
    def tearDownAll(self):
        clear_mappers()

class ConstructTest(ProxyTestBase):
    """tests that we can build SQL constructs without engine-specific parameters, particulary
    oid_column, being needed, as the proxy engine is usually not connected yet."""

    def test_join(self):
        engine = ProxyEngine()
        t = Table('table1', engine, 
            Column('col1', Integer, primary_key=True))
        t2 = Table('table2', engine, 
            Column('col2', Integer, ForeignKey('table1.col1')))
        j = join(t, t2)
        

class ProxyEngineTest1(ProxyTestBase):

    def test_engine_connect(self):
        # connect to a real engine
        module_engine.connect(testbase.db_uri)
        module_metadata.create_all(module_engine)

        session = create_session(bind_to=module_engine)
        try:

            user = User()
            user.user_name='fred'
            user.password='*'

            session.save(user)
            session.flush()

            query = session.query(User)

            # select
            sqluser = query.select_by(user_name='fred')[0]
            assert sqluser.user_name == 'fred'

            # modify
            sqluser.user_name = 'fred jones'

            # flush - saves everything that changed
            session.flush()
        
            allusers = [ user.user_name for user in query.select() ]
            assert allusers == ['fred jones']

        finally:
            module_metadata.drop_all(module_engine)


class ThreadProxyTest(ProxyTestBase):

    def tearDownAll(self):
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
                    module_metadata.create_all(module_engine)
                    try:
                        session = create_session(bind_to=module_engine)

                        query = session.query(User)

                        all = list(query.select())
                        assert all == []

                        u = User()
                        u.user_name = uname
                        u.password = 'whatever'

                        session.save(u)
                        session.flush()

                        names = [u.user_name for u in query.select()]
                        assert names == [uname]
                    finally:
                        module_metadata.drop_all(module_engine)
                        module_engine.get_engine().dispose()
                except Exception, e:
                    import traceback
                    traceback.print_exc()
                    queue.put(e)
                else:
                    queue.put(False)
            return test

        a = Thread(target=run('sqlite:///threadtesta.db', 'jim', qa))
        b = Thread(target=run('sqlite:///threadtestb.db', 'joe', qb))
        
        a.start()
        b.start()
        
        # block and wait for the threads to push their results
        res = qa.get()
        if res != False:
            raise res

        res = qb.get()
        if res != False:
            raise res


class ProxyEngineTest2(ProxyTestBase):

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

        cats.create(engine)
        cats.drop(engine)

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
        cats.create(engine)
        cats.drop(engine)

if __name__ == "__main__":
    testbase.main()
