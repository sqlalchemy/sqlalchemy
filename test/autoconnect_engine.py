from sqlalchemy import *
from sqlalchemy.ext.proxy import AutoConnectEngine

from testbase import PersistTest
import testbase
import os

#
# Define an engine, table and mapper at the module level, to show that the
# table and mapper can be used with different real engines in multiple threads
#


module_engine = AutoConnectEngine( testbase.db_uri )
users = Table('users', module_engine, 
              Column('user_id', Integer, primary_key=True),
              Column('user_name', String(16)),
              Column('password', String(20))
              )

class User(object):
    pass


class AutoConnectEngineTest1(PersistTest):

    def setUp(self):
        clear_mappers()
        objectstore.clear()
        
    def test_engine_connect(self):
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


        
        
if __name__ == "__main__":
    testbase.main()





























