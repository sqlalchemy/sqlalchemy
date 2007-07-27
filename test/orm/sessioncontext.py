import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.orm.session import object_session, Session
from testlib import *


metadata = MetaData()
users = Table('users', metadata,
    Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
    Column('user_name', String(40)),
)

class SessionContextTest(AssertMixin):
    def setUp(self):
        clear_mappers()
        
    def do_test(self, class_, context):
        """test session assignment on object creation"""
        obj = class_()
        assert context.current == object_session(obj)

        # keep a reference so the old session doesn't get gc'd
        old_session = context.current

        context.current = Session()
        assert context.current != object_session(obj)
        assert old_session == object_session(obj)

        new_session = context.current
        del context.current
        assert context.current != new_session
        assert old_session == object_session(obj)
        
        obj2 = class_()
        assert context.current == object_session(obj2)
    
    def test_mapper_extension(self):
        context = SessionContext(Session)
        class User(object): pass
        User.mapper = mapper(User, users, extension=context.mapper_extension)
        self.do_test(User, context)


if __name__ == "__main__":
    testbase.main()        
