from testbase import PersistTest, AssertMixin
import testbase

from sqlalchemy import *

from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.ext.sessioncontext import SessionContext

class OverrideAttributesTest(PersistTest):
    def setUpAll(self):
        global metadata, table, table2
        metadata = MetaData(testbase.db)
        table = Table('sometable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        table2 = Table('someothertable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id'))
            )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass
    def test_override_attributes(self):
        class SomeObject(object):pass
        class SomeOtherObject(object):pass
        
        ctx = SessionContext(create_session)
        assign_mapper(ctx, SomeObject, table, properties={
            # this is the current workaround for class attribute name/collection collision: specify collection_class
            # explicitly.   when we do away with class attributes specifying collection classes, this wont be
            # needed anymore.
            'options':relation(SomeOtherObject, collection_class=list)
        })
        assign_mapper(ctx, SomeOtherObject, table2)
        class_mapper(SomeObject)
        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        ctx.current.flush()
        ctx.current.clear()
        
        assert SomeObject.get_by(id=s.id).options[0].id == sso.id
        
if __name__ == '__main__':
    testbase.main()
