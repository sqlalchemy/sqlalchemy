from testbase import PersistTest, AssertMixin
import testbase

from sqlalchemy import *
from sqlalchemy.orm import create_session, clear_mappers, relation, class_mapper

from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.ext.sessioncontext import SessionContext
from testbase import Table, Column

class OverrideAttributesTest(PersistTest):
    def setUpAll(self):
        global metadata, table, table2
        metadata = BoundMetaData(testbase.db)
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
            'options':relation(SomeOtherObject)
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
