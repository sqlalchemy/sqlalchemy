import testbase

from sqlalchemy import *
from sqlalchemy.orm import create_session, clear_mappers, relation, class_mapper
from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.ext.sessioncontext import SessionContext
from testlib import *


class AssignMapperTest(PersistTest):
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

    def setUp(self):
        global SomeObject, SomeOtherObject, ctx
        class SomeObject(object):pass
        class SomeOtherObject(object):pass
        
        ctx = SessionContext(create_session)
        assign_mapper(ctx, SomeObject, table, properties={
            'options':relation(SomeOtherObject)
        })
        assign_mapper(ctx, SomeOtherObject, table2)

        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        ctx.current.flush()
        ctx.current.clear()

    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        for table in metadata.table_iterator(reverse=True):
            table.delete().execute()
        clear_mappers()
        
    def test_override_attributes(self):
        
        sso = SomeOtherObject.query().first()
        
        assert SomeObject.query.filter_by(id=1).one().options[0].id == sso.id

        s2 = SomeObject(someid=12)
        s3 = SomeOtherObject(someid=123, bogus=345)
         
        class ValidatedOtherObject(object):pass
        assign_mapper(ctx, ValidatedOtherObject, table2, validate=True)

        v1 = ValidatedOtherObject(someid=12)
        try:
            v2 = ValidatedOtherObject(someid=12, bogus=345)
            assert False
        except exceptions.ArgumentError:
            pass
    
    def test_dont_clobber_methods(self):
        class MyClass(object):
            def expunge(self):
                return "an expunge !"
        
        assign_mapper(ctx, MyClass, table2)
        
        assert MyClass().expunge() == "an expunge !"

        
if __name__ == '__main__':
    testbase.main()
