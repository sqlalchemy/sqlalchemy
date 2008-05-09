import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import *
from testlib import *
from testlib import fixtures


class ScopedSessionTest(ORMTest):

    def define_tables(self, metadata):
        global table, table2
        table = Table('sometable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        table2 = Table('someothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id'))
            )

    def test_basic(self):
        Session = scoped_session(sessionmaker())

        class SomeObject(fixtures.Base):
            query = Session.query_property()
        class SomeOtherObject(fixtures.Base):
            query = Session.query_property()

        mapper(SomeObject, table, properties={
            'options':relation(SomeOtherObject)
        })
        mapper(SomeOtherObject, table2)

        s = SomeObject(id=1, data="hello")
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.add(s)
        Session.commit()
        Session.refresh(sso)
        Session.remove()

        self.assertEquals(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]), Session.query(SomeObject).one())
        self.assertEquals(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]), SomeObject.query.one())
        self.assertEquals(SomeOtherObject(someid=1), SomeOtherObject.query.filter(SomeOtherObject.someid==sso.someid).one())


class ScopedMapperTest(TestBase):
    def setUpAll(self):
        global metadata, table, table2
        metadata = MetaData(testing.db)
        table = Table('sometable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        table2 = Table('someothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id'))
            )
        metadata.create_all()

    def setUp(self):
        global SomeObject, SomeOtherObject
        class SomeObject(fixtures.Base):pass
        class SomeOtherObject(fixtures.Base):pass

        global Session

        Session = scoped_session(create_session)
        Session.mapper(SomeObject, table, properties={
            'options':relation(SomeOtherObject)
        })
        Session.mapper(SomeOtherObject, table2)

        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.flush()
        Session.clear()

    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        for table in metadata.table_iterator(reverse=True):
            table.delete().execute()
        clear_mappers()

    def test_query(self):
        sso = SomeOtherObject.query().first()
        assert SomeObject.query.filter_by(id=1).one().options[0].id == sso.id

    def test_query_compiles(self):
        class Foo(object):
            pass
        Session.mapper(Foo, table2)
        assert hasattr(Foo, 'query')

        ext = MapperExtension()

        class Bar(object):
            pass
        Session.mapper(Bar, table2, extension=[ext])
        assert hasattr(Bar, 'query')

        class Baz(object):
            pass
        Session.mapper(Baz, table2, extension=ext)
        assert hasattr(Baz, 'query')

    def test_validating_constructor(self):
        s2 = SomeObject(someid=12)
        s3 = SomeOtherObject(someid=123, bogus=345)

        class ValidatedOtherObject(object): pass
        Session.mapper(ValidatedOtherObject, table2, validate=True)

        v1 = ValidatedOtherObject(someid=12)
        self.assertRaises(sa_exc.ArgumentError, ValidatedOtherObject, someid=12, bogus=345)

    def test_dont_clobber_methods(self):
        class MyClass(object):
            def expunge(self):
                return "an expunge !"

        Session.mapper(MyClass, table2)

        assert MyClass().expunge() == "an expunge !"

class ScopedMapperTest2(ORMTest):
    def define_tables(self, metadata):
        global table, table2
        table = Table('sometable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('type', String(30))

            )
        table2 = Table('someothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id')),
            Column('somedata', String(30)),
            )

    def test_inheritance(self):
        def expunge_list(l):
            for x in l:
                Session.expunge(x)
            return l

        class BaseClass(fixtures.Base):
            pass
        class SubClass(BaseClass):
            pass

        Session = scoped_session(sessionmaker())
        Session.mapper(BaseClass, table, polymorphic_identity='base', polymorphic_on=table.c.type)
        Session.mapper(SubClass, table2, polymorphic_identity='sub', inherits=BaseClass)

        b = BaseClass(data='b1')
        s =  SubClass(data='s1', somedata='somedata')
        Session.commit()
        Session.clear()

        assert expunge_list([BaseClass(data='b1'), SubClass(data='s1', somedata='somedata')]) == BaseClass.query.all()
        assert expunge_list([SubClass(data='s1', somedata='somedata')]) == SubClass.query.all()



if __name__ == "__main__":
    testenv.main()
