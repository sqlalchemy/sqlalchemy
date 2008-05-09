import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from sqlalchemy.orm import scoped_session
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation
from testlib.testing import eq_
from orm import _base


class _ScopedTest(_base.MappedTest):
    """Adds another lookup bucket to emulate Session globals."""

    run_setup_mappers = 'once'

    _artifact_registries = (
        _base.MappedTest._artifact_registries + ('scoping',))

    def setUpAll(self):
        type(self).scoping = _base.adict()
        _base.MappedTest.setUpAll(self)

    def tearDownAll(self):
        self.scoping.clear()
        _base.MappedTest.tearDownAll(self)


class ScopedSessionTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('table1', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(30)))
        Table('table2', metadata,
              Column('id', Integer, primary_key=True),
              Column('someid', None, ForeignKey('table1.id')))

    @testing.resolve_artifact_names
    def test_basic(self):
        Session = scoped_session(sa.orm.sessionmaker())

        class SomeObject(_base.ComparableEntity):
            query = Session.query_property()
        class SomeOtherObject(_base.ComparableEntity):
            query = Session.query_property()

        mapper(SomeObject, table1, properties={
            'options':relation(SomeOtherObject)})
        mapper(SomeOtherObject, table2)

        s = SomeObject(id=1, data="hello")
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.add(s)
        Session.commit()
        Session.refresh(sso)
        Session.remove()

        eq_(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]),
            Session.query(SomeObject).one())
        eq_(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]),
            SomeObject.query.one())
        eq_(SomeOtherObject(someid=1),
            SomeOtherObject.query.filter(
                SomeOtherObject.someid == sso.someid).one())


class ScopedMapperTest(_ScopedTest):

    def define_tables(self, metadata):
        Table('table1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        Table('table2', metadata,
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('table1.id')))

    def setup_classes(self):
        class SomeObject(_base.ComparableEntity):
            pass
        class SomeOtherObject(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        Session = scoped_session(sa.orm.create_session)
        Session.mapper(SomeObject, table1, properties={
            'options':relation(SomeOtherObject)
        })
        Session.mapper(SomeOtherObject, table2)

        self.scoping['Session'] = Session

    @testing.resolve_artifact_names
    def insert_data(self):
        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.flush()
        Session.clear()

    @testing.resolve_artifact_names
    def test_query(self):
        sso = SomeOtherObject.query().first()
        assert SomeObject.query.filter_by(id=1).one().options[0].id == sso.id

    @testing.resolve_artifact_names
    def test_query_compiles(self):
        class Foo(object):
            pass
        Session.mapper(Foo, table2)
        assert hasattr(Foo, 'query')

        ext = sa.orm.MapperExtension()

        class Bar(object):
            pass
        Session.mapper(Bar, table2, extension=[ext])
        assert hasattr(Bar, 'query')

        class Baz(object):
            pass
        Session.mapper(Baz, table2, extension=ext)
        assert hasattr(Baz, 'query')

    @testing.resolve_artifact_names
    def test_validating_constructor(self):
        s2 = SomeObject(someid=12)
        s3 = SomeOtherObject(someid=123, bogus=345)

        class ValidatedOtherObject(object): pass
        Session.mapper(ValidatedOtherObject, table2, validate=True)

        v1 = ValidatedOtherObject(someid=12)
        self.assertRaises(sa.exc.ArgumentError, ValidatedOtherObject,
                          someid=12, bogus=345)

    @testing.resolve_artifact_names
    def test_dont_clobber_methods(self):
        class MyClass(object):
            def expunge(self):
                return "an expunge !"

        Session.mapper(MyClass, table2)

        assert MyClass().expunge() == "an expunge !"


class ScopedMapperTest2(_ScopedTest):

    def define_tables(self, metadata):
        Table('table1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('type', String(30)))
        Table('table2', metadata,
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('table1.id')),
            Column('somedata', String(30)))

    def setup_classes(self):
        class BaseClass(_base.ComparableEntity):
            pass
        class SubClass(BaseClass):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        Session = scoped_session(sa.orm.sessionmaker())

        Session.mapper(BaseClass, table1,
                       polymorphic_identity='base',
                       polymorphic_on=table1.c.type)
        Session.mapper(SubClass, table2,
                       polymorphic_identity='sub',
                       inherits=BaseClass)

        self.scoping['Session'] = Session

    @testing.resolve_artifact_names
    def test_inheritance(self):
        def expunge_list(l):
            for x in l:
                Session.expunge(x)
            return l

        b = BaseClass(data='b1')
        s =  SubClass(data='s1', somedata='somedata')
        Session.commit()
        Session.clear()

        eq_(expunge_list([BaseClass(data='b1'),
                          SubClass(data='s1', somedata='somedata')]),
            BaseClass.query.all())
        eq_(expunge_list([SubClass(data='s1', somedata='somedata')]),
            SubClass.query.all())


if __name__ == "__main__":
    testenv.main()
