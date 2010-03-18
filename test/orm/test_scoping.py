from sqlalchemy.test.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy.orm import scoped_session
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, query
from sqlalchemy.test.testing import eq_
from test.orm import _base



class _ScopedTest(_base.MappedTest):
    """Adds another lookup bucket to emulate Session globals."""

    run_setup_mappers = 'once'

    _artifact_registries = (
        _base.MappedTest._artifact_registries + ('scoping',))

    @classmethod
    def setup_class(cls):
        cls.scoping = _base.adict()
        super(_ScopedTest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        cls.scoping.clear()
        super(_ScopedTest, cls).teardown_class()


class ScopedSessionTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(30)))
        Table('table2', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('someid', None, ForeignKey('table1.id')))

    @testing.resolve_artifact_names
    def test_basic(self):
        Session = scoped_session(sa.orm.sessionmaker())

        class CustomQuery(query.Query):
            pass

        class SomeObject(_base.ComparableEntity):
            query = Session.query_property()
        class SomeOtherObject(_base.ComparableEntity):
            query = Session.query_property()
            custom_query = Session.query_property(query_cls=CustomQuery)

        mapper(SomeObject, table1, properties={
            'options':relationship(SomeOtherObject)})
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
        assert isinstance(SomeOtherObject.query, query.Query)
        assert not isinstance(SomeOtherObject.query, CustomQuery)
        assert isinstance(SomeOtherObject.custom_query, query.Query)


class ScopedMapperTest(_ScopedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30)))
        Table('table2', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('someid', None, ForeignKey('table1.id')))

    @classmethod
    def setup_classes(cls):
        class SomeObject(_base.ComparableEntity):
            pass
        class SomeOtherObject(_base.ComparableEntity):
            pass

    @classmethod
    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        Session = scoped_session(sa.orm.create_session)
        Session.mapper(SomeObject, table1, properties={
            'options':relationship(SomeOtherObject)
        })
        Session.mapper(SomeOtherObject, table2)

        cls.scoping['Session'] = Session

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.flush()
        Session.expunge_all()

    @testing.resolve_artifact_names
    def test_query(self):
        sso = SomeOtherObject.query().first()
        assert SomeObject.query.filter_by(id=1).one().options[0].id == sso.id

    @testing.uses_deprecated()
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

    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def test_default_constructor_state_not_shared(self):
        scope = scoped_session(sa.orm.sessionmaker())

        class A(object):
            pass
        class B(object):
            def __init__(self):
                pass

        scope.mapper(A, table1)
        scope.mapper(B, table2)

        A(foo='bar')
        assert_raises(TypeError, B, foo='bar')

        scope = scoped_session(sa.orm.sessionmaker())

        class C(object):
            def __init__(self):
                pass
        class D(object):
            pass

        scope.mapper(C, table1)
        scope.mapper(D, table2)

        assert_raises(TypeError, C, foo='bar')
        D(foo='bar')

    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def test_validating_constructor(self):
        s2 = SomeObject(someid=12)
        s3 = SomeOtherObject(someid=123, bogus=345)

        class ValidatedOtherObject(object): pass
        Session.mapper(ValidatedOtherObject, table2, validate=True)

        v1 = ValidatedOtherObject(someid=12)
        assert_raises(sa.exc.ArgumentError, ValidatedOtherObject,
                          someid=12, bogus=345)

    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def test_dont_clobber_methods(self):
        class MyClass(object):
            def expunge(self):
                return "an expunge !"

        Session.mapper(MyClass, table2)

        assert MyClass().expunge() == "an expunge !"


class ScopedMapperTest2(_ScopedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30)),
            Column('type', String(30)))
        Table('table2', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('someid', None, ForeignKey('table1.id')),
            Column('somedata', String(30)))

    @classmethod
    def setup_classes(cls):
        class BaseClass(_base.ComparableEntity):
            pass
        class SubClass(BaseClass):
            pass

    @classmethod
    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        Session = scoped_session(sa.orm.sessionmaker())

        Session.mapper(BaseClass, table1,
                       polymorphic_identity='base',
                       polymorphic_on=table1.c.type)
        Session.mapper(SubClass, table2,
                       polymorphic_identity='sub',
                       inherits=BaseClass)

        cls.scoping['Session'] = Session

    @testing.resolve_artifact_names
    def test_inheritance(self):
        def expunge_list(l):
            for x in l:
                Session.expunge(x)
            return l

        b = BaseClass(data='b1')
        s =  SubClass(data='s1', somedata='somedata')
        Session.commit()
        Session.expunge_all()

        eq_(expunge_list([BaseClass(data='b1'),
                          SubClass(data='s1', somedata='somedata')]),
            BaseClass.query.all())
        eq_(expunge_list([SubClass(data='s1', somedata='somedata')]),
            SubClass.query.all())


