from sqlalchemy.orm import descriptor_props, aliased
from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import column
from sqlalchemy import Column, Integer, func, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.util import partial
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import eq_

class TestDescriptor(descriptor_props.DescriptorProperty):
    def __init__(self, cls, key, descriptor=None, doc=None,
            comparator_factory = None):
        self.parent = cls.__mapper__
        self.key = key
        self.doc = doc
        self.descriptor = descriptor
        if comparator_factory:
            self._comparator_factory = partial(comparator_factory, self)
        else:
            self._comparator_factory = lambda mapper: None

class DescriptorInstrumentationTest(fixtures.ORMTest):
    def _fixture(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = 'foo'
            id = Column(Integer, primary_key=True)

        return Foo

    def test_fixture(self):
        Foo = self._fixture()

        d = TestDescriptor(Foo, 'foo')
        d.instrument_class(Foo.__mapper__)

        assert Foo.foo

    def test_property_wrapped_classlevel(self):
        Foo = self._fixture()
        prop = property(lambda self:None)
        Foo.foo = prop

        d = TestDescriptor(Foo, 'foo')
        d.instrument_class(Foo.__mapper__)

        assert Foo().foo is None
        assert Foo.foo is not prop

    def test_property_subclass_wrapped_classlevel(self):
        Foo = self._fixture()

        class myprop(property):
            attr = 'bar'

            def method1(self):
                return "method1"

        prop = myprop(lambda self:None)
        Foo.foo = prop

        d = TestDescriptor(Foo, 'foo')
        d.instrument_class(Foo.__mapper__)

        assert Foo().foo is None
        assert Foo.foo is not prop
        assert Foo.foo.attr == 'bar'
        assert Foo.foo.method1() == 'method1'

    def test_comparator(self):
        class Comparator(PropComparator):
            __hash__ = None

            attr = 'bar'

            def method1(self):
                return "method1"

            def method2(self, other):
                return "method2"

            def __getitem__(self, key):
                return 'value'

            def __eq__(self, other):
                return column('foo') == func.upper(other)

        Foo = self._fixture()
        d = TestDescriptor(Foo, 'foo', comparator_factory=Comparator)
        d.instrument_class(Foo.__mapper__)
        eq_(Foo.foo.method1(), "method1")
        eq_(Foo.foo.method2('x'), "method2")
        assert Foo.foo.attr == 'bar'
        assert Foo.foo['bar'] == 'value'
        eq_(
            (Foo.foo == 'bar').__str__(),
            "foo = upper(:upper_1)"
        )


    def test_aliased_comparator(self):
        class Comparator(ColumnProperty.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) ==\
                            func.foobar(other)

        Foo = self._fixture()
        Foo._name = Column('name', String)

        def comparator_factory(self, mapper):
            prop = mapper._props['_name']
            return Comparator(prop, mapper)

        d = TestDescriptor(Foo, 'foo', comparator_factory=comparator_factory)
        d.instrument_class(Foo.__mapper__)

        eq_(
            str(Foo.foo == 'ed'),
           "foobar(foo.name) = foobar(:foobar_1)"
        )
        eq_(
            str(aliased(Foo).foo == 'ed'),
            "foobar(foo_1.name) = foobar(:foobar_1)"
        )

