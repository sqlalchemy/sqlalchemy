from sqlalchemy import Integer, ForeignKey, String
from sqlalchemy.types import PickleType, TypeDecorator, VARCHAR
from sqlalchemy.orm import mapper, Session, composite
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.instrumentation import ClassManager
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing import eq_, assert_raises_message
from sqlalchemy.testing.util import picklers
from sqlalchemy.testing import fixtures
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.ext.mutable import MutableDict


class Foo(fixtures.BasicEntity):
    pass


class SubFoo(Foo):
    pass


class FooWithEq(object):
    def __init__(self, **kw):
        for k in kw:
            setattr(self, k, kw[k])

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


class Point(MutableComposite):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __composite_values__(self):
        return self.x, self.y

    def __getstate__(self):
        return self.x, self.y

    def __setstate__(self, state):
        self.x, self.y = state

    def __eq__(self, other):
        return isinstance(other, Point) and \
            other.x == self.x and \
            other.y == self.y


class MyPoint(Point):
    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, tuple):
            value = Point(*value)
        return value


class _MutableDictTestBase(object):
    run_define_tables = 'each'

    @classmethod
    def _type_fixture(cls):
        return MutableDict

    def setup_mappers(cls):
        foo = cls.tables.foo

        mapper(Foo, foo)

    def teardown(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()
        super(_MutableDictTestBase, self).teardown()

    def test_coerce_none(self):
        sess = Session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, None)

    def test_coerce_raise(self):
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects of type",
            Foo, data=set([1, 2, 3])
        )

    def test_in_place_mutation(self):
        sess = Session()

        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        f1.data['a'] = 'c'
        sess.commit()

        eq_(f1.data, {'a': 'c'})

    def test_clear(self):
        sess = Session()

        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        f1.data.clear()
        sess.commit()

        eq_(f1.data, {})

    def test_update(self):
        sess = Session()

        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        f1.data.update({'a': 'z'})
        sess.commit()

        eq_(f1.data, {'a': 'z'})

    def test_setdefault(self):
        sess = Session()

        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        eq_(f1.data.setdefault('c', 'd'), 'd')
        sess.commit()

        eq_(f1.data, {'a': 'b', 'c': 'd'})

        eq_(f1.data.setdefault('c', 'q'), 'd')
        sess.commit()

        eq_(f1.data, {'a': 'b', 'c': 'd'})

    def test_replace(self):
        sess = Session()
        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.flush()

        f1.data = {'b': 'c'}
        sess.commit()
        eq_(f1.data, {'b': 'c'})

    def test_replace_itself_still_ok(self):
        sess = Session()
        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.flush()

        f1.data = f1.data
        f1.data['b'] = 'c'
        sess.commit()
        eq_(f1.data, {'a': 'b', 'b': 'c'})

    def test_pickle_parent(self):
        sess = Session()

        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()
        f1.data
        sess.close()

        for loads, dumps in picklers():
            sess = Session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data['a'] = 'c'
            assert f2 in sess.dirty

    def test_unrelated_flush(self):
        sess = Session()
        f1 = Foo(data={"a": "b"}, unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data["a"] = "c"
        sess.commit()
        eq_(f1.data["a"], "c")

    def _test_non_mutable(self):
        sess = Session()

        f1 = Foo(non_mutable_data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        f1.non_mutable_data['a'] = 'c'
        sess.commit()

        eq_(f1.non_mutable_data, {'a': 'b'})

class MutableWithScalarPickleTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()

        mutable_pickle = MutableDict.as_mutable(PickleType)
        Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('skip', mutable_pickle),
            Column('data', mutable_pickle),
            Column('non_mutable_data', PickleType),
            Column('unrelated_data', String(50))
        )

    def test_non_mutable(self):
        self._test_non_mutable()

class MutableWithScalarJSONTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        MutableDict = cls._type_fixture()

        Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', MutableDict.as_mutable(JSONEncodedDict)),
            Column('non_mutable_data', JSONEncodedDict),
            Column('unrelated_data', String(50))
        )

    def test_non_mutable(self):
        self._test_non_mutable()

class MutableAssocWithAttrInheritTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):

        Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', PickleType),
            Column('non_mutable_data', PickleType),
            Column('unrelated_data', String(50))
        )

        Table('subfoo', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
        )

    def setup_mappers(cls):
        foo = cls.tables.foo
        subfoo = cls.tables.subfoo

        mapper(Foo, foo)
        mapper(SubFoo, subfoo, inherits=Foo)
        MutableDict.associate_with_attribute(Foo.data)

    def test_in_place_mutation(self):
        sess = Session()

        f1 = SubFoo(data={'a': 'b'})
        sess.add(f1)
        sess.commit()

        f1.data['a'] = 'c'
        sess.commit()

        eq_(f1.data, {'a': 'c'})

    def test_replace(self):
        sess = Session()
        f1 = SubFoo(data={'a': 'b'})
        sess.add(f1)
        sess.flush()

        f1.data = {'b': 'c'}
        sess.commit()
        eq_(f1.data, {'b': 'c'})

class MutableAssociationScalarPickleTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        MutableDict = cls._type_fixture()
        MutableDict.associate_with(PickleType)

        Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('skip', PickleType),
            Column('data', PickleType),
            Column('unrelated_data', String(50))
        )

class MutableAssociationScalarJSONTest(_MutableDictTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        MutableDict = cls._type_fixture()
        MutableDict.associate_with(JSONEncodedDict)

        Table('foo', metadata,
            Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('data', JSONEncodedDict),
            Column('unrelated_data', String(50))
        )


class CustomMutableAssociationScalarJSONTest(_MutableDictTestBase, fixtures.MappedTest):

    CustomMutableDict = None

    @classmethod
    def _type_fixture(cls):
        if not(getattr(cls, 'CustomMutableDict')):
            MutableDict = super(CustomMutableAssociationScalarJSONTest, cls)._type_fixture()
            class CustomMutableDict(MutableDict):
                pass
            cls.CustomMutableDict = CustomMutableDict
        return cls.CustomMutableDict

    @classmethod
    def define_tables(cls, metadata):
        import json

        class JSONEncodedDict(TypeDecorator):
            impl = VARCHAR(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = json.dumps(value)

                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = json.loads(value)
                return value

        CustomMutableDict = cls._type_fixture()
        CustomMutableDict.associate_with(JSONEncodedDict)

        Table('foo', metadata,
            Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('data', JSONEncodedDict),
            Column('unrelated_data', String(50))
        )

    def test_pickle_parent(self):
        # Picklers don't know how to pickle CustomMutableDict, but we aren't testing that here
        pass

    def test_coerce(self):
        sess = Session()
        f1 = Foo(data={'a': 'b'})
        sess.add(f1)
        sess.flush()
        eq_(type(f1.data), self._type_fixture())


class _CompositeTestBase(object):
    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
            Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('x', Integer),
            Column('y', Integer),
            Column('unrelated_data', String(50))
        )

    def setup(self):
        from sqlalchemy.ext import mutable
        mutable._setup_composite_listener()
        super(_CompositeTestBase, self).setup()


    def teardown(self):
        # clear out mapper events
        Mapper.dispatch._clear()
        ClassManager.dispatch._clear()
        super(_CompositeTestBase, self).teardown()

    @classmethod
    def _type_fixture(cls):


        return Point

class MutableCompositesUnpickleTest(_CompositeTestBase, fixtures.MappedTest):

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        cls.Point = cls._type_fixture()

        mapper(FooWithEq, foo, properties={
            'data': composite(cls.Point, foo.c.x, foo.c.y)
        })

    def test_unpickle_modified_eq(self):
        u1 = FooWithEq(data=self.Point(3, 5))
        for loads, dumps in picklers():
            loads(dumps(u1))

class MutableCompositesTest(_CompositeTestBase, fixtures.MappedTest):

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        Point = cls._type_fixture()

        mapper(Foo, foo, properties={
            'data': composite(Point, foo.c.x, foo.c.y)
        })

    def test_in_place_mutation(self):
        sess = Session()
        d = Point(3, 4)
        f1 = Foo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data.y = 5
        sess.commit()

        eq_(f1.data, Point(3, 5))

    def test_pickle_of_parent(self):
        sess = Session()
        d = Point(3, 4)
        f1 = Foo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data
        assert 'data' in f1.__dict__
        sess.close()

        for loads, dumps in picklers():
            sess = Session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data.y = 12
            assert f2 in sess.dirty

    def test_set_none(self):
        sess = Session()
        f1 = Foo(data=None)
        sess.add(f1)
        sess.commit()
        eq_(f1.data, Point(None, None))

        f1.data.y = 5
        sess.commit()
        eq_(f1.data, Point(None, 5))

    def test_set_illegal(self):
        f1 = Foo()
        assert_raises_message(
            ValueError,
            "Attribute 'data' does not accept objects",
            setattr, f1, 'data', 'foo'
        )

    def test_unrelated_flush(self):
        sess = Session()
        f1 = Foo(data=Point(3, 4), unrelated_data="unrelated")
        sess.add(f1)
        sess.flush()
        f1.unrelated_data = "unrelated 2"
        sess.flush()
        f1.data.x = 5
        sess.commit()

        eq_(f1.data.x, 5)

class MutableCompositeCallableTest(_CompositeTestBase, fixtures.MappedTest):

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        Point = cls._type_fixture()

        # in this case, this is not actually a MutableComposite.
        # so we don't expect it to track changes
        mapper(Foo, foo, properties={
            'data': composite(lambda x, y: Point(x, y), foo.c.x, foo.c.y)
        })

    def test_basic(self):
        sess = Session()
        f1 = Foo(data=Point(3, 4))
        sess.add(f1)
        sess.flush()
        f1.data.x = 5
        sess.commit()

        # we didn't get the change.
        eq_(f1.data.x, 3)


class MutableCompositeCustomCoerceTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def _type_fixture(cls):

        return MyPoint


    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        Point = cls._type_fixture()

        mapper(Foo, foo, properties={
            'data': composite(Point, foo.c.x, foo.c.y)
        })

    def test_custom_coerce(self):
        f = Foo()
        f.data = (3, 4)
        eq_(f.data, Point(3, 4))

    def test_round_trip_ok(self):
        sess = Session()
        f = Foo()
        f.data = (3, 4)

        sess.add(f)
        sess.commit()

        eq_(f.data, Point(3, 4))


class MutableInheritedCompositesTest(_CompositeTestBase, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('x', Integer),
            Column('y', Integer)
        )
        Table('subfoo', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
        )

    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo
        subfoo = cls.tables.subfoo

        Point = cls._type_fixture()

        mapper(Foo, foo, properties={
            'data': composite(Point, foo.c.x, foo.c.y)
        })
        mapper(SubFoo, subfoo, inherits=Foo)

    def test_in_place_mutation_subclass(self):
        sess = Session()
        d = Point(3, 4)
        f1 = SubFoo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data.y = 5
        sess.commit()

        eq_(f1.data, Point(3, 5))

    def test_pickle_of_parent_subclass(self):
        sess = Session()
        d = Point(3, 4)
        f1 = SubFoo(data=d)
        sess.add(f1)
        sess.commit()

        f1.data
        assert 'data' in f1.__dict__
        sess.close()

        for loads, dumps in picklers():
            sess = Session()
            f2 = loads(dumps(f1))
            sess.add(f2)
            f2.data.y = 12
            assert f2 in sess.dirty

