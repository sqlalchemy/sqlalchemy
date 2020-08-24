import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.indexable import index_property
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import ARRAY
from sqlalchemy.sql.sqltypes import JSON
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import ne_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.schema import Column


class IndexPropertyTest(fixtures.TestBase):
    def test_array(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            array = Column("_array", ARRAY(Integer), default=[])
            first = index_property("array", 0)
            tenth = index_property("array", 9)

        a = A(array=[1, 2, 3])
        eq_(a.first, 1)
        assert_raises(AttributeError, lambda: a.tenth)
        a.first = 100
        eq_(a.first, 100)
        eq_(a.array, [100, 2, 3])
        del a.first
        eq_(a.first, 2)

        a2 = A(first=5)
        eq_(a2.first, 5)
        eq_(a2.array, [5])

    def test_array_longinit(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            array = Column("_array", ARRAY(Integer), default=[])
            first = index_property("array", 0)

            fifth = index_property("array", 4)

        a1 = A(fifth=10)
        a2 = A(first=5)

        eq_(a1.array, [None, None, None, None, 10])
        eq_(a2.array, [5])

        assert_raises(IndexError, setattr, a2, "fifth", 10)

    def test_json(self):
        Base = declarative_base()

        class J(Base):
            __tablename__ = "j"
            id = Column("id", Integer, primary_key=True)
            json = Column("_json", JSON, default={})
            field = index_property("json", "field")

        j = J(json={"a": 1, "b": 2})
        assert_raises(AttributeError, lambda: j.field)
        j.field = "test"
        eq_(j.field, "test")
        eq_(j.json, {"a": 1, "b": 2, "field": "test"})

        j2 = J(field="test")
        eq_(j2.json, {"field": "test"})
        eq_(j2.field, "test")

    def test_value_is_none_attributeerror(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            array = Column("_array", ARRAY(Integer))
            first = index_property("array", 1)

        a = A()
        assert_raises(AttributeError, getattr, a, "first")

        assert_raises(AttributeError, delattr, a, "first")

    def test_get_attribute_error(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            array = Column("_array", ARRAY(Integer))
            first = index_property("array", 1)

        a = A(array=[])
        assert_raises(AttributeError, lambda: a.first)

    def test_set_immutable(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            array = Column(ARRAY(Integer))
            first = index_property("array", 1, mutable=False)

        a = A()

        def set_():
            a.first = 10

        assert_raises(AttributeError, set_)

    def test_set_mutable_dict(self):
        Base = declarative_base()

        class J(Base):
            __tablename__ = "j"
            id = Column(Integer, primary_key=True)
            json = Column(JSON, default={})
            field = index_property("json", "field")

        j = J()

        j.field = 10

        j.json = {}
        assert_raises(AttributeError, lambda: j.field)
        assert_raises(AttributeError, delattr, j, "field")

        j.field = 10
        eq_(j.field, 10)

    def test_get_default_value(self):
        Base = declarative_base()

        class J(Base):
            __tablename__ = "j"
            id = Column(Integer, primary_key=True)
            json = Column(JSON, default={})
            default = index_property("json", "field", default="default")
            none = index_property("json", "field", default=None)

        j = J()
        assert j.json is None

        assert j.default == "default"
        assert j.none is None
        j.json = {}
        assert j.default == "default"
        assert j.none is None
        j.default = None
        assert j.default is None
        assert j.none is None
        j.none = 10
        assert j.default == 10
        assert j.none == 10


class IndexPropertyArrayTest(fixtures.DeclarativeMappedTest):

    __requires__ = ("array_type",)
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Array(fixtures.ComparableEntity, Base):
            __tablename__ = "array"

            id = Column(
                sa.Integer, primary_key=True, test_needs_autoincrement=True
            )
            array = Column(ARRAY(Integer), default=[])
            array0 = Column(ARRAY(Integer, zero_indexes=True), default=[])
            first = index_property("array", 0)
            first0 = index_property("array0", 0, onebased=False)

    def test_query(self):
        Array = self.classes.Array
        s = Session(testing.db)

        s.add_all(
            [
                Array(),
                Array(array=[1, 2, 3], array0=[1, 2, 3]),
                Array(array=[4, 5, 6], array0=[4, 5, 6]),
            ]
        )
        s.commit()

        a1 = s.query(Array).filter(Array.array == [1, 2, 3]).one()
        a2 = s.query(Array).filter(Array.first == 1).one()
        eq_(a1.id, a2.id)
        a3 = s.query(Array).filter(Array.first == 4).one()
        ne_(a1.id, a3.id)
        a4 = s.query(Array).filter(Array.first0 == 1).one()
        eq_(a1.id, a4.id)
        a5 = s.query(Array).filter(Array.first0 == 4).one()
        ne_(a1.id, a5.id)

    def test_mutable(self):
        Array = self.classes.Array
        s = Session(testing.db)

        a = Array(array=[1, 2, 3])
        s.add(a)
        s.commit()

        a.first = 42
        eq_(a.first, 42)
        s.commit()
        eq_(a.first, 42)

        del a.first
        eq_(a.first, 2)
        s.commit()
        eq_(a.first, 2)

    def test_modified(self):
        from sqlalchemy import inspect

        Array = self.classes.Array
        s = Session(testing.db)

        a = Array(array=[1, 2, 3])
        s.add(a)
        s.commit()

        i = inspect(a)
        is_(i.modified, False)
        in_("array", i.unmodified)

        a.first = 10

        is_(i.modified, True)
        not_in("array", i.unmodified)


class IndexPropertyJsonTest(fixtures.DeclarativeMappedTest):

    # TODO: remove reliance on "astext" for these tests
    __requires__ = ("json_type",)
    __only_on__ = "postgresql"

    __backend__ = True

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.dialects.postgresql import JSON

        Base = cls.DeclarativeBasic

        class json_property(index_property):
            def __init__(self, attr_name, index, cast_type):
                super(json_property, self).__init__(attr_name, index)
                self.cast_type = cast_type

            def expr(self, model):
                expr = super(json_property, self).expr(model)
                return expr.astext.cast(self.cast_type)

        class Json(fixtures.ComparableEntity, Base):
            __tablename__ = "json"

            id = Column(
                sa.Integer, primary_key=True, test_needs_autoincrement=True
            )
            json = Column(JSON, default={})
            field = index_property("json", "field")
            json_field = index_property("json", "field")
            int_field = json_property("json", "field", Integer)
            text_field = json_property("json", "field", Text)
            other = index_property("json", "other")
            subfield = json_property("other", "field", Text)

    def test_query(self):
        Json = self.classes.Json
        s = Session(testing.db)

        s.add_all([Json(), Json(json={"field": 10}), Json(json={"field": 20})])
        s.commit()

        a1 = (
            s.query(Json)
            .filter(Json.json["field"].astext.cast(Integer) == 10)
            .one()
        )
        a2 = s.query(Json).filter(Json.field.astext == "10").one()
        eq_(a1.id, a2.id)
        a3 = s.query(Json).filter(Json.field.astext == "20").one()
        ne_(a1.id, a3.id)

        a4 = s.query(Json).filter(Json.json_field.astext == "10").one()
        eq_(a2.id, a4.id)
        a5 = s.query(Json).filter(Json.int_field == 10).one()
        eq_(a2.id, a5.id)
        a6 = s.query(Json).filter(Json.text_field == "10").one()
        eq_(a2.id, a6.id)

    def test_mutable(self):
        Json = self.classes.Json
        s = Session(testing.db)

        j = Json(json={})
        s.add(j)
        s.commit()

        j.other = 42
        eq_(j.other, 42)
        s.commit()
        eq_(j.other, 42)

    def test_modified(self):

        Json = self.classes.Json
        s = Session(testing.db)

        j = Json(json={})
        s.add(j)
        s.commit()

        i = inspect(j)
        is_(i.modified, False)
        in_("json", i.unmodified)

        j.other = 42

        is_(i.modified, True)
        not_in("json", i.unmodified)

    def test_cast_type(self):
        Json = self.classes.Json
        s = Session(testing.db)

        j = Json(json={"field": 10})
        s.add(j)
        s.commit()

        jq = s.query(Json).filter(Json.int_field == 10).one()
        eq_(j.id, jq.id)

        jq = s.query(Json).filter(Json.text_field == "10").one()
        eq_(j.id, jq.id)

        jq = s.query(Json).filter(Json.json_field.astext == "10").one()
        eq_(j.id, jq.id)

        jq = s.query(Json).filter(Json.text_field == "wrong").first()
        is_(jq, None)

        j.json = {"field": True}
        s.commit()

        jq = s.query(Json).filter(Json.text_field == "true").one()
        eq_(j.id, jq.id)

    def test_multi_dimension(self):
        Json = self.classes.Json

        s = Session(testing.db)

        j = Json(json={"other": {"field": "multi"}})
        s.add(j)
        s.commit()

        eq_(j.other, {"field": "multi"})
        eq_(j.subfield, "multi")

        jq = s.query(Json).filter(Json.subfield == "multi").first()
        eq_(j.id, jq.id)

    def test_nested_property_init(self):
        Json = self.classes.Json

        # subfield initializer
        j = Json(subfield="a")
        eq_(j.json, {"other": {"field": "a"}})

    def test_nested_property_set(self):
        Json = self.classes.Json

        j = Json()
        j.subfield = "a"
        eq_(j.json, {"other": {"field": "a"}})
