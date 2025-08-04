from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import from_dml_column
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import LABEL_STYLE_DISAMBIGUATE_ONLY
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import literal_column
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import tuple_
from sqlalchemy.ext import hybrid
from sqlalchemy.orm import aliased
from sqlalchemy.orm import column_property
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm.context import _ORMSelectCompileState
from sqlalchemy.sql import coercions
from sqlalchemy.sql import operators
from sqlalchemy.sql import roles
from sqlalchemy.sql import update
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column


class PropertyComparatorTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self, use_inplace=False, use_classmethod=False):
        Base = declarative_base()

        class UCComparator(hybrid.Comparator):
            def __eq__(self, other):
                if other is None:
                    return self.expression is None
                else:
                    return func.upper(self.expression) == func.upper(other)

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                "This is a docstring"
                return self._value - 5

            if use_classmethod:
                if use_inplace:

                    @value.inplace.comparator
                    @classmethod
                    def _value_comparator(cls):
                        return UCComparator(cls._value)

                else:

                    @value.comparator
                    @classmethod
                    def value(cls):
                        return UCComparator(cls._value)

            else:
                if use_inplace:

                    @value.inplace.comparator
                    def _value_comparator(cls):
                        return UCComparator(cls._value)

                else:

                    @value.comparator
                    def value(cls):
                        return UCComparator(cls._value)

            @value.setter
            def value(self, v):
                self._value = v + 5

        return A

    def test_set_get(self):
        A = self._fixture()
        a1 = A(value=5)
        eq_(a1._value, 10)
        eq_(a1.value, 5)

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_value(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        eq_(str(A.value == 5), "upper(a.value) = upper(:upper_1)")

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_aliased_value(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        eq_(str(aliased(A).value == 5), "upper(a_1.value) = upper(:upper_1)")

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_query(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        sess = fixture_session()
        self.assert_compile(
            sess.query(A.value), "SELECT a.value AS a_value FROM a"
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_aliased_query(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A).value),
            "SELECT a_1.value AS a_1_value FROM a AS a_1",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_aliased_filter(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.id AS a_1_id, a_1.value AS a_1_value "
            "FROM a AS a_1 WHERE upper(a_1.value) = upper(:upper_1)",
        )

    def test_docstring(self):
        A = self._fixture()
        eq_(A.value.__doc__, "This is a docstring")

    def test_no_name_one(self):
        """test :ticket:`6215`"""

        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

            @hybrid.hybrid_property
            def same_name(self):
                return self.id

            def name1(self):
                return self.id

            different_name = hybrid.hybrid_property(name1)

            no_name = hybrid.hybrid_property(lambda self: self.name)

        stmt = select(A.same_name, A.different_name, A.no_name)
        compiled = stmt.compile()

        eq_(
            [ent._label_name for ent in compiled.compile_state._entities],
            ["same_name", "id", "name"],
        )

    def test_no_name_two(self):
        """test :ticket:`6215`"""
        Base = declarative_base()

        class SomeMixin:
            @hybrid.hybrid_property
            def same_name(self):
                return self.id

            def name1(self):
                return self.id

            different_name = hybrid.hybrid_property(name1)

            no_name = hybrid.hybrid_property(lambda self: self.name)

        class A(SomeMixin, Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        stmt = select(A.same_name, A.different_name, A.no_name)
        compiled = stmt.compile()

        eq_(
            [ent._label_name for ent in compiled.compile_state._entities],
            ["same_name", "id", "name"],
        )

    def test_custom_op(self, registry):
        """test #3162"""

        my_op = operators.custom_op(
            "my_op", python_impl=lambda a, b: a + "_foo_" + b
        )

        @registry.mapped
        class SomeClass:
            __tablename__ = "sc"
            id = Column(Integer, primary_key=True)
            data = Column(String)

            @hybrid.hybrid_property
            def foo_data(self):
                return my_op(self.data, "bar")

        eq_(SomeClass(data="data").foo_data, "data_foo_bar")

        self.assert_compile(SomeClass.foo_data, "sc.data my_op :data_1")


class PropertyExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self, use_inplace=False, use_classmethod=False):
        use_inplace, use_classmethod = bool(use_inplace), bool(use_classmethod)
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                "This is an instance-level docstring"
                return int(self._value) - 5

            @value.setter
            def value(self, v):
                self._value = v + 5

            if use_classmethod:
                if use_inplace:

                    @value.inplace.expression
                    @classmethod
                    def _value_expr(cls):
                        "This is a class-level docstring"
                        return func.foo(cls._value) + cls.bar_value

                else:

                    @value.expression
                    @classmethod
                    def value(cls):
                        "This is a class-level docstring"
                        return func.foo(cls._value) + cls.bar_value

            else:
                if use_inplace:

                    @value.inplace.expression
                    def _value_expr(cls):
                        "This is a class-level docstring"
                        return func.foo(cls._value) + cls.bar_value

                else:

                    @value.expression
                    def value(cls):
                        "This is a class-level docstring"
                        return func.foo(cls._value) + cls.bar_value

            @hybrid.hybrid_property
            def bar_value(cls):
                return func.bar(cls._value)

        return A

    def _wrong_expr_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                return self._value is not None

            @value.expression
            def value(cls):
                return cls._value is not None

        return A

    def _relationship_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            b_id = Column("bid", Integer, ForeignKey("b.id"))
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                return int(self._value) - 5

            @value.expression
            def value(cls):
                return func.foo(cls._value) + cls.bar_value

            @value.setter
            def value(self, v):
                self._value = v + 5

            @hybrid.hybrid_property
            def bar_value(cls):
                return func.bar(cls._value)

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)

            as_ = relationship("A")

        return A, B

    @testing.fixture
    def _related_polymorphic_attr_fixture(self):
        """test for #7425"""

        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

            bs = relationship("B", back_populates="a", lazy="joined")

        class B(Base):
            __tablename__ = "poly"
            __mapper_args__ = {
                "polymorphic_on": "type",
                # if with_polymorphic is removed, issue does not occur
                "with_polymorphic": "*",
            }
            name = Column(String, primary_key=True)
            type = Column(String)
            a_id = Column(ForeignKey(A.id))

            a = relationship(A, back_populates="bs")

            @hybrid.hybrid_property
            def is_foo(self):
                return self.name == "foo"

        return A, B

    def test_cloning_in_polymorphic_any(
        self, _related_polymorphic_attr_fixture
    ):
        A, B = _related_polymorphic_attr_fixture

        session = fixture_session()

        # in the polymorphic case, A.bs.any() does a traverse() / clone()
        # on the expression.  so the proxedattribute coming from the hybrid
        # has to support this.

        self.assert_compile(
            session.query(A).filter(A.bs.any(B.name == "foo")),
            "SELECT a.id AS a_id, poly_1.name AS poly_1_name, poly_1.type "
            "AS poly_1_type, poly_1.a_id AS poly_1_a_id FROM a "
            "LEFT OUTER JOIN poly AS poly_1 ON a.id = poly_1.a_id "
            "WHERE EXISTS (SELECT 1 FROM poly WHERE a.id = poly.a_id "
            "AND poly.name = :name_1)",
        )

        # SQL should be identical
        self.assert_compile(
            session.query(A).filter(A.bs.any(B.is_foo)),
            "SELECT a.id AS a_id, poly_1.name AS poly_1_name, poly_1.type "
            "AS poly_1_type, poly_1.a_id AS poly_1_a_id FROM a "
            "LEFT OUTER JOIN poly AS poly_1 ON a.id = poly_1.a_id "
            "WHERE EXISTS (SELECT 1 FROM poly WHERE a.id = poly.a_id "
            "AND poly.name = :name_1)",
        )

    @testing.fixture
    def _unnamed_expr_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            firstname = Column(String)
            lastname = Column(String)

            @hybrid.hybrid_property
            def name(self):
                return self.firstname + " " + self.lastname

        return A

    @testing.fixture
    def _unnamed_expr_matches_col_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            foo = Column(String)

            @hybrid.hybrid_property
            def bar(self):
                return self.foo

        return A

    def test_access_from_unmapped(self):
        """test #9519"""

        class DnsRecord:
            name = Column("name", String)

            @hybrid.hybrid_property
            def ip_value(self):
                return self.name[1:3]

            @ip_value.expression
            def ip_value(cls):
                return func.substring(cls.name, 1, 3)

        raw_attr = DnsRecord.ip_value
        is_(raw_attr._parententity, None)

        self.assert_compile(
            raw_attr, "substring(name, :substring_1, :substring_2)"
        )

        self.assert_compile(
            select(DnsRecord.ip_value),
            "SELECT substring(name, :substring_2, :substring_3) "
            "AS substring_1",
        )

    def test_access_from_not_yet_mapped(self, decl_base):
        """test #9519"""

        class DnsRecord(decl_base):
            __tablename__ = "dnsrecord"
            id = Column(Integer, primary_key=True)
            name = Column(String, unique=False, nullable=False)

            @declared_attr
            def thing(cls):
                return column_property(cls.ip_value)

            name = Column("name", String)

            @hybrid.hybrid_property
            def ip_value(self):
                return self.name[1:3]

            @ip_value.expression
            def ip_value(cls):
                return func.substring(cls.name, 1, 3)

        self.assert_compile(
            select(DnsRecord.thing),
            "SELECT substring(dnsrecord.name, :substring_2, :substring_3) "
            "AS substring_1 FROM dnsrecord",
        )

    def test_labeling_for_unnamed(self, _unnamed_expr_fixture):
        A = _unnamed_expr_fixture

        stmt = select(A.id, A.name)
        self.assert_compile(
            stmt,
            "SELECT a.id, a.firstname || :firstname_1 || a.lastname AS name "
            "FROM a",
        )

        eq_(stmt.subquery().c.keys(), ["id", "name"])

        self.assert_compile(
            select(stmt.subquery()),
            "SELECT anon_1.id, anon_1.name "
            "FROM (SELECT a.id AS id, a.firstname || :firstname_1 || "
            "a.lastname AS name FROM a) AS anon_1",
        )

    @testing.variation("pre_populate_col_proxy", [True, False])
    def test_labeling_for_unnamed_matches_col(
        self, _unnamed_expr_matches_col_fixture, pre_populate_col_proxy
    ):
        """test #11728"""

        A = _unnamed_expr_matches_col_fixture

        if pre_populate_col_proxy:
            pre_stmt = select(A.id, A.foo)
            pre_stmt.subquery().c

        stmt = select(A.id, A.bar)
        self.assert_compile(
            stmt,
            "SELECT a.id, a.foo FROM a",
        )

        compile_state = _ORMSelectCompileState._create_orm_context(
            stmt, toplevel=True, compiler=None
        )
        eq_(
            compile_state._column_naming_convention(
                LABEL_STYLE_DISAMBIGUATE_ONLY, legacy=False
            )(list(stmt.inner_columns)[1]),
            "bar",
        )
        eq_(stmt.subquery().c.keys(), ["id", "bar"])

        self.assert_compile(
            select(stmt.subquery()),
            "SELECT anon_1.id, anon_1.foo FROM "
            "(SELECT a.id AS id, a.foo AS foo FROM a) AS anon_1",
        )

    def test_labeling_for_unnamed_tablename_plus_col(
        self, _unnamed_expr_fixture
    ):
        A = _unnamed_expr_fixture

        stmt = select(A.id, A.name).set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        # looks like legacy query
        self.assert_compile(
            stmt,
            "SELECT a.id AS a_id, a.firstname || :firstname_1 || "
            "a.lastname AS name FROM a",
        )

        eq_(stmt.subquery().c.keys(), ["a_id", "name"])

        self.assert_compile(
            select(stmt.subquery()),
            "SELECT anon_1.a_id, anon_1.name FROM (SELECT a.id AS a_id, "
            "a.firstname || :firstname_1 || a.lastname AS name FROM a) "
            "AS anon_1",
        )

    def test_labeling_for_unnamed_legacy(self, _unnamed_expr_fixture):
        A = _unnamed_expr_fixture

        sess = fixture_session()

        stmt = sess.query(A.id, A.name)

        self.assert_compile(
            stmt,
            "SELECT a.id AS a_id, a.firstname || "
            ":firstname_1 || a.lastname AS name FROM a",
        )

        # for the subquery, we lose the "ORM-ness" from the subquery
        # so we have to carry it over using _proxy_key
        eq_(stmt.subquery().c.keys(), ["id", "name"])

        self.assert_compile(
            sess.query(stmt.subquery()),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT a.id AS id, a.firstname || :firstname_1 || "
            "a.lastname AS name FROM a) AS anon_1",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_info(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        inspect(A).all_orm_descriptors.value.info["some key"] = "some value"
        eq_(
            inspect(A).all_orm_descriptors.value.info,
            {"some key": "some value"},
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_set_get(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        a1 = A(value=5)
        eq_(a1._value, 10)
        eq_(a1.value, 5)

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_expression(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        self.assert_compile(
            A.value.__clause_element__(), "foo(a.value) + bar(a.value)"
        )

    def test_expression_isnt_clause_element(self):
        A = self._wrong_expr_fixture()

        with testing.expect_raises_message(
            exc.InvalidRequestError,
            'When interpreting attribute "A.value" as a SQL expression, '
            r"expected __clause_element__\(\) to return a "
            "ClauseElement object, got: True",
        ):
            coercions.expect(roles.ExpressionElementRole, A.value)

    def test_any(self):
        A, B = self._relationship_fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(B).filter(B.as_.any(value=5)),
            "SELECT b.id AS b_id FROM b WHERE EXISTS "
            "(SELECT 1 FROM a WHERE b.id = a.bid "
            "AND foo(a.value) + bar(a.value) = :param_1)",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_aliased_expression(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        self.assert_compile(
            aliased(A).value.__clause_element__(),
            "foo(a_1.value) + bar(a_1.value)",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_query(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        sess = fixture_session()
        self.assert_compile(
            sess.query(A).filter_by(value="foo"),
            "SELECT a.id AS a_id, a.value AS a_value "
            "FROM a WHERE foo(a.value) + bar(a.value) = :param_1",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_aliased_query(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.id AS a_1_id, a_1.value AS a_1_value "
            "FROM a AS a_1 WHERE foo(a_1.value) + bar(a_1.value) = :param_1",
        )

    @testing.variation("use_inplace", [True, False])
    @testing.variation("use_classmethod", [True, False])
    def test_docstring(self, use_inplace, use_classmethod):
        A = self._fixture(
            use_inplace=use_inplace, use_classmethod=use_classmethod
        )
        eq_(A.value.__doc__, "This is a class-level docstring")

        # no docstring here since we get a literal
        a1 = A(_value=10)
        eq_(a1.value, 5)


class PropertyValueTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self, assignable):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                return self._value - 5

            if assignable:

                @value.setter
                def value(self, v):
                    self._value = v + 5

        return A

    def test_nonassignable(self):
        A = self._fixture(False)
        a1 = A(_value=5)
        assert_raises_message(
            AttributeError, "can't set attribute", setattr, a1, "value", 10
        )

    def test_nondeletable(self):
        A = self._fixture(False)
        a1 = A(_value=5)
        assert_raises_message(
            AttributeError, "can't delete attribute", delattr, a1, "value"
        )

    def test_set_get(self):
        A = self._fixture(True)
        a1 = A(value=5)
        eq_(a1.value, 5)
        eq_(a1._value, 10)


class PropertyOverrideTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self):
        Base = declarative_base()

        class Person(Base):
            __tablename__ = "person"
            id = Column(Integer, primary_key=True)
            _name = Column(String)

            @hybrid.hybrid_property
            def name(self):
                return self._name

            @name.setter
            def name(self, value):
                self._name = value.title()

        class OverrideSetter(Person):
            __tablename__ = "override_setter"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            other = Column(String)

            @Person.name.setter
            def name(self, value):
                self._name = value.upper()

        class OverrideGetter(Person):
            __tablename__ = "override_getter"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            other = Column(String)

            @Person.name.getter
            def name(self):
                return "Hello " + self._name

        class OverrideExpr(Person):
            __tablename__ = "override_expr"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            other = Column(String)

            @Person.name.overrides.expression
            def name(self):
                return func.concat("Hello", self._name)

        class FooComparator(hybrid.Comparator):
            def __clause_element__(self):
                return func.concat("Hello", self.expression._name)

        class OverrideComparator(Person):
            __tablename__ = "override_comp"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            other = Column(String)

            @Person.name.overrides.comparator
            def name(self):
                return FooComparator(self)

        return (
            Person,
            OverrideSetter,
            OverrideGetter,
            OverrideExpr,
            OverrideComparator,
        )

    def test_property(self):
        Person, _, _, _, _ = self._fixture()
        p1 = Person()
        p1.name = "mike"
        eq_(p1._name, "Mike")
        eq_(p1.name, "Mike")

    def test_override_setter(self):
        _, OverrideSetter, _, _, _ = self._fixture()
        p1 = OverrideSetter()
        p1.name = "mike"
        eq_(p1._name, "MIKE")
        eq_(p1.name, "MIKE")

    def test_override_getter(self):
        _, _, OverrideGetter, _, _ = self._fixture()
        p1 = OverrideGetter()
        p1.name = "mike"
        eq_(p1._name, "Mike")
        eq_(p1.name, "Hello Mike")

    def test_override_expr(self):
        Person, _, _, OverrideExpr, _ = self._fixture()

        self.assert_compile(Person.name.__clause_element__(), "person._name")

        self.assert_compile(
            OverrideExpr.name.__clause_element__(),
            "concat(:concat_1, person._name)",
        )

    def test_override_comparator(self):
        Person, _, _, _, OverrideComparator = self._fixture()

        self.assert_compile(Person.name.__clause_element__(), "person._name")

        self.assert_compile(
            OverrideComparator.name.__clause_element__(),
            "concat(:concat_1, person._name)",
        )


class PropertyMirrorTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                "This is an instance-level docstring"
                return self._value

        return A

    @testing.fixture
    def _function_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            value = Column(Integer)

            @hybrid.hybrid_property
            def foo_value(self):
                return func.foo(self.value)

        return A

    @testing.fixture
    def _name_mismatch_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            addresses = relationship("B")

            @hybrid.hybrid_property
            def some_email(self):
                if self.addresses:
                    return self.addresses[0].email_address
                else:
                    return None

            @some_email.expression
            def some_email(cls):
                return B.email_address

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            aid = Column(ForeignKey("a.id"))
            email_address = Column(String)

        return A, B

    def test_dont_assume_attr_key_is_present(self, _name_mismatch_fixture):
        A, B = _name_mismatch_fixture
        self.assert_compile(
            select(A, A.some_email).join(A.addresses),
            "SELECT a.id, b.email_address FROM a JOIN b ON a.id = b.aid",
        )

    def test_dont_assume_attr_key_is_present_ac(self, _name_mismatch_fixture):
        A, B = _name_mismatch_fixture

        ac = aliased(A)
        self.assert_compile(
            select(ac, ac.some_email).join(ac.addresses),
            "SELECT a_1.id, b.email_address "
            "FROM a AS a_1 JOIN b ON a_1.id = b.aid",
        )

    def test_c_collection_func_element(self, _function_fixture):
        A = _function_fixture

        stmt = select(A.id, A.foo_value)
        eq_(stmt.subquery().c.keys(), ["id", "foo_value"])

    def test_filter_by_mismatched_col(self, _name_mismatch_fixture):
        A, B = _name_mismatch_fixture
        self.assert_compile(
            select(A).filter_by(some_email="foo").join(A.addresses),
            "SELECT a.id FROM a JOIN b ON a.id = b.aid "
            "WHERE b.email_address = :email_address_1",
        )

    def test_aliased_mismatched_col(self, _name_mismatch_fixture):
        A, B = _name_mismatch_fixture
        sess = fixture_session()

        # so what should this do ?   it's just a weird hybrid case
        self.assert_compile(
            sess.query(aliased(A).some_email),
            "SELECT b.email_address AS b_email_address FROM b",
        )

    def test_property(self):
        A = self._fixture()

        is_(A.value.property, A._value.property)

    def test_key(self):
        A = self._fixture()
        eq_(A.value.key, "value")
        eq_(A._value.key, "_value")

    def test_class(self):
        A = self._fixture()
        is_(A.value.class_, A._value.class_)

    def test_get_history(self):
        A = self._fixture()
        inst = A(_value=5)
        eq_(A.value.get_history(inst), A._value.get_history(inst))

    def test_info_not_mirrored(self):
        A = self._fixture()
        A._value.info["foo"] = "bar"
        A.value.info["bar"] = "hoho"

        eq_(A._value.info, {"foo": "bar"})
        eq_(A.value.info, {"bar": "hoho"})

    def test_info_from_hybrid(self):
        A = self._fixture()
        A._value.info["foo"] = "bar"
        A.value.info["bar"] = "hoho"

        insp = inspect(A)
        is_(insp.all_orm_descriptors["value"].info, A.value.info)


class SynonymOfPropertyTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
                return self._value

            value_syn = synonym("value")

            @hybrid.hybrid_property
            def string_value(self):
                return "foo"

            string_value_syn = synonym("string_value")

            @hybrid.hybrid_property
            def string_expr_value(self):
                return "foo"

            @string_expr_value.expression
            def string_expr_value(cls):
                return literal_column("'foo'")

            string_expr_value_syn = synonym("string_expr_value")

        return A

    def test_hasattr(self):
        A = self._fixture()

        is_false(hasattr(A.value_syn, "nonexistent"))

        is_false(hasattr(A.string_value_syn, "nonexistent"))

        is_false(hasattr(A.string_expr_value_syn, "nonexistent"))

    def test_instance_access(self):
        A = self._fixture()

        a1 = A(_value="hi")

        eq_(a1.value_syn, "hi")

        eq_(a1.string_value_syn, "foo")

        eq_(a1.string_expr_value_syn, "foo")

    def test_expression_property(self):
        A = self._fixture()

        self.assert_compile(
            select(A.id, A.value_syn).where(A.value_syn == "value"),
            "SELECT a.id, a.value FROM a WHERE a.value = :value_1",
        )

    def test_expression_expr(self):
        A = self._fixture()

        self.assert_compile(
            select(A.id, A.string_expr_value_syn).where(
                A.string_expr_value_syn == "value"
            ),
            "SELECT a.id, 'foo' FROM a WHERE 'foo' = :'foo'_1",
        )


class InplaceCreationTest(fixtures.TestBase, AssertsCompiledSQL):
    """test 'inplace' definitions added for 2.0 to assist with typing
    limitations.

    """

    __dialect__ = "default"

    def test_property_integration(self, decl_base):
        class Person(decl_base):
            __tablename__ = "person"
            id = Column(Integer, primary_key=True)
            _name = Column(String)

            @hybrid.hybrid_property
            def name(self):
                return self._name

            @name.inplace.setter
            def _name_setter(self, value):
                self._name = value.title()

            @name.inplace.expression
            def _name_expression(self):
                return func.concat("Hello", self._name)

        p1 = Person(_name="name")
        eq_(p1.name, "name")
        p1.name = "new name"
        eq_(p1.name, "New Name")

        self.assert_compile(Person.name, "concat(:concat_1, person._name)")

    def test_method_integration(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_method
            def value(self, x):
                return int(self._value) + x

            @value.inplace.expression
            def _value_expression(cls, value):
                return func.foo(cls._value, value) + value

        a1 = A(_value="10")
        eq_(a1.value(5), 15)

        self.assert_compile(A.value(column("q")), "foo(a.value, q) + q")

    def test_property_unit(self):
        def one():
            pass

        def two():
            pass

        def three():
            pass

        prop = hybrid.hybrid_property(one)

        prop2 = prop.inplace.expression(two)

        prop3 = prop.inplace.setter(three)

        is_(prop, prop2)
        is_(prop, prop3)

        def four():
            pass

        prop4 = prop.setter(four)
        is_not(prop, prop4)


class MethodExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_method
            def value(self, x):
                "This is an instance-level docstring"
                return int(self._value) + x

            @value.expression
            def value(cls, value):
                "This is a class-level docstring"
                return func.foo(cls._value, value) + value

            @hybrid.hybrid_method
            def other_value(self, x):
                "This is an instance-level docstring"
                return int(self._value) + x

            @other_value.expression
            def other_value(cls, value):
                return func.foo(cls._value, value) + value

        return A

    def test_call(self):
        A = self._fixture()
        a1 = A(_value=10)
        eq_(a1.value(7), 17)

    def test_expression(self):
        A = self._fixture()
        self.assert_compile(A.value(5), "foo(a.value, :foo_1) + :foo_2")

    def test_info(self):
        A = self._fixture()
        inspect(A).all_orm_descriptors.value.info["some key"] = "some value"
        eq_(
            inspect(A).all_orm_descriptors.value.info,
            {"some key": "some value"},
        )

    def test_aliased_expression(self):
        A = self._fixture()
        self.assert_compile(
            aliased(A).value(5), "foo(a_1.value, :foo_1) + :foo_2"
        )

    def test_query(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(A).filter(A.value(5) == "foo"),
            "SELECT a.id AS a_id, a.value AS a_value "
            "FROM a WHERE foo(a.value, :foo_1) + :foo_2 = :param_1",
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = fixture_session()
        a1 = aliased(A)
        self.assert_compile(
            sess.query(a1).filter(a1.value(5) == "foo"),
            "SELECT a_1.id AS a_1_id, a_1.value AS a_1_value "
            "FROM a AS a_1 WHERE foo(a_1.value, :foo_1) + :foo_2 = :param_1",
        )

    def test_query_col(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(A.value(5)),
            "SELECT foo(a.value, :foo_1) + :foo_2 AS anon_1 FROM a",
        )

    def test_aliased_query_col(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A).value(5)),
            "SELECT foo(a_1.value, :foo_1) + :foo_2 AS anon_1 FROM a AS a_1",
        )

    def test_docstring(self):
        A = self._fixture()
        eq_(A.value.__doc__, "This is a class-level docstring")
        eq_(A.other_value.__doc__, "This is an instance-level docstring")
        a1 = A(_value=10)

        # a1.value is still a method, so it has a
        # docstring
        eq_(a1.value.__doc__, "This is an instance-level docstring")

        eq_(a1.other_value.__doc__, "This is an instance-level docstring")


class BulkUpdateTest(fixtures.DeclarativeMappedTest, AssertsCompiledSQL):
    """Original DML test suite when we first added the ability for ORM
    UPDATE to handle hybrid values.

    """

    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Person(Base):
            __tablename__ = "person"

            id = Column(Integer, primary_key=True)
            first_name = Column(String(10))
            last_name = Column(String(10))

            @hybrid.hybrid_property
            def name(self):
                return self.first_name + " " + self.last_name

            @name.setter
            def name(self, value):
                self.first_name, self.last_name = value.split(" ", 1)

            @name.expression
            def name(cls):
                return func.concat(cls.first_name, " ", cls.last_name)

            @name.update_expression
            def name(cls, value):
                f, l = value.split(" ", 1)
                return [(cls.first_name, f), (cls.last_name, l)]

            @hybrid.hybrid_property
            def uname(self):
                return self.name

            @hybrid.hybrid_property
            def fname(self):
                return self.first_name

            @hybrid.hybrid_property
            def fname2(self):
                return self.fname

    @classmethod
    def insert_data(cls, connection):
        s = Session(connection)
        jill = cls.classes.Person(id=3, first_name="jill")
        s.add(jill)
        s.commit()

    def test_update_plain(self):
        Person = self.classes.Person

        statement = update(Person).values({Person.fname: "Dr."})

        self.assert_compile(
            statement,
            "UPDATE person SET first_name=:first_name",
            params={"first_name": "Dr."},
        )

    @testing.combinations("attr", "str", "kwarg", argnames="keytype")
    def test_update_expr(self, keytype):
        Person = self.classes.Person

        if keytype == "attr":
            statement = update(Person).values({Person.name: "Dr. No"})
        elif keytype == "str":
            statement = update(Person).values({"name": "Dr. No"})
        elif keytype == "kwarg":
            statement = update(Person).values(name="Dr. No")
        else:
            assert False

        self.assert_compile(
            statement,
            "UPDATE person SET first_name=:first_name, last_name=:last_name",
            checkparams={"first_name": "Dr.", "last_name": "No"},
        )

    @testing.combinations("attr", "str", "kwarg", argnames="keytype")
    def test_insert_expr(self, keytype):
        Person = self.classes.Person

        if keytype == "attr":
            statement = insert(Person).values({Person.name: "Dr. No"})
        elif keytype == "str":
            statement = insert(Person).values({"name": "Dr. No"})
        elif keytype == "kwarg":
            statement = insert(Person).values(name="Dr. No")
        else:
            assert False

        self.assert_compile(
            statement,
            "INSERT INTO person (first_name, last_name) VALUES "
            "(:first_name, :last_name)",
            checkparams={"first_name": "Dr.", "last_name": "No"},
        )

    # these tests all run two UPDATES to assert that caching is not
    # interfering.  this is #7209

    def test_evaluate_non_hybrid_attr(self):
        # this is a control case
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.first_name: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.first_name, "moonbeam")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.first_name: "sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.first_name, "sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "sunshine",
        )

    def test_evaluate_hybrid_attr_indirect(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.fname2: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.fname2, "moonbeam")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.fname2: "sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.fname2, "sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "sunshine",
        )

    def test_evaluate_hybrid_attr_plain(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.fname: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.fname, "moonbeam")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.fname: "sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.fname, "sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "sunshine",
        )

    def test_fetch_hybrid_attr_indirect(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.fname2: "moonbeam"}, synchronize_session="fetch"
        )
        eq_(jill.fname2, "moonbeam")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.fname2: "sunshine"}, synchronize_session="fetch"
        )
        eq_(jill.fname2, "sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "sunshine",
        )

    def test_fetch_hybrid_attr_plain(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.fname: "moonbeam"}, synchronize_session="fetch"
        )
        eq_(jill.fname, "moonbeam")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.fname: "sunshine"}, synchronize_session="fetch"
        )
        eq_(jill.fname, "sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "sunshine",
        )

    def test_evaluate_hybrid_attr_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.name: "moonbeam sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "moonbeam sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.name: "first last"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "first last")
        eq_(s.scalar(select(Person.first_name).where(Person.id == 3)), "first")

    def test_fetch_hybrid_attr_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.name: "moonbeam sunshine"}, synchronize_session="fetch"
        )
        eq_(jill.name, "moonbeam sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.name: "first last"}, synchronize_session="fetch"
        )
        eq_(jill.name, "first last")
        eq_(s.scalar(select(Person.first_name).where(Person.id == 3)), "first")

    def test_evaluate_hybrid_attr_indirect_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.get(Person, 3)

        s.query(Person).update(
            {Person.uname: "moonbeam sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam sunshine")
        eq_(
            s.scalar(select(Person.first_name).where(Person.id == 3)),
            "moonbeam",
        )

        s.query(Person).update(
            {Person.uname: "first last"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "first last")
        eq_(s.scalar(select(Person.first_name).where(Person.id == 3)), "first")


if TYPE_CHECKING:
    from sqlalchemy.sql import SQLColumnExpression


@dataclasses.dataclass(eq=False)
class Point(hybrid.Comparator):
    x: int | SQLColumnExpression[int]
    y: int | SQLColumnExpression[int]

    def operate(self, op, other, **kwargs):
        return op(self.x, other.x) & op(self.y, other.y)

    def __clause_element__(self):
        return tuple_(self.x, self.y)


class DMLTest(
    fixtures.TestBase, AssertsCompiledSQL, testing.AssertsExecutionResults
):
    """updated DML test suite when #12496 was done, where we created the use
    cases of "expansive" and "derived" hybrids and how their use cases
    differ, and also added the bulk_dml hook as well as the from_dml_column
    construct.


    """

    __dialect__ = "default"

    @testing.fixture
    def single_plain(self, decl_base):
        """fixture with a single-col hybrid"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            x: Mapped[int]

            @hybrid.hybrid_property
            def x_plain(self):
                return self.x

        return A

    @testing.fixture
    def expand_plain(self, decl_base):
        """fixture with an expand hybrid (deals w/ a value object that spans
        multiple columns)"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            x: Mapped[int]
            y: Mapped[int]

            @hybrid.hybrid_property
            def xy(self):
                return Point(self.x, self.y)

        return A

    @testing.fixture
    def expand_update(self, decl_base):
        """fixture with an expand hybrid (deals w/ a value object that spans
        multiple columns)"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            x: Mapped[int]
            y: Mapped[int]

            @hybrid.hybrid_property
            def xy(self):
                return Point(self.x, self.y)

            @xy.inplace.update_expression
            @classmethod
            def _xy(cls, value):
                return [(cls.x, value.x), (cls.y, value.y)]

        return A

    @testing.fixture
    def expand_dml(self, decl_base):
        """fixture with an expand hybrid (deals w/ a value object that spans
        multiple columns)"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            x: Mapped[int]
            y: Mapped[int]

            @hybrid.hybrid_property
            def xy(self):
                return Point(self.x, self.y)

            @xy.inplace.bulk_dml
            @classmethod
            def _xy(cls, mapping, value):
                mapping["x"] = value.x
                mapping["y"] = value.y

        return A

    @testing.fixture
    def derived_update(self, decl_base):
        """fixture with a derive hybrid (value is derived from other columns
        with data that's not in the value object itself)
        """

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            amount: Mapped[int]
            rate: Mapped[float]

            @hybrid.hybrid_property
            def adjusted_amount(self):
                return self.amount * self.rate

            @adjusted_amount.inplace.update_expression
            @classmethod
            def _adjusted_amount(cls, value):
                return [(cls.amount, value / from_dml_column(cls.rate))]

        return A

    @testing.fixture
    def derived_dml(self, decl_base):
        """fixture with a derive hybrid (value is derived from other columns
        with data that's not in the value object itself)
        """

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

            amount: Mapped[int]
            rate: Mapped[float]

            @hybrid.hybrid_property
            def adjusted_amount(self):
                return self.amount * self.rate

            @adjusted_amount.inplace.bulk_dml
            @classmethod
            def _adjusted_amount(cls, mapping, value):
                mapping["amount"] = value / mapping["rate"]

        return A

    def test_single_plain_update_values(self, single_plain):
        A = single_plain
        self.assert_compile(
            update(A).values({A.x_plain: 10}),
            "UPDATE a SET x=:x",
            checkparams={"x": 10},
        )

    def test_single_plain_insert_values(self, single_plain):
        A = single_plain
        self.assert_compile(
            insert(A).values({A.x_plain: 10}),
            "INSERT INTO a (x) VALUES (:x)",
            checkparams={"x": 10},
        )

    @testing.variation("crud", ["insert", "update"])
    def test_single_plain_bulk(self, crud, decl_base, single_plain):
        A = single_plain

        decl_base.metadata.create_all(testing.db)

        with expect_raises_message(
            exc.InvalidRequestError,
            "Can't evaluate bulk DML statement; "
            "please supply a bulk_dml decorated function",
        ):
            with Session(testing.db) as session:
                session.execute(
                    insert(A) if crud.insert else update(A),
                    [
                        {"x_plain": 10},
                        {"x_plain": 11},
                    ],
                )

    @testing.variation("keytype", ["attr", "string"])
    def test_expand_plain_update_values(self, expand_plain, keytype):
        A = expand_plain

        # SQL tuple_ update happens instead due to __clause_element__
        self.assert_compile(
            update(A)
            .where(A.xy == Point(10, 12))
            .values({"xy" if keytype.string else A.xy: Point(5, 6)}),
            "UPDATE a SET (x, y)=(:param_1, :param_2) "
            "WHERE a.x = :x_1 AND a.y = :y_1",
            {"param_1": 5, "param_2": 6, "x_1": 10, "y_1": 12},
        )

    @testing.variation("crud", ["insert", "update"])
    def test_expand_update_bulk(self, crud, expand_update, decl_base):
        A = expand_update
        decl_base.metadata.create_all(testing.db)

        with expect_raises_message(
            exc.InvalidRequestError,
            "Can't evaluate bulk DML statement; "
            "please supply a bulk_dml decorated function",
        ):
            with Session(testing.db) as session:
                session.execute(
                    insert(A) if crud.insert else update(A),
                    [
                        {"xy": Point(3, 4)},
                        {"xy": Point(5, 6)},
                    ],
                )

    @testing.variation("crud", ["insert", "update"])
    def test_expand_dml_bulk(self, crud, expand_dml, decl_base, connection):
        A = expand_dml
        decl_base.metadata.create_all(connection)

        with self.sql_execution_asserter(connection) as asserter:
            with Session(connection) as session:
                session.execute(
                    insert(A),
                    [
                        {"id": 1, "xy": Point(3, 4)},
                        {"id": 2, "xy": Point(5, 6)},
                    ],
                )

                if crud.update:
                    session.execute(
                        update(A),
                        [
                            {"id": 1, "xy": Point(10, 9)},
                            {"id": 2, "xy": Point(7, 8)},
                        ],
                    )
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO a (id, x, y) VALUES (:id, :x, :y)",
                [{"id": 1, "x": 3, "y": 4}, {"id": 2, "x": 5, "y": 6}],
            ),
            Conditional(
                crud.update,
                [
                    CompiledSQL(
                        "UPDATE a SET x=:x, y=:y WHERE a.id = :a_id",
                        [
                            {"x": 10, "y": 9, "a_id": 1},
                            {"x": 7, "y": 8, "a_id": 2},
                        ],
                    )
                ],
                [],
            ),
        )

    @testing.variation("keytype", ["attr", "string"])
    def test_expand_update_insert_values(self, expand_update, keytype):
        A = expand_update
        self.assert_compile(
            insert(A).values({"xy" if keytype.string else A.xy: Point(5, 6)}),
            "INSERT INTO a (x, y) VALUES (:x, :y)",
            checkparams={"x": 5, "y": 6},
        )

    @testing.variation("keytype", ["attr", "string"])
    def test_expand_update_update_values(self, expand_update, keytype):
        A = expand_update
        self.assert_compile(
            update(A).values({"xy" if keytype.string else A.xy: Point(5, 6)}),
            "UPDATE a SET x=:x, y=:y",
            checkparams={"x": 5, "y": 6},
        )

    #####################################################

    @testing.variation("keytype", ["attr", "string"])
    def test_derived_update_insert_values(self, derived_update, keytype):
        A = derived_update
        self.assert_compile(
            insert(A).values(
                {
                    "rate" if keytype.string else A.rate: 1.5,
                    (
                        "adjusted_amount"
                        if keytype.string
                        else A.adjusted_amount
                    ): 25,
                }
            ),
            "INSERT INTO a (amount, rate) VALUES "
            "((:param_1 / CAST(:rate AS FLOAT)), :rate)",
            checkparams={"param_1": 25, "rate": 1.5},
        )

    @testing.variation("keytype", ["attr", "string"])
    @testing.variation("rate_present", [True, False])
    def test_derived_update_update_values(
        self, derived_update, rate_present, keytype
    ):
        A = derived_update

        if rate_present:
            # when column is present in UPDATE SET, from_dml_column
            # uses that expression
            self.assert_compile(
                update(A).values(
                    {
                        "rate" if keytype.string else A.rate: 1.5,
                        (
                            "adjusted_amount"
                            if keytype.string
                            else A.adjusted_amount
                        ): 25,
                    }
                ),
                "UPDATE a SET amount=(:param_1 / CAST(:rate AS FLOAT)), "
                "rate=:rate",
                checkparams={"param_1": 25, "rate": 1.5},
            )
        else:
            # when column is not present in UPDATE SET, from_dml_column
            # renders the column, which will work in an UPDATE, but not INSERT
            self.assert_compile(
                update(A).values(
                    {
                        (
                            "adjusted_amount"
                            if keytype.string
                            else A.adjusted_amount
                        ): 25
                    }
                ),
                "UPDATE a SET amount=(:param_1 / CAST(a.rate AS FLOAT))",
                checkparams={"param_1": 25},
            )

    @testing.variation("crud", ["insert", "update"])
    def test_derived_dml_bulk(self, crud, derived_dml, decl_base, connection):
        A = derived_dml
        decl_base.metadata.create_all(connection)

        with self.sql_execution_asserter(connection) as asserter:
            with Session(connection) as session:
                session.execute(
                    insert(A),
                    [
                        {"rate": 1.5, "adjusted_amount": 25},
                        {"rate": 2.5, "adjusted_amount": 25},
                    ],
                )

                if crud.update:
                    session.execute(
                        update(A),
                        [
                            {"id": 1, "rate": 1.8, "adjusted_amount": 30},
                            {"id": 2, "rate": 2.8, "adjusted_amount": 40},
                        ],
                    )
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO a (amount, rate) VALUES (:amount, :rate)",
                [
                    {"amount": 25 / 1.5, "rate": 1.5},
                    {"amount": 25 / 2.5, "rate": 2.5},
                ],
            ),
            Conditional(
                crud.update,
                [
                    CompiledSQL(
                        "UPDATE a SET amount=:amount, rate=:rate "
                        "WHERE a.id = :a_id",
                        [
                            {"amount": 30 / 1.8, "rate": 1.8, "a_id": 1},
                            {"amount": 40 / 2.8, "rate": 2.8, "a_id": 2},
                        ],
                    )
                ],
                [],
            ),
        )


class SpecialObjectTest(fixtures.TestBase, AssertsCompiledSQL):
    """tests against hybrids that return a non-ClauseElement.

    use cases derived from the example at
    https://techspot.zzzeek.org/2011/10/21/hybrids-and-value-agnostic-types/

    """

    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy import literal

        symbols = ("usd", "gbp", "cad", "eur", "aud")
        currency_lookup = {
            (currency_from, currency_to): Decimal(str(rate))
            for currency_to, values in zip(
                symbols,
                [
                    (1, 1.59009, 0.988611, 1.37979, 1.02962),
                    (0.628895, 1, 0.621732, 0.867748, 0.647525),
                    (1.01152, 1.6084, 1, 1.39569, 1.04148),
                    (0.724743, 1.1524, 0.716489, 1, 0.746213),
                    (0.971228, 1.54434, 0.960166, 1.34009, 1),
                ],
            )
            for currency_from, rate in zip(symbols, values)
        }

        class Amount:
            def __init__(self, amount, currency):
                self.currency = currency
                self.amount = amount

            def __add__(self, other):
                return Amount(
                    self.amount + other.as_currency(self.currency).amount,
                    self.currency,
                )

            def __sub__(self, other):
                return Amount(
                    self.amount - other.as_currency(self.currency).amount,
                    self.currency,
                )

            def __lt__(self, other):
                return self.amount < other.as_currency(self.currency).amount

            def __gt__(self, other):
                return self.amount > other.as_currency(self.currency).amount

            def __eq__(self, other):
                return self.amount == other.as_currency(self.currency).amount

            def as_currency(self, other_currency):
                return Amount(
                    currency_lookup[(self.currency, other_currency)]
                    * self.amount,
                    other_currency,
                )

            def __clause_element__(self):
                # helper method for SQLAlchemy to interpret
                # the Amount object as a SQL element
                if isinstance(self.amount, (float, int, Decimal)):
                    return literal(self.amount)
                else:
                    return self.amount

            def __str__(self):
                return "%2.4f %s" % (self.amount, self.currency)

            def __repr__(self):
                return "Amount(%r, %r)" % (self.amount, self.currency)

        Base = declarative_base()

        class BankAccount(Base):
            __tablename__ = "bank_account"
            id = Column(Integer, primary_key=True)

            _balance = Column("balance", Numeric)

            @hybrid.hybrid_property
            def balance(self):
                """Return an Amount view of the current balance."""
                return Amount(self._balance, "usd")

            @balance.setter
            def balance(self, value):
                self._balance = value.as_currency("usd").amount

        cls.Amount = Amount
        cls.BankAccount = BankAccount

    def test_instance_one(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        account = BankAccount(balance=Amount(4000, "usd"))

        # 3b. print balance in usd
        eq_(account.balance.amount, 4000)

    def test_instance_two(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        account = BankAccount(balance=Amount(4000, "usd"))

        # 3c. print balance in gbp
        eq_(account.balance.as_currency("gbp").amount, Decimal("2515.58"))

    def test_instance_three(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        account = BankAccount(balance=Amount(4000, "usd"))

        # 3d. perform currency-agnostic comparisons, math
        is_(account.balance > Amount(500, "cad"), True)

    def test_instance_four(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        account = BankAccount(balance=Amount(4000, "usd"))
        eq_(
            account.balance + Amount(500, "cad") - Amount(50, "eur"),
            Amount(Decimal("4425.316"), "usd"),
        )

    def test_query_one(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        session = fixture_session()

        query = session.query(BankAccount).filter(
            BankAccount.balance == Amount(10000, "cad")
        )

        self.assert_compile(
            query,
            "SELECT bank_account.id AS bank_account_id, "
            "bank_account.balance AS bank_account_balance "
            "FROM bank_account "
            "WHERE bank_account.balance = :balance_1",
            checkparams={"balance_1": Decimal("9886.110000")},
        )

    def test_query_two(self):
        BankAccount, Amount = self.BankAccount, self.Amount
        session = fixture_session()

        # alternatively we can do the calc on the DB side.
        query = (
            session.query(BankAccount)
            .filter(
                BankAccount.balance.as_currency("cad") > Amount(9999, "cad")
            )
            .filter(
                BankAccount.balance.as_currency("cad") < Amount(10001, "cad")
            )
        )
        self.assert_compile(
            query,
            "SELECT bank_account.id AS bank_account_id, "
            "bank_account.balance AS bank_account_balance "
            "FROM bank_account "
            "WHERE :balance_1 * bank_account.balance > :param_1 "
            "AND :balance_2 * bank_account.balance < :param_2",
            checkparams={
                "balance_1": Decimal("1.01152"),
                "balance_2": Decimal("1.01152"),
                "param_1": Decimal("9999"),
                "param_2": Decimal("10001"),
            },
        )

    def test_query_three(self):
        BankAccount = self.BankAccount
        session = fixture_session()

        query = session.query(BankAccount).filter(
            BankAccount.balance.as_currency("cad")
            > BankAccount.balance.as_currency("eur")
        )
        self.assert_compile(
            query,
            "SELECT bank_account.id AS bank_account_id, "
            "bank_account.balance AS bank_account_balance "
            "FROM bank_account "
            "WHERE :balance_1 * bank_account.balance > "
            ":param_1 * :balance_2 * bank_account.balance",
            checkparams={
                "balance_1": Decimal("1.01152"),
                "balance_2": Decimal("0.724743"),
                "param_1": Decimal("1.39569"),
            },
        )

    def test_query_four(self):
        BankAccount = self.BankAccount
        session = fixture_session()

        # 4c. query all amounts, converting to "CAD" on the DB side
        query = session.query(BankAccount.balance.as_currency("cad").amount)
        self.assert_compile(
            query,
            "SELECT :balance_1 * bank_account.balance AS anon_1 "
            "FROM bank_account",
            checkparams={"balance_1": Decimal("1.01152")},
        )

    def test_query_five(self):
        BankAccount = self.BankAccount
        session = fixture_session()

        # 4d. average balance in EUR
        query = session.query(func.avg(BankAccount.balance.as_currency("eur")))
        self.assert_compile(
            query,
            "SELECT avg(:balance_1 * bank_account.balance) AS avg_1 "
            "FROM bank_account",
            checkparams={"balance_1": Decimal("0.724743")},
        )

    def test_docstring(self):
        BankAccount = self.BankAccount
        eq_(
            BankAccount.balance.__doc__,
            "Return an Amount view of the current balance.",
        )
