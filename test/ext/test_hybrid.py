from decimal import Decimal

from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext import hybrid
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.sql import update
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column


class PropertyComparatorTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self):
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

    def test_value(self):
        A = self._fixture()
        eq_(str(A.value == 5), "upper(a.value) = upper(:upper_1)")

    def test_aliased_value(self):
        A = self._fixture()
        eq_(str(aliased(A).value == 5), "upper(a_1.value) = upper(:upper_1)")

    def test_query(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(A.value), "SELECT a.value AS a_value FROM a"
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A).value),
            "SELECT a_1.value AS a_1_value FROM a AS a_1",
        )

    def test_aliased_filter(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
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

        class SomeMixin(object):
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


class PropertyExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
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
                return int(self._value) - 5

            @value.expression
            def value(cls):
                "This is a class-level docstring"
                return func.foo(cls._value) + cls.bar_value

            @value.setter
            def value(self, v):
                self._value = v + 5

            @hybrid.hybrid_property
            def bar_value(cls):
                return func.bar(cls._value)

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

    def test_info(self):
        A = self._fixture()
        inspect(A).all_orm_descriptors.value.info["some key"] = "some value"
        eq_(
            inspect(A).all_orm_descriptors.value.info,
            {"some key": "some value"},
        )

    def test_set_get(self):
        A = self._fixture()
        a1 = A(value=5)
        eq_(a1._value, 10)
        eq_(a1.value, 5)

    def test_expression(self):
        A = self._fixture()
        self.assert_compile(
            A.value.__clause_element__(), "foo(a.value) + bar(a.value)"
        )

    def test_any(self):
        A, B = self._relationship_fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(B).filter(B.as_.any(value=5)),
            "SELECT b.id AS b_id FROM b WHERE EXISTS "
            "(SELECT 1 FROM a WHERE b.id = a.bid "
            "AND foo(a.value) + bar(a.value) = :param_1)",
        )

    def test_aliased_expression(self):
        A = self._fixture()
        self.assert_compile(
            aliased(A).value.__clause_element__(),
            "foo(a_1.value) + bar(a_1.value)",
        )

    def test_query(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(A).filter_by(value="foo"),
            "SELECT a.value AS a_value, a.id AS a_id "
            "FROM a WHERE foo(a.value) + bar(a.value) = :param_1",
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = fixture_session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
            "FROM a AS a_1 WHERE foo(a_1.value) + bar(a_1.value) = :param_1",
        )

    def test_docstring(self):
        A = self._fixture()
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
            "SELECT a.value AS a_value, a.id AS a_id "
            "FROM a WHERE foo(a.value, :foo_1) + :foo_2 = :param_1",
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = fixture_session()
        a1 = aliased(A)
        self.assert_compile(
            sess.query(a1).filter(a1.value(5) == "foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
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

    def test_update_expr(self):
        Person = self.classes.Person

        statement = update(Person).values({Person.name: "Dr. No"})

        self.assert_compile(
            statement,
            "UPDATE person SET first_name=:first_name, last_name=:last_name",
            params={"first_name": "Dr.", "last_name": "No"},
        )

    def test_evaluate_hybrid_attr_indirect(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.fname2: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.fname2, "moonbeam")

    def test_evaluate_hybrid_attr_plain(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.fname: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.fname, "moonbeam")

    def test_fetch_hybrid_attr_indirect(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.fname2: "moonbeam"}, synchronize_session="fetch"
        )
        eq_(jill.fname2, "moonbeam")

    def test_fetch_hybrid_attr_plain(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.fname: "moonbeam"}, synchronize_session="fetch"
        )
        eq_(jill.fname, "moonbeam")

    def test_evaluate_hybrid_attr_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.name: "moonbeam sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "moonbeam sunshine")

    def test_fetch_hybrid_attr_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.name: "moonbeam sunshine"}, synchronize_session="fetch"
        )
        eq_(jill.name, "moonbeam sunshine")

    def test_evaluate_hybrid_attr_indirect_w_update_expr(self):
        Person = self.classes.Person

        s = fixture_session()
        jill = s.query(Person).get(3)

        s.query(Person).update(
            {Person.uname: "moonbeam sunshine"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam sunshine")


class SpecialObjectTest(fixtures.TestBase, AssertsCompiledSQL):
    """tests against hybrids that return a non-ClauseElement.

    use cases derived from the example at
    http://techspot.zzzeek.org/2011/10/21/hybrids-and-value-agnostic-types/

    """

    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy import literal

        symbols = ("usd", "gbp", "cad", "eur", "aud")
        currency_lookup = dict(
            ((currency_from, currency_to), Decimal(str(rate)))
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
        )

        class Amount(object):
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
            "SELECT bank_account.balance AS bank_account_balance, "
            "bank_account.id AS bank_account_id FROM bank_account "
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
            "SELECT bank_account.balance AS bank_account_balance, "
            "bank_account.id AS bank_account_id "
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
            "SELECT bank_account.balance AS bank_account_balance, "
            "bank_account.id AS bank_account_id FROM bank_account "
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
