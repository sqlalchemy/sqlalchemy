from sqlalchemy import func, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, Session, aliased
from sqlalchemy.testing.schema import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext import hybrid
from sqlalchemy.testing import eq_, AssertsCompiledSQL, assert_raises_message
from sqlalchemy.testing import fixtures

class PropertyComparatorTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _fixture(self):
        Base = declarative_base()

        class UCComparator(hybrid.Comparator):

            def __eq__(self, other):
                if other is None:
                    return self.expression == None
                else:
                    return func.upper(self.expression) == func.upper(other)

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)

            _value = Column("value", String)

            @hybrid.hybrid_property
            def value(self):
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
        eq_(str(A.value==5), "upper(a.value) = upper(:upper_1)")

    def test_aliased_value(self):
        A = self._fixture()
        eq_(str(aliased(A).value==5), "upper(a_1.value) = upper(:upper_1)")

    def test_query(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(A.value),
            "SELECT a.value AS a_value FROM a"
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(aliased(A).value),
            "SELECT a_1.value AS a_1_value FROM a AS a_1"
        )

    def test_aliased_filter(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
            "FROM a AS a_1 WHERE upper(a_1.value) = upper(:upper_1)"
        )

class PropertyExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'
    def _fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
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

        return A

    def _relationship_fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            b_id = Column('bid', Integer, ForeignKey('b.id'))
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
            __tablename__ = 'b'
            id = Column(Integer, primary_key=True)

            as_ = relationship("A")

        return A, B

    def test_set_get(self):
        A = self._fixture()
        a1 = A(value=5)
        eq_(a1._value, 10)
        eq_(a1.value, 5)

    def test_expression(self):
        A = self._fixture()
        self.assert_compile(
            A.value,
            "foo(a.value) + bar(a.value)"
        )

    def test_any(self):
        A, B = self._relationship_fixture()
        sess = Session()
        self.assert_compile(
            sess.query(B).filter(B.as_.any(value=5)),
            "SELECT b.id AS b_id FROM b WHERE EXISTS "
            "(SELECT 1 FROM a WHERE b.id = a.bid "
            "AND foo(a.value) + bar(a.value) = :param_1)"
        )


    def test_aliased_expression(self):
        A = self._fixture()
        self.assert_compile(
            aliased(A).value,
            "foo(a_1.value) + bar(a_1.value)"
        )

    def test_query(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(A).filter_by(value="foo"),
            "SELECT a.value AS a_value, a.id AS a_id "
            "FROM a WHERE foo(a.value) + bar(a.value) = :param_1"
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(aliased(A)).filter_by(value="foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
            "FROM a AS a_1 WHERE foo(a_1.value) + bar(a_1.value) = :param_1"
        )

class PropertyValueTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'
    def _fixture(self, assignable):
        Base = declarative_base()

        class A(Base):
            __tablename__ = 'a'
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
            AttributeError,
            "can't set attribute",
            setattr, a1, 'value', 10
        )

    def test_nondeletable(self):
        A = self._fixture(False)
        a1 = A(_value=5)
        assert_raises_message(
            AttributeError,
            "can't delete attribute",
            delattr, a1, 'value'
        )


    def test_set_get(self):
        A = self._fixture(True)
        a1 = A(value=5)
        eq_(a1.value, 5)
        eq_(a1._value, 10)

class MethodExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'
    def _fixture(self):
        Base = declarative_base()

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            _value = Column("value", String)

            @hybrid.hybrid_method
            def value(self, x):
                return int(self._value) + x

            @value.expression
            def value(cls, value):
                return func.foo(cls._value, value) + value

        return A

    def test_call(self):
        A = self._fixture()
        a1 = A(_value=10)
        eq_(a1.value(7), 17)

    def test_expression(self):
        A = self._fixture()
        self.assert_compile(
            A.value(5),
            "foo(a.value, :foo_1) + :foo_2"
        )

    def test_aliased_expression(self):
        A = self._fixture()
        self.assert_compile(
            aliased(A).value(5),
            "foo(a_1.value, :foo_1) + :foo_2"
        )

    def test_query(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(A).filter(A.value(5)=="foo"),
            "SELECT a.value AS a_value, a.id AS a_id "
            "FROM a WHERE foo(a.value, :foo_1) + :foo_2 = :param_1"
        )

    def test_aliased_query(self):
        A = self._fixture()
        sess = Session()
        a1 = aliased(A)
        self.assert_compile(
            sess.query(a1).filter(a1.value(5)=="foo"),
            "SELECT a_1.value AS a_1_value, a_1.id AS a_1_id "
            "FROM a AS a_1 WHERE foo(a_1.value, :foo_1) + :foo_2 = :param_1"
        )

    def test_query_col(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(A.value(5)),
            "SELECT foo(a.value, :foo_1) + :foo_2 AS anon_1 FROM a"
        )

    def test_aliased_query_col(self):
        A = self._fixture()
        sess = Session()
        self.assert_compile(
            sess.query(aliased(A).value(5)),
            "SELECT foo(a_1.value, :foo_1) + :foo_2 AS anon_1 FROM a AS a_1"
        )
