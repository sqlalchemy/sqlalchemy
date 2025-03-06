import random
import unittest

from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from .qualify import qualify

qt_table = Table(
    "qt",
    MetaData(),
    Column("i", Integer),
    Column("p", Integer),
    Column("o", Integer),
)


class QualifyCompileTest(AssertsCompiledSQL, fixtures.CacheKeySuite):
    """A sample test suite for the QUALIFY clause, making use of SQLAlchemy
    testing utilities.

    """

    __dialect__ = "default"

    @fixtures.CacheKeySuite.run_suite_tests
    def test_qualify_cache_key(self):
        """A cache key suite using the ``CacheKeySuite.run_suite_tests``
        decorator.

        This suite intends to test that the "_traverse_internals" structure
        of the custom SQL construct covers all the structural elements of
        the object.    A decorated function should return a callable (e.g.
        a lambda) which returns a list of SQL structures.    The suite will
        call upon this lambda multiple times, to make the same list of
        SQL structures repeatedly.  It then runs comparisons of the generated
        cache key for each element in a particular list to all the other
        elements in that same list, as well as other versions of the list.

        The rules for this list are then as follows:

        * Each element of the list should store a SQL structure that is
          **structurally identical** each time, for a given position in the
          list.   Successive versions of this SQL structure will be compared
          to previous ones in the same list position and they must be
          identical.

        * Each element of the list should store a SQL structure that is
          **structurally different** from **all other** elements in the list.
          Successive versions of this SQL structure will be compared to
          other members in other list positions, and they must be different
          each time.

        * The SQL structures returned in the list should exercise all of the
          structural features that are provided by the construct.  This is
          to ensure that two different structural elements generate a
          different cache key and won't be mis-cached.

        * Literal parameters like strings and numbers are **not** part of the
          cache key itself since these are not "structural" elements; two
          SQL structures that are identical can nonetheless have different
          parameterized values.  To better exercise testing that this variation
          is not stored as part of the cache key, ``random`` functions like
          ``random.randint()`` or ``random.choice()`` can be used to generate
          random literal values within a single element.


        """

        def stmt0():
            return select(qt_table)

        def stmt1():
            stmt = stmt0()

            return stmt.ext(qualify(qt_table.c.p == random.choice([2, 6, 10])))

        def stmt2():
            stmt = stmt0()

            return stmt.ext(
                qualify(func.row_number().over(order_by=qt_table.c.o))
            )

        def stmt3():
            stmt = stmt0()

            return stmt.ext(
                qualify(
                    func.row_number().over(
                        partition_by=qt_table.c.i, order_by=qt_table.c.o
                    )
                )
            )

        return lambda: [stmt0(), stmt1(), stmt2(), stmt3()]

    def test_query_one(self):
        """A compilation test.  This makes use of the
        ``AssertsCompiledSQL.assert_compile()`` utility.

        """

        stmt = select(qt_table).ext(
            qualify(
                func.row_number().over(
                    partition_by=qt_table.c.p, order_by=qt_table.c.o
                )
                == 1
            )
        )

        self.assert_compile(
            stmt,
            "SELECT qt.i, qt.p, qt.o FROM qt QUALIFY row_number() "
            "OVER (PARTITION BY qt.p ORDER BY qt.o) = :param_1",
        )

    def test_query_two(self):
        """A compilation test.  This makes use of the
        ``AssertsCompiledSQL.assert_compile()`` utility.

        """

        row_num = (
            func.row_number()
            .over(partition_by=qt_table.c.p, order_by=qt_table.c.o)
            .label("row_num")
        )
        stmt = select(qt_table, row_num).ext(
            qualify(row_num.as_reference() == 1)
        )

        self.assert_compile(
            stmt,
            "SELECT qt.i, qt.p, qt.o, row_number() OVER "
            "(PARTITION BY qt.p ORDER BY qt.o) AS row_num "
            "FROM qt QUALIFY row_num = :param_1",
        )

    def test_propagate_attrs(self):
        """ORM propagate test.  this is an optional test that tests
        apply_propagate_attrs, indicating when you pass ORM classes /
        attributes to your construct, there's a dictionary called
        ``._propagate_attrs`` that gets carried along to the statement,
        which marks it as an "ORM" statement.

        """
        row_num = (
            func.row_number().over(partition_by=qt_table.c.p).label("row_num")
        )
        row_num._propagate_attrs = {"foo": "bar"}

        stmt = select(1).ext(qualify(row_num.as_reference() == 1))

        eq_(stmt._propagate_attrs, {"foo": "bar"})


class QualifyCompileUnittest(QualifyCompileTest, unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
