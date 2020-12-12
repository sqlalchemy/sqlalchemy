from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.sql import base as sql_base
from sqlalchemy.sql import coercions
from sqlalchemy.sql import column
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import roles
from sqlalchemy.sql import util as sql_util
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures


class MiscTest(fixtures.TestBase):
    def test_column_element_no_visit(self):
        class MyElement(ColumnElement):
            _traverse_internals = []

        eq_(sql_util.find_tables(MyElement(), check_columns=True), [])

    def test_find_tables_selectable(self):
        metadata = MetaData()
        common = Table(
            "common",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
            Column("extra", String(45)),
        )

        subset_select = select(common.c.id, common.c.data).alias()

        eq_(set(sql_util.find_tables(subset_select)), {common})

    def test_find_tables_aliases(self):
        metadata = MetaData()
        common = Table(
            "common",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
            Column("extra", String(45)),
        )

        calias = common.alias()
        subset_select = select(common.c.id, calias.c.data).subquery()

        eq_(
            set(sql_util.find_tables(subset_select, include_aliases=True)),
            {common, calias, subset_select},
        )

    def test_incompatible_options_add_clslevel(self):
        class opt1(sql_base.CacheableOptions):
            _cache_key_traversal = []
            foo = "bar"

        with expect_raises_message(
            TypeError,
            "dictionary contains attributes not covered by "
            "Options class .*opt1.* .*'bar'.*",
        ):
            o1 = opt1

            o1 += {"foo": "f", "bar": "b"}

    def test_incompatible_options_add_instancelevel(self):
        class opt1(sql_base.CacheableOptions):
            _cache_key_traversal = []
            foo = "bar"

        o1 = opt1(foo="bat")

        with expect_raises_message(
            TypeError,
            "dictionary contains attributes not covered by "
            "Options class .*opt1.* .*'bar'.*",
        ):
            o1 += {"foo": "f", "bar": "b"}

    def test_options_merge(self):
        class opt1(sql_base.CacheableOptions):
            _cache_key_traversal = []

        class opt2(sql_base.CacheableOptions):
            _cache_key_traversal = []

            foo = "bar"

        class opt3(sql_base.CacheableOptions):
            _cache_key_traversal = []

            foo = "bar"
            bat = "hi"

        o2 = opt2.safe_merge(opt1)
        eq_(o2.__dict__, {})
        eq_(o2.foo, "bar")

        assert_raises_message(
            TypeError,
            r"other element .*opt2.* is not empty, is not of type .*opt1.*, "
            r"and contains attributes not covered here .*'foo'.*",
            opt1.safe_merge,
            opt2,
        )

        o2 = opt2 + {"foo": "bat"}
        o3 = opt2.safe_merge(o2)

        eq_(o3.foo, "bat")

        o4 = opt3.safe_merge(o2)
        eq_(o4.foo, "bat")
        eq_(o4.bat, "hi")

        assert_raises(TypeError, opt2.safe_merge, o4)

    @testing.combinations(
        (column("q"), [column("q")]),
        (column("q").desc(), [column("q")]),
        (column("q").desc().label(None), [column("q")]),
        (column("q").label(None).desc(), [column("q")]),
        (column("q").label(None).desc().label(None), [column("q")]),
        ("foo", []),  # textual label reference
        (
            select(column("q")).scalar_subquery().label(None),
            [select(column("q")).scalar_subquery().label(None)],
        ),
        (
            select(column("q")).scalar_subquery().label(None).desc(),
            [select(column("q")).scalar_subquery().label(None)],
        ),
    )
    def test_unwrap_order_by(self, expr, expected):

        expr = coercions.expect(roles.OrderByRole, expr)

        unwrapped = sql_util.unwrap_order_by(expr)

        for a, b in util.zip_longest(unwrapped, expected):
            assert a is not None and a.compare(b)
