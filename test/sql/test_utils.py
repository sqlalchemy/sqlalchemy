from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.sql import base as sql_base
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
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

        subset_select = select([common.c.id, common.c.data]).alias()

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
        subset_select = select([common.c.id, calias.c.data]).subquery()

        eq_(
            set(sql_util.find_tables(subset_select, include_aliases=True)),
            {common, calias, subset_select},
        )

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
