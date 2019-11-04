from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class MiscTest(fixtures.TestBase):
    def test_column_element_no_visit(self):
        class MyElement(ColumnElement):
            _traverse_internals = []

        eq_(sql_util.find_tables(MyElement(), check_columns=True), [])
