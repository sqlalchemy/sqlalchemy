import testbase
import re
from sqlalchemy import *
from sqlalchemy.databases import mssql
from testlib import *

msdialect = mssql.MSSQLDialect()

# TODO: migrate all MS-SQL tests here

class CompileTest(AssertMixin):
    def _test(self, statement, expected, **params):
        if len(params):
            res = str(statement.compile(dialect=msdialect, parameters=params))
        else:
            res = str(statement.compile(dialect=msdialect))
        res = re.sub(r'\n', '', res)

        assert res == expected, res
        
    def test_insert(self):
        t = table('sometable', column('somecolumn'))
        self._test(t.insert(), "INSERT INTO sometable (somecolumn) VALUES (:somecolumn)")

    def test_update(self):
        t = table('sometable', column('somecolumn'))
	self._test(t.update(t.c.somecolumn==7), "UPDATE sometable SET somecolumn=:somecolumn WHERE sometable.somecolumn = :sometable_somecolumn", somecolumn=10)

    def test_count(self):
        t = table('sometable', column('somecolumn'))
	self._test(t.count(), "SELECT count(sometable.somecolumn) AS tbl_row_count FROM sometable")
    
if __name__ == "__main__":
    testbase.main()
