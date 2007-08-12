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
    
    def test_union(self):
        t1 = table('t1', 
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4')
            )
        t2 = table('t2',
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4'))
        
        (s1, s2) = (
                    select([t1.c.col3.label('col3'), t1.c.col4.label('col4')], t1.c.col2.in_("t1col2r1", "t1col2r2")),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')], t2.c.col2.in_("t2col2r2", "t2col2r3"))
        )        
        u = union(s1, s2, order_by=['col3', 'col4'])
        self._test(u, "SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE t1.col2 IN (:t1_col2, :t1_col2_1) UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:t2_col2, :t2_col2_1) ORDER BY col3, col4")

        self._test(u.alias('bar').select(), "SELECT bar.col3, bar.col4 FROM (SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE t1.col2 IN (:t1_col2, :t1_col2_1) UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:t2_col2, :t2_col2_1)) AS bar")
        
if __name__ == "__main__":
    testbase.main()
