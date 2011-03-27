from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.databases import access
from test.lib import *


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = access.dialect()

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))

        mapping = {
            'month': 'm',
            'day': 'd',
            'year': 'yyyy',
            'second': 's',
            'hour': 'h',
            'doy': 'y',
            'minute': 'n',
            'quarter': 'q',
            'dow': 'w',
            'week': 'ww'
            }

        for field, subst in mapping.items():
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                'SELECT DATEPART("%s", t.col1) AS anon_1 FROM t' % subst)


