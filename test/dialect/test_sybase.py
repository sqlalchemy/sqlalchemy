from sqlalchemy import extract, select
from sqlalchemy import sql
from sqlalchemy.databases import sybase
from sqlalchemy.testing import assert_raises_message, \
    fixtures, AssertsCompiledSQL


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = sybase.dialect()

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))

        mapping = {
            'day': 'day',
            'doy': 'dayofyear',
            'dow': 'weekday',
            'milliseconds': 'millisecond',
            'millisecond': 'millisecond',
            'year': 'year',
        }

        for field, subst in list(mapping.items()):
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                'SELECT DATEPART("%s", t.col1) AS anon_1 FROM t' % subst)

    def test_offset_not_supported(self):
        stmt = select([1]).offset(10)
        assert_raises_message(
            NotImplementedError,
            "Sybase ASE does not support OFFSET",
            stmt.compile, dialect=self.__dialect__
        )
