import testbase
import datetime
from sqlalchemy import *
from sqlalchemy import databases, exceptions, sql
from sqlalchemy.sql.compiler import BIND_TEMPLATES
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from testlib import *

from sqlalchemy.databases import *
# every dialect in databases.__all__ is expected to pass these tests.
dialects = [getattr(databases, mod).dialect()
            for mod in databases.__all__
            # fixme!
            if mod not in ('access',)]

# if the configured dialect is out-of-tree or not yet in __all__, include it
# too.
if testbase.db.name not in databases.__all__:
    dialects.append(testbase.db.dialect)


class CompileTest(SQLCompileTest):
    def test_compile(self):
        for dialect in dialects:
            bindtemplate = BIND_TEMPLATES[dialect.paramstyle]
            self.assert_compile(func.current_timestamp(), "CURRENT_TIMESTAMP", dialect=dialect)
            self.assert_compile(func.localtime(), "LOCALTIME", dialect=dialect)
            if isinstance(dialect, firebird.dialect):
                self.assert_compile(func.nosuchfunction(), "nosuchfunction", dialect=dialect)
            else:
                self.assert_compile(func.nosuchfunction(), "nosuchfunction()", dialect=dialect)
            self.assert_compile(func.char_length('foo'), "char_length(%s)" % bindtemplate % {'name':'param_1', 'position':1}, dialect=dialect)

    def test_constructor(self):
        try:
            func.current_timestamp('somearg')
            assert False
        except TypeError:
            assert True

        try:
            func.char_length('a', 'b')
            assert False
        except TypeError:
            assert True

        try:
            func.char_length()
            assert False
        except TypeError:
            assert True

    def test_typing(self):
        assert isinstance(func.coalesce(datetime.date(2007, 10, 5), datetime.date(2005, 10, 15)).type, sqltypes.Date)

        assert isinstance(func.coalesce(None, datetime.date(2005, 10, 15)).type, sqltypes.Date)

        assert isinstance(func.concat("foo", "bar").type, sqltypes.String)

class ExecuteTest(PersistTest):

    def test_standalone_execute(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        assert (x == y == z) is True

        # ansi func
        x = testbase.db.func.current_date()
        assert isinstance(x.type, Date)
        assert isinstance(x.execute().scalar(), datetime.date)

    def test_conn_execute(self):
        conn = testbase.db.connect()
        try:
            x = conn.execute(func.current_date()).scalar()
            y = conn.execute(func.current_date().select()).scalar()
            z = conn.scalar(func.current_date())
        finally:
            conn.close()
        assert (x == y == z) is True

    def test_update(self):
        """
        Tests sending functions and SQL expressions to the VALUES and SET
        clauses of INSERT/UPDATE instances, and that column-level defaults
        get overridden.
        """

        meta = MetaData(testbase.db)
        t = Table('t1', meta,
            Column('id', Integer, Sequence('t1idseq', optional=True), primary_key=True),
            Column('value', Integer)
        )
        t2 = Table('t2', meta,
            Column('id', Integer, Sequence('t2idseq', optional=True), primary_key=True),
            Column('value', Integer, default=7),
            Column('stuff', String(20), onupdate="thisisstuff")
        )
        meta.create_all()
        try:
            t.insert(values=dict(value=func.length("one"))).execute()
            assert t.select().execute().fetchone()['value'] == 3
            t.update(values=dict(value=func.length("asfda"))).execute()
            assert t.select().execute().fetchone()['value'] == 5

            r = t.insert(values=dict(value=func.length("sfsaafsda"))).execute()
            id = r.last_inserted_ids()[0]
            assert t.select(t.c.id==id).execute().fetchone()['value'] == 9
            t.update(values={t.c.value:func.length("asdf")}).execute()
            assert t.select().execute().fetchone()['value'] == 4
            print "--------------------------"
            t2.insert().execute()
            t2.insert(values=dict(value=func.length("one"))).execute()
            t2.insert(values=dict(value=func.length("asfda") + -19)).execute(stuff="hi")

            res = exec_sorted(select([t2.c.value, t2.c.stuff]))
            self.assertEquals(res, [(-14, 'hi'), (3, None), (7, None)])

            t2.update(values=dict(value=func.length("asdsafasd"))).execute(stuff="some stuff")
            assert select([t2.c.value, t2.c.stuff]).execute().fetchall() == [(9,"some stuff"), (9,"some stuff"), (9,"some stuff")]

            t2.delete().execute()

            t2.insert(values=dict(value=func.length("one") + 8)).execute()
            assert t2.select().execute().fetchone()['value'] == 11

            t2.update(values=dict(value=func.length("asfda"))).execute()
            assert select([t2.c.value, t2.c.stuff]).execute().fetchone() == (5, "thisisstuff")

            t2.update(values={t2.c.value:func.length("asfdaasdf"), t2.c.stuff:"foo"}).execute()
            print "HI", select([t2.c.value, t2.c.stuff]).execute().fetchone()
            assert select([t2.c.value, t2.c.stuff]).execute().fetchone() == (9, "foo")
        finally:
            meta.drop_all()

    @testing.fails_on_everything_except('postgres')
    def test_as_from(self):
        # TODO: shouldnt this work on oracle too ?
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        w = select(['*'], from_obj=[testbase.db.func.current_date()]).scalar()

        # construct a column-based FROM object out of a function, like in [ticket:172]
        s = select([sql.column('date', type_=DateTime)], from_obj=[testbase.db.func.current_date()])
        q = s.execute().fetchone()[s.c.date]
        r = s.alias('datequery').select().scalar()

        assert x == y == z == w == q == r

def exec_sorted(statement, *args, **kw):
    """Executes a statement and returns a sorted list plain tuple rows."""

    return sorted([tuple(row)
                   for row in statement.execute(*args, **kw).fetchall()])

if __name__ == '__main__':
    testbase.main()
