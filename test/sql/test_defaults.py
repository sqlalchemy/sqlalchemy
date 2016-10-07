from sqlalchemy.testing import eq_, assert_raises_message, \
    assert_raises, AssertsCompiledSQL, expect_warnings
import datetime
from sqlalchemy.schema import CreateSequence, DropSequence, CreateTable
from sqlalchemy.sql import select, text, literal_column
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing import engines
from sqlalchemy import (
    MetaData, Integer, String, ForeignKey, Boolean, exc, Sequence, func,
    literal, Unicode, cast)
from sqlalchemy.types import TypeDecorator, TypeEngine
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.dialects import sqlite
from sqlalchemy.testing import fixtures
from sqlalchemy.util import u, b
from sqlalchemy import util
import itertools

t = f = f2 = ts = currenttime = metadata = default_generator = None


class DDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_string(self):
        # note: that the datatype is an Integer here doesn't matter,
        # the server_default is interpreted independently of the
        # column's datatype.
        m = MetaData()
        t = Table('t', m, Column('x', Integer, server_default='5'))
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT '5')"
        )

    def test_string_w_quotes(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, server_default="5'6"))
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT '5''6')"
        )

    def test_text(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, server_default=text('5 + 8')))
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT 5 + 8)"
        )

    def test_text_w_quotes(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, server_default=text("5 ' 8")))
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT 5 ' 8)"
        )

    def test_literal_binds_w_quotes(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer,
                  server_default=literal("5 ' 8")))
        self.assert_compile(
            CreateTable(t),
            """CREATE TABLE t (x INTEGER DEFAULT '5 '' 8')"""
        )

    def test_text_literal_binds(self):
        m = MetaData()
        t = Table(
            't', m,
            Column(
                'x', Integer, server_default=text('q + :x1').bindparams(x1=7)))
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT q + 7)"
        )

    def test_sqlexpr(self):
        m = MetaData()
        t = Table('t', m, Column(
            'x', Integer,
            server_default=literal_column('a') + literal_column('b'))
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT a + b)"
        )

    def test_literal_binds_plain(self):
        m = MetaData()
        t = Table('t', m, Column(
            'x', Integer,
            server_default=literal('a') + literal('b'))
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER DEFAULT 'a' || 'b')"
        )

    def test_literal_binds_pgarray(self):
        from sqlalchemy.dialects.postgresql import ARRAY, array
        m = MetaData()
        t = Table('t', m, Column(
            'x', ARRAY(Integer),
            server_default=array([1, 2, 3]))
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER[] DEFAULT ARRAY[1, 2, 3])",
            dialect='postgresql'
        )


class DefaultTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global t, f, f2, ts, currenttime, metadata, default_generator

        db = testing.db
        metadata = MetaData(db)
        default_generator = {'x': 50}

        def mydefault():
            default_generator['x'] += 1
            return default_generator['x']

        def myupdate_with_ctx(ctx):
            conn = ctx.connection
            return conn.execute(sa.select([sa.text('13')])).scalar()

        def mydefault_using_connection(ctx):
            conn = ctx.connection
            try:
                return conn.execute(sa.select([sa.text('12')])).scalar()
            finally:
                # ensure a "close()" on this connection does nothing,
                # since its a "branched" connection
                conn.close()

        use_function_defaults = testing.against('postgresql', 'mssql')
        is_oracle = testing.against('oracle')

        class MyClass(object):

            @classmethod
            def gen_default(cls, ctx):
                return "hi"

        class MyType(TypeDecorator):
            impl = String(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = "BIND" + value
                return value

        # select "count(1)" returns different results on different DBs also
        # correct for "current_date" compatible as column default, value
        # differences
        currenttime = func.current_date(type_=sa.Date, bind=db)
        if is_oracle:
            ts = db.scalar(
                sa.select(
                    [
                        func.trunc(
                            func.sysdate(), sa.literal_column("'DAY'"),
                            type_=sa.Date)]))
            assert isinstance(ts, datetime.date) and not isinstance(
                ts, datetime.datetime)
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(
                currenttime, sa.literal_column("'DAY'"), bind=db,
                type_=sa.Date)
            def1 = currenttime
            def2 = func.trunc(
                sa.text("sysdate"), sa.literal_column("'DAY'"), type_=sa.Date)

            deftype = sa.Date
        elif use_function_defaults:
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            def1 = currenttime
            deftype = sa.Date
            if testing.against('mssql'):
                def2 = sa.text("getdate()")
            else:
                def2 = sa.text("current_date")
            ts = db.scalar(func.current_date())
        else:
            f = len('abcdef')
            f2 = len('abcdefghijk')
            def1 = def2 = "3"
            ts = 3
            deftype = Integer

        t = Table(
            'default_test1', metadata,
            # python function
            Column('col1', Integer, primary_key=True,
                   default=mydefault),

            # python literal
            Column('col2', String(20),
                   default="imthedefault",
                   onupdate="im the update"),

            # preexecute expression
            Column('col3', Integer,
                   default=func.length('abcdef'),
                   onupdate=func.length('abcdefghijk')),

            # SQL-side default from sql expression
            Column('col4', deftype,
                   server_default=def1),

            # SQL-side default from literal expression
            Column('col5', deftype,
                   server_default=def2),

            # preexecute + update timestamp
            Column('col6', sa.Date,
                   default=currenttime,
                   onupdate=currenttime),

            Column('boolcol1', sa.Boolean, default=True),
            Column('boolcol2', sa.Boolean, default=False),

            # python function which uses ExecutionContext
            Column('col7', Integer,
                   default=mydefault_using_connection,
                   onupdate=myupdate_with_ctx),

            # python builtin
            Column('col8', sa.Date,
                   default=datetime.date.today,
                   onupdate=datetime.date.today),
            # combo
            Column('col9', String(20),
                   default='py',
                   server_default='ddl'),

            # python method w/ context
            Column('col10', String(20), default=MyClass.gen_default),

            # fixed default w/ type that has bound processor
            Column('col11', MyType(), default='foo')
        )

        t.create()

    @classmethod
    def teardown_class(cls):
        t.drop()

    def teardown(self):
        default_generator['x'] = 50
        t.delete().execute()

    def test_bad_arg_signature(self):
        ex_msg = "ColumnDefault Python function takes zero " \
            "or one positional arguments"

        def fn1(x, y):
            pass

        def fn2(x, y, z=3):
            pass

        class fn3(object):

            def __init__(self, x, y):
                pass

        class FN4(object):

            def __call__(self, x, y):
                pass
        fn4 = FN4()

        for fn in fn1, fn2, fn3, fn4:
            assert_raises_message(
                sa.exc.ArgumentError, ex_msg, sa.ColumnDefault, fn)

    def test_arg_signature(self):

        def fn1():
            pass

        def fn2():
            pass

        def fn3(x=1):
            eq_(x, 1)

        def fn4(x=1, y=2, z=3):
            eq_(x, 1)
        fn5 = list

        class fn6a(object):

            def __init__(self, x):
                eq_(x, "context")

        class fn6b(object):

            def __init__(self, x, y=3):
                eq_(x, "context")

        class FN7(object):

            def __call__(self, x):
                eq_(x, "context")
        fn7 = FN7()

        class FN8(object):

            def __call__(self, x, y=3):
                eq_(x, "context")
        fn8 = FN8()

        for fn in fn1, fn2, fn3, fn4, fn5, fn6a, fn6b, fn7, fn8:
            c = sa.ColumnDefault(fn)
            c.arg("context")


    @testing.fails_on('firebird', 'Data type unknown')
    def test_standalone(self):
        c = testing.db.engine.contextual_connect()
        x = c.execute(t.c.col1.default)
        y = t.c.col2.default.execute()
        z = c.execute(t.c.col3.default)
        assert 50 <= x <= 57
        eq_(y, 'imthedefault')
        eq_(z, f)
        eq_(f2, 11)

    def test_py_vs_server_default_detection(self):

        def has_(name, *wanted):
            slots = [
                'default', 'onupdate', 'server_default', 'server_onupdate']
            col = tbl.c[name]
            for slot in wanted:
                slots.remove(slot)
                assert getattr(col, slot) is not None, getattr(col, slot)
            for slot in slots:
                assert getattr(col, slot) is None, getattr(col, slot)

        tbl = t
        has_('col1', 'default')
        has_('col2', 'default', 'onupdate')
        has_('col3', 'default', 'onupdate')
        has_('col4', 'server_default')
        has_('col5', 'server_default')
        has_('col6', 'default', 'onupdate')
        has_('boolcol1', 'default')
        has_('boolcol2', 'default')
        has_('col7', 'default', 'onupdate')
        has_('col8', 'default', 'onupdate')
        has_('col9', 'default', 'server_default')

        ColumnDefault, DefaultClause = sa.ColumnDefault, sa.DefaultClause

        t2 = Table('t2', MetaData(),
                   Column('col1', Integer, Sequence('foo')),
                   Column('col2', Integer,
                          default=Sequence('foo'),
                          server_default='y'),
                   Column('col3', Integer,
                          Sequence('foo'),
                          server_default='x'),
                   Column('col4', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y')),
                   Column('col4', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y'),
                          DefaultClause('y', for_update=True)),
                   Column('col5', Integer,
                          ColumnDefault('x'),
                          DefaultClause('y'),
                          onupdate='z'),
                   Column('col6', Integer,
                          ColumnDefault('x'),
                          server_default='y',
                          onupdate='z'),
                   Column('col7', Integer,
                          default='x',
                          server_default='y',
                          onupdate='z'),
                   Column('col8', Integer,
                          server_onupdate='u',
                          default='x',
                          server_default='y',
                          onupdate='z'))
        tbl = t2
        has_('col1', 'default')
        has_('col2', 'default', 'server_default')
        has_('col3', 'default', 'server_default')
        has_('col4', 'default', 'server_default', 'server_onupdate')
        has_('col5', 'default', 'server_default', 'onupdate')
        has_('col6', 'default', 'server_default', 'onupdate')
        has_('col7', 'default', 'server_default', 'onupdate')
        has_(
            'col8', 'default', 'server_default', 'onupdate', 'server_onupdate')

    @testing.fails_on('firebird', 'Data type unknown')
    def test_insert(self):
        r = t.insert().execute()
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        r = t.insert(inline=True).execute()
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        t.insert().execute()

        ctexec = sa.select(
            [currenttime.label('now')], bind=testing.db).scalar()
        l = t.select().order_by(t.c.col1).execute()
        today = datetime.date.today()
        eq_(l.fetchall(), [
            (x, 'imthedefault', f, ts, ts, ctexec, True, False,
             12, today, 'py', 'hi', 'BINDfoo')
            for x in range(51, 54)])

        t.insert().execute(col9=None)
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))

        eq_(t.select(t.c.col1 == 54).execute().fetchall(),
            [(54, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, None, 'hi', 'BINDfoo')])

    def test_insertmany(self):
        t.insert().execute({}, {}, {})

        ctexec = currenttime.scalar()
        l = t.select().execute()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo'),
             (52, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo'),
             (53, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo')])

    @testing.requires.multivalues_inserts
    def test_insert_multivalues(self):

        t.insert().values([{}, {}, {}]).execute()

        ctexec = currenttime.scalar()
        l = t.select().execute()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo'),
             (52, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo'),
             (53, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py', 'hi', 'BINDfoo')])

    def test_no_embed_in_sql(self):
        """Using a DefaultGenerator, Sequence, DefaultClause
        in the columns, where clause of a select, or in the values
        clause of insert, update, raises an informative error"""

        for const in (
            sa.Sequence('y'),
            sa.ColumnDefault('y'),
            sa.DefaultClause('y')
        ):
            assert_raises_message(
                sa.exc.ArgumentError,
                "SQL expression object or string expected, got object of type "
                "<.* 'list'> instead",
                t.select, [const]
            )
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "cannot be used directly as a column expression.",
                str, t.insert().values(col4=const)
            )
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "cannot be used directly as a column expression.",
                str, t.update().values(col4=const)
            )

    def test_missing_many_param(self):
        assert_raises_message(
            exc.StatementError,
            "A value is required for bind parameter 'col7', in parameter "
            "group 1",
            t.insert().execute,
            {'col4': 7, 'col7': 12, 'col8': 19},
            {'col4': 7, 'col8': 19},
            {'col4': 7, 'col7': 12, 'col8': 19},
        )

    def test_insert_values(self):
        t.insert(values={'col3': 50}).execute()
        l = t.select().execute()
        eq_(50, l.first()['col3'])

    @testing.fails_on('firebird', 'Data type unknown')
    def test_updatemany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql+mysqldb') and
                testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        t.insert().execute({}, {}, {})

        t.update(t.c.col1 == sa.bindparam('pkval')).execute(
            {'pkval': 51, 'col7': None, 'col8': None, 'boolcol1': False})

        t.update(t.c.col1 == sa.bindparam('pkval')).execute(
            {'pkval': 51},
            {'pkval': 52},
            {'pkval': 53})

        l = t.select().execute()
        ctexec = currenttime.scalar()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'im the update', f2, ts, ts, ctexec, False, False,
              13, today, 'py', 'hi', 'BINDfoo'),
             (52, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py', 'hi', 'BINDfoo'),
             (53, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py', 'hi', 'BINDfoo')])

    @testing.fails_on('firebird', 'Data type unknown')
    def test_update(self):
        r = t.insert().execute()
        pk = r.inserted_primary_key[0]
        t.update(t.c.col1 == pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        l = t.select(t.c.col1 == pk).execute()
        l = l.first()
        eq_(l,
            (pk, 'im the update', f2, None, None, ctexec, True, False,
             13, datetime.date.today(), 'py', 'hi', 'BINDfoo'))
        eq_(11, f2)

    @testing.fails_on('firebird', 'Data type unknown')
    def test_update_values(self):
        r = t.insert().execute()
        pk = r.inserted_primary_key[0]
        t.update(t.c.col1 == pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1 == pk).execute()
        l = l.first()
        eq_(55, l['col3'])


class CTEDefaultTest(fixtures.TablesTest):
    __requires__ = ('ctes',)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'q', metadata,
            Column('x', Integer, default=2),
            Column('y', Integer, onupdate=5),
            Column('z', Integer)
        )

        Table(
            'p', metadata,
            Column('s', Integer),
            Column('t', Integer),
            Column('u', Integer, onupdate=1)
        )

    def _test_a_in_b(self, a, b):
        q = self.tables.q
        p = self.tables.p

        with testing.db.connect() as conn:
            if a == 'delete':
                conn.execute(q.insert().values(y=10, z=1))
                cte = q.delete().\
                    where(q.c.z == 1).returning(q.c.z).cte('c')
                expected = None
            elif a == "insert":
                cte = q.insert().values(z=1, y=10).returning(q.c.z).cte('c')
                expected = (2, 10)
            elif a == "update":
                conn.execute(q.insert().values(x=5, y=10, z=1))
                cte = q.update().\
                    where(q.c.z == 1).values(x=7).returning(q.c.z).cte('c')
                expected = (7, 5)
            elif a == "select":
                conn.execute(q.insert().values(x=5, y=10, z=1))
                cte = sa.select([q.c.z]).cte('c')
                expected = (5, 10)

            if b == "select":
                conn.execute(p.insert().values(s=1))
                stmt = select([p.c.s, cte.c.z])
            elif b == "insert":
                sel = select([1, cte.c.z, ])
                stmt = p.insert().from_select(['s', 't'], sel).returning(
                    p.c.s, p.c.t)
            elif b == "delete":
                stmt = p.insert().values(s=1, t=cte.c.z).returning(
                    p.c.s, cte.c.z)
            elif b == "update":
                conn.execute(p.insert().values(s=1))
                stmt = p.update().values(t=5).\
                    where(p.c.s == cte.c.z).\
                    returning(p.c.u, cte.c.z)
            eq_(
                conn.execute(stmt).fetchall(),
                [(1, 1)]
            )

            eq_(
                conn.execute(select([q.c.x, q.c.y])).fetchone(),
                expected
            )

    def test_update_in_select(self):
        self._test_a_in_b("update", "select")

    def test_delete_in_select(self):
        self._test_a_in_b("update", "select")

    def test_insert_in_select(self):
        self._test_a_in_b("update", "select")

    def test_select_in_update(self):
        self._test_a_in_b("select", "update")

    def test_select_in_insert(self):
        self._test_a_in_b("select", "insert")

    # TODO: updates / inserts can be run in one statement w/ CTE ?
    # deletes?


class PKDefaultTest(fixtures.TablesTest):
    __requires__ = ('subqueries',)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        t2 = Table(
            't2', metadata,
            Column('nextid', Integer))

        Table(
            't1', metadata,
            Column(
                'id', Integer, primary_key=True,
                default=sa.select([func.max(t2.c.nextid)]).as_scalar()),
            Column('data', String(30)))

    @testing.requires.returning
    def test_with_implicit_returning(self):
        self._test(True)

    def test_regular(self):
        self._test(False)

    def _test(self, returning):
        t2, t1 = self.tables.t2, self.tables.t1

        if not returning and not testing.db.dialect.implicit_returning:
            engine = testing.db
        else:
            engine = engines.testing_engine(
                options={'implicit_returning': returning})
        engine.execute(t2.insert(), nextid=1)
        r = engine.execute(t1.insert(), data='hi')
        eq_([1], r.inserted_primary_key)

        engine.execute(t2.insert(), nextid=2)
        r = engine.execute(t1.insert(), data='there')
        eq_([2], r.inserted_primary_key)


class PKIncrementTest(fixtures.TablesTest):
    run_define_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("aitable", metadata,
              Column('id', Integer, Sequence('ai_id_seq', optional=True),
                     primary_key=True),
              Column('int1', Integer),
              Column('str1', String(20)))

    # TODO: add coverage for increment on a secondary column in a key
    @testing.fails_on('firebird', 'Data type unknown')
    def _test_autoincrement(self, bind):
        aitable = self.tables.aitable

        ids = set()
        rs = bind.execute(aitable.insert(), int1=1)
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), str1='row 2')
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), int1=3, str1='row 3')
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(values={'int1': func.length('four')}))
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        eq_(ids, set([1, 2, 3, 4]))

        eq_(list(bind.execute(aitable.select().order_by(aitable.c.id))),
            [(1, 1, None), (2, None, 'row 2'), (3, 3, 'row 3'), (4, 4, None)])

    def test_autoincrement_autocommit(self):
        self._test_autoincrement(testing.db)

    def test_autoincrement_transaction(self):
        con = testing.db.connect()
        tx = con.begin()
        try:
            try:
                self._test_autoincrement(con)
            except:
                try:
                    tx.rollback()
                except:
                    pass
                raise
            else:
                tx.commit()
        finally:
            con.close()


class EmptyInsertTest(fixtures.TestBase):
    __backend__ = True

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no empty insert support')
    @testing.fails_on('oracle', 'FIXME: unknown')
    @testing.provide_metadata
    def test_empty_insert(self):
        t1 = Table(
            't1', self.metadata,
            Column('is_true', Boolean, server_default=('1')))
        self.metadata.create_all()
        t1.insert().execute()
        eq_(1, select([func.count(text('*'))], from_obj=t1).scalar())
        eq_(True, t1.select().scalar())


class AutoIncrementTest(fixtures.TablesTest):
    __requires__ = ('identity',)
    run_define_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        """Each test manipulates self.metadata individually."""

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_autoincrement_single_col(self):
        single = Table('single', self.metadata,
                       Column('id', Integer, primary_key=True))
        single.create()

        r = single.insert().execute()
        id_ = r.inserted_primary_key[0]
        eq_(id_, 1)
        eq_(1, sa.select([func.count(sa.text('*'))], from_obj=single).scalar())

    def test_autoincrement_fk(self):
        nodes = Table(
            'nodes', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))
        nodes.create()

        r = nodes.insert().execute(data='foo')
        id_ = r.inserted_primary_key[0]
        nodes.insert().execute(data='bar', parent_id=id_)

    def test_autoinc_detection_no_affinity(self):
        class MyType(TypeDecorator):
            impl = TypeEngine

        assert MyType()._type_affinity is None
        t = Table(
            'x', MetaData(),
            Column('id', MyType(), primary_key=True)
        )
        assert t._autoincrement_column is None

    def test_autoincrement_ignore_fk(self):
        m = MetaData()
        Table(
            'y', m,
            Column('id', Integer(), primary_key=True)
        )
        x = Table(
            'x', m,
            Column(
                'id', Integer(), ForeignKey('y.id'),
                autoincrement="ignore_fk", primary_key=True)
        )
        assert x._autoincrement_column is x.c.id

    def test_autoincrement_fk_disqualifies(self):
        m = MetaData()
        Table(
            'y', m,
            Column('id', Integer(), primary_key=True)
        )
        x = Table(
            'x', m,
            Column('id', Integer(), ForeignKey('y.id'), primary_key=True)
        )
        assert x._autoincrement_column is None

    @testing.only_on("sqlite")
    def test_non_autoincrement(self):
        # sqlite INT primary keys can be non-unique! (only for ints)
        nonai = Table(
            "nonaitest", self.metadata,
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai.create()

        def go():
            # postgresql + mysql strict will fail on first row,
            # mysql in legacy mode fails on second row
            nonai.insert().execute(data='row 1')
            nonai.insert().execute(data='row 2')

        # just testing SQLite for now, it passes
        with expect_warnings(
            ".*has no Python-side or server-side default.*",
        ):
            go()

    def test_col_w_sequence_non_autoinc_no_firing(self):
        metadata = self.metadata
        # plain autoincrement/PK table in the actual schema
        Table(
            "x", metadata,
            Column("set_id", Integer, primary_key=True)
        )
        metadata.create_all()

        # for the INSERT use a table with a Sequence
        # and autoincrement=False.  Using a ForeignKey
        # would have the same effect
        dataset_no_autoinc = Table(
            "x", MetaData(),
            Column(
                "set_id", Integer, Sequence("some_seq"),
                primary_key=True, autoincrement=False)
        )

        testing.db.execute(dataset_no_autoinc.insert())
        eq_(
            testing.db.scalar(
                select([func.count('*')]).select_from(dataset_no_autoinc)), 1
        )


class SequenceDDLTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'
    __backend__ = True

    def test_create_drop_ddl(self):
        self.assert_compile(
            CreateSequence(Sequence('foo_seq')),
            "CREATE SEQUENCE foo_seq",
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', start=5)),
            "CREATE SEQUENCE foo_seq START WITH 5",
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', increment=2)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2",
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', increment=2, start=5)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 5",
        )

        self.assert_compile(
            CreateSequence(Sequence(
                            'foo_seq', increment=2, start=0, minvalue=0)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 0 MINVALUE 0",
        )

        self.assert_compile(
            CreateSequence(Sequence(
                            'foo_seq', increment=2, start=1, maxvalue=5)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 1 MAXVALUE 5",
        )

        self.assert_compile(
            CreateSequence(Sequence(
                            'foo_seq', increment=2, start=1, nomaxvalue=True)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 1 NO MAXVALUE",
        )

        self.assert_compile(
            CreateSequence(Sequence(
                            'foo_seq', increment=2, start=0, nominvalue=True)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 0 NO MINVALUE",
        )

        self.assert_compile(
            CreateSequence(Sequence(
                            'foo_seq', start=1, maxvalue=10, cycle=True)),
            "CREATE SEQUENCE foo_seq START WITH 1 MAXVALUE 10 CYCLE",
        )

        self.assert_compile(
            DropSequence(Sequence('foo_seq')),
            "DROP SEQUENCE foo_seq",
        )


class SequenceExecTest(fixtures.TestBase):
    __requires__ = ('sequences',)
    __backend__ = True

    @classmethod
    def setup_class(cls):
        cls.seq = Sequence("my_sequence")
        cls.seq.create(testing.db)

    @classmethod
    def teardown_class(cls):
        cls.seq.drop(testing.db)

    def _assert_seq_result(self, ret):
        """asserts return of next_value is an int"""

        assert isinstance(ret, util.int_types)
        assert ret > 0

    def test_implicit_connectionless(self):
        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        self._assert_seq_result(s.execute())

    def test_explicit(self):
        s = Sequence("my_sequence")
        self._assert_seq_result(s.execute(testing.db))

    def test_explicit_optional(self):
        """test dialect executes a Sequence, returns nextval, whether
        or not "optional" is set """

        s = Sequence("my_sequence", optional=True)
        self._assert_seq_result(s.execute(testing.db))

    def test_func_implicit_connectionless_execute(self):
        """test func.next_value().execute()/.scalar() works
        with connectionless execution. """

        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        self._assert_seq_result(s.next_value().execute().scalar())

    def test_func_explicit(self):
        s = Sequence("my_sequence")
        self._assert_seq_result(testing.db.scalar(s.next_value()))

    def test_func_implicit_connectionless_scalar(self):
        """test func.next_value().execute()/.scalar() works. """

        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        self._assert_seq_result(s.next_value().scalar())

    def test_func_embedded_select(self):
        """test can use next_value() in select column expr"""

        s = Sequence("my_sequence")
        self._assert_seq_result(
            testing.db.scalar(select([s.next_value()]))
        )

    @testing.fails_on('oracle', "ORA-02287: sequence number not allowed here")
    @testing.provide_metadata
    def test_func_embedded_whereclause(self):
        """test can use next_value() in whereclause"""

        metadata = self.metadata
        t1 = Table(
            't', metadata,
            Column('x', Integer)
        )
        t1.create(testing.db)
        testing.db.execute(t1.insert(), [{'x': 1}, {'x': 300}, {'x': 301}])
        s = Sequence("my_sequence")
        eq_(
            testing.db.execute(
                t1.select().where(t1.c.x > s.next_value())
            ).fetchall(),
            [(300, ), (301, )]
        )

    @testing.provide_metadata
    def test_func_embedded_valuesbase(self):
        """test can use next_value() in values() of _ValuesBase"""

        metadata = self.metadata
        t1 = Table(
            't', metadata,
            Column('x', Integer)
        )
        t1.create(testing.db)
        s = Sequence("my_sequence")
        testing.db.execute(
            t1.insert().values(x=s.next_value())
        )
        self._assert_seq_result(
            testing.db.scalar(t1.select())
        )

    @testing.provide_metadata
    def test_inserted_pk_no_returning(self):
        """test inserted_primary_key contains [None] when
        pk_col=next_value(), implicit returning is not used."""

        metadata = self.metadata
        e = engines.testing_engine(options={'implicit_returning': False})
        s = Sequence("my_sequence")
        metadata.bind = e
        t1 = Table(
            't', metadata,
            Column('x', Integer, primary_key=True)
        )
        t1.create()
        r = e.execute(t1.insert().values(x=s.next_value()))
        eq_(r.inserted_primary_key, [None])

    @testing.requires.returning
    @testing.provide_metadata
    def test_inserted_pk_implicit_returning(self):
        """test inserted_primary_key contains the result when
        pk_col=next_value(), when implicit returning is used."""

        metadata = self.metadata
        e = engines.testing_engine(options={'implicit_returning': True})
        s = Sequence("my_sequence")
        metadata.bind = e
        t1 = Table(
            't', metadata,
            Column('x', Integer, primary_key=True)
        )
        t1.create()
        r = e.execute(
            t1.insert().values(x=s.next_value())
        )
        self._assert_seq_result(r.inserted_primary_key[0])


class SequenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __requires__ = ('sequences',)
    __backend__ = True

    @testing.fails_on('firebird', 'no FB support for start/increment')
    def test_start_increment(self):
        for seq in (
                Sequence('foo_seq'),
                Sequence('foo_seq', start=8),
                Sequence('foo_seq', increment=5)):
            seq.create(testing.db)
            try:
                values = [
                    testing.db.execute(seq) for i in range(3)
                ]
                start = seq.start or 1
                inc = seq.increment or 1
                assert values == list(range(start, start + inc * 3, inc))

            finally:
                seq.drop(testing.db)

    def _has_sequence(self, name):
        return testing.db.dialect.has_sequence(testing.db, name)

    def test_nextval_render(self):
        """test dialect renders the "nextval" construct,
        whether or not "optional" is set """

        for s in (Sequence("my_seq"), Sequence("my_seq", optional=True)):
            assert str(s.next_value().compile(dialect=testing.db.dialect)) in (
                "nextval('my_seq')", "gen_id(my_seq, 1)", "my_seq.nextval",)

    def test_nextval_unsupported(self):
        """test next_value() used on non-sequence platform
        raises NotImplementedError."""

        s = Sequence("my_seq")
        d = sqlite.dialect()
        assert_raises_message(
            NotImplementedError,
            "Dialect 'sqlite' does not support sequence increments.",
            s.next_value().compile,
            dialect=d
        )

    def test_checkfirst_sequence(self):
        s = Sequence("my_sequence")
        s.create(testing.db, checkfirst=False)
        assert self._has_sequence('my_sequence')
        s.create(testing.db, checkfirst=True)
        s.drop(testing.db, checkfirst=False)
        assert not self._has_sequence('my_sequence')
        s.drop(testing.db, checkfirst=True)

    def test_checkfirst_metadata(self):
        m = MetaData()
        Sequence("my_sequence", metadata=m)
        m.create_all(testing.db, checkfirst=False)
        assert self._has_sequence('my_sequence')
        m.create_all(testing.db, checkfirst=True)
        m.drop_all(testing.db, checkfirst=False)
        assert not self._has_sequence('my_sequence')
        m.drop_all(testing.db, checkfirst=True)

    def test_checkfirst_table(self):
        m = MetaData()
        s = Sequence("my_sequence")
        t = Table('t', m, Column('c', Integer, s, primary_key=True))
        t.create(testing.db, checkfirst=False)
        assert self._has_sequence('my_sequence')
        t.create(testing.db, checkfirst=True)
        t.drop(testing.db, checkfirst=False)
        assert not self._has_sequence('my_sequence')
        t.drop(testing.db, checkfirst=True)

    @testing.provide_metadata
    def test_table_overrides_metadata_create(self):
        metadata = self.metadata
        Sequence("s1", metadata=metadata)
        s2 = Sequence("s2", metadata=metadata)
        s3 = Sequence("s3")
        t = Table(
            't', metadata,
            Column('c', Integer, s3, primary_key=True))
        assert s3.metadata is metadata

        t.create(testing.db, checkfirst=True)
        s3.drop(testing.db)

        # 't' is created, and 's3' won't be
        # re-created since it's linked to 't'.
        # 's1' and 's2' are, however.
        metadata.create_all(testing.db)
        assert self._has_sequence('s1')
        assert self._has_sequence('s2')
        assert not self._has_sequence('s3')

        s2.drop(testing.db)
        assert self._has_sequence('s1')
        assert not self._has_sequence('s2')

        metadata.drop_all(testing.db)
        assert not self._has_sequence('s1')
        assert not self._has_sequence('s2')

    @testing.requires.returning
    @testing.provide_metadata
    def test_freestanding_sequence_via_autoinc(self):
        t = Table(
            'some_table', self.metadata,
            Column(
                'id', Integer,
                autoincrement=True,
                primary_key=True,
                default=Sequence(
                    'my_sequence', metadata=self.metadata).next_value())
        )
        self.metadata.create_all(testing.db)

        result = testing.db.execute(t.insert())
        eq_(result.inserted_primary_key, [1])

cartitems = sometable = metadata = None


class TableBoundSequenceTest(fixtures.TestBase):
    __requires__ = ('sequences',)
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global cartitems, sometable, metadata
        metadata = MetaData(testing.db)
        cartitems = Table(
            "cartitems", metadata,
            Column(
                "cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", sa.DateTime())
        )
        sometable = Table(
            'Manager', metadata,
            Column('obj_id', Integer, Sequence('obj_id_seq')),
            Column('name', String(128)),
            Column(
                'id', Integer, Sequence('Manager_id_seq', optional=True),
                primary_key=True),
        )

        metadata.create_all()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_insert_via_seq(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        r = cartitems.insert().execute(description='lala')

        assert r.inserted_primary_key and r.inserted_primary_key[0] is not None
        id_ = r.inserted_primary_key[0]

        eq_(1,
            sa.select([func.count(cartitems.c.cart_id)],
                      sa.and_(cartitems.c.description == 'lala',
                              cartitems.c.cart_id == id_)).scalar())

        cartitems.select().execute().fetchall()

    def test_seq_nonpk(self):
        """test sequences fire off as defaults on non-pk columns"""

        engine = engines.testing_engine(options={'implicit_returning': False})
        result = engine.execute(sometable.insert(), name="somename")

        assert set(result.postfetch_cols()) == set([sometable.c.obj_id])

        result = engine.execute(sometable.insert(), name="someother")
        assert set(result.postfetch_cols()) == set([sometable.c.obj_id])

        sometable.insert().execute(
            {'name': 'name3'},
            {'name': 'name4'})
        eq_(sometable.select().order_by(sometable.c.id).execute().fetchall(),
            [(1, "somename", 1),
             (2, "someother", 2),
             (3, "name3", 3),
             (4, "name4", 4)])


class SpecialTypePKTest(fixtures.TestBase):

    """test process_result_value in conjunction with primary key columns.

    Also tests that "autoincrement" checks are against
    column.type._type_affinity, rather than the class of "type" itself.

    """
    __backend__ = True

    @classmethod
    def setup_class(cls):
        class MyInteger(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return int(value[4:])

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return "INT_%d" % value

        cls.MyInteger = MyInteger

    @testing.provide_metadata
    def _run_test(self, *arg, **kw):
        metadata = self.metadata
        implicit_returning = kw.pop('implicit_returning', True)
        kw['primary_key'] = True
        if kw.get('autoincrement', True):
            kw['test_needs_autoincrement'] = True
        t = Table(
            'x', metadata,
            Column('y', self.MyInteger, *arg, **kw),
            Column('data', Integer),
            implicit_returning=implicit_returning
        )

        t.create()
        r = t.insert().values(data=5).execute()

        # we don't pre-fetch 'server_default'.
        if 'server_default' in kw and (
                not testing.db.dialect.implicit_returning or
                not implicit_returning):
            eq_(r.inserted_primary_key, [None])
        else:
            eq_(r.inserted_primary_key, ['INT_1'])
        r.close()

        eq_(
            t.select().execute().first(),
            ('INT_1', 5)
        )

    def test_plain(self):
        # among other things, tests that autoincrement
        # is enabled.
        self._run_test()

    def test_literal_default_label(self):
        self._run_test(
            default=literal("INT_1", type_=self.MyInteger).label('foo'))

    def test_literal_default_no_label(self):
        self._run_test(default=literal("INT_1", type_=self.MyInteger))

    def test_sequence(self):
        self._run_test(Sequence('foo_seq'))

    def test_server_default(self):
        self._run_test(server_default='1',)

    def test_server_default_no_autoincrement(self):
        self._run_test(server_default='1', autoincrement=False)

    def test_clause(self):
        stmt = select([cast("INT_1", type_=self.MyInteger)]).as_scalar()
        self._run_test(default=stmt)

    @testing.requires.returning
    def test_no_implicit_returning(self):
        self._run_test(implicit_returning=False)

    @testing.requires.returning
    def test_server_default_no_implicit_returning(self):
        self._run_test(server_default='1', autoincrement=False)


class ServerDefaultsOnPKTest(fixtures.TestBase):
    __backend__ = True

    @testing.provide_metadata
    def test_string_default_none_on_insert(self):
        """Test that without implicit returning, we return None for
        a string server default.

        That is, we don't want to attempt to pre-execute "server_default"
        generically - the user should use a Python side-default for a case
        like this.   Testing that all backends do the same thing here.

        """

        metadata = self.metadata
        t = Table(
            'x', metadata,
            Column(
                'y', String(10), server_default='key_one', primary_key=True),
            Column('data', String(10)),
            implicit_returning=False
        )
        metadata.create_all()
        r = t.insert().execute(data='data')
        eq_(r.inserted_primary_key, [None])
        eq_(
            t.select().execute().fetchall(),
            [('key_one', 'data')]
        )

    @testing.requires.returning
    @testing.provide_metadata
    def test_string_default_on_insert_with_returning(self):
        """With implicit_returning, we get a string PK default back no
        problem."""

        metadata = self.metadata
        t = Table(
            'x', metadata,
            Column(
                'y', String(10), server_default='key_one', primary_key=True),
            Column('data', String(10))
        )
        metadata.create_all()
        r = t.insert().execute(data='data')
        eq_(r.inserted_primary_key, ['key_one'])
        eq_(
            t.select().execute().fetchall(),
            [('key_one', 'data')]
        )

    @testing.provide_metadata
    def test_int_default_none_on_insert(self):
        metadata = self.metadata
        t = Table(
            'x', metadata,
            Column('y', Integer, server_default='5', primary_key=True),
            Column('data', String(10)),
            implicit_returning=False
        )
        assert t._autoincrement_column is None
        metadata.create_all()
        r = t.insert().execute(data='data')
        eq_(r.inserted_primary_key, [None])
        if testing.against('sqlite'):
            eq_(
                t.select().execute().fetchall(),
                [(1, 'data')]
            )
        else:
            eq_(
                t.select().execute().fetchall(),
                [(5, 'data')]
            )

    @testing.provide_metadata
    def test_autoincrement_reflected_from_server_default(self):
        metadata = self.metadata
        t = Table(
            'x', metadata,
            Column('y', Integer, server_default='5', primary_key=True),
            Column('data', String(10)),
            implicit_returning=False
        )
        assert t._autoincrement_column is None
        metadata.create_all()

        m2 = MetaData(metadata.bind)
        t2 = Table('x', m2, autoload=True, implicit_returning=False)
        assert t2._autoincrement_column is None

    @testing.provide_metadata
    def test_int_default_none_on_insert_reflected(self):
        metadata = self.metadata
        Table(
            'x', metadata,
            Column('y', Integer, server_default='5', primary_key=True),
            Column('data', String(10)),
            implicit_returning=False
        )
        metadata.create_all()

        m2 = MetaData(metadata.bind)
        t2 = Table('x', m2, autoload=True, implicit_returning=False)

        r = t2.insert().execute(data='data')
        eq_(r.inserted_primary_key, [None])
        if testing.against('sqlite'):
            eq_(
                t2.select().execute().fetchall(),
                [(1, 'data')]
            )
        else:
            eq_(
                t2.select().execute().fetchall(),
                [(5, 'data')]
            )

    @testing.requires.returning
    @testing.provide_metadata
    def test_int_default_on_insert_with_returning(self):
        metadata = self.metadata
        t = Table(
            'x', metadata,
            Column('y', Integer, server_default='5', primary_key=True),
            Column('data', String(10))
        )

        metadata.create_all()
        r = t.insert().execute(data='data')
        eq_(r.inserted_primary_key, [5])
        eq_(
            t.select().execute().fetchall(),
            [(5, 'data')]
        )


class UnicodeDefaultsTest(fixtures.TestBase):
    __backend__ = True

    def test_no_default(self):
        Column(Unicode(32))

    def test_unicode_default(self):
        default = u('foo')
        Column(Unicode(32), default=default)

    def test_nonunicode_default(self):
        default = b('foo')
        assert_raises_message(
            sa.exc.SAWarning,
            "Unicode column 'foobar' has non-unicode "
            "default value b?'foo' specified.",
            Column,
            "foobar", Unicode(32),
            default=default
        )


class InsertFromSelectTest(fixtures.TestBase):
    __backend__ = True

    def _fixture(self):
        data = Table(
            'data', self.metadata,
            Column('x', Integer),
            Column('y', Integer)
        )
        data.create()
        testing.db.execute(data.insert(), {'x': 2, 'y': 5}, {'x': 7, 'y': 12})
        return data

    @testing.provide_metadata
    def test_insert_from_select_override_defaults(self):
        data = self._fixture()

        table = Table('sometable', self.metadata,
                      Column('x', Integer),
                      Column('foo', Integer, default=12),
                      Column('y', Integer))

        table.create()

        sel = select([data.c.x, data.c.y])

        ins = table.insert().\
            from_select(["x", "y"], sel)
        testing.db.execute(ins)

        eq_(
            testing.db.execute(table.select().order_by(table.c.x)).fetchall(),
            [(2, 12, 5), (7, 12, 12)]
        )

    @testing.provide_metadata
    def test_insert_from_select_fn_defaults(self):
        data = self._fixture()

        counter = itertools.count(1)

        def foo(ctx):
            return next(counter)

        table = Table('sometable', self.metadata,
                      Column('x', Integer),
                      Column('foo', Integer, default=foo),
                      Column('y', Integer))

        table.create()

        sel = select([data.c.x, data.c.y])

        ins = table.insert().\
            from_select(["x", "y"], sel)
        testing.db.execute(ins)

        # counter is only called once!
        eq_(
            testing.db.execute(table.select().order_by(table.c.x)).fetchall(),
            [(2, 1, 5), (7, 1, 12)]
        )
