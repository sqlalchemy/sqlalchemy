from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import datetime
from sqlalchemy import Sequence, Column, func
from sqlalchemy.schema import CreateSequence, DropSequence
from sqlalchemy.sql import select, text
import sqlalchemy as sa
from sqlalchemy.test import testing, engines
from sqlalchemy import MetaData, Integer, String, ForeignKey, Boolean, exc
from sqlalchemy.test.schema import Table
from sqlalchemy.test.testing import eq_
from test.sql import _base


class DefaultTest(testing.TestBase):

    @classmethod
    def setup_class(cls):
        global t, f, f2, ts, currenttime, metadata, default_generator

        db = testing.db
        metadata = MetaData(db)
        default_generator = {'x':50}

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

        use_function_defaults = testing.against('postgresql', 'mssql', 'maxdb')
        is_oracle = testing.against('oracle')

        # select "count(1)" returns different results on different DBs also
        # correct for "current_date" compatible as column default, value
        # differences
        currenttime = func.current_date(type_=sa.Date, bind=db)
        if is_oracle:
            ts = db.scalar(sa.select([func.trunc(func.sysdate(), sa.literal_column("'DAY'"), type_=sa.Date).label('today')]))
            assert isinstance(ts, datetime.date) and not isinstance(ts, datetime.datetime)
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(currenttime, sa.literal_column("'DAY'"), bind=db, type_=sa.Date)
            def1 = currenttime
            def2 = func.trunc(sa.text("sysdate"), sa.literal_column("'DAY'"), type_=sa.Date)

            deftype = sa.Date
        elif use_function_defaults:
            f = sa.select([func.length('abcdef')], bind=db).scalar()
            f2 = sa.select([func.length('abcdefghijk')], bind=db).scalar()
            def1 = currenttime
            deftype = sa.Date
            if testing.against('maxdb'):
                def2 = sa.text("curdate")
            elif testing.against('mssql'):
                def2 = sa.text("getdate()")
            else:
                def2 = sa.text("current_date")
            ts = db.func.current_date().scalar()
        else:
            f = len('abcdef')
            f2 = len('abcdefghijk')
            def1 = def2 = "3"
            ts = 3
            deftype = Integer

        t = Table('default_test1', metadata,
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
                   server_default='ddl'))
        t.create()

    @classmethod
    def teardown_class(cls):
        t.drop()

    def teardown(self):
        default_generator['x'] = 50
        t.delete().execute()

    def test_bad_arg_signature(self):
        ex_msg = \
          "ColumnDefault Python function takes zero or one positional arguments"

        def fn1(x, y): pass
        def fn2(x, y, z=3): pass
        class fn3(object):
            def __init__(self, x, y):
                pass
        class FN4(object):
            def __call__(self, x, y):
                pass
        fn4 = FN4()

        for fn in fn1, fn2, fn3, fn4:
            assert_raises_message(sa.exc.ArgumentError,
                                     ex_msg,
                                     sa.ColumnDefault, fn)
    
    def test_arg_signature(self):
        def fn1(): pass
        def fn2(): pass
        def fn3(x=1): pass
        def fn4(x=1, y=2, z=3): pass
        fn5 = list
        class fn6(object):
            def __init__(self, x):
                pass
        class fn6(object):
            def __init__(self, x, y=3):
                pass
        class FN7(object):
            def __call__(self, x):
                pass
        fn7 = FN7()
        class FN8(object):
            def __call__(self, x, y=3):
                pass
        fn8 = FN8()

        for fn in fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8:
            c = sa.ColumnDefault(fn)

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
            slots = ['default', 'onupdate', 'server_default', 'server_onupdate']
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
        has_('col8', 'default', 'server_default', 'onupdate', 'server_onupdate')

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

        ctexec = sa.select([currenttime.label('now')], bind=testing.db).scalar()
        l = t.select().order_by(t.c.col1).execute()
        today = datetime.date.today()
        eq_(l.fetchall(), [
            (x, 'imthedefault', f, ts, ts, ctexec, True, False,
             12, today, 'py')
            for x in range(51, 54)])

        t.insert().execute(col9=None)
        assert r.lastrow_has_defaults()
        eq_(set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]))
        
        eq_(t.select(t.c.col1==54).execute().fetchall(),
            [(54, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, None)])

    @testing.fails_on('firebird', 'Data type unknown')
    def test_insertmany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql+mysqldb') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        r = t.insert().execute({}, {}, {})

        ctexec = currenttime.scalar()
        l = t.select().execute()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py'),
             (52, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py'),
             (53, 'imthedefault', f, ts, ts, ctexec, True, False,
              12, today, 'py')])
    
    def test_missing_many_param(self):
        assert_raises_message(exc.InvalidRequestError, 
            "A value is required for bind parameter 'col7', in parameter group 1",
            t.insert().execute,
            {'col4':7, 'col7':12, 'col8':19},
            {'col4':7, 'col8':19},
            {'col4':7, 'col7':12, 'col8':19},
        )
        
    def test_insert_values(self):
        t.insert(values={'col3':50}).execute()
        l = t.select().execute()
        eq_(50, l.first()['col3'])

    @testing.fails_on('firebird', 'Data type unknown')
    def test_updatemany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql+mysqldb') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        t.insert().execute({}, {}, {})

        t.update(t.c.col1==sa.bindparam('pkval')).execute(
            {'pkval':51,'col7':None, 'col8':None, 'boolcol1':False})

        t.update(t.c.col1==sa.bindparam('pkval')).execute(
            {'pkval':51,},
            {'pkval':52,},
            {'pkval':53,})

        l = t.select().execute()
        ctexec = currenttime.scalar()
        today = datetime.date.today()
        eq_(l.fetchall(),
            [(51, 'im the update', f2, ts, ts, ctexec, False, False,
              13, today, 'py'),
             (52, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py'),
             (53, 'im the update', f2, ts, ts, ctexec, True, False,
              13, today, 'py')])

    @testing.fails_on('firebird', 'Data type unknown')
    def test_update(self):
        r = t.insert().execute()
        pk = r.inserted_primary_key[0]
        t.update(t.c.col1==pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        l = t.select(t.c.col1==pk).execute()
        l = l.first()
        eq_(l,
            (pk, 'im the update', f2, None, None, ctexec, True, False,
             13, datetime.date.today(), 'py'))
        eq_(11, f2)

    @testing.fails_on('firebird', 'Data type unknown')
    def test_update_values(self):
        r = t.insert().execute()
        pk = r.inserted_primary_key[0]
        t.update(t.c.col1==pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1==pk).execute()
        l = l.first()
        eq_(55, l['col3'])

    
class PKDefaultTest(_base.TablesTest):
    __requires__ = ('subqueries',)

    @classmethod
    def define_tables(cls, metadata):
        t2 = Table('t2', metadata,
            Column('nextid', Integer))

        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                     default=sa.select([func.max(t2.c.nextid)]).as_scalar()),
              Column('data', String(30)))
    
    @testing.requires.returning
    def test_with_implicit_returning(self):
        self._test(True)
        
    def test_regular(self):
        self._test(False)
        
    @testing.resolve_artifact_names
    def _test(self, returning):
        if not returning and not testing.db.dialect.implicit_returning:
            engine = testing.db
        else:
            engine = engines.testing_engine(options={'implicit_returning':returning})
        engine.execute(t2.insert(), nextid=1)
        r = engine.execute(t1.insert(), data='hi')
        eq_([1], r.inserted_primary_key)

        engine.execute(t2.insert(), nextid=2)
        r = engine.execute(t1.insert(), data='there')
        eq_([2], r.inserted_primary_key)

class PKIncrementTest(_base.TablesTest):
    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table("aitable", metadata,
              Column('id', Integer, Sequence('ai_id_seq', optional=True),
                     primary_key=True),
              Column('int1', Integer),
              Column('str1', String(20)))

    # TODO: add coverage for increment on a secondary column in a key
    @testing.fails_on('firebird', 'Data type unknown')
    @testing.resolve_artifact_names
    def _test_autoincrement(self, bind):
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

        rs = bind.execute(aitable.insert(values={'int1':func.length('four')}))
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        eq_(ids, set([1,2,3,4]))
        
        eq_(list(bind.execute(aitable.select().order_by(aitable.c.id))),
            [(1, 1, None), (2, None, 'row 2'), (3, 3, 'row 3'), (4, 4, None)])

    @testing.resolve_artifact_names
    def test_autoincrement_autocommit(self):
        self._test_autoincrement(testing.db)

    @testing.resolve_artifact_names
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


class EmptyInsertTest(testing.TestBase):
    @testing.exclude('sqlite', '<', (3, 3, 8), 'no empty insert support')
    @testing.fails_on('oracle', 'FIXME: unknown')
    def test_empty_insert(self):
        metadata = MetaData(testing.db)
        t1 = Table('t1', metadata,
                Column('is_true', Boolean, server_default=('1')))
        metadata.create_all()
        
        try:
            result = t1.insert().execute()
            eq_(1, select([func.count(text('*'))], from_obj=t1).scalar())
            eq_(True, t1.select().scalar())
        finally:
            metadata.drop_all()

class AutoIncrementTest(_base.TablesTest):
    __requires__ = ('identity',)
    run_define_tables = 'each'

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
        nodes = Table('nodes', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))
        nodes.create()

        r = nodes.insert().execute(data='foo')
        id_ = r.inserted_primary_key[0]
        nodes.insert().execute(data='bar', parent_id=id_)

    @testing.fails_on('sqlite', 'FIXME: unknown')
    def test_non_autoincrement(self):
        # sqlite INT primary keys can be non-unique! (only for ints)
        nonai = Table("nonaitest", self.metadata,
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai.create()


        try:
            # postgresql + mysql strict will fail on first row,
            # mysql in legacy mode fails on second row
            nonai.insert().execute(data='row 1')
            nonai.insert().execute(data='row 2')
            assert False
        except sa.exc.SQLError, e:
            assert True

        nonai.insert().execute(id=1, data='row 1')


class SequenceTest(testing.TestBase, testing.AssertsCompiledSQL):

    @classmethod
    @testing.requires.sequences
    def setup_class(cls):
        global cartitems, sometable, metadata
        metadata = MetaData(testing.db)
        cartitems = Table("cartitems", metadata,
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", sa.DateTime())
        )
        sometable = Table( 'Manager', metadata,
               Column('obj_id', Integer, Sequence('obj_id_seq'), ),
               Column('name', String(128)),
               Column('id', Integer, Sequence('Manager_id_seq', optional=True),
                      primary_key=True),
           )

        metadata.create_all()


    def test_compile(self):
        self.assert_compile(
            CreateSequence(Sequence('foo_seq')),
            "CREATE SEQUENCE foo_seq",
            use_default_dialect=True,
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', start=5)),
            "CREATE SEQUENCE foo_seq START WITH 5",
            use_default_dialect=True,
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', increment=2)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2",
            use_default_dialect=True,
        )

        self.assert_compile(
            CreateSequence(Sequence('foo_seq', increment=2, start=5)),
            "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 5",
            use_default_dialect=True,
        )

        self.assert_compile(
            DropSequence(Sequence('foo_seq')),
            "DROP SEQUENCE foo_seq",
            use_default_dialect=True,
        )
        

    @testing.fails_on('firebird', 'no FB support for start/increment')
    @testing.requires.sequences
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
                assert values == list(xrange(start, start + inc * 3, inc))
                
            finally:
                seq.drop(testing.db)
        
    @testing.requires.sequences
    def test_seq_nonpk(self):
        """test sequences fire off as defaults on non-pk columns"""

        engine = engines.testing_engine(options={'implicit_returning':False})
        result = engine.execute(sometable.insert(), name="somename")

        assert set(result.postfetch_cols()) == set([sometable.c.obj_id])

        result = engine.execute(sometable.insert(), name="someother")
        assert set(result.postfetch_cols()) == set([sometable.c.obj_id])

        sometable.insert().execute(
            {'name':'name3'},
            {'name':'name4'})
        eq_(sometable.select().order_by(sometable.c.id).execute().fetchall(),
            [(1, "somename", 1),
             (2, "someother", 2),
             (3, "name3", 3),
             (4, "name4", 4)])

    @testing.requires.sequences
    def test_sequence(self):
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

    @testing.fails_on('maxdb', 'FIXME: unknown')
    # maxdb db-api seems to double-execute NEXTVAL internally somewhere,
    # throwing off the numbers for these tests...
    @testing.requires.sequences
    def test_implicit_sequence_exec(self):
        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        s.create()
        try:
            x = s.execute()
            eq_(x, 1)
        finally:
            s.drop()

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.requires.sequences
    def teststandalone_explicit(self):
        s = Sequence("my_sequence")
        s.create(bind=testing.db)
        try:
            x = s.execute(testing.db)
            eq_(x, 1)
        finally:
            s.drop(testing.db)

    @testing.requires.sequences
    def test_checkfirst(self):
        s = Sequence("my_sequence")
        s.create(testing.db, checkfirst=False)
        s.create(testing.db, checkfirst=True)
        s.drop(testing.db, checkfirst=False)
        s.drop(testing.db, checkfirst=True)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.requires.sequences
    def teststandalone2(self):
        x = cartitems.c.cart_id.default.execute()
        self.assert_(1 <= x <= 4)

    @classmethod
    @testing.requires.sequences
    def teardown_class(cls):
        metadata.drop_all()


