import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions, schema, util
from sqlalchemy.orm import mapper, create_session
from testlib import *


class DefaultTest(TestBase):

    def setUpAll(self):
        global t, f, f2, ts, currenttime, metadata, default_generator

        db = testing.db
        metadata = MetaData(db)
        default_generator = {'x':50}

        def mydefault():
            default_generator['x'] += 1
            return default_generator['x']

        def myupdate_with_ctx(ctx):
            conn = ctx.connection
            return conn.execute(select([text('13')])).scalar()

        def mydefault_using_connection(ctx):
            conn = ctx.connection
            try:
                return conn.execute(select([text('12')])).scalar()
            finally:
                # ensure a "close()" on this connection does nothing,
                # since its a "branched" connection
                conn.close()

        use_function_defaults = testing.against('postgres', 'oracle')
        is_oracle = testing.against('oracle')

        # select "count(1)" returns different results on different DBs
        # also correct for "current_date" compatible as column default, value differences
        currenttime = func.current_date(type_=Date, bind=db)

        if is_oracle:
            ts = db.scalar(select([func.trunc(func.sysdate(), literal_column("'DAY'"), type_=Date).label('today')]))
            assert isinstance(ts, datetime.date) and not isinstance(ts, datetime.datetime)
            f = select([func.length('abcdef')], bind=db).scalar()
            f2 = select([func.length('abcdefghijk')], bind=db).scalar()
            # TODO: engine propigation across nested functions not working
            currenttime = func.trunc(currenttime, literal_column("'DAY'"), bind=db, type_=Date)
            def1 = currenttime
            def2 = func.trunc(text("sysdate"), literal_column("'DAY'"), type_=Date)

            deftype = Date
        elif use_function_defaults:
            f = select([func.length('abcdef')], bind=db).scalar()
            f2 = select([func.length('abcdefghijk')], bind=db).scalar()
            def1 = currenttime
            if testing.against('maxdb'):
                def2 = text("curdate")
            else:
                def2 = text("current_date")
            deftype = Date
            ts = db.func.current_date().scalar()
        else:
            f = select([func.length('abcdef')], bind=db).scalar()
            f2 = select([func.length('abcdefghijk')], bind=db).scalar()
            def1 = def2 = "3"
            ts = 3
            deftype = Integer

        t = Table('default_test1', metadata,
            # python function
            Column('col1', Integer, primary_key=True, default=mydefault),

            # python literal
            Column('col2', String(20), default="imthedefault", onupdate="im the update"),

            # preexecute expression
            Column('col3', Integer, default=func.length('abcdef'), onupdate=func.length('abcdefghijk')),

            # SQL-side default from sql expression
            Column('col4', deftype, PassiveDefault(def1)),

            # SQL-side default from literal expression
            Column('col5', deftype, PassiveDefault(def2)),

            # preexecute + update timestamp
            Column('col6', Date, default=currenttime, onupdate=currenttime),

            Column('boolcol1', Boolean, default=True),
            Column('boolcol2', Boolean, default=False),

            # python function which uses ExecutionContext
            Column('col7', Integer, default=mydefault_using_connection, onupdate=myupdate_with_ctx),

            # python builtin
            Column('col8', Date, default=datetime.date.today, onupdate=datetime.date.today)
        )
        t.create()

    def tearDownAll(self):
        t.drop()

    def tearDown(self):
        default_generator['x'] = 50
        t.delete().execute()

    def test_bad_argsignature(self):
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
            try:
                c = ColumnDefault(fn)
                assert False, str(fn)
            except exceptions.ArgumentError, e:
                assert str(e) == ex_msg

    def test_argsignature(self):
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
            c = ColumnDefault(fn)

    def teststandalone(self):
        c = testing.db.engine.contextual_connect()
        x = c.execute(t.c.col1.default)
        y = t.c.col2.default.execute()
        z = c.execute(t.c.col3.default)
        self.assert_(50 <= x <= 57)
        self.assert_(y == 'imthedefault')
        self.assert_(z == f)
        self.assert_(f2==11)

    def testinsert(self):
        r = t.insert().execute()
        assert r.lastrow_has_defaults()
        assert util.Set(r.context.postfetch_cols) == util.Set([t.c.col3, t.c.col5, t.c.col4, t.c.col6])

        r = t.insert(inline=True).execute()
        assert r.lastrow_has_defaults()
        assert util.Set(r.context.postfetch_cols) == util.Set([t.c.col3, t.c.col5, t.c.col4, t.c.col6])

        t.insert().execute()
        t.insert().execute()

        ctexec = select([currenttime.label('now')], bind=testing.db).scalar()
        l = t.select().execute()
        today = datetime.date.today()
        self.assertEquals(l.fetchall(), [
            (51, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today),
            (52, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today),
            (53, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today),
            (54, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today),
            ])

    def testinsertmany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        r = t.insert().execute({}, {}, {})

        ctexec = currenttime.scalar()
        l = t.select().execute()
        today = datetime.date.today()
        self.assert_(l.fetchall() == [(51, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today), (52, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today), (53, 'imthedefault', f, ts, ts, ctexec, True, False, 12, today)])

    def testinsertvalues(self):
        t.insert(values={'col3':50}).execute()
        l = t.select().execute()
        self.assert_(l.fetchone()['col3'] == 50)

    def testupdatemany(self):
        # MySQL-Python 1.2.2 breaks functions in execute_many :(
        if (testing.against('mysql') and
            testing.db.dialect.dbapi.version_info[:3] == (1, 2, 2)):
            return

        t.insert().execute({}, {}, {})

        t.update(t.c.col1==bindparam('pkval')).execute(
            {'pkval':51,'col7':None, 'col8':None, 'boolcol1':False},
        )

        t.update(t.c.col1==bindparam('pkval')).execute(
            {'pkval':51,},
            {'pkval':52,},
            {'pkval':53,},
        )

        l = t.select().execute()
        ctexec = currenttime.scalar()
        today = datetime.date.today()
        self.assert_(l.fetchall() == [(51, 'im the update', f2, ts, ts, ctexec, False, False, 13, today), (52, 'im the update', f2, ts, ts, ctexec, True, False, 13, today), (53, 'im the update', f2, ts, ts, ctexec, True, False, 13, today)])

    def testupdate(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk).execute(col4=None, col5=None)
        ctexec = currenttime.scalar()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l == (pk, 'im the update', f2, None, None, ctexec, True, False, 13, datetime.date.today()))
        self.assert_(f2==11)

    def testupdatevalues(self):
        r = t.insert().execute()
        pk = r.last_inserted_ids()[0]
        t.update(t.c.col1==pk, values={'col3': 55}).execute()
        l = t.select(t.c.col1==pk).execute()
        l = l.fetchone()
        self.assert_(l['col3'] == 55)

    @testing.fails_on_everything_except('postgres')
    def testpassiveoverride(self):
        """primarily for postgres, tests that when we get a primary key column back
        from reflecting a table which has a default value on it, we pre-execute
        that PassiveDefault upon insert, even though PassiveDefault says
        "let the database execute this", because in postgres we must have all the primary
        key values in memory before insert; otherwise we cant locate the just inserted row."""

        try:
            meta = MetaData(testing.db)
            testing.db.execute("""
             CREATE TABLE speedy_users
             (
                 speedy_user_id   SERIAL     PRIMARY KEY,

                 user_name        VARCHAR    NOT NULL,
                 user_password    VARCHAR    NOT NULL
             );
            """, None)

            t = Table("speedy_users", meta, autoload=True)
            t.insert().execute(user_name='user', user_password='lala')
            l = t.select().execute().fetchall()
            self.assert_(l == [(1, 'user', 'lala')])
        finally:
            testing.db.execute("drop table speedy_users", None)

class PKDefaultTest(TestBase):
    def setUpAll(self):
        global metadata, t1, t2

        metadata = MetaData(testing.db)

        t2 = Table('t2', metadata,
            Column('nextid', Integer))

        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True, default=select([func.max(t2.c.nextid)]).as_scalar()),
            Column('data', String(30)))

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    @testing.unsupported('mssql')
    def test_basic(self):
        t2.insert().execute(nextid=1)
        r = t1.insert().execute(data='hi')
        assert r.last_inserted_ids() == [1]

        t2.insert().execute(nextid=2)
        r = t1.insert().execute(data='there')
        assert r.last_inserted_ids() == [2]


class AutoIncrementTest(TestBase):
    def setUp(self):
        global aitable, aimeta

        aimeta = MetaData(testing.db)
        aitable = Table("aitest", aimeta,
            Column('id', Integer, Sequence('ai_id_seq', optional=True),
                   primary_key=True),
            Column('int1', Integer),
            Column('str1', String(20)))
        aimeta.create_all()

    def tearDown(self):
        aimeta.drop_all()

    # should fail everywhere... was: @supported('postgres', 'mysql', 'maxdb')
    @testing.fails_on('sqlite')
    def testnonautoincrement(self):
        # sqlite INT primary keys can be non-unique! (only for ints)
        meta = MetaData(testing.db)
        nonai_table = Table("nonaitest", meta,
            Column('id', Integer, autoincrement=False, primary_key=True),
            Column('data', String(20)))
        nonai_table.create(checkfirst=True)
        try:
            try:
                # postgres + mysql strict will fail on first row,
                # mysql in legacy mode fails on second row
                nonai_table.insert().execute(data='row 1')
                nonai_table.insert().execute(data='row 2')
                assert False
            except exceptions.SQLError, e:
                print "Got exception", str(e)
                assert True

            nonai_table.insert().execute(id=1, data='row 1')
        finally:
            nonai_table.drop()

    # TODO: add coverage for increment on a secondary column in a key
    def _test_autoincrement(self, bind):
        ids = set()
        rs = bind.execute(aitable.insert(), int1=1)
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), str1='row 2')
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(), int1=3, str1='row 3')
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = bind.execute(aitable.insert(values={'int1':func.length('four')}))
        last = rs.last_inserted_ids()[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        self.assert_(
            list(bind.execute(aitable.select().order_by(aitable.c.id))) ==
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

    def test_autoincrement_fk(self):
        if not testing.db.dialect.supports_pk_autoincrement:
            return True

        metadata = MetaData(testing.db)

        # No optional sequence here.
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))
        metadata.create_all()
        try:
            r = nodes.insert().execute(data='foo')
            id_ = r.last_inserted_ids()[0]
            nodes.insert().execute(data='bar', parent_id=id_)
        finally:
            metadata.drop_all()


class SequenceTest(TestBase):
    __unsupported_on__ = ('sqlite', 'mysql', 'mssql', 'firebird',
                          'sybase', 'access')

    def setUpAll(self):
        global cartitems, sometable, metadata
        metadata = MetaData(testing.db)
        cartitems = Table("cartitems", metadata,
            Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
            Column("description", String(40)),
            Column("createdate", DateTime())
        )
        sometable = Table( 'Manager', metadata,
               Column('obj_id', Integer, Sequence('obj_id_seq'), ),
               Column('name', String(128)),
               Column('id', Integer, Sequence('Manager_id_seq', optional=True),
                      primary_key=True),
           )

        metadata.create_all()

    def testseqnonpk(self):
        """test sequences fire off as defaults on non-pk columns"""

        result = sometable.insert().execute(name="somename")
        assert 'id' in result.postfetch_cols()

        result = sometable.insert().execute(name="someother")
        assert 'id' in result.postfetch_cols()

        sometable.insert().execute(
            {'name':'name3'},
            {'name':'name4'}
        )
        assert sometable.select().execute().fetchall() == [
            (1, "somename", 1),
            (2, "someother", 2),
            (3, "name3", 3),
            (4, "name4", 4),
        ]

    def testsequence(self):
        cartitems.insert().execute(description='hi')
        cartitems.insert().execute(description='there')
        r = cartitems.insert().execute(description='lala')

        assert r.last_inserted_ids() and r.last_inserted_ids()[0] is not None
        id_ = r.last_inserted_ids()[0]

        assert select([func.count(cartitems.c.cart_id)],
                      and_(cartitems.c.description == 'lala',
                           cartitems.c.cart_id == id_)).scalar() == 1

        cartitems.select().execute().fetchall()


    @testing.fails_on('maxdb')
    # maxdb db-api seems to double-execute NEXTVAL internally somewhere,
    # throwing off the numbers for these tests...
    def test_implicit_sequence_exec(self):
        s = Sequence("my_sequence", metadata=MetaData(testing.db))
        s.create()
        try:
            x = s.execute()
            self.assert_(x == 1)
        finally:
            s.drop()

    @testing.fails_on('maxdb')
    def teststandalone_explicit(self):
        s = Sequence("my_sequence")
        s.create(bind=testing.db)
        try:
            x = s.execute(testing.db)
            self.assert_(x == 1)
        finally:
            s.drop(testing.db)

    def test_checkfirst(self):
        s = Sequence("my_sequence")
        s.create(testing.db, checkfirst=False)
        s.create(testing.db, checkfirst=True)
        s.drop(testing.db, checkfirst=False)
        s.drop(testing.db, checkfirst=True)

    @testing.fails_on('maxdb')
    def teststandalone2(self):
        x = cartitems.c.cart_id.sequence.execute()
        self.assert_(1 <= x <= 4)

    def tearDownAll(self):
        metadata.drop_all()


if __name__ == "__main__":
    testenv.main()
