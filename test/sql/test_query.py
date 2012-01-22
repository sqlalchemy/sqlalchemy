from test.lib.testing import eq_, assert_raises_message, assert_raises
import datetime
from sqlalchemy import *
from sqlalchemy import exc, sql, util
from sqlalchemy.engine import default, base
from test.lib import *
from test.lib.schema import Table, Column

class QueryTest(fixtures.TestBase):

    @classmethod
    def setup_class(cls):
        global users, users2, addresses, metadata
        metadata = MetaData(testing.db)
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key=True, test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )
        addresses = Table('query_addresses', metadata,
            Column('address_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)),
            test_needs_acid=True
            )

        users2 = Table('u2', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        addresses.delete().execute()
        users.delete().execute()
        users2.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_insert(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1

    def test_insert_heterogeneous_params(self):
        """test that executemany parameters are asserted to match the parameter set of the first."""

        assert_raises_message(exc.StatementError, 
            r"A value is required for bind parameter 'user_name', in "
            "parameter group 2 \(original cause: (sqlalchemy.exc.)?InvalidRequestError: A "
            "value is required for bind parameter 'user_name', in "
            "parameter group 2\) 'INSERT INTO query_users",
            users.insert().execute,
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9}
        )

        # this succeeds however.   We aren't yet doing 
        # a length check on all subsequent parameters.
        users.insert().execute(
            {'user_id':7},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9}
        )

    def test_update(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1

        users.update(users.c.user_id == 7).execute(user_name = 'fred')
        assert users.select(users.c.user_id==7).execute().first()['user_name'] == 'fred'

    def test_lastrow_accessor(self):
        """Tests the inserted_primary_key and lastrow_has_id() functions."""

        def insert_values(engine, table, values):
            """
            Inserts a row into a table, returns the full list of values
            INSERTed including defaults that fired off on the DB side and
            detects rows that had defaults and post-fetches.
            """

            # verify implicit_returning is working
            if engine.dialect.implicit_returning:
                ins = table.insert()
                comp = ins.compile(engine, column_keys=list(values))
                if not set(values).issuperset(c.key for c in table.primary_key):
                    assert comp.returning

            result = engine.execute(table.insert(), **values)
            ret = values.copy()

            for col, id in zip(table.primary_key, result.inserted_primary_key):
                ret[col.key] = id

            if result.lastrow_has_defaults():
                criterion = and_(*[col==id for col, id in 
                                    zip(table.primary_key, result.inserted_primary_key)])
                row = engine.execute(table.select(criterion)).first()
                for c in table.c:
                    ret[c.key] = row[c]
            return ret

        if testing.against('firebird', 'postgresql', 'oracle', 'mssql'):
            assert testing.db.dialect.implicit_returning

        if testing.db.dialect.implicit_returning:
            test_engines = [
                engines.testing_engine(options={'implicit_returning':False}),
                engines.testing_engine(options={'implicit_returning':True}),
            ]
        else:
            test_engines = [testing.db]

        for engine in test_engines:
            metadata = MetaData()
            for supported, table, values, assertvalues in [
                (
                    {'unsupported':['sqlite']},
                    Table("t1", metadata,
                        Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                        Column('foo', String(30), primary_key=True)),
                    {'foo':'hi'},
                    {'id':1, 'foo':'hi'}
                ),
                (
                    {'unsupported':['sqlite']},
                    Table("t2", metadata,
                        Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                        Column('foo', String(30), primary_key=True),
                        Column('bar', String(30), server_default='hi')
                    ),
                    {'foo':'hi'},
                    {'id':1, 'foo':'hi', 'bar':'hi'}
                ),
                (
                    {'unsupported':[]},
                    Table("t3", metadata,
                        Column("id", String(40), primary_key=True),
                        Column('foo', String(30), primary_key=True),
                        Column("bar", String(30))
                        ),
                        {'id':'hi', 'foo':'thisisfoo', 'bar':"thisisbar"},
                        {'id':'hi', 'foo':'thisisfoo', 'bar':"thisisbar"}
                ),
                (
                    {'unsupported':[]},
                    Table("t4", metadata,
                        Column('id', Integer, Sequence('t4_id_seq', optional=True), primary_key=True),
                        Column('foo', String(30), primary_key=True),
                        Column('bar', String(30), server_default='hi')
                    ),
                    {'foo':'hi', 'id':1},
                    {'id':1, 'foo':'hi', 'bar':'hi'}
                ),
                (
                    {'unsupported':[]},
                    Table("t5", metadata,
                        Column('id', String(10), primary_key=True),
                        Column('bar', String(30), server_default='hi')
                    ),
                    {'id':'id1'},
                    {'id':'id1', 'bar':'hi'},
                ),
                (
                    {'unsupported':['sqlite']},
                    Table("t6", metadata,
                        Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                        Column('bar', Integer, primary_key=True)
                    ),
                    {'bar':0},
                    {'id':1, 'bar':0},
                ),
            ]:
                if testing.db.name in supported['unsupported']:
                    continue
                try:
                    table.create(bind=engine, checkfirst=True)
                    i = insert_values(engine, table, values)
                    assert i == assertvalues, "tablename: %s %r %r" % (table.name, repr(i), repr(assertvalues))
                finally:
                    table.drop(bind=engine)

    @testing.fails_on('sqlite', "sqlite autoincremnt doesn't work with composite pks")
    def test_misordered_lastrow(self):
        related = Table('related', metadata,
            Column('id', Integer, primary_key=True),
            mysql_engine='MyISAM'
        )
        t6 = Table("t6", metadata,
            Column('manual_id', Integer, ForeignKey('related.id'), primary_key=True),
            Column('auto_id', Integer, primary_key=True, test_needs_autoincrement=True),
            mysql_engine='MyISAM'
        )

        metadata.create_all()
        r = related.insert().values(id=12).execute()
        id = r.inserted_primary_key[0]
        assert id==12

        r = t6.insert().values(manual_id=id).execute()
        eq_(r.inserted_primary_key, [12, 1])

    def test_autoclose_on_insert(self):
        if testing.against('firebird', 'postgresql', 'oracle', 'mssql'):
            test_engines = [
                engines.testing_engine(options={'implicit_returning':False}),
                engines.testing_engine(options={'implicit_returning':True}),
            ]
        else:
            test_engines = [testing.db]

        for engine in test_engines:

            r = engine.execute(users.insert(), 
                {'user_name':'jack'},
            )
            assert r.closed

    def test_row_iteration(self):
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9, 'user_name':'fred'},
        )
        r = users.select().execute()
        l = []
        for row in r:
            l.append(row)
        self.assert_(len(l) == 3)

    @testing.fails_on('firebird', "kinterbasdb doesn't send full type information")
    @testing.requires.subqueries
    def test_anonymous_rows(self):
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9, 'user_name':'fred'},
        )

        sel = select([users.c.user_id]).where(users.c.user_name=='jack').as_scalar()
        for row in select([sel + 1, sel + 3], bind=users.bind).execute():
            assert row['anon_1'] == 8
            assert row['anon_2'] == 10

    @testing.fails_on('firebird', "kinterbasdb doesn't send full type information")
    def test_order_by_label(self):
        """test that a label within an ORDER BY works on each backend.

        This test should be modified to support [ticket:1068] when that ticket
        is implemented.  For now, you need to put the actual string in the
        ORDER BY.

        """

        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9, 'user_name':'fred'},
        )

        concat = ("test: " + users.c.user_name).label('thedata')
        print select([concat]).order_by("thedata")
        eq_(
            select([concat]).order_by("thedata").execute().fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)]
        )

        eq_(
            select([concat]).order_by("thedata").execute().fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)]
        )

        concat = ("test: " + users.c.user_name).label('thedata')
        eq_(
            select([concat]).order_by(desc('thedata')).execute().fetchall(),
            [("test: jack",), ("test: fred",), ("test: ed",)]
        )

        @testing.fails_on('postgresql', 'only simple labels allowed')
        @testing.fails_on('sybase', 'only simple labels allowed')
        @testing.fails_on('mssql', 'only simple labels allowed')
        def go():
            concat = ("test: " + users.c.user_name).label('thedata')
            eq_(
                select([concat]).order_by(literal_column('thedata') + "x").execute().fetchall(),
                [("test: ed",), ("test: fred",), ("test: jack",)]
            )
        go()


    def test_row_comparison(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        rp = users.select().execute().first()

        self.assert_(rp == rp)
        self.assert_(not(rp != rp))

        equal = (7, 'jack')

        self.assert_(rp == equal)
        self.assert_(equal == rp)
        self.assert_(not (rp != equal))
        self.assert_(not (equal != equal))

    @testing.provide_metadata
    def test_column_label_overlap_fallback(self):
        content = Table('content', self.metadata,
            Column('type', String(30)),
        )
        bar = Table('bar', self.metadata, 
            Column('content_type', String(30))
        )
        self.metadata.create_all(testing.db)
        testing.db.execute(content.insert().values(type="t1"))

        row = testing.db.execute(content.select(use_labels=True)).first()
        assert content.c.type in row
        assert bar.c.content_type not in row
        assert sql.column('content_type') in row

        row = testing.db.execute(select([content.c.type.label("content_type")])).first()
        assert content.c.type in row
        assert bar.c.content_type not in row
        assert sql.column('content_type') in row

        row = testing.db.execute(select([func.now().label("content_type")])).first()
        assert content.c.type not in row
        assert bar.c.content_type not in row
        assert sql.column('content_type') in row

    def test_pickled_rows(self):
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9, 'user_name':'fred'},
        )

        for pickle in False, True:
            for use_labels in False, True:
                result = users.select(use_labels=use_labels).order_by(users.c.user_id).execute().fetchall()

                if pickle:
                    result = util.pickle.loads(util.pickle.dumps(result))

                eq_(
                    result, 
                    [(7, "jack"), (8, "ed"), (9, "fred")]
                )
                if use_labels:
                    eq_(result[0]['query_users_user_id'], 7)
                    eq_(result[0].keys(), ["query_users_user_id", "query_users_user_name"])
                else:
                    eq_(result[0]['user_id'], 7)
                    eq_(result[0].keys(), ["user_id", "user_name"])

                eq_(result[0][0], 7)
                eq_(result[0][users.c.user_id], 7)
                eq_(result[0][users.c.user_name], 'jack')

                if not pickle or use_labels:
                    assert_raises(exc.NoSuchColumnError, lambda: result[0][addresses.c.user_id])
                else:
                    # test with a different table.  name resolution is 
                    # causing 'user_id' to match when use_labels wasn't used.
                    eq_(result[0][addresses.c.user_id], 7)

                assert_raises(exc.NoSuchColumnError, lambda: result[0]['fake key'])
                assert_raises(exc.NoSuchColumnError, lambda: result[0][addresses.c.address_id])

    def test_column_error_printing(self):
        row = testing.db.execute(select([1])).first()
        class unprintable(object):
            def __str__(self):
                raise ValueError("nope")

        msg = r"Could not locate column in row for column '%s'"

        for accessor, repl in [
            ("x", "x"),
            (Column("q", Integer), "q"),
            (Column("q", Integer) + 12, r"q \+ :q_1"),
            (unprintable(), "unprintable element.*"),
        ]:
            assert_raises_message(
                exc.NoSuchColumnError, 
                msg % repl,
                lambda: row[accessor]
            )


    @testing.requires.boolean_col_expressions
    def test_or_and_as_columns(self):
        true, false = literal(True), literal(False)

        eq_(testing.db.execute(select([and_(true, false)])).scalar(), False)
        eq_(testing.db.execute(select([and_(true, true)])).scalar(), True)
        eq_(testing.db.execute(select([or_(true, false)])).scalar(), True)
        eq_(testing.db.execute(select([or_(false, false)])).scalar(), False)
        eq_(testing.db.execute(select([not_(or_(false, false))])).scalar(), True)

        row = testing.db.execute(select([or_(false, false).label("x"), and_(true, false).label("y")])).first()
        assert row.x == False
        assert row.y == False

        row = testing.db.execute(select([or_(true, false).label("x"), and_(true, false).label("y")])).first()
        assert row.x == True
        assert row.y == False

    def test_fetchmany(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'ed')
        users.insert().execute(user_id = 9, user_name = 'fred')
        r = users.select().execute()
        l = []
        for row in r.fetchmany(size=2):
            l.append(row)
        self.assert_(len(l) == 2, "fetchmany(size=2) got %s rows" % len(l))

    def test_like_ops(self):
        users.insert().execute(
            {'user_id':1, 'user_name':'apples'},
            {'user_id':2, 'user_name':'oranges'},
            {'user_id':3, 'user_name':'bananas'},
            {'user_id':4, 'user_name':'legumes'},
            {'user_id':5, 'user_name':'hi % there'},
        )

        for expr, result in (
            (select([users.c.user_id]).\
                    where(users.c.user_name.startswith('apple')), [(1,)]),
            (select([users.c.user_id]).\
                    where(users.c.user_name.contains('i % t')), [(5,)]),
            (select([users.c.user_id]).\
                    where(
                        users.c.user_name.endswith('anas')
                    ), [(3,)]),
            (select([users.c.user_id]).\
                    where(
                        users.c.user_name.contains('i % t', escape='\\')
                    ), [(5,)]),
        ):
            eq_(expr.execute().fetchall(), result)

    @testing.fails_on("firebird", "see dialect.test_firebird:MiscTest.test_percents_in_text")
    @testing.fails_on("oracle", "neither % nor %% are accepted")
    @testing.fails_on("informix", "neither % nor %% are accepted")
    @testing.fails_on("+pg8000", "can't interpret result column from '%%'")
    @testing.emits_warning('.*now automatically escapes.*')
    def test_percents_in_text(self):
        for expr, result in (
            (text("select 6 % 10"), 6),
            (text("select 17 % 10"), 7),
            (text("select '%'"), '%'),
            (text("select '%%'"), '%%'),
            (text("select '%%%'"), '%%%'),
            (text("select 'hello % world'"), "hello % world")
        ):
            eq_(testing.db.scalar(expr), result)

    def test_ilike(self):
        users.insert().execute(
            {'user_id':1, 'user_name':'one'},
            {'user_id':2, 'user_name':'TwO'},
            {'user_id':3, 'user_name':'ONE'},
            {'user_id':4, 'user_name':'OnE'},
        )

        eq_(select([users.c.user_id]).where(users.c.user_name.ilike('one')).execute().fetchall(), [(1, ), (3, ), (4, )])

        eq_(select([users.c.user_id]).where(users.c.user_name.ilike('TWO')).execute().fetchall(), [(2, )])

        if testing.against('postgresql'):
            eq_(select([users.c.user_id]).where(users.c.user_name.like('one')).execute().fetchall(), [(1, )])
            eq_(select([users.c.user_id]).where(users.c.user_name.like('TWO')).execute().fetchall(), [])


    def test_compiled_execute(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        s = select([users], users.c.user_id==bindparam('id')).compile()
        c = testing.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_compiled_insert_execute(self):
        users.insert().compile().execute(user_id = 7, user_name = 'jack')
        s = select([users], users.c.user_id==bindparam('id')).compile()
        c = testing.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_repeated_bindparams(self):
        """Tests that a BindParam can be used more than once.

        This should be run for DB-APIs with both positional and named
        paramstyles.
        """

        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')

        u = bindparam('userid')
        s = users.select(and_(users.c.user_name==u, users.c.user_name==u))
        r = s.execute(userid='fred').fetchall()
        assert len(r) == 1

    def test_bindparam_detection(self):
        dialect = default.DefaultDialect(paramstyle='qmark')
        prep = lambda q: str(sql.text(q).compile(dialect=dialect))

        def a_eq(got, wanted):
            if got != wanted:
                print "Wanted %s" % wanted
                print "Received %s" % got
            self.assert_(got == wanted, got)

        a_eq(prep('select foo'), 'select foo')
        a_eq(prep("time='12:30:00'"), "time='12:30:00'")
        a_eq(prep(u"time='12:30:00'"), u"time='12:30:00'")
        a_eq(prep(":this:that"), ":this:that")
        a_eq(prep(":this :that"), "? ?")
        a_eq(prep("(:this),(:that :other)"), "(?),(? ?)")
        a_eq(prep("(:this),(:that:other)"), "(?),(:that:other)")
        a_eq(prep("(:this),(:that,:other)"), "(?),(?,?)")
        a_eq(prep("(:that_:other)"), "(:that_:other)")
        a_eq(prep("(:that_ :other)"), "(? ?)")
        a_eq(prep("(:that_other)"), "(?)")
        a_eq(prep("(:that$other)"), "(?)")
        a_eq(prep("(:that$:other)"), "(:that$:other)")
        a_eq(prep(".:that$ :other."), ".? ?.")

        a_eq(prep(r'select \foo'), r'select \foo')
        a_eq(prep(r"time='12\:30:00'"), r"time='12\:30:00'")
        a_eq(prep(":this \:that"), "? :that")
        a_eq(prep(r"(\:that$other)"), "(:that$other)")
        a_eq(prep(r".\:that$ :other."), ".:that$ ?.")

    def test_select_from_bindparam(self):
        """Test result row processing when selecting from a plain bind param."""

        class MyInteger(TypeDecorator):
            impl = Integer
            def process_bind_param(self, value, dialect):
                return int(value[4:])

            def process_result_value(self, value, dialect):
                return "INT_%d" % value

        eq_(
            testing.db.scalar(select([literal("INT_5", type_=MyInteger)])),
            "INT_5"
        )
        eq_(
            testing.db.scalar(select([literal("INT_5", type_=MyInteger).label('foo')])),
            "INT_5"
        )


    def test_delete(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        print repr(users.select().execute().fetchall())

        users.delete(users.c.user_name == 'fred').execute()

        print repr(users.select().execute().fetchall())



    @testing.exclude('mysql', '<', (5, 0, 37), 'database bug')
    def test_scalar_select(self):
        """test that scalar subqueries with labels get their type propagated to the result set."""

        # mysql and/or mysqldb has a bug here, type isn't propagated for scalar
        # subquery.
        datetable = Table('datetable', metadata,
            Column('id', Integer, primary_key=True),
            Column('today', DateTime))
        datetable.create()
        try:
            datetable.insert().execute(id=1, today=datetime.datetime(2006, 5, 12, 12, 0, 0))
            s = select([datetable.alias('x').c.today]).as_scalar()
            s2 = select([datetable.c.id, s.label('somelabel')])
            #print s2.c.somelabel.type
            assert isinstance(s2.execute().first()['somelabel'], datetime.datetime)
        finally:
            datetable.drop()

    def test_order_by(self):
        """Exercises ORDER BY clause generation.

        Tests simple, compound, aliased and DESC clauses.
        """

        users.insert().execute(user_id=1, user_name='c')
        users.insert().execute(user_id=2, user_name='b')
        users.insert().execute(user_id=3, user_name='a')

        def a_eq(executable, wanted):
            got = list(executable.execute())
            eq_(got, wanted)

        for labels in False, True:
            a_eq(users.select(order_by=[users.c.user_id],
                              use_labels=labels),
                 [(1, 'c'), (2, 'b'), (3, 'a')])

            a_eq(users.select(order_by=[users.c.user_name, users.c.user_id],
                              use_labels=labels),
                 [(3, 'a'), (2, 'b'), (1, 'c')])

            a_eq(select([users.c.user_id.label('foo')],
                        use_labels=labels,
                        order_by=[users.c.user_id]),
                 [(1,), (2,), (3,)])

            a_eq(select([users.c.user_id.label('foo'), users.c.user_name],
                        use_labels=labels,
                        order_by=[users.c.user_name, users.c.user_id]),
                 [(3, 'a'), (2, 'b'), (1, 'c')])

            a_eq(users.select(distinct=True,
                              use_labels=labels,
                              order_by=[users.c.user_id]),
                 [(1, 'c'), (2, 'b'), (3, 'a')])

            a_eq(select([users.c.user_id.label('foo')],
                        distinct=True,
                        use_labels=labels,
                        order_by=[users.c.user_id]),
                 [(1,), (2,), (3,)])

            a_eq(select([users.c.user_id.label('a'),
                         users.c.user_id.label('b'),
                         users.c.user_name],
                        use_labels=labels,
                        order_by=[users.c.user_id]),
                 [(1, 1, 'c'), (2, 2, 'b'), (3, 3, 'a')])

            a_eq(users.select(distinct=True,
                              use_labels=labels,
                              order_by=[desc(users.c.user_id)]),
                 [(3, 'a'), (2, 'b'), (1, 'c')])

            a_eq(select([users.c.user_id.label('foo')],
                        distinct=True,
                        use_labels=labels,
                        order_by=[users.c.user_id.desc()]),
                 [(3,), (2,), (1,)])

    @testing.requires.nullsordering
    def test_order_by_nulls(self):
        """Exercises ORDER BY clause generation.

        Tests simple, compound, aliased and DESC clauses.
        """

        users.insert().execute(user_id=1)
        users.insert().execute(user_id=2, user_name='b')
        users.insert().execute(user_id=3, user_name='a')

        def a_eq(executable, wanted):
            got = list(executable.execute())
            eq_(got, wanted)

        for labels in False, True:
            a_eq(users.select(order_by=[users.c.user_name.nullsfirst()],
                              use_labels=labels),
                 [(1, None), (3, 'a'), (2, 'b')])

            a_eq(users.select(order_by=[users.c.user_name.nullslast()],
                              use_labels=labels),
                 [(3, 'a'), (2, 'b'), (1, None)])

            a_eq(users.select(order_by=[asc(users.c.user_name).nullsfirst()],
                              use_labels=labels),
                 [(1, None), (3, 'a'), (2, 'b')])

            a_eq(users.select(order_by=[asc(users.c.user_name).nullslast()],
                              use_labels=labels),
                 [(3, 'a'), (2, 'b'), (1, None)])

            a_eq(users.select(order_by=[users.c.user_name.desc().nullsfirst()],
                              use_labels=labels),
                 [(1, None), (2, 'b'), (3, 'a')])

            a_eq(users.select(order_by=[users.c.user_name.desc().nullslast()],
                              use_labels=labels),
                 [(2, 'b'), (3, 'a'), (1, None)])

            a_eq(users.select(order_by=[desc(users.c.user_name).nullsfirst()],
                              use_labels=labels),
                 [(1, None), (2, 'b'), (3, 'a')])

            a_eq(users.select(order_by=[desc(users.c.user_name).nullslast()],
                              use_labels=labels),
                 [(2, 'b'), (3, 'a'), (1, None)])

            a_eq(users.select(order_by=[users.c.user_name.nullsfirst(), users.c.user_id],
                              use_labels=labels),
                 [(1, None), (3, 'a'), (2, 'b')])

            a_eq(users.select(order_by=[users.c.user_name.nullslast(), users.c.user_id],
                              use_labels=labels),
                 [(3, 'a'), (2, 'b'), (1, None)])

    @testing.fails_on("+pyodbc", "pyodbc row doesn't seem to accept slices")
    def test_column_slices(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        addresses.insert().execute(address_id=1, user_id=2, address='foo@bar.com')

        r = text("select * from query_addresses", bind=testing.db).execute().first()
        self.assert_(r[0:1] == (1,))
        self.assert_(r[1:] == (2, 'foo@bar.com'))
        self.assert_(r[:-1] == (1, 2))

    def test_column_accessor(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        addresses.insert().execute(address_id=1, user_id=2, address='foo@bar.com')

        r = users.select(users.c.user_id==2).execute().first()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        r = text("select * from query_users where user_id=2", bind=testing.db).execute().first()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        # test a little sqlite weirdness - with the UNION, 
        # cols come back as "query_users.user_id" in cursor.description
        r = text("select query_users.user_id, query_users.user_name from query_users "
            "UNION select query_users.user_id, query_users.user_name from query_users",
            bind=testing.db).execute().first()
        self.assert_(r['user_id']) == 1
        self.assert_(r['user_name']) == "john"

        # test using literal tablename.colname
        r = text('select query_users.user_id AS "query_users.user_id", '
                'query_users.user_name AS "query_users.user_name" from query_users', 
                bind=testing.db).execute().first()
        self.assert_(r['query_users.user_id']) == 1
        self.assert_(r['query_users.user_name']) == "john"

        # unary experssions
        r = select([users.c.user_name.distinct()]).order_by(users.c.user_name).execute().first()
        eq_(r[users.c.user_name], 'jack')
        eq_(r.user_name, 'jack')

    @testing.requires.dbapi_lastrowid
    def test_native_lastrowid(self):
        r = testing.db.execute(
            users.insert(),
            {'user_id':1, 'user_name':'ed'}
        )

        eq_(r.lastrowid, 1)

    def test_returns_rows_flag_insert(self):
        r = testing.db.execute(
            users.insert(),
            {'user_id':1, 'user_name':'ed'}
        )
        assert r.is_insert
        assert not r.returns_rows

    def test_returns_rows_flag_update(self):
        r = testing.db.execute(
            users.update().values(user_name='fred')
        )
        assert not r.is_insert
        assert not r.returns_rows

    def test_returns_rows_flag_select(self):
        r = testing.db.execute(
            users.select()
        )
        assert not r.is_insert
        assert r.returns_rows

    @testing.requires.returning
    def test_returns_rows_flag_insert_returning(self):
        r = testing.db.execute(
            users.insert().returning(users.c.user_id),
            {'user_id':1, 'user_name':'ed'}
        )
        assert r.is_insert
        assert r.returns_rows

    def test_graceful_fetch_on_non_rows(self):
        """test that calling fetchone() etc. on a result that doesn't
        return rows fails gracefully.

        """

        # these proxies don't work with no cursor.description present.
        # so they don't apply to this test at the moment.
        # base.FullyBufferedResultProxy,
        # base.BufferedRowResultProxy,
        # base.BufferedColumnResultProxy

        conn = testing.db.connect()
        for meth in ('fetchone', 'fetchall', 'first', 'scalar', 'fetchmany'):
            trans = conn.begin()
            result = conn.execute(users.insert(), user_id=1)
            assert_raises_message(
                exc.ResourceClosedError,
                "This result object does not return rows. "
                "It has been closed automatically.",
                getattr(result, meth),
            )
            trans.rollback()

    def test_no_inserted_pk_on_non_insert(self):
        result = testing.db.execute("select * from query_users")
        assert_raises_message(
            exc.InvalidRequestError,
            r"Statement is not an insert\(\) expression construct.",
            getattr, result, 'inserted_primary_key'
        )

    @testing.requires.returning
    def test_no_inserted_pk_on_returning(self):
        result = testing.db.execute(users.insert().returning(users.c.user_id, users.c.user_name))
        assert_raises_message(
            exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr, result, 'inserted_primary_key'
        )

    def test_fetchone_til_end(self):
        result = testing.db.execute("select * from query_users")
        eq_(result.fetchone(), None)
        assert_raises_message(
            exc.ResourceClosedError,
            "This result object is closed.",
            result.fetchone
        )

    def test_result_case_sensitivity(self):
        """test name normalization for result sets."""

        row = testing.db.execute(
            select([
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive")
            ])
        ).first()

        assert row.keys() == ["case_insensitive", "CaseSensitive"]


    def test_row_as_args(self):
        users.insert().execute(user_id=1, user_name='john')
        r = users.select(users.c.user_id==1).execute().first()
        users.delete().execute()
        users.insert().execute(r)
        eq_(users.select().execute().fetchall(), [(1, 'john')])

    def test_result_as_args(self):
        users.insert().execute([dict(user_id=1, user_name='john'), dict(user_id=2, user_name='ed')])
        r = users.select().execute()
        users2.insert().execute(list(r))
        assert users2.select().execute().fetchall() == [(1, 'john'), (2, 'ed')]

        users2.delete().execute()
        r = users.select().execute()
        users2.insert().execute(*list(r))
        assert users2.select().execute().fetchall() == [(1, 'john'), (2, 'ed')]

    def test_ambiguous_column(self):
        users.insert().execute(user_id=1, user_name='john')
        r = users.outerjoin(addresses).select().execute().first()
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

        r = util.pickle.loads(util.pickle.dumps(r))
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

        result = users.outerjoin(addresses).select().execute()
        result = base.BufferedColumnResultProxy(result.context)
        r = result.first()
        assert isinstance(r, base.BufferedColumnRow)
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

    @testing.requires.subqueries
    def test_column_label_targeting(self):
        users.insert().execute(user_id=7, user_name='ed')

        for s in (
            users.select().alias('foo'),
            users.select().alias(users.name),
        ):
            row = s.select(use_labels=True).execute().first()
            assert row[s.c.user_id] == 7
            assert row[s.c.user_name] == 'ed'

    def test_keys(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute()
        eq_([x.lower() for x in r.keys()], ['user_id', 'user_name'])
        r = r.first()
        eq_([x.lower() for x in r.keys()], ['user_id', 'user_name'])

    def test_items(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().first()
        eq_([(x[0].lower(), x[1]) for x in r.items()], [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().first()
        eq_(len(r), 2)

        r = testing.db.execute('select user_name, user_id from query_users').first()
        eq_(len(r), 2)
        r = testing.db.execute('select user_name from query_users').first()
        eq_(len(r), 1)

    @testing.uses_deprecated(r'.*which subclass Executable')
    def test_cant_execute_join(self):
        try:
            users.join(addresses).execute()
        except exc.StatementError, e:
            assert str(e).startswith('Not an executable clause ')



    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select(users.c.user_id==1).execute().first()
        eq_(r[0], 1)
        eq_(r[1], 'foo')
        eq_([x.lower() for x in r.keys()], ['user_id', 'user_name'])
        eq_(r.values(), [1, 'foo'])

    def test_column_order_with_text_query(self):
        # should return values in query order
        users.insert().execute(user_id=1, user_name='foo')
        r = testing.db.execute('select user_name, user_id from query_users').first()
        eq_(r[0], 'foo')
        eq_(r[1], 1)
        eq_([x.lower() for x in r.keys()], ['user_name', 'user_id'])
        eq_(r.values(), ['foo', 1])

    @testing.crashes('oracle', 'FIXME: unknown, varify not fails_on()')
    @testing.crashes('firebird', 'An identifier must begin with a letter')
    @testing.crashes('maxdb', 'FIXME: unknown, verify not fails_on()')
    def test_column_accessor_shadow(self):
        meta = MetaData(testing.db)
        shadowed = Table('test_shadowed', meta,
                         Column('shadow_id', INT, primary_key = True),
                         Column('shadow_name', VARCHAR(20)),
                         Column('parent', VARCHAR(20)),
                         Column('row', VARCHAR(40)),
                         Column('_parent', VARCHAR(20)),
                         Column('_row', VARCHAR(20)),
        )
        shadowed.create(checkfirst=True)
        try:
            shadowed.insert().execute(shadow_id=1, shadow_name='The Shadow', parent='The Light', 
                                            row='Without light there is no shadow', 
                                            _parent='Hidden parent', 
                                            _row='Hidden row')
            r = shadowed.select(shadowed.c.shadow_id==1).execute().first()
            self.assert_(r.shadow_id == r['shadow_id'] == r[shadowed.c.shadow_id] == 1)
            self.assert_(r.shadow_name == r['shadow_name'] == r[shadowed.c.shadow_name] == 'The Shadow')
            self.assert_(r.parent == r['parent'] == r[shadowed.c.parent] == 'The Light')
            self.assert_(r.row == r['row'] == r[shadowed.c.row] == 'Without light there is no shadow')
            self.assert_(r['_parent'] == 'Hidden parent')
            self.assert_(r['_row'] == 'Hidden row')
            try:
                print r._parent, r._row
                self.fail('Should not allow access to private attributes')
            except AttributeError:
                pass # expected
        finally:
            shadowed.drop(checkfirst=True)

    @testing.emits_warning('.*empty sequence.*')
    def test_in_filtering(self):
        """test the behavior of the in_() function."""

        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        users.insert().execute(user_id = 9, user_name = None)

        s = users.select(users.c.user_name.in_([]))
        r = s.execute().fetchall()
        # No username is in empty set
        assert len(r) == 0

        s = users.select(not_(users.c.user_name.in_([])))
        r = s.execute().fetchall()
        # All usernames with a value are outside an empty set
        assert len(r) == 2

        s = users.select(users.c.user_name.in_(['jack','fred']))
        r = s.execute().fetchall()
        assert len(r) == 2

        s = users.select(not_(users.c.user_name.in_(['jack','fred'])))
        r = s.execute().fetchall()
        # Null values are not outside any set
        assert len(r) == 0

    @testing.emits_warning('.*empty sequence.*')
    @testing.fails_on('firebird', "uses sql-92 rules")
    @testing.fails_on('sybase', "uses sql-92 rules")
    @testing.fails_on('mssql+mxodbc', "uses sql-92 rules")
    @testing.fails_if(lambda: 
                         testing.against('mssql+pyodbc') and not testing.db.dialect.freetds,
                         "uses sql-92 rules")
    def test_bind_in(self):
        """test calling IN against a bind parameter.

        this isn't allowed on several platforms since we
        generate ? = ?.

        """

        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        users.insert().execute(user_id = 9, user_name = None)

        u = bindparam('search_key')

        s = users.select(not_(u.in_([])))
        r = s.execute(search_key='john').fetchall()
        assert len(r) == 3
        r = s.execute(search_key=None).fetchall()
        assert len(r) == 0

    @testing.emits_warning('.*empty sequence.*')
    @testing.fails_on('firebird', 'uses sql-92 bind rules')
    def test_literal_in(self):
        """similar to test_bind_in but use a bind with a value."""

        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        users.insert().execute(user_id = 9, user_name = None)

        s = users.select(not_(literal("john").in_([])))
        r = s.execute().fetchall()
        assert len(r) == 3


    @testing.emits_warning('.*empty sequence.*')
    @testing.requires.boolean_col_expressions
    def test_in_filtering_advanced(self):
        """test the behavior of the in_() function when 
        comparing against an empty collection, specifically
        that a proper boolean value is generated.

        """

        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        users.insert().execute(user_id = 9, user_name = None)

        s = users.select(users.c.user_name.in_([]) == True)
        r = s.execute().fetchall()
        assert len(r) == 0
        s = users.select(users.c.user_name.in_([]) == False)
        r = s.execute().fetchall()
        assert len(r) == 2
        s = users.select(users.c.user_name.in_([]) == None)
        r = s.execute().fetchall()
        assert len(r) == 1

class PercentSchemaNamesTest(fixtures.TestBase):
    """tests using percent signs, spaces in table and column names.

    Doesn't pass for mysql, postgresql, but this is really a 
    SQLAlchemy bug - we should be escaping out %% signs for this
    operation the same way we do for text() and column labels.

    """

    @classmethod
    def setup_class(cls):
        global percent_table, metadata
        metadata = MetaData(testing.db)
        percent_table = Table('percent%table', metadata,
            Column("percent%", Integer),
            Column("spaces % more spaces", Integer),
        )
        metadata.create_all()

    def teardown(self):
        percent_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.skip_if(lambda: testing.against('postgresql'), "psycopg2 2.4 no longer accepts % in bind placeholders")
    def test_single_roundtrip(self):
        percent_table.insert().execute(
            {'percent%':5, 'spaces % more spaces':12},
        )
        percent_table.insert().execute(
            {'percent%':7, 'spaces % more spaces':11},
        )
        percent_table.insert().execute(
            {'percent%':9, 'spaces % more spaces':10},
        )
        percent_table.insert().execute(
            {'percent%':11, 'spaces % more spaces':9},
        )
        self._assert_table()

    @testing.skip_if(lambda: testing.against('postgresql'), "psycopg2 2.4 no longer accepts % in bind placeholders")
    @testing.crashes('mysql+mysqldb', 'MySQLdb handles executemany() inconsistently vs. execute()')
    def test_executemany_roundtrip(self):
        percent_table.insert().execute(
            {'percent%':5, 'spaces % more spaces':12},
        )
        percent_table.insert().execute(
            {'percent%':7, 'spaces % more spaces':11},
            {'percent%':9, 'spaces % more spaces':10},
            {'percent%':11, 'spaces % more spaces':9},
        )
        self._assert_table()

    def _assert_table(self):
        for table in (percent_table, percent_table.alias()):
            eq_(
                table.select().order_by(table.c['percent%']).execute().fetchall(),
                [
                    (5, 12),
                    (7, 11),
                    (9, 10),
                    (11, 9)
                ]
            )

            eq_(
                table.select().
                    where(table.c['spaces % more spaces'].in_([9, 10])).
                    order_by(table.c['percent%']).execute().fetchall(),
                    [
                        (9, 10),
                        (11, 9)
                    ]
            )

            result = table.select().order_by(table.c['percent%']).execute()
            row = result.fetchone()
            eq_(row[table.c['percent%']], 5)
            eq_(row[table.c['spaces % more spaces']], 12)
            row = result.fetchone()
            eq_(row['percent%'], 7)
            eq_(row['spaces % more spaces'], 11)
            result.close()

        percent_table.update().values({percent_table.c['spaces % more spaces']:15}).execute()

        eq_(
            percent_table.select().order_by(percent_table.c['percent%']).execute().fetchall(),
            [
                (5, 15),
                (7, 15),
                (9, 15),
                (11, 15)
            ]
        )



class LimitTest(fixtures.TestBase):

    @classmethod
    def setup_class(cls):
        global users, addresses, metadata
        metadata = MetaData(testing.db)
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table('query_addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)))
        metadata.create_all()

        users.insert().execute(user_id=1, user_name='john')
        addresses.insert().execute(address_id=1, user_id=1, address='addr1')
        users.insert().execute(user_id=2, user_name='jack')
        addresses.insert().execute(address_id=2, user_id=2, address='addr1')
        users.insert().execute(user_id=3, user_name='ed')
        addresses.insert().execute(address_id=3, user_id=3, address='addr2')
        users.insert().execute(user_id=4, user_name='wendy')
        addresses.insert().execute(address_id=4, user_id=4, address='addr3')
        users.insert().execute(user_id=5, user_name='laura')
        addresses.insert().execute(address_id=5, user_id=5, address='addr4')
        users.insert().execute(user_id=6, user_name='ralph')
        addresses.insert().execute(address_id=6, user_id=6, address='addr5')
        users.insert().execute(user_id=7, user_name='fido')
        addresses.insert().execute(address_id=7, user_id=7, address='addr5')

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_select_limit(self):
        r = users.select(limit=3, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')], repr(r))

    @testing.requires.offset
    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_select_limit_offset(self):
        """Test the interaction between limit and offset"""

        r = users.select(limit=3, offset=2, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = users.select(offset=5, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(6, 'ralph'), (7, 'fido')])

    def test_select_distinct_limit(self):
        """Test the interaction between limit and distinct"""

        r = sorted([x[0] for x in select([addresses.c.address]).distinct().limit(3).order_by(addresses.c.address).execute().fetchall()])
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))

    @testing.requires.offset
    @testing.fails_on('mssql', 'FIXME: unknown')
    def test_select_distinct_offset(self):
        """Test the interaction between distinct and offset"""

        r = sorted([x[0] for x in select([addresses.c.address]).distinct().offset(1).order_by(addresses.c.address).execute().fetchall()])
        self.assert_(len(r) == 4, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2] and r[2] != [3], repr(r))

    @testing.requires.offset
    def test_select_distinct_limit_offset(self):
        """Test the interaction between limit and limit/offset"""

        r = select([addresses.c.address]).order_by(addresses.c.address).distinct().offset(2).limit(3).execute().fetchall()
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))

class CompoundTest(fixtures.TestBase):
    """test compound statements like UNION, INTERSECT, particularly their ability to nest on
    different databases."""
    @classmethod
    def setup_class(cls):
        global metadata, t1, t2, t3
        metadata = MetaData(testing.db)
        t1 = Table('t1', metadata,
            Column('col1', Integer, Sequence('t1pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30))
            )
        t2 = Table('t2', metadata,
            Column('col1', Integer, Sequence('t2pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        t3 = Table('t3', metadata,
            Column('col1', Integer, Sequence('t3pkseq'), primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        metadata.create_all()

        t1.insert().execute([
            dict(col2="t1col2r1", col3="aaa", col4="aaa"),
            dict(col2="t1col2r2", col3="bbb", col4="bbb"),
            dict(col2="t1col2r3", col3="ccc", col4="ccc"),
        ])
        t2.insert().execute([
            dict(col2="t2col2r1", col3="aaa", col4="bbb"),
            dict(col2="t2col2r2", col3="bbb", col4="ccc"),
            dict(col2="t2col2r3", col3="ccc", col4="aaa"),
        ])
        t3.insert().execute([
            dict(col2="t3col2r1", col3="aaa", col4="ccc"),
            dict(col2="t3col2r2", col3="bbb", col4="aaa"),
            dict(col2="t3col2r3", col3="ccc", col4="bbb"),
        ])

    @engines.close_first
    def teardown(self):
        pass

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def _fetchall_sorted(self, executed):
        return sorted([tuple(row) for row in executed.fetchall()])

    @testing.requires.subqueries
    def test_union(self):
        (s1, s2) = (
            select([t1.c.col3.label('col3'), t1.c.col4.label('col4')],
                   t1.c.col2.in_(["t1col2r1", "t1col2r2"])),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')],
                   t2.c.col2.in_(["t2col2r2", "t2col2r3"]))
        )
        u = union(s1, s2)

        wanted = [('aaa', 'aaa'), ('bbb', 'bbb'), ('bbb', 'ccc'),
                  ('ccc', 'aaa')]
        found1 = self._fetchall_sorted(u.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(u.alias('bar').select().execute())
        eq_(found2, wanted)

    @testing.fails_on('firebird', "doesn't like ORDER BY with UNIONs")
    def test_union_ordered(self):
        (s1, s2) = (
            select([t1.c.col3.label('col3'), t1.c.col4.label('col4')],
                   t1.c.col2.in_(["t1col2r1", "t1col2r2"])),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')],
                   t2.c.col2.in_(["t2col2r2", "t2col2r3"]))
        )
        u = union(s1, s2, order_by=['col3', 'col4'])

        wanted = [('aaa', 'aaa'), ('bbb', 'bbb'), ('bbb', 'ccc'),
                  ('ccc', 'aaa')]
        eq_(u.execute().fetchall(), wanted)

    @testing.fails_on('firebird', "doesn't like ORDER BY with UNIONs")
    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.requires.subqueries
    def test_union_ordered_alias(self):
        (s1, s2) = (
            select([t1.c.col3.label('col3'), t1.c.col4.label('col4')],
                   t1.c.col2.in_(["t1col2r1", "t1col2r2"])),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')],
                   t2.c.col2.in_(["t2col2r2", "t2col2r3"]))
        )
        u = union(s1, s2, order_by=['col3', 'col4'])

        wanted = [('aaa', 'aaa'), ('bbb', 'bbb'), ('bbb', 'ccc'),
                  ('ccc', 'aaa')]
        eq_(u.alias('bar').select().execute().fetchall(), wanted)

    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.fails_on('firebird', "has trouble extracting anonymous column from union subquery")
    @testing.fails_on('mysql', 'FIXME: unknown')
    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.fails_on('informix', "FIXME: unknown (maybe the second alias isn't allows)")
    def test_union_all(self):
        e = union_all(
            select([t1.c.col3]),
            union(
                select([t1.c.col3]),
                select([t1.c.col3]),
            )
        )

        wanted = [('aaa',),('aaa',),('bbb',), ('bbb',), ('ccc',),('ccc',)]
        found1 = self._fetchall_sorted(e.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('foo').select().execute())
        eq_(found2, wanted)

    def test_union_all_lightweight(self):
        """like test_union_all, but breaks the sub-union into 
        a subquery with an explicit column reference on the outside,
        more palatable to a wider variety of engines.

        """

        u = union(
            select([t1.c.col3]),
            select([t1.c.col3]),
        ).alias()

        e = union_all(
            select([t1.c.col3]),
            select([u.c.col3])
        )

        wanted = [('aaa',),('aaa',),('bbb',), ('bbb',), ('ccc',),('ccc',)]
        found1 = self._fetchall_sorted(e.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('foo').select().execute())
        eq_(found2, wanted)

    @testing.requires.intersect
    def test_intersect(self):
        i = intersect(
            select([t2.c.col3, t2.c.col4]),
            select([t2.c.col3, t2.c.col4], t2.c.col4==t3.c.col3)
        )

        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]

        found1 = self._fetchall_sorted(i.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(i.alias('bar').select().execute())
        eq_(found2, wanted)

    @testing.requires.except_
    @testing.fails_on('sqlite', "Can't handle this style of nesting")
    def test_except_style1(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found = self._fetchall_sorted(e.alias().select().execute())
        eq_(found, wanted)

    @testing.requires.except_
    def test_except_style2(self):
        # same as style1, but add alias().select() to the except_().
        # sqlite can handle it now.

        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ).alias().select(), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found1 = self._fetchall_sorted(e.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(e.alias().select().execute())
        eq_(found2, wanted)

    @testing.fails_on('sqlite', "Can't handle this style of nesting")
    @testing.requires.except_
    def test_except_style3(self):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        e = except_(
            select([t1.c.col3]), # aaa, bbb, ccc
            except_(
                select([t2.c.col3]), # aaa, bbb, ccc
                select([t3.c.col3], t3.c.col3 == 'ccc'), #ccc
            )
        )
        eq_(e.execute().fetchall(), [('ccc',)])
        eq_(e.alias('foo').select().execute().fetchall(),
                          [('ccc',)])

    @testing.requires.except_
    def test_except_style4(self):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        e = except_(
            select([t1.c.col3]), # aaa, bbb, ccc
            except_(
                select([t2.c.col3]), # aaa, bbb, ccc
                select([t3.c.col3], t3.c.col3 == 'ccc'), #ccc
            ).alias().select()
        )

        eq_(e.execute().fetchall(), [('ccc',)])
        eq_(
            e.alias().select().execute().fetchall(),
            [('ccc',)]
        )

    @testing.requires.intersect
    @testing.fails_on('sqlite', "sqlite can't handle leading parenthesis")
    def test_intersect_unions(self):
        u = intersect(
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ),
            union(
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias().select()
        )
        wanted = [('aaa', 'ccc'), ('bbb', 'aaa'), ('ccc', 'bbb')]
        found = self._fetchall_sorted(u.execute())

        eq_(found, wanted)

    @testing.requires.intersect
    def test_intersect_unions_2(self):
        u = intersect(
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias().select(),
            union(
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias().select()
        )
        wanted = [('aaa', 'ccc'), ('bbb', 'aaa'), ('ccc', 'bbb')]
        found = self._fetchall_sorted(u.execute())

        eq_(found, wanted)

    @testing.requires.intersect
    def test_intersect_unions_3(self):
        u = intersect(
            select([t2.c.col3, t2.c.col4]),
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias().select()
        )
        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        found = self._fetchall_sorted(u.execute())

        eq_(found, wanted)

    @testing.requires.intersect
    def test_composite_alias(self):
        ua = intersect(
            select([t2.c.col3, t2.c.col4]),
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias().select()
        ).alias()

        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        found = self._fetchall_sorted(ua.select().execute())
        eq_(found, wanted)


class JoinTest(fixtures.TestBase):
    """Tests join execution.

    The compiled SQL emitted by the dialect might be ANSI joins or
    theta joins ('old oracle style', with (+) for OUTER).  This test
    tries to exercise join syntax and uncover any inconsistencies in
    `JOIN rhs ON lhs.col=rhs.col` vs `rhs.col=lhs.col`.  At least one
    database seems to be sensitive to this.
    """

    @classmethod
    def setup_class(cls):
        global metadata
        global t1, t2, t3

        metadata = MetaData(testing.db)
        t1 = Table('t1', metadata,
                   Column('t1_id', Integer, primary_key=True),
                   Column('name', String(32)))
        t2 = Table('t2', metadata,
                   Column('t2_id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.t1_id')),
                   Column('name', String(32)))
        t3 = Table('t3', metadata,
                   Column('t3_id', Integer, primary_key=True),
                   Column('t2_id', Integer, ForeignKey('t2.t2_id')),
                   Column('name', String(32)))
        metadata.drop_all()
        metadata.create_all()

        # t1.10 -> t2.20 -> t3.30
        # t1.11 -> t2.21
        # t1.12
        t1.insert().execute({'t1_id': 10, 'name': 't1 #10'},
                            {'t1_id': 11, 'name': 't1 #11'},
                            {'t1_id': 12, 'name': 't1 #12'})
        t2.insert().execute({'t2_id': 20, 't1_id': 10, 'name': 't2 #20'},
                            {'t2_id': 21, 't1_id': 11, 'name': 't2 #21'})
        t3.insert().execute({'t3_id': 30, 't2_id': 20, 'name': 't3 #30'})

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def assertRows(self, statement, expected):
        """Execute a statement and assert that rows returned equal expected."""

        found = sorted([tuple(row)
                       for row in statement.execute().fetchall()])

        eq_(found, sorted(expected))

    def test_join_x1(self):
        """Joins t1->t2."""

        for criteria in (t1.c.t1_id==t2.c.t1_id, t2.c.t1_id==t1.c.t1_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2, criteria)])
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_join_x2(self):
        """Joins t1->t2->t3."""

        for criteria in (t1.c.t1_id==t2.c.t1_id, t2.c.t1_id==t1.c.t1_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2, criteria)])
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_outerjoin_x1(self):
        """Outer joins t1->t2."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2).join(t3, criteria)])
            self.assertRows(expr, [(10, 20)])

    def test_outerjoin_x2(self):
        """Outer joins t1->t2,t3."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                from_obj=[t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id). \
                          outerjoin(t3, criteria)])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None), (12, None, None)])

    def test_outerjoin_where_x2_t1(self):
        """Outer joins t1->t2,t3, where on t1."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t1.c.name == 't1 #10',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t1.c.t1_id < 12,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t2(self):
        """Outer joins t1->t2,t3, where on t2."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t2.c.name == 't2 #20',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t2.c.t2_id < 29,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t1t2(self):
        """Outer joins t1->t2,t3, where on t1 and t2."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t2.c.name == 't2 #20'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 19, 29 > t2.c.t2_id),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t3(self):
        """Outer joins t1->t2,t3, where on t3."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t3.c.name == 't3 #30',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t3.c.t3_id < 39,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t3(self):
        """Outer joins t1->t2,t3, where on t1 and t3."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t3.c.name == 't3 #30'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 19, t3.c.t3_id < 39),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t2(self):
        """Outer joins t1->t2,t3, where on t1 and t2."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t2.c.name == 't2 #20'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 12, t2.c.t2_id < 39),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t1t2t3(self):
        """Outer joins t1->t2,t3, where on t1, t2 and t3."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10',
                     t2.c.name == 't2 #20',
                     t3.c.name == 't3 #30'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 19,
                     t2.c.t2_id < 29,
                     t3.c.t3_id < 39),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id==t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_mixed(self):
        """Joins t1->t2, outer t2->t3."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            print expr
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_mixed_where(self):
        """Joins t1->t2, outer t2->t3, plus a where on each table in turn."""

        for criteria in (t2.c.t2_id==t3.c.t2_id, t3.c.t2_id==t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t1.c.name == 't1 #10',
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t2.c.name == 't2 #20',
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t3.c.name == 't3 #30',
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t2.c.name == 't2 #20'),
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t2.c.name == 't2 #20', t3.c.name == 't3 #30'),
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10',
                     t2.c.name == 't2 #20',
                     t3.c.name == 't3 #30'),
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])


class OperatorTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        global metadata, flds
        metadata = MetaData(testing.db)
        flds = Table('flds', metadata,
            Column('idcol', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('intcol', Integer),
            Column('strcol', String(50)),
            )
        metadata.create_all()

        flds.insert().execute([
            dict(intcol=5, strcol='foo'),
            dict(intcol=13, strcol='bar')
        ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()


    # TODO: seems like more tests warranted for this setup.
    def test_modulo(self):
        eq_(
            select([flds.c.intcol % 3],
                   order_by=flds.c.idcol).execute().fetchall(),
            [(2,),(1,)]
        )

    @testing.requires.window_functions
    def test_over(self):
        eq_(
            select([
                flds.c.intcol, func.row_number().over(order_by=flds.c.strcol)
            ]).execute().fetchall(),
            [(13, 1L), (5, 2L)]
        )



