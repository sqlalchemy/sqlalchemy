import datetime
from sqlalchemy import *
from sqlalchemy import exc, sql
from sqlalchemy.engine import default
from sqlalchemy.test import *
from sqlalchemy.test.testing import eq_

class QueryTest(TestBase):

    @classmethod
    def setup_class(cls):
        global users, users2, addresses, metadata
        metadata = MetaData(testing.db)
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table('query_addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)))
            
        users2 = Table('u2', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    def tearDown(self):
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
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9}
        )
        assert users.select().execute().fetchall() == [(7, 'jack'), (8, 'ed'), (9, None)]

    def test_update(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1

        users.update(users.c.user_id == 7).execute(user_name = 'fred')
        assert users.select(users.c.user_id==7).execute().fetchone()['user_name'] == 'fred'

    def test_lastrow_accessor(self):
        """Tests the last_inserted_ids() and lastrow_has_id() functions."""

        def insert_values(table, values):
            """
            Inserts a row into a table, returns the full list of values
            INSERTed including defaults that fired off on the DB side and
            detects rows that had defaults and post-fetches.
            """

            result = table.insert().execute(**values)
            ret = values.copy()
            
            for col, id in zip(table.primary_key, result.last_inserted_ids()):
                ret[col.key] = id

            if result.lastrow_has_defaults():
                criterion = and_(*[col==id for col, id in zip(table.primary_key, result.last_inserted_ids())])
                row = table.select(criterion).execute().fetchone()
                for c in table.c:
                    ret[c.key] = row[c]
            return ret

        for supported, table, values, assertvalues in [
            (
                {'unsupported':['sqlite']},
                Table("t1", metadata,
                    Column('id', Integer, Sequence('t1_id_seq', optional=True), primary_key=True),
                    Column('foo', String(30), primary_key=True)),
                {'foo':'hi'},
                {'id':1, 'foo':'hi'}
            ),
            (
                {'unsupported':['sqlite']},
                Table("t2", metadata,
                    Column('id', Integer, Sequence('t2_id_seq', optional=True), primary_key=True),
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
        ]:
            if testing.db.name in supported['unsupported']:
                continue
            try:
                table.create()
                i = insert_values(table, values)
                assert i == assertvalues, repr(i) + " " + repr(assertvalues)
            finally:
                table.drop()

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

    @testing.fails_on('firebird', 'Data type unknown')
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

    def test_order_by_label(self):
        """test that a label within an ORDER BY works on each backend.
        
        simple labels in ORDER BYs now render as the actual labelname 
        which not every database supports.
        
        """
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9, 'user_name':'fred'},
        )
        
        concat = ("test: " + users.c.user_name).label('thedata')
        eq_(
            select([concat]).order_by(concat).execute().fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)]
        )

        concat = ("test: " + users.c.user_name).label('thedata')
        eq_(
            select([concat]).order_by(desc(concat)).execute().fetchall(),
            [("test: jack",), ("test: fred",), ("test: ed",)]
        )

        concat = ("test: " + users.c.user_name).label('thedata')
        eq_(
            select([concat]).order_by(concat + "x").execute().fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)]
        )
        
        
    def test_row_comparison(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        rp = users.select().execute().fetchone()

        self.assert_(rp == rp)
        self.assert_(not(rp != rp))

        equal = (7, 'jack')

        self.assert_(rp == equal)
        self.assert_(equal == rp)
        self.assert_(not (rp != equal))
        self.assert_(not (equal != equal))

    @testing.fails_on('mssql', 'No support for boolean logic in column select.')
    @testing.fails_on('oracle', 'FIXME: unknown')
    def test_or_and_as_columns(self):
        true, false = literal(True), literal(False)
        
        eq_(testing.db.execute(select([and_(true, false)])).scalar(), False)
        eq_(testing.db.execute(select([and_(true, true)])).scalar(), True)
        eq_(testing.db.execute(select([or_(true, false)])).scalar(), True)
        eq_(testing.db.execute(select([or_(false, false)])).scalar(), False)
        eq_(testing.db.execute(select([not_(or_(false, false))])).scalar(), True)

        row = testing.db.execute(select([or_(false, false).label("x"), and_(true, false).label("y")])).fetchone()
        assert row.x == False
        assert row.y == False

        row = testing.db.execute(select([or_(true, false).label("x"), and_(true, false).label("y")])).fetchone()
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
            (select([users.c.user_id]).where(users.c.user_name.startswith('apple')), [(1,)]),
            (select([users.c.user_id]).where(users.c.user_name.contains('i % t')), [(5,)]),
            (select([users.c.user_id]).where(users.c.user_name.endswith('anas')), [(3,)]),
        ):
            eq_(expr.execute().fetchall(), result)
    

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

        if testing.against('postgres'):
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

    def test_bindparam_shortname(self):
        """test the 'shortname' field on BindParamClause."""
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        u = bindparam('userid', shortname='someshortname')
        s = users.select(users.c.user_name==u)
        r = s.execute(someshortname='fred').fetchall()
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
            assert isinstance(s2.execute().fetchone()['somelabel'], datetime.datetime)
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

    def test_column_accessor(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        addresses.insert().execute(address_id=1, user_id=2, address='foo@bar.com')

        r = users.select(users.c.user_id==2).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        r = text("select * from query_users where user_id=2", bind=testing.db).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        # test slices
        r = text("select * from query_addresses", bind=testing.db).execute().fetchone()
        self.assert_(r[0:1] == (1,))
        self.assert_(r[1:] == (2, 'foo@bar.com'))
        self.assert_(r[:-1] == (1, 2))

        # test a little sqlite weirdness - with the UNION, cols come back as "query_users.user_id" in cursor.description
        r = text("select query_users.user_id, query_users.user_name from query_users "
            "UNION select query_users.user_id, query_users.user_name from query_users", bind=testing.db).execute().fetchone()
        self.assert_(r['user_id']) == 1
        self.assert_(r['user_name']) == "john"

        # test using literal tablename.colname
        r = text('select query_users.user_id AS "query_users.user_id", query_users.user_name AS "query_users.user_name" from query_users', bind=testing.db).execute().fetchone()
        self.assert_(r['query_users.user_id']) == 1
        self.assert_(r['query_users.user_name']) == "john"

        # unary experssions
        r = select([users.c.user_name.distinct()]).order_by(users.c.user_name).execute().fetchone()
        eq_(r[users.c.user_name], 'jack')
        eq_(r.user_name, 'jack')
        r.close()
        
        
    def test_row_as_args(self):
        users.insert().execute(user_id=1, user_name='john')
        r = users.select(users.c.user_id==1).execute().fetchone()
        users.delete().execute()
        users.insert().execute(r)
        assert users.select().execute().fetchall() == [(1, 'john')]
    
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
        r = users.outerjoin(addresses).select().execute().fetchone()
        try:
            print r['user_id']
            assert False
        except exc.InvalidRequestError, e:
            assert str(e) == "Ambiguous column name 'user_id' in result set! try 'use_labels' option on select statement." or \
                   str(e) == "Ambiguous column name 'USER_ID' in result set! try 'use_labels' option on select statement."

    @testing.requires.subqueries
    def test_column_label_targeting(self):
        users.insert().execute(user_id=7, user_name='ed')

        for s in (
            users.select().alias('foo'),
            users.select().alias(users.name),
        ):
            row = s.select(use_labels=True).execute().fetchone()
            assert row[s.c.user_id] == 7
            assert row[s.c.user_name] == 'ed'

    def test_keys(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        eq_([x.lower() for x in r.keys()], ['user_id', 'user_name'])

    def test_items(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        eq_([(x[0].lower(), x[1]) for x in r.items()], [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        eq_(len(r), 2)
        r.close()
        r = testing.db.execute('select user_name, user_id from query_users').fetchone()
        eq_(len(r), 2)
        r.close()
        r = testing.db.execute('select user_name from query_users').fetchone()
        eq_(len(r), 1)
        r.close()

    def test_cant_execute_join(self):
        try:
            users.join(addresses).execute()
        except exc.ArgumentError, e:
            assert str(e).startswith('Not an executable clause: ')



    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select(users.c.user_id==1).execute().fetchone()
        eq_(r[0], 1)
        eq_(r[1], 'foo')
        eq_([x.lower() for x in r.keys()], ['user_id', 'user_name'])
        eq_(r.values(), [1, 'foo'])

    def test_column_order_with_text_query(self):
        # should return values in query order
        users.insert().execute(user_id=1, user_name='foo')
        r = testing.db.execute('select user_name, user_id from query_users').fetchone()
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
                         Column('__parent', VARCHAR(20)),
                         Column('__row', VARCHAR(20)),
        )
        shadowed.create(checkfirst=True)
        try:
            shadowed.insert().execute(shadow_id=1, shadow_name='The Shadow', parent='The Light', row='Without light there is no shadow', __parent='Hidden parent', __row='Hidden row')
            r = shadowed.select(shadowed.c.shadow_id==1).execute().fetchone()
            self.assert_(r.shadow_id == r['shadow_id'] == r[shadowed.c.shadow_id] == 1)
            self.assert_(r.shadow_name == r['shadow_name'] == r[shadowed.c.shadow_name] == 'The Shadow')
            self.assert_(r.parent == r['parent'] == r[shadowed.c.parent] == 'The Light')
            self.assert_(r.row == r['row'] == r[shadowed.c.row] == 'Without light there is no shadow')
            self.assert_(r['__parent'] == 'Hidden parent')
            self.assert_(r['__row'] == 'Hidden row')
            try:
                print r.__parent, r.__row
                self.fail('Should not allow access to private attributes')
            except AttributeError:
                pass # expected
            r.close()
        finally:
            shadowed.drop(checkfirst=True)

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

        u = bindparam('search_key')

        s = users.select(u.in_([]))
        r = s.execute(search_key='john').fetchall()
        assert len(r) == 0
        r = s.execute(search_key=None).fetchall()
        assert len(r) == 0

        s = users.select(not_(u.in_([])))
        r = s.execute(search_key='john').fetchall()
        assert len(r) == 3
        r = s.execute(search_key=None).fetchall()
        assert len(r) == 0

    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.fails_on('oracle', 'FIXME: unknown')
    @testing.fails_on('mssql', 'FIXME: unknown')
    def test_in_filtering_advanced(self):
        """test the behavior of the in_() function when comparing against an empty collection."""

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

class PercentSchemaNamesTest(TestBase):
    """tests using percent signs, spaces in table and column names.
    
    Doesn't pass for mysql, postgres, but this is really a 
    SQLAlchemy bug - we should be escaping out %% signs for this
    operation the same way we do for text() and column labels.
    
    """
    @classmethod
    @testing.crashes('mysql', 'mysqldb calls name % (params)')
    @testing.crashes('postgres', 'postgres calls name % (params)')
    def setup_class(cls):
        global percent_table, metadata
        metadata = MetaData(testing.db)
        percent_table = Table('percent%table', metadata,
            Column("percent%", Integer),
            Column("%(oneofthese)s", Integer),
            Column("spaces % more spaces", Integer),
        )
        metadata.create_all()

    @classmethod
    @testing.crashes('mysql', 'mysqldb calls name % (params)')
    @testing.crashes('postgres', 'postgres calls name % (params)')
    def teardown_class(cls):
        metadata.drop_all()
    
    @testing.crashes('mysql', 'mysqldb calls name % (params)')
    @testing.crashes('postgres', 'postgres calls name % (params)')
    def test_roundtrip(self):
        percent_table.insert().execute(
            {'percent%':5, '%(oneofthese)s':7, 'spaces % more spaces':12},
        )
        percent_table.insert().execute(
            {'percent%':7, '%(oneofthese)s':8, 'spaces % more spaces':11},
            {'percent%':9, '%(oneofthese)s':9, 'spaces % more spaces':10},
            {'percent%':11, '%(oneofthese)s':10, 'spaces % more spaces':9},
        )
        
        for table in (percent_table, percent_table.alias()):
            eq_(
                table.select().order_by(table.c['%(oneofthese)s']).execute().fetchall(),
                [
                    (5, 7, 12),
                    (7, 8, 11),
                    (9, 9, 10),
                    (11, 10, 9)
                ]
            )

            eq_(
                table.select().
                    where(table.c['spaces % more spaces'].in_([9, 10])).
                    order_by(table.c['%(oneofthese)s']).execute().fetchall(),
                    [
                        (9, 9, 10),
                        (11, 10, 9)
                    ]
            )

            result = table.select().order_by(table.c['%(oneofthese)s']).execute()
            row = result.fetchone()
            eq_(row[table.c['percent%']], 5)
            eq_(row[table.c['%(oneofthese)s']], 7)
            eq_(row[table.c['spaces % more spaces']], 12)
            row = result.fetchone()
            eq_(row['percent%'], 7)
            eq_(row['%(oneofthese)s'], 8)
            eq_(row['spaces % more spaces'], 11)
            result.close()

        percent_table.update().values({percent_table.c['%(oneofthese)s']:9, percent_table.c['spaces % more spaces']:15}).execute()

        eq_(
            percent_table.select().order_by(percent_table.c['%(oneofthese)s']).execute().fetchall(),
            [
                (5, 9, 15),
                (7, 9, 15),
                (9, 9, 15),
                (11, 9, 15)
            ]
        )
        
        
        
class LimitTest(TestBase):

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

    @testing.fails_on('mssql', 'FIXME: unknown')
    def test_select_distinct_offset(self):
        """Test the interaction between distinct and offset"""

        r = sorted([x[0] for x in select([addresses.c.address]).distinct().offset(1).order_by(addresses.c.address).execute().fetchall()])
        self.assert_(len(r) == 4, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2] and r[2] != [3], repr(r))

    def test_select_distinct_limit_offset(self):
        """Test the interaction between limit and limit/offset"""

        r = select([addresses.c.address]).order_by(addresses.c.address).distinct().offset(2).limit(3).execute().fetchall()
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))

class CompoundTest(TestBase):
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
    @testing.fails_on('mysql', 'FIXME: unknown')
    @testing.fails_on('sqlite', 'FIXME: unknown')
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

    @testing.crashes('firebird', 'Does not support intersect')
    @testing.crashes('sybase', 'FIXME: unknown, verify not fails_on')
    @testing.fails_on('mysql', 'FIXME: unknown')
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

    @testing.crashes('firebird', 'Does not support except')
    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.crashes('sybase', 'FIXME: unknown, verify not fails_on')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_except_style1(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found = self._fetchall_sorted(e.alias('bar').select().execute())
        eq_(found, wanted)

    @testing.crashes('firebird', 'Does not support except')
    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.crashes('sybase', 'FIXME: unknown, verify not fails_on')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_except_style2(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ).alias('foo').select(), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found1 = self._fetchall_sorted(e.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('bar').select().execute())
        eq_(found2, wanted)

    @testing.crashes('firebird', 'Does not support except')
    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.crashes('sybase', 'FIXME: unknown, verify not fails_on')
    @testing.fails_on('mysql', 'FIXME: unknown')
    @testing.fails_on('sqlite', 'FIXME: unknown')
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

    @testing.crashes('firebird', 'Does not support intersect')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_composite(self):
        u = intersect(
            select([t2.c.col3, t2.c.col4]),
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias('foo').select()
        )
        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        found = self._fetchall_sorted(u.execute())

        eq_(found, wanted)

    @testing.crashes('firebird', 'Does not support intersect')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_composite_alias(self):
        ua = intersect(
            select([t2.c.col3, t2.c.col4]),
            union(
                select([t1.c.col3, t1.c.col4]),
                select([t2.c.col3, t2.c.col4]),
                select([t3.c.col3, t3.c.col4]),
            ).alias('foo').select()
        ).alias('bar')

        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]
        found = self._fetchall_sorted(ua.select().execute())
        eq_(found, wanted)


class JoinTest(TestBase):
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


class OperatorTest(TestBase):
    @classmethod
    def setup_class(cls):
        global metadata, flds
        metadata = MetaData(testing.db)
        flds = Table('flds', metadata,
            Column('idcol', Integer, Sequence('t1pkseq'), primary_key=True),
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

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_modulo(self):
        eq_(
            select([flds.c.intcol % 3],
                   order_by=flds.c.idcol).execute().fetchall(),
            [(2,),(1,)]
        )
