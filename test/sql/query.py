import testbase
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions, sql
from sqlalchemy.engine import default
from testlib import *


class QueryTest(PersistTest):

    def setUpAll(self):
        global users, addresses, metadata
        metadata = MetaData(testbase.db)
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table('query_addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)))
        metadata.create_all()

    def tearDown(self):
        addresses.delete().execute()
        users.delete().execute()

    def tearDownAll(self):
        metadata.drop_all()

    def testinsert(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        assert users.count().scalar() == 1

    def test_insert_heterogeneous_params(self):
        users.insert().execute(
            {'user_id':7, 'user_name':'jack'},
            {'user_id':8, 'user_name':'ed'},
            {'user_id':9}
        )
        assert users.select().execute().fetchall() == [(7, 'jack'), (8, 'ed'), (9, None)]

    def testupdate(self):
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
                    Column('bar', String(30), PassiveDefault('hi'))
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
                    Column('bar', String(30), PassiveDefault('hi'))
                ),
                {'foo':'hi', 'id':1},
                {'id':1, 'foo':'hi', 'bar':'hi'}
            ),
            (
                {'unsupported':[]},
                Table("t5", metadata,
                    Column('id', String(10), primary_key=True),
                    Column('bar', String(30), PassiveDefault('hi'))
                ),
                {'id':'id1'},
                {'id':'id1', 'bar':'hi'},
            ),
        ]:
            if testbase.db.name in supported['unsupported']:
                continue
            try:
                table.create()
                i = insert_values(table, values)
                assert i == assertvalues, repr(i) + " " + repr(assertvalues)
            finally:
                table.drop()

    def testrowiteration(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'ed')
        users.insert().execute(user_id = 9, user_name = 'fred')
        r = users.select().execute()
        l = []
        for row in r:
            l.append(row)
        self.assert_(len(l) == 3)

    def test_fetchmany(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'ed')
        users.insert().execute(user_id = 9, user_name = 'fred')
        r = users.select().execute()
        l = []
        for row in r.fetchmany(size=2):
            l.append(row)
        self.assert_(len(l) == 2, "fetchmany(size=2) got %s rows" % len(l))

    def test_compiled_execute(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        s = select([users], users.c.user_id==bindparam('id')).compile()
        c = testbase.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_compiled_insert_execute(self):
        users.insert().compile().execute(user_id = 7, user_name = 'jack')
        s = select([users], users.c.user_id==bindparam('id')).compile()
        c = testbase.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_repeated_bindparams(self):
        """Tests that a BindParam can be used more than once.

        This should be run for DB-APIs with both positional and named
        paramstyles.
        """
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')

        u = bindparam('userid')
        s = users.select(or_(users.c.user_name==u, users.c.user_name==u))
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
        dialect = default.DefaultDialect(default_paramstyle='qmark')
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

    def testdelete(self):
        users.insert().execute(user_id = 7, user_name = 'jack')
        users.insert().execute(user_id = 8, user_name = 'fred')
        print repr(users.select().execute().fetchall())

        users.delete(users.c.user_name == 'fred').execute()

        print repr(users.select().execute().fetchall())

    def testselectlimit(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        users.insert().execute(user_id=3, user_name='ed')
        users.insert().execute(user_id=4, user_name='wendy')
        users.insert().execute(user_id=5, user_name='laura')
        users.insert().execute(user_id=6, user_name='ralph')
        users.insert().execute(user_id=7, user_name='fido')
        r = users.select(limit=3, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')], repr(r))

    @testing.unsupported('mssql')
    @testing.fails_on('maxdb')
    def testselectlimitoffset(self):
        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        users.insert().execute(user_id=3, user_name='ed')
        users.insert().execute(user_id=4, user_name='wendy')
        users.insert().execute(user_id=5, user_name='laura')
        users.insert().execute(user_id=6, user_name='ralph')
        users.insert().execute(user_id=7, user_name='fido')
        r = users.select(limit=3, offset=2, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = users.select(offset=5, order_by=[users.c.user_id]).execute().fetchall()
        self.assert_(r==[(6, 'ralph'), (7, 'fido')])

    @testing.supported('mssql')
    @testing.fails_on('maxdb')
    def test_select_limit_nooffset(self):
        try:
            r = users.select(limit=3, offset=2, order_by=[users.c.user_id]).execute().fetchall()
            assert False # InvalidRequestError should have been raised
        except exceptions.InvalidRequestError:
            pass

    @testing.unsupported('mysql')
    def test_scalar_select(self):
        """test that scalar subqueries with labels get their type propigated to the result set."""
        # mysql and/or mysqldb has a bug here, type isnt propigated for scalar subquery.
        datetable = Table('datetable', metadata,
            Column('id', Integer, primary_key=True),
            Column('today', DateTime))
        datetable.create()
        try:
            datetable.insert().execute(id=1, today=datetime.datetime(2006, 5, 12, 12, 0, 0))
            s = select([datetable.alias('x').c.today], scalar=True)
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
            self.assertEquals(got, wanted)

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

        r = text("select * from query_users where user_id=2", bind=testbase.db).execute().fetchone()
        self.assert_(r.user_id == r['user_id'] == r[users.c.user_id] == 2)
        self.assert_(r.user_name == r['user_name'] == r[users.c.user_name] == 'jack')

        # test slices
        r = text("select * from query_addresses", bind=testbase.db).execute().fetchone()
        self.assert_(r[0:1] == (1,))
        self.assert_(r[1:] == (2, 'foo@bar.com'))
        self.assert_(r[:-1] == (1, 2))

    def test_ambiguous_column(self):
        users.insert().execute(user_id=1, user_name='john')
        r = users.outerjoin(addresses).select().execute().fetchone()
        try:
            print r['user_id']
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Ambiguous column name 'user_id' in result set! try 'use_labels' option on select statement." or \
                   str(e) == "Ambiguous column name 'USER_ID' in result set! try 'use_labels' option on select statement."

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
        self.assertEqual([x.lower() for x in r.keys()], ['user_id', 'user_name'])

    def test_items(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        self.assertEqual([(x[0].lower(), x[1]) for x in r.items()], [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = testbase.db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(len(r), 2)
        r.close()
        r = testbase.db.execute('select user_name from query_users', {}).fetchone()
        self.assertEqual(len(r), 1)
        r.close()

    def test_cant_execute_join(self):
        try:
            users.join(addresses).execute()
        except exceptions.ArgumentError, e:
            assert str(e).startswith('Not an executable clause: ')

    def test_functions(self):
        x = testbase.db.func.current_date().execute().scalar()
        y = testbase.db.func.current_date().select().execute().scalar()
        z = testbase.db.func.current_date().scalar()
        assert (x == y == z) is True

        x = testbase.db.func.current_date(type_=Date)
        assert isinstance(x.type, Date)
        assert isinstance(x.execute().scalar(), datetime.date)

    def test_conn_functions(self):
        conn = testbase.db.connect()
        try:
            x = conn.execute(func.current_date()).scalar()
            y = conn.execute(func.current_date().select()).scalar()
            z = conn.scalar(func.current_date())
        finally:
            conn.close()
        assert (x == y == z) is True

    def test_update_functions(self):
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

    @testing.supported('postgres')
    def test_functions_with_cols(self):
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


    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users.insert().execute(user_id=1, user_name='foo')
        r = users.select(users.c.user_id==1).execute().fetchone()
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 'foo')
        self.assertEqual([x.lower() for x in r.keys()], ['user_id', 'user_name'])
        self.assertEqual(r.values(), [1, 'foo'])

    def test_column_order_with_text_query(self):
        # should return values in query order
        users.insert().execute(user_id=1, user_name='foo')
        r = testbase.db.execute('select user_name, user_id from query_users', {}).fetchone()
        self.assertEqual(r[0], 'foo')
        self.assertEqual(r[1], 1)
        self.assertEqual([x.lower() for x in r.keys()], ['user_name', 'user_id'])
        self.assertEqual(r.values(), ['foo', 1])

    @testing.unsupported('oracle', 'firebird', 'maxdb')
    def test_column_accessor_shadow(self):
        meta = MetaData(testbase.db)
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

    @testing.supported('mssql')
    def test_fetchid_trigger(self):
        meta = MetaData(testbase.db)
        t1 = Table('t1', meta,
                Column('id', Integer, Sequence('fred', 100, 1), primary_key=True),
                Column('descr', String(200)))
        t2 = Table('t2', meta,
                Column('id', Integer, Sequence('fred', 200, 1), primary_key=True),
                Column('descr', String(200)))
        meta.create_all()
        con = testbase.db.connect()
        con.execute("""create trigger paj on t1 for insert as
            insert into t2 (descr) select descr from inserted""")

        try:
            tr = con.begin()
            r = con.execute(t2.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [200])
            r = con.execute(t1.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [100])

        finally:
            tr.commit()
            con.execute("""drop trigger paj""")
            meta.drop_all()

    @testing.supported('mssql')
    def test_insertid_schema(self):
        meta = MetaData(testbase.db)
        con = testbase.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    @testing.supported('mssql')
    def test_insertid_reserved(self):
        meta = MetaData(testbase.db)
        table = Table(
            'select', meta,
            Column('col', Integer, primary_key=True)
        )
        table.create()

        meta2 = MetaData(testbase.db)
        try:
            table.insert().execute(col=7)
        finally:
            table.drop()

    @testing.fails_on('maxdb')
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

        s = users.select(users.c.user_name.in_([]) == True)
        r = s.execute().fetchall()
        assert len(r) == 0
        s = users.select(users.c.user_name.in_([]) == False)
        r = s.execute().fetchall()
        assert len(r) == 2
        s = users.select(users.c.user_name.in_([]) == None)
        r = s.execute().fetchall()
        assert len(r) == 1


class CompoundTest(PersistTest):
    """test compound statements like UNION, INTERSECT, particularly their ability to nest on
    different databases."""
    def setUpAll(self):
        global metadata, t1, t2, t3
        metadata = MetaData(testbase.db)
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

    def tearDownAll(self):
        metadata.drop_all()

    def _fetchall_sorted(self, executed):
        return sorted([tuple(row) for row in executed.fetchall()])

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
        self.assertEquals(found1, wanted)

        found2 = self._fetchall_sorted(u.alias('bar').select().execute())
        self.assertEquals(found2, wanted)

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
        self.assertEquals(u.execute().fetchall(), wanted)

    @testing.fails_on('maxdb')
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
        self.assertEquals(u.alias('bar').select().execute().fetchall(), wanted)

    @testing.unsupported('sqlite', 'mysql', 'oracle')
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
        self.assertEquals(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('foo').select().execute())
        self.assertEquals(found2, wanted)

    @testing.unsupported('mysql', 'sybase')
    def test_intersect(self):
        i = intersect(
            select([t2.c.col3, t2.c.col4]),
            select([t2.c.col3, t2.c.col4], t2.c.col4==t3.c.col3)
        )

        wanted = [('aaa', 'bbb'), ('bbb', 'ccc'), ('ccc', 'aaa')]

        found1 = self._fetchall_sorted(i.execute())
        self.assertEquals(found1, wanted)

        found2 = self._fetchall_sorted(i.alias('bar').select().execute())
        self.assertEquals(found2, wanted)

    @testing.unsupported('mysql', 'oracle', 'sybase')
    def test_except_style1(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found = self._fetchall_sorted(e.alias('bar').select().execute())
        self.assertEquals(found, wanted)

    @testing.unsupported('mysql', 'oracle', 'sybase')
    def test_except_style2(self):
        e = except_(union(
            select([t1.c.col3, t1.c.col4]),
            select([t2.c.col3, t2.c.col4]),
            select([t3.c.col3, t3.c.col4]),
        ).alias('foo').select(), select([t2.c.col3, t2.c.col4]))

        wanted = [('aaa', 'aaa'), ('aaa', 'ccc'), ('bbb', 'aaa'),
                  ('bbb', 'bbb'), ('ccc', 'bbb'), ('ccc', 'ccc')]

        found1 = self._fetchall_sorted(e.execute())
        self.assertEquals(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('bar').select().execute())
        self.assertEquals(found2, wanted)

    @testing.unsupported('sqlite', 'mysql', 'oracle', 'sybase')
    def test_except_style3(self):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        e = except_(
            select([t1.c.col3]), # aaa, bbb, ccc
            except_(
                select([t2.c.col3]), # aaa, bbb, ccc
                select([t3.c.col3], t3.c.col3 == 'ccc'), #ccc
            )
        )
        self.assertEquals(e.execute().fetchall(), [('ccc',)])
        self.assertEquals(e.alias('foo').select().execute().fetchall(),
                          [('ccc',)])

    @testing.unsupported('mysql')
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

        self.assertEquals(found, wanted)

    @testing.unsupported('mysql')
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
        self.assertEquals(found, wanted)


class JoinTest(PersistTest):
    """Tests join execution.

    The compiled SQL emitted by the dialect might be ANSI joins or
    theta joins ('old oracle style', with (+) for OUTER).  This test
    tries to exercise join syntax and uncover any inconsistencies in
    `JOIN rhs ON lhs.col=rhs.col` vs `rhs.col=lhs.col`.  At least one
    database seems to be sensitive to this.
    """

    def setUpAll(self):
        global metadata
        global t1, t2, t3

        metadata = MetaData(testbase.db)
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

    def tearDownAll(self):
        metadata.drop_all()

    def assertRows(self, statement, expected):
        """Execute a statement and assert that rows returned equal expected."""

        found = exec_sorted(statement)
        self.assertEquals(found, sorted(expected))

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


class OperatorTest(PersistTest):
    def setUpAll(self):
        global metadata, flds
        metadata = MetaData(testbase.db)
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

    def tearDownAll(self):
        metadata.drop_all()

    @testing.fails_on('maxdb')
    def test_modulo(self):
        self.assertEquals(
            select([flds.c.intcol % 3],
                   order_by=flds.c.idcol).execute().fetchall(),
            [(2,),(1,)]
        )


def exec_sorted(statement, *args, **kw):
    """Executes a statement and returns a sorted list plain tuple rows."""

    return sorted([tuple(row)
                   for row in statement.execute(*args, **kw).fetchall()])


if __name__ == "__main__":
    testbase.main()
