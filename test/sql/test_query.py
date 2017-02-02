from sqlalchemy.testing import eq_, assert_raises_message, assert_raises, \
    is_, in_, not_in_
from sqlalchemy import testing
from sqlalchemy.testing import fixtures, engines
from sqlalchemy import (
    exc, sql, func, select, String, Integer, MetaData, and_, ForeignKey,
    union, intersect, except_, union_all, VARCHAR, INT, text,
    bindparam, literal, not_, literal_column, desc, asc,
    TypeDecorator, or_, cast)
from sqlalchemy.engine import default
from sqlalchemy.testing.schema import Table, Column

# ongoing - these are old tests.  those which are of general use
# to test a dialect are being slowly migrated to
# sqlalhcemy.testing.suite

users = users2 = addresses = metadata = None


class QueryTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, users2, addresses, metadata
        metadata = MetaData(testing.db)
        users = Table(
            'query_users', metadata,
            Column(
                'user_id', INT, primary_key=True,
                test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )
        addresses = Table(
            'query_addresses', metadata,
            Column(
                'address_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)),
            test_needs_acid=True
        )

        users2 = Table(
            'u2', metadata,
            Column('user_id', INT, primary_key=True),
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

    @testing.fails_on(
        'firebird', "kinterbasdb doesn't send full type information")
    def test_order_by_label(self):
        """test that a label within an ORDER BY works on each backend.

        This test should be modified to support [ticket:1068] when that ticket
        is implemented.  For now, you need to put the actual string in the
        ORDER BY.

        """

        users.insert().execute(
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9, 'user_name': 'fred'},
        )

        concat = ("test: " + users.c.user_name).label('thedata')
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

    @testing.requires.order_by_label_with_expression
    def test_order_by_label_compound(self):
        users.insert().execute(
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9, 'user_name': 'fred'},
        )

        concat = ("test: " + users.c.user_name).label('thedata')
        eq_(
            select([concat]).order_by(literal_column('thedata') + "x").
            execute().fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)]
        )

    @testing.requires.boolean_col_expressions
    def test_or_and_as_columns(self):
        true, false = literal(True), literal(False)

        eq_(testing.db.execute(select([and_(true, false)])).scalar(), False)
        eq_(testing.db.execute(select([and_(true, true)])).scalar(), True)
        eq_(testing.db.execute(select([or_(true, false)])).scalar(), True)
        eq_(testing.db.execute(select([or_(false, false)])).scalar(), False)
        eq_(
            testing.db.execute(select([not_(or_(false, false))])).scalar(),
            True)

        row = testing.db.execute(
            select(
                [or_(false, false).label("x"),
                    and_(true, false).label("y")])).first()
        assert row.x == False  # noqa
        assert row.y == False  # noqa

        row = testing.db.execute(
            select(
                [or_(true, false).label("x"),
                    and_(true, false).label("y")])).first()
        assert row.x == True  # noqa
        assert row.y == False  # noqa

    def test_like_ops(self):
        users.insert().execute(
            {'user_id': 1, 'user_name': 'apples'},
            {'user_id': 2, 'user_name': 'oranges'},
            {'user_id': 3, 'user_name': 'bananas'},
            {'user_id': 4, 'user_name': 'legumes'},
            {'user_id': 5, 'user_name': 'hi % there'},
        )

        for expr, result in (
            (select([users.c.user_id]).
                where(users.c.user_name.startswith('apple')), [(1,)]),
            (select([users.c.user_id]).
                where(users.c.user_name.contains('i % t')), [(5,)]),
            (select([users.c.user_id]).
                where(users.c.user_name.endswith('anas')), [(3,)]),
            (select([users.c.user_id]).
                where(users.c.user_name.contains('i % t', escape='&')),
                [(5,)]),
        ):
            eq_(expr.execute().fetchall(), result)

    @testing.requires.mod_operator_as_percent_sign
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
            {'user_id': 1, 'user_name': 'one'},
            {'user_id': 2, 'user_name': 'TwO'},
            {'user_id': 3, 'user_name': 'ONE'},
            {'user_id': 4, 'user_name': 'OnE'},
        )

        eq_(
            select([users.c.user_id]).where(users.c.user_name.ilike('one')).
            execute().fetchall(), [(1, ), (3, ), (4, )])

        eq_(
            select([users.c.user_id]).where(users.c.user_name.ilike('TWO')).
            execute().fetchall(), [(2, )])

        if testing.against('postgresql'):
            eq_(
                select([users.c.user_id]).
                where(users.c.user_name.like('one')).execute().fetchall(),
                [(1, )])
            eq_(
                select([users.c.user_id]).
                where(users.c.user_name.like('TWO')).execute().fetchall(), [])

    def test_compiled_execute(self):
        users.insert().execute(user_id=7, user_name='jack')
        s = select([users], users.c.user_id == bindparam('id')).compile()
        c = testing.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_compiled_insert_execute(self):
        users.insert().compile().execute(user_id=7, user_name='jack')
        s = select([users], users.c.user_id == bindparam('id')).compile()
        c = testing.db.connect()
        assert c.execute(s, id=7).fetchall()[0]['user_id'] == 7

    def test_repeated_bindparams(self):
        """Tests that a BindParam can be used more than once.

        This should be run for DB-APIs with both positional and named
        paramstyles.
        """

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='fred')

        u = bindparam('userid')
        s = users.select(and_(users.c.user_name == u, users.c.user_name == u))
        r = s.execute(userid='fred').fetchall()
        assert len(r) == 1

    def test_bindparam_detection(self):
        dialect = default.DefaultDialect(paramstyle='qmark')

        def prep(q): return str(sql.text(q).compile(dialect=dialect))

        def a_eq(got, wanted):
            if got != wanted:
                print("Wanted %s" % wanted)
                print("Received %s" % got)
            self.assert_(got == wanted, got)

        a_eq(prep('select foo'), 'select foo')
        a_eq(prep("time='12:30:00'"), "time='12:30:00'")
        a_eq(prep("time='12:30:00'"), "time='12:30:00'")
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
        a_eq(prep(r":this \:that"), "? :that")
        a_eq(prep(r"(\:that$other)"), "(:that$other)")
        a_eq(prep(r".\:that$ :other."), ".:that$ ?.")

    @testing.requires.standalone_binds
    def test_select_from_bindparam(self):
        """Test result row processing when selecting from a plain bind
        param."""

        class MyInteger(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                return int(value[4:])

            def process_result_value(self, value, dialect):
                return "INT_%d" % value

        eq_(
            testing.db.scalar(select([cast("INT_5", type_=MyInteger)])),
            "INT_5"
        )
        eq_(
            testing.db.scalar(
                select([cast("INT_5", type_=MyInteger).label('foo')])),
            "INT_5"
        )

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

            a_eq(
                users.select(
                    order_by=[users.c.user_name.desc().nullslast()],
                    use_labels=labels),
                [(2, 'b'), (3, 'a'), (1, None)])

            a_eq(
                users.select(
                    order_by=[desc(users.c.user_name).nullsfirst()],
                    use_labels=labels),
                [(1, None), (2, 'b'), (3, 'a')])

            a_eq(users.select(order_by=[desc(users.c.user_name).nullslast()],
                              use_labels=labels),
                 [(2, 'b'), (3, 'a'), (1, None)])

            a_eq(
                users.select(
                    order_by=[users.c.user_name.nullsfirst(), users.c.user_id],
                    use_labels=labels),
                [(1, None), (3, 'a'), (2, 'b')])

            a_eq(
                users.select(
                    order_by=[users.c.user_name.nullslast(), users.c.user_id],
                    use_labels=labels),
                [(3, 'a'), (2, 'b'), (1, None)])

    @testing.emits_warning('.*empty sequence.*')
    def test_in_filtering(self):
        """test the behavior of the in_() function."""

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='fred')
        users.insert().execute(user_id=9, user_name=None)

        s = users.select(users.c.user_name.in_([]))
        r = s.execute().fetchall()
        # No username is in empty set
        assert len(r) == 0

        s = users.select(not_(users.c.user_name.in_([])))
        r = s.execute().fetchall()
        # All usernames with a value are outside an empty set
        assert len(r) == 2

        s = users.select(users.c.user_name.in_(['jack', 'fred']))
        r = s.execute().fetchall()
        assert len(r) == 2

        s = users.select(not_(users.c.user_name.in_(['jack', 'fred'])))
        r = s.execute().fetchall()
        # Null values are not outside any set
        assert len(r) == 0

    @testing.emits_warning('.*empty sequence.*')
    @testing.fails_on('firebird', "uses sql-92 rules")
    @testing.fails_on('sybase', "uses sql-92 rules")
    @testing.fails_if(
        lambda: testing.against('mssql+pyodbc') and not
        testing.db.dialect.freetds, "uses sql-92 rules")
    def test_bind_in(self):
        """test calling IN against a bind parameter.

        this isn't allowed on several platforms since we
        generate ? = ?.

        """

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='fred')
        users.insert().execute(user_id=9, user_name=None)

        u = bindparam('search_key')

        s = users.select(not_(u.in_([])))
        r = s.execute(search_key='john').fetchall()
        assert len(r) == 3
        r = s.execute(search_key=None).fetchall()
        assert len(r) == 0

    @testing.emits_warning('.*empty sequence.*')
    def test_literal_in(self):
        """similar to test_bind_in but use a bind with a value."""

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='fred')
        users.insert().execute(user_id=9, user_name=None)

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

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='fred')
        users.insert().execute(user_id=9, user_name=None)

        s = users.select(users.c.user_name.in_([]) == True)  # noqa
        r = s.execute().fetchall()
        assert len(r) == 0
        s = users.select(users.c.user_name.in_([]) == False)  # noqa
        r = s.execute().fetchall()
        assert len(r) == 2
        s = users.select(users.c.user_name.in_([]) == None)  # noqa
        r = s.execute().fetchall()
        assert len(r) == 1


class RequiredBindTest(fixtures.TablesTest):
    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            Column('x', Integer)
        )

    def _assert_raises(self, stmt, params):
        assert_raises_message(
            exc.StatementError,
            "A value is required for bind parameter 'x'",
            testing.db.execute, stmt, **params)

        assert_raises_message(
            exc.StatementError,
            "A value is required for bind parameter 'x'",
            testing.db.execute, stmt, params)

    def test_insert(self):
        stmt = self.tables.foo.insert().values(
            x=bindparam('x'), data=bindparam('data'))
        self._assert_raises(stmt, {'data': 'data'})

    def test_select_where(self):
        stmt = select([self.tables.foo]). \
            where(self.tables.foo.c.data == bindparam('data')). \
            where(self.tables.foo.c.x == bindparam('x'))
        self._assert_raises(stmt, {'data': 'data'})

    @testing.requires.standalone_binds
    def test_select_columns(self):
        stmt = select([bindparam('data'), bindparam('x')])
        self._assert_raises(
            stmt, {'data': 'data'}
        )

    def test_text(self):
        stmt = text("select * from foo where x=:x and data=:data1")
        self._assert_raises(
            stmt, {'data1': 'data'}
        )

    def test_required_flag(self):
        is_(bindparam('foo').required, True)
        is_(bindparam('foo', required=False).required, False)
        is_(bindparam('foo', 'bar').required, False)
        is_(bindparam('foo', 'bar', required=True).required, True)

        def c(): return None
        is_(bindparam('foo', callable_=c, required=True).required, True)
        is_(bindparam('foo', callable_=c).required, False)
        is_(bindparam('foo', callable_=c, required=False).required, False)


class LimitTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, addresses, metadata
        metadata = MetaData(testing.db)
        users = Table(
            'query_users', metadata,
            Column('user_id', INT, primary_key=True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table(
            'query_addresses', metadata,
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
        r = users.select(limit=3, order_by=[users.c.user_id]).execute(). \
            fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')], repr(r))

    @testing.requires.offset
    def test_select_limit_offset(self):
        """Test the interaction between limit and offset"""

        r = users.select(limit=3, offset=2, order_by=[users.c.user_id]). \
            execute().fetchall()
        self.assert_(r == [(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = users.select(offset=5, order_by=[users.c.user_id]).execute(). \
            fetchall()
        self.assert_(r == [(6, 'ralph'), (7, 'fido')])

    def test_select_distinct_limit(self):
        """Test the interaction between limit and distinct"""

        r = sorted(
            [x[0] for x in select([addresses.c.address]).distinct().
                limit(3).order_by(addresses.c.address).execute().fetchall()])
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))

    @testing.requires.offset
    @testing.fails_on('mssql', 'FIXME: unknown')
    def test_select_distinct_offset(self):
        """Test the interaction between distinct and offset"""

        r = sorted(
            [x[0] for x in select([addresses.c.address]).distinct().
                offset(1).order_by(addresses.c.address).
                execute().fetchall()])
        eq_(len(r), 4)
        self.assert_(r[0] != r[1] and r[1] != r[2] and r[2] != [3], repr(r))

    @testing.requires.offset
    def test_select_distinct_limit_offset(self):
        """Test the interaction between limit and limit/offset"""

        r = select([addresses.c.address]).order_by(addresses.c.address). \
            distinct().offset(2).limit(3).execute().fetchall()
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))


class CompoundTest(fixtures.TestBase):

    """test compound statements like UNION, INTERSECT, particularly their
    ability to nest on different databases."""

    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, t1, t2, t3
        metadata = MetaData(testing.db)
        t1 = Table(
            't1', metadata,
            Column(
                'col1', Integer, test_needs_autoincrement=True,
                primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        t2 = Table(
            't2', metadata,
            Column(
                'col1', Integer, test_needs_autoincrement=True,
                primary_key=True),
            Column('col2', String(30)),
            Column('col3', String(40)),
            Column('col4', String(30)))
        t3 = Table(
            't3', metadata,
            Column(
                'col1', Integer, test_needs_autoincrement=True,
                primary_key=True),
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
    @testing.fails_on(
        'firebird',
        "has trouble extracting anonymous column from union subquery")
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

        wanted = [('aaa',), ('aaa',), ('bbb',), ('bbb',), ('ccc',), ('ccc',)]
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

        wanted = [('aaa',), ('aaa',), ('bbb',), ('bbb',), ('ccc',), ('ccc',)]
        found1 = self._fetchall_sorted(e.execute())
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(e.alias('foo').select().execute())
        eq_(found2, wanted)

    @testing.requires.intersect
    def test_intersect(self):
        i = intersect(
            select([t2.c.col3, t2.c.col4]),
            select([t2.c.col3, t2.c.col4], t2.c.col4 == t3.c.col3)
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
            select([t1.c.col3]),  # aaa, bbb, ccc
            except_(
                select([t2.c.col3]),  # aaa, bbb, ccc
                select([t3.c.col3], t3.c.col3 == 'ccc'),  # ccc
            )
        )
        eq_(e.execute().fetchall(), [('ccc',)])
        eq_(e.alias('foo').select().execute().fetchall(), [('ccc',)])

    @testing.requires.except_
    def test_except_style4(self):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        e = except_(
            select([t1.c.col3]),  # aaa, bbb, ccc
            except_(
                select([t2.c.col3]),  # aaa, bbb, ccc
                select([t3.c.col3], t3.c.col3 == 'ccc'),  # ccc
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


t1 = t2 = t3 = None


class JoinTest(fixtures.TestBase):

    """Tests join execution.

    The compiled SQL emitted by the dialect might be ANSI joins or
    theta joins ('old oracle style', with (+) for OUTER).  This test
    tries to exercise join syntax and uncover any inconsistencies in
    `JOIN rhs ON lhs.col=rhs.col` vs `rhs.col=lhs.col`.  At least one
    database seems to be sensitive to this.
    """
    __backend__ = True

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

        for criteria in (t1.c.t1_id == t2.c.t1_id, t2.c.t1_id == t1.c.t1_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2, criteria)])
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_join_x2(self):
        """Joins t1->t2->t3."""

        for criteria in (t1.c.t1_id == t2.c.t1_id, t2.c.t1_id == t1.c.t1_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2, criteria)])
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_outerjoin_x1(self):
        """Outer joins t1->t2."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id],
                from_obj=[t1.join(t2).join(t3, criteria)])
            self.assertRows(expr, [(10, 20)])

    def test_outerjoin_x2(self):
        """Outer joins t1->t2,t3."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                from_obj=[t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                          outerjoin(t3, criteria)])
            self.assertRows(
                expr, [(10, 20, 30), (11, 21, None), (12, None, None)])

    def test_outerjoin_where_x2_t1(self):
        """Outer joins t1->t2,t3, where on t1."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t1.c.name == 't1 #10',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t1.c.t1_id < 12,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t2(self):
        """Outer joins t1->t2,t3, where on t2."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t2.c.name == 't2 #20',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t2.c.t2_id < 29,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t3(self):
        """Outer joins t1->t2,t3, where on t3."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t3.c.name == 't3 #30',
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                t3.c.t3_id < 39,
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t3(self):
        """Outer joins t1->t2,t3, where on t1 and t3."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t3.c.name == 't3 #30'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 19, t3.c.t3_id < 39),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t2(self):
        """Outer joins t1->t2,t3, where on t1 and t2."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10', t2.c.name == 't2 #20'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 12, t2.c.t2_id < 39),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t1t2t3(self):
        """Outer joins t1->t2,t3, where on t1, t2 and t3."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.name == 't1 #10',
                     t2.c.name == 't2 #20',
                     t3.c.name == 't3 #30'),
                from_obj=[(t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                           outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                and_(t1.c.t1_id < 19, t2.c.t2_id < 29, t3.c.t3_id < 39),
                from_obj=[
                    (t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).
                        outerjoin(t3, criteria))])
            self.assertRows(expr, [(10, 20, 30)])

    def test_mixed(self):
        """Joins t1->t2, outer t2->t3."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(
                [t1.c.t1_id, t2.c.t2_id, t3.c.t3_id],
                from_obj=[(t1.join(t2).outerjoin(t3, criteria))])
            print(expr)
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_mixed_where(self):
        """Joins t1->t2, outer t2->t3, plus a where on each table in turn."""

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
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


metadata = flds = None


class OperatorTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, flds
        metadata = MetaData(testing.db)
        flds = Table(
            'flds', metadata,
            Column(
                'idcol', Integer, primary_key=True,
                test_needs_autoincrement=True),
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
            [(2,), (1,)]
        )

    @testing.requires.window_functions
    def test_over(self):
        eq_(
            select([
                flds.c.intcol, func.row_number().over(order_by=flds.c.strcol)
            ]).execute().fetchall(),
            [(13, 1), (5, 2)]
        )
