from sqlalchemy.testing import eq_, assert_raises_message, assert_raises, \
    in_, not_in_, is_, ne_
from sqlalchemy import testing
from sqlalchemy.testing import fixtures, engines
from sqlalchemy import util
from sqlalchemy import (
    exc, sql, func, select, String, Integer, MetaData, ForeignKey,
    VARCHAR, INT, CHAR, text, type_coerce, literal_column,
    TypeDecorator, table, column)
from sqlalchemy.engine import result as _result
from sqlalchemy.testing.schema import Table, Column
import operator


class ResultProxyTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'users', metadata,
            Column(
                'user_id', INT, primary_key=True,
                test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )
        Table(
            'addresses', metadata,
            Column(
                'address_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('user_id', Integer, ForeignKey('users.user_id')),
            Column('address', String(30)),
            test_needs_acid=True
        )

        Table(
            'users2', metadata,
            Column('user_id', INT, primary_key=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True
        )

    def test_row_iteration(self):
        users = self.tables.users

        users.insert().execute(
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9, 'user_name': 'fred'},
        )
        r = users.select().execute()
        l = []
        for row in r:
            l.append(row)
        eq_(len(l), 3)

    @testing.requires.subqueries
    def test_anonymous_rows(self):
        users = self.tables.users

        users.insert().execute(
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9, 'user_name': 'fred'},
        )

        sel = select([users.c.user_id]).where(users.c.user_name == 'jack'). \
            as_scalar()
        for row in select([sel + 1, sel + 3], bind=users.bind).execute():
            eq_(row['anon_1'], 8)
            eq_(row['anon_2'], 10)

    def test_row_comparison(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name='jack')
        rp = users.select().execute().first()

        eq_(rp, rp)
        is_(not(rp != rp), True)

        equal = (7, 'jack')

        eq_(rp, equal)
        eq_(equal, rp)
        is_((not (rp != equal)), True)
        is_(not (equal != equal), True)

        def endless():
            while True:
                yield 1
        ne_(rp, endless())
        ne_(endless(), rp)

        # test that everything compares the same
        # as it would against a tuple
        for compare in [False, 8, endless(), 'xyz', (7, 'jack')]:
            for op in [
                operator.eq, operator.ne, operator.gt,
                operator.lt, operator.ge, operator.le
            ]:

                try:
                    control = op(equal, compare)
                except TypeError:
                    # Py3K raises TypeError for some invalid comparisons
                    assert_raises(TypeError, op, rp, compare)
                else:
                    eq_(control, op(rp, compare))

                try:
                    control = op(compare, equal)
                except TypeError:
                    # Py3K raises TypeError for some invalid comparisons
                    assert_raises(TypeError, op, compare, rp)
                else:
                    eq_(control, op(compare, rp))

    @testing.provide_metadata
    def test_column_label_overlap_fallback(self):
        content = Table(
            'content', self.metadata,
            Column('type', String(30)),
        )
        bar = Table(
            'bar', self.metadata,
            Column('content_type', String(30))
        )
        self.metadata.create_all(testing.db)
        testing.db.execute(content.insert().values(type="t1"))

        row = testing.db.execute(content.select(use_labels=True)).first()
        in_(content.c.type, row)
        not_in_(bar.c.content_type, row)
        in_(sql.column('content_type'), row)

        row = testing.db.execute(
            select([content.c.type.label("content_type")])).first()
        in_(content.c.type, row)

        not_in_(bar.c.content_type, row)

        in_(sql.column('content_type'), row)

        row = testing.db.execute(select([func.now().label("content_type")])). \
            first()
        not_in_(content.c.type, row)

        not_in_(bar.c.content_type, row)

        in_(sql.column('content_type'), row)

    def test_pickled_rows(self):
        users = self.tables.users
        addresses = self.tables.addresses

        users.insert().execute(
            {'user_id': 7, 'user_name': 'jack'},
            {'user_id': 8, 'user_name': 'ed'},
            {'user_id': 9, 'user_name': 'fred'},
        )

        for pickle in False, True:
            for use_labels in False, True:
                result = users.select(use_labels=use_labels).order_by(
                    users.c.user_id).execute().fetchall()

                if pickle:
                    result = util.pickle.loads(util.pickle.dumps(result))

                eq_(
                    result,
                    [(7, "jack"), (8, "ed"), (9, "fred")]
                )
                if use_labels:
                    eq_(result[0]['users_user_id'], 7)
                    eq_(
                        list(result[0].keys()),
                        ["users_user_id", "users_user_name"])
                else:
                    eq_(result[0]['user_id'], 7)
                    eq_(list(result[0].keys()), ["user_id", "user_name"])

                eq_(result[0][0], 7)
                eq_(result[0][users.c.user_id], 7)
                eq_(result[0][users.c.user_name], 'jack')

                if not pickle or use_labels:
                    assert_raises(
                        exc.NoSuchColumnError,
                        lambda: result[0][addresses.c.user_id])
                else:
                    # test with a different table.  name resolution is
                    # causing 'user_id' to match when use_labels wasn't used.
                    eq_(result[0][addresses.c.user_id], 7)

                assert_raises(
                    exc.NoSuchColumnError, lambda: result[0]['fake key'])
                assert_raises(
                    exc.NoSuchColumnError,
                    lambda: result[0][addresses.c.address_id])

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

    def test_fetchmany(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name='jack')
        users.insert().execute(user_id=8, user_name='ed')
        users.insert().execute(user_id=9, user_name='fred')
        r = users.select().execute()
        l = []
        for row in r.fetchmany(size=2):
            l.append(row)
        eq_(len(l), 2)

    def test_column_slices(self):
        users = self.tables.users
        addresses = self.tables.addresses

        users.insert().execute(user_id=1, user_name='john')
        users.insert().execute(user_id=2, user_name='jack')
        addresses.insert().execute(
            address_id=1, user_id=2, address='foo@bar.com')

        r = text(
            "select * from addresses", bind=testing.db).execute().first()
        eq_(r[0:1], (1,))
        eq_(r[1:], (2, 'foo@bar.com'))
        eq_(r[:-1], (1, 2))

    def test_column_accessor_basic_compiled(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
            dict(user_id=2, user_name='jack')
        )

        r = users.select(users.c.user_id == 2).execute().first()
        eq_(r.user_id, 2)
        eq_(r['user_id'], 2)
        eq_(r[users.c.user_id], 2)

        eq_(r.user_name, 'jack')
        eq_(r['user_name'], 'jack')
        eq_(r[users.c.user_name], 'jack')

    def test_column_accessor_basic_text(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
            dict(user_id=2, user_name='jack')
        )
        r = testing.db.execute(
            text("select * from users where user_id=2")).first()

        eq_(r.user_id, 2)
        eq_(r['user_id'], 2)
        eq_(r[users.c.user_id], 2)

        eq_(r.user_name, 'jack')
        eq_(r['user_name'], 'jack')
        eq_(r[users.c.user_name], 'jack')

    def test_column_accessor_textual_select(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
            dict(user_id=2, user_name='jack')
        )
        # this will create column() objects inside
        # the select(), these need to match on name anyway
        r = testing.db.execute(
            select([
                column('user_id'), column('user_name')
            ]).select_from(table('users')).
            where(text('user_id=2'))
        ).first()

        eq_(r.user_id, 2)
        eq_(r['user_id'], 2)
        eq_(r[users.c.user_id], 2)

        eq_(r.user_name, 'jack')
        eq_(r['user_name'], 'jack')
        eq_(r[users.c.user_name], 'jack')

    def test_column_accessor_dotted_union(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
        )

        # test a little sqlite weirdness - with the UNION,
        # cols come back as "users.user_id" in cursor.description
        r = testing.db.execute(
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users"
            )
        ).first()
        eq_(r['user_id'], 1)
        eq_(r['user_name'], "john")
        eq_(list(r.keys()), ["user_id", "user_name"])

    @testing.only_on("sqlite", "sqlite specific feature")
    def test_column_accessor_sqlite_raw(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
        )

        r = text(
            "select users.user_id, users.user_name "
            "from users "
            "UNION select users.user_id, "
            "users.user_name from users",
            bind=testing.db).execution_options(sqlite_raw_colnames=True). \
            execute().first()
        not_in_('user_id', r)
        not_in_('user_name', r)
        eq_(r['users.user_id'], 1)
        eq_(r['users.user_name'], "john")
        eq_(list(r.keys()), ["users.user_id", "users.user_name"])

    @testing.only_on("sqlite", "sqlite specific feature")
    def test_column_accessor_sqlite_translated(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
        )

        r = text(
            "select users.user_id, users.user_name "
            "from users "
            "UNION select users.user_id, "
            "users.user_name from users",
            bind=testing.db).execute().first()
        eq_(r['user_id'], 1)
        eq_(r['user_name'], "john")
        eq_(r['users.user_id'], 1)
        eq_(r['users.user_name'], "john")
        eq_(list(r.keys()), ["user_id", "user_name"])

    def test_column_accessor_labels_w_dots(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
        )
        # test using literal tablename.colname
        r = text(
            'select users.user_id AS "users.user_id", '
            'users.user_name AS "users.user_name" '
            'from users', bind=testing.db).\
            execution_options(sqlite_raw_colnames=True).execute().first()
        eq_(r['users.user_id'], 1)
        eq_(r['users.user_name'], "john")
        not_in_("user_name", r)
        eq_(list(r.keys()), ["users.user_id", "users.user_name"])

    def test_column_accessor_unary(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='john'),
        )

        # unary experssions
        r = select([users.c.user_name.distinct()]).order_by(
            users.c.user_name).execute().first()
        eq_(r[users.c.user_name], 'john')
        eq_(r.user_name, 'john')

    def test_column_accessor_err(self):
        r = testing.db.execute(select([1])).first()
        assert_raises_message(
            AttributeError,
            "Could not locate column in row for column 'foo'",
            getattr, r, "foo"
        )
        assert_raises_message(
            KeyError,
            "Could not locate column in row for column 'foo'",
            lambda: r['foo']
        )

    def test_graceful_fetch_on_non_rows(self):
        """test that calling fetchone() etc. on a result that doesn't
        return rows fails gracefully.

        """

        # these proxies don't work with no cursor.description present.
        # so they don't apply to this test at the moment.
        # result.FullyBufferedResultProxy,
        # result.BufferedRowResultProxy,
        # result.BufferedColumnResultProxy

        users = self.tables.users

        conn = testing.db.connect()
        for meth in [
            lambda r: r.fetchone(),
            lambda r: r.fetchall(),
            lambda r: r.first(),
            lambda r: r.scalar(),
            lambda r: r.fetchmany(),
            lambda r: r._getter('user'),
            lambda r: r._has_key('user'),
        ]:
            trans = conn.begin()
            result = conn.execute(users.insert(), user_id=1)
            assert_raises_message(
                exc.ResourceClosedError,
                "This result object does not return rows. "
                "It has been closed automatically.",
                meth, result,
            )
            trans.rollback()

    def test_fetchone_til_end(self):
        result = testing.db.execute("select * from users")
        eq_(result.fetchone(), None)
        eq_(result.fetchone(), None)
        eq_(result.fetchone(), None)
        result.close()
        assert_raises_message(
            exc.ResourceClosedError,
            "This result object is closed.",
            result.fetchone
        )

    def test_row_case_sensitive(self):
        row = testing.db.execute(
            select([
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive")
            ])
        ).first()

        eq_(list(row.keys()), ["case_insensitive", "CaseSensitive"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        not_in_("casesensitive", row._keymap)

        eq_(row["case_insensitive"], 1)
        eq_(row["CaseSensitive"], 2)

        assert_raises(
            KeyError,
            lambda: row["Case_insensitive"]
        )
        assert_raises(
            KeyError,
            lambda: row["casesensitive"]
        )

    def test_row_case_sensitive_unoptimized(self):
        ins_db = engines.testing_engine(options={"case_sensitive": True})
        row = ins_db.execute(
            select([
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive"),
                text("3 AS screw_up_the_cols")
            ])
        ).first()

        eq_(
            list(row.keys()),
            ["case_insensitive", "CaseSensitive", "screw_up_the_cols"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        not_in_("casesensitive", row._keymap)

        eq_(row["case_insensitive"], 1)
        eq_(row["CaseSensitive"], 2)
        eq_(row["screw_up_the_cols"], 3)

        assert_raises(KeyError, lambda: row["Case_insensitive"])
        assert_raises(KeyError, lambda: row["casesensitive"])
        assert_raises(KeyError, lambda: row["screw_UP_the_cols"])

    def test_row_case_insensitive(self):
        ins_db = engines.testing_engine(options={"case_sensitive": False})
        row = ins_db.execute(
            select([
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive")
            ])
        ).first()

        eq_(list(row.keys()), ["case_insensitive", "CaseSensitive"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        in_("casesensitive", row._keymap)

        eq_(row["case_insensitive"], 1)
        eq_(row["CaseSensitive"], 2)
        eq_(row["Case_insensitive"], 1)
        eq_(row["casesensitive"], 2)

    def test_row_case_insensitive_unoptimized(self):
        ins_db = engines.testing_engine(options={"case_sensitive": False})
        row = ins_db.execute(
            select([
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive"),
                text("3 AS screw_up_the_cols")
            ])
        ).first()

        eq_(
            list(row.keys()),
            ["case_insensitive", "CaseSensitive", "screw_up_the_cols"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        in_("casesensitive", row._keymap)

        eq_(row["case_insensitive"], 1)
        eq_(row["CaseSensitive"], 2)
        eq_(row["screw_up_the_cols"], 3)
        eq_(row["Case_insensitive"], 1)
        eq_(row["casesensitive"], 2)
        eq_(row["screw_UP_the_cols"], 3)

    def test_row_as_args(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='john')
        r = users.select(users.c.user_id == 1).execute().first()
        users.delete().execute()
        users.insert().execute(r)
        eq_(users.select().execute().fetchall(), [(1, 'john')])

    def test_result_as_args(self):
        users = self.tables.users
        users2 = self.tables.users2

        users.insert().execute([
            dict(user_id=1, user_name='john'),
            dict(user_id=2, user_name='ed')])
        r = users.select().execute()
        users2.insert().execute(list(r))
        eq_(
            users2.select().order_by(users2.c.user_id).execute().fetchall(),
            [(1, 'john'), (2, 'ed')]
        )

        users2.delete().execute()
        r = users.select().execute()
        users2.insert().execute(*list(r))
        eq_(
            users2.select().order_by(users2.c.user_id).execute().fetchall(),
            [(1, 'john'), (2, 'ed')]
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column(self):
        users = self.tables.users
        addresses = self.tables.addresses

        users.insert().execute(user_id=1, user_name='john')
        result = users.outerjoin(addresses).select().execute()
        r = result.first()

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r[users.c.user_id]
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r[addresses.c.user_id]
        )

        # try to trick it - fake_table isn't in the result!
        # we get the correct error
        fake_table = Table('fake', MetaData(), Column('user_id', Integer))
        assert_raises_message(
            exc.InvalidRequestError,
            "Could not locate column in row for column 'fake.user_id'",
            lambda: r[fake_table.c.user_id]
        )

        r = util.pickle.loads(util.pickle.dumps(r))
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

        result = users.outerjoin(addresses).select().execute()
        result = _result.BufferedColumnResultProxy(result.context)
        r = result.first()
        assert isinstance(r, _result.BufferedColumnRow)
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r['user_id']
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_by_col(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='john')
        ua = users.alias()
        u2 = users.alias()
        result = select([users.c.user_id, ua.c.user_id]).execute()
        row = result.first()

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: row[users.c.user_id]
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: row[ua.c.user_id]
        )

        # Unfortunately, this fails -
        # we'd like
        # "Could not locate column in row"
        # to be raised here, but the check for
        # "common column" in _compare_name_for_result()
        # has other requirements to be more liberal.
        # Ultimately the
        # expression system would need a way to determine
        # if given two columns in a "proxy" relationship, if they
        # refer to a different parent table
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: row[u2.c.user_id]
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_contains(self):
        users = self.tables.users
        addresses = self.tables.addresses

        # ticket 2702.  in 0.7 we'd get True, False.
        # in 0.8, both columns are present so it's True;
        # but when they're fetched you'll get the ambiguous error.
        users.insert().execute(user_id=1, user_name='john')
        result = select([users.c.user_id, addresses.c.user_id]).\
            select_from(users.outerjoin(addresses)).execute()
        row = result.first()

        eq_(
            set([users.c.user_id in row, addresses.c.user_id in row]),
            set([True])
        )

    def test_ambiguous_column_by_col_plus_label(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='john')
        result = select(
            [users.c.user_id,
                type_coerce(users.c.user_id, Integer).label('foo')]).execute()
        row = result.first()
        eq_(
            row[users.c.user_id], 1
        )
        eq_(
            row[1], 1
        )

    def test_fetch_partial_result_map(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name='ed')

        t = text("select * from users").columns(
            user_name=String()
        )
        eq_(
            testing.db.execute(t).fetchall(), [(7, 'ed')]
        )

    def test_fetch_unordered_result_map(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name='ed')

        class Goofy1(TypeDecorator):
            impl = String

            def process_result_value(self, value, dialect):
                return value + "a"

        class Goofy2(TypeDecorator):
            impl = String

            def process_result_value(self, value, dialect):
                return value + "b"

        class Goofy3(TypeDecorator):
            impl = String

            def process_result_value(self, value, dialect):
                return value + "c"

        t = text(
            "select user_name as a, user_name as b, "
            "user_name as c from users").columns(
            a=Goofy1(), b=Goofy2(), c=Goofy3()
        )
        eq_(
            testing.db.execute(t).fetchall(), [
                ('eda', 'edb', 'edc')
            ]
        )

    @testing.requires.subqueries
    def test_column_label_targeting(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name='ed')

        for s in (
            users.select().alias('foo'),
            users.select().alias(users.name),
        ):
            row = s.select(use_labels=True).execute().first()
            eq_(row[s.c.user_id], 7)
            eq_(row[s.c.user_name], 'ed')

    def test_keys(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        result = users.select().execute()
        eq_(
            result.keys(),
            ['user_id', 'user_name']
        )
        row = result.first()
        eq_(
            row.keys(),
            ['user_id', 'user_name']
        )

    def test_keys_anon_labels(self):
        """test [ticket:3483]"""

        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        result = testing.db.execute(
            select([
                users.c.user_id,
                users.c.user_name.label(None),
                func.count(literal_column('1'))]).
            group_by(users.c.user_id, users.c.user_name)
        )

        eq_(
            result.keys(),
            ['user_id', 'user_name_1', 'count_1']
        )
        row = result.first()
        eq_(
            row.keys(),
            ['user_id', 'user_name_1', 'count_1']
        )

    def test_items(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().first()
        eq_(
            [(x[0].lower(), x[1]) for x in list(r.items())],
            [('user_id', 1), ('user_name', 'foo')])

    def test_len(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        r = users.select().execute().first()
        eq_(len(r), 2)

        r = testing.db.execute('select user_name, user_id from users'). \
            first()
        eq_(len(r), 2)
        r = testing.db.execute('select user_name from users').first()
        eq_(len(r), 1)

    def test_sorting_in_python(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name='foo'),
            dict(user_id=2, user_name='bar'),
            dict(user_id=3, user_name='def'),
        )

        rows = users.select().order_by(users.c.user_name).execute().fetchall()

        eq_(rows, [(2, 'bar'), (3, 'def'), (1, 'foo')])

        eq_(sorted(rows), [(1, 'foo'), (2, 'bar'), (3, 'def')])

    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        r = users.select(users.c.user_id == 1).execute().first()
        eq_(r[0], 1)
        eq_(r[1], 'foo')
        eq_([x.lower() for x in list(r.keys())], ['user_id', 'user_name'])
        eq_(list(r.values()), [1, 'foo'])

    def test_column_order_with_text_query(self):
        # should return values in query order
        users = self.tables.users

        users.insert().execute(user_id=1, user_name='foo')
        r = testing.db.execute('select user_name, user_id from users'). \
            first()
        eq_(r[0], 'foo')
        eq_(r[1], 1)
        eq_([x.lower() for x in list(r.keys())], ['user_name', 'user_id'])
        eq_(list(r.values()), ['foo', 1])

    @testing.crashes('oracle', 'FIXME: unknown, varify not fails_on()')
    @testing.crashes('firebird', 'An identifier must begin with a letter')
    @testing.provide_metadata
    def test_column_accessor_shadow(self):
        shadowed = Table(
            'test_shadowed', self.metadata,
            Column('shadow_id', INT, primary_key=True),
            Column('shadow_name', VARCHAR(20)),
            Column('parent', VARCHAR(20)),
            Column('row', VARCHAR(40)),
            Column('_parent', VARCHAR(20)),
            Column('_row', VARCHAR(20)),
        )
        self.metadata.create_all()
        shadowed.insert().execute(
            shadow_id=1, shadow_name='The Shadow', parent='The Light',
            row='Without light there is no shadow',
            _parent='Hidden parent', _row='Hidden row')
        r = shadowed.select(shadowed.c.shadow_id == 1).execute().first()

        eq_(r.shadow_id, 1)
        eq_(r['shadow_id'], 1)
        eq_(r[shadowed.c.shadow_id], 1)

        eq_(r.shadow_name, 'The Shadow')
        eq_(r['shadow_name'], 'The Shadow')
        eq_(r[shadowed.c.shadow_name], 'The Shadow')

        eq_(r.parent, 'The Light')
        eq_(r['parent'], 'The Light')
        eq_(r[shadowed.c.parent], 'The Light')

        eq_(r.row, 'Without light there is no shadow')
        eq_(r['row'], 'Without light there is no shadow')
        eq_(r[shadowed.c.row], 'Without light there is no shadow')

        eq_(r['_parent'], 'Hidden parent')
        eq_(r['_row'], 'Hidden row')


class KeyTargetingTest(fixtures.TablesTest):
    run_inserts = 'once'
    run_deletes = None
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'keyed1', metadata, Column("a", CHAR(2), key="b"),
            Column("c", CHAR(2), key="q")
        )
        Table('keyed2', metadata, Column("a", CHAR(2)), Column("b", CHAR(2)))
        Table('keyed3', metadata, Column("a", CHAR(2)), Column("d", CHAR(2)))
        Table('keyed4', metadata, Column("b", CHAR(2)), Column("q", CHAR(2)))
        Table('content', metadata, Column('t', String(30), key="type"))
        Table('bar', metadata, Column('ctype', String(30), key="content_type"))

        if testing.requires.schemas.enabled:
            Table(
                'wschema', metadata,
                Column("a", CHAR(2), key="b"),
                Column("c", CHAR(2), key="q"),
                schema=testing.config.test_schema
            )

    @classmethod
    def insert_data(cls):
        cls.tables.keyed1.insert().execute(dict(b="a1", q="c1"))
        cls.tables.keyed2.insert().execute(dict(a="a2", b="b2"))
        cls.tables.keyed3.insert().execute(dict(a="a3", d="d3"))
        cls.tables.keyed4.insert().execute(dict(b="b4", q="q4"))
        cls.tables.content.insert().execute(type="t1")

        if testing.requires.schemas.enabled:
            cls.tables[
                '%s.wschema' % testing.config.test_schema].insert().execute(
                dict(b="a1", q="c1"))

    @testing.requires.schemas
    def test_keyed_accessor_wschema(self):
        keyed1 = self.tables['%s.wschema' % testing.config.test_schema]
        row = testing.db.execute(keyed1.select()).first()

        eq_(row.b, "a1")
        eq_(row.q, "c1")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    def test_keyed_accessor_single(self):
        keyed1 = self.tables.keyed1
        row = testing.db.execute(keyed1.select()).first()

        eq_(row.b, "a1")
        eq_(row.q, "c1")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    def test_keyed_accessor_single_labeled(self):
        keyed1 = self.tables.keyed1
        row = testing.db.execute(keyed1.select().apply_labels()).first()

        eq_(row.keyed1_b, "a1")
        eq_(row.keyed1_q, "c1")
        eq_(row.keyed1_a, "a1")
        eq_(row.keyed1_c, "c1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_conflict_2(self):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = testing.db.execute(select([keyed1, keyed2])).first()
        # row.b is unambiguous
        eq_(row.b, "b2")
        # row.a is ambiguous
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambig",
            getattr, row, "a"
        )

    def test_keyed_accessor_composite_names_precedent(self):
        keyed1 = self.tables.keyed1
        keyed4 = self.tables.keyed4

        row = testing.db.execute(select([keyed1, keyed4])).first()
        eq_(row.b, "b4")
        eq_(row.q, "q4")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_keys_precedent(self):
        keyed1 = self.tables.keyed1
        keyed3 = self.tables.keyed3

        row = testing.db.execute(select([keyed1, keyed3])).first()
        eq_(row.q, "c1")
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name 'b'",
            getattr, row, "b"
        )
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name 'a'",
            getattr, row, "a"
        )
        eq_(row.d, "d3")

    def test_keyed_accessor_composite_labeled(self):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = testing.db.execute(select([keyed1, keyed2]).apply_labels()). \
            first()
        eq_(row.keyed1_b, "a1")
        eq_(row.keyed1_a, "a1")
        eq_(row.keyed1_q, "c1")
        eq_(row.keyed1_c, "c1")
        eq_(row.keyed2_a, "a2")
        eq_(row.keyed2_b, "b2")
        assert_raises(KeyError, lambda: row['keyed2_c'])
        assert_raises(KeyError, lambda: row['keyed2_q'])

    def test_column_label_overlap_fallback(self):
        content, bar = self.tables.content, self.tables.bar
        row = testing.db.execute(
            select([content.c.type.label("content_type")])).first()

        not_in_(content.c.type, row)
        not_in_(bar.c.content_type, row)

        in_(sql.column('content_type'), row)

        row = testing.db.execute(select([func.now().label("content_type")])). \
            first()
        not_in_(content.c.type, row)
        not_in_(bar.c.content_type, row)
        in_(sql.column('content_type'), row)

    def test_column_label_overlap_fallback_2(self):
        content, bar = self.tables.content, self.tables.bar
        row = testing.db.execute(content.select(use_labels=True)).first()
        in_(content.c.type, row)
        not_in_(bar.c.content_type, row)
        not_in_(sql.column('content_type'), row)

    def test_columnclause_schema_column_one(self):
        keyed2 = self.tables.keyed2

        # this is addressed by [ticket:2932]
        # ColumnClause._compare_name_for_result allows the
        # columns which the statement is against to be lightweight
        # cols, which results in a more liberal comparison scheme
        a, b = sql.column('a'), sql.column('b')
        stmt = select([a, b]).select_from(table("keyed2"))
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row)
        in_(keyed2.c.b, row)
        in_(a, row)
        in_(b, row)

    def test_columnclause_schema_column_two(self):
        keyed2 = self.tables.keyed2

        a, b = sql.column('a'), sql.column('b')
        stmt = select([keyed2.c.a, keyed2.c.b])
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row)
        in_(keyed2.c.b, row)
        in_(a, row)
        in_(b, row)

    def test_columnclause_schema_column_three(self):
        keyed2 = self.tables.keyed2

        # this is also addressed by [ticket:2932]

        a, b = sql.column('a'), sql.column('b')
        stmt = text("select a, b from keyed2").columns(a=CHAR, b=CHAR)
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row)
        in_(keyed2.c.b, row)
        in_(a, row)
        in_(b, row)
        in_(stmt.c.a, row)
        in_(stmt.c.b, row)

    def test_columnclause_schema_column_four(self):
        keyed2 = self.tables.keyed2

        # this is also addressed by [ticket:2932]

        a, b = sql.column('keyed2_a'), sql.column('keyed2_b')
        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            a, b)
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row)
        in_(keyed2.c.b, row)
        in_(a, row)
        in_(b, row)
        in_(stmt.c.keyed2_a, row)
        in_(stmt.c.keyed2_b, row)

    def test_columnclause_schema_column_five(self):
        keyed2 = self.tables.keyed2

        # this is also addressed by [ticket:2932]

        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_a=CHAR, keyed2_b=CHAR)
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row)
        in_(keyed2.c.b, row)
        in_(stmt.c.keyed2_a, row)
        in_(stmt.c.keyed2_b, row)
