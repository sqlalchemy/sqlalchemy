import collections
from contextlib import contextmanager
import csv
import operator

from sqlalchemy import CHAR
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import type_coerce
from sqlalchemy import TypeDecorator
from sqlalchemy import util
from sqlalchemy import VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.engine import result as _result
from sqlalchemy.engine import Row
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import expression
from sqlalchemy.sql.selectable import TextualSelect
from sqlalchemy.sql.sqltypes import NULLTYPE
from sqlalchemy.sql.util import ClauseAdapter
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import le_
from sqlalchemy.testing import ne_
from sqlalchemy.testing import not_in_
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import collections_abc


class ResultProxyTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        Table(
            "addresses",
            metadata,
            Column(
                "address_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("user_id", Integer, ForeignKey("users.user_id")),
            Column("address", String(30)),
            test_needs_acid=True,
        )

        Table(
            "users2",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    def test_row_iteration(self):
        users = self.tables.users

        users.insert().execute(
            {"user_id": 7, "user_name": "jack"},
            {"user_id": 8, "user_name": "ed"},
            {"user_id": 9, "user_name": "fred"},
        )
        r = users.select().execute()
        rows = []
        for row in r:
            rows.append(row)
        eq_(len(rows), 3)

    def test_row_next(self):
        users = self.tables.users

        users.insert().execute(
            {"user_id": 7, "user_name": "jack"},
            {"user_id": 8, "user_name": "ed"},
            {"user_id": 9, "user_name": "fred"},
        )
        r = users.select().execute()
        rows = []
        while True:
            row = next(r, "foo")
            if row == "foo":
                break
            rows.append(row)
        eq_(len(rows), 3)

    @testing.requires.subqueries
    def test_anonymous_rows(self):
        users = self.tables.users

        users.insert().execute(
            {"user_id": 7, "user_name": "jack"},
            {"user_id": 8, "user_name": "ed"},
            {"user_id": 9, "user_name": "fred"},
        )

        sel = (
            select([users.c.user_id])
            .where(users.c.user_name == "jack")
            .scalar_subquery()
        )
        for row in select([sel + 1, sel + 3], bind=users.bind).execute():
            eq_(row._mapping["anon_1"], 8)
            eq_(row._mapping["anon_2"], 10)

    def test_row_comparison(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name="jack")
        rp = users.select().execute().first()

        eq_(rp, rp)
        is_(not (rp != rp), True)

        equal = (7, "jack")

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
        for compare in [False, 8, endless(), "xyz", (7, "jack")]:
            for op in [
                operator.eq,
                operator.ne,
                operator.gt,
                operator.lt,
                operator.ge,
                operator.le,
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
        content = Table("content", self.metadata, Column("type", String(30)))
        bar = Table("bar", self.metadata, Column("content_type", String(30)))
        self.metadata.create_all(testing.db)
        testing.db.execute(content.insert().values(type="t1"))

        row = testing.db.execute(content.select(use_labels=True)).first()
        in_(content.c.type, row._mapping)
        not_in_(bar.c.content_type, row._mapping)

        not_in_(bar.c.content_type, row._mapping)

        row = testing.db.execute(
            select([func.now().label("content_type")])
        ).first()

        not_in_(content.c.type, row._mapping)

        not_in_(bar.c.content_type, row._mapping)

    def test_pickled_rows(self):
        users = self.tables.users

        users.insert().execute(
            {"user_id": 7, "user_name": "jack"},
            {"user_id": 8, "user_name": "ed"},
            {"user_id": 9, "user_name": "fred"},
        )

        for pickle in False, True:
            for use_labels in False, True:
                result = (
                    users.select(use_labels=use_labels)
                    .order_by(users.c.user_id)
                    .execute()
                    .fetchall()
                )

                if pickle:
                    result = util.pickle.loads(util.pickle.dumps(result))

                eq_(result, [(7, "jack"), (8, "ed"), (9, "fred")])
                if use_labels:
                    eq_(result[0]._mapping["users_user_id"], 7)
                    eq_(
                        list(result[0]._fields),
                        ["users_user_id", "users_user_name"],
                    )
                else:
                    eq_(result[0]._mapping["user_id"], 7)
                    eq_(list(result[0]._fields), ["user_id", "user_name"])

                eq_(result[0][0], 7)

                assert_raises(
                    exc.NoSuchColumnError, lambda: result[0]["fake key"]
                )

                assert_raises(
                    exc.NoSuchColumnError,
                    lambda: result[0]._mapping["fake key"],
                )

    def test_column_error_printing(self):
        result = testing.db.execute(select([1]))
        row = result.first()

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
                exc.NoSuchColumnError, msg % repl, result._getter, accessor
            )

            is_(result._getter(accessor, False), None)

            assert_raises_message(
                exc.NoSuchColumnError,
                msg % repl,
                lambda: row._mapping[accessor],
            )

    def test_fetchmany(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name="jack")
        users.insert().execute(user_id=8, user_name="ed")
        users.insert().execute(user_id=9, user_name="fred")
        r = users.select().execute()
        rows = []
        for row in r.fetchmany(size=2):
            rows.append(row)
        eq_(len(rows), 2)

    def test_column_slices(self):
        users = self.tables.users
        addresses = self.tables.addresses

        users.insert().execute(user_id=1, user_name="john")
        users.insert().execute(user_id=2, user_name="jack")
        addresses.insert().execute(
            address_id=1, user_id=2, address="foo@bar.com"
        )

        r = text("select * from addresses", bind=testing.db).execute().first()
        eq_(r[0:1], (1,))
        eq_(r[1:], (2, "foo@bar.com"))
        eq_(r[:-1], (1, 2))

    def test_column_accessor_basic_compiled_mapping(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )

        r = users.select(users.c.user_id == 2).execute().first()
        eq_(r.user_id, 2)
        eq_(r._mapping["user_id"], 2)
        eq_(r._mapping[users.c.user_id], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")
        eq_(r._mapping[users.c.user_name], "jack")

    def test_column_accessor_basic_compiled_traditional(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )

        r = users.select(users.c.user_id == 2).execute().first()

        eq_(r.user_id, 2)
        eq_(r._mapping["user_id"], 2)
        eq_(r._mapping[users.c.user_id], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")
        eq_(r._mapping[users.c.user_name], "jack")

    def test_row_getitem_string(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )

        r = testing.db.execute(
            text("select * from users where user_id=2")
        ).first()

        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_basic_text(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )
        r = testing.db.execute(
            text("select * from users where user_id=2")
        ).first()

        eq_(r.user_id, 2)

        eq_(r.user_name, "jack")

        eq_(r._mapping["user_id"], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_text_colexplicit(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )
        r = testing.db.execute(
            text("select * from users where user_id=2").columns(
                users.c.user_id, users.c.user_name
            )
        ).first()

        eq_(r.user_id, 2)
        eq_(r._mapping["user_id"], 2)
        eq_(r._mapping[users.c.user_id], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")
        eq_(r._mapping[users.c.user_name], "jack")

    def test_column_accessor_textual_select(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )
        # this will create column() objects inside
        # the select(), these need to match on name anyway
        r = testing.db.execute(
            select([column("user_id"), column("user_name")])
            .select_from(table("users"))
            .where(text("user_id=2"))
        ).first()

        # keyed access works in many ways
        eq_(r.user_id, 2)
        eq_(r.user_name, "jack")
        eq_(r._mapping["user_id"], 2)
        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_dotted_union(self):
        users = self.tables.users

        users.insert().execute(dict(user_id=1, user_name="john"))

        # test a little sqlite < 3.10.0 weirdness - with the UNION,
        # cols come back as "users.user_id" in cursor.description
        r = testing.db.execute(
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users"
            )
        ).first()
        eq_(r._mapping["user_id"], 1)
        eq_(r._mapping["user_name"], "john")
        eq_(list(r._fields), ["user_id", "user_name"])

    def test_column_accessor_sqlite_raw(self):
        users = self.tables.users

        users.insert().execute(dict(user_id=1, user_name="john"))

        r = (
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users",
                bind=testing.db,
            )
            .execution_options(sqlite_raw_colnames=True)
            .execute()
            .first()
        )

        if testing.against("sqlite < 3.10.0"):
            not_in_("user_id", r)
            not_in_("user_name", r)
            eq_(r["users.user_id"], 1)
            eq_(r["users.user_name"], "john")

            eq_(list(r._fields), ["users.user_id", "users.user_name"])
        else:
            not_in_("users.user_id", r._mapping)
            not_in_("users.user_name", r._mapping)
            eq_(r._mapping["user_id"], 1)
            eq_(r._mapping["user_name"], "john")

            eq_(list(r._fields), ["user_id", "user_name"])

    def test_column_accessor_sqlite_translated(self):
        users = self.tables.users

        users.insert().execute(dict(user_id=1, user_name="john"))

        r = (
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users",
                bind=testing.db,
            )
            .execute()
            .first()
        )
        eq_(r._mapping["user_id"], 1)
        eq_(r._mapping["user_name"], "john")

        if testing.against("sqlite < 3.10.0"):
            eq_(r._mapping["users.user_id"], 1)
            eq_(r._mapping["users.user_name"], "john")
        else:
            not_in_("users.user_id", r._mapping)
            not_in_("users.user_name", r._mapping)

        eq_(list(r._fields), ["user_id", "user_name"])

    def test_column_accessor_labels_w_dots(self):
        users = self.tables.users

        users.insert().execute(dict(user_id=1, user_name="john"))
        # test using literal tablename.colname
        r = (
            text(
                'select users.user_id AS "users.user_id", '
                'users.user_name AS "users.user_name" '
                "from users",
                bind=testing.db,
            )
            .execution_options(sqlite_raw_colnames=True)
            .execute()
            .first()
        )
        eq_(r._mapping["users.user_id"], 1)
        eq_(r._mapping["users.user_name"], "john")
        not_in_("user_name", r._mapping)
        eq_(list(r._fields), ["users.user_id", "users.user_name"])

    def test_column_accessor_unary(self):
        users = self.tables.users

        users.insert().execute(dict(user_id=1, user_name="john"))

        # unary expressions
        r = (
            select([users.c.user_name.distinct()])
            .order_by(users.c.user_name)
            .execute()
            .first()
        )
        eq_(r._mapping[users.c.user_name], "john")
        eq_(r.user_name, "john")

    def test_column_accessor_err(self):
        r = testing.db.execute(select([1])).first()
        assert_raises_message(
            AttributeError,
            "Could not locate column in row for column 'foo'",
            getattr,
            r,
            "foo",
        )
        assert_raises_message(
            KeyError,
            "Could not locate column in row for column 'foo'",
            lambda: r._mapping["foo"],
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
            lambda r: r._getter("user"),
            lambda r: r._has_key("user"),
        ]:
            trans = conn.begin()
            result = conn.execute(users.insert(), user_id=1)
            assert_raises_message(
                exc.ResourceClosedError,
                "This result object does not return rows. "
                "It has been closed automatically.",
                meth,
                result,
            )
            trans.rollback()

    def test_fetchone_til_end(self, connection):
        result = connection.exec_driver_sql("select * from users")
        eq_(result.fetchone(), None)
        eq_(result.fetchone(), None)
        eq_(result.fetchone(), None)
        result.close()
        assert_raises_message(
            exc.ResourceClosedError,
            "This result object is closed.",
            result.fetchone,
        )

    def test_connectionless_autoclose_rows_exhausted(self):
        users = self.tables.users
        with testing.db.connect() as conn:
            conn.execute(users.insert(), dict(user_id=1, user_name="john"))

        result = testing.db.execute(text("select * from users"))
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), (1, "john"))
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    @testing.requires.returning
    def test_connectionless_autoclose_crud_rows_exhausted(self):
        users = self.tables.users
        stmt = (
            users.insert()
            .values(user_id=1, user_name="john")
            .returning(users.c.user_id)
        )
        result = testing.db.execute(stmt)
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), (1,))
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    def test_connectionless_autoclose_no_rows(self):
        result = testing.db.execute(text("select * from users"))
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    @testing.requires.updateable_autoincrement_pks
    def test_connectionless_autoclose_no_metadata(self):
        result = testing.db.execute(text("update users set user_id=5"))
        connection = result.connection
        assert connection.closed
        assert_raises_message(
            exc.ResourceClosedError,
            "This result object does not return rows.",
            result.fetchone,
        )

    def test_row_case_sensitive(self):
        row = testing.db.execute(
            select(
                [
                    literal_column("1").label("case_insensitive"),
                    literal_column("2").label("CaseSensitive"),
                ]
            )
        ).first()

        eq_(list(row._fields), ["case_insensitive", "CaseSensitive"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        not_in_("casesensitive", row._keymap)

        eq_(row._mapping["case_insensitive"], 1)
        eq_(row._mapping["CaseSensitive"], 2)

        assert_raises(KeyError, lambda: row._mapping["Case_insensitive"])
        assert_raises(KeyError, lambda: row._mapping["casesensitive"])

    def test_row_case_sensitive_unoptimized(self):
        ins_db = engines.testing_engine()
        row = ins_db.execute(
            select(
                [
                    literal_column("1").label("case_insensitive"),
                    literal_column("2").label("CaseSensitive"),
                    text("3 AS screw_up_the_cols"),
                ]
            )
        ).first()

        eq_(
            list(row._fields),
            ["case_insensitive", "CaseSensitive", "screw_up_the_cols"],
        )

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        not_in_("casesensitive", row._keymap)

        eq_(row._mapping["case_insensitive"], 1)
        eq_(row._mapping["CaseSensitive"], 2)
        eq_(row._mapping["screw_up_the_cols"], 3)

        assert_raises(KeyError, lambda: row._mapping["Case_insensitive"])
        assert_raises(KeyError, lambda: row._mapping["casesensitive"])
        assert_raises(KeyError, lambda: row._mapping["screw_UP_the_cols"])

    def test_row_as_args(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="john")
        r = users.select(users.c.user_id == 1).execute().first()
        users.delete().execute()
        users.insert().execute(r._mapping)
        eq_(users.select().execute().fetchall(), [(1, "john")])

    def test_result_as_args(self):
        users = self.tables.users
        users2 = self.tables.users2

        users.insert().execute(
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="ed"),
            ]
        )
        r = users.select().execute()
        users2.insert().execute([row._mapping for row in r])
        eq_(
            users2.select().order_by(users2.c.user_id).execute().fetchall(),
            [(1, "john"), (2, "ed")],
        )

        users2.delete().execute()
        r = users.select().execute()
        users2.insert().execute(*[row._mapping for row in r])
        eq_(
            users2.select().order_by(users2.c.user_id).execute().fetchall(),
            [(1, "john"), (2, "ed")],
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column(self):
        users = self.tables.users
        addresses = self.tables.addresses

        users.insert().execute(user_id=1, user_name="john")
        result = users.outerjoin(addresses).select().execute()
        r = result.first()

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r._mapping["user_id"],
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            result._getter,
            "user_id",
        )

        # pure positional targeting; users.c.user_id
        # and addresses.c.user_id are known!
        # works as of 1.1 issue #3501
        eq_(r._mapping[users.c.user_id], 1)
        eq_(r._mapping[addresses.c.user_id], None)

        # try to trick it - fake_table isn't in the result!
        # we get the correct error
        fake_table = Table("fake", MetaData(), Column("user_id", Integer))
        assert_raises_message(
            exc.InvalidRequestError,
            "Could not locate column in row for column 'fake.user_id'",
            lambda: r._mapping[fake_table.c.user_id],
        )

        r = util.pickle.loads(util.pickle.dumps(r))
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r._mapping["user_id"],
        )

        result = users.outerjoin(addresses).select().execute()
        result = _result.BufferedColumnResultProxy(result.context)
        r = result.first()
        assert isinstance(r, _result.BufferedColumnRow)
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name",
            lambda: r._mapping["user_id"],
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_by_col(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="john")
        ua = users.alias()
        u2 = users.alias()
        result = (
            select([users.c.user_id, ua.c.user_id])
            .select_from(users.join(ua, true()))
            .execute()
        )
        row = result.first()

        # as of 1.1 issue #3501, we use pure positional
        # targeting for the column objects here
        eq_(row._mapping[users.c.user_id], 1)

        eq_(row._mapping[ua.c.user_id], 1)

        # this now works as of 1.1 issue #3501;
        # previously this was stuck on "ambiguous column name"
        assert_raises_message(
            exc.InvalidRequestError,
            "Could not locate column in row",
            lambda: row._mapping[u2.c.user_id],
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_contains(self):
        users = self.tables.users
        addresses = self.tables.addresses

        # ticket 2702.  in 0.7 we'd get True, False.
        # in 0.8, both columns are present so it's True;
        # but when they're fetched you'll get the ambiguous error.
        users.insert().execute(user_id=1, user_name="john")
        result = (
            select([users.c.user_id, addresses.c.user_id])
            .select_from(users.outerjoin(addresses))
            .execute()
        )
        row = result.first()

        eq_(
            set(
                [
                    users.c.user_id in row._mapping,
                    addresses.c.user_id in row._mapping,
                ]
            ),
            set([True]),
        )

    def test_loose_matching_one(self):
        users = self.tables.users
        addresses = self.tables.addresses

        with testing.db.connect() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "john"})
            conn.execute(
                addresses.insert(),
                {"address_id": 1, "user_id": 1, "address": "email"},
            )

            # use some column labels in the SELECT
            result = conn.execute(
                TextualSelect(
                    text(
                        "select users.user_name AS users_user_name, "
                        "users.user_id AS user_id, "
                        "addresses.address_id AS address_id "
                        "FROM users JOIN addresses "
                        "ON users.user_id = addresses.user_id "
                        "WHERE users.user_id=1 "
                    ),
                    [
                        users.c.user_id,
                        users.c.user_name,
                        addresses.c.address_id,
                    ],
                    positional=False,
                )
            )
            row = result.first()
            eq_(row._mapping[users.c.user_id], 1)
            eq_(row._mapping[users.c.user_name], "john")

    def test_loose_matching_two(self):
        users = self.tables.users
        addresses = self.tables.addresses

        with testing.db.connect() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "john"})
            conn.execute(
                addresses.insert(),
                {"address_id": 1, "user_id": 1, "address": "email"},
            )

            # use some column labels in the SELECT
            result = conn.execute(
                TextualSelect(
                    text(
                        "select users.user_name AS users_user_name, "
                        "users.user_id AS user_id, "
                        "addresses.user_id "
                        "FROM users JOIN addresses "
                        "ON users.user_id = addresses.user_id "
                        "WHERE users.user_id=1 "
                    ),
                    [users.c.user_id, users.c.user_name, addresses.c.user_id],
                    positional=False,
                )
            )
            row = result.first()

            assert_raises_message(
                exc.InvalidRequestError,
                "Ambiguous column name",
                lambda: row._mapping[users.c.user_id],
            )
            assert_raises_message(
                exc.InvalidRequestError,
                "Ambiguous column name",
                lambda: row._mapping[addresses.c.user_id],
            )
            eq_(row._mapping[users.c.user_name], "john")

    def test_ambiguous_column_by_col_plus_label(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="john")
        result = select(
            [
                users.c.user_id,
                type_coerce(users.c.user_id, Integer).label("foo"),
            ]
        ).execute()
        row = result.first()
        eq_(row._mapping[users.c.user_id], 1)
        eq_(row[1], 1)

    def test_fetch_partial_result_map(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name="ed")

        t = text("select * from users").columns(user_name=String())
        eq_(testing.db.execute(t).fetchall(), [(7, "ed")])

    def test_fetch_unordered_result_map(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name="ed")

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
            "user_name as c from users"
        ).columns(a=Goofy1(), b=Goofy2(), c=Goofy3())
        eq_(testing.db.execute(t).fetchall(), [("eda", "edb", "edc")])

    @testing.requires.subqueries
    def test_column_label_targeting(self):
        users = self.tables.users

        users.insert().execute(user_id=7, user_name="ed")

        for s in (
            users.select().alias("foo"),
            users.select().alias(users.name),
        ):
            row = s.select(use_labels=True).execute().first()
            eq_(row._mapping[s.c.user_id], 7)
            eq_(row._mapping[s.c.user_name], "ed")

    @testing.requires.python3
    def test_ro_mapping_py3k(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        result = users.select().execute()

        row = result.first()
        dict_row = row._asdict()

        # dictionaries aren't ordered in Python 3 until 3.7
        odict_row = collections.OrderedDict(
            [("user_id", 1), ("user_name", "foo")]
        )
        eq_(dict_row, odict_row)

        mapping_row = row._mapping

        eq_(list(mapping_row), list(mapping_row.keys()))
        eq_(odict_row.keys(), mapping_row.keys())
        eq_(odict_row.values(), mapping_row.values())
        eq_(odict_row.items(), mapping_row.items())

    @testing.requires.python2
    def test_ro_mapping_py2k(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        result = users.select().execute()

        row = result.first()
        dict_row = row._asdict()

        odict_row = collections.OrderedDict(
            [("user_id", 1), ("user_name", "foo")]
        )
        eq_(dict_row, odict_row)
        mapping_row = row._mapping

        eq_(list(mapping_row), list(mapping_row.keys()))
        eq_(odict_row.keys(), list(mapping_row.keys()))
        eq_(odict_row.values(), list(mapping_row.values()))
        eq_(odict_row.items(), list(mapping_row.items()))

    def test_keys(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        result = users.select().execute()
        eq_(result.keys(), ["user_id", "user_name"])
        row = result.first()
        eq_(list(row._mapping.keys()), ["user_id", "user_name"])
        eq_(row._fields, ("user_id", "user_name"))

    def test_row_keys_legacy_dont_warn(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        result = users.select().execute()
        row = result.first()
        # DO NOT WARN DEPRECATED IN 1.x, ONLY 2.0 WARNING
        eq_(dict(row), {"user_id": 1, "user_name": "foo"})
        eq_(row.keys(), ["user_id", "user_name"])

    def test_keys_anon_labels(self):
        """test [ticket:3483]"""

        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        result = testing.db.execute(
            select(
                [
                    users.c.user_id,
                    users.c.user_name.label(None),
                    func.count(literal_column("1")),
                ]
            ).group_by(users.c.user_id, users.c.user_name)
        )

        eq_(result.keys(), ["user_id", "user_name_1", "count_1"])
        row = result.first()
        eq_(row._fields, ("user_id", "user_name_1", "count_1"))
        eq_(list(row._mapping.keys()), ["user_id", "user_name_1", "count_1"])

    def test_items(self):
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        r = users.select().execute().first()
        eq_(
            [(x[0].lower(), x[1]) for x in list(r._mapping.items())],
            [("user_id", 1), ("user_name", "foo")],
        )

    def test_len(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        r = connection.execute(users.select()).first()
        eq_(len(r), 2)

        r = connection.exec_driver_sql(
            "select user_name, user_id from users"
        ).first()
        eq_(len(r), 2)
        r = connection.exec_driver_sql("select user_name from users").first()
        eq_(len(r), 1)

    def test_sorting_in_python(self):
        users = self.tables.users

        users.insert().execute(
            dict(user_id=1, user_name="foo"),
            dict(user_id=2, user_name="bar"),
            dict(user_id=3, user_name="def"),
        )

        rows = users.select().order_by(users.c.user_name).execute().fetchall()

        eq_(rows, [(2, "bar"), (3, "def"), (1, "foo")])

        eq_(sorted(rows), [(1, "foo"), (2, "bar"), (3, "def")])

    def test_column_order_with_simple_query(self):
        # should return values in column definition order
        users = self.tables.users

        users.insert().execute(user_id=1, user_name="foo")
        r = users.select(users.c.user_id == 1).execute().first()
        eq_(r[0], 1)
        eq_(r[1], "foo")
        eq_([x.lower() for x in r._fields], ["user_id", "user_name"])
        eq_(list(r._mapping.values()), [1, "foo"])

    def test_column_order_with_text_query(self, connection):
        # should return values in query order
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))

        r = connection.exec_driver_sql(
            "select user_name, user_id from users"
        ).first()
        eq_(r[0], "foo")
        eq_(r[1], 1)
        eq_([x.lower() for x in r._fields], ["user_name", "user_id"])
        eq_(list(r._mapping.values()), ["foo", 1])

    @testing.crashes("oracle", "FIXME: unknown, varify not fails_on()")
    @testing.crashes("firebird", "An identifier must begin with a letter")
    @testing.provide_metadata
    def test_column_accessor_shadow(self):
        shadowed = Table(
            "test_shadowed",
            self.metadata,
            Column("shadow_id", INT, primary_key=True),
            Column("shadow_name", VARCHAR(20)),
            Column("parent", VARCHAR(20)),
            Column("row", VARCHAR(40)),
            Column("_parent", VARCHAR(20)),
            Column("_row", VARCHAR(20)),
        )
        self.metadata.create_all()
        shadowed.insert().execute(
            shadow_id=1,
            shadow_name="The Shadow",
            parent="The Light",
            row="Without light there is no shadow",
            _parent="Hidden parent",
            _row="Hidden row",
        )
        r = shadowed.select(shadowed.c.shadow_id == 1).execute().first()

        eq_(r.shadow_id, 1)
        eq_(r._mapping["shadow_id"], 1)
        eq_(r._mapping[shadowed.c.shadow_id], 1)

        eq_(r.shadow_name, "The Shadow")
        eq_(r._mapping["shadow_name"], "The Shadow")
        eq_(r._mapping[shadowed.c.shadow_name], "The Shadow")

        eq_(r.parent, "The Light")
        eq_(r._mapping["parent"], "The Light")
        eq_(r._mapping[shadowed.c.parent], "The Light")

        eq_(r.row, "Without light there is no shadow")
        eq_(r._mapping["row"], "Without light there is no shadow")
        eq_(r._mapping[shadowed.c.row], "Without light there is no shadow")

        eq_(r._mapping["_parent"], "Hidden parent")
        eq_(r._mapping["_row"], "Hidden row")

    def test_nontuple_row(self):
        """ensure the C version of BaseRow handles
        duck-type-dependent rows.


        As of 1.4 they are converted internally to tuples in any case.

        """

        class MyList(object):
            def __init__(self, data):
                self.internal_list = data

            def __len__(self):
                return len(self.internal_list)

            def __getitem__(self, i):
                return list.__getitem__(self.internal_list, i)

        proxy = Row(
            object(),
            [None],
            {"key": (0, None, "key"), 0: (0, None, "key")},
            MyList(["value"]),
        )
        eq_(list(proxy), ["value"])
        eq_(proxy[0], "value")
        eq_(proxy._mapping["key"], "value")

    @testing.provide_metadata
    def test_no_rowcount_on_selects_inserts(self):
        """assert that rowcount is only called on deletes and updates.

        This because cursor.rowcount may can be expensive on some dialects
        such as Firebird, however many dialects require it be called
        before the cursor is closed.

        """

        metadata = self.metadata

        engine = engines.testing_engine()

        t = Table("t1", metadata, Column("data", String(10)))
        metadata.create_all(engine)

        with patch.object(
            engine.dialect.execution_ctx_cls, "rowcount"
        ) as mock_rowcount:
            mock_rowcount.__get__ = Mock()
            engine.execute(
                t.insert(), {"data": "d1"}, {"data": "d2"}, {"data": "d3"}
            )

            eq_(len(mock_rowcount.__get__.mock_calls), 0)

            eq_(
                engine.execute(t.select()).fetchall(),
                [("d1",), ("d2",), ("d3",)],
            )
            eq_(len(mock_rowcount.__get__.mock_calls), 0)

            engine.execute(t.update(), {"data": "d4"})

            eq_(len(mock_rowcount.__get__.mock_calls), 1)

            engine.execute(t.delete())
            eq_(len(mock_rowcount.__get__.mock_calls), 2)

    def test_row_is_sequence(self):

        row = Row(
            object(), [None], {"key": (None, 0), 0: (None, 0)}, ["value"]
        )
        is_true(isinstance(row, collections_abc.Sequence))

    def test_row_is_hashable(self):

        row = Row(
            object(),
            [None, None, None],
            {"key": (None, 0), 0: (None, 0)},
            (1, "value", "foo"),
        )
        eq_(hash(row), hash((1, "value", "foo")))

    @testing.provide_metadata
    def test_row_getitem_indexes_compiled(self):
        values = Table(
            "rp",
            self.metadata,
            Column("key", String(10), primary_key=True),
            Column("value", String(10)),
        )
        values.create()

        testing.db.execute(values.insert(), dict(key="One", value="Uno"))
        row = testing.db.execute(values.select()).first()
        eq_(row._mapping["key"], "One")
        eq_(row._mapping["value"], "Uno")
        eq_(row[0], "One")
        eq_(row[1], "Uno")
        eq_(row[-2], "One")
        eq_(row[-1], "Uno")
        eq_(row[1:0:-1], ("Uno",))

    @testing.only_on("sqlite")
    def test_row_getitem_indexes_raw(self, connection):
        row = connection.exec_driver_sql(
            "select 'One' as key, 'Uno' as value"
        ).first()
        eq_(row._mapping["key"], "One")
        eq_(row._mapping["value"], "Uno")
        eq_(row[0], "One")
        eq_(row[1], "Uno")
        eq_(row[-2], "One")
        eq_(row[-1], "Uno")
        eq_(row[1:0:-1], ("Uno",))

    @testing.requires.cextensions
    def test_row_c_sequence_check(self):

        metadata = MetaData()
        metadata.bind = "sqlite://"
        users = Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(40)),
        )
        users.create()

        users.insert().execute(name="Test")
        row = users.select().execute().fetchone()

        s = util.StringIO()
        writer = csv.writer(s)
        # csv performs PySequenceCheck call
        writer.writerow(row)
        assert s.getvalue().strip() == "1,Test"

    @testing.requires.selectone
    def test_empty_accessors(self, connection):
        statements = [
            (
                "select 1",
                [
                    lambda r: r.last_inserted_params(),
                    lambda r: r.last_updated_params(),
                    lambda r: r.prefetch_cols(),
                    lambda r: r.postfetch_cols(),
                    lambda r: r.inserted_primary_key,
                ],
                "Statement is not a compiled expression construct.",
            ),
            (
                select([1]),
                [
                    lambda r: r.last_inserted_params(),
                    lambda r: r.inserted_primary_key,
                ],
                r"Statement is not an insert\(\) expression construct.",
            ),
            (
                select([1]),
                [lambda r: r.last_updated_params()],
                r"Statement is not an update\(\) expression construct.",
            ),
            (
                select([1]),
                [lambda r: r.prefetch_cols(), lambda r: r.postfetch_cols()],
                r"Statement is not an insert\(\) "
                r"or update\(\) expression construct.",
            ),
        ]

        for stmt, meths, msg in statements:
            if isinstance(stmt, str):
                r = connection.exec_driver_sql(stmt)
            else:
                r = connection.execute(stmt)
            try:
                for meth in meths:
                    assert_raises_message(
                        sa_exc.InvalidRequestError, msg, meth, r
                    )

            finally:
                r.close()


class KeyTargetingTest(fixtures.TablesTest):
    run_inserts = "once"
    run_deletes = None
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "keyed1",
            metadata,
            Column("a", CHAR(2), key="b"),
            Column("c", CHAR(2), key="q"),
        )
        Table("keyed2", metadata, Column("a", CHAR(2)), Column("b", CHAR(2)))
        Table("keyed3", metadata, Column("a", CHAR(2)), Column("d", CHAR(2)))
        Table("keyed4", metadata, Column("b", CHAR(2)), Column("q", CHAR(2)))
        Table("content", metadata, Column("t", String(30), key="type"))
        Table("bar", metadata, Column("ctype", String(30), key="content_type"))

        if testing.requires.schemas.enabled:
            Table(
                "wschema",
                metadata,
                Column("a", CHAR(2), key="b"),
                Column("c", CHAR(2), key="q"),
                schema=testing.config.test_schema,
            )

    @classmethod
    def insert_data(cls):
        with testing.db.begin() as conn:
            conn.execute(cls.tables.keyed1.insert(), dict(b="a1", q="c1"))
            conn.execute(cls.tables.keyed2.insert(), dict(a="a2", b="b2"))
            conn.execute(cls.tables.keyed3.insert(), dict(a="a3", d="d3"))
            conn.execute(cls.tables.keyed4.insert(), dict(b="b4", q="q4"))
            conn.execute(cls.tables.content.insert(), dict(type="t1"))

            if testing.requires.schemas.enabled:
                conn.execute(
                    cls.tables[
                        "%s.wschema" % testing.config.test_schema
                    ].insert(),
                    dict(b="a1", q="c1"),
                )

    @testing.requires.schemas
    def test_keyed_accessor_wschema(self):
        keyed1 = self.tables["%s.wschema" % testing.config.test_schema]
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

    def _test_keyed_targeting_no_label_at_all(self, expression):
        lt = literal_column("2")
        stmt = select([literal_column("1"), expression, lt]).select_from(
            self.tables.keyed1
        )
        row = testing.db.execute(stmt).first()

        eq_(row._mapping[expression], "a1")
        eq_(row._mapping[lt], 2)

        # Postgresql for example has the key as "?column?", which dupes
        # easily.  we get around that because we know that "2" is unique
        eq_(row._mapping["2"], 2)

    def test_keyed_targeting_no_label_at_all_one(self):
        class not_named_max(expression.ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # explicit add
            kw["add_to_result_map"](None, None, (element,), NULLTYPE)
            return "max(a)"

        # assert that there is no "AS max_" or any label of any kind.
        eq_(str(select([not_named_max()])), "SELECT max(a)")

        nnm = not_named_max()
        self._test_keyed_targeting_no_label_at_all(nnm)

    def test_keyed_targeting_no_label_at_all_two(self):
        class not_named_max(expression.ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # we don't add to keymap here; compiler should be doing it
            return "max(a)"

        # assert that there is no "AS max_" or any label of any kind.
        eq_(str(select([not_named_max()])), "SELECT max(a)")

        nnm = not_named_max()
        self._test_keyed_targeting_no_label_at_all(nnm)

    def test_keyed_targeting_no_label_at_all_text(self):
        t1 = text("max(a)")
        t2 = text("min(a)")

        stmt = select([t1, t2]).select_from(self.tables.keyed1)
        row = testing.db.execute(stmt).first()

        eq_(row._mapping[t1], "a1")
        eq_(row._mapping[t2], "a1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_conflict_2(self):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = testing.db.execute(
            select([keyed1, keyed2]).select_from(keyed1.join(keyed2, true()))
        ).first()

        # column access is unambiguous
        eq_(row._mapping[self.tables.keyed2.c.b], "b2")

        # row.a is ambiguous
        assert_raises_message(
            exc.InvalidRequestError, "Ambig", getattr, row, "a"
        )

        # for "b" we have kind of a choice.  the name "b" is not ambiguous in
        # cursor.description in this case.  It is however ambiguous as far as
        # the objects we have queried against, because keyed1.c.a has key="b"
        # and keyed1.c.b is "b".   historically this was allowed as
        # non-ambiguous, however the column it targets changes based on
        # whether or not the dupe is present so it's ambiguous
        # eq_(row.b, "b2")
        assert_raises_message(
            exc.InvalidRequestError, "Ambig", getattr, row, "b"
        )

        # illustrate why row.b above is ambiguous, and not "b2"; because
        # if we didn't have keyed2, now it matches row.a.  a new column
        # shouldn't be able to grab the value from a previous column.
        row = testing.db.execute(select([keyed1])).first()
        eq_(row.b, "a1")

    def test_keyed_accessor_composite_conflict_2_fix_w_uselabels(self):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = testing.db.execute(
            select([keyed1, keyed2])
            .select_from(keyed1.join(keyed2, true()))
            .apply_labels()
        ).first()

        # column access is unambiguous
        eq_(row._mapping[self.tables.keyed2.c.b], "b2")

        eq_(row._mapping["keyed2_b"], "b2")
        eq_(row._mapping["keyed1_a"], "a1")

    def test_keyed_accessor_composite_names_precedent(self):
        keyed1 = self.tables.keyed1
        keyed4 = self.tables.keyed4

        row = testing.db.execute(
            select([keyed1, keyed4]).select_from(keyed1.join(keyed4, true()))
        ).first()
        eq_(row.b, "b4")
        eq_(row.q, "q4")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_keys_precedent(self):
        keyed1 = self.tables.keyed1
        keyed3 = self.tables.keyed3

        row = testing.db.execute(
            select([keyed1, keyed3]).select_from(keyed1.join(keyed3, true()))
        ).first()
        eq_(row.q, "c1")

        # prior to 1.4 #4887, this raised an "ambiguous column name 'a'""
        # message, because "b" is linked to "a" which is a dupe.  but we know
        # where "b" is in the row by position.
        eq_(row.b, "a1")

        # "a" is of course ambiguous
        assert_raises_message(
            exc.InvalidRequestError,
            "Ambiguous column name 'a'",
            getattr,
            row,
            "a",
        )
        eq_(row.d, "d3")

    def test_keyed_accessor_composite_labeled(self):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = testing.db.execute(
            select([keyed1, keyed2])
            .select_from(keyed1.join(keyed2, true()))
            .apply_labels()
        ).first()
        eq_(row.keyed1_b, "a1")
        eq_(row.keyed1_a, "a1")
        eq_(row.keyed1_q, "c1")
        eq_(row.keyed1_c, "c1")
        eq_(row.keyed2_a, "a2")
        eq_(row.keyed2_b, "b2")

        assert_raises(KeyError, lambda: row["keyed2_c"])
        assert_raises(KeyError, lambda: row["keyed2_q"])
        assert_raises(KeyError, lambda: row._mapping["keyed2_c"])
        assert_raises(KeyError, lambda: row._mapping["keyed2_q"])

    def test_keyed_accessor_column_is_repeated_multiple_times(self):
        # test new logic added as a result of the combination of #4892 and
        # #4887.   We allow duplicate columns, but we also have special logic
        # to disambiguate for the same column repeated, and as #4887 adds
        # stricter ambiguous result column logic, the compiler has to know to
        # not add these dupe columns to the result map, else they register as
        # ambiguous.

        keyed2 = self.tables.keyed2
        keyed3 = self.tables.keyed3

        stmt = (
            select(
                [
                    keyed2.c.a,
                    keyed3.c.a,
                    keyed2.c.a,
                    keyed2.c.a,
                    keyed3.c.a,
                    keyed3.c.a,
                    keyed3.c.d,
                    keyed3.c.d,
                ]
            )
            .select_from(keyed2.join(keyed3, true()))
            .apply_labels()
        )

        result = testing.db.execute(stmt)
        is_false(result._metadata.matched_on_name)

        # ensure the result map is the same number of cols so we can
        # use positional targeting
        eq_(
            [rec[0] for rec in result.context.compiled._result_columns],
            [
                "keyed2_a",
                "keyed3_a",
                "keyed2_a__1",
                "keyed2_a__1",
                "keyed3_a__1",
                "keyed3_a__1",
                "keyed3_d",
                "keyed3_d__1",
            ],
        )
        row = result.first()

        # keyed access will ignore the dupe cols
        eq_(row._mapping[keyed2.c.a], "a2")
        eq_(row._mapping[keyed3.c.a], "a3")
        eq_(result._getter(keyed3.c.a)(row), "a3")
        eq_(row._mapping[keyed3.c.d], "d3")

        # however we can get everything positionally
        eq_(row, ("a2", "a3", "a2", "a2", "a3", "a3", "d3", "d3"))
        eq_(row[0], "a2")
        eq_(row[1], "a3")
        eq_(row[2], "a2")
        eq_(row[3], "a2")
        eq_(row[4], "a3")
        eq_(row[5], "a3")
        eq_(row[6], "d3")
        eq_(row[7], "d3")

    def test_columnclause_schema_column_one(self):
        # originally addressed by [ticket:2932], however liberalized
        # Column-targeting rules are deprecated
        a, b = sql.column("a"), sql.column("b")
        stmt = select([a, b]).select_from(table("keyed2"))
        row = testing.db.execute(stmt).first()

        in_(a, row._mapping)
        in_(b, row._mapping)

    def test_columnclause_schema_column_two(self):
        keyed2 = self.tables.keyed2

        stmt = select([keyed2.c.a, keyed2.c.b])
        row = testing.db.execute(stmt).first()

        in_(keyed2.c.a, row._mapping)
        in_(keyed2.c.b, row._mapping)

    def test_columnclause_schema_column_three(self):
        # this is also addressed by [ticket:2932]
        stmt = text("select a, b from keyed2").columns(a=CHAR, b=CHAR)
        row = testing.db.execute(stmt).first()

        in_(stmt.selected_columns.a, row._mapping)
        in_(stmt.selected_columns.b, row._mapping)

    def test_columnclause_schema_column_four(self):
        # originally addressed by [ticket:2932], however liberalized
        # Column-targeting rules are deprecated

        a, b = sql.column("keyed2_a"), sql.column("keyed2_b")
        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            a, b
        )
        row = testing.db.execute(stmt).first()

        in_(a, row._mapping)
        in_(b, row._mapping)

        in_(stmt.selected_columns.keyed2_a, row._mapping)
        in_(stmt.selected_columns.keyed2_b, row._mapping)

    def test_columnclause_schema_column_five(self):
        # this is also addressed by [ticket:2932]

        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_a=CHAR, keyed2_b=CHAR
        )
        row = testing.db.execute(stmt).first()

        in_(stmt.selected_columns.keyed2_a, row._mapping)
        in_(stmt.selected_columns.keyed2_b, row._mapping)

    def _adapt_result_columns_fixture_one(self):
        keyed1 = self.tables.keyed1
        stmt = (
            select([keyed1.c.b, keyed1.c.q.label("foo")])
            .apply_labels()
            .subquery()
        )

        return select([stmt.c.keyed1_b, stmt.c.foo])

    def _adapt_result_columns_fixture_two(self):
        return text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_a=CHAR, keyed2_b=CHAR
        )

    def _adapt_result_columns_fixture_three(self):
        keyed1 = self.tables.keyed1
        stmt = select([keyed1.c.b, keyed1.c.q.label("foo")]).subquery()

        return select([stmt.c.b, stmt.c.foo])

    def _adapt_result_columns_fixture_four(self):
        keyed1 = self.tables.keyed1

        stmt1 = select([keyed1]).apply_labels()

        a1 = keyed1.alias()
        stmt2 = ClauseAdapter(a1).traverse(stmt1)

        return stmt2

    @testing.combinations(
        _adapt_result_columns_fixture_one,
        _adapt_result_columns_fixture_two,
        _adapt_result_columns_fixture_three,
        _adapt_result_columns_fixture_four,
        argnames="stmt_fn",
    )
    def test_adapt_result_columns(self, connection, stmt_fn):
        """test adaptation of a CursorResultMetadata to another one.


        This copies the _keymap from one to the other in terms of the
        selected columns of a target selectable.

        This is used by the statement caching process to re-use the
        CursorResultMetadata from the cached statement against the same
        statement sent separately.

        """

        stmt1 = stmt_fn(self)
        stmt2 = stmt_fn(self)

        eq_(stmt1._generate_cache_key(), stmt2._generate_cache_key())

        column_linkage = dict(
            zip(stmt1.selected_columns, stmt2.selected_columns)
        )

        result = connection.execute(stmt1)

        mock_context = Mock(
            compiled=result.context.compiled, invoked_statement=stmt2
        )
        existing_metadata = result._metadata
        adapted_metadata = existing_metadata._adapt_to_context(mock_context)

        eq_(existing_metadata.keys, adapted_metadata.keys)

        for k in existing_metadata._keymap:
            if isinstance(k, ColumnElement) and k in column_linkage:
                other_k = column_linkage[k]
            else:
                other_k = k

            is_(
                existing_metadata._keymap[k], adapted_metadata._keymap[other_k]
            )
        return stmt1, existing_metadata, stmt2, adapted_metadata


class PositionalTextTest(fixtures.TablesTest):
    run_inserts = "once"
    run_deletes = None
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "text1",
            metadata,
            Column("a", CHAR(2)),
            Column("b", CHAR(2)),
            Column("c", CHAR(2)),
            Column("d", CHAR(2)),
        )

    @classmethod
    def insert_data(cls):
        cls.tables.text1.insert().execute(
            [dict(a="a1", b="b1", c="c1", d="d1")]
        )

    def test_via_column(self):
        c1, c2, c3, c4 = column("q"), column("p"), column("r"), column("d")
        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)

        result = testing.db.execute(stmt)
        row = result.first()

        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c4], "d1")
        eq_(row[1], "b1")
        eq_(row._mapping["b"], "b1")
        eq_(list(row._mapping.keys()), ["a", "b", "c", "d"])
        eq_(row._fields, ("a", "b", "c", "d"))
        eq_(row._mapping["r"], "c1")
        eq_(row._mapping["d"], "d1")

    def test_fewer_cols_than_sql_positional(self):
        c1, c2 = column("q"), column("p")
        stmt = text("select a, b, c, d from text1").columns(c1, c2)

        # no warning as this can be similar for non-positional
        result = testing.db.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping["c"], "c1")

    def test_fewer_cols_than_sql_non_positional(self):
        c1, c2 = column("a"), column("p")
        stmt = text("select a, b, c, d from text1").columns(c2, c1, d=CHAR)

        # no warning as this can be similar for non-positional
        result = testing.db.execute(stmt)
        row = result.first()

        # c1 name matches, locates
        eq_(row._mapping[c1], "a1")
        eq_(row._mapping["c"], "c1")

        # c2 name does not match, doesn't locate
        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'p'",
            lambda: row._mapping[c2],
        )

    def test_more_cols_than_sql_positional(self):
        c1, c2, c3, c4 = column("q"), column("p"), column("r"), column("d")
        stmt = text("select a, b from text1").columns(c1, c2, c3, c4)

        with assertions.expect_warnings(
            r"Number of columns in textual SQL \(4\) is "
            r"smaller than number of columns requested \(2\)"
        ):
            result = testing.db.execute(stmt)

        row = result.first()
        eq_(row._mapping[c2], "b1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_more_cols_than_sql_nonpositional(self):
        c1, c2, c3, c4 = column("b"), column("a"), column("r"), column("d")
        stmt = TextualSelect(
            text("select a, b from text1"), [c1, c2, c3, c4], positional=False
        )

        # no warning for non-positional
        result = testing.db.execute(stmt)

        row = result.first()
        eq_(row._mapping[c1], "b1")
        eq_(row._mapping[c2], "a1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_more_cols_than_sql_nonpositional_labeled_cols(self):
        text1 = self.tables.text1
        c1, c2, c3, c4 = text1.c.b, text1.c.a, column("r"), column("d")

        # the compiler will enable loose matching for this statement
        # so that column._label is taken into account
        stmt = TextualSelect(
            text("select a, b AS text1_b from text1"),
            [c1, c2, c3, c4],
            positional=False,
        )

        # no warning for non-positional
        result = testing.db.execute(stmt)

        row = result.first()
        eq_(row._mapping[c1], "b1")
        eq_(row._mapping[c2], "a1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_dupe_col_obj(self):
        c1, c2, c3 = column("q"), column("p"), column("r")
        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c2)

        assert_raises_message(
            exc.InvalidRequestError,
            "Duplicate column expression requested in "
            "textual SQL: <.*.ColumnClause.*; p>",
            testing.db.execute,
            stmt,
        )

    def test_anon_aliased_unique(self):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.c
        c3 = text1.alias().c.b
        c4 = text1.alias().c.d.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = testing.db.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c3], "c1")
        eq_(row._mapping[c4], "d1")

        # text1.c.b goes nowhere....because we hit key fallback
        # but the text1.c.b doesn't derive from text1.c.c
        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'text1.b'",
            lambda: row._mapping[text1.c.b],
        )

    def test_anon_aliased_overlapping(self):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.a
        c3 = text1.alias().c.a.label(None)
        c4 = text1.c.a.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = testing.db.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c3], "c1")
        eq_(row._mapping[c4], "d1")

    def test_anon_aliased_name_conflict(self):
        text1 = self.tables.text1

        c1 = text1.c.a.label("a")
        c2 = text1.alias().c.a
        c3 = text1.alias().c.a.label("a")
        c4 = text1.c.a.label("a")

        # all cols are named "a".  if we are positional, we don't care.
        # this is new logic in 1.1
        stmt = text("select a, b as a, c as a, d as a from text1").columns(
            c1, c2, c3, c4
        )
        result = testing.db.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c3], "c1")
        eq_(row._mapping[c4], "d1")

        # fails, because we hit key fallback and find conflicts
        # in columns that are presnet
        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'text1.a'",
            lambda: row._mapping[text1.c.a],
        )


class AlternateResultProxyTest(fixtures.TablesTest):
    __requires__ = ("sqlite",)

    @classmethod
    def setup_bind(cls):
        cls.engine = engine = engines.testing_engine("sqlite://")
        return engine

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("x", Integer, primary_key=True),
            Column("y", String(50)),
        )

    @classmethod
    def insert_data(cls):
        cls.engine.execute(
            cls.tables.test.insert(),
            [{"x": i, "y": "t_%d" % i} for i in range(1, 12)],
        )

    @contextmanager
    def _proxy_fixture(self, cls):
        self.table = self.tables.test

        class ExcCtx(default.DefaultExecutionContext):
            def get_result_cursor_strategy(self, result):
                return cls.create(result)

            def get_result_proxy(self):
                raise NotImplementedError()

        self.patcher = patch.object(
            self.engine.dialect, "execution_ctx_cls", ExcCtx
        )
        with self.patcher:
            yield

    def _test_proxy(self, cls):
        with self._proxy_fixture(cls):
            rows = []
            r = self.engine.execute(select([self.table]))
            assert isinstance(r.cursor_strategy, cls)
            for i in range(5):
                rows.append(r.fetchone())
            eq_(rows, [(i, "t_%d" % i) for i in range(1, 6)])

            rows = r.fetchmany(3)
            eq_(rows, [(i, "t_%d" % i) for i in range(6, 9)])

            rows = r.fetchall()
            eq_(rows, [(i, "t_%d" % i) for i in range(9, 12)])

            r = self.engine.execute(select([self.table]))
            rows = r.fetchmany(None)
            eq_(rows[0], (1, "t_1"))
            # number of rows here could be one, or the whole thing
            assert len(rows) == 1 or len(rows) == 11

            r = self.engine.execute(select([self.table]).limit(1))
            r.fetchone()
            eq_(r.fetchone(), None)

            r = self.engine.execute(select([self.table]).limit(5))
            rows = r.fetchmany(6)
            eq_(rows, [(i, "t_%d" % i) for i in range(1, 6)])

            # result keeps going just fine with blank results...
            eq_(r.fetchmany(2), [])

            eq_(r.fetchmany(2), [])

            eq_(r.fetchall(), [])

            eq_(r.fetchone(), None)

            # until we close
            r.close()

            self._assert_result_closed(r)

            r = self.engine.execute(select([self.table]).limit(5))
            eq_(r.first(), (1, "t_1"))
            self._assert_result_closed(r)

            r = self.engine.execute(select([self.table]).limit(5))
            eq_(r.scalar(), 1)
            self._assert_result_closed(r)

    def _assert_result_closed(self, r):
        assert_raises_message(
            sa_exc.ResourceClosedError, "object is closed", r.fetchone
        )

        assert_raises_message(
            sa_exc.ResourceClosedError, "object is closed", r.fetchmany, 2
        )

        assert_raises_message(
            sa_exc.ResourceClosedError, "object is closed", r.fetchall
        )

    def test_basic_plain(self):
        self._test_proxy(_result.DefaultCursorFetchStrategy)

    def test_basic_buffered_row_result_proxy(self):
        self._test_proxy(_result.BufferedRowCursorFetchStrategy)

    def test_basic_fully_buffered_result_proxy(self):
        self._test_proxy(_result.FullyBufferedCursorFetchStrategy)

    def test_basic_buffered_column_result_proxy(self):
        self._test_proxy(_result.DefaultCursorFetchStrategy)

    def test_resultprocessor_plain(self):
        self._test_result_processor(_result.DefaultCursorFetchStrategy, False)

    def test_resultprocessor_plain_cached(self):
        self._test_result_processor(_result.DefaultCursorFetchStrategy, True)

    def test_resultprocessor_buffered_row(self):
        self._test_result_processor(
            _result.BufferedRowCursorFetchStrategy, False
        )

    def test_resultprocessor_buffered_row_cached(self):
        self._test_result_processor(
            _result.BufferedRowCursorFetchStrategy, True
        )

    def test_resultprocessor_fully_buffered(self):
        self._test_result_processor(
            _result.FullyBufferedCursorFetchStrategy, False
        )

    def test_resultprocessor_fully_buffered_cached(self):
        self._test_result_processor(
            _result.FullyBufferedCursorFetchStrategy, True
        )

    def _test_result_processor(self, cls, use_cache):
        class MyType(TypeDecorator):
            impl = String()

            def process_result_value(self, value, dialect):
                return "HI " + value

        with self._proxy_fixture(cls):
            with self.engine.connect() as conn:
                if use_cache:
                    cache = {}
                    conn = conn.execution_options(compiled_cache=cache)

                stmt = select([literal("THERE", type_=MyType())])
                for i in range(2):
                    r = conn.execute(stmt)
                    eq_(r.scalar(), "HI THERE")

    @testing.fixture
    def row_growth_fixture(self):
        with self._proxy_fixture(_result.BufferedRowCursorFetchStrategy):
            with self.engine.connect() as conn:
                conn.execute(
                    self.table.insert(),
                    [{"x": i, "y": "t_%d" % i} for i in range(15, 3000)],
                )
                yield conn

    @testing.combinations(
        ("no option", None, {0: 5, 1: 25, 9: 125, 135: 625, 274: 1000}),
        ("lt 1000", 27, {0: 5, 16: 27, 70: 27, 150: 27, 250: 27}),
        (
            "gt 1000",
            1500,
            {0: 5, 1: 25, 9: 125, 135: 625, 274: 1500, 1351: 1500},
        ),
        (
            "gt 1500",
            2000,
            {0: 5, 1: 25, 9: 125, 135: 625, 274: 2000, 1351: 2000},
        ),
        id_="iaa",
        argnames="max_row_buffer,checks",
    )
    def test_buffered_row_growth(
        self, row_growth_fixture, max_row_buffer, checks
    ):
        if max_row_buffer:
            result = row_growth_fixture.execution_options(
                max_row_buffer=max_row_buffer
            ).execute(self.table.select())
        else:
            result = row_growth_fixture.execute(self.table.select())

        assertion = {}
        max_size = max(checks.values())
        for idx, row in enumerate(result, 0):
            if idx in checks:
                assertion[idx] = result.cursor_strategy._bufsize
            le_(len(result.cursor_strategy._rowbuffer), max_size)

        eq_(checks, assertion)
