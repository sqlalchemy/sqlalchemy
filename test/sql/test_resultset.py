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
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.engine import default
from sqlalchemy.engine import Row
from sqlalchemy.engine.result import SimpleResultMetaData
from sqlalchemy.engine.row import KEY_INTEGER_ONLY
from sqlalchemy.engine.row import KEY_OBJECTS_BUT_WARN
from sqlalchemy.engine.row import LegacyRow
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import expression
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.sql.selectable import TextualSelect
from sqlalchemy.sql.sqltypes import NULLTYPE
from sqlalchemy.sql.util import ClauseAdapter
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import le_
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import collections_abc


class CursorResultTest(fixtures.TablesTest):
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

    def test_row_iteration(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )
        r = connection.execute(users.select())
        rows = []
        for row in r:
            rows.append(row)
        eq_(len(rows), 3)

    def test_row_next(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )
        r = connection.execute(users.select())
        rows = []
        while True:
            row = next(r, "foo")
            if row == "foo":
                break
            rows.append(row)
        eq_(len(rows), 3)

    @testing.requires.subqueries
    def test_anonymous_rows(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )

        sel = (
            select(users.c.user_id)
            .where(users.c.user_name == "jack")
            .scalar_subquery()
        )
        for row in connection.execute(
            select([sel + 1, sel + 3], bind=users.bind)
        ):
            eq_(row._mapping["anon_1"], 8)
            eq_(row._mapping["anon_2"], 10)

    def test_row_comparison(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        rp = connection.execute(users.select()).first()

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
    def test_column_label_overlap_fallback(self, connection):
        content = Table("content", self.metadata, Column("type", String(30)))
        bar = Table("bar", self.metadata, Column("content_type", String(30)))
        self.metadata.create_all(connection)
        connection.execute(content.insert().values(type="t1"))

        row = connection.execute(
            content.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        ).first()
        in_(content.c.type, row._mapping)
        not_in(bar.c.content_type, row._mapping)

        not_in(bar.c.content_type, row._mapping)

        row = connection.execute(
            select(func.now().label("content_type"))
        ).first()

        not_in(content.c.type, row._mapping)

        not_in(bar.c.content_type, row._mapping)

    def test_pickled_rows(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )

        for pickle in False, True:
            for use_labels in False, True:
                result = connection.execute(
                    users.select()
                    .order_by(users.c.user_id)
                    .set_label_style(
                        LABEL_STYLE_TABLENAME_PLUS_COL
                        if use_labels
                        else LABEL_STYLE_NONE
                    )
                ).fetchall()

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

    def test_column_error_printing(self, connection):
        result = connection.execute(select(1))
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

    def test_fetchmany(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [{"user_id": i, "user_name": "n%d" % i} for i in range(7, 15)],
        )
        r = connection.execute(users.select())
        rows = []
        for row in r.fetchmany(size=2):
            rows.append(row)
        eq_(len(rows), 2)

    def test_fetchmany_arraysize_default(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [{"user_id": i, "user_name": "n%d" % i} for i in range(1, 150)],
        )
        r = connection.execute(users.select())
        arraysize = r.cursor.arraysize
        rows = list(r.fetchmany())

        eq_(len(rows), min(arraysize, 150))

    def test_fetchmany_arraysize_set(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [{"user_id": i, "user_name": "n%d" % i} for i in range(7, 15)],
        )
        r = connection.execute(users.select())
        r.cursor.arraysize = 4
        rows = list(r.fetchmany())
        eq_(len(rows), 4)

    def test_column_slices(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        connection.execute(users.insert(), dict(user_id=2, user_name="jack"))
        connection.execute(
            addresses.insert(),
            dict(address_id=1, user_id=2, address="foo@bar.com"),
        )

        r = connection.execute(text("select * from addresses")).first()
        eq_(r[0:1], (1,))
        eq_(r[1:], (2, "foo@bar.com"))
        eq_(r[:-1], (1, 2))

    def test_mappings(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        connection.execute(users.insert(), dict(user_id=2, user_name="jack"))
        connection.execute(
            addresses.insert(),
            dict(address_id=1, user_id=2, address="foo@bar.com"),
        )

        r = connection.execute(text("select * from addresses"))
        eq_(
            r.mappings().all(),
            [{"address_id": 1, "user_id": 2, "address": "foo@bar.com"}],
        )

    def test_column_accessor_basic_compiled_mapping(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )

        r = connection.execute(users.select(users.c.user_id == 2)).first()
        eq_(r.user_id, 2)
        eq_(r._mapping["user_id"], 2)
        eq_(r._mapping[users.c.user_id], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")
        eq_(r._mapping[users.c.user_name], "jack")

    def test_column_accessor_basic_compiled_traditional(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )

        r = connection.execute(users.select(users.c.user_id == 2)).first()

        eq_(r.user_id, 2)
        eq_(r._mapping["user_id"], 2)
        eq_(r._mapping[users.c.user_id], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")
        eq_(r._mapping[users.c.user_name], "jack")

    def test_row_getitem_string(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )

        r = connection.execute(
            text("select * from users where user_id=2")
        ).first()

        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_basic_text(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )
        r = connection.execute(
            text("select * from users where user_id=2")
        ).first()

        eq_(r.user_id, 2)

        eq_(r.user_name, "jack")

        eq_(r._mapping["user_id"], 2)

        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_text_colexplicit(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )
        r = connection.execute(
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

    def test_column_accessor_textual_select(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )
        # this will create column() objects inside
        # the select(), these need to match on name anyway
        r = connection.execute(
            select(column("user_id"), column("user_name"))
            .select_from(table("users"))
            .where(text("user_id=2"))
        ).first()

        # keyed access works in many ways
        eq_(r.user_id, 2)
        eq_(r.user_name, "jack")
        eq_(r._mapping["user_id"], 2)
        eq_(r.user_name, "jack")
        eq_(r._mapping["user_name"], "jack")

    def test_column_accessor_dotted_union(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))

        # test a little sqlite < 3.10.0 weirdness - with the UNION,
        # cols come back as "users.user_id" in cursor.description
        r = connection.execute(
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

    def test_column_accessor_sqlite_raw(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))

        r = connection.execute(
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users",
            ).execution_options(sqlite_raw_colnames=True)
        ).first()

        if testing.against("sqlite < 3.10.0"):
            not_in("user_id", r)
            not_in("user_name", r)
            eq_(r["users.user_id"], 1)
            eq_(r["users.user_name"], "john")

            eq_(list(r._fields), ["users.user_id", "users.user_name"])
        else:
            not_in("users.user_id", r._mapping)
            not_in("users.user_name", r._mapping)
            eq_(r._mapping["user_id"], 1)
            eq_(r._mapping["user_name"], "john")

            eq_(list(r._fields), ["user_id", "user_name"])

    def test_column_accessor_sqlite_translated(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))

        r = connection.execute(
            text(
                "select users.user_id, users.user_name "
                "from users "
                "UNION select users.user_id, "
                "users.user_name from users",
            )
        ).first()
        eq_(r._mapping["user_id"], 1)
        eq_(r._mapping["user_name"], "john")

        if testing.against("sqlite < 3.10.0"):
            eq_(r._mapping["users.user_id"], 1)
            eq_(r._mapping["users.user_name"], "john")
        else:
            not_in("users.user_id", r._mapping)
            not_in("users.user_name", r._mapping)

        eq_(list(r._fields), ["user_id", "user_name"])

    def test_column_accessor_labels_w_dots(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        # test using literal tablename.colname
        r = connection.execute(
            text(
                'select users.user_id AS "users.user_id", '
                'users.user_name AS "users.user_name" '
                "from users",
            ).execution_options(sqlite_raw_colnames=True)
        ).first()
        eq_(r._mapping["users.user_id"], 1)
        eq_(r._mapping["users.user_name"], "john")
        not_in("user_name", r._mapping)
        eq_(list(r._fields), ["users.user_id", "users.user_name"])

    def test_column_accessor_unary(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))

        # unary expressions
        r = connection.execute(
            select(users.c.user_name.distinct()).order_by(users.c.user_name)
        ).first()
        eq_(r._mapping[users.c.user_name], "john")
        eq_(r.user_name, "john")

    def test_column_accessor_err(self, connection):
        r = connection.execute(select(1)).first()
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
        # result.FullyBufferedCursorResult,
        # result.BufferedRowCursorResult,
        # result.BufferedColumnCursorResult

        users = self.tables.users

        conn = testing.db.connect()
        for meth in [
            lambda r: r.fetchone(),
            lambda r: r.fetchall(),
            lambda r: r.first(),
            lambda r: r.scalar(),
            lambda r: r.fetchmany(),
            lambda r: r._getter("user"),
            lambda r: r.keys(),
            lambda r: r.columns("user"),
            lambda r: r.cursor_strategy.fetchone(r, r.cursor),
        ]:
            trans = conn.begin()
            result = conn.execute(users.insert(), dict(user_id=1))
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

    def test_row_case_sensitive(self, connection):
        row = connection.execute(
            select(
                literal_column("1").label("case_insensitive"),
                literal_column("2").label("CaseSensitive"),
            )
        ).first()

        eq_(list(row._fields), ["case_insensitive", "CaseSensitive"])

        in_("case_insensitive", row._keymap)
        in_("CaseSensitive", row._keymap)
        not_in("casesensitive", row._keymap)

        eq_(row._mapping["case_insensitive"], 1)
        eq_(row._mapping["CaseSensitive"], 2)

        assert_raises(KeyError, lambda: row._mapping["Case_insensitive"])
        assert_raises(KeyError, lambda: row._mapping["casesensitive"])

    def test_row_case_sensitive_unoptimized(self, testing_engine):
        with testing_engine().connect() as ins_conn:
            row = ins_conn.execute(
                select(
                    literal_column("1").label("case_insensitive"),
                    literal_column("2").label("CaseSensitive"),
                    text("3 AS screw_up_the_cols"),
                )
            ).first()

            eq_(
                list(row._fields),
                ["case_insensitive", "CaseSensitive", "screw_up_the_cols"],
            )

            in_("case_insensitive", row._keymap)
            in_("CaseSensitive", row._keymap)
            not_in("casesensitive", row._keymap)

            eq_(row._mapping["case_insensitive"], 1)
            eq_(row._mapping["CaseSensitive"], 2)
            eq_(row._mapping["screw_up_the_cols"], 3)

            assert_raises(KeyError, lambda: row._mapping["Case_insensitive"])
            assert_raises(KeyError, lambda: row._mapping["casesensitive"])
            assert_raises(KeyError, lambda: row._mapping["screw_UP_the_cols"])

    def test_row_as_args(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        r = connection.execute(users.select(users.c.user_id == 1)).first()
        connection.execute(users.delete())
        connection.execute(users.insert(), r._mapping)
        eq_(connection.execute(users.select()).fetchall(), [(1, "john")])

    def test_result_as_args(self, connection):
        users = self.tables.users
        users2 = self.tables.users2

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="ed"),
            ],
        )
        r = connection.execute(users.select())
        connection.execute(users2.insert(), [row._mapping for row in r])
        eq_(
            connection.execute(
                users2.select().order_by(users2.c.user_id)
            ).fetchall(),
            [(1, "john"), (2, "ed")],
        )

        connection.execute(users2.delete())
        r = connection.execute(users.select())
        connection.execute(users2.insert(), [row._mapping for row in r])
        eq_(
            connection.execute(
                users2.select().order_by(users2.c.user_id)
            ).fetchall(),
            [(1, "john"), (2, "ed")],
        )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        result = connection.execute(
            users.outerjoin(addresses)
            .select()
            .set_label_style(LABEL_STYLE_NONE)
        )
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

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_by_col(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        ua = users.alias()
        u2 = users.alias()
        result = connection.execute(
            select(users.c.user_id, ua.c.user_id)
            .select_from(users.join(ua, true()))
            .set_label_style(LABEL_STYLE_NONE)
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
    def test_ambiguous_column_contains(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        # ticket 2702.  in 0.7 we'd get True, False.
        # in 0.8, both columns are present so it's True;
        # but when they're fetched you'll get the ambiguous error.
        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        result = connection.execute(
            select(users.c.user_id, addresses.c.user_id).select_from(
                users.outerjoin(addresses)
            )
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

    def test_loose_matching_one(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        connection.execute(users.insert(), {"user_id": 1, "user_name": "john"})
        connection.execute(
            addresses.insert(),
            {"address_id": 1, "user_id": 1, "address": "email"},
        )

        # use some column labels in the SELECT
        result = connection.execute(
            TextualSelect(
                text(
                    "select users.user_name AS users_user_name, "
                    "users.user_id AS user_id, "
                    "addresses.address_id AS address_id "
                    "FROM users JOIN addresses "
                    "ON users.user_id = addresses.user_id "
                    "WHERE users.user_id=1 "
                ),
                [users.c.user_id, users.c.user_name, addresses.c.address_id],
                positional=False,
            )
        )
        row = result.first()
        eq_(row._mapping[users.c.user_id], 1)
        eq_(row._mapping[users.c.user_name], "john")

    def test_loose_matching_two(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses

        connection.execute(users.insert(), {"user_id": 1, "user_name": "john"})
        connection.execute(
            addresses.insert(),
            {"address_id": 1, "user_id": 1, "address": "email"},
        )

        # use some column labels in the SELECT
        result = connection.execute(
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

    def test_ambiguous_column_by_col_plus_label(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="john"))
        result = connection.execute(
            select(
                users.c.user_id,
                type_coerce(users.c.user_id, Integer).label("foo"),
            )
        )
        row = result.first()
        eq_(row._mapping[users.c.user_id], 1)
        eq_(row[1], 1)

    def test_fetch_partial_result_map(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="ed"))

        t = text("select * from users").columns(user_name=String())
        eq_(connection.execute(t).fetchall(), [(7, "ed")])

    def test_fetch_unordered_result_map(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="ed"))

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
        eq_(connection.execute(t).fetchall(), [("eda", "edb", "edc")])

    @testing.requires.subqueries
    def test_column_label_targeting(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="ed"))

        for s in (
            users.select().alias("foo"),
            users.select().alias(users.name),
        ):
            row = connection.execute(
                s.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            ).first()
            eq_(row._mapping[s.c.user_id], 7)
            eq_(row._mapping[s.c.user_name], "ed")

    @testing.requires.python3
    def test_ro_mapping_py3k(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())

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
    def test_ro_mapping_py2k(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())

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

    @testing.combinations(
        (lambda result: result),
        (lambda result: result.first(),),
        (lambda result: result.first()._mapping),
        argnames="get_object",
    )
    @testing.combinations(
        (True,),
        (False,),
        argnames="future",
    )
    def test_keys(self, connection, get_object, future):
        users = self.tables.users
        addresses = self.tables.addresses

        if future:
            connection = connection.execution_options(future_result=True)

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())

        obj = get_object(result)

        # Row still has a .keys() method as well as LegacyRow
        # as in 1.3.x, the KeyedTuple object also had a keys() method.
        # it emits a 2.0 deprecation warning.
        if isinstance(obj, Row):
            with assertions.expect_deprecated_20("The Row.keys()"):
                keys = obj.keys()
        else:
            keys = obj.keys()

        # in 1.4, keys() is now a view that includes support for testing
        # of columns and other objects
        eq_(len(keys), 2)
        eq_(list(keys), ["user_id", "user_name"])
        eq_(keys, ["user_id", "user_name"])
        ne_(keys, ["user_name", "user_id"])
        in_("user_id", keys)
        not_in("foo", keys)
        in_(users.c.user_id, keys)
        not_in(0, keys)
        not_in(addresses.c.user_id, keys)
        not_in(addresses.c.address, keys)

        if isinstance(obj, Row):
            eq_(obj._fields, ("user_id", "user_name"))

    def test_row_mapping_keys(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())
        eq_(result.keys(), ["user_id", "user_name"])
        row = result.first()
        eq_(list(row._mapping.keys()), ["user_id", "user_name"])
        eq_(row._fields, ("user_id", "user_name"))

        with assertions.expect_deprecated_20("The Row.keys()"):
            in_("user_id", row.keys())
        with assertions.expect_deprecated_20("The Row.keys()"):
            not_in("foo", row.keys())
        with assertions.expect_deprecated_20("The Row.keys()"):
            in_(users.c.user_id, row.keys())

    def test_row_keys_legacy_dont_warn(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())
        row = result.first()
        # DO NOT WARN DEPRECATED IN 1.x, ONLY 2.0 WARNING

        with assertions.expect_deprecated_20("The Row.keys()"):
            eq_(dict(row), {"user_id": 1, "user_name": "foo"})

        with assertions.expect_deprecated_20("The Row.keys()"):
            eq_(row.keys(), ["user_id", "user_name"])

    def test_row_namedtuple_legacy_ok(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(users.select())
        row = result.first()
        eq_(row.user_id, 1)
        eq_(row.user_name, "foo")

    def test_keys_anon_labels(self, connection):
        """test [ticket:3483]"""

        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        result = connection.execute(
            select(
                users.c.user_id,
                users.c.user_name.label(None),
                func.count(literal_column("1")),
            ).group_by(users.c.user_id, users.c.user_name)
        )

        eq_(result.keys(), ["user_id", "user_name_1", "count_1"])
        row = result.first()
        eq_(row._fields, ("user_id", "user_name_1", "count_1"))
        eq_(list(row._mapping.keys()), ["user_id", "user_name_1", "count_1"])

    def test_items(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        r = connection.execute(users.select()).first()
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

    def test_sorting_in_python(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=1, user_name="foo"),
                dict(user_id=2, user_name="bar"),
                dict(user_id=3, user_name="def"),
            ],
        )

        rows = connection.execute(
            users.select().order_by(users.c.user_name)
        ).fetchall()

        eq_(rows, [(2, "bar"), (3, "def"), (1, "foo")])

        eq_(sorted(rows), [(1, "foo"), (2, "bar"), (3, "def")])

    def test_column_order_with_simple_query(self, connection):
        # should return values in column definition order
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="foo"))
        r = connection.execute(users.select(users.c.user_id == 1)).first()
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

    @testing.crashes("oracle", "FIXME: unknown, verify not fails_on()")
    @testing.crashes("firebird", "An identifier must begin with a letter")
    @testing.provide_metadata
    def test_column_accessor_shadow(self, connection):
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
        self.metadata.create_all(connection)
        connection.execute(
            shadowed.insert(),
            dict(
                shadow_id=1,
                shadow_name="The Shadow",
                parent="The Light",
                row="Without light there is no shadow",
                _parent="Hidden parent",
                _row="Hidden row",
            ),
        )
        r = connection.execute(
            shadowed.select(shadowed.c.shadow_id == 1)
        ).first()

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
            Row._default_key_style,
            MyList(["value"]),
        )
        eq_(list(proxy), ["value"])
        eq_(proxy[0], "value")
        eq_(proxy._mapping["key"], "value")

    def test_no_rowcount_on_selects_inserts(self, metadata, testing_engine):
        """assert that rowcount is only called on deletes and updates.

        This because cursor.rowcount may can be expensive on some dialects
        such as Firebird, however many dialects require it be called
        before the cursor is closed.

        """

        engine = testing_engine()

        t = Table("t1", metadata, Column("data", String(10)))
        metadata.create_all(engine)

        with patch.object(
            engine.dialect.execution_ctx_cls, "rowcount"
        ) as mock_rowcount:
            with engine.begin() as conn:
                mock_rowcount.__get__ = Mock()
                conn.execute(
                    t.insert(),
                    [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}],
                )

                eq_(len(mock_rowcount.__get__.mock_calls), 0)

                eq_(
                    conn.execute(t.select()).fetchall(),
                    [("d1",), ("d2",), ("d3",)],
                )
                eq_(len(mock_rowcount.__get__.mock_calls), 0)

                conn.execute(t.update(), {"data": "d4"})

                eq_(len(mock_rowcount.__get__.mock_calls), 1)

                conn.execute(t.delete())
                eq_(len(mock_rowcount.__get__.mock_calls), 2)

    def test_row_is_sequence(self):

        row = Row(
            object(),
            [None],
            {"key": (None, 0), 0: (None, 0)},
            Row._default_key_style,
            ["value"],
        )
        is_true(isinstance(row, collections_abc.Sequence))

    @testing.combinations((Row,), (LegacyRow,))
    def test_row_special_names(self, row_cls):
        metadata = SimpleResultMetaData(["key", "count", "index", "foo"])
        row = row_cls(
            metadata,
            [None, None, None, None],
            metadata._keymap,
            row_cls._default_key_style,
            ["kv", "cv", "iv", "f"],
        )
        is_true(isinstance(row, collections_abc.Sequence))

        eq_(row.key, "kv")
        eq_(row.count, "cv")
        eq_(row.index, "iv")

        with assertions.expect_deprecated_20(
            "Retrieving row members using strings or other non-integers "
            "is deprecated; use row._mapping for a dictionary interface "
            "to the row"
        ):
            eq_(row["foo"], "f")
            eq_(row["count"], "cv")
            eq_(row["index"], "iv")

        eq_(row._mapping["count"], "cv")
        eq_(row._mapping["index"], "iv")

        metadata = SimpleResultMetaData(["key", "q", "p"])

        row = row_cls(
            metadata,
            [None, None, None],
            metadata._keymap,
            Row._default_key_style,
            ["kv", "cv", "iv"],
        )
        is_true(isinstance(row, collections_abc.Sequence))

        eq_(row.key, "kv")
        eq_(row.q, "cv")
        eq_(row.p, "iv")
        eq_(row.index("cv"), 1)
        eq_(row.count("cv"), 1)
        eq_(row.count("x"), 0)

    @testing.combinations((Row,), (LegacyRow,))
    def test_row_dict_behaviors_warn_mode(self, row_cls):
        metadata = SimpleResultMetaData(
            [
                "a",
                "b",
                "count",
            ]
        )
        row = row_cls(
            metadata,
            [None, None, None],
            metadata._keymap,
            KEY_OBJECTS_BUT_WARN,
            ["av", "bv", "cv"],
        )

        # as of #6218, dict(row) and row["x"] work for
        # both LegacyRow and Row, with 2.0 deprecation warnings
        # for both
        with assertions.expect_deprecated_20(
            "Retrieving row members using strings or other non-integers "
            "is deprecated; use row._mapping for a dictionary interface "
            "to the row"
        ):
            eq_(dict(row), {"a": "av", "b": "bv", "count": "cv"})

        with assertions.expect_deprecated_20(
            "Retrieving row members using strings or other non-integers "
            "is deprecated; use row._mapping for a dictionary interface "
            "to the row"
        ):
            eq_(row["a"], "av")
            eq_(row["count"], "cv")

        # keys is keys
        with assertions.expect_deprecated_20("The Row.keys()"):
            eq_(list(row.keys()), ["a", "b", "count"])

    def test_new_row_no_dict_behaviors(self):
        """This mode is not used currently but will be once we are in 2.0."""
        metadata = SimpleResultMetaData(
            [
                "a",
                "b",
                "count",
            ]
        )
        row = Row(
            metadata,
            [None, None, None],
            metadata._keymap,
            KEY_INTEGER_ONLY,
            ["av", "bv", "cv"],
        )

        with assertions.expect_raises_message(
            TypeError,
            "TypeError: tuple indices must be integers or slices, not str",
        ):
            with assertions.expect_deprecated_20("The Row.keys()"):
                eq_(dict(row), {"a": "av", "b": "bv", "count": "cv"})

        with assertions.expect_raises_message(
            TypeError,
            "TypeError: tuple indices must be integers or slices, not str",
        ):
            eq_(row["a"], "av")

        with assertions.expect_raises_message(
            TypeError,
            "TypeError: tuple indices must be integers or slices, not str",
        ):
            eq_(row["count"], "cv")

        # keys is keys
        with assertions.expect_deprecated_20("The Row.keys()"):
            eq_(list(row.keys()), ["a", "b", "count"])

    def test_row_is_hashable(self):

        row = Row(
            object(),
            [None, None, None],
            {"key": (None, 0), 0: (None, 0)},
            Row._default_key_style,
            (1, "value", "foo"),
        )
        eq_(hash(row), hash((1, "value", "foo")))

    @testing.provide_metadata
    def test_row_getitem_indexes_compiled(self, connection):
        values = Table(
            "rp",
            self.metadata,
            Column("key", String(10), primary_key=True),
            Column("value", String(10)),
        )
        values.create(connection)

        connection.execute(values.insert(), dict(key="One", value="Uno"))
        row = connection.execute(values.select()).first()
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
    @testing.provide_metadata
    def test_row_c_sequence_check(self, connection):
        users = self.tables.users2

        connection.execute(users.insert(), dict(user_id=1, user_name="Test"))
        row = connection.execute(
            users.select().where(users.c.user_id == 1)
        ).fetchone()

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
                select(1),
                [
                    lambda r: r.last_inserted_params(),
                    lambda r: r.inserted_primary_key,
                ],
                r"Statement is not an insert\(\) expression construct.",
            ),
            (
                select(1),
                [lambda r: r.last_updated_params()],
                r"Statement is not an update\(\) expression construct.",
            ),
            (
                select(1),
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

    @testing.requires.dbapi_lastrowid
    def test_lastrowid(self, connection):
        users = self.tables.users

        r = connection.execute(
            users.insert(), dict(user_id=1, user_name="Test")
        )
        eq_(r.lastrowid, r.context.get_lastrowid())

    def test_raise_errors(self, connection):
        users = self.tables.users

        class Wrapper:
            def __init__(self, context):
                self.context = context

            def __getattr__(self, name):
                if name in ("rowcount", "get_lastrowid"):
                    raise Exception("canary")
                return getattr(self.context, name)

        r = connection.execute(
            users.insert(), dict(user_id=1, user_name="Test")
        )
        r.context = Wrapper(r.context)
        with expect_raises_message(Exception, "canary"):
            r.rowcount
        with expect_raises_message(Exception, "canary"):
            r.lastrowid


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

        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("team_id", metadata, ForeignKey("teams.id")),
        )
        Table(
            "teams",
            metadata,
            Column("id", Integer, primary_key=True),
        )

    @classmethod
    def insert_data(cls, connection):
        conn = connection
        conn.execute(cls.tables.keyed1.insert(), dict(b="a1", q="c1"))
        conn.execute(cls.tables.keyed2.insert(), dict(a="a2", b="b2"))
        conn.execute(cls.tables.keyed3.insert(), dict(a="a3", d="d3"))
        conn.execute(cls.tables.keyed4.insert(), dict(b="b4", q="q4"))
        conn.execute(cls.tables.content.insert(), dict(type="t1"))

        conn.execute(cls.tables.teams.insert(), dict(id=1))
        conn.execute(cls.tables.users.insert(), dict(id=1, team_id=1))

        if testing.requires.schemas.enabled:
            conn.execute(
                cls.tables["%s.wschema" % testing.config.test_schema].insert(),
                dict(b="a1", q="c1"),
            )

    @testing.requires.schemas
    def test_keyed_accessor_wschema(self, connection):
        keyed1 = self.tables["%s.wschema" % testing.config.test_schema]
        row = connection.execute(keyed1.select()).first()

        eq_(row.b, "a1")
        eq_(row.q, "c1")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    def test_keyed_accessor_single(self, connection):
        keyed1 = self.tables.keyed1
        row = connection.execute(keyed1.select()).first()

        eq_(row.b, "a1")
        eq_(row.q, "c1")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    def test_keyed_accessor_single_labeled(self, connection):
        keyed1 = self.tables.keyed1
        row = connection.execute(
            keyed1.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        ).first()

        eq_(row.keyed1_b, "a1")
        eq_(row.keyed1_q, "c1")
        eq_(row.keyed1_a, "a1")
        eq_(row.keyed1_c, "c1")

    def _test_keyed_targeting_no_label_at_all(self, expression, conn):
        lt = literal_column("2")
        stmt = select(literal_column("1"), expression, lt).select_from(
            self.tables.keyed1
        )
        row = conn.execute(stmt).first()

        eq_(row._mapping[expression], "a1")
        eq_(row._mapping[lt], 2)

        # Postgresql for example has the key as "?column?", which dupes
        # easily.  we get around that because we know that "2" is unique
        eq_(row._mapping["2"], 2)

    def test_keyed_targeting_no_label_at_all_one(self, connection):
        class not_named_max(expression.ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # explicit add
            kw["add_to_result_map"](None, None, (element,), NULLTYPE)
            return "max(a)"

        # assert that there is no "AS max_" or any label of any kind.
        eq_(str(select(not_named_max())), "SELECT max(a)")

        nnm = not_named_max()
        self._test_keyed_targeting_no_label_at_all(nnm, connection)

    def test_keyed_targeting_no_label_at_all_two(self, connection):
        class not_named_max(expression.ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # we don't add to keymap here; compiler should be doing it
            return "max(a)"

        # assert that there is no "AS max_" or any label of any kind.
        eq_(str(select(not_named_max())), "SELECT max(a)")

        nnm = not_named_max()
        self._test_keyed_targeting_no_label_at_all(nnm, connection)

    def test_keyed_targeting_no_label_at_all_text(self, connection):
        t1 = text("max(a)")
        t2 = text("min(a)")

        stmt = select(t1, t2).select_from(self.tables.keyed1)
        row = connection.execute(stmt).first()

        eq_(row._mapping[t1], "a1")
        eq_(row._mapping[t2], "a1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_conflict_2(self, connection):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = connection.execute(
            select(keyed1, keyed2)
            .select_from(keyed1.join(keyed2, true()))
            .set_label_style(LABEL_STYLE_NONE)
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
        row = connection.execute(select(keyed1)).first()
        eq_(row.b, "a1")

    def test_keyed_accessor_composite_conflict_2_fix_w_uselabels(
        self, connection
    ):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = connection.execute(
            select(keyed1, keyed2)
            .select_from(keyed1.join(keyed2, true()))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        ).first()

        # column access is unambiguous
        eq_(row._mapping[self.tables.keyed2.c.b], "b2")

        eq_(row._mapping["keyed2_b"], "b2")
        eq_(row._mapping["keyed1_a"], "a1")

    def test_keyed_accessor_composite_names_precedent(self, connection):
        keyed1 = self.tables.keyed1
        keyed4 = self.tables.keyed4

        row = connection.execute(
            select(keyed1, keyed4).select_from(keyed1.join(keyed4, true()))
        ).first()
        eq_(row.b, "b4")
        eq_(row.q, "q4")
        eq_(row.a, "a1")
        eq_(row.c, "c1")

    @testing.requires.duplicate_names_in_cursor_description
    def test_keyed_accessor_composite_keys_precedent(self, connection):
        keyed1 = self.tables.keyed1
        keyed3 = self.tables.keyed3

        row = connection.execute(
            select(keyed1, keyed3)
            .select_from(keyed1.join(keyed3, true()))
            .set_label_style(LABEL_STYLE_NONE)
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

    def test_keyed_accessor_composite_labeled(self, connection):
        keyed1 = self.tables.keyed1
        keyed2 = self.tables.keyed2

        row = connection.execute(
            select(keyed1, keyed2)
            .select_from(keyed1.join(keyed2, true()))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
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

    def test_keyed_accessor_column_is_repeated_multiple_times(
        self, connection
    ):
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
                keyed2.c.a,
                keyed3.c.a,
                keyed2.c.a,
                keyed2.c.a,
                keyed3.c.a,
                keyed3.c.a,
                keyed3.c.d,
                keyed3.c.d,
            )
            .select_from(keyed2.join(keyed3, true()))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        )

        result = connection.execute(stmt)

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

    def test_columnclause_schema_column_one(self, connection):
        # originally addressed by [ticket:2932], however liberalized
        # Column-targeting rules are deprecated
        a, b = sql.column("a"), sql.column("b")
        stmt = select(a, b).select_from(table("keyed2"))
        row = connection.execute(stmt).first()

        in_(a, row._mapping)
        in_(b, row._mapping)

    def test_columnclause_schema_column_two(self, connection):
        keyed2 = self.tables.keyed2

        stmt = select(keyed2.c.a, keyed2.c.b)
        row = connection.execute(stmt).first()

        in_(keyed2.c.a, row._mapping)
        in_(keyed2.c.b, row._mapping)

    def test_columnclause_schema_column_three(self, connection):
        # this is also addressed by [ticket:2932]
        stmt = text("select a, b from keyed2").columns(a=CHAR, b=CHAR)
        row = connection.execute(stmt).first()

        in_(stmt.selected_columns.a, row._mapping)
        in_(stmt.selected_columns.b, row._mapping)

    def test_columnclause_schema_column_four(self, connection):
        # originally addressed by [ticket:2932], however liberalized
        # Column-targeting rules are deprecated

        a, b = sql.column("keyed2_a"), sql.column("keyed2_b")
        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            a, b
        )
        row = connection.execute(stmt).first()

        in_(a, row._mapping)
        in_(b, row._mapping)

        in_(stmt.selected_columns.keyed2_a, row._mapping)
        in_(stmt.selected_columns.keyed2_b, row._mapping)

    def test_columnclause_schema_column_five(self, connection):
        # this is also addressed by [ticket:2932]

        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_a=CHAR, keyed2_b=CHAR
        )
        row = connection.execute(stmt).first()

        in_(stmt.selected_columns.keyed2_a, row._mapping)
        in_(stmt.selected_columns.keyed2_b, row._mapping)

    def _adapt_result_columns_fixture_one(self):
        keyed1 = self.tables.keyed1
        stmt = (
            select(keyed1.c.b, keyed1.c.q.label("foo"))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        return select(stmt.c.keyed1_b, stmt.c.foo)

    def _adapt_result_columns_fixture_two(self):
        return text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            column("keyed2_a", CHAR), column("keyed2_b", CHAR)
        )

    def _adapt_result_columns_fixture_three(self):
        keyed1 = self.tables.keyed1
        stmt = select(keyed1.c.b, keyed1.c.q.label("foo")).subquery()

        return select(stmt.c.b, stmt.c.foo)

    def _adapt_result_columns_fixture_four(self):
        keyed1 = self.tables.keyed1

        stmt1 = select(keyed1).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        a1 = keyed1.alias()
        stmt2 = ClauseAdapter(a1).traverse(stmt1)

        return stmt2

    def _adapt_result_columns_fixture_five(self):
        users, teams = self.tables("users", "teams")
        return select([users.c.id, teams.c.id]).select_from(
            users.outerjoin(teams)
        )

    def _adapt_result_columns_fixture_six(self):
        # this has _result_columns structure that is not ordered
        # the same as the cursor.description.
        return text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_b=CHAR,
            keyed2_a=CHAR,
        )

    def _adapt_result_columns_fixture_seven(self):
        # this has _result_columns structure that is not ordered
        # the same as the cursor.description.
        return text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_b=CHAR, bogus_col=CHAR
        )

    @testing.combinations(
        _adapt_result_columns_fixture_one,
        _adapt_result_columns_fixture_two,
        _adapt_result_columns_fixture_three,
        _adapt_result_columns_fixture_four,
        _adapt_result_columns_fixture_five,
        _adapt_result_columns_fixture_six,
        _adapt_result_columns_fixture_seven,
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

        for i in range(2):
            try:
                result = connection.execute(stmt1)

                mock_context = Mock(
                    compiled=result.context.compiled, invoked_statement=stmt2
                )
                existing_metadata = result._metadata
                adapted_metadata = existing_metadata._adapt_to_context(
                    mock_context
                )

                eq_(existing_metadata.keys, adapted_metadata.keys)

                for k in existing_metadata._keymap:
                    if isinstance(k, ColumnElement) and k in column_linkage:
                        other_k = column_linkage[k]
                    else:
                        other_k = k

                    is_(
                        existing_metadata._keymap[k],
                        adapted_metadata._keymap[other_k],
                    )
            finally:
                result.close()

    @testing.combinations(
        _adapt_result_columns_fixture_one,
        _adapt_result_columns_fixture_two,
        _adapt_result_columns_fixture_three,
        _adapt_result_columns_fixture_four,
        _adapt_result_columns_fixture_five,
        _adapt_result_columns_fixture_six,
        _adapt_result_columns_fixture_seven,
        argnames="stmt_fn",
    )
    def test_adapt_result_columns_from_cache(self, connection, stmt_fn):
        stmt1 = stmt_fn(self)
        stmt2 = stmt_fn(self)

        cache = {}
        result = connection._execute_20(
            stmt1,
            execution_options={"compiled_cache": cache, "future_result": True},
        )
        result.close()
        assert cache

        result = connection._execute_20(
            stmt2,
            execution_options={"compiled_cache": cache, "future_result": True},
        )

        row = result.first()
        for col in stmt2.selected_columns:
            if "bogus" in col.name:
                assert col not in row._mapping
            else:
                assert col in row._mapping


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
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.text1.insert(),
            [dict(a="a1", b="b1", c="c1", d="d1")],
        )

    def test_via_column(self, connection):
        c1, c2, c3, c4 = column("q"), column("p"), column("r"), column("d")
        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)

        result = connection.execute(stmt)
        row = result.first()

        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c4], "d1")
        eq_(row[1], "b1")
        eq_(row._mapping["b"], "b1")
        eq_(list(row._mapping.keys()), ["a", "b", "c", "d"])
        eq_(row._fields, ("a", "b", "c", "d"))
        eq_(row._mapping["r"], "c1")
        eq_(row._mapping["d"], "d1")

    def test_fewer_cols_than_sql_positional(self, connection):
        c1, c2 = column("q"), column("p")
        stmt = text("select a, b, c, d from text1").columns(c1, c2)

        # no warning as this can be similar for non-positional
        result = connection.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping["c"], "c1")

    def test_fewer_cols_than_sql_non_positional(self, connection):
        c1, c2 = column("a"), column("p")
        stmt = text("select a, b, c, d from text1").columns(c2, c1, d=CHAR)

        # no warning as this can be similar for non-positional
        result = connection.execute(stmt)
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

    def test_more_cols_than_sql_positional(self, connection):
        c1, c2, c3, c4 = column("q"), column("p"), column("r"), column("d")
        stmt = text("select a, b from text1").columns(c1, c2, c3, c4)

        with assertions.expect_warnings(
            r"Number of columns in textual SQL \(4\) is "
            r"smaller than number of columns requested \(2\)"
        ):
            result = connection.execute(stmt)

        row = result.first()
        eq_(row._mapping[c2], "b1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_more_cols_than_sql_nonpositional(self, connection):
        c1, c2, c3, c4 = column("b"), column("a"), column("r"), column("d")
        stmt = TextualSelect(
            text("select a, b from text1"), [c1, c2, c3, c4], positional=False
        )

        # no warning for non-positional
        result = connection.execute(stmt)

        row = result.first()
        eq_(row._mapping[c1], "b1")
        eq_(row._mapping[c2], "a1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_more_cols_than_sql_nonpositional_labeled_cols(self, connection):
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
        result = connection.execute(stmt)

        row = result.first()
        eq_(row._mapping[c1], "b1")
        eq_(row._mapping[c2], "a1")

        assert_raises_message(
            exc.NoSuchColumnError,
            "in row for column 'r'",
            lambda: row._mapping[c3],
        )

    def test_dupe_col_obj(self, connection):
        c1, c2, c3 = column("q"), column("p"), column("r")
        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c2)

        assert_raises_message(
            exc.InvalidRequestError,
            "Duplicate column expression requested in "
            "textual SQL: <.*.ColumnClause.*; p>",
            connection.execute,
            stmt,
        )

    def test_anon_aliased_unique(self, connection):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.c
        c3 = text1.alias().c.b
        c4 = text1.alias().c.d.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = connection.execute(stmt)
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

    def test_anon_aliased_overlapping(self, connection):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.a
        c3 = text1.alias().c.a.label(None)
        c4 = text1.c.a.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = connection.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c3], "c1")
        eq_(row._mapping[c4], "d1")

    def test_anon_aliased_name_conflict(self, connection):
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
        result = connection.execute(stmt)
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


class AlternateCursorResultTest(fixtures.TablesTest):
    __requires__ = ("sqlite",)

    @classmethod
    def setup_bind(cls):
        cls.engine = engine = engines.testing_engine(
            "sqlite://", options={"scope": "class"}
        )
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
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.test.insert(),
            [{"x": i, "y": "t_%d" % i} for i in range(1, 12)],
        )

    @contextmanager
    def _proxy_fixture(self, cls):
        self.table = self.tables.test

        class ExcCtx(default.DefaultExecutionContext):
            def post_exec(self):
                if cls is _cursor.CursorFetchStrategy:
                    pass
                elif cls is _cursor.BufferedRowCursorFetchStrategy:
                    self.cursor_fetch_strategy = cls(
                        self.cursor, self.execution_options
                    )
                elif cls is _cursor.FullyBufferedCursorFetchStrategy:
                    self.cursor_fetch_strategy = cls(
                        self.cursor,
                        self.cursor.description,
                        self.cursor.fetchall(),
                    )
                else:
                    assert False

        self.patcher = patch.object(
            self.engine.dialect, "execution_ctx_cls", ExcCtx
        )

        with self.patcher:
            yield

    def _test_proxy(self, cls):
        with self._proxy_fixture(cls):
            rows = []
            with self.engine.connect() as conn:
                r = conn.execute(select(self.table))
                assert isinstance(r.cursor_strategy, cls)
                for i in range(5):
                    rows.append(r.fetchone())
                eq_(rows, [(i, "t_%d" % i) for i in range(1, 6)])

                rows = r.fetchmany(3)
                eq_(rows, [(i, "t_%d" % i) for i in range(6, 9)])

                rows = r.fetchall()
                eq_(rows, [(i, "t_%d" % i) for i in range(9, 12)])

                r = conn.execute(select(self.table))
                rows = r.fetchmany(None)
                eq_(rows[0], (1, "t_1"))
                # number of rows here could be one, or the whole thing
                assert len(rows) == 1 or len(rows) == 11

                r = conn.execute(select(self.table).limit(1))
                r.fetchone()
                eq_(r.fetchone(), None)

                r = conn.execute(select(self.table).limit(5))
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

                r = conn.execute(select(self.table).limit(5))
                eq_(r.first(), (1, "t_1"))
                self._assert_result_closed(r)

                r = conn.execute(select(self.table).limit(5))
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
        self._test_proxy(_cursor.CursorFetchStrategy)

    def test_basic_buffered_row_result_proxy(self):
        self._test_proxy(_cursor.BufferedRowCursorFetchStrategy)

    def test_basic_fully_buffered_result_proxy(self):
        self._test_proxy(_cursor.FullyBufferedCursorFetchStrategy)

    def test_basic_buffered_column_result_proxy(self):
        self._test_proxy(_cursor.CursorFetchStrategy)

    def test_resultprocessor_plain(self):
        self._test_result_processor(_cursor.CursorFetchStrategy, False)

    def test_resultprocessor_plain_cached(self):
        self._test_result_processor(_cursor.CursorFetchStrategy, True)

    def test_resultprocessor_buffered_row(self):
        self._test_result_processor(
            _cursor.BufferedRowCursorFetchStrategy, False
        )

    def test_resultprocessor_buffered_row_cached(self):
        self._test_result_processor(
            _cursor.BufferedRowCursorFetchStrategy, True
        )

    def test_resultprocessor_fully_buffered(self):
        self._test_result_processor(
            _cursor.FullyBufferedCursorFetchStrategy, False
        )

    def test_resultprocessor_fully_buffered_cached(self):
        self._test_result_processor(
            _cursor.FullyBufferedCursorFetchStrategy, True
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

                stmt = select(literal("THERE", type_=MyType()))
                for i in range(2):
                    r = conn.execute(stmt)
                    eq_(r.scalar(), "HI THERE")

    @testing.fixture
    def row_growth_fixture(self):
        with self._proxy_fixture(_cursor.BufferedRowCursorFetchStrategy):
            with self.engine.begin() as conn:
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

    def test_buffered_fetchmany_fixed(self, row_growth_fixture):
        """The BufferedRow cursor strategy will defer to the fetchmany
        size passed when given rather than using the buffer growth
        heuristic.

        """
        result = row_growth_fixture.execute(self.table.select())
        eq_(len(result.cursor_strategy._rowbuffer), 1)

        rows = result.fetchmany(300)
        eq_(len(rows), 300)
        eq_(len(result.cursor_strategy._rowbuffer), 0)

        rows = result.fetchmany(300)
        eq_(len(rows), 300)
        eq_(len(result.cursor_strategy._rowbuffer), 0)

        bufsize = result.cursor_strategy._bufsize
        result.fetchone()

        # the fetchone() caused it to buffer a full set of rows
        eq_(len(result.cursor_strategy._rowbuffer), bufsize - 1)

        # assert partitions uses fetchmany(), therefore controlling
        # how the buffer is used
        lens = []
        for partition in result.partitions(180):
            lens.append(len(partition))
            eq_(len(result.cursor_strategy._rowbuffer), 0)

        for lp in lens[0:-1]:
            eq_(lp, 180)

    def test_buffered_fetchmany_yield_per(self, connection):
        table = self.tables.test

        connection.execute(
            table.insert(),
            [{"x": i, "y": "t_%d" % i} for i in range(15, 3000)],
        )

        result = connection.execute(table.select())
        assert isinstance(result.cursor_strategy, _cursor.CursorFetchStrategy)

        result.fetchmany(5)

        result = result.yield_per(100)
        assert isinstance(
            result.cursor_strategy, _cursor.BufferedRowCursorFetchStrategy
        )
        eq_(result.cursor_strategy._bufsize, 100)
        eq_(result.cursor_strategy._growth_factor, 0)
        eq_(len(result.cursor_strategy._rowbuffer), 0)

        result.fetchone()
        eq_(len(result.cursor_strategy._rowbuffer), 99)

        for i, row in enumerate(result):
            if i == 188:
                break

        # buffer of 98, plus buffer of 99 - 89, 10 rows
        eq_(len(result.cursor_strategy._rowbuffer), 10)

    def test_buffered_fetchmany_yield_per_all(self, connection):
        table = self.tables.test

        connection.execute(
            table.insert(),
            [{"x": i, "y": "t_%d" % i} for i in range(15, 500)],
        )

        result = connection.execute(table.select())
        assert isinstance(result.cursor_strategy, _cursor.CursorFetchStrategy)

        result.fetchmany(5)

        result = result.yield_per(0)
        assert isinstance(
            result.cursor_strategy, _cursor.BufferedRowCursorFetchStrategy
        )
        eq_(result.cursor_strategy._bufsize, 0)
        eq_(result.cursor_strategy._growth_factor, 0)
        eq_(len(result.cursor_strategy._rowbuffer), 0)

        result.fetchone()
        eq_(len(result.cursor_strategy._rowbuffer), 490)

        for i, row in enumerate(result):
            if i == 188:
                break

        eq_(len(result.cursor_strategy._rowbuffer), 301)

        # already buffered, so this doesn't change things
        result.yield_per(10)

        result.fetchmany(5)
        eq_(len(result.cursor_strategy._rowbuffer), 296)

        self._test_result_processor(
            _cursor.BufferedRowCursorFetchStrategy, False
        )

    @testing.combinations(
        _cursor.CursorFetchStrategy,
        _cursor.BufferedRowCursorFetchStrategy,
        # does not handle error in fetch
        # _cursor.FullyBufferedCursorFetchStrategy,
        argnames="strategy_cls",
    )
    @testing.combinations(
        "fetchone",
        "fetchmany",
        "fetchmany_w_num",
        "fetchall",
        argnames="method_name",
    )
    def test_handle_error_in_fetch(self, strategy_cls, method_name):
        class cursor(object):
            def raise_(self):
                raise IOError("random non-DBAPI error during cursor operation")

            def fetchone(self):
                self.raise_()

            def fetchmany(self, num=None):
                self.raise_()

            def fetchall(self):
                self.raise_()

            def close(self):
                self.raise_()

        with self._proxy_fixture(strategy_cls):
            with self.engine.connect() as conn:
                r = conn.execute(select(self.table))
                assert isinstance(r.cursor_strategy, strategy_cls)
                with mock.patch.object(r, "cursor", cursor()):

                    with testing.expect_raises_message(
                        IOError, "random non-DBAPI"
                    ):
                        if method_name == "fetchmany_w_num":
                            r.fetchmany(10)
                        else:
                            getattr(r, method_name)()
                            getattr(r, method_name)()

                r.close()

    def test_buffered_row_close_error_during_fetchone(self):
        def raise_(**kw):
            raise IOError("random non-DBAPI error during cursor operation")

        with self._proxy_fixture(_cursor.BufferedRowCursorFetchStrategy):
            with self.engine.connect() as conn:
                r = conn.execute(select(self.table).limit(1))

                r.fetchone()
                with mock.patch.object(
                    r, "_soft_close", raise_
                ), testing.expect_raises_message(IOError, "random non-DBAPI"):
                    r.first()
                r.close()


class MergeCursorResultTest(fixtures.TablesTest):
    __backend__ = True

    __requires__ = ("independent_cursors",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True, autoincrement=False),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "u1"},
                {"user_id": 8, "user_name": "u2"},
                {"user_id": 9, "user_name": "u3"},
                {"user_id": 10, "user_name": "u4"},
                {"user_id": 11, "user_name": "u5"},
                {"user_id": 12, "user_name": "u6"},
            ],
        )

    @testing.fixture
    def merge_fixture(self):
        users = self.tables.users

        def results(connection):

            r1 = connection.execute(
                users.select()
                .where(users.c.user_id.in_([7, 8]))
                .order_by(users.c.user_id)
            )
            r2 = connection.execute(
                users.select()
                .where(users.c.user_id.in_([9]))
                .order_by(users.c.user_id)
            )
            r3 = connection.execute(
                users.select()
                .where(users.c.user_id.in_([10, 11]))
                .order_by(users.c.user_id)
            )
            r4 = connection.execute(
                users.select()
                .where(users.c.user_id.in_([12]))
                .order_by(users.c.user_id)
            )
            return r1, r2, r3, r4

        return results

    def test_merge_results(self, connection, merge_fixture):
        r1, r2, r3, r4 = merge_fixture(connection)

        result = r1.merge(r2, r3, r4)

        eq_(result.keys(), ["user_id", "user_name"])
        row = result.fetchone()
        eq_(row, (7, "u1"))
        result.close()

    def test_close(self, connection, merge_fixture):
        r1, r2, r3, r4 = merge_fixture(connection)

        result = r1.merge(r2, r3, r4)

        for r in [result, r1, r2, r3, r4]:
            assert not r.closed

        result.close()
        for r in [result, r1, r2, r3, r4]:
            assert r.closed

    def test_fetchall(self, connection, merge_fixture):
        r1, r2, r3, r4 = merge_fixture(connection)

        result = r1.merge(r2, r3, r4)
        eq_(
            result.fetchall(),
            [
                (7, "u1"),
                (8, "u2"),
                (9, "u3"),
                (10, "u4"),
                (11, "u5"),
                (12, "u6"),
            ],
        )
        for r in [r1, r2, r3, r4]:
            assert r._soft_closed

    def test_first(self, connection, merge_fixture):
        r1, r2, r3, r4 = merge_fixture(connection)

        result = r1.merge(r2, r3, r4)
        eq_(
            result.first(),
            (7, "u1"),
        )
        for r in [r1, r2, r3, r4]:
            assert r.closed

    def test_columns(self, connection, merge_fixture):
        r1, r2, r3, r4 = merge_fixture(connection)

        result = r1.merge(r2, r3, r4)
        eq_(
            result.columns("user_name").fetchmany(4),
            [("u1",), ("u2",), ("u3",), ("u4",)],
        )
        result.close()


class GenerativeResultTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True, autoincrement=False),
            Column("user_name", VARCHAR(20)),
            Column("x", Integer),
            Column("y", Integer),
            test_needs_acid=True,
        )
        Table(
            "users_autoinc",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    def test_fetchall(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack", "x": 1, "y": 2},
                {"user_id": 8, "user_name": "ed", "x": 2, "y": 3},
                {"user_id": 9, "user_name": "fred", "x": 15, "y": 20},
            ],
        )

        result = connection.execute(select(users).order_by(users.c.user_id))
        eq_(
            result.all(),
            [(7, "jack", 1, 2), (8, "ed", 2, 3), (9, "fred", 15, 20)],
        )

    @testing.combinations(
        ((1, 0), [("jack", 7), ("ed", 8), ("fred", 9)]),
        ((3,), [(2,), (3,), (20,)]),
        ((-2, -1), [(1, 2), (2, 3), (15, 20)]),
        argnames="columns, expected",
    )
    def test_columns(self, connection, columns, expected):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack", "x": 1, "y": 2},
                {"user_id": 8, "user_name": "ed", "x": 2, "y": 3},
                {"user_id": 9, "user_name": "fred", "x": 15, "y": 20},
            ],
        )

        result = connection.execute(select(users).order_by(users.c.user_id))

        all_ = result.columns(*columns).all()
        eq_(all_, expected)

        # ensure Row / LegacyRow comes out with .columns
        assert type(all_[0]) is result._process_row

    def test_columns_twice(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [{"user_id": 7, "user_name": "jack", "x": 1, "y": 2}],
        )

        result = connection.execute(select(users).order_by(users.c.user_id))

        all_ = (
            result.columns("x", "y", "user_name", "user_id")
            .columns("user_name", "x")
            .all()
        )
        eq_(all_, [("jack", 1)])

        # ensure Row / LegacyRow comes out with .columns
        assert type(all_[0]) is result._process_row

    def test_columns_plus_getter(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [{"user_id": 7, "user_name": "jack", "x": 1, "y": 2}],
        )

        result = connection.execute(select(users).order_by(users.c.user_id))

        result = result.columns("x", "y", "user_name")
        getter = result._metadata._getter("y")

        eq_(getter(result.first()), 2)

    def test_partitions(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {
                    "user_id": i,
                    "user_name": "user %s" % i,
                    "x": i * 5,
                    "y": i * 20,
                }
                for i in range(500)
            ],
        )

        result = connection.execute(select(users).order_by(users.c.user_id))

        start = 0
        for partition in result.columns(0, 1).partitions(20):
            eq_(
                partition,
                [(i, "user %s" % i) for i in range(start, start + 20)],
            )
            start += 20

        assert result._soft_closed
