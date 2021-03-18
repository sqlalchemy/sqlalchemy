import contextlib
import functools

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import between
from sqlalchemy import bindparam
from sqlalchemy import Boolean
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import collate
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import LABEL_STYLE_DISAMBIGUATE_ONLY
from sqlalchemy import LABEL_STYLE_NONE
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import type_coerce
from sqlalchemy import Unicode
from sqlalchemy import union
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defer
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm.context import QueryContext
from sqlalchemy.orm.util import join
from sqlalchemy.orm.util import with_parent
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.assertions import is_not_none
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import collections_abc
from test.orm import _fixtures


class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()


class MiscTest(QueryTest):
    run_create_tables = None
    run_inserts = None

    def test_with_session(self):
        User = self.classes.User
        s1 = fixture_session()
        s2 = fixture_session()
        q1 = s1.query(User)
        q2 = q1.with_session(s2)
        assert q2.session is s2
        assert q1.session is s1

    @testing.combinations(
        (lambda s, User: s.query(User)),
        (lambda s, User: s.query(User).filter_by(name="x")),
        (lambda s, User: s.query(User.id, User.name).filter_by(name="x")),
        (
            lambda s, User: s.query(func.count(User.id)).filter(
                User.name == "x"
            )
        ),
    )
    def test_rudimentary_statement_accessors(self, test_case):
        User = self.classes.User

        s = fixture_session()

        q1 = testing.resolve_lambda(test_case, s=s, User=User)

        is_true(
            q1.statement.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).compare(q1.__clause_element__())
        )

        is_true(
            q1.statement.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).compare(q1.selectable)
        )


class OnlyReturnTuplesTest(QueryTest):
    def test_single_entity_false(self):
        User = self.classes.User
        query = fixture_session().query(User).only_return_tuples(False)
        is_true(query.is_single_entity)
        row = query.first()
        assert isinstance(row, User)

    def test_single_entity_true(self):
        User = self.classes.User
        query = fixture_session().query(User).only_return_tuples(True)
        is_false(query.is_single_entity)
        row = query.first()
        assert isinstance(row, collections_abc.Sequence)
        assert isinstance(row._mapping, collections_abc.Mapping)

    def test_multiple_entity_false(self):
        User = self.classes.User
        query = (
            fixture_session().query(User.id, User).only_return_tuples(False)
        )
        is_false(query.is_single_entity)
        row = query.first()
        assert isinstance(row, collections_abc.Sequence)
        assert isinstance(row._mapping, collections_abc.Mapping)

    def test_multiple_entity_true(self):
        User = self.classes.User
        query = fixture_session().query(User.id, User).only_return_tuples(True)
        is_false(query.is_single_entity)
        row = query.first()
        assert isinstance(row, collections_abc.Sequence)
        assert isinstance(row._mapping, collections_abc.Mapping)


class RowTupleTest(QueryTest):
    run_setup_mappers = None

    @testing.combinations((True,), (False,), argnames="legacy")
    @testing.combinations((True,), (False,), argnames="use_subquery")
    @testing.combinations((True,), (False,), argnames="set_column_key")
    def test_custom_names(self, legacy, use_subquery, set_column_key):
        """Test labeling as used with ORM attributes named differently from
        the column.

        Compare to the tests in RowLabelingTest which tests this also,
        this test is more oriented towards legacy Query use.

        """
        User, users = self.classes.User, self.tables.users

        if set_column_key:
            uwkey = Table(
                "users",
                MetaData(),
                Column("id", Integer, primary_key=True),
                Column("name", String, key="uname"),
            )
            mapper(User, uwkey)
        else:
            mapper(User, users, properties={"uname": users.c.name})

        s = fixture_session()
        if legacy:
            q = s.query(User.id, User.uname).filter(User.id == 7)
            if use_subquery:
                q = s.query(q.subquery())
            row = q.first()
        else:
            q = select(User.id, User.uname).filter(User.id == 7)
            if use_subquery:
                q = select(q.subquery())
            row = s.execute(q).first()

        eq_(row.id, 7)

        eq_(row.uname, "jack")

    @testing.combinations(
        (lambda s, users: s.query(users),),
        (lambda s, User: s.query(User.id, User.name),),
        (lambda s, users: s.query(users.c.id, users.c.name),),
        (lambda s, users: s.query(users.c.id, users.c.name),),
    )
    def test_modern_tuple(self, test_case):
        # check we are not getting a LegacyRow back

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = fixture_session()

        q = testing.resolve_lambda(test_case, **locals())

        row = q.order_by(User.id).first()
        assert "jack" in row

    @testing.combinations(
        (lambda s, users: s.query(users),),
        (lambda s, User: s.query(User.id, User.name),),
        (lambda s, users: s.query(users.c.id, users.c.name),),
        (lambda s, users: select(users),),
        (lambda s, User: select(User.id, User.name),),
        (lambda s, users: select(users.c.id, users.c.name),),
    )
    def test_modern_tuple_future(self, test_case):
        # check we are not getting a LegacyRow back

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = fixture_session()

        q = testing.resolve_lambda(test_case, **locals())

        row = s.execute(q.order_by(User.id)).first()
        assert "jack" in row

    @testing.combinations(
        (lambda s, users: select(users),),
        (lambda s, User: select(User.id, User.name),),
        (lambda s, users: select(users.c.id, users.c.name),),
    )
    def test_legacy_tuple_old_select(self, test_case):

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = fixture_session()

        q = testing.resolve_lambda(test_case, **locals())

        row = s.execute(q.order_by(User.id)).first()

        # s.execute() is now new style row
        assert "jack" in row

    def test_entity_mapping_access(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        s = fixture_session()

        row = s.query(User).only_return_tuples(True).first()
        eq_(row._mapping[User], row[0])

        row = s.query(User, Address).join(User.addresses).first()
        eq_(row._mapping[User], row[0])
        eq_(row._mapping[Address], row[1])
        eq_(row._mapping["User"], row[0])
        eq_(row._mapping["Address"], row[1])

        u1 = aliased(User)
        row = s.query(u1).only_return_tuples(True).first()
        eq_(row._mapping[u1], row[0])
        assert_raises(KeyError, lambda: row._mapping[User])

        row = (
            s.query(User.id, Address.email_address)
            .join(User.addresses)
            .first()
        )

        eq_(row._mapping[User.id], row[0])
        eq_(row._mapping[User.id], row[0])
        eq_(row._mapping["id"], row[0])
        eq_(row._mapping[Address.email_address], row[1])
        eq_(row._mapping["email_address"], row[1])
        eq_(row._mapping[users.c.id], row[0])
        eq_(row._mapping[addresses.c.email_address], row[1])
        assert_raises(KeyError, lambda: row._mapping[User.name])
        assert_raises(KeyError, lambda: row._mapping[users.c.name])

    @testing.combinations(
        lambda sess, User: (
            sess.query(User),
            [
                {
                    "name": "User",
                    "type": User,
                    "aliased": False,
                    "expr": User,
                    "entity": User,
                }
            ],
        ),
        lambda sess, User, users: (
            sess.query(User.id, User),
            [
                {
                    "name": "id",
                    "type": users.c.id.type,
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": "User",
                    "type": User,
                    "aliased": False,
                    "expr": User,
                    "entity": User,
                },
            ],
        ),
        lambda sess, User, user_alias, users: (
            sess.query(User.id, user_alias),
            [
                {
                    "name": "id",
                    "type": users.c.id.type,
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": None,
                    "type": User,
                    "aliased": True,
                    "expr": user_alias,
                    "entity": user_alias,
                },
            ],
        ),
        lambda sess, user_alias, users: (
            sess.query(user_alias.id),
            [
                {
                    "name": "id",
                    "type": users.c.id.type,
                    "aliased": True,
                    "expr": user_alias.id,
                    "entity": user_alias,
                }
            ],
        ),
        lambda sess, user_alias_id_label, users, user_alias: (
            sess.query(user_alias_id_label),
            [
                {
                    "name": "foo",
                    "type": users.c.id.type,
                    "aliased": True,
                    "expr": user_alias_id_label,
                    "entity": user_alias,
                }
            ],
        ),
        lambda sess, address_alias, Address: (
            sess.query(address_alias),
            [
                {
                    "name": "aalias",
                    "type": Address,
                    "aliased": True,
                    "expr": address_alias,
                    "entity": address_alias,
                }
            ],
        ),
        lambda sess, name_label, fn, users, User: (
            sess.query(name_label, fn),
            [
                {
                    "name": "uname",
                    "type": users.c.name.type,
                    "aliased": False,
                    "expr": name_label,
                    "entity": User,
                },
                {
                    "name": None,
                    "type": fn.type,
                    "aliased": False,
                    "expr": fn,
                    "entity": User,
                },
            ],
        ),
        lambda sess, cte: (
            sess.query(cte),
            [
                {
                    "aliased": False,
                    "expr": cte.c.id,
                    "type": cte.c.id.type,
                    "name": "id",
                    "entity": None,
                }
            ],
        ),
        lambda sess, subq1: (
            sess.query(subq1.c.id),
            [
                {
                    "aliased": False,
                    "expr": subq1.c.id,
                    "type": subq1.c.id.type,
                    "name": "id",
                    "entity": None,
                }
            ],
        ),
        lambda sess, subq2: (
            sess.query(subq2.c.id),
            [
                {
                    "aliased": False,
                    "expr": subq2.c.id,
                    "type": subq2.c.id.type,
                    "name": "id",
                    "entity": None,
                }
            ],
        ),
        lambda sess, users: (
            sess.query(users),
            [
                {
                    "aliased": False,
                    "expr": users.c.id,
                    "type": users.c.id.type,
                    "name": "id",
                    "entity": None,
                },
                {
                    "aliased": False,
                    "expr": users.c.name,
                    "type": users.c.name.type,
                    "name": "name",
                    "entity": None,
                },
            ],
        ),
        lambda sess, users: (
            sess.query(users.c.name),
            [
                {
                    "name": "name",
                    "type": users.c.name.type,
                    "aliased": False,
                    "expr": users.c.name,
                    "entity": None,
                }
            ],
        ),
        lambda sess, bundle, User: (
            sess.query(bundle),
            [
                {
                    "aliased": False,
                    "expr": bundle,
                    "type": Bundle,
                    "name": "b1",
                    "entity": User,
                }
            ],
        ),
    )
    def test_column_metadata(self, test_case):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses)
        sess = fixture_session()
        user_alias = aliased(User)
        user_alias_id_label = user_alias.id.label("foo")
        address_alias = aliased(Address, name="aalias")
        fn = func.count(User.id)
        name_label = User.name.label("uname")
        bundle = Bundle("b1", User.id, User.name)
        subq1 = sess.query(User.id).subquery()
        subq2 = sess.query(bundle).subquery()
        cte = sess.query(User.id).cte()

        q, asserted = testing.resolve_lambda(test_case, **locals())

        eq_(q.column_descriptions, asserted)

    def test_unhashable_type(self):
        from sqlalchemy.types import TypeDecorator, Integer
        from sqlalchemy.sql import type_coerce

        class MyType(TypeDecorator):
            impl = Integer
            hashable = False

            def process_result_value(self, value, dialect):
                return [value]

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = fixture_session()
        q = s.query(User, type_coerce(users.c.id, MyType).label("foo")).filter(
            User.id == 7
        )
        row = q.first()
        eq_(row, (User(id=7), [7]))


class RowLabelingTest(QueryTest):
    @testing.fixture
    def assert_row_keys(self):
        def go(stmt, expected, coreorm_exec, selected_columns=None):

            if coreorm_exec == "core":
                with testing.db.connect() as conn:
                    row = conn.execute(stmt).first()
            else:
                s = fixture_session()

                row = s.execute(stmt).first()

            eq_(row.keys(), expected)

            if selected_columns is None:
                selected_columns = expected

            # we are disambiguating in exported_columns even if
            # LABEL_STYLE_NONE, this seems weird also
            if (
                stmt._label_style is not LABEL_STYLE_NONE
                and coreorm_exec == "core"
            ):
                eq_(stmt.exported_columns.keys(), list(selected_columns))

            if (
                stmt._label_style is not LABEL_STYLE_NONE
                and coreorm_exec == "orm"
            ):

                for k in expected:
                    is_not_none(getattr(row, k))

                try:
                    column_descriptions = stmt.column_descriptions
                except (NotImplementedError, AttributeError):
                    pass
                else:
                    eq_(
                        [
                            entity["name"]
                            for entity in column_descriptions
                            if entity["name"] is not None
                        ],
                        list(selected_columns),
                    )

        return go

    def test_entity(self, assert_row_keys):
        User = self.classes.User
        stmt = select(User)

        assert_row_keys(stmt, ("User",), "orm")

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("id", "name")),
        (LABEL_STYLE_TABLENAME_PLUS_COL, ("users_id", "users_name")),
        argnames="label_style,expected",
    )
    @testing.combinations(("core",), ("orm",), argnames="coreorm_exec")
    @testing.combinations(("core",), ("orm",), argnames="coreorm_cols")
    def test_explicit_cols(
        self,
        assert_row_keys,
        label_style,
        expected,
        coreorm_cols,
        coreorm_exec,
    ):
        User = self.classes.User
        users = self.tables.users

        if coreorm_cols == "core":
            stmt = select(users.c.id, users.c.name).set_label_style(
                label_style
            )
        else:
            stmt = select(User.id, User.name).set_label_style(label_style)

        assert_row_keys(stmt, expected, coreorm_exec)

    def test_explicit_cols_legacy(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User.id, User.name)
        row = q.first()

        eq_(row.keys(), ("id", "name"))

        eq_(
            [entity["name"] for entity in q.column_descriptions],
            ["id", "name"],
        )

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name", "id", "name")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("id", "name", "id_1", "name_1")),
        (
            LABEL_STYLE_TABLENAME_PLUS_COL,
            ("u1_id", "u1_name", "u2_id", "u2_name"),
        ),
        argnames="label_style,expected",
    )
    @testing.combinations(("core",), ("orm",), argnames="coreorm_exec")
    @testing.combinations(("core",), ("orm",), argnames="coreorm_cols")
    def test_explicit_ambiguous_cols_subq(
        self,
        assert_row_keys,
        label_style,
        expected,
        coreorm_cols,
        coreorm_exec,
    ):
        User = self.classes.User
        users = self.tables.users

        if coreorm_cols == "core":
            u1 = select(users.c.id, users.c.name).subquery("u1")
            u2 = select(users.c.id, users.c.name).subquery("u2")
        elif coreorm_cols == "orm":
            u1 = select(User.id, User.name).subquery("u1")
            u2 = select(User.id, User.name).subquery("u2")

        stmt = (
            select(u1, u2)
            .join_from(u1, u2, u1.c.id == u2.c.id)
            .set_label_style(label_style)
        )
        assert_row_keys(stmt, expected, coreorm_exec)

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name", "User", "id", "name", "a1")),
        (
            LABEL_STYLE_DISAMBIGUATE_ONLY,
            ("id", "name", "User", "id_1", "name_1", "a1"),
        ),
        (
            LABEL_STYLE_TABLENAME_PLUS_COL,
            ("u1_id", "u1_name", "User", "u2_id", "u2_name", "a1"),
        ),
        argnames="label_style,expected",
    )
    def test_explicit_ambiguous_cols_w_entities(
        self,
        assert_row_keys,
        label_style,
        expected,
    ):
        User = self.classes.User
        u1 = select(User.id, User.name).subquery("u1")
        u2 = select(User.id, User.name).subquery("u2")

        a1 = aliased(User, name="a1")
        stmt = (
            select(u1, User, u2, a1)
            .join_from(u1, u2, u1.c.id == u2.c.id)
            .join(User, User.id == u1.c.id)
            .join(a1, a1.id == u1.c.id)
            .set_label_style(label_style)
        )
        assert_row_keys(stmt, expected, "orm")

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name", "id", "name")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("id", "name", "id_1", "name_1")),
        (
            LABEL_STYLE_TABLENAME_PLUS_COL,
            ("u1_id", "u1_name", "u2_id", "u2_name"),
        ),
        argnames="label_style,expected",
    )
    def test_explicit_ambiguous_cols_subq_fromstatement(
        self, assert_row_keys, label_style, expected
    ):
        User = self.classes.User

        u1 = select(User.id, User.name).subquery("u1")
        u2 = select(User.id, User.name).subquery("u2")

        stmt = (
            select(u1, u2)
            .join_from(u1, u2, u1.c.id == u2.c.id)
            .set_label_style(label_style)
        )

        stmt = select(u1, u2).from_statement(stmt)

        assert_row_keys(stmt, expected, "orm")

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name", "id", "name")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("id", "name", "id", "name")),
        (LABEL_STYLE_TABLENAME_PLUS_COL, ("id", "name", "id", "name")),
        argnames="label_style,expected",
    )
    def test_explicit_ambiguous_cols_subq_fromstatement_legacy(
        self, label_style, expected
    ):
        User = self.classes.User

        u1 = select(User.id, User.name).subquery("u1")
        u2 = select(User.id, User.name).subquery("u2")

        stmt = (
            select(u1, u2)
            .join_from(u1, u2, u1.c.id == u2.c.id)
            .set_label_style(label_style)
        )

        s = fixture_session()
        row = s.query(u1, u2).from_statement(stmt).first()
        eq_(row.keys(), expected)

    def test_explicit_ambiguous_orm_cols_legacy(self):
        User = self.classes.User

        u1 = select(User.id, User.name).subquery("u1")
        u2 = select(User.id, User.name).subquery("u2")

        s = fixture_session()
        row = s.query(u1, u2).join(u2, u1.c.id == u2.c.id).first()
        eq_(row.keys(), ["id", "name", "id", "name"])

    @testing.fixture
    def uname_fixture(self):
        class Foo(object):
            pass

        if False:
            m = MetaData()
            users = Table(
                "users",
                m,
                Column("id", Integer, primary_key=True),
                Column("name", String, key="uname"),
            )
            mapper(Foo, users, properties={"uname": users.c.uname})
        else:
            users = self.tables.users
            mapper(Foo, users, properties={"uname": users.c.name})

        return Foo

    @testing.combinations(
        (LABEL_STYLE_NONE, ("id", "name"), ("id", "uname")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("id", "name"), ("id", "uname")),
        (
            LABEL_STYLE_TABLENAME_PLUS_COL,
            ("users_id", "users_name"),
            ("users_id", "users_uname"),
        ),
        argnames="label_style,expected_core,expected_orm",
    )
    @testing.combinations(("core",), ("orm",), argnames="coreorm_exec")
    def test_renamed_properties_columns(
        self,
        label_style,
        expected_core,
        expected_orm,
        uname_fixture,
        assert_row_keys,
        coreorm_exec,
    ):
        Foo = uname_fixture

        stmt = select(Foo.id, Foo.uname).set_label_style(label_style)

        if coreorm_exec == "core":
            assert_row_keys(
                stmt,
                expected_core,
                coreorm_exec,
                selected_columns=expected_orm,
            )
        else:
            assert_row_keys(stmt, expected_orm, coreorm_exec)

    @testing.combinations(
        (
            LABEL_STYLE_NONE,
            ("id", "name", "id", "name"),
            ("id", "uname", "id", "uname"),
        ),
        (
            LABEL_STYLE_DISAMBIGUATE_ONLY,
            ("id", "name", "id_1", "name_1"),
            ("id", "uname", "id_1", "uname_1"),
        ),
        (
            LABEL_STYLE_TABLENAME_PLUS_COL,
            ("u1_id", "u1_name", "u2_id", "u2_name"),
            ("u1_id", "u1_uname", "u2_id", "u2_uname"),
        ),
        argnames="label_style,expected_core,expected_orm",
    )
    @testing.combinations(("core",), ("orm",), argnames="coreorm_exec")
    # @testing.combinations(("orm",), argnames="coreorm_exec")
    def test_renamed_properties_subq(
        self,
        label_style,
        expected_core,
        expected_orm,
        uname_fixture,
        assert_row_keys,
        coreorm_exec,
    ):
        Foo = uname_fixture

        u1 = select(Foo.id, Foo.uname).subquery("u1")
        u2 = select(Foo.id, Foo.uname).subquery("u2")

        stmt = (
            select(u1, u2)
            .join_from(u1, u2, u1.c.id == u2.c.id)
            .set_label_style(label_style)
        )
        if coreorm_exec == "core":
            assert_row_keys(
                stmt,
                expected_core,
                coreorm_exec,
                selected_columns=expected_orm,
            )
        else:
            assert_row_keys(stmt, expected_orm, coreorm_exec)

    def test_entity_anon_aliased(self, assert_row_keys):
        User = self.classes.User

        u1 = aliased(User)
        stmt = select(u1)

        assert_row_keys(stmt, (), "orm")

    def test_entity_name_aliased(self, assert_row_keys):
        User = self.classes.User

        u1 = aliased(User, name="u1")
        stmt = select(u1)

        assert_row_keys(stmt, ("u1",), "orm")

    @testing.combinations(
        (LABEL_STYLE_NONE, ("u1", "u2")),
        (LABEL_STYLE_DISAMBIGUATE_ONLY, ("u1", "u2")),
        (LABEL_STYLE_TABLENAME_PLUS_COL, ("u1", "u2")),
        argnames="label_style,expected",
    )
    def test_multi_entity_name_aliased(
        self, assert_row_keys, label_style, expected
    ):
        User = self.classes.User

        u1 = aliased(User, name="u1")
        u2 = aliased(User, name="u2")
        stmt = (
            select(u1, u2)
            .join_from(u1, u2, u1.id == u2.id)
            .set_label_style(label_style)
        )

        assert_row_keys(stmt, expected, "orm")


class GetTest(QueryTest):
    def test_loader_options(self):
        User = self.classes.User

        s = fixture_session()

        u1 = s.query(User).options(joinedload(User.addresses)).get(8)
        eq_(len(u1.__dict__["addresses"]), 3)

    def test_loader_options_future(self):
        User = self.classes.User

        s = fixture_session()

        u1 = s.get(User, 8, options=[joinedload(User.addresses)])
        eq_(len(u1.__dict__["addresses"]), 3)

    def test_get_composite_pk_keyword_based_no_result(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        is_(s.query(CompositePk).get({"i": 100, "j": 100}), None)

    def test_get_composite_pk_keyword_based_result(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        one_two = s.query(CompositePk).get({"i": 1, "j": 2})
        eq_(one_two.i, 1)
        eq_(one_two.j, 2)
        eq_(one_two.k, 3)

    def test_get_composite_pk_keyword_based_wrong_keys(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, {"i": 1, "k": 2})

    def test_get_composite_pk_keyword_based_too_few_keys(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, {"i": 1})

    def test_get_composite_pk_keyword_based_too_many_keys(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(
            sa_exc.InvalidRequestError, q.get, {"i": 1, "j": "2", "k": 3}
        )

    def test_get(self):
        User = self.classes.User

        s = fixture_session()
        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2
        s.expunge_all()
        u2 = s.query(User).get(7)
        assert u is not u2

    def test_get_future(self):
        User = self.classes.User

        s = fixture_session()
        assert s.get(User, 19) is None
        u = s.get(User, 7)
        u2 = s.get(User, 7)
        assert u is u2
        s.expunge_all()
        u2 = s.get(User, 7)
        assert u is not u2

    def test_get_composite_pk_no_result(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        assert s.query(CompositePk).get((100, 100)) is None

    def test_get_composite_pk_result(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        one_two = s.query(CompositePk).get((1, 2))
        assert one_two.i == 1
        assert one_two.j == 2
        assert one_two.k == 3

    def test_get_too_few_params(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)

    def test_get_too_few_params_tuple(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7,))

    def test_get_too_many_params(self):
        CompositePk = self.classes.CompositePk

        s = fixture_session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7, 10, 100))

    def test_get_against_col(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.get, (5,))

    @testing.fixture
    def outerjoin_mapping(self):
        users, addresses = self.tables.users, self.tables.addresses

        s = users.outerjoin(addresses)

        class UserThing(fixtures.ComparableEntity):
            pass

        mapper(
            UserThing,
            s,
            properties={
                "id": (users.c.id, addresses.c.user_id),
                "address_id": addresses.c.id,
            },
        )
        return UserThing

    def test_get_null_pk(self, outerjoin_mapping):
        """test that a mapping which can have None in a
        PK (i.e. map to an outerjoin) works with get()."""

        UserThing = outerjoin_mapping
        sess = fixture_session()
        u10 = sess.query(UserThing).get((10, None))
        eq_(u10, UserThing(id=10))

    def test_get_fully_null_pk(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User)
        assert_raises_message(
            sa_exc.SAWarning,
            r"fully NULL primary key identity cannot load any object.  "
            "This condition may raise an error in a future release.",
            q.get,
            None,
        )

    def test_get_fully_null_composite_pk(self, outerjoin_mapping):
        UserThing = outerjoin_mapping

        s = fixture_session()
        q = s.query(UserThing)

        assert_raises_message(
            sa_exc.SAWarning,
            r"fully NULL primary key identity cannot load any object.  "
            "This condition may raise an error in a future release.",
            q.get,
            (None, None),
        )

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion"""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        q = s.query(User).join("addresses").filter(Address.user_id == 8)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)
        assert_raises(
            sa_exc.InvalidRequestError,
            s.query(User).filter(User.id == 7).get,
            19,
        )

        # order_by()/get() doesn't raise
        s.query(User).order_by(User.id).get(8)

    def test_no_criterion_when_already_loaded(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion, even when we're only using the identity map."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        s.query(User).get(7)

        q = s.query(User).join("addresses").filter(Address.user_id == 8)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)

    def test_unique_param_names(self):
        users = self.tables.users

        class SomeUser(object):
            pass

        s = users.select(users.c.id != 12).alias("users")
        m = mapper(SomeUser, s)
        assert s.primary_key == m.primary_key

        sess = fixture_session()
        assert sess.query(SomeUser).get(7).name == "jack"

    def test_load(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session(autoflush=False)

        assert s.query(User).populate_existing().get(19) is None

        u = s.query(User).populate_existing().get(7)
        u2 = s.query(User).populate_existing().get(7)
        assert u is u2
        s.expunge_all()
        u2 = s.query(User).populate_existing().get(7)
        assert u is not u2

        u2.name = "some name"
        a = Address(email_address="some other name")
        u2.addresses.append(a)
        assert u2 in s.dirty
        assert a in u2.addresses

        s.query(User).populate_existing().get(7)

        assert u2 not in s.dirty
        assert u2.name == "jack"
        assert a not in u2.addresses

    @testing.requires.unicode_connections
    def test_unicode(self, metadata, connection):
        table = Table(
            "unicode_data",
            metadata,
            Column("id", Unicode(40), primary_key=True),
            Column("data", Unicode(40)),
        )
        metadata.create_all(connection)
        ustring = util.b("petit voix m\xe2\x80\x99a").decode("utf-8")

        connection.execute(table.insert(), dict(id=ustring, data=ustring))

        class LocalFoo(self.classes.Base):
            pass

        mapper(LocalFoo, table)
        with Session(connection) as sess:
            eq_(
                sess.get(LocalFoo, ustring),
                LocalFoo(id=ustring, data=ustring),
            )

    def test_populate_existing(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session(autoflush=False)

        userlist = s.query(User).all()

        u = userlist[0]
        u.name = "foo"
        a = Address(name="ed")
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.query(User).populate_existing().all()

        self.assert_(u not in s.dirty)

        self.assert_(u.name == "jack")

        self.assert_(a not in u.addresses)

        u.addresses[0].email_address = "lala"
        u.orders[1].items[2].description = "item 12"
        # test that lazy load doesn't change child items
        s.query(User).populate_existing().all()
        assert u.addresses[0].email_address == "lala"
        assert u.orders[1].items[2].description == "item 12"

        # eager load does
        s.query(User).options(
            joinedload("addresses"), joinedload("orders").joinedload("items")
        ).populate_existing().all()
        assert u.addresses[0].email_address == "jack@bean.com"
        assert u.orders[1].items[2].description == "item 5"

    def test_populate_existing_future(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session(autoflush=False)

        userlist = s.query(User).all()

        u = userlist[0]
        u.name = "foo"
        a = Address(name="ed")
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        stmt = select(User).execution_options(populate_existing=True)

        s.execute(
            stmt,
        ).scalars().all()

        self.assert_(u not in s.dirty)

        self.assert_(u.name == "jack")

        self.assert_(a not in u.addresses)

        u.addresses[0].email_address = "lala"
        u.orders[1].items[2].description = "item 12"
        # test that lazy load doesn't change child items
        s.query(User).populate_existing().all()
        assert u.addresses[0].email_address == "lala"
        assert u.orders[1].items[2].description == "item 12"

        # eager load does

        stmt = (
            select(User)
            .options(
                joinedload("addresses"),
                joinedload("orders").joinedload("items"),
            )
            .execution_options(populate_existing=True)
        )

        s.execute(stmt).unique().scalars().all()

        assert u.addresses[0].email_address == "jack@bean.com"
        assert u.orders[1].items[2].description == "item 5"

    def test_option_transfer_future(self):
        User = self.classes.User
        stmt = select(User).execution_options(
            populate_existing=True, autoflush=False, yield_per=10
        )
        s = fixture_session()

        m1 = mock.Mock()

        event.listen(s, "do_orm_execute", m1)

        s.execute(stmt)

        eq_(
            m1.mock_calls[0][1][0].load_options,
            QueryContext.default_load_options(
                _autoflush=False, _populate_existing=True, _yield_per=10
            ),
        )


class InvalidGenerationsTest(QueryTest, AssertsCompiledSQL):
    @testing.combinations(
        lambda s, User: s.query(User).limit(2),
        lambda s, User: s.query(User).filter(User.id == 1).offset(2),
        lambda s, User: s.query(User).limit(2).offset(2),
    )
    def test_no_limit_offset(self, test_case):
        User = self.classes.User

        s = fixture_session()

        q = testing.resolve_lambda(test_case, User=User, s=s)

        assert_raises(sa_exc.InvalidRequestError, q.join, "addresses")

        assert_raises(sa_exc.InvalidRequestError, q.filter, User.name == "ed")

        assert_raises(sa_exc.InvalidRequestError, q.filter_by, name="ed")

        assert_raises(sa_exc.InvalidRequestError, q.order_by, "foo")

        assert_raises(sa_exc.InvalidRequestError, q.group_by, "foo")

        assert_raises(sa_exc.InvalidRequestError, q.having, "foo")

        q.enable_assertions(False).join("addresses")
        q.enable_assertions(False).filter(User.name == "ed")
        q.enable_assertions(False).order_by("foo")
        q.enable_assertions(False).group_by("foo")

    def test_no_from(self):
        users, User = self.tables.users, self.classes.User

        s = fixture_session()

        q = s.query(User).select_from(users)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q = s.query(User).join("addresses")
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q = s.query(User).order_by(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q.enable_assertions(False).select_from(users)

        with testing.expect_deprecated("The Query.from_self"):
            # this is fine, however
            q.from_self()

    def test_invalid_select_from(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id == 5)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id)

    def test_invalid_from_statement(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        s = fixture_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.from_statement, User.id == 5)
        assert_raises(
            sa_exc.ArgumentError, q.from_statement, users.join(addresses)
        )

    def test_invalid_column(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.add_columns, object())

    def test_invalid_column_tuple(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.add_columns, (1, 1))

    def test_distinct(self):
        """test that a distinct() call is not valid before 'clauseelement'
        conditions."""

        User = self.classes.User

        s = fixture_session()
        q = s.query(User).distinct()
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(
            sa_exc.InvalidRequestError,
            q.from_statement,
            text("select * from table"),
        )
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_order_by(self):
        """test that an order_by() call is not valid before 'clauseelement'
        conditions."""

        User = self.classes.User

        s = fixture_session()
        q = s.query(User).order_by(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(
            sa_exc.InvalidRequestError,
            q.from_statement,
            text("select * from table"),
        )
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_only_full_mapper_zero(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        q = s.query(User, Address)
        assert_raises(sa_exc.InvalidRequestError, q.get, 5)

    def test_entity_or_mapper_zero_from_context(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        q = s.query(User, Address)._compile_state()
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(User))

        u1 = aliased(User)
        q = s.query(u1, Address)._compile_state()
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(u1))

        q = s.query(User).select_from(Address)._compile_state()
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(Address))

        q = s.query(User.name, Address)._compile_state()
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(User))

        q = s.query(u1.name, Address)._compile_state()
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(u1))

        q1 = s.query(User).exists()
        q = s.query(q1)._compile_state()
        is_(q._mapper_zero(), None)
        is_(q._entity_zero(), None)

        q1 = s.query(Bundle("b1", User.id, User.name))._compile_state()
        is_(q1._mapper_zero(), inspect(User))
        is_(q1._entity_zero(), inspect(User))

    @testing.combinations(
        lambda s, User: s.query(User).filter(User.id == 5),
        lambda s, User: s.query(User).filter_by(id=5),
        lambda s, User: s.query(User).limit(5),
        lambda s, User: s.query(User).group_by(User.name),
        lambda s, User: s.query(User).order_by(User.name),
    )
    def test_from_statement(self, test_case):
        User = self.classes.User

        s = fixture_session()

        q = testing.resolve_lambda(test_case, User=User, s=s)

        assert_raises(sa_exc.InvalidRequestError, q.from_statement, text("x"))

    @testing.combinations(
        (Query.filter, lambda meth, User: meth(User.id == 5)),
        (Query.filter_by, lambda meth: meth(id=5)),
        (Query.limit, lambda meth: meth(5)),
        (Query.group_by, lambda meth, User: meth(User.name)),
        (Query.order_by, lambda meth, User: meth(User.name)),
    )
    def test_from_statement_text(self, meth, test_case):

        User = self.classes.User
        s = fixture_session()
        q = s.query(User)

        q = q.from_statement(text("x"))
        m = functools.partial(meth, q)

        assert_raises(
            sa_exc.InvalidRequestError,
            testing.resolve_lambda,
            test_case,
            meth=m,
            User=User,
            s=s,
        )

    def test_illegal_coercions(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            "SQL expression element expected, got .*User",
            distinct,
            User,
        )

        ua = aliased(User)
        assert_raises_message(
            sa_exc.ArgumentError,
            "SQL expression element expected, got .*User",
            distinct,
            ua,
        )

        s = fixture_session()
        assert_raises_message(
            sa_exc.ArgumentError,
            "SQL expression element or literal value expected, got .*User",
            lambda: s.query(User).filter(User.name == User),
        )

        u1 = User()
        assert_raises_message(
            sa_exc.ArgumentError,
            "SQL expression element expected, got .*User",
            distinct,
            u1,
        )

        assert_raises_message(
            sa_exc.ArgumentError,
            "SQL expression element or literal value expected, got .*User",
            lambda: s.query(User).filter(User.name == u1),
        )


class OperatorTest(QueryTest, AssertsCompiledSQL):
    """test sql.Comparator implementation for MapperProperties"""

    __dialect__ = "default"

    def _test(self, clause, expected, entity=None, checkparams=None):
        dialect = default.DefaultDialect()
        if entity is not None:
            # specify a lead entity, so that when we are testing
            # correlation, the correlation actually happens
            sess = fixture_session()
            lead = sess.query(entity)
            context = lead._compile_context()
            context.compile_state.statement._label_style = (
                LABEL_STYLE_TABLENAME_PLUS_COL
            )
            lead = context.compile_state.statement.compile(dialect=dialect)
            expected = (str(lead) + " WHERE " + expected).replace("\n", "")
            clause = sess.query(entity).filter(clause)
        self.assert_compile(clause, expected, checkparams=checkparams)

    def _test_filter_aliases(
        self, clause, expected, from_, onclause, checkparams=None
    ):
        dialect = default.DefaultDialect()
        sess = fixture_session()
        lead = sess.query(from_).join(onclause, aliased=True)
        full = lead.filter(clause)
        context = lead._compile_context()
        context.statement._label_style = LABEL_STYLE_TABLENAME_PLUS_COL
        lead = context.statement.compile(dialect=dialect)
        expected = (str(lead) + " WHERE " + expected).replace("\n", "")

        self.assert_compile(full, expected, checkparams=checkparams)

    @testing.combinations(
        (operators.add, "+"),
        (operators.mul, "*"),
        (operators.sub, "-"),
        (operators.truediv, "/"),
        (operators.div, "/"),
        argnames="py_op, sql_op",
        id_="ar",
    )
    @testing.combinations(
        (lambda User: 5, lambda User: User.id, ":id_1 %s users.id"),
        (lambda: 5, lambda: literal(6), ":param_1 %s :param_2"),
        (lambda User: User.id, lambda: 5, "users.id %s :id_1"),
        (lambda User: User.id, lambda: literal("b"), "users.id %s :param_1"),
        (lambda User: User.id, lambda User: User.id, "users.id %s users.id"),
        (lambda: literal(5), lambda: "b", ":param_1 %s :param_2"),
        (lambda: literal(5), lambda User: User.id, ":param_1 %s users.id"),
        (lambda: literal(5), lambda: literal(6), ":param_1 %s :param_2"),
        argnames="lhs, rhs, res",
        id_="aar",
    )
    def test_arithmetic(self, py_op, sql_op, lhs, rhs, res):
        User = self.classes.User

        lhs = testing.resolve_lambda(lhs, User=User)
        rhs = testing.resolve_lambda(rhs, User=User)

        fixture_session().query(User)
        self._test(py_op(lhs, rhs), res % sql_op)

    @testing.combinations(
        (operators.lt, "<", ">"),
        (operators.gt, ">", "<"),
        (operators.eq, "=", "="),
        (operators.ne, "!=", "!="),
        (operators.le, "<=", ">="),
        (operators.ge, ">=", "<="),
        id_="arr",
        argnames="py_op, fwd_op, rev_op",
    )
    @testing.lambda_combinations(
        lambda User, ualias: (
            ("a", User.id, ":id_1", "users.id"),
            ("a", literal("b"), ":param_2", ":param_1"),  # note swap!
            (User.id, "b", "users.id", ":id_1"),
            (User.id, literal("b"), "users.id", ":param_1"),
            (User.id, User.id, "users.id", "users.id"),
            (literal("a"), "b", ":param_1", ":param_2"),
            (literal("a"), User.id, ":param_1", "users.id"),
            (literal("a"), literal("b"), ":param_1", ":param_2"),
            (ualias.id, literal("b"), "users_1.id", ":param_1"),
            (User.id, ualias.name, "users.id", "users_1.name"),
            (User.name, ualias.name, "users.name", "users_1.name"),
            (ualias.name, User.name, "users_1.name", "users.name"),
        ),
        argnames="fixture",
    )
    def test_comparison(self, py_op, fwd_op, rev_op, fixture):
        User = self.classes.User

        fixture_session().query(User)
        ualias = aliased(User)

        lhs, rhs, l_sql, r_sql = fixture(User=User, ualias=ualias)

        # the compiled clause should match either (e.g.):
        # 'a' < 'b' -or- 'b' > 'a'.
        compiled = str(
            py_op(lhs, rhs).compile(dialect=default.DefaultDialect())
        )
        fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
        rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

        self.assert_(
            compiled == fwd_sql or compiled == rev_sql,
            "\n'"
            + compiled
            + "'\n does not match\n'"
            + fwd_sql
            + "'\n or\n'"
            + rev_sql
            + "'",
        )

    def test_o2m_compare_to_null(self):
        User = self.classes.User

        self._test(User.id == None, "users.id IS NULL")  # noqa
        self._test(User.id != None, "users.id IS NOT NULL")  # noqa
        self._test(~(User.id == None), "users.id IS NOT NULL")  # noqa
        self._test(~(User.id != None), "users.id IS NULL")  # noqa
        self._test(None == User.id, "users.id IS NULL")  # noqa
        self._test(~(None == User.id), "users.id IS NOT NULL")  # noqa

    def test_m2o_compare_to_null(self):
        Address = self.classes.Address
        self._test(Address.user == None, "addresses.user_id IS NULL")  # noqa
        self._test(
            ~(Address.user == None), "addresses.user_id IS NOT NULL"  # noqa
        )
        self._test(
            ~(Address.user != None), "addresses.user_id IS NULL"  # noqa
        )
        self._test(None == Address.user, "addresses.user_id IS NULL")  # noqa
        self._test(
            ~(None == Address.user), "addresses.user_id IS NOT NULL"  # noqa
        )

    def test_o2m_compare_to_null_aliased(self):
        User = self.classes.User
        u1 = aliased(User)
        self._test(u1.id == None, "users_1.id IS NULL")  # noqa
        self._test(u1.id != None, "users_1.id IS NOT NULL")  # noqa
        self._test(~(u1.id == None), "users_1.id IS NOT NULL")  # noqa
        self._test(~(u1.id != None), "users_1.id IS NULL")  # noqa

    def test_m2o_compare_to_null_aliased(self):
        Address = self.classes.Address
        a1 = aliased(Address)
        self._test(a1.user == None, "addresses_1.user_id IS NULL")  # noqa
        self._test(
            ~(a1.user == None), "addresses_1.user_id IS NOT NULL"  # noqa
        )
        self._test(a1.user != None, "addresses_1.user_id IS NOT NULL")  # noqa
        self._test(~(a1.user != None), "addresses_1.user_id IS NULL")  # noqa

    def test_relationship_unimplemented(self):
        User = self.classes.User
        for op in [
            User.addresses.like,
            User.addresses.ilike,
            User.addresses.__le__,
            User.addresses.__gt__,
        ]:
            assert_raises(NotImplementedError, op, "x")

    def test_o2m_any(self):
        User, Address = self.classes.User, self.classes.Address
        self._test(
            User.addresses.any(Address.id == 17),
            "EXISTS (SELECT 1 FROM addresses "
            "WHERE users.id = addresses.user_id AND addresses.id = :id_1)",
            entity=User,
        )

    def test_o2m_any_aliased(self):
        User, Address = self.classes.User, self.classes.Address
        u1 = aliased(User)
        a1 = aliased(Address)
        self._test(
            u1.addresses.of_type(a1).any(a1.id == 17),
            "EXISTS (SELECT 1 FROM addresses AS addresses_1 "
            "WHERE users_1.id = addresses_1.user_id AND "
            "addresses_1.id = :id_1)",
            entity=u1,
        )

    def test_m2o_compare_instance(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        self._test(Address.user == u7, ":param_1 = addresses.user_id")

    def test_m2o_compare_instance_negated(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        self._test(
            Address.user != u7,
            "addresses.user_id != :user_id_1 OR addresses.user_id IS NULL",
            checkparams={"user_id_1": 7},
        )

    def test_m2o_compare_instance_negated_warn_on_none(self):
        User, Address = self.classes.User, self.classes.Address

        u7_transient = User(id=None)

        with expect_warnings("Got None for value of column users.id; "):
            self._test(
                Address.user != u7_transient,
                "addresses.user_id != :user_id_1 "
                "OR addresses.user_id IS NULL",
                checkparams={"user_id_1": None},
            )

    def test_m2o_compare_instance_aliased(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        u7_transient = User(id=7)

        a1 = aliased(Address)
        self._test(
            a1.user == u7,
            ":param_1 = addresses_1.user_id",
            checkparams={"param_1": 7},
        )

        self._test(
            a1.user != u7,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            checkparams={"user_id_1": 7},
        )

        a1 = aliased(Address)
        self._test(
            a1.user == u7_transient,
            ":param_1 = addresses_1.user_id",
            checkparams={"param_1": 7},
        )

        self._test(
            a1.user != u7_transient,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            checkparams={"user_id_1": 7},
        )

    def test_selfref_relationship(self):

        Node = self.classes.Node

        nalias = aliased(Node)

        # auto self-referential aliasing
        self._test(
            Node.children.any(Node.data == "n1"),
            "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
            "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)",
            entity=Node,
            checkparams={"data_1": "n1"},
        )

        # needs autoaliasing
        self._test(
            Node.children == None,  # noqa
            "NOT (EXISTS (SELECT 1 FROM nodes AS nodes_1 "
            "WHERE nodes.id = nodes_1.parent_id))",
            entity=Node,
            checkparams={},
        )

        self._test(
            Node.parent == None,  # noqa
            "nodes.parent_id IS NULL",
            checkparams={},
        )

        self._test(
            nalias.parent == None,  # noqa
            "nodes_1.parent_id IS NULL",
            checkparams={},
        )

        self._test(
            nalias.parent != None,  # noqa
            "nodes_1.parent_id IS NOT NULL",
            checkparams={},
        )

        self._test(
            nalias.children == None,  # noqa
            "NOT (EXISTS ("
            "SELECT 1 FROM nodes WHERE nodes_1.id = nodes.parent_id))",
            entity=nalias,
            checkparams={},
        )

        self._test(
            nalias.children.any(Node.data == "some data"),
            "EXISTS (SELECT 1 FROM nodes WHERE "
            "nodes_1.id = nodes.parent_id AND nodes.data = :data_1)",
            entity=nalias,
            checkparams={"data_1": "some data"},
        )

        # this fails because self-referential any() is auto-aliasing;
        # the fact that we use "nalias" here means we get two aliases.
        # self._test(
        #        Node.children.any(nalias.data == 'some data'),
        #        "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
        #        "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)",
        #        entity=Node
        #        )

        self._test(
            nalias.parent.has(Node.data == "some data"),
            "EXISTS (SELECT 1 FROM nodes WHERE nodes.id = nodes_1.parent_id "
            "AND nodes.data = :data_1)",
            entity=nalias,
            checkparams={"data_1": "some data"},
        )

        self._test(
            Node.parent.has(Node.data == "some data"),
            "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
            "nodes_1.id = nodes.parent_id AND nodes_1.data = :data_1)",
            entity=Node,
            checkparams={"data_1": "some data"},
        )

        self._test(
            Node.parent == Node(id=7),
            ":param_1 = nodes.parent_id",
            checkparams={"param_1": 7},
        )

        self._test(
            nalias.parent == Node(id=7),
            ":param_1 = nodes_1.parent_id",
            checkparams={"param_1": 7},
        )

        self._test(
            nalias.parent != Node(id=7),
            "nodes_1.parent_id != :parent_id_1 "
            "OR nodes_1.parent_id IS NULL",
            checkparams={"parent_id_1": 7},
        )

        self._test(
            nalias.parent != Node(id=7),
            "nodes_1.parent_id != :parent_id_1 "
            "OR nodes_1.parent_id IS NULL",
            checkparams={"parent_id_1": 7},
        )

        self._test(
            nalias.children.contains(Node(id=7, parent_id=12)),
            "nodes_1.id = :param_1",
            checkparams={"param_1": 12},
        )

    def test_multilevel_any(self):
        User, Address, Dingaling = (
            self.classes.User,
            self.classes.Address,
            self.classes.Dingaling,
        )
        sess = fixture_session()

        q = sess.query(User).filter(
            User.addresses.any(
                and_(Address.id == Dingaling.address_id, Dingaling.data == "x")
            )
        )
        # new since #2746 - correlate_except() now takes context into account
        # so its usage in any() is not as disrupting.
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM addresses, dingalings "
            "WHERE users.id = addresses.user_id AND "
            "addresses.id = dingalings.address_id AND "
            "dingalings.data = :data_1)",
        )

    def test_op(self):
        User = self.classes.User

        self._test(User.name.op("ilike")("17"), "users.name ilike :name_1")

    def test_in(self):
        User = self.classes.User

        self._test(User.id.in_(["a", "b"]), "users.id IN ([POSTCOMPILE_id_1])")

    def test_in_on_relationship_not_supported(self):
        User, Address = self.classes.User, self.classes.Address

        assert_raises(NotImplementedError, Address.user.in_, [User(id=5)])

    def test_neg(self):
        User = self.classes.User

        self._test(-User.id, "-users.id")
        self._test(User.id + -User.id, "users.id + -users.id")

    def test_between(self):
        User = self.classes.User

        self._test(
            User.id.between("a", "b"), "users.id BETWEEN :id_1 AND :id_2"
        )

    def test_collate(self):
        User = self.classes.User

        self._test(collate(User.id, "utf8_bin"), "users.id COLLATE utf8_bin")

        self._test(User.id.collate("utf8_bin"), "users.id COLLATE utf8_bin")

    def test_selfref_between(self):
        User = self.classes.User

        ualias = aliased(User)
        self._test(
            User.id.between(ualias.id, ualias.id),
            "users.id BETWEEN users_1.id AND users_1.id",
        )
        self._test(
            ualias.id.between(User.id, User.id),
            "users_1.id BETWEEN users.id AND users.id",
        )

    def test_clauses(self):
        User, Address = self.classes.User, self.classes.Address

        for (expr, compare) in (
            (func.max(User.id), "max(users.id)"),
            (User.id.desc(), "users.id DESC"),
            (
                between(5, User.id, Address.id),
                ":param_1 BETWEEN users.id AND addresses.id",
            ),
            # this one would require adding compile() to
            # InstrumentedScalarAttribute.  do we want this ?
            # (User.id, "users.id")
        ):
            c = expr.compile(dialect=default.DefaultDialect())
            assert str(c) == compare, "%s != %s" % (str(c), compare)


class ExpressionTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_function_element_column_labels(self):
        users = self.tables.users
        sess = fixture_session()

        class max_(expression.FunctionElement):
            name = "max"

        @compiles(max_)
        def visit_max(element, compiler, **kw):
            return "max(%s)" % compiler.process(element.clauses, **kw)

        q = sess.query(max_(users.c.id))
        eq_(q.all(), [(10,)])

    def test_truly_unlabeled_sql_expressions(self):
        users = self.tables.users
        sess = fixture_session()

        class not_named_max(expression.ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            return "max(id)"

        # assert that there is no "AS max_" or any label of any kind.
        eq_(str(select(not_named_max())), "SELECT max(id)")

        # ColumnElement still handles it by applying label()
        q = sess.query(not_named_max()).select_from(users)
        eq_(q.all(), [(10,)])

    def test_deferred_instances(self):
        User, addresses, Address = (
            self.classes.User,
            self.tables.addresses,
            self.classes.Address,
        )

        session = fixture_session()
        s = (
            session.query(User)
            .filter(
                and_(
                    addresses.c.email_address == bindparam("emailad"),
                    Address.user_id == User.id,
                )
            )
            .statement
        )

        result = list(
            session.query(User)
            .params(emailad="jack@bean.com")
            .from_statement(s)
        )
        eq_([User(id=7)], result)

    def test_aliased_sql_construct(self):
        User, Address = self.classes.User, self.classes.Address

        j = join(User, Address)
        a1 = aliased(j)
        self.assert_compile(
            a1.select(),
            "SELECT anon_1.users_id, anon_1.users_name, anon_1.addresses_id, "
            "anon_1.addresses_user_id, anon_1.addresses_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM users JOIN addresses "
            "ON users.id = addresses.user_id) AS anon_1",
        )

    def test_aliased_sql_construct_raises_adapt_on_names(self):
        User, Address = self.classes.User, self.classes.Address

        j = join(User, Address)
        assert_raises_message(
            sa_exc.ArgumentError,
            "adapt_on_names only applies to ORM elements",
            aliased,
            j,
            adapt_on_names=True,
        )

    def test_scalar_subquery_compile_whereclause(self):
        User = self.classes.User
        Address = self.classes.Address

        session = fixture_session()

        q = session.query(User.id).filter(User.id == 7).scalar_subquery()

        q = session.query(Address).filter(Address.user_id == q)

        assert isinstance(q.whereclause.right, expression.ColumnElement)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM addresses WHERE "
            "addresses.user_id = (SELECT users.id "
            "FROM users WHERE users.id = :id_1)",
        )

    def test_subquery_no_eagerloads(self):
        User = self.classes.User
        s = fixture_session()

        self.assert_compile(
            s.query(User).options(joinedload(User.addresses)).subquery(),
            "SELECT users.id, users.name FROM users",
        )

    def test_exists_no_eagerloads(self):
        User = self.classes.User
        s = fixture_session()

        self.assert_compile(
            s.query(
                s.query(User).options(joinedload(User.addresses)).exists()
            ),
            "SELECT EXISTS (SELECT 1 FROM users) AS anon_1",
        )

    def test_named_subquery(self):
        User = self.classes.User

        session = fixture_session()
        a1 = session.query(User.id).filter(User.id == 7).subquery("foo1")
        a2 = session.query(User.id).filter(User.id == 7).subquery(name="foo2")
        a3 = session.query(User.id).filter(User.id == 7).subquery()

        eq_(a1.name, "foo1")
        eq_(a2.name, "foo2")
        eq_(a3.name, "%%(%d anon)s" % id(a3))

    def test_labeled_subquery(self):
        User = self.classes.User

        session = fixture_session()
        a1 = (
            session.query(User.id)
            .filter(User.id == 7)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        assert a1.c.users_id is not None

    def test_reduced_subquery(self):
        User = self.classes.User
        ua = aliased(User)

        session = fixture_session()
        a1 = (
            session.query(User.id, ua.id, ua.name)
            .filter(User.id == ua.id)
            .subquery(reduce_columns=True)
        )
        self.assert_compile(
            a1,
            "SELECT users.id, users_1.name FROM "
            "users, users AS users_1 "
            "WHERE users.id = users_1.id",
        )

    def test_label(self):
        User = self.classes.User

        session = fixture_session()

        q = session.query(User.id).filter(User.id == 7).label("foo")
        self.assert_compile(
            session.query(q),
            "SELECT (SELECT users.id FROM users "
            "WHERE users.id = :id_1) AS foo",
        )

    def test_scalar_subquery(self):
        User = self.classes.User

        session = fixture_session()

        q = session.query(User.id).filter(User.id == 7).scalar_subquery()

        self.assert_compile(
            session.query(User).filter(User.id.in_(q)),
            "SELECT users.id AS users_id, users.name "
            "AS users_name FROM users WHERE users.id "
            "IN (SELECT users.id FROM users WHERE "
            "users.id = :id_1)",
        )

    def test_param_transfer(self):
        User = self.classes.User

        session = fixture_session()

        q = (
            session.query(User.id)
            .filter(User.id == bindparam("foo"))
            .params(foo=7)
            .scalar_subquery()
        )

        q = session.query(User).filter(User.id.in_(q))

        eq_(User(id=7), q.one())

    def test_in(self):
        User, Address = self.classes.User, self.classes.Address

        session = fixture_session()
        s = (
            session.query(User.id)
            .join(User.addresses)
            .group_by(User.id)
            .having(func.count(Address.id) > 2)
        )
        eq_(session.query(User).filter(User.id.in_(s)).all(), [User(id=8)])

    def test_union(self):
        User = self.classes.User

        s = fixture_session()

        q1 = s.query(User).filter(User.name == "ed")
        q2 = s.query(User).filter(User.name == "fred")
        eq_(
            s.query(User)
            .from_statement(union(q1, q2).order_by("users_name"))
            .all(),
            [User(name="ed"), User(name="fred")],
        )

    def test_select(self):
        User = self.classes.User

        s = fixture_session()

        q1 = s.query(User).filter(User.name == "ed")

        self.assert_compile(
            select(q1.subquery()),
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users WHERE users.name = :name_1) AS anon_1",
        )

    def test_join(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        # TODO: do we want aliased() to detect a query and convert to
        # subquery() automatically ?
        q1 = s.query(Address).filter(Address.email_address == "jack@bean.com")
        adalias = aliased(Address, q1.subquery())
        eq_(
            s.query(User, adalias)
            .join(adalias, User.id == adalias.user_id)
            .all(),
            [
                (
                    User(id=7, name="jack"),
                    Address(email_address="jack@bean.com", user_id=7, id=1),
                )
            ],
        )

    def test_group_by_plain(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).group_by(User.name)
        self.assert_compile(
            select(q1.subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users GROUP BY users.name) AS anon_1",
        )

    def test_group_by_append(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).group_by(User.name)

        # test append something to group_by
        self.assert_compile(
            select(q1.group_by(User.id).subquery()),
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users GROUP BY users.name, users.id) AS anon_1",
        )

    def test_group_by_cancellation(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).group_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select(q1.group_by(None).group_by(User.id).subquery()),
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users GROUP BY users.id) AS anon_1",
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select(q1.group_by(None).subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users) AS anon_1",
        )

    def test_group_by_cancelled_still_present(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).group_by(User.name).group_by(None)

        q1._no_criterion_assertion("foo")

    def test_order_by_plain(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        self.assert_compile(
            select(q1.subquery()),
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users ORDER BY users.name) AS anon_1",
        )

    def test_order_by_append(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name)

        # test append something to order_by
        self.assert_compile(
            select(q1.order_by(User.id).subquery()),
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users ORDER BY users.name, users.id) AS anon_1",
        )

    def test_order_by_cancellation(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select(q1.order_by(None).order_by(User.id).subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users ORDER BY users.id) AS anon_1",
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select(q1.order_by(None).subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users) AS anon_1",
        )

    def test_order_by_cancellation_false(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select(q1.order_by(False).order_by(User.id).subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users ORDER BY users.id) AS anon_1",
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select(q1.order_by(False).subquery()),
            "SELECT anon_1.id, anon_1.name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users) AS anon_1",
        )

    def test_order_by_cancelled_allows_assertions(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name).order_by(None)

        q1._no_criterion_assertion("foo")

    def test_legacy_order_by_cancelled_allows_assertions(self):
        User = self.classes.User
        s = fixture_session()

        q1 = s.query(User.id, User.name).order_by(User.name).order_by(False)

        q1._no_criterion_assertion("foo")


class ColumnPropertyTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"
    run_setup_mappers = "each"

    def _fixture(self, label=True, polymorphic=False):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")
        stmt = (
            select(func.max(addresses.c.email_address))
            .where(addresses.c.user_id == users.c.id)
            .correlate(users)
        )
        if label:
            stmt = stmt.label("email_ad")
        else:
            stmt = stmt.scalar_subquery()

        mapper(
            User,
            users,
            properties={"ead": column_property(stmt)},
            with_polymorphic="*" if polymorphic else None,
        )
        mapper(Address, addresses)

    def _func_fixture(self, label=False):
        User = self.classes.User
        users = self.tables.users

        if label:
            mapper(
                User,
                users,
                properties={
                    "foobar": column_property(
                        func.foob(users.c.name).label(None)
                    )
                },
            )
        else:
            mapper(
                User,
                users,
                properties={
                    "foobar": column_property(func.foob(users.c.name))
                },
            )

    def test_anon_label_function_auto(self):
        self._func_fixture()
        User = self.classes.User

        s = fixture_session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar, u1.foobar),
            "SELECT foob(users.name) AS foob_1, foob(users_1.name) AS foob_2 "
            "FROM users, users AS users_1",
        )

    def test_anon_label_function_manual(self):
        self._func_fixture(label=True)
        User = self.classes.User

        s = fixture_session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar, u1.foobar),
            "SELECT foob(users.name) AS foob_1, foob(users_1.name) AS foob_2 "
            "FROM users, users AS users_1",
        )

    def test_anon_label_ad_hoc_labeling(self):
        self._func_fixture()
        User = self.classes.User

        s = fixture_session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar.label("x"), u1.foobar.label("y")),
            "SELECT foob(users.name) AS x, foob(users_1.name) AS y "
            "FROM users, users AS users_1",
        )

    def test_order_by_column_prop_string(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = fixture_session()
        q = s.query(User).order_by("email_ad")
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id) AS email_ad, "
            "users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY email_ad",
        )

    def test_order_by_column_prop_aliased_string(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = fixture_session()
        ua = aliased(User)
        q = s.query(ua).order_by("email_ad")

        assert_raises_message(
            sa.exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
        )

    def test_order_by_column_labeled_prop_attr_aliased_one(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1",
        )

    def test_order_by_column_labeled_prop_attr_aliased_two(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(ua.ead).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, "
            "users AS users_1 WHERE addresses.user_id = users_1.id) "
            "AS anon_1 ORDER BY anon_1",
        )

        # we're also testing that the state of "ua" is OK after the
        # previous call, so the batching into one test is intentional
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1",
        )

    def test_order_by_column_labeled_prop_attr_aliased_three(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(User.ead, ua.ead).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users WHERE addresses.user_id = users.id) "
            "AS email_ad, (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users AS users_1 WHERE addresses.user_id = "
            "users_1.id) AS anon_1 ORDER BY email_ad, anon_1",
        )

        q = s.query(User, ua).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users.id) AS "
            "email_ad, users.id AS users_id, users.name AS users_name, "
            "(SELECT max(addresses.email_address) AS max_1 FROM addresses "
            "WHERE addresses.user_id = users_1.id) AS anon_1, users_1.id "
            "AS users_1_id, users_1.name AS users_1_name FROM users, "
            "users AS users_1 ORDER BY email_ad, anon_1",
        )

    def test_order_by_column_labeled_prop_attr_aliased_four(self):
        User = self.classes.User
        self._fixture(label=True, polymorphic=True)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(ua, User.id).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 FROM "
            "addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name, "
            "users.id AS users_id FROM users AS users_1, "
            "users ORDER BY anon_1",
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_one(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1",
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_two(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(ua.ead).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, "
            "users AS users_1 WHERE addresses.user_id = users_1.id) "
            "AS anon_1 ORDER BY anon_1",
        )

        # we're also testing that the state of "ua" is OK after the
        # previous call, so the batching into one test is intentional
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1",
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_three(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = fixture_session()
        q = s.query(User.ead, ua.ead).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users WHERE addresses.user_id = users.id) "
            "AS anon_1, (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users AS users_1 "
            "WHERE addresses.user_id = users_1.id) AS anon_2 "
            "ORDER BY anon_1, anon_2",
        )

        q = s.query(User, ua).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users.id) AS "
            "anon_1, users.id AS users_id, users.name AS users_name, "
            "(SELECT max(addresses.email_address) AS max_1 FROM addresses "
            "WHERE addresses.user_id = users_1.id) AS anon_2, users_1.id "
            "AS users_1_id, users_1.name AS users_1_name FROM users, "
            "users AS users_1 ORDER BY anon_1, anon_2",
        )

    def test_order_by_column_prop_attr(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = fixture_session()
        q = s.query(User).order_by(User.ead)
        # this one is a bit of a surprise; this is compiler
        # label-order-by logic kicking in, but won't work in more
        # complex cases.
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id) AS email_ad, "
            "users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY email_ad",
        )

    def test_order_by_column_prop_attr_non_present(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = fixture_session()
        q = s.query(User).options(defer(User.ead)).order_by(User.ead)
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY "
            "(SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id)",
        )


class ComparatorTest(QueryTest):
    def test_clause_element_query_resolve(self):
        from sqlalchemy.orm.properties import ColumnProperty

        User = self.classes.User

        class Comparator(ColumnProperty.Comparator):
            def __init__(self, expr):
                self.expr = expr

            def __clause_element__(self):
                return self.expr

        # this use case isn't exactly needed in this form, however it tests
        # that we resolve for multiple __clause_element__() calls as is needed
        # by systems like composites
        sess = fixture_session()
        eq_(
            sess.query(Comparator(User.id))
            .order_by(Comparator(User.id))
            .all(),
            [(7,), (8,), (9,), (10,)],
        )


# more slice tests are available in test/orm/generative.py
class SliceTest(QueryTest):
    __dialect__ = "default"
    __backend__ = True

    def test_first(self):
        User = self.classes.User

        assert User(id=7) == fixture_session().query(User).first()

        assert (
            fixture_session().query(User).filter(User.id == 27).first() is None
        )

    def test_negative_indexes_raise(self):
        User = self.classes.User

        sess = fixture_session(future=True)
        q = sess.query(User).order_by(User.id)

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            q[-5:-2]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            q[-1]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            q[-5]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            q[:-2]

        # this doesn't evaluate anything because it's a net-negative
        eq_(q[-2:-5], [])

    def test_limit_offset_applies(self):
        """Test that the expected LIMIT/OFFSET is applied for slices.

        The LIMIT/OFFSET syntax differs slightly on all databases, and
        query[x:y] executes immediately, so we are asserting against
        SQL strings using sqlite's syntax.

        """

        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User).order_by(User.id)

        self.assert_sql(
            testing.db,
            lambda: q[10:20],
            [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT :param_1 OFFSET :param_2",
                    {"param_1": 10, "param_2": 10},
                )
            ],
        )

        self.assert_sql(
            testing.db,
            lambda: q[:20],
            [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT :param_1",
                    {"param_1": 20},
                )
            ],
        )

        self.assert_sql(
            testing.db,
            lambda: q[5:],
            [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT -1 OFFSET :param_1",
                    {"param_1": 5},
                )
            ],
        )

        self.assert_sql(testing.db, lambda: q[2:2], [])

        self.assert_sql(testing.db, lambda: q[-2:-5], [])

        self.assert_sql(
            testing.db,
            lambda: q[:],
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id",
                    {},
                )
            ],
        )

    @testing.requires.sql_expression_limit_offset
    def test_first_against_expression_offset(self):
        User = self.classes.User

        sess = fixture_session()
        q = (
            sess.query(User)
            .order_by(User.id)
            .offset(literal_column("2") + literal_column("3"))
        )

        self.assert_sql(
            testing.db,
            q.first,
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id "
                    "LIMIT :param_1 OFFSET 2 + 3",
                    [{"param_1": 1}],
                )
            ],
        )

    @testing.requires.sql_expression_limit_offset
    def test_full_slice_against_expression_offset(self):
        User = self.classes.User

        sess = fixture_session()
        q = (
            sess.query(User)
            .order_by(User.id)
            .offset(literal_column("2") + literal_column("3"))
        )

        self.assert_sql(
            testing.db,
            lambda: q[2:5],
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id "
                    "LIMIT :param_1 OFFSET 2 + 3 + :param_2",
                    [{"param_1": 3, "param_2": 2}],
                )
            ],
        )

    def test_full_slice_against_integer_offset(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User).order_by(User.id).offset(2)

        self.assert_sql(
            testing.db,
            lambda: q[2:5],
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id "
                    "LIMIT :param_1 OFFSET :param_2",
                    [{"param_1": 3, "param_2": 4}],
                )
            ],
        )

    @testing.requires.sql_expression_limit_offset
    def test_start_slice_against_expression_offset(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User).order_by(User.id).offset(literal_column("2"))

        self.assert_sql(
            testing.db,
            lambda: q[2:],
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id "
                    "LIMIT -1 OFFSET 2 + :2_1",
                    [{"2_1": 2}],
                )
            ],
        )


class FilterTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_basic(self):
        User = self.classes.User

        users = fixture_session().query(User).all()
        eq_([User(id=7), User(id=8), User(id=9), User(id=10)], users)

    @testing.requires.offset
    def test_limit_offset(self):
        User = self.classes.User

        sess = fixture_session()

        assert [User(id=8), User(id=9)] == sess.query(User).order_by(
            User.id
        ).limit(2).offset(1).all()

        assert [User(id=8), User(id=9)] == list(
            sess.query(User).order_by(User.id)[1:3]
        )

        assert User(id=8) == sess.query(User).order_by(User.id)[1]

        assert [] == sess.query(User).order_by(User.id)[3:3]
        assert [] == sess.query(User).order_by(User.id)[0:0]

    @testing.requires.bound_limit_offset
    def test_select_with_bindparam_offset_limit(self):
        """Does a query allow bindparam for the limit?"""
        User = self.classes.User
        sess = fixture_session()
        q1 = (
            sess.query(self.classes.User)
            .order_by(self.classes.User.id)
            .limit(bindparam("n"))
        )

        for n in range(1, 4):
            result = q1.params(n=n).all()
            eq_(len(result), n)

        eq_(
            sess.query(User)
            .order_by(User.id)
            .limit(bindparam("limit"))
            .offset(bindparam("offset"))
            .params(limit=2, offset=1)
            .all(),
            [User(id=8), User(id=9)],
        )

    @testing.fails_on(
        ["mysql", "mariadb"], "doesn't like CAST in the limit clause"
    )
    @testing.requires.bound_limit_offset
    def test_select_with_bindparam_offset_limit_w_cast(self):
        User = self.classes.User
        sess = fixture_session()
        eq_(
            list(
                sess.query(User)
                .params(a=1, b=3)
                .order_by(User.id)[
                    cast(bindparam("a"), Integer) : cast(
                        bindparam("b"), Integer
                    )
                ]
            ),
            [User(id=8), User(id=9)],
        )

    @testing.requires.boolean_col_expressions
    def test_exists(self):
        User = self.classes.User

        sess = fixture_session()

        assert sess.query(exists().where(User.id == 9)).scalar()
        assert not sess.query(exists().where(User.id == 29)).scalar()

    def test_one_filter(self):
        User = self.classes.User

        assert [User(id=8), User(id=9)] == fixture_session().query(
            User
        ).filter(User.name.endswith("ed")).all()

    def test_contains(self):
        """test comparing a collection to an object instance."""

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        address = sess.query(Address).get(3)
        assert [User(id=8)] == sess.query(User).filter(
            User.addresses.contains(address)
        ).all()

        try:
            sess.query(User).filter(User.addresses == address)
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        assert [User(id=10)] == sess.query(User).filter(
            User.addresses == None
        ).all()  # noqa

        try:
            assert [User(id=7), User(id=9), User(id=10)] == sess.query(
                User
            ).filter(User.addresses != address).all()
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        # assert [User(id=7), User(id=9), User(id=10)] ==
        # sess.query(User).filter(User.addresses!=address).all()

    def test_clause_element_ok(self):
        User = self.classes.User
        s = fixture_session()
        self.assert_compile(
            s.query(User).filter(User.addresses),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users, addresses WHERE users.id = addresses.user_id",
        )

    def test_unique_binds_join_cond(self):
        """test that binds used when the lazyclause is used in criterion are
        unique"""

        User, Address = self.classes.User, self.classes.Address
        sess = fixture_session()
        a1, a2 = sess.query(Address).order_by(Address.id)[0:2]
        self.assert_compile(
            sess.query(User)
            .filter(User.addresses.contains(a1))
            .union(sess.query(User).filter(User.addresses.contains(a2))),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users WHERE users.id = :param_1 "
            "UNION SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = :param_2) AS anon_1",
            checkparams={"param_1": 7, "param_2": 8},
        )

    def test_any(self):
        # see also HasAnyTest, a newer suite which tests these at the level of
        # SQL compilation
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        assert [User(id=8), User(id=9)] == sess.query(User).filter(
            User.addresses.any(Address.email_address.like("%ed%"))
        ).all()

        assert [User(id=8)] == sess.query(User).filter(
            User.addresses.any(Address.email_address.like("%ed%"), id=4)
        ).all()

        assert [User(id=8)] == sess.query(User).filter(
            User.addresses.any(Address.email_address.like("%ed%"))
        ).filter(User.addresses.any(id=4)).all()

        assert [User(id=9)] == sess.query(User).filter(
            User.addresses.any(email_address="fred@fred.com")
        ).all()

        # test that the contents are not adapted by the aliased join
        ua = aliased(Address)
        assert [User(id=7), User(id=8)] == sess.query(User).join(
            ua, "addresses"
        ).filter(
            ~User.addresses.any(Address.email_address == "fred@fred.com")
        ).all()

        assert [User(id=10)] == sess.query(User).outerjoin(
            ua, "addresses"
        ).filter(~User.addresses.any()).all()

    def test_any_doesnt_overcorrelate(self):
        # see also HasAnyTest, a newer suite which tests these at the level of
        # SQL compilation
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        # test that any() doesn't overcorrelate
        assert [User(id=7), User(id=8)] == sess.query(User).join(
            "addresses"
        ).filter(
            ~User.addresses.any(Address.email_address == "fred@fred.com")
        ).all()

    def test_has(self):
        # see also HasAnyTest, a newer suite which tests these at the level of
        # SQL compilation
        Dingaling, User, Address = (
            self.classes.Dingaling,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        assert [Address(id=5)] == sess.query(Address).filter(
            Address.user.has(name="fred")
        ).all()

        assert [
            Address(id=2),
            Address(id=3),
            Address(id=4),
            Address(id=5),
        ] == sess.query(Address).filter(
            Address.user.has(User.name.like("%ed%"))
        ).order_by(
            Address.id
        ).all()

        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(
            Address
        ).filter(Address.user.has(User.name.like("%ed%"), id=8)).order_by(
            Address.id
        ).all()

        # test has() doesn't overcorrelate
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(
            Address
        ).join("user").filter(
            Address.user.has(User.name.like("%ed%"), id=8)
        ).order_by(
            Address.id
        ).all()

        # test has() doesn't get subquery contents adapted by aliased join
        ua = aliased(User)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(
            Address
        ).join(ua, "user").filter(
            Address.user.has(User.name.like("%ed%"), id=8)
        ).order_by(
            Address.id
        ).all()

        dingaling = sess.query(Dingaling).get(2)
        assert [User(id=9)] == sess.query(User).filter(
            User.addresses.any(Address.dingaling == dingaling)
        ).all()

    def test_contains_m2m(self):
        Item, Order = self.classes.Item, self.classes.Order

        sess = fixture_session()
        item = sess.query(Item).get(3)

        eq_(
            sess.query(Order)
            .filter(Order.items.contains(item))
            .order_by(Order.id)
            .all(),
            [Order(id=1), Order(id=2), Order(id=3)],
        )
        eq_(
            sess.query(Order)
            .filter(~Order.items.contains(item))
            .order_by(Order.id)
            .all(),
            [Order(id=4), Order(id=5)],
        )

        item2 = sess.query(Item).get(5)
        eq_(
            sess.query(Order)
            .filter(Order.items.contains(item))
            .filter(Order.items.contains(item2))
            .all(),
            [Order(id=3)],
        )

    @testing.combinations(
        lambda sess, User, Address: (
            sess.query(Address).filter(
                Address.user == sess.query(User).scalar_subquery()
            )
        ),
        lambda sess, User, Address: (
            sess.query(Address).filter_by(
                user=sess.query(User).scalar_subquery()
            )
        ),
        lambda sess, User, Address: (
            sess.query(Address).filter(Address.user == sess.query(User))
        ),
        lambda sess, User, Address: (
            sess.query(Address).filter(
                Address.user == sess.query(User).subquery()
            )
        ),
        lambda sess, User, Address: (
            sess.query(Address).filter_by(user="foo")
        ),
    )
    def test_object_comparison_needs_object(self, fn):
        User, Address = (
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        assert_raises_message(
            sa.exc.ArgumentError,
            "Mapped instance expected for relationship comparison to object.",
            fn,
            sess,
            User,
            Address,
        ),

    def test_object_comparison(self):
        """test scalar comparison to an object instance"""

        Item, Order, Dingaling, User, Address = (
            self.classes.Item,
            self.classes.Order,
            self.classes.Dingaling,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(
            Address
        ).filter(Address.user == user).all()

        assert [Address(id=1), Address(id=5)] == sess.query(Address).filter(
            Address.user != user
        ).all()

        # generates an IS NULL
        assert (
            [] == sess.query(Address).filter(Address.user == None).all()
        )  # noqa
        assert [] == sess.query(Address).filter(Address.user == null()).all()

        assert [Order(id=5)] == sess.query(Order).filter(
            Order.address == None
        ).all()  # noqa

        # o2o
        dingaling = sess.query(Dingaling).get(2)
        assert [Address(id=5)] == sess.query(Address).filter(
            Address.dingaling == dingaling
        ).all()

        # m2m
        eq_(
            sess.query(Item)
            .filter(Item.keywords == None)
            .order_by(Item.id)  # noqa
            .all(),
            [Item(id=4), Item(id=5)],
        )
        eq_(
            sess.query(Item)
            .filter(Item.keywords != None)
            .order_by(Item.id)  # noqa
            .all(),
            [Item(id=1), Item(id=2), Item(id=3)],
        )

    def test_filter_by(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(
            Address
        ).filter_by(user=user).all()

        # many to one generates IS NULL
        assert [] == sess.query(Address).filter_by(user=None).all()
        assert [] == sess.query(Address).filter_by(user=null()).all()

        # one to many generates WHERE NOT EXISTS
        assert [User(name="chuck")] == sess.query(User).filter_by(
            addresses=None
        ).all()
        assert [User(name="chuck")] == sess.query(User).filter_by(
            addresses=null()
        ).all()

    def test_filter_by_tables(self):
        users = self.tables.users
        addresses = self.tables.addresses
        sess = fixture_session()
        self.assert_compile(
            sess.query(users)
            .filter_by(name="ed")
            .join(addresses, users.c.id == addresses.c.user_id)
            .filter_by(email_address="ed@ed.com"),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "WHERE users.name = :name_1 AND "
            "addresses.email_address = :email_address_1",
            checkparams={"email_address_1": "ed@ed.com", "name_1": "ed"},
        )

    def test_empty_filters(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query(User)

        is_(None, q1.filter().whereclause)
        is_(None, q1.filter_by().whereclause)

    def test_filter_by_no_property(self):
        addresses = self.tables.addresses
        sess = fixture_session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            'Entity namespace for "addresses" has no property "name"',
            sess.query(addresses).filter_by,
            name="ed",
        )

    def test_none_comparison(self):
        Order, User, Address = (
            self.classes.Order,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()

        # scalar
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id == None).all(),  # noqa
        )
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id == null()).all(),
        )

        # o2o
        eq_(
            [Address(id=1), Address(id=3), Address(id=4)],
            sess.query(Address)
            .filter(Address.dingaling == None)
            .order_by(Address.id)  # noqa
            .all(),
        )
        eq_(
            [Address(id=1), Address(id=3), Address(id=4)],
            sess.query(Address)
            .filter(Address.dingaling == null())
            .order_by(Address.id)
            .all(),
        )
        eq_(
            [Address(id=2), Address(id=5)],
            sess.query(Address)
            .filter(Address.dingaling != None)
            .order_by(Address.id)  # noqa
            .all(),
        )
        eq_(
            [Address(id=2), Address(id=5)],
            sess.query(Address)
            .filter(Address.dingaling != null())
            .order_by(Address.id)
            .all(),
        )

        # m2o
        eq_(
            [Order(id=5)],
            sess.query(Order).filter(Order.address == None).all(),
        )  # noqa
        eq_(
            [Order(id=1), Order(id=2), Order(id=3), Order(id=4)],
            sess.query(Order)
            .order_by(Order.id)
            .filter(Order.address != None)
            .all(),
        )  # noqa

        # o2m
        eq_(
            [User(id=10)],
            sess.query(User).filter(User.addresses == None).all(),
        )  # noqa
        eq_(
            [User(id=7), User(id=8), User(id=9)],
            sess.query(User)
            .filter(User.addresses != None)
            .order_by(User.id)  # noqa
            .all(),
        )

    def test_blank_filter_by(self):
        User = self.classes.User

        eq_(
            [(7,), (8,), (9,), (10,)],
            fixture_session()
            .query(User.id)
            .filter_by()
            .order_by(User.id)
            .all(),
        )
        eq_(
            [(7,), (8,), (9,), (10,)],
            fixture_session()
            .query(User.id)
            .filter_by(**{})
            .order_by(User.id)
            .all(),
        )

    def test_text_coerce(self):
        User = self.classes.User
        s = fixture_session()
        self.assert_compile(
            s.query(User).filter(text("name='ed'")),
            "SELECT users.id AS users_id, users.name "
            "AS users_name FROM users WHERE name='ed'",
        )

    def test_filter_by_non_entity(self):
        s = fixture_session()
        e = sa.func.count(123)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            r'Entity namespace for "count\(\:count_1\)" has no property "col"',
            s.query(e).filter_by,
            col=42,
        )


class HasAnyTest(fixtures.DeclarativeMappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class D(Base):
            __tablename__ = "d"
            id = Column(Integer, primary_key=True)

        class C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            d_id = Column(ForeignKey(D.id))

            bs = relationship("B", back_populates="c")

        b_d = Table(
            "b_d",
            Base.metadata,
            Column("bid", ForeignKey("b.id")),
            Column("did", ForeignKey("d.id")),
        )

        # note we are using the ForeignKey pattern identified as a bug
        # in [ticket:4367]
        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            c_id = Column(ForeignKey(C.id))

            c = relationship("C", back_populates="bs")

            d = relationship("D", secondary=b_d)

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey(B.id))

            d = relationship(
                "D",
                secondary="join(B, C)",
                primaryjoin="A.b_id == B.id",
                secondaryjoin="C.d_id == D.id",
                uselist=False,
            )

    def test_has_composite_secondary(self):
        A, D = self.classes("A", "D")
        s = fixture_session()
        self.assert_compile(
            s.query(A).filter(A.d.has(D.id == 1)),
            "SELECT a.id AS a_id, a.b_id AS a_b_id FROM a WHERE EXISTS "
            "(SELECT 1 FROM d, b JOIN c ON c.id = b.c_id "
            "WHERE a.b_id = b.id AND c.d_id = d.id AND d.id = :id_1)",
        )

    def test_has_many_to_one(self):
        B, C = self.classes("B", "C")
        s = fixture_session()
        self.assert_compile(
            s.query(B).filter(B.c.has(C.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id FROM b WHERE "
            "EXISTS (SELECT 1 FROM c WHERE c.id = b.c_id AND c.id = :id_1)",
        )

    def test_any_many_to_many(self):
        B, D = self.classes("B", "D")
        s = fixture_session()
        self.assert_compile(
            s.query(B).filter(B.d.any(D.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id FROM b WHERE "
            "EXISTS (SELECT 1 FROM b_d, d WHERE b.id = b_d.bid "
            "AND d.id = b_d.did AND d.id = :id_1)",
        )

    def test_any_one_to_many(self):
        B, C = self.classes("B", "C")
        s = fixture_session()
        self.assert_compile(
            s.query(C).filter(C.bs.any(B.id == 1)),
            "SELECT c.id AS c_id, c.d_id AS c_d_id FROM c WHERE "
            "EXISTS (SELECT 1 FROM b WHERE c.id = b.c_id AND b.id = :id_1)",
        )

    def test_any_many_to_many_doesnt_overcorrelate(self):
        B, D = self.classes("B", "D")
        s = fixture_session()

        self.assert_compile(
            s.query(B).join(B.d).filter(B.d.any(D.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id FROM "
            "b JOIN b_d AS b_d_1 ON b.id = b_d_1.bid "
            "JOIN d ON d.id = b_d_1.did WHERE "
            "EXISTS (SELECT 1 FROM b_d, d WHERE b.id = b_d.bid "
            "AND d.id = b_d.did AND d.id = :id_1)",
        )

    def test_has_doesnt_overcorrelate(self):
        B, C = self.classes("B", "C")
        s = fixture_session()

        self.assert_compile(
            s.query(B).join(B.c).filter(B.c.has(C.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id "
            "FROM b JOIN c ON c.id = b.c_id "
            "WHERE EXISTS "
            "(SELECT 1 FROM c WHERE c.id = b.c_id AND c.id = :id_1)",
        )

    def test_has_doesnt_get_aliased_join_subq(self):
        B, C = self.classes("B", "C")
        s = fixture_session()

        ca = aliased(C)
        self.assert_compile(
            s.query(B).join(ca, B.c).filter(B.c.has(C.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id "
            "FROM b JOIN c AS c_1 ON c_1.id = b.c_id "
            "WHERE EXISTS "
            "(SELECT 1 FROM c WHERE c.id = b.c_id AND c.id = :id_1)",
        )

    def test_any_many_to_many_doesnt_get_aliased_join_subq(self):
        B, D = self.classes("B", "D")
        s = fixture_session()

        da = aliased(D)
        self.assert_compile(
            s.query(B).join(da, B.d).filter(B.d.any(D.id == 1)),
            "SELECT b.id AS b_id, b.c_id AS b_c_id "
            "FROM b JOIN b_d AS b_d_1 ON b.id = b_d_1.bid "
            "JOIN d AS d_1 ON d_1.id = b_d_1.did "
            "WHERE EXISTS "
            "(SELECT 1 FROM b_d, d WHERE b.id = b_d.bid "
            "AND d.id = b_d.did AND d.id = :id_1)",
        )


class HasMapperEntitiesTest(QueryTest):
    def test_entity(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User)

        assert q._compile_state()._has_mapper_entities

    def test_cols(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User.id)

        assert not q._compile_state()._has_mapper_entities

    def test_cols_set_entities(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User.id)

        q._set_entities(User)
        assert q._compile_state()._has_mapper_entities

    def test_entity_set_entities(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User)

        q._set_entities(User.id)
        assert not q._compile_state()._has_mapper_entities


class SetOpsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_union(self):
        User = self.classes.User

        s = fixture_session()

        fred = s.query(User).filter(User.name == "fred")
        ed = s.query(User).filter(User.name == "ed")
        jack = s.query(User).filter(User.name == "jack")

        eq_(
            fred.union(ed).order_by(User.name).all(),
            [User(name="ed"), User(name="fred")],
        )

        eq_(
            fred.union(ed, jack).order_by(User.name).all(),
            [User(name="ed"), User(name="fred"), User(name="jack")],
        )

        eq_(
            fred.union(ed).union(jack).order_by(User.name).all(),
            [User(name="ed"), User(name="fred"), User(name="jack")],
        )

    def test_statement_labels(self):
        """test that label conflicts don't occur with joins etc."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q1 = (
            s.query(User, Address)
            .join(User.addresses)
            .filter(Address.email_address == "ed@wood.com")
        )
        q2 = (
            s.query(User, Address)
            .join(User.addresses)
            .filter(Address.email_address == "jack@bean.com")
        )
        q3 = q1.union(q2).order_by(User.name)

        eq_(
            q3.all(),
            [
                (User(name="ed"), Address(email_address="ed@wood.com")),
                (User(name="jack"), Address(email_address="jack@bean.com")),
            ],
        )

    def test_union_literal_expressions_compile(self):
        """test that column expressions translate during
        the _from_statement() portion of union(), others"""

        User = self.classes.User

        s = fixture_session()
        q1 = s.query(User, literal("x"))
        q2 = s.query(User, literal_column("'y'"))
        q3 = q1.union(q2)

        self.assert_compile(
            q3,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.anon_2 AS anon_1_anon_2 FROM "
            "(SELECT users.id AS users_id, users.name AS users_name, "
            ":param_1 AS anon_2 FROM users "
            "UNION SELECT users.id AS users_id, users.name AS users_name, "
            "'y' FROM users) AS anon_1",
        )

    def test_union_literal_expressions_results(self):
        User = self.classes.User

        s = fixture_session()

        x_literal = literal("x")
        q1 = s.query(User, x_literal)
        q2 = s.query(User, literal_column("'y'"))
        q3 = q1.union(q2)

        q4 = s.query(User, literal_column("'x'").label("foo"))
        q5 = s.query(User, literal("y"))
        q6 = q4.union(q5)

        eq_([x["name"] for x in q6.column_descriptions], ["User", "foo"])

        for q in (
            q3.order_by(User.id, x_literal),
            q6.order_by(User.id, "foo"),
        ):
            eq_(
                q.all(),
                [
                    (User(id=7, name="jack"), "x"),
                    (User(id=7, name="jack"), "y"),
                    (User(id=8, name="ed"), "x"),
                    (User(id=8, name="ed"), "y"),
                    (User(id=9, name="fred"), "x"),
                    (User(id=9, name="fred"), "y"),
                    (User(id=10, name="chuck"), "x"),
                    (User(id=10, name="chuck"), "y"),
                ],
            )

    def test_union_labeled_anonymous_columns(self):
        User = self.classes.User

        s = fixture_session()

        c1, c2 = column("c1"), column("c2")
        q1 = s.query(User, c1.label("foo"), c1.label("bar"))
        q2 = s.query(User, c1.label("foo"), c2.label("bar"))
        q3 = q1.union(q2)

        eq_(
            [x["name"] for x in q3.column_descriptions], ["User", "foo", "bar"]
        )

        self.assert_compile(
            q3,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.foo AS anon_1_foo, anon_1.bar AS anon_1_bar "
            "FROM (SELECT users.id AS users_id, users.name AS users_name, "
            "c1 AS foo, c1 AS bar FROM users UNION SELECT users.id AS "
            "users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1",
        )

    def test_order_by_anonymous_col(self):
        User = self.classes.User

        s = fixture_session()

        c1, c2 = column("c1"), column("c2")
        f = c1.label("foo")
        q1 = s.query(User, f, c2.label("bar"))
        q2 = s.query(User, c1.label("foo"), c2.label("bar"))
        q3 = q1.union(q2)

        self.assert_compile(
            q3.order_by(c1),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name, anon_1.foo AS anon_1_foo, anon_1.bar AS "
            "anon_1_bar FROM (SELECT users.id AS users_id, users.name AS "
            "users_name, c1 AS foo, c2 AS bar "
            "FROM users UNION SELECT users.id "
            "AS users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1 ORDER BY anon_1.foo",
        )

        self.assert_compile(
            q3.order_by(f),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name, anon_1.foo AS anon_1_foo, anon_1.bar AS "
            "anon_1_bar FROM (SELECT users.id AS users_id, users.name AS "
            "users_name, c1 AS foo, c2 AS bar "
            "FROM users UNION SELECT users.id "
            "AS users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1 ORDER BY anon_1.foo",
        )

    def test_union_mapped_colnames_preserved_across_subquery(self):
        User = self.classes.User

        s = fixture_session()
        q1 = s.query(User.name)
        q2 = s.query(User.name)

        # the label names in the subquery are the typical anonymized ones
        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.name AS users_name FROM users "
            "UNION SELECT users.name AS users_name FROM users) AS anon_1",
        )

        # but in the returned named tuples,
        # due to [ticket:1942], this should be 'name', not 'users_name'
        eq_([x["name"] for x in q1.union(q2).column_descriptions], ["name"])

    @testing.requires.intersect
    def test_intersect(self):
        User = self.classes.User

        s = fixture_session()

        fred = s.query(User).filter(User.name == "fred")
        ed = s.query(User).filter(User.name == "ed")
        jack = s.query(User).filter(User.name == "jack")
        eq_(fred.intersect(ed, jack).all(), [])

        eq_(fred.union(ed).intersect(ed.union(jack)).all(), [User(name="ed")])

    def test_eager_load(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        fred = s.query(User).filter(User.name == "fred")
        ed = s.query(User).filter(User.name == "ed")

        def go():
            eq_(
                fred.union(ed)
                .order_by(User.name)
                .options(joinedload(User.addresses))
                .all(),
                [
                    User(
                        name="ed", addresses=[Address(), Address(), Address()]
                    ),
                    User(name="fred", addresses=[Address()]),
                ],
            )

        self.assert_sql_count(testing.db, go, 1)


class AggregateTest(QueryTest):
    def test_sum(self):
        Order = self.classes.Order

        sess = fixture_session()
        orders = sess.query(Order).filter(Order.id.in_([2, 3, 4]))
        eq_(
            orders.with_entities(
                func.sum(Order.user_id * Order.address_id)
            ).scalar(),
            79,
        )

    def test_apply(self):
        Order = self.classes.Order

        sess = fixture_session()
        assert sess.query(func.sum(Order.user_id * Order.address_id)).filter(
            Order.id.in_([2, 3, 4])
        ).one() == (79,)

    def test_having(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        assert [User(name="ed", id=8)] == sess.query(User).order_by(
            User.id
        ).group_by(User).join("addresses").having(
            func.count(Address.id) > 2
        ).all()

        assert [
            User(name="jack", id=7),
            User(name="fred", id=9),
        ] == sess.query(User).order_by(User.id).group_by(User).join(
            "addresses"
        ).having(
            func.count(Address.id) < 2
        ).all()


class ExistsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_exists(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query(User)
        self.assert_compile(
            sess.query(q1.exists()),
            "SELECT EXISTS (" "SELECT 1 FROM users" ") AS anon_1",
        )

        q2 = sess.query(User).filter(User.name == "fred")
        self.assert_compile(
            sess.query(q2.exists()),
            "SELECT EXISTS ("
            "SELECT 1 FROM users WHERE users.name = :name_1"
            ") AS anon_1",
        )

    def test_exists_col_expression(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query(User.id)
        self.assert_compile(
            sess.query(q1.exists()),
            "SELECT EXISTS (" "SELECT 1 FROM users" ") AS anon_1",
        )

    def test_exists_labeled_col_expression(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query(User.id.label("foo"))
        self.assert_compile(
            sess.query(q1.exists()),
            "SELECT EXISTS (" "SELECT 1 FROM users" ") AS anon_1",
        )

    def test_exists_arbitrary_col_expression(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query(func.foo(User.id))
        self.assert_compile(
            sess.query(q1.exists()),
            "SELECT EXISTS (" "SELECT 1 FROM users" ") AS anon_1",
        )

    def test_exists_col_warning(self):
        User = self.classes.User
        Address = self.classes.Address
        sess = fixture_session()

        q1 = sess.query(User, Address).filter(User.id == Address.user_id)
        self.assert_compile(
            sess.query(q1.exists()),
            "SELECT EXISTS ("
            "SELECT 1 FROM users, addresses "
            "WHERE users.id = addresses.user_id"
            ") AS anon_1",
        )

    def test_exists_w_select_from(self):
        User = self.classes.User
        sess = fixture_session()

        q1 = sess.query().select_from(User).exists()
        self.assert_compile(
            sess.query(q1), "SELECT EXISTS (SELECT 1 FROM users) AS anon_1"
        )


class CountTest(QueryTest):
    def test_basic(self):
        users, User = self.tables.users, self.classes.User

        s = fixture_session()

        eq_(s.query(User).count(), 4)

        eq_(s.query(User).filter(users.c.name.endswith("ed")).count(), 2)

    def test_basic_future(self):
        User = self.classes.User

        s = fixture_session()

        eq_(
            s.execute(select(func.count()).select_from(User)).scalar(),
            4,
        )

        eq_(
            s.execute(
                select(func.count()).filter(User.name.endswith("ed"))
            ).scalar(),
            2,
        )

    def test_loader_options_ignored(self):
        """test the count()-specific legacy behavior that loader
        options are effectively ignored, as they previously were applied
        before the count() function would be.

        """

        User = self.classes.User

        s = fixture_session()

        eq_(s.query(User).options(joinedload(User.addresses)).count(), 4)

    def test_count_char(self):
        User = self.classes.User
        s = fixture_session()
        # '*' is favored here as the most common character,
        # it is reported that Informix doesn't like count(1),
        # rumors about Oracle preferring count(1) don't appear
        # to be well founded.
        self.assert_sql_execution(
            testing.db,
            s.query(User).count,
            CompiledSQL(
                "SELECT count(*) AS count_1 FROM "
                "(SELECT users.id AS users_id, users.name "
                "AS users_name FROM users) AS anon_1",
                {},
            ),
        )

    def test_multiple_entity(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(User, Address).join(Address, true())
        eq_(q.count(), 20)  # cartesian product

        q = s.query(User, Address).join(User.addresses)
        eq_(q.count(), 5)

    def test_multiple_entity_future(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        stmt = select(User, Address).join(Address, true())

        stmt = select(func.count()).select_from(stmt.subquery())
        eq_(s.scalar(stmt), 20)  # cartesian product

        stmt = select(User, Address).join(Address)

        stmt = select(func.count()).select_from(stmt.subquery())
        eq_(s.scalar(stmt), 5)

    def test_nested(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(User, Address).join(Address, true()).limit(2)
        eq_(q.count(), 2)

        q = s.query(User, Address).join(Address, true()).limit(100)
        eq_(q.count(), 20)

        q = s.query(User, Address).join(User.addresses).limit(100)
        eq_(q.count(), 5)

    def test_nested_future(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        stmt = select(User, Address).join(Address, true()).limit(2)
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            2,
        )

        stmt = select(User, Address).join(Address, true()).limit(100)
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            20,
        )

        stmt = select(User, Address).join(Address).limit(100)
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            5,
        )

    def test_cols(self):
        """test that column-based queries always nest."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        q = s.query(func.count(distinct(User.name)))
        eq_(q.count(), 1)

        q = s.query(func.count(distinct(User.name))).distinct()
        eq_(q.count(), 1)

        q = s.query(User.name)
        eq_(q.count(), 4)

        q = s.query(User.name, Address).join(Address, true())
        eq_(q.count(), 20)

        q = s.query(Address.user_id)
        eq_(q.count(), 5)
        eq_(q.distinct().count(), 3)

    def test_cols_future(self):

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        stmt = select(func.count(distinct(User.name)))
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            1,
        )

        stmt = select(func.count(distinct(User.name))).distinct()

        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            1,
        )

        stmt = select(User.name)
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            4,
        )

        stmt = select(User.name, Address).join(Address, true())
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            20,
        )

        stmt = select(Address.user_id)
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            5,
        )

        stmt = stmt.distinct()
        eq_(
            s.scalar(select(func.count()).select_from(stmt.subquery())),
            3,
        )


class DistinctTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_basic(self):
        User = self.classes.User

        eq_(
            [User(id=7), User(id=8), User(id=9), User(id=10)],
            fixture_session().query(User).order_by(User.id).distinct().all(),
        )
        eq_(
            [User(id=7), User(id=9), User(id=8), User(id=10)],
            fixture_session()
            .query(User)
            .distinct()
            .order_by(desc(User.name))
            .all(),
        )

    def test_basic_standalone(self):
        User = self.classes.User

        # issue 6008.  the UnaryExpression now places itself into the
        # result map so that it can be matched positionally without the need
        # for any label.
        q = fixture_session().query(distinct(User.id)).order_by(User.id)
        self.assert_compile(
            q, "SELECT DISTINCT users.id FROM users ORDER BY users.id"
        )
        eq_([(7,), (8,), (9,), (10,)], q.all())

    def test_standalone_w_subquery(self):
        User = self.classes.User
        q = fixture_session().query(distinct(User.id))

        subq = q.subquery()
        q = fixture_session().query(subq).order_by(subq.c[0])
        eq_([(7,), (8,), (9,), (10,)], q.all())

    def test_no_automatic_distinct_thing_w_future(self):
        User = self.classes.User

        stmt = select(User.id).order_by(User.name).distinct()

        self.assert_compile(
            stmt, "SELECT DISTINCT users.id FROM users ORDER BY users.name"
        )

    def test_issue_5470_one(self):
        User = self.classes.User

        expr = (User.id.op("+")(2)).label("label")

        sess = fixture_session()

        q = sess.query(expr).select_from(User).order_by(desc(expr)).distinct()

        # no double col in the select list,
        # orders by the label
        self.assert_compile(
            q,
            "SELECT DISTINCT users.id + :id_1 AS label "
            "FROM users ORDER BY label DESC",
        )

    def test_issue_5470_two(self):
        User = self.classes.User

        expr = User.id + literal(1)

        sess = fixture_session()
        q = sess.query(expr).select_from(User).order_by(asc(expr)).distinct()

        # no double col in the select list,
        # there's no label so this is the requested SQL
        self.assert_compile(
            q,
            "SELECT DISTINCT users.id + :param_1 AS anon_1 "
            "FROM users ORDER BY users.id + :param_1 ASC",
        )

    def test_issue_5470_three(self):
        User = self.classes.User

        expr = (User.id + literal(1)).label("label")

        sess = fixture_session()
        q = sess.query(expr).select_from(User).order_by(asc(expr)).distinct()

        # no double col in the select list,
        # orders by the label
        self.assert_compile(
            q,
            "SELECT DISTINCT users.id + :param_1 AS label "
            "FROM users ORDER BY label ASC",
        )

    def test_issue_5470_four(self):
        User = self.classes.User

        expr = (User.id + literal(1)).label("label")

        sess = fixture_session()
        q = (
            sess.query(expr)
            .select_from(User)
            .order_by(asc("label"))
            .distinct()
        )

        # no double col in the select list,
        # orders by the label
        self.assert_compile(
            q,
            "SELECT DISTINCT users.id + :param_1 AS label "
            "FROM users ORDER BY label ASC",
        )

    def test_issue_5470_five(self):
        User = self.classes.User

        expr = (User.id.op("+")(2)).label("label")

        stmt = select(expr).select_from(User).order_by(desc(expr)).distinct()

        # no double col in the select list,
        # orders by the label
        self.assert_compile(
            stmt,
            "SELECT DISTINCT users.id + :id_1 AS label "
            "FROM users ORDER BY label DESC",
        )

    def test_columns_augmented_roundtrip_one_from_subq(self):
        """Test workaround for legacy style DISTINCT on extra column.

        See #5134

        """
        User, Address = self.classes.User, self.classes.Address
        sess = fixture_session()

        subq = (
            sess.query(User, Address.email_address)
            .join("addresses")
            .distinct()
            .subquery()
        )
        ua = aliased(User, subq)
        aa = aliased(Address, subq)
        q = sess.query(ua).order_by(desc(aa.email_address))

        eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_one_aliased(self):
        """Test workaround for legacy style DISTINCT on extra column,
        but also without using from_self().

        See #5134

        """
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        q = (
            sess.query(User, Address.email_address)
            .join("addresses")
            .distinct()
        )

        subq = q.subquery()

        entity = aliased(User, subq)
        q = sess.query(entity).order_by(subq.c.email_address.desc())

        eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_two(self):
        """Test workaround for legacy style DISTINCT on extra column.

        See #5134

        """

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        # test that it works on embedded joinedload/LIMIT subquery
        q = (
            sess.query(User)
            .join("addresses")
            .distinct()
            .options(joinedload("addresses"))
            .order_by(desc(Address.email_address))
            .limit(2)
        )

        def go():
            assert [
                User(id=7, addresses=[Address(id=1)]),
                User(id=9, addresses=[Address(id=5)]),
            ] == q.all()

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_augmented_roundtrip_three_from_self(self):
        """Test workaround for legacy style DISTINCT on extra column.

        See #5134

        """

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        subq = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
                Address.email_address,
            )
            .join(Address, true())
            .filter(User.name == "jack")
            .filter(User.id + Address.user_id > 0)
            .distinct()
            .subquery()
        )

        ua, aa = aliased(User, subq), aliased(Address, subq)

        q = sess.query(ua.id, ua.name.label("foo"), aa.id).order_by(
            ua.id, ua.name, aa.email_address
        )

        eq_(
            q.all(),
            [
                (7, "jack", 3),
                (7, "jack", 4),
                (7, "jack", 2),
                (7, "jack", 5),
                (7, "jack", 1),
            ],
        )
        for row in q:
            eq_(row._mapping.keys(), ["id", "foo", "id"])

    def test_columns_augmented_roundtrip_three_aliased(self):
        """Test workaround for legacy style DISTINCT on extra column,
        but also without using from_self().

        See #5134

        """

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
                Address.email_address,
            )
            .join(Address, true())
            .filter(User.name == "jack")
            .filter(User.id + Address.user_id > 0)
            .distinct()
        )

        subq = q.subquery()

        # note this is a bit cutting edge; two differnet entities against
        # the same subquery.
        uentity = aliased(User, subq)
        aentity = aliased(Address, subq)

        q = sess.query(
            uentity.id, uentity.name.label("foo"), aentity.id
        ).order_by(uentity.id, uentity.name, aentity.email_address)

        eq_(
            q.all(),
            [
                (7, "jack", 3),
                (7, "jack", 4),
                (7, "jack", 2),
                (7, "jack", 5),
                (7, "jack", 1),
            ],
        )

    def test_columns_augmented_sql_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        subq = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
                Address.email_address,
            )
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        ua, aa = aliased(User, subq), aliased(Address, subq)

        q = sess.query(ua.id, ua.name.label("foo"), aa.id)

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.foo AS foo, "
            "anon_1.addresses_id AS anon_1_addresses_id "
            "FROM ("
            "SELECT DISTINCT users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, addresses.email_address AS "
            "addresses_email_address FROM users, addresses ORDER BY "
            "users.id, users.name, addresses.email_address"
            ") AS anon_1",
        )

    def test_columns_augmented_sql_union_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
                Address.email_address,
            )
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
        )
        q2 = sess.query(
            User.id,
            User.name.label("foo"),
            Address.id,
            Address.email_address,
        )

        self.assert_compile(
            q.union(q2),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.foo AS anon_1_foo, anon_1.addresses_id AS "
            "anon_1_addresses_id, anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address FROM "
            "((SELECT DISTINCT users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, addresses.email_address "
            "AS addresses_email_address FROM users, addresses "
            "ORDER BY users.id, users.name, addresses.email_address) "
            "UNION SELECT users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, addresses.email_address AS "
            "addresses_email_address FROM users, addresses) AS anon_1",
        )

    def test_columns_augmented_sql_union_two(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
            )
            .distinct(Address.email_address)
            .order_by(User.id, User.name)
        )
        q2 = sess.query(User.id, User.name.label("foo"), Address.id)

        self.assert_compile(
            q.union(q2),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.foo AS anon_1_foo, anon_1.addresses_id AS "
            "anon_1_addresses_id FROM "
            "((SELECT DISTINCT ON (addresses.email_address) users.id "
            "AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id FROM users, addresses "
            "ORDER BY users.id, users.name) "
            "UNION SELECT users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id FROM users, addresses) AS anon_1",
            dialect="postgresql",
        )

    def test_columns_augmented_sql_two(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User)
            .options(joinedload(User.addresses))
            .distinct()
            .order_by(User.name, Address.email_address)
            .limit(5)
        )

        # addresses.email_address is added to inner query so that
        # it is available in ORDER BY
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT DISTINCT users.id AS users_id, "
            "users.name AS users_name, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "ORDER BY users.name, addresses.email_address "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN "
            "addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name, "
            "anon_1.addresses_email_address, addresses_1.id",
        )

    def test_columns_augmented_sql_three(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .distinct(User.name)
            .order_by(User.id, User.name, Address.email_address)
        )

        # no columns are added when DISTINCT ON is used
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (users.name) users.id AS users_id, "
            "users.name AS foo, addresses.id AS addresses_id FROM users, "
            "addresses ORDER BY users.id, users.name, addresses.email_address",
            dialect="postgresql",
        )

    def test_columns_augmented_distinct_on(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        subq = (
            sess.query(
                User.id,
                User.name.label("foo"),
                Address.id,
                Address.email_address,
            )
            .distinct(Address.email_address)
            .order_by(User.id, User.name, Address.email_address)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        ua = aliased(User, subq)
        aa = aliased(Address, subq)
        q = sess.query(ua.id, ua.name.label("foo"), aa.id)

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.foo AS foo, "
            "anon_1.addresses_id AS anon_1_addresses_id "
            "FROM ("
            "SELECT DISTINCT ON (addresses.email_address) "
            "users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, addresses.email_address AS "
            "addresses_email_address FROM users, addresses ORDER BY "
            "users.id, users.name, addresses.email_address"
            ") AS anon_1",
            dialect="postgresql",
        )

    def test_columns_augmented_sql_three_using_label_reference(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .distinct("name")
            .order_by(User.id, User.name, Address.email_address)
        )

        # no columns are added when DISTINCT ON is used
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (users.name) users.id AS users_id, "
            "users.name AS foo, addresses.id AS addresses_id FROM users, "
            "addresses ORDER BY users.id, users.name, addresses.email_address",
            dialect="postgresql",
        )

    def test_columns_augmented_sql_illegal_label_reference(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = sess.query(User.id, User.name.label("foo"), Address.id).distinct(
            "not a label"
        )

        from sqlalchemy.dialects import postgresql

        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / "
            "GROUP BY / DISTINCT etc.",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
            dialect=postgresql.dialect(),
        )

    def test_columns_augmented_sql_four(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User)
            .join("addresses")
            .distinct(Address.email_address)
            .options(joinedload("addresses"))
            .order_by(desc(Address.email_address))
            .limit(2)
        )

        # but for the subquery / eager load case, we still need to make
        # the inner columns available for the ORDER BY even though its
        # a DISTINCT ON
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT DISTINCT ON (addresses.email_address) "
            "users.id AS users_id, users.name AS users_name, "
            "addresses.email_address AS addresses_email_address "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "ORDER BY addresses.email_address DESC  "
            "LIMIT %(param_1)s) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.addresses_email_address DESC, addresses_1.id",
            dialect="postgresql",
        )


class PrefixSuffixWithTest(QueryTest, AssertsCompiledSQL):
    def test_one_prefix(self):
        User = self.classes.User
        sess = fixture_session()
        query = sess.query(User.name).prefix_with("PREFIX_1")
        expected = "SELECT PREFIX_1 " "users.name AS users_name FROM users"
        self.assert_compile(query, expected, dialect=default.DefaultDialect())

    def test_one_suffix(self):
        User = self.classes.User
        sess = fixture_session()
        query = sess.query(User.name).suffix_with("SUFFIX_1")
        # trailing space for some reason
        expected = "SELECT users.name AS users_name FROM users SUFFIX_1 "
        self.assert_compile(query, expected, dialect=default.DefaultDialect())

    def test_many_prefixes(self):
        User = self.classes.User
        sess = fixture_session()
        query = sess.query(User.name).prefix_with("PREFIX_1", "PREFIX_2")
        expected = (
            "SELECT PREFIX_1 PREFIX_2 " "users.name AS users_name FROM users"
        )
        self.assert_compile(query, expected, dialect=default.DefaultDialect())

    def test_chained_prefixes(self):
        User = self.classes.User
        sess = fixture_session()
        query = (
            sess.query(User.name)
            .prefix_with("PREFIX_1")
            .prefix_with("PREFIX_2", "PREFIX_3")
        )
        expected = (
            "SELECT PREFIX_1 PREFIX_2 PREFIX_3 "
            "users.name AS users_name FROM users"
        )
        self.assert_compile(query, expected, dialect=default.DefaultDialect())


class YieldTest(_fixtures.FixtureTest):
    run_setup_mappers = "each"
    run_inserts = "each"

    def _eagerload_mappings(self, addresses_lazy=True, user_lazy=True):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    lazy=addresses_lazy,
                    backref=backref("user", lazy=user_lazy),
                )
            },
        )
        mapper(Address, addresses)

    def test_basic(self):
        self._eagerload_mappings()

        User = self.classes.User

        sess = fixture_session()
        q = iter(
            sess.query(User)
            .yield_per(1)
            .from_statement(text("select * from users"))
        )

        ret = []
        eq_(len(sess.identity_map), 0)
        ret.append(next(q))
        ret.append(next(q))
        eq_(len(sess.identity_map), 2)
        ret.append(next(q))
        ret.append(next(q))
        eq_(len(sess.identity_map), 4)
        try:
            next(q)
            assert False
        except StopIteration:
            pass

    def test_yield_per_and_execution_options_legacy(self):
        self._eagerload_mappings()

        User = self.classes.User

        sess = fixture_session()

        @event.listens_for(sess, "do_orm_execute")
        def check(ctx):
            eq_(ctx.load_options._yield_per, 15)
            eq_(
                {
                    k: v
                    for k, v in ctx.execution_options.items()
                    if not k.startswith("_")
                },
                {"max_row_buffer": 15, "stream_results": True, "foo": "bar"},
            )

        q = sess.query(User).yield_per(15)
        q = q.execution_options(foo="bar")

        q.all()

    def test_yield_per_and_execution_options(self):
        self._eagerload_mappings()

        User = self.classes.User

        sess = fixture_session()

        @event.listens_for(sess, "do_orm_execute")
        def check(ctx):
            eq_(ctx.load_options._yield_per, 15)
            eq_(
                {
                    k: v
                    for k, v in ctx.execution_options.items()
                    if not k.startswith("_")
                },
                {
                    "max_row_buffer": 15,
                    "stream_results": True,
                    "yield_per": 15,
                },
            )

        stmt = select(User).execution_options(yield_per=15)
        sess.execute(stmt)

    def test_no_joinedload_opt(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = fixture_session()
        q = sess.query(User).options(joinedload("addresses")).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Can't use yield_per with eager loaders that require "
            "uniquing or row buffering",
            q.all,
        )

    def test_no_subqueryload_opt(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = fixture_session()
        q = sess.query(User).options(subqueryload("addresses")).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Can't use yield_per with eager loaders that require "
            "uniquing or row buffering",
            q.all,
        )

    def test_no_subqueryload_mapping(self):
        self._eagerload_mappings(addresses_lazy="subquery")

        User = self.classes.User
        sess = fixture_session()
        q = sess.query(User).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Can't use yield_per with eager loaders that require "
            "uniquing or row buffering",
            q.all,
        )

    def test_joinedload_m2o_ok(self):
        self._eagerload_mappings(user_lazy="joined")
        Address = self.classes.Address
        sess = fixture_session()
        q = sess.query(Address).yield_per(1)
        q.all()

    def test_eagerload_opt_disable(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = fixture_session()
        q = (
            sess.query(User)
            .options(subqueryload("addresses"))
            .enable_eagerloads(False)
            .yield_per(1)
        )
        q.all()

        q = (
            sess.query(User)
            .options(joinedload("addresses"))
            .enable_eagerloads(False)
            .yield_per(1)
        )
        q.all()

    def test_m2o_joinedload_not_others(self):
        self._eagerload_mappings(addresses_lazy="joined")
        Address = self.classes.Address
        sess = fixture_session()
        q = (
            sess.query(Address)
            .options(lazyload("*"), joinedload("user"))
            .yield_per(1)
            .filter_by(id=1)
        )

        def go():
            result = q.all()
            assert result[0].user

        self.assert_sql_count(testing.db, go, 1)


class HintsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_hints(self):
        User = self.classes.User

        from sqlalchemy.dialects import mysql

        dialect = mysql.dialect()

        sess = fixture_session()

        self.assert_compile(
            sess.query(User).with_hint(
                User, "USE INDEX (col1_index,col2_index)"
            ),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users USE INDEX (col1_index,col2_index)",
            dialect=dialect,
        )

        self.assert_compile(
            sess.query(User).with_hint(
                User, "WITH INDEX col1_index", "sybase"
            ),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users",
            dialect=dialect,
        )

        ualias = aliased(User)
        self.assert_compile(
            sess.query(User, ualias)
            .with_hint(ualias, "USE INDEX (col1_index,col2_index)")
            .join(ualias, ualias.id > User.id),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users INNER JOIN users AS users_1 "
            "USE INDEX (col1_index,col2_index) "
            "ON users_1.id > users.id",
            dialect=dialect,
        )

    def test_statement_hints(self):
        User = self.classes.User

        sess = fixture_session()
        stmt = (
            sess.query(User)
            .with_statement_hint("test hint one")
            .with_statement_hint("test hint two")
            .with_statement_hint("test hint three", "postgresql")
        )

        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users test hint one test hint two",
        )

        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users test hint one test hint two test hint three",
            dialect="postgresql",
        )


class TextTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_needs_text(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            "Textual SQL expression",
            fixture_session().query(User).from_statement,
            "select * from users order by id",
        )

    def test_select_star(self):
        User = self.classes.User

        eq_(
            fixture_session()
            .query(User)
            .from_statement(text("select * from users order by id"))
            .first(),
            User(id=7),
        )
        eq_(
            fixture_session()
            .query(User)
            .from_statement(
                text("select * from users where name='nonexistent'")
            )
            .first(),
            None,
        )

    def test_select_star_future(self):
        User = self.classes.User

        sess = fixture_session()
        eq_(
            sess.execute(
                select(User).from_statement(
                    text("select * from users order by id")
                )
            )
            .scalars()
            .first(),
            User(id=7),
        )
        eq_(
            sess.execute(
                select(User).from_statement(
                    text("select * from users where name='nonexistent'")
                )
            )
            .scalars()
            .first(),
            None,
        )

    def test_columns_mismatched(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter
        User = self.classes.User

        s = fixture_session()
        q = s.query(User).from_statement(
            text(
                "select name, 27 as foo, id as users_id from users order by id"
            )
        )
        eq_(
            q.all(),
            [
                User(id=7, name="jack"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=10, name="chuck"),
            ],
        )

    def test_columns_mismatched_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter
        User = self.classes.User

        s = fixture_session()
        q = select(User).from_statement(
            text(
                "select name, 27 as foo, id as users_id from users order by id"
            )
        )
        eq_(
            s.execute(q).scalars().all(),
            [
                User(id=7, name="jack"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=10, name="chuck"),
            ],
        )

    def test_columns_multi_table_uselabels(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User, Address).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            )
        )

        eq_(
            q.all(),
            [
                (User(id=8), Address(id=2)),
                (User(id=8), Address(id=3)),
                (User(id=8), Address(id=4)),
            ],
        )

    def test_columns_multi_table_uselabels_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = select(User, Address).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            )
        )

        eq_(
            s.execute(q).all(),
            [
                (User(id=8), Address(id=2)),
                (User(id=8), Address(id=3)),
                (User(id=8), Address(id=4)),
            ],
        )

    def test_columns_multi_table_uselabels_contains_eager(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                )
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = q.all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_contains_eager_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            select(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                )
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = s.execute(q).unique().scalars().all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_cols_contains_eager(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                ).columns(User.name, User.id, Address.id)
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = q.all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_cols_contains_eager_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            select(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                ).columns(User.name, User.id, Address.id)
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = s.execute(q).unique().scalars().all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_textual_select_orm_columns(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address
        users = self.tables.users
        addresses = self.tables.addresses

        s = fixture_session()
        q = s.query(User.name, User.id, Address.id).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            ).columns(users.c.name, users.c.id, addresses.c.id)
        )

        eq_(q.all(), [("ed", 8, 2), ("ed", 8, 3), ("ed", 8, 4)])

    @testing.combinations(
        (
            False,
            subqueryload,
            # sqlite seems happy to interpret the broken SQL and give you the
            # correct result somehow, this is a bug in SQLite so don't rely
            # upon it doing that
            testing.fails("not working yet") + testing.skip_if("sqlite"),
        ),
        (True, subqueryload, testing.fails("not sure about implementation")),
        (False, selectinload),
        (True, selectinload),
    )
    def test_related_eagerload_against_text(self, add_columns, loader_option):
        # new in 1.4.   textual selects have columns so subqueryloaders
        # and selectinloaders can join onto them.   we add columns
        # automatiacally to TextClause as well, however subqueryloader
        # is not working at the moment due to execution model refactor,
        # it creates a subquery w/ adapter before those columns are
        # available.  this is a super edge case and as we want to rewrite
        # the loaders to use select(), maybe we can get it then.
        User = self.classes.User

        text_clause = text("select * from users")
        if add_columns:
            text_clause = text_clause.columns(User.id, User.name)

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(text_clause)
            .options(loader_option(User.addresses))
        )

        def go():
            eq_(set(q.all()), set(self.static.user_address_result))

        self.assert_sql_count(testing.db, go, 2)

    def test_whereclause(self):
        User = self.classes.User

        eq_(
            fixture_session().query(User).filter(text("id in (8, 9)")).all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            fixture_session()
            .query(User)
            .filter(text("name='fred'"))
            .filter(text("id=9"))
            .all(),
            [User(id=9)],
        )
        eq_(
            fixture_session()
            .query(User)
            .filter(text("name='fred'"))
            .filter(User.id == 9)
            .all(),
            [User(id=9)],
        )

    def test_whereclause_future(self):
        User = self.classes.User

        s = fixture_session()
        eq_(
            s.execute(select(User).filter(text("id in (8, 9)")))
            .scalars()
            .all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            s.execute(
                select(User).filter(text("name='fred'")).filter(text("id=9"))
            )
            .scalars()
            .all(),
            [User(id=9)],
        )
        eq_(
            s.execute(
                select(User).filter(text("name='fred'")).filter(User.id == 9)
            )
            .scalars()
            .all(),
            [User(id=9)],
        )

    def test_binds_coerce(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            r"Textual SQL expression 'id in \(:id1, :id2\)' "
            "should be explicitly declared",
            fixture_session().query(User).filter,
            "id in (:id1, :id2)",
        )

    def test_plain_textual_column(self):
        User = self.classes.User

        s = fixture_session()

        self.assert_compile(
            s.query(User.id, text("users.name")),
            "SELECT users.id AS users_id, users.name FROM users",
        )

        eq_(
            s.query(User.id, text("users.name")).all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

        eq_(
            s.query(User.id, literal_column("name")).order_by(User.id).all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

    def test_via_select(self):
        User = self.classes.User
        s = fixture_session()
        eq_(
            s.query(User)
            .from_statement(
                select(column("id"), column("name"))
                .select_from(table("users"))
                .order_by("id")
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .from_statement(
                text("select * from users order by id").columns(
                    id=Integer, name=String
                )
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_columns_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User.id, User.name)
            .from_statement(
                text("select * from users order by id").columns(
                    id=Integer, name=String
                )
            )
            .all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

    def test_via_textasfrom_use_mapped_columns(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .from_statement(
                text("select * from users order by id").columns(
                    User.id, User.name
                )
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_via_textasfrom_select_from(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .select_entity_from(
                text("select * from users")
                .columns(User.id, User.name)
                .subquery()
            )
            .order_by(User.id)
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_group_by_accepts_text(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User).group_by(text("name"))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users GROUP BY name",
        )

    def test_orm_columns_accepts_text(self):
        from sqlalchemy.orm.base import _orm_columns

        t = text("x")
        eq_(_orm_columns(t), [t])

    def test_order_by_w_eager_one(self):
        User = self.classes.User
        s = fixture_session()

        # from 1.0.0 thru 1.0.2, the "name" symbol here was considered
        # to be part of the things we need to ORDER BY and it was being
        # placed into the inner query's columns clause, as part of
        # query._compound_eager_statement where we add unwrap_order_by()
        # to the columns clause.  However, as #3392 illustrates, unlocatable
        # string expressions like "name desc" will only fail in this scenario,
        # so in general the changing of the query structure with string labels
        # is dangerous.
        #
        # the queries here are again "invalid" from a SQL perspective, as the
        # "name" field isn't matched up to anything.
        #

        q = (
            s.query(User)
            .options(joinedload("addresses"))
            .order_by(desc("name"))
            .limit(1)
        )
        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY.",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
        )

    def test_order_by_w_eager_two(self):
        User = self.classes.User
        s = fixture_session()

        q = (
            s.query(User)
            .options(joinedload("addresses"))
            .order_by("name")
            .limit(1)
        )
        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY.",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
        )

    def test_order_by_w_eager_three(self):
        User = self.classes.User
        s = fixture_session()

        self.assert_compile(
            s.query(User)
            .options(joinedload("addresses"))
            .order_by("users_name")
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name, addresses_1.id",
        )

        # however! this works (again?)
        eq_(
            s.query(User)
            .options(joinedload("addresses"))
            .order_by("users_name")
            .first(),
            User(name="chuck", addresses=[]),
        )

    def test_order_by_w_eager_four(self):
        User = self.classes.User
        Address = self.classes.Address
        s = fixture_session()

        self.assert_compile(
            s.query(User)
            .options(joinedload("addresses"))
            .order_by(desc("users_name"))
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name DESC "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name DESC, addresses_1.id",
        )

        # however! this works (again?)
        eq_(
            s.query(User)
            .options(joinedload("addresses"))
            .order_by(desc("users_name"))
            .first(),
            User(name="jack", addresses=[Address()]),
        )

    def test_order_by_w_eager_five(self):
        """essentially the same as test_eager_relations -> test_limit_3,
        but test for textual label elements that are freeform.
        this is again #3392."""

        User = self.classes.User
        Address = self.classes.Address

        sess = fixture_session()

        q = sess.query(User, Address.email_address.label("email_address"))

        result = (
            q.join("addresses")
            .options(joinedload(User.orders))
            .order_by("email_address desc")
            .limit(1)
            .offset(0)
        )

        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY",
            result.all,
        )


class TextErrorTest(QueryTest, AssertsCompiledSQL):
    def _test(self, fn, arg, offending_clause):
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Textual (?:SQL|column|SQL FROM) expression %(stmt)r should be "
            r"explicitly declared (?:with|as) text\(%(stmt)r\)"
            % {"stmt": util.ellipses_string(offending_clause)},
            fn,
            arg,
        )

    def test_filter(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).filter, "myid == 5", "myid == 5"
        )

    def test_having(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).having, "myid == 5", "myid == 5"
        )

    def test_from_statement(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).from_statement,
            "select id from user",
            "select id from user",
        )


class ParentTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_o2m(self):
        User, orders, Order = (
            self.classes.User,
            self.tables.orders,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property="orders").all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        o = sess.query(Order).with_parent(u1, property=User.orders).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        o = sess.query(Order).filter(with_parent(u1, User.orders)).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        # test generative criterion
        o = sess.query(Order).with_parent(u1).filter(orders.c.id > 2).all()
        assert [
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        # test against None for parent? this can't be done with the current
        # API since we don't know what mapper to use
        # assert
        #     sess.query(Order).with_parent(None, property='addresses').all()
        #     == [Order(description="order 5")]

    def test_select_from(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        q = sess.query(Address).select_from(Address).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_from_entity_standalone_fn(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        q = sess.query(User, Address).filter(
            with_parent(u1, "addresses", from_entity=Address)
        )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_from_entity_query_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        q = sess.query(User, Address).with_parent(
            u1, "addresses", from_entity=Address
        )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        q = sess.query(a1).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_explicit_prop(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        q = sess.query(a1).with_parent(u1, "addresses")
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_from_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        q = sess.query(a1, a2).with_parent(u1, User.addresses, from_entity=a2)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        q = sess.query(a1, a2).with_parent(u1, User.addresses.of_type(a2))
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_noparent(self):
        Item, User = self.classes.Item, self.classes.User

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except sa_exc.InvalidRequestError as e:
            assert (
                str(e) == "Could not locate a property which relates "
                "instances of class 'Item' to instances of class 'User'"
            )

    def test_m2m(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [
            Keyword(name="red"),
            Keyword(name="small"),
            Keyword(name="square"),
        ] == k

    def test_with_transient(self):
        User, Order = self.classes.User, self.classes.Order

        sess = fixture_session()

        q = sess.query(User)
        u1 = q.filter_by(name="jack").one()
        utrans = User(id=u1.id)
        o = sess.query(Order).with_parent(utrans, "orders")
        eq_(
            [
                Order(description="order 1"),
                Order(description="order 3"),
                Order(description="order 5"),
            ],
            o.all(),
        )

        o = sess.query(Order).filter(with_parent(utrans, "orders"))
        eq_(
            [
                Order(description="order 1"),
                Order(description="order 3"),
                Order(description="order 5"),
            ],
            o.all(),
        )

    def test_with_pending_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        o1 = sess.query(Order).first()
        opending = Order(id=20, user_id=o1.user_id)
        sess.add(opending)
        eq_(
            sess.query(User).with_parent(opending, "user").one(),
            User(id=o1.user_id),
        )
        eq_(
            sess.query(User).filter(with_parent(opending, "user")).one(),
            User(id=o1.user_id),
        )

    def test_with_pending_no_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session(autoflush=False)

        o1 = sess.query(Order).first()
        opending = Order(user_id=o1.user_id)
        sess.add(opending)
        eq_(
            sess.query(User).with_parent(opending, "user").one(),
            User(id=o1.user_id),
        )

    def test_unique_binds_union(self):
        """bindparams used in the 'parent' query are unique"""
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        q1 = sess.query(Address).with_parent(u1, "addresses")
        q2 = sess.query(Address).with_parent(u2, "addresses")

        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.addresses_id AS anon_1_addresses_id, "
            "anon_1.addresses_user_id AS anon_1_addresses_user_id, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address FROM (SELECT addresses.id AS "
            "addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address FROM "
            "addresses WHERE :param_1 = addresses.user_id UNION SELECT "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address "
            "AS addresses_email_address "
            "FROM addresses WHERE :param_2 = addresses.user_id) AS anon_1",
            checkparams={"param_1": 7, "param_2": 8},
        )

    def test_unique_binds_or(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        self.assert_compile(
            sess.query(Address).filter(
                or_(with_parent(u1, "addresses"), with_parent(u2, "addresses"))
            ),
            "SELECT addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM addresses WHERE "
            ":param_1 = addresses.user_id OR :param_2 = addresses.user_id",
            checkparams={"param_1": 7, "param_2": 8},
        )


class WithTransientOnNone(_fixtures.FixtureTest, AssertsCompiledSQL):
    run_inserts = None
    __dialect__ = "default"

    def _fixture1(self):
        User, Address, Dingaling, HasDingaling = (
            self.classes.User,
            self.classes.Address,
            self.classes.Dingaling,
            self.classes.HasDingaling,
        )
        users, addresses, dingalings, has_dingaling = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
            self.tables.has_dingaling,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(User),
                "special_user": relationship(
                    User,
                    primaryjoin=and_(
                        users.c.id == addresses.c.user_id,
                        users.c.name == addresses.c.email_address,
                    ),
                    viewonly=True,
                ),
            },
        )
        mapper(Dingaling, dingalings)
        mapper(
            HasDingaling,
            has_dingaling,
            properties={
                "dingaling": relationship(
                    Dingaling,
                    primaryjoin=and_(
                        dingalings.c.id == has_dingaling.c.dingaling_id,
                        dingalings.c.data == "hi",
                    ),
                )
            },
        )

    def test_filter_with_transient_dont_assume_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = sess.query(Address).filter(Address.user == User())
        assert_raises_message(
            sa_exc.StatementError,
            "Can't resolve value for column users.id on object "
            ".User at .*; no value has been set for this column",
            q.all,
        )

    def test_filter_with_transient_given_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = sess.query(Address).filter(Address.user == User(id=None))
        with expect_warnings("Got None for value of column "):
            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id",
                checkparams={"param_1": None},
            )

    def test_filter_with_transient_given_pk_but_only_later(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User()
        # id is not set, so evaluates to NEVER_SET
        q = sess.query(Address).filter(Address.user == u1)

        # but we set it, so we should get the warning
        u1.id = None
        with expect_warnings("Got None for value of column "):
            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id",
                checkparams={"param_1": None},
            )

    def test_filter_with_transient_warn_for_none_against_non_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(Address).filter(
            Address.special_user == User(id=None, name=None)
        )
        with expect_warnings("Got None for value of column"):

            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND :param_2 = addresses.email_address",
                checkparams={"param_1": None, "param_2": None},
            )

    def test_filter_with_persistent_non_pk_col_is_default_null(self):
        # test #4676 - comparison to a persistent column that is
        # NULL in the database, but is not fetched
        self._fixture1()
        Dingaling, HasDingaling = (
            self.classes.Dingaling,
            self.classes.HasDingaling,
        )
        s = fixture_session()
        d = Dingaling(id=1)
        s.add(d)
        s.flush()
        assert "data" not in d.__dict__

        q = s.query(HasDingaling).filter_by(dingaling=d)

        with expect_warnings("Got None for value of column"):
            self.assert_compile(
                q,
                "SELECT has_dingaling.id AS has_dingaling_id, "
                "has_dingaling.dingaling_id AS has_dingaling_dingaling_id "
                "FROM has_dingaling WHERE :param_1 = "
                "has_dingaling.dingaling_id AND :param_2 = :data_1",
                checkparams={"param_1": 1, "param_2": None, "data_1": "hi"},
            )

    def test_filter_with_detached_non_pk_col_is_default_null(self):
        self._fixture1()
        Dingaling, HasDingaling = (
            self.classes.Dingaling,
            self.classes.HasDingaling,
        )
        s = fixture_session()
        d = Dingaling()
        s.add(d)
        s.flush()
        s.commit()
        d.id
        s.expire(d, ["data"])
        s.expunge(d)
        assert "data" not in d.__dict__
        assert "id" in d.__dict__

        q = s.query(HasDingaling).filter_by(dingaling=d)

        # this case we still can't handle, object is detached so we assume
        # nothing

        assert_raises_message(
            sa_exc.StatementError,
            r"Can't resolve value for column dingalings.data on "
            r"object .*Dingaling.* the object is detached and "
            r"the value was expired",
            q.all,
        )

    def test_filter_with_detached_non_pk_col_has_value(self):
        self._fixture1()
        Dingaling, HasDingaling = (
            self.classes.Dingaling,
            self.classes.HasDingaling,
        )
        s = fixture_session()
        d = Dingaling(data="some data")
        s.add(d)
        s.commit()
        s.expire(d)
        assert "data" not in d.__dict__

        q = s.query(HasDingaling).filter_by(dingaling=d)

        self.assert_compile(
            q,
            "SELECT has_dingaling.id AS has_dingaling_id, "
            "has_dingaling.dingaling_id AS has_dingaling_dingaling_id "
            "FROM has_dingaling WHERE :param_1 = "
            "has_dingaling.dingaling_id AND :param_2 = :data_1",
            checkparams={"param_1": 1, "param_2": "some data", "data_1": "hi"},
        )

    def test_with_parent_with_transient_assume_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = sess.query(User).with_parent(Address(user_id=None), "user")
        with expect_warnings("Got None for value of column"):
            self.assert_compile(
                q,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1",
                checkparams={"param_1": None},
            )

    def test_with_parent_with_transient_warn_for_none_against_non_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(User).with_parent(
            Address(user_id=None, email_address=None), "special_user"
        )
        with expect_warnings("Got None for value of column"):

            self.assert_compile(
                q,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1 "
                "AND users.name = :param_2",
                checkparams={"param_1": None, "param_2": None},
            )

    def test_negated_contains_or_equals_plain_m2o(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(Address).filter(Address.user != User(id=None))
        with expect_warnings("Got None for value of column"):
            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses "
                "WHERE addresses.user_id != :user_id_1 "
                "OR addresses.user_id IS NULL",
                checkparams={"user_id_1": None},
            )

    def test_negated_contains_or_equals_complex_rel(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        # this one does *not* warn because we do the criteria
        # without deferral
        q = s.query(Address).filter(Address.special_user != User(id=None))
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses "
            "WHERE NOT (EXISTS (SELECT 1 "
            "FROM users "
            "WHERE users.id = addresses.user_id AND "
            "users.name = addresses.email_address AND users.id IS NULL))",
            checkparams={},
        )


class SynonymTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        (
            users,
            Keyword,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            keywords,
            Order,
            item_keywords,
            addresses,
        ) = (
            cls.tables.users,
            cls.classes.Keyword,
            cls.tables.items,
            cls.tables.order_items,
            cls.tables.orders,
            cls.classes.Item,
            cls.classes.User,
            cls.classes.Address,
            cls.tables.keywords,
            cls.classes.Order,
            cls.tables.item_keywords,
            cls.tables.addresses,
        )

        mapper(
            User,
            users,
            properties={
                "name_syn": synonym("name"),
                "addresses": relationship(Address),
                "orders": relationship(
                    Order, backref="user", order_by=orders.c.id
                ),  # o2m, m2o
                "orders_syn": synonym("orders"),
                "orders_syn_2": synonym("orders_syn"),
            },
        )
        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=order_items),  # m2m
                "address": relationship(Address),  # m2o
                "items_syn": synonym("items"),
            },
        )
        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords
                )  # m2m
            },
        )
        mapper(Keyword, keywords)

    def test_options(self):
        User, Order = self.classes.User, self.classes.Order

        s = fixture_session()

        def go():
            result = (
                s.query(User)
                .filter_by(name="jack")
                .options(joinedload(User.orders_syn))
                .all()
            )
            eq_(
                result,
                [
                    User(
                        id=7,
                        name="jack",
                        orders=[
                            Order(description="order 1"),
                            Order(description="order 3"),
                            Order(description="order 5"),
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_options_syn_of_syn(self):
        User, Order = self.classes.User, self.classes.Order

        s = fixture_session()

        def go():
            result = (
                s.query(User)
                .filter_by(name="jack")
                .options(joinedload(User.orders_syn_2))
                .all()
            )
            eq_(
                result,
                [
                    User(
                        id=7,
                        name="jack",
                        orders=[
                            Order(description="order 1"),
                            Order(description="order 3"),
                            Order(description="order 5"),
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_options_syn_of_syn_string(self):
        User, Order = self.classes.User, self.classes.Order

        s = fixture_session()

        def go():
            result = (
                s.query(User)
                .filter_by(name="jack")
                .options(joinedload("orders_syn_2"))
                .all()
            )
            eq_(
                result,
                [
                    User(
                        id=7,
                        name="jack",
                        orders=[
                            Order(description="order 1"),
                            Order(description="order 3"),
                            Order(description="order 5"),
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_joins(self):
        User, Order = self.classes.User, self.classes.Order

        for j in (
            [User.orders, Order.items],
            [User.orders_syn, Order.items],
            [User.orders_syn, Order.items],
            [User.orders_syn_2, Order.items],
            [User.orders, Order.items_syn],
            [User.orders_syn, Order.items_syn],
            [User.orders_syn_2, Order.items_syn],
        ):
            with fixture_session() as sess:
                q = sess.query(User)
                for path in j:
                    q = q.join(path)
                q = q.filter_by(id=3)
                result = q.all()
                eq_(
                    result,
                    [
                        User(id=7, name="jack"),
                        User(id=9, name="fred"),
                    ],
                )

    def test_with_parent(self):
        Order, User = self.classes.Order, self.classes.User

        for nameprop, orderprop in (
            ("name", "orders"),
            ("name_syn", "orders"),
            ("name", "orders_syn"),
            ("name", "orders_syn_2"),
            ("name_syn", "orders_syn"),
            ("name_syn", "orders_syn_2"),
        ):
            with fixture_session() as sess:
                q = sess.query(User)

                u1 = q.filter_by(**{nameprop: "jack"}).one()

                o = sess.query(Order).with_parent(u1, property=orderprop).all()
                assert [
                    Order(description="order 1"),
                    Order(description="order 3"),
                    Order(description="order 5"),
                ] == o

    def test_froms_aliased_col(self):
        Address, User = self.classes.Address, self.classes.User

        sess = fixture_session()
        ua = aliased(User)

        q = sess.query(ua.name_syn).join(Address, ua.id == Address.user_id)
        self.assert_compile(
            q,
            "SELECT users_1.name AS users_1_name FROM "
            "users AS users_1 JOIN addresses "
            "ON users_1.id = addresses.user_id",
        )


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (
            cls.classes.Address,
            cls.tables.addresses,
            cls.tables.users,
            cls.classes.User,
        )

        mapper(Address, addresses)

        mapper(User, users, properties=dict(addresses=relationship(Address)))

    def test_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        assert_raises_message(
            sa.orm.exc.NoResultFound,
            r"No row was found when one was required",
            sess.query(User).filter(User.id == 99).one,
        )

        eq_(sess.query(User).filter(User.id == 7).one().id, 7)

        assert_raises_message(
            sa.orm.exc.MultipleResultsFound,
            r"Multiple rows were found when exactly one",
            sess.query(User).one,
        )

        assert_raises(
            sa.orm.exc.NoResultFound,
            sess.query(User.id, User.name).filter(User.id == 99).one,
        )

        eq_(
            sess.query(User.id, User.name).filter(User.id == 7).one(),
            (7, "jack"),
        )

        assert_raises(
            sa.orm.exc.MultipleResultsFound, sess.query(User.id, User.name).one
        )

        assert_raises(
            sa.orm.exc.NoResultFound,
            (
                sess.query(User, Address)
                .join(User.addresses)
                .filter(Address.id == 99)
            ).one,
        )

        eq_(
            (
                sess.query(User, Address)
                .join(User.addresses)
                .filter(Address.id == 4)
            ).one(),
            (User(id=8), Address(id=4)),
        )

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User, Address).join(User.addresses).one,
        )

        # this result returns multiple rows, the first
        # two rows being the same.  but uniquing is
        # not applied for a column based result.
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id)
            .join(User.addresses)
            .filter(User.id.in_([8, 9]))
            .order_by(User.id)
            .one,
        )

        # test that a join which ultimately returns
        # multiple identities across many rows still
        # raises, even though the first two rows are of
        # the same identity and unique filtering
        # is applied ([ticket:1688])
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User)
            .join(User.addresses)
            .filter(User.id.in_([8, 9]))
            .order_by(User.id)
            .one,
        )

    def test_one_or_none(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        eq_(sess.query(User).filter(User.id == 99).one_or_none(), None)

        eq_(sess.query(User).filter(User.id == 7).one_or_none().id, 7)

        assert_raises_message(
            sa.orm.exc.MultipleResultsFound,
            r"Multiple rows were found when one or none was required",
            sess.query(User).one_or_none,
        )

        eq_(
            sess.query(User.id, User.name).filter(User.id == 99).one_or_none(),
            None,
        )

        eq_(
            sess.query(User.id, User.name).filter(User.id == 7).one_or_none(),
            (7, "jack"),
        )

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id, User.name).one_or_none,
        )

        eq_(
            (
                sess.query(User, Address)
                .join(User.addresses)
                .filter(Address.id == 99)
            ).one_or_none(),
            None,
        )

        eq_(
            (
                sess.query(User, Address)
                .join(User.addresses)
                .filter(Address.id == 4)
            ).one_or_none(),
            (User(id=8), Address(id=4)),
        )

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User, Address).join(User.addresses).one_or_none,
        )

        # this result returns multiple rows, the first
        # two rows being the same.  but uniquing is
        # not applied for a column based result.
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id)
            .join(User.addresses)
            .filter(User.id.in_([8, 9]))
            .order_by(User.id)
            .one_or_none,
        )

        # test that a join which ultimately returns
        # multiple identities across many rows still
        # raises, even though the first two rows are of
        # the same identity and unique filtering
        # is applied ([ticket:1688])
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User)
            .join(User.addresses)
            .filter(User.id.in_([8, 9]))
            .order_by(User.id)
            .one_or_none,
        )

    @testing.future
    def test_getslice(self):
        assert False

    def test_scalar(self):
        User = self.classes.User

        sess = fixture_session()

        eq_(sess.query(User.id).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id, User.name).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id).filter_by(id=0).scalar(), None)
        eq_(
            sess.query(User).filter_by(id=7).scalar(),
            sess.query(User).filter_by(id=7).one(),
        )

        assert_raises(sa.orm.exc.MultipleResultsFound, sess.query(User).scalar)
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id, User.name).scalar,
        )


class ExecutionOptionsTest(QueryTest):
    def test_option_building(self):
        User = self.classes.User

        sess = fixture_session(autocommit=False)

        q1 = sess.query(User)
        eq_(q1._execution_options, dict())
        q2 = q1.execution_options(foo="bar", stream_results=True)
        # q1's options should be unchanged.
        eq_(q1._execution_options, dict())
        # q2 should have them set.
        eq_(q2._execution_options, dict(foo="bar", stream_results=True))
        q3 = q2.execution_options(foo="not bar", answer=42)
        eq_(q2._execution_options, dict(foo="bar", stream_results=True))

        q3_options = dict(foo="not bar", stream_results=True, answer=42)
        eq_(q3._execution_options, q3_options)

    def test_get_options(self):
        User = self.classes.User

        sess = fixture_session(autocommit=False)

        q = sess.query(User).execution_options(foo="bar", stream_results=True)
        eq_(q.get_execution_options(), dict(foo="bar", stream_results=True))

    def test_options_in_connection(self):
        User = self.classes.User

        execution_options = dict(foo="bar", stream_results=True)

        class TQuery(Query):
            def instances(self, result, ctx):
                try:
                    eq_(
                        result.connection._execution_options, execution_options
                    )
                finally:
                    result.close()
                return iter([])

        sess = fixture_session(autocommit=False, query_cls=TQuery)
        q1 = sess.query(User).execution_options(**execution_options)
        q1.all()


class BooleanEvalTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """test standalone booleans being wrapped in an AsBoolean, as well
    as true/false compilation."""

    def _dialect(self, native_boolean):
        d = default.DefaultDialect()
        d.supports_native_boolean = native_boolean
        return d

    def test_one(self):
        s = fixture_session()
        c = column("x", Boolean)
        self.assert_compile(
            s.query(c).filter(c),
            "SELECT x WHERE x",
            dialect=self._dialect(True),
        )

    def test_two(self):
        s = fixture_session()
        c = column("x", Boolean)
        self.assert_compile(
            s.query(c).filter(c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False),
        )

    def test_three(self):
        s = fixture_session()
        c = column("x", Boolean)
        self.assert_compile(
            s.query(c).filter(~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False),
        )

    def test_four(self):
        s = fixture_session()
        c = column("x", Boolean)
        self.assert_compile(
            s.query(c).filter(~c),
            "SELECT x WHERE NOT x",
            dialect=self._dialect(True),
        )

    def test_five(self):
        s = fixture_session()
        c = column("x", Boolean)
        self.assert_compile(
            s.query(c).having(c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False),
        )


class SessionBindTest(QueryTest):
    @contextlib.contextmanager
    def _assert_bind_args(self, session, expect_mapped_bind=True):
        get_bind = mock.Mock(side_effect=session.get_bind)
        with mock.patch.object(session, "get_bind", get_bind):
            yield
        for call_ in get_bind.mock_calls:
            if expect_mapped_bind:
                eq_(
                    call_,
                    mock.call(
                        clause=mock.ANY, mapper=inspect(self.classes.User)
                    ),
                )
            else:
                eq_(call_, mock.call(clause=mock.ANY))

    def test_single_entity_q(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User).all()

    def test_aliased_entity_q(self):
        User = self.classes.User
        u = aliased(User)
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(u).all()

    def test_sql_expr_entity_q(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User.id).all()

    def test_sql_expr_subquery_from_entity(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            subq = session.query(User.id).subquery()
            session.query(subq).all()

    @testing.requires.boolean_col_expressions
    def test_sql_expr_exists_from_entity(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            subq = session.query(User.id).exists()
            session.query(subq).all()

    def test_sql_expr_cte_from_entity(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            cte = session.query(User.id).cte()
            subq = session.query(cte).subquery()
            session.query(subq).all()

    def test_sql_expr_bundle_cte_from_entity(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            cte = session.query(User.id, User.name).cte()
            subq = session.query(cte).subquery()
            bundle = Bundle(subq.c.id, subq.c.name)
            session.query(bundle).all()

    def test_count(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User).count()

    def test_single_col(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User.name).all()

    def test_single_col_from_subq(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            subq = session.query(User.id, User.name).subquery()
            session.query(subq.c.name).all()

    def test_aggregate_fn(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(func.max(User.name)).all()

    def test_case(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(case([(User.name == "x", "C")], else_="W")).all()

    def test_cast(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(cast(User.name, String())).all()

    def test_type_coerce(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(type_coerce(User.name, String())).all()

    def test_binary_op(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User.name + "x").all()

    @testing.requires.boolean_col_expressions
    def test_boolean_op(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(User.name == "x").all()

    def test_bulk_update_no_sync(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).update(
                {"name": "foob"}, synchronize_session=False
            )

    def test_bulk_delete_no_sync(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).delete(
                synchronize_session=False
            )

    def test_bulk_update_fetch_sync(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).update(
                {"name": "foob"}, synchronize_session="fetch"
            )

    def test_bulk_delete_fetch_sync(self):
        User = self.classes.User
        session = fixture_session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).delete(
                synchronize_session="fetch"
            )

    def test_column_property(self):
        User = self.classes.User

        mapper = inspect(User)
        mapper.add_property(
            "score",
            column_property(func.coalesce(self.tables.users.c.name, None)),
        )
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=True):
            session.query(func.max(User.score)).scalar()

    def test_plain_table(self):
        User = self.classes.User

        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=False):
            session.query(inspect(User).local_table).all()

    def _test_plain_table_from_self(self):
        User = self.classes.User

        # TODO: this test is dumb
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=False):
            session.query(inspect(User).local_table).from_self().all()

    def test_plain_table_count(self):
        User = self.classes.User

        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=False):
            session.query(inspect(User).local_table).count()

    def test_plain_table_select_from(self):
        User = self.classes.User

        table = inspect(User).local_table
        session = fixture_session()
        with self._assert_bind_args(session, expect_mapped_bind=False):
            session.query(table).select_from(table).all()

    @testing.requires.nested_aggregates
    def test_column_property_select(self):
        User = self.classes.User
        Address = self.classes.Address

        mapper = inspect(User)
        mapper.add_property(
            "score",
            column_property(
                select(func.sum(Address.id))
                .where(Address.user_id == User.id)
                .scalar_subquery()
            ),
        )
        session = fixture_session()

        with self._assert_bind_args(session):
            session.query(func.max(User.score)).scalar()


class QueryClsTest(QueryTest):
    def _fn_fixture(self):
        def query(*arg, **kw):
            return Query(*arg, **kw)

        return query

    def _subclass_fixture(self):
        class MyQuery(Query):
            pass

        return MyQuery

    def _callable_fixture(self):
        class MyQueryFactory(object):
            def __call__(self, *arg, **kw):
                return Query(*arg, **kw)

        return MyQueryFactory()

    def _plain_fixture(self):
        return Query

    def _test_get(self, fixture):
        User = self.classes.User

        s = fixture_session(query_cls=fixture())

        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2

    def _test_o2m_lazyload(self, fixture):
        User, Address = self.classes("User", "Address")

        s = fixture_session(query_cls=fixture())

        u1 = s.query(User).filter(User.id == 7).first()
        eq_(u1.addresses, [Address(id=1)])

    def _test_m2o_lazyload(self, fixture):
        User, Address = self.classes("User", "Address")

        s = fixture_session(query_cls=fixture())

        a1 = s.query(Address).filter(Address.id == 1).first()
        eq_(a1.user, User(id=7))

    def _test_expr(self, fixture):
        User, Address = self.classes("User", "Address")

        s = fixture_session(query_cls=fixture())

        q = s.query(func.max(User.id).label("max"))
        eq_(q.scalar(), 10)

    def _test_expr_undocumented_query_constructor(self, fixture):
        # see #4269.  not documented but already out there.
        User, Address = self.classes("User", "Address")

        s = fixture_session(query_cls=fixture())

        q = Query(func.max(User.id).label("max")).with_session(s)
        eq_(q.scalar(), 10)

    def test_plain_get(self):
        self._test_get(self._plain_fixture)

    def test_callable_get(self):
        self._test_get(self._callable_fixture)

    def test_subclass_get(self):
        self._test_get(self._subclass_fixture)

    def test_fn_get(self):
        self._test_get(self._fn_fixture)

    def test_plain_expr(self):
        self._test_expr(self._plain_fixture)

    def test_callable_expr(self):
        self._test_expr(self._callable_fixture)

    def test_subclass_expr(self):
        self._test_expr(self._subclass_fixture)

    def test_fn_expr(self):
        self._test_expr(self._fn_fixture)

    def test_plain_expr_undocumented_query_constructor(self):
        self._test_expr_undocumented_query_constructor(self._plain_fixture)

    def test_callable_expr_undocumented_query_constructor(self):
        self._test_expr_undocumented_query_constructor(self._callable_fixture)

    def test_subclass_expr_undocumented_query_constructor(self):
        self._test_expr_undocumented_query_constructor(self._subclass_fixture)

    def test_fn_expr_undocumented_query_constructor(self):
        self._test_expr_undocumented_query_constructor(self._fn_fixture)

    def test_callable_o2m_lazyload(self):
        self._test_o2m_lazyload(self._callable_fixture)

    def test_subclass_o2m_lazyload(self):
        self._test_o2m_lazyload(self._subclass_fixture)

    def test_fn_o2m_lazyload(self):
        self._test_o2m_lazyload(self._fn_fixture)

    def test_callable_m2o_lazyload(self):
        self._test_m2o_lazyload(self._callable_fixture)

    def test_subclass_m2o_lazyload(self):
        self._test_m2o_lazyload(self._subclass_fixture)

    def test_fn_m2o_lazyload(self):
        self._test_m2o_lazyload(self._fn_fixture)
