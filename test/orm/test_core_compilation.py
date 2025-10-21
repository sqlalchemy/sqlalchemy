from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import union
from sqlalchemy import update
from sqlalchemy import util
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.orm import aliased
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import deferred
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_expression
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql import and_
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.selectable import Join as core_join
from sqlalchemy.sql.selectable import LABEL_STYLE_DISAMBIGUATE_ONLY
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing import Variation
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.util import resolve_lambda
from sqlalchemy.util.langhelpers import hybridproperty
from .inheritance import _poly_fixtures
from .test_query import QueryTest
from ..sql import test_compiler
from ..sql.test_compiler import CorrelateTest as _CoreCorrelateTest

# TODO:
# composites / unions, etc.


class SelectableTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_filter_by(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).filter_by(name="ed")

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "WHERE users.name = :name_1",
        )

    def test_c_accessor_not_mutated_subq(self):
        """test #6394, ensure all_selected_columns is generated each time"""
        User = self.classes.User

        s1 = select(User.id)

        eq_(s1.subquery().c.keys(), ["id"])
        eq_(s1.subquery().c.keys(), ["id"])

    def test_integration_w_8285_subc(self):
        Address = self.classes.Address

        s1 = select(
            Address.id, Address.__table__.c["user_id", "email_address"]
        )
        self.assert_compile(
            s1,
            "SELECT addresses.id, addresses.user_id, "
            "addresses.email_address FROM addresses",
        )

        subq = s1.subquery()
        self.assert_compile(
            select(subq.c.user_id, subq.c.id),
            "SELECT anon_1.user_id, anon_1.id FROM (SELECT addresses.id AS "
            "id, addresses.user_id AS user_id, addresses.email_address "
            "AS email_address FROM addresses) AS anon_1",
        )

    def test_scalar_subquery_from_subq_same_source(self):
        """test #6394, ensure all_selected_columns is generated each time"""
        User = self.classes.User

        s1 = select(User.id)

        for i in range(2):
            stmt = s1.subquery().select().scalar_subquery()
            self.assert_compile(
                stmt,
                "(SELECT anon_1.id FROM "
                "(SELECT users.id AS id FROM users) AS anon_1)",
            )

    def test_froms_single_table(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).filter_by(name="ed")

        eq_(stmt.get_final_froms(), [self.tables.users])

    def test_froms_join(self):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")

        stmt = select(User).join(User.addresses)

        assert stmt.get_final_froms()[0].compare(users.join(addresses))

    @testing.combinations(
        (
            lambda User: (User,),
            lambda User: [
                {
                    "name": "User",
                    "type": User,
                    "aliased": False,
                    "expr": User,
                    "entity": User,
                }
            ],
        ),
        (
            lambda user_alias: (user_alias,),
            lambda User, user_alias: [
                {
                    "name": None,
                    "type": User,
                    "aliased": True,
                    "expr": user_alias,
                    "entity": user_alias,
                }
            ],
        ),
        (
            lambda User: (User.id,),
            lambda User: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                }
            ],
        ),
        (
            lambda User, Address: (User.id, Address),
            lambda User, Address: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": "Address",
                    "type": Address,
                    "aliased": False,
                    "expr": Address,
                    "entity": Address,
                },
            ],
        ),
        (
            lambda User, Address: (User.id, text("whatever")),
            lambda User, Address: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": None,
                    "type": testing.eq_type_affinity(sqltypes.NullType),
                    "aliased": False,
                    "expr": testing.eq_clause_element(text("whatever")),
                    "entity": None,
                },
            ],
        ),
        (
            lambda user_table: (user_table,),
            lambda user_table: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "expr": user_table.c.id,
                },
                {
                    "name": "name",
                    "type": testing.eq_type_affinity(sqltypes.String),
                    "expr": user_table.c.name,
                },
            ],
        ),
        argnames="cols, expected",
    )
    def test_column_descriptions(self, cols, expected):
        User, Address = self.classes("User", "Address")
        ua = aliased(User)

        cols = testing.resolve_lambda(
            cols,
            User=User,
            Address=Address,
            user_alias=ua,
            user_table=inspect(User).local_table,
        )
        expected = testing.resolve_lambda(
            expected,
            User=User,
            Address=Address,
            user_alias=ua,
            user_table=inspect(User).local_table,
        )

        stmt = select(*cols)

        eq_(stmt.column_descriptions, expected)

        if stmt._propagate_attrs:
            stmt = select(*cols).from_statement(stmt)
            eq_(stmt.column_descriptions, expected)

    @testing.combinations(insert, update, delete, argnames="dml_construct")
    @testing.combinations(
        (
            lambda User: User,
            lambda User: (User.id, User.name),
            lambda User, user_table: {
                "name": "User",
                "type": User,
                "expr": User,
                "entity": User,
                "table": user_table,
            },
            lambda User: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": "name",
                    "type": testing.eq_type_affinity(sqltypes.String),
                    "aliased": False,
                    "expr": User.name,
                    "entity": User,
                },
            ],
        ),
        argnames="entity, cols, expected_entity, expected_returning",
    )
    def test_dml_descriptions(
        self, dml_construct, entity, cols, expected_entity, expected_returning
    ):
        User, Address = self.classes("User", "Address")

        lambda_args = dict(
            User=User,
            Address=Address,
            user_table=inspect(User).local_table,
        )
        entity = testing.resolve_lambda(entity, **lambda_args)
        cols = testing.resolve_lambda(cols, **lambda_args)
        expected_entity = testing.resolve_lambda(
            expected_entity, **lambda_args
        )
        expected_returning = testing.resolve_lambda(
            expected_returning, **lambda_args
        )

        stmt = dml_construct(entity)
        if cols:
            stmt = stmt.returning(*cols)

        eq_(stmt.entity_description, expected_entity)
        eq_(stmt.returning_column_descriptions, expected_returning)

    @testing.combinations(
        (
            lambda User, Address: select(User.name)
            .select_from(User, Address)
            .where(User.id == Address.user_id),
            "SELECT users.name FROM users, addresses "
            "WHERE users.id = addresses.user_id",
        ),
        (
            lambda User, Address: select(User.name)
            .select_from(Address, User)
            .where(User.id == Address.user_id),
            "SELECT users.name FROM addresses, users "
            "WHERE users.id = addresses.user_id",
        ),
    )
    def test_select_from_ordering(self, stmt, expected):
        User, Address = self.classes("User", "Address")

        lambda_args = dict(
            User=User,
            Address=Address,
            user_table=inspect(User).local_table,
        )

        stmt = testing.resolve_lambda(stmt, **lambda_args)
        self.assert_compile(stmt, expected)

    def test_limit_offset_select(self):
        User = self.classes.User

        stmt = select(User.id).limit(5).offset(6)
        self.assert_compile(
            stmt,
            "SELECT users.id FROM users LIMIT :param_1 OFFSET :param_2",
            checkparams={"param_1": 5, "param_2": 6},
        )

    @testing.combinations(
        (None, "ROWS ONLY"),
        ({"percent": True}, "PERCENT ROWS ONLY"),
        ({"percent": True, "with_ties": True}, "PERCENT ROWS WITH TIES"),
    )
    def test_fetch_offset_select(self, options, fetch_clause):
        User = self.classes.User

        if options is None:
            stmt = select(User.id).fetch(5).offset(6)
        else:
            stmt = select(User.id).fetch(5, **options).offset(6)

        self.assert_compile(
            stmt,
            "SELECT users.id FROM users OFFSET :param_1 "
            "ROWS FETCH FIRST :param_2 %s" % (fetch_clause,),
            checkparams={"param_1": 6, "param_2": 5},
        )


class PropagateAttrsTest(QueryTest):
    __backend__ = True

    def propagate_cases():
        def distinct_deprecated(User, user_table):
            with expect_deprecated("Passing expression to"):
                return select(1).distinct(User.id).select_from(user_table)

        return testing.combinations(
            (lambda: select(1), False),
            (lambda User: select(User.id), True),
            (lambda User: select(User.id + User.id), True),
            (lambda User: select(User.id + User.id + User.id), True),
            (lambda User: select(sum([User.id] * 10, User.id)), True),  # type: ignore  # noqa: E501
            (
                lambda User: select(literal_column("3") + User.id + User.id),
                True,
            ),
            (lambda User: select(func.count(User.id)), True),
            (
                lambda User: select(1).select_from(select(User).subquery()),
                True,
            ),
            (
                lambda User: select(
                    select(User.id).where(User.id == 5).scalar_subquery()
                ),
                True,
            ),
            (
                lambda User: select(
                    select(User.id).where(User.id == 5).label("x")
                ),
                True,
            ),
            (lambda User: select(1).select_from(User), True),
            (lambda User: select(1).where(exists(User.id)), True),
            (lambda User: select(1).where(~exists(User.id)), True),
            (
                # changed as part of #9805
                lambda User: select(1).where(User.id == 1),
                True,
            ),
            (
                # changed as part of #9805
                lambda User, user_table: select(func.count(1))
                .select_from(user_table)
                .group_by(user_table.c.id)
                .having(User.id == 1),
                True,
            ),
            (
                # changed as part of #9805
                lambda User, user_table: select(1)
                .select_from(user_table)
                .order_by(User.id),
                True,
            ),
            (
                # changed as part of #9805
                lambda User, user_table: select(1)
                .select_from(user_table)
                .group_by(User.id),
                True,
            ),
            (
                lambda User, user_table: select(user_table).join(
                    aliased(User), true()
                ),
                True,
            ),
            (
                # changed as part of #9805
                distinct_deprecated,
                True,
                testing.requires.supports_distinct_on,
            ),
            (
                lambda user_table, User: select(1)
                .ext(distinct_on(User.id))
                .select_from(user_table),
                True,
                testing.requires.supports_distinct_on,
            ),
            (lambda user_table: select(user_table), False),
            (lambda User: select(User), True),
            (lambda User: union(select(User), select(User)), True),
            (
                lambda User: select(1).select_from(
                    union(select(User), select(User)).subquery()
                ),
                True,
            ),
            (lambda User: select(User.id), True),
            # these are meaningless, correlate by itself has no effect
            (lambda User: select(1).correlate(User), False),
            (lambda User: select(1).correlate_except(User), False),
            (lambda User: delete(User).where(User.id > 20), True),
            (
                lambda User, user_table: delete(user_table).where(
                    User.id > 20
                ),
                True,
            ),
            (lambda User: update(User).values(name="x"), True),
            (
                lambda User, user_table: update(user_table)
                .values(name="x")
                .where(User.id > 20),
                True,
            ),
            (lambda User: insert(User).values(name="x"), True),
        )

    @propagate_cases()
    def test_propagate_attr_yesno(self, test_case, expected):
        User = self.classes.User
        user_table = self.tables.users

        stmt = resolve_lambda(test_case, User=User, user_table=user_table)

        eq_(bool(stmt._propagate_attrs), expected)

    @propagate_cases()
    def test_autoflushes(self, test_case, expected):
        User = self.classes.User
        user_table = self.tables.users

        stmt = resolve_lambda(test_case, User=User, user_table=user_table)

        with Session(testing.db) as s:
            with mock.patch.object(s, "_autoflush", wrap=True) as before_flush:
                r = s.execute(stmt)
                r.close()

        # After issue #9809: unconditionally autoflush on all executions
        eq_(before_flush.mock_calls, [mock.call()])


class DMLTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default_enhanced"

    @testing.variation("stmt_type", ["update", "delete"])
    def test_dml_ctes(self, stmt_type: testing.Variation):
        User = self.classes.User

        if stmt_type.update:
            fn = update
        elif stmt_type.delete:
            fn = delete
        else:
            stmt_type.fail()

        inner_cte = fn(User).returning(User.id).cte("uid")

        stmt = select(inner_cte)

        if stmt_type.update:
            self.assert_compile(
                stmt,
                "WITH uid AS (UPDATE users SET id=:id, name=:name "
                "RETURNING users.id) SELECT uid.id FROM uid",
            )
        elif stmt_type.delete:
            self.assert_compile(
                stmt,
                "WITH uid AS (DELETE FROM users "
                "RETURNING users.id) SELECT uid.id FROM uid",
            )
        else:
            stmt_type.fail()

    @testing.variation("stmt_type", ["core", "orm"])
    def test_aliased_update(self, stmt_type: testing.Variation):
        """test #10279"""
        if stmt_type.orm:
            User = self.classes.User
            u1 = aliased(User)
            stmt = update(u1).where(u1.name == "xyz").values(name="newname")
        elif stmt_type.core:
            user_table = self.tables.users
            u1 = user_table.alias()
            stmt = update(u1).where(u1.c.name == "xyz").values(name="newname")
        else:
            stmt_type.fail()

        self.assert_compile(
            stmt,
            "UPDATE users AS users_1 SET name=:name "
            "WHERE users_1.name = :name_1",
        )

    @testing.variation("stmt_type", ["core", "orm"])
    def test_aliased_delete(self, stmt_type: testing.Variation):
        """test #10279"""
        if stmt_type.orm:
            User = self.classes.User
            u1 = aliased(User)
            stmt = delete(u1).where(u1.name == "xyz")
        elif stmt_type.core:
            user_table = self.tables.users
            u1 = user_table.alias()
            stmt = delete(u1).where(u1.c.name == "xyz")
        else:
            stmt_type.fail()

        self.assert_compile(
            stmt,
            "DELETE FROM users AS users_1 WHERE users_1.name = :name_1",
        )

    @testing.variation("stmt_type", ["core", "orm"])
    def test_add_cte(self, stmt_type: testing.Variation):
        """test #10167"""

        if stmt_type.orm:
            User = self.classes.User
            cte_select = select(User.name).limit(1).cte()
            cte_insert = insert(User).from_select(["name"], cte_select).cte()
        elif stmt_type.core:
            user_table = self.tables.users
            cte_select = select(user_table.c.name).limit(1).cte()
            cte_insert = (
                insert(user_table).from_select(["name"], cte_select).cte()
            )
        else:
            stmt_type.fail()

        select_stmt = select(cte_select).add_cte(cte_insert)

        self.assert_compile(
            select_stmt,
            "WITH anon_2 AS (SELECT users.name AS name FROM users LIMIT "
            ":param_1), anon_1 AS (INSERT INTO users (name) "
            "SELECT anon_2.name AS name FROM anon_2) "
            "SELECT anon_2.name FROM anon_2",
        )


class ColumnsClauseFromsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_exclude_eagerloads(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).options(joinedload(User.addresses))

        froms = stmt.columns_clause_froms

        mapper = inspect(User)
        is_(froms[0], inspect(User).__clause_element__())
        eq_(
            froms[0]._annotations,
            {
                "entity_namespace": mapper,
                "parententity": mapper,
                "parentmapper": mapper,
            },
        )
        eq_(len(froms), 1)

    def test_maintain_annotations_from_table(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User)

        mapper = inspect(User)
        froms = stmt.columns_clause_froms
        is_(froms[0], inspect(User).__clause_element__())
        eq_(
            froms[0]._annotations,
            {
                "entity_namespace": mapper,
                "parententity": mapper,
                "parentmapper": mapper,
            },
        )
        eq_(len(froms), 1)

    def test_maintain_annotations_from_annoated_cols(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User.id)

        mapper = inspect(User)
        froms = stmt.columns_clause_froms
        is_(froms[0], inspect(User).__clause_element__())
        eq_(
            froms[0]._annotations,
            {
                "entity_namespace": mapper,
                "parententity": mapper,
                "parentmapper": mapper,
            },
        )
        eq_(len(froms), 1)

    @testing.combinations((True,), (False,))
    def test_replace_into_select_from_maintains_existing(self, use_flag):
        User, Address = self.classes("User", "Address")

        stmt = select(User.id).select_from(Address)

        if use_flag:
            stmt = stmt.with_only_columns(
                func.count(), maintain_column_froms=True
            )
        else:
            stmt = stmt.select_from(
                *stmt.columns_clause_froms
            ).with_only_columns(func.count())

        # Address is maintained in the FROM list
        self.assert_compile(
            stmt, "SELECT count(*) AS count_1 FROM addresses, users"
        )

    @testing.combinations((True,), (False,))
    def test_replace_into_select_from_with_loader_criteria(self, use_flag):
        User, Address = self.classes("User", "Address")

        stmt = select(User.id).options(
            with_loader_criteria(User, User.name == "ed")
        )

        if use_flag:
            stmt = stmt.with_only_columns(
                func.count(), maintain_column_froms=True
            )
        else:
            stmt = stmt.select_from(
                *stmt.columns_clause_froms
            ).with_only_columns(func.count())

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM users WHERE users.name = :name_1",
        )


class JoinTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_join_from_no_onclause(self):
        User, Address = self.classes("User", "Address")

        stmt = select(literal_column("1")).join_from(User, Address)
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users JOIN addresses "
            "ON users.id = addresses.user_id",
        )

    def test_join_from_w_relationship(self):
        User, Address = self.classes("User", "Address")

        stmt = select(literal_column("1")).join_from(
            User, Address, User.addresses
        )
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users JOIN addresses "
            "ON users.id = addresses.user_id",
        )

    def test_join_from_alised_w_relationship(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(literal_column("1")).join_from(u1, Address, u1.addresses)
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users AS users_1 JOIN addresses "
            "ON users_1.id = addresses.user_id",
        )

    def test_join_conflicting_right_side(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).join(Address, User.orders)
        assert_raises_message(
            exc.InvalidRequestError,
            "Join target .*Address.* does not correspond to the right side "
            "of join condition User.orders",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_plain(self):
        User, Address, Order = self.classes("User", "Address", "Order")

        stmt = select(User).join_from(User, Address, Order.address)
        assert_raises_message(
            exc.InvalidRequestError,
            r"explicit from clause .*User.* does not match .* Order.address",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_mapper_vs_aliased(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(User).join_from(User, Address, u1.addresses)
        assert_raises_message(
            exc.InvalidRequestError,
            # the display of the attribute here is not consistent vs.
            # the straight aliased class, should improve this.
            r"explicit from clause .*User.* does not match left side .*"
            r"of relationship attribute aliased\(User\).addresses",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_aliased_vs_mapper(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(u1).join_from(u1, Address, User.addresses)
        assert_raises_message(
            exc.InvalidRequestError,
            r"explicit from clause aliased\(User\) does not match left "
            "side of relationship attribute User.addresses",
            stmt.compile,
        )

    def test_join_from_we_can_explicitly_tree_joins(self):
        User, Address, Order, Item, Keyword = self.classes(
            "User", "Address", "Order", "Item", "Keyword"
        )

        stmt = (
            select(User)
            .join(User.addresses)
            .join_from(User, Order, User.orders)
            .join(Order.items)
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id JOIN orders "
            "ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items "
            "ON items.id = order_items_1.item_id",
        )

    def test_join_from_w_filter_by(self):
        User, Address, Order, Item, Keyword = self.classes(
            "User", "Address", "Order", "Item", "Keyword"
        )

        stmt = (
            select(User)
            .filter_by(name="n1")
            .join(User.addresses)
            .filter_by(email_address="a1")
            .join_from(User, Order, User.orders)
            .filter_by(description="d1")
            .join(Order.items)
            .filter_by(description="d2")
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id "
            "JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "WHERE users.name = :name_1 "
            "AND addresses.email_address = :email_address_1 "
            "AND orders.description = :description_1 "
            "AND items.description = :description_2",
            checkparams={
                "name_1": "n1",
                "email_address_1": "a1",
                "description_1": "d1",
                "description_2": "d2",
            },
        )

    @testing.combinations(
        (
            lambda User: select(User).where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join_from(User, Address)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join_from(User, Address, User.addresses)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join(User.addresses)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
    )
    def test_params_with_join(
        self, test_case, expected, bindparams, expected_params
    ):
        User, Address = self.classes("User", "Address")

        stmt = resolve_lambda(test_case, **locals())

        stmt = stmt.params(**bindparams)

        self.assert_compile(stmt, expected, checkparams=expected_params)

    @testing.fixture
    def grandchild_fixture(self, decl_base):
        class Parent(decl_base):
            __tablename__ = "parent"
            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str]

        class Child(decl_base):
            __tablename__ = "child"
            id: Mapped[int] = mapped_column(primary_key=True)
            parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))

        class GrandchildWParent(decl_base):
            __tablename__ = "grandchildwparent"
            id: Mapped[int] = mapped_column(primary_key=True)
            parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))
            child_id: Mapped[int] = mapped_column(ForeignKey("child.id"))

        return Parent, Child, GrandchildWParent

    @testing.variation(
        "jointype",
        ["child_grandchild", "parent_grandchild", "grandchild_alone"],
    )
    def test_join_from_favors_explicit_left(
        self, grandchild_fixture, jointype
    ):
        """test #12931 in terms of ORM joins"""

        Parent, Child, GrandchildWParent = grandchild_fixture

        if jointype.child_grandchild:
            stmt = (
                select(Parent)
                .join_from(Parent, Child)
                .join_from(Child, GrandchildWParent)
            )

            self.assert_compile(
                stmt,
                "SELECT parent.id, parent.data FROM parent JOIN "
                "child ON parent.id = child.parent_id "
                "JOIN grandchildwparent "
                "ON child.id = grandchildwparent.child_id",
            )

        elif jointype.parent_grandchild:
            stmt = (
                select(Parent)
                .join_from(Parent, Child)
                .join_from(Parent, GrandchildWParent)
            )

            self.assert_compile(
                stmt,
                "SELECT parent.id, parent.data FROM parent "
                "JOIN child ON parent.id = child.parent_id "
                "JOIN grandchildwparent "
                "ON parent.id = grandchildwparent.parent_id",
            )
        elif jointype.grandchild_alone:
            stmt = (
                select(Parent).join_from(Parent, Child).join(GrandchildWParent)
            )

            self.assert_compile(
                stmt,
                "SELECT parent.id, parent.data FROM parent "
                "JOIN child ON parent.id = child.parent_id "
                "JOIN grandchildwparent "
                "ON child.id = grandchildwparent.child_id",
            )
        else:
            jointype.fail()


class LoadersInSubqueriesTest(QueryTest, AssertsCompiledSQL):
    """The Query object calls eanble_eagerloads(False) when you call
    .subquery().  With Core select, we don't have that information, we instead
    have to look at the "toplevel" flag to know where we are.   make sure
    the many different combinations that these two objects and still
    too many flags at the moment work as expected on the outside.

    """

    __dialect__ = "default"

    run_setup_mappers = None

    @testing.fixture
    def joinedload_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="joined")},
        )

        self.mapper_registry.map_imperatively(Address, addresses)

        return User, Address

    @testing.fixture
    def deferred_fixture(self):
        User = self.classes.User
        users = self.tables.users

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name": deferred(users.c.name),
                "name_upper": column_property(
                    func.upper(users.c.name), deferred=True
                ),
            },
        )

        return User

    @testing.fixture
    def non_deferred_fixture(self):
        User = self.classes.User
        users = self.tables.users

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name_upper": column_property(func.upper(users.c.name))
            },
        )

        return User

    def test_no_joinedload_in_subquery_select_rows(self, joinedload_fixture):
        User, Address = joinedload_fixture

        sess = fixture_session()
        stmt1 = sess.query(User).subquery()
        stmt1 = sess.query(stmt1)

        stmt2 = select(User).subquery()

        stmt2 = select(stmt2)

        expected = (
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users) AS anon_1"
        )
        self.assert_compile(
            stmt1._final_statement(legacy_query_style=False),
            expected,
        )

        self.assert_compile(stmt2, expected)

    def test_no_joinedload_in_subquery_select_entity(self, joinedload_fixture):
        User, Address = joinedload_fixture

        sess = fixture_session()
        stmt1 = sess.query(User).subquery()
        ua = aliased(User, stmt1)
        stmt1 = sess.query(ua)

        stmt2 = select(User).subquery()

        ua = aliased(User, stmt2)
        stmt2 = select(ua)

        expected = (
            "SELECT anon_1.id, anon_1.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM "
            "(SELECT users.id AS id, users.name AS name FROM users) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.id = addresses_1.user_id"
        )

        self.assert_compile(
            stmt1._final_statement(legacy_query_style=False),
            expected,
        )

        self.assert_compile(stmt2, expected)

    def test_deferred_subq_one(self, deferred_fixture):
        """test for #6661"""
        User = deferred_fixture

        subq = select(User).subquery()

        u1 = aliased(User, subq)
        q = select(u1)

        self.assert_compile(
            q,
            "SELECT anon_1.id "
            "FROM (SELECT users.id AS id, users.name AS name "
            "FROM users) AS anon_1",
        )

        # testing deferred opts separately for deterministic SQL generation

        q = select(u1).options(undefer(u1.name))

        self.assert_compile(
            q,
            "SELECT anon_1.id, anon_1.name "
            "FROM (SELECT users.id AS id, users.name AS name "
            "FROM users) AS anon_1",
        )

        q = select(u1).options(undefer(u1.name_upper))

        self.assert_compile(
            q,
            "SELECT upper(anon_1.name) AS upper_1, anon_1.id "
            "FROM (SELECT users.id AS id, users.name AS name "
            "FROM users) AS anon_1",
        )

    def test_non_deferred_subq_one(self, non_deferred_fixture):
        """test for #6661

        cols that aren't deferred go into subqueries.  1.3 did this also.

        """
        User = non_deferred_fixture

        subq = select(User).subquery()

        u1 = aliased(User, subq)
        q = select(u1)

        self.assert_compile(
            q,
            "SELECT upper(anon_1.name) AS upper_1, anon_1.id, anon_1.name "
            "FROM (SELECT upper(users.name) AS upper_2, users.id AS id, "
            "users.name AS name FROM users) AS anon_1",
        )

    def test_deferred_subq_two(self, deferred_fixture):
        """test for #6661

        in this test, we are only confirming the current contract of ORM
        subqueries which is that deferred + derived column_property's don't
        export themselves into the .c. collection of a subquery.
        We might want to revisit this in some way.

        """
        User = deferred_fixture

        subq = select(User).subquery()

        assert not hasattr(subq.c, "name_upper")

        # "undefer" it by including it
        subq = select(User, User.name_upper).subquery()

        assert hasattr(subq.c, "name_upper")

    def test_non_deferred_col_prop_targetable_in_subq(
        self, non_deferred_fixture
    ):
        """test for #6661"""
        User = non_deferred_fixture

        subq = select(User).subquery()

        assert hasattr(subq.c, "name_upper")

    def test_recursive_cte_render_on_deferred(self, deferred_fixture):
        """test for #6661.

        this test is most directly the bug reported in #6661,
        as the CTE uses stmt._exported_columns_iterator() ahead of compiling
        the SELECT in order to get the list of columns that will be selected,
        this has to match what the subquery is going to render.

        This is also pretty fundamental to why deferred() as an option
        can't be honored in a subquery; the subquery needs to export the
        correct columns and it needs to do so without having to process
        all the loader options.  1.3 OTOH when you got a subquery from
        Query, it did a full compile_context.  1.4/2.0 we don't do that
        anymore.

        """

        User = deferred_fixture

        cte = select(User).cte(recursive=True)

        # nonsensical, but we are just testing form
        cte = cte.union_all(select(User).join(cte, cte.c.id == User.id))

        stmt = select(User).join(cte, User.id == cte.c.id)

        self.assert_compile(
            stmt,
            "WITH RECURSIVE anon_1(id, name) AS "
            "(SELECT users.id AS id, users.name AS name FROM users "
            "UNION ALL SELECT users.id AS id, users.name AS name "
            "FROM users JOIN anon_1 ON anon_1.id = users.id) "
            "SELECT users.id FROM users JOIN anon_1 ON users.id = anon_1.id",
        )

        # testing deferred opts separately for deterministic SQL generation
        self.assert_compile(
            stmt.options(undefer(User.name_upper)),
            "WITH RECURSIVE anon_1(id, name) AS "
            "(SELECT users.id AS id, users.name AS name FROM users "
            "UNION ALL SELECT users.id AS id, users.name AS name "
            "FROM users JOIN anon_1 ON anon_1.id = users.id) "
            "SELECT upper(users.name) AS upper_1, users.id "
            "FROM users JOIN anon_1 ON users.id = anon_1.id",
        )

        self.assert_compile(
            stmt.options(undefer(User.name)),
            "WITH RECURSIVE anon_1(id, name) AS "
            "(SELECT users.id AS id, users.name AS name FROM users "
            "UNION ALL SELECT users.id AS id, users.name AS name "
            "FROM users JOIN anon_1 ON anon_1.id = users.id) "
            "SELECT users.id, users.name "
            "FROM users JOIN anon_1 ON users.id = anon_1.id",
        )

    def test_nested_union_deferred(self, deferred_fixture):
        """test #6678"""
        User = deferred_fixture

        s1 = select(User).where(User.id == 5)
        s2 = select(User).where(User.id == 6)

        s3 = select(User).where(User.id == 7)

        stmt = union(s1.union(s2), s3)

        u_alias = aliased(User, stmt.subquery())

        self.assert_compile(
            select(u_alias),
            "SELECT anon_1.id FROM ((SELECT users.id AS id, "
            "users.name AS name "
            "FROM users "
            "WHERE users.id = :id_1 UNION SELECT users.id AS id, "
            "users.name AS name "
            "FROM users WHERE users.id = :id_2) "
            "UNION SELECT users.id AS id, users.name AS name "
            "FROM users WHERE users.id = :id_3) AS anon_1",
        )

    def test_nested_union_undefer_option(self, deferred_fixture):
        """test #6678

        in this case we want to see that the unions include the deferred
        columns so that if we undefer on the outside we can get the
        column.

        """
        User = deferred_fixture

        s1 = select(User).where(User.id == 5)
        s2 = select(User).where(User.id == 6)

        s3 = select(User).where(User.id == 7)

        stmt = union(s1.union(s2), s3)

        u_alias = aliased(User, stmt.subquery())

        self.assert_compile(
            select(u_alias).options(undefer(u_alias.name)),
            "SELECT anon_1.id, anon_1.name FROM "
            "((SELECT users.id AS id, users.name AS name FROM users "
            "WHERE users.id = :id_1 UNION SELECT users.id AS id, "
            "users.name AS name "
            "FROM users WHERE users.id = :id_2) "
            "UNION SELECT users.id AS id, users.name AS name "
            "FROM users WHERE users.id = :id_3) AS anon_1",
        )


class ExtraColsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = None

    @testing.fixture
    def query_expression_fixture(self):
        users, User = (
            self.tables.users,
            self.classes.User,
        )
        addresses, Address = (self.tables.addresses, self.classes.Address)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("value", query_expression()),
                    (
                        "value_w_default",
                        query_expression(default_expr=literal(15)),
                    ),
                ]
            ),
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        return User

    @testing.fixture
    def deferred_fixture(self):
        User = self.classes.User
        users = self.tables.users

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name": deferred(users.c.name),
                "name_upper": column_property(
                    func.upper(users.c.name), deferred=True
                ),
            },
        )

        return User

    @testing.fixture
    def query_expression_w_joinedload_fixture(self):
        users, User = (
            self.tables.users,
            self.classes.User,
        )
        addresses, Address = (self.tables.addresses, self.classes.Address)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("value", query_expression()),
                    (
                        "addresses",
                        relationship(
                            Address,
                            primaryjoin=and_(
                                addresses.c.user_id == users.c.id,
                                addresses.c.email_address != None,
                            ),
                        ),
                    ),
                ]
            ),
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        return User

    @testing.fixture
    def column_property_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("concat", column_property(users.c.id * 2)),
                    (
                        "count",
                        column_property(
                            select(func.count(addresses.c.id))
                            .where(
                                users.c.id == addresses.c.user_id,
                            )
                            .correlate(users)
                            .scalar_subquery()
                        ),
                    ),
                ]
            ),
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                )
            },
        )

        return User, Address

    @testing.fixture
    def plain_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="user")
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )

        return User, Address

    @testing.fixture
    def hard_labeled_self_ref_fixture(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            data = Column(String)
            data_lower = column_property(func.lower(data).label("hardcoded"))

            as_ = relationship("A")

        return A

    def test_no_joinedload_embedded(self, plain_fixture):
        User, Address = plain_fixture

        stmt = select(Address).options(joinedload(Address.user))

        subq = stmt.subquery()

        s2 = select(subq)

        self.assert_compile(
            s2,
            "SELECT anon_1.id, anon_1.user_id, anon_1.email_address "
            "FROM (SELECT addresses.id AS id, addresses.user_id AS "
            "user_id, addresses.email_address AS email_address "
            "FROM addresses) AS anon_1",
        )

    def test_with_expr_one(self, query_expression_fixture):
        User = query_expression_fixture

        stmt = select(User).options(
            with_expression(User.value, User.name + "foo")
        )

        self.assert_compile(
            stmt,
            "SELECT users.name || :name_1 AS anon_1, :param_1 AS anon_2, "
            "users.id, "
            "users.name FROM users",
        )

    def test_exported_columns_query_expression(self, query_expression_fixture):
        """test behaviors related to #8881"""
        User = query_expression_fixture

        stmt = select(User)

        eq_(
            stmt.selected_columns.keys(),
            ["value_w_default", "id", "name"],
        )

        stmt = select(User).options(
            with_expression(User.value, User.name + "foo")
        )

        # bigger problem.  we still don't include 'value', because we dont
        # run query options here.  not "correct", but is at least consistent
        # with deferred
        eq_(
            stmt.selected_columns.keys(),
            ["value_w_default", "id", "name"],
        )

    def test_exported_columns_colprop(self, column_property_fixture):
        """test behaviors related to #8881"""
        User, _ = column_property_fixture

        stmt = select(User)

        # we get all the cols because they are not deferred and have a value
        eq_(
            stmt.selected_columns.keys(),
            ["concat", "count", "id", "name"],
        )

    def test_exported_columns_deferred(self, deferred_fixture):
        """test behaviors related to #8881"""
        User = deferred_fixture

        stmt = select(User)

        # don't include 'name_upper' as it's deferred and readonly.
        # "name" however is a column on the table, so even though it is
        # deferred, it gets special treatment (related to #6661)
        eq_(
            stmt.selected_columns.keys(),
            ["id", "name"],
        )

        stmt = select(User).options(
            undefer(User.name), undefer(User.name_upper)
        )

        # undefer doesn't affect the readonly col because we dont look
        # at options when we do selected_columns
        eq_(
            stmt.selected_columns.keys(),
            ["id", "name"],
        )

    def test_with_expr_two(self, query_expression_fixture):
        User = query_expression_fixture

        stmt = select(User.id, User.name, (User.name + "foo").label("foo"))

        subq = stmt.subquery()
        u1 = aliased(User, subq)

        stmt = select(u1).options(with_expression(u1.value, subq.c.foo))

        self.assert_compile(
            stmt,
            "SELECT anon_1.foo, :param_1 AS anon_2, anon_1.id, "
            "anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name, "
            "users.name || :name_1 AS foo FROM users) AS anon_1",
        )

    def test_with_expr_three(self, query_expression_w_joinedload_fixture):
        """test :ticket:`6259`"""
        User = query_expression_w_joinedload_fixture

        stmt = select(User).options(joinedload(User.addresses)).limit(1)

        # test that the outer IS NULL is rendered
        # test that the inner query does not include a NULL default
        self.assert_compile(
            stmt,
            "SELECT anon_1.id, anon_1.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM "
            "(SELECT users.id AS id, users.name AS name FROM users "
            "LIMIT :param_1) AS anon_1 LEFT OUTER "
            "JOIN addresses AS addresses_1 ON addresses_1.user_id = anon_1.id "
            "AND addresses_1.email_address IS NOT NULL",
        )

    def test_with_expr_four(self, query_expression_w_joinedload_fixture):
        """test :ticket:`6259`"""
        User = query_expression_w_joinedload_fixture

        stmt = (
            select(User)
            .options(
                with_expression(User.value, null()), joinedload(User.addresses)
            )
            .limit(1)
        )

        # test that the outer IS NULL is rendered, not adapted
        # test that the inner query includes the NULL we asked for

        # ironically, this statement would not actually fetch due to the NULL
        # not allowing adaption and therefore failing on the result set
        # matching, this was addressed in #7154.
        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1, anon_2.id, anon_2.name, "
            "addresses_1.id AS id_1, addresses_1.user_id, "
            "addresses_1.email_address FROM (SELECT NULL AS anon_1, "
            "users.id AS id, users.name AS name FROM users LIMIT :param_1) "
            "AS anon_2 LEFT OUTER JOIN addresses AS addresses_1 "
            "ON addresses_1.user_id = anon_2.id "
            "AND addresses_1.email_address IS NOT NULL",
        )

    def test_joinedload_outermost(self, plain_fixture):
        User, Address = plain_fixture

        stmt = select(Address).options(joinedload(Address.user))

        # render joined eager loads with stringify
        self.assert_compile(
            stmt,
            "SELECT addresses.id, addresses.user_id, addresses.email_address, "
            "users_1.id AS id_1, users_1.name FROM addresses "
            "LEFT OUTER JOIN users AS users_1 "
            "ON users_1.id = addresses.user_id",
        )

    def test_joinedload_outermost_w_wrapping_elements(self, plain_fixture):
        User, Address = plain_fixture

        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .limit(10)
            .distinct()
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.id, anon_1.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM "
            "(SELECT DISTINCT users.id AS id, users.name AS name FROM users "
            "LIMIT :param_1) "
            "AS anon_1 LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.id = addresses_1.user_id",
        )

    def test_contains_eager_outermost_w_wrapping_elements(self, plain_fixture):
        """test #8569"""

        User, Address = plain_fixture

        stmt = (
            select(User)
            .join(User.addresses)
            .options(contains_eager(User.addresses))
            .limit(10)
            .distinct()
        )

        self.assert_compile(
            stmt,
            "SELECT DISTINCT addresses.id, addresses.user_id, "
            "addresses.email_address, users.id AS id_1, users.name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "LIMIT :param_1",
        )

    def test_joinedload_hard_labeled_selfref(
        self, hard_labeled_self_ref_fixture
    ):
        """test #8569"""

        A = hard_labeled_self_ref_fixture

        stmt = select(A).options(joinedload(A.as_)).distinct()
        self.assert_compile(
            stmt,
            "SELECT anon_1.hardcoded, anon_1.id, anon_1.a_id, anon_1.data, "
            "lower(a_1.data) AS lower_1, a_1.id AS id_1, a_1.a_id AS a_id_1, "
            "a_1.data AS data_1 FROM (SELECT DISTINCT lower(a.data) AS "
            "hardcoded, a.id AS id, a.a_id AS a_id, a.data AS data FROM a) "
            "AS anon_1 LEFT OUTER JOIN a AS a_1 ON anon_1.id = a_1.a_id",
        )

    def test_contains_eager_hard_labeled_selfref(
        self, hard_labeled_self_ref_fixture
    ):
        """test #8569"""

        A = hard_labeled_self_ref_fixture

        a1 = aliased(A)
        stmt = (
            select(A)
            .join(A.as_.of_type(a1))
            .options(contains_eager(A.as_.of_type(a1)))
            .distinct()
        )
        self.assert_compile(
            stmt,
            "SELECT DISTINCT lower(a.data) AS hardcoded, "
            "lower(a_1.data) AS hardcoded, a_1.id, a_1.a_id, a_1.data, "
            "a.id AS id_1, a.a_id AS a_id_1, a.data AS data_1 "
            "FROM a JOIN a AS a_1 ON a.id = a_1.a_id",
        )

    def test_column_properties(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        stmt = select(User)

        self.assert_compile(
            stmt,
            "SELECT users.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users.id = addresses.user_id) AS anon_2, users.id, "
            "users.name FROM users",
            checkparams={"id_1": 2},
        )

    def test_column_properties_can_we_use(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        # User, Address = column_property_fixture

        # stmt = select(User)

        # TODO: shouldn't we be able to get at count ?

    # stmt = stmt.where(stmt.selected_columns.count > 5)

    # self.assert_compile(stmt, "")

    def test_column_properties_subquery(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        stmt = select(User)

        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(stmt.subquery())

        # TODO: shouldn't we be able to get to stmt.subquery().c.count ?
        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1, anon_2.anon_3, anon_2.id, anon_2.name "
            "FROM (SELECT users.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users.id = addresses.user_id) AS anon_3, users.id AS id, "
            "users.name AS name FROM users) AS anon_2",
            checkparams={"id_1": 2},
        )

    def test_column_properties_subquery_two(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        # col properties will retain anonymous labels, however will
        # adopt the .key within the subquery collection so they can
        # be addressed.
        stmt = select(
            User.id,
            User.name,
            User.concat,
            User.count,
        )

        subq = stmt.subquery()
        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(subq).where(subq.c.concat == "foo")

        self.assert_compile(
            stmt,
            "SELECT anon_1.id, anon_1.name, anon_1.anon_2, anon_1.anon_3 "
            "FROM (SELECT users.id AS id, users.name AS name, "
            "users.id * :id_1 AS anon_2, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users.id = addresses.user_id) AS anon_3 "
            "FROM users) AS anon_1 WHERE anon_1.anon_2 = :param_1",
            checkparams={"id_1": 2, "param_1": "foo"},
        )

    def test_column_properties_aliased_subquery(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        u1 = aliased(User)
        stmt = select(u1)

        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(stmt.subquery())
        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1, anon_2.anon_3, anon_2.id, anon_2.name "
            "FROM (SELECT users_1.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users_1.id = addresses.user_id) AS anon_3, "
            "users_1.id AS id, users_1.name AS name "
            "FROM users AS users_1) AS anon_2",
            checkparams={"id_1": 2},
        )


class RelationshipNaturalCompileTest(QueryTest, AssertsCompiledSQL):
    """test using core join() with relationship attributes.

    as __clause_element__() produces a workable SQL expression, this should
    be generally possible.

    However, it can't work for many-to-many relationships, as these
    require two joins.    Only the ORM can look at the entities and decide
    that there's a separate "secondary" table to be rendered as a separate
    join.

    """

    __dialect__ = "default"

    def test_of_type_implicit_join(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)
        a1 = aliased(Address)

        stmt1 = select(u1).where(u1.addresses.of_type(a1))
        stmt2 = (
            fixture_session()
            .query(u1)
            .filter(u1.addresses.of_type(a1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT users_1.id, users_1.name FROM users AS users_1, "
            "addresses AS addresses_1 WHERE users_1.id = addresses_1.user_id"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_of_type_explicit_join(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)
        a1 = aliased(Address)

        stmt = select(u1).join(u1.addresses.of_type(a1))

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name FROM users AS users_1 "
            "JOIN addresses AS addresses_1 "
            "ON users_1.id = addresses_1.user_id",
        )

    def test_many_to_many_explicit_join(self):
        Item, Keyword = self.classes("Item", "Keyword")

        stmt = select(Item).join(Keyword, Item.keywords)

        self.assert_compile(
            stmt,
            "SELECT items.id, items.description FROM items "
            "JOIN item_keywords AS item_keywords_1 "
            "ON items.id = item_keywords_1.item_id "
            "JOIN keywords ON keywords.id = item_keywords_1.keyword_id",
        )

    def test_many_to_many_implicit_join(self):
        Item, Keyword = self.classes("Item", "Keyword")

        stmt = select(Item).where(Item.keywords)

        # this was the intent of the primary + secondary clauseelement.
        # it can do enough of the right thing in an implicit join
        # context.
        self.assert_compile(
            stmt,
            "SELECT items.id, items.description FROM items, "
            "item_keywords AS item_keywords_1, keywords "
            "WHERE items.id = item_keywords_1.item_id "
            "AND keywords.id = item_keywords_1.keyword_id",
        )


class InheritedTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = "once"


class ExplicitWithPolymorphicTest(
    _poly_fixtures._PolymorphicUnions, AssertsCompiledSQL
):
    __dialect__ = "default"

    default_punion = (
        "(SELECT pjoin.person_id AS person_id, "
        "pjoin.company_id AS company_id, "
        "pjoin.name AS name, pjoin.type AS type, "
        "pjoin.status AS status, pjoin.engineer_name AS engineer_name, "
        "pjoin.primary_language AS primary_language, "
        "pjoin.manager_name AS manager_name "
        "FROM (SELECT engineers.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "CAST(NULL AS VARCHAR(50)) AS manager_name "
        "FROM people JOIN engineers ON people.person_id = engineers.person_id "
        "UNION ALL SELECT managers.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, managers.status AS status, "
        "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
        "CAST(NULL AS VARCHAR(50)) AS primary_language, "
        "managers.manager_name AS manager_name FROM people "
        "JOIN managers ON people.person_id = managers.person_id) AS pjoin) "
        "AS anon_1"
    )

    def test_subquery_col_expressions_wpoly_one(self):
        Person, Manager, Engineer = self.classes(
            "Person", "Manager", "Engineer"
        )

        wp1 = with_polymorphic(Person, [Manager, Engineer])

        subq1 = select(wp1).subquery()

        wp2 = with_polymorphic(Person, [Engineer, Manager])
        subq2 = select(wp2).subquery()

        # first thing we see, is that when we go through with_polymorphic,
        # the entities that get placed into the aliased class go through
        # Mapper._mappers_from_spec(), which matches them up to the
        # existing Mapper.self_and_descendants collection, meaning,
        # the order is the same every time.   Assert here that's still
        # happening.  If a future internal change modifies this assumption,
        # that's not necessarily bad, but it would change things.

        eq_(
            subq1.c.keys(),
            [
                "person_id",
                "company_id",
                "name",
                "type",
                "person_id_1",
                "status",
                "engineer_name",
                "primary_language",
                "person_id_1",
                "status_1",
                "manager_name",
            ],
        )
        eq_(
            subq2.c.keys(),
            [
                "person_id",
                "company_id",
                "name",
                "type",
                "person_id_1",
                "status",
                "engineer_name",
                "primary_language",
                "person_id_1",
                "status_1",
                "manager_name",
            ],
        )

    def test_subquery_col_expressions_wpoly_two(self):
        Person, Manager, Engineer = self.classes(
            "Person", "Manager", "Engineer"
        )

        wp1 = with_polymorphic(Person, [Manager, Engineer])

        subq1 = select(wp1).subquery()

        stmt = select(subq1).where(
            or_(
                subq1.c.engineer_name == "dilbert",
                subq1.c.manager_name == "dogbert",
            )
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.person_id, anon_1.company_id, anon_1.name, "
            "anon_1.type, anon_1.person_id AS person_id_1, anon_1.status, "
            "anon_1.engineer_name, anon_1.primary_language, "
            "anon_1.person_id AS person_id_2, anon_1.status AS status_1, "
            "anon_1.manager_name FROM "
            "%s WHERE "
            "anon_1.engineer_name = :engineer_name_1 "
            "OR anon_1.manager_name = :manager_name_1" % (self.default_punion),
        )


class ImplicitWithPolymorphicTest(
    _poly_fixtures._PolymorphicUnions, AssertsCompiledSQL
):
    """Test a series of mappers with a very awkward with_polymorphic setting,
    that tables and columns are rendered using the selectable in the correct
    contexts.  PolymorphicUnions represent the most awkward and verbose
    polymorphic fixtures you can have.   expressions need to be maximally
    accurate in terms of the mapped selectable in order to produce correct
    queries, which also will be really wrong if that mapped selectable is not
    in use.

    """

    __dialect__ = "default"

    def test_select_columns_where_baseclass(self):
        Person = self.classes.Person

        stmt = (
            select(Person.person_id, Person.name)
            .where(Person.name == "some name")
            .order_by(Person.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Person.person_id, Person.name)
            .filter(Person.name == "some name")
            .order_by(Person.person_id)
        )

        expected = (
            "SELECT pjoin.person_id, pjoin.name FROM "
            "(SELECT engineers.person_id AS person_id, people.company_id AS "
            "company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, engineers.engineer_name AS "
            "engineer_name, engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) AS "
            "pjoin WHERE pjoin.name = :name_1 ORDER BY pjoin.person_id"
        )
        self.assert_compile(stmt, expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )

    def test_select_where_baseclass(self):
        Person = self.classes.Person

        stmt = (
            select(Person)
            .where(Person.name == "some name")
            .order_by(Person.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Person)
            .filter(Person.name == "some name")
            .order_by(Person.person_id)
        )

        expected = (
            "SELECT pjoin.person_id, pjoin.company_id, pjoin.name, "
            "pjoin.type, pjoin.status, pjoin.engineer_name, "
            "pjoin.primary_language, pjoin.manager_name FROM "
            "(SELECT engineers.person_id AS person_id, people.company_id "
            "AS company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, engineers.engineer_name AS "
            "engineer_name, engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) AS "
            "pjoin WHERE pjoin.name = :name_1 ORDER BY pjoin.person_id"
        )
        self.assert_compile(stmt, expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )

    def test_select_where_subclass(self):
        Engineer = self.classes.Engineer

        # what will *not* work with Core, that the ORM does for now,
        # is that if you do where/orderby Person.column, it will de-adapt
        # the Person columns from the polymorphic union

        stmt = (
            select(Engineer)
            .where(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Engineer)
            .filter(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        plain_expected = (  # noqa
            "SELECT engineers.person_id, people.person_id, people.company_id, "
            "people.name, "
            "people.type, engineers.status, "
            "engineers.engineer_name, engineers.primary_language "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )
        # when we have disambiguating labels turned on
        disambiguate_expected = (  # noqa
            "SELECT engineers.person_id, people.person_id AS person_id_1, "
            "people.company_id, "
            "people.name, "
            "people.type, engineers.status, "
            "engineers.engineer_name, engineers.primary_language "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )

        # these change based on how we decide to apply labels
        # in context.py
        self.assert_compile(stmt, disambiguate_expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            disambiguate_expected,
        )

    def test_select_where_columns_subclass(self):
        Engineer = self.classes.Engineer

        # what will *not* work with Core, that the ORM does for now,
        # is that if you do where/orderby Person.column, it will de-adapt
        # the Person columns from the polymorphic union

        # After many attempts to get the JOIN to render, by annotating
        # the columns with the "join" that they come from and trying to
        # get Select() to render out that join, there's no approach
        # that really works without stepping on other assumptions, so
        # add select_from(Engineer) explicitly.   It's still puzzling why the
        # ORM seems to know how to make this decision more effectively
        # when the select() has the same amount of information.
        stmt = (
            select(Engineer.person_id, Engineer.name)
            .where(Engineer.name == "some name")
            .select_from(Engineer)
            .order_by(Engineer.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Engineer.person_id, Engineer.name)
            .filter(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        expected = (
            "SELECT engineers.person_id, people.name "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )

        self.assert_compile(stmt, expected)
        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )


class RelationshipNaturalInheritedTest(InheritedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id"
    )

    default_pjoin = (
        "(people LEFT OUTER "
        "JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id "
        "LEFT OUTER JOIN boss ON managers.person_id = boss.boss_id) "
        "ON companies.company_id = people.company_id"
    )

    flat_aliased_pjoin = (
        "(people AS people_1 LEFT OUTER JOIN engineers AS "
        "engineers_1 ON people_1.person_id = engineers_1.person_id "
        "LEFT OUTER JOIN managers AS managers_1 "
        "ON people_1.person_id = managers_1.person_id "
        "LEFT OUTER JOIN boss AS boss_1 ON "
        "managers_1.person_id = boss_1.boss_id) "
        "ON companies.company_id = people_1.company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language "
        "AS engineers_primary_language, managers.person_id "
        "AS managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name, "
        "boss.boss_id AS boss_boss_id, boss.golf_swing AS boss_golf_swing "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON "
        "people.person_id = managers.person_id LEFT OUTER JOIN boss "
        "ON managers.person_id = boss.boss_id) AS anon_1 "
        "ON companies.company_id = anon_1.people_company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id "
        "JOIN paperwork ON people.person_id = paperwork.person_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id "
        "WHERE people.name = :name_1"
    )

    poly_columns = "SELECT people.person_id FROM people"

    def test_straight(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt1 = select(Company).select_from(
            orm_join(Company, Person, Company.employees)
        )
        stmt2 = select(Company).join(Company.employees)
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.straight_company_to_person_expected)
        self.assert_compile(stmt2, self.straight_company_to_person_expected)
        self.assert_compile(stmt3, self.straight_company_to_person_expected)

    def test_columns(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt = select(Person.person_id)

        self.assert_compile(stmt, self.poly_columns)

    def test_straight_whereclause(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt1 = (
            select(Company)
            .select_from(orm_join(Company, Person, Company.employees))
            .where(Person.name == "ed")
        )

        stmt2 = (
            select(Company).join(Company.employees).where(Person.name == "ed")
        )
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            .filter(Person.name == "ed")
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.c_to_p_whereclause)
        self.assert_compile(stmt2, self.c_to_p_whereclause)
        self.assert_compile(stmt3, self.c_to_p_whereclause)

    def test_two_level(self):
        Company, Person, Paperwork = self.classes(
            "Company", "Person", "Paperwork"
        )

        stmt1 = select(Company).select_from(
            orm_join(Company, Person, Company.employees).join(
                Paperwork, Person.paperwork
            )
        )

        stmt2 = select(Company).join(Company.employees).join(Person.paperwork)
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            .join(Person.paperwork)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.person_paperwork_expected)
        self.assert_compile(stmt2, self.person_paperwork_expected)
        self.assert_compile(stmt3, self.person_paperwork_expected)

    def test_wpoly_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        p1 = with_polymorphic(Person, "*")

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(Company.employees.of_type(p1))
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )
        expected = (
            "SELECT companies.company_id, companies.name "
            "FROM companies JOIN %s" % self.default_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)

    def test_wpoly_aliased_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )
        s = fixture_session()

        p1 = with_polymorphic(Person, "*", aliased=True)

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(p1, Company.employees.of_type(p1))

        stmt3 = (
            s.query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT companies.company_id, companies.name FROM companies "
            "JOIN %s" % self.aliased_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)

    def test_wpoly_aliased_flat_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        p1 = with_polymorphic(Person, "*", aliased=True, flat=True)

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(p1, Company.employees.of_type(p1))

        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT companies.company_id, companies.name FROM companies "
            "JOIN %s" % self.flat_aliased_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)


class RelNaturalAliasedJoinsTest(
    _poly_fixtures._PolymorphicAliasedJoins, RelationshipNaturalInheritedTest
):
    # this is the label style for the polymorphic selectable, not the
    # outside query
    label_style = LABEL_STYLE_TABLENAME_PLUS_COL

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin ON companies.company_id = "
        "pjoin.people_company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.people_company_id "
        "JOIN paperwork ON pjoin.people_person_id = paperwork.person_id"
    )

    default_pjoin = (
        "(SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin "
        "ON companies.company_id = pjoin.people_company_id"
    )
    flat_aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id "
        "AS engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, "
        "managers.status AS managers_status, managers.manager_name "
        "AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin_1 ON companies.company_id = pjoin_1.people_company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin_1 "
        "ON companies.company_id = pjoin_1.people_company_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.people_company_id "
        "WHERE pjoin.people_name = :people_name_1"
    )

    poly_columns = (
        "SELECT pjoin.people_person_id FROM (SELECT people.person_id AS "
        "people_person_id, people.company_id AS people_company_id, "
        "people.name AS people_name, people.type AS people_type, "
        "engineers.person_id AS engineers_person_id, engineers.status "
        "AS engineers_status, engineers.engineer_name AS "
        "engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin"
    )


class RelNaturalAliasedJoinsDisamTest(
    _poly_fixtures._PolymorphicAliasedJoins, RelationshipNaturalInheritedTest
):
    # this is the label style for the polymorphic selectable, not the
    # outside query
    label_style = LABEL_STYLE_DISAMBIGUATE_ONLY

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS person_id, people.company_id "
        "AS company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, managers.person_id "
        "AS person_id_2, managers.status AS status_1, managers.manager_name "
        "AS manager_name FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id "
        "JOIN paperwork ON pjoin.person_id = paperwork.person_id"
    )

    default_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, engineers.primary_language "
        "AS primary_language, managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin "
        "ON companies.company_id = pjoin.company_id"
    )
    flat_aliased_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin_1 ON companies.company_id = pjoin_1.company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, engineers.primary_language "
        "AS primary_language, managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin_1 "
        "ON companies.company_id = pjoin_1.company_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id "
        "WHERE pjoin.name = :name_1"
    )

    poly_columns = (
        "SELECT pjoin.person_id FROM (SELECT people.person_id AS "
        "person_id, people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin"
    )


class JoinedInhTest(
    InheritedTest, _poly_fixtures._Polymorphic, AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_load_only_on_sub_table(self):
        Company = self.classes.Company
        Engineer = self.classes.Engineer

        e1 = aliased(Engineer, inspect(Engineer).local_table)

        q = select(Company.name, e1.primary_language).join(
            Company.employees.of_type(e1)
        )

        self.assert_compile(
            q,
            "SELECT companies.name, engineers.primary_language "
            "FROM companies JOIN engineers "
            "ON companies.company_id = people.company_id",
        )

    def test_load_only_on_sub_table_aliased(self):
        Company = self.classes.Company
        Engineer = self.classes.Engineer

        e1 = aliased(Engineer, inspect(Engineer).local_table.alias())

        q = select(Company.name, e1.primary_language).join(
            Company.employees.of_type(e1)
        )

        self.assert_compile(
            q,
            "SELECT companies.name, engineers_1.primary_language "
            "FROM companies JOIN engineers AS engineers_1 "
            "ON companies.company_id = people.company_id",
        )

    def test_cte_recursive_handles_dupe_columns(self):
        """test #10169"""
        Engineer = self.classes.Engineer

        my_cte = select(Engineer).cte(recursive=True)

        self.assert_compile(
            select(my_cte),
            "WITH RECURSIVE anon_1(person_id, person_id_1, company_id, name, "
            "type, status, "
            "engineer_name, primary_language) AS (SELECT engineers.person_id "
            "AS person_id, people.person_id AS person_id_1, people.company_id "
            "AS company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, engineers.engineer_name AS "
            "engineer_name, engineers.primary_language AS primary_language "
            "FROM people JOIN engineers ON people.person_id = "
            "engineers.person_id) SELECT anon_1.person_id, "
            "anon_1.person_id_1, anon_1.company_id, "
            "anon_1.name, anon_1.type, anon_1.status, anon_1.engineer_name, "
            "anon_1.primary_language FROM anon_1",
        )

    @testing.variation("named", [True, False])
    @testing.variation("flat", [True, False])
    def test_aliased_joined_entities(self, named, flat):
        Company = self.classes.Company
        Engineer = self.classes.Engineer

        if named:
            e1 = aliased(Engineer, flat=flat, name="myengineer")
        else:
            e1 = aliased(Engineer, flat=flat)

        q = select(Company.name, e1.primary_language).join(
            Company.employees.of_type(e1)
        )

        if not flat:
            name = "anon_1" if not named else "myengineer"

            self.assert_compile(
                q,
                "SELECT companies.name, "
                f"{name}.engineers_primary_language FROM companies "
                "JOIN (SELECT people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.person_id AS engineers_person_id, "
                "engineers.status AS engineers_status, "
                "engineers.engineer_name AS engineers_engineer_name, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.person_id) AS "
                f"{name} "
                f"ON companies.company_id = {name}.people_company_id",
            )
        elif named:
            self.assert_compile(
                q,
                "SELECT companies.name, "
                "myengineer_engineers.primary_language "
                "FROM companies JOIN (people AS myengineer_people "
                "JOIN engineers AS myengineer_engineers "
                "ON myengineer_people.person_id = "
                "myengineer_engineers.person_id) "
                "ON companies.company_id = myengineer_people.company_id",
            )
        else:
            self.assert_compile(
                q,
                "SELECT companies.name, engineers_1.primary_language "
                "FROM companies JOIN (people AS people_1 "
                "JOIN engineers AS engineers_1 "
                "ON people_1.person_id = engineers_1.person_id) "
                "ON companies.company_id = people_1.company_id",
            )


class RawSelectTest(QueryTest, AssertsCompiledSQL):
    """older tests from test_query.   Here, they are converted to use
    future selects with ORM compilation.

    """

    __dialect__ = "default"

    def test_select_from_entity(self):
        User = self.classes.User

        self.assert_compile(
            select(literal_column("*")).select_from(User),
            "SELECT * FROM users",
        )

    def test_where_relationship(self):
        User = self.classes.User

        stmt1 = select(User).where(User.addresses)
        stmt2 = (
            fixture_session()
            .query(User)
            .filter(User.addresses)
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT users.id, users.name FROM users, addresses "
            "WHERE users.id = addresses.user_id"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_where_m2m_relationship(self):
        Item = self.classes.Item

        expected = (
            "SELECT items.id, items.description FROM items, "
            "item_keywords AS item_keywords_1, keywords "
            "WHERE items.id = item_keywords_1.item_id "
            "AND keywords.id = item_keywords_1.keyword_id"
        )

        stmt1 = select(Item).where(Item.keywords)
        stmt2 = (
            fixture_session()
            .query(Item)
            .filter(Item.keywords)
            ._final_statement(legacy_query_style=False)
        )
        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_inline_select_from_entity(self):
        User = self.classes.User

        expected = "SELECT * FROM users"
        stmt1 = select(literal_column("*")).select_from(User)
        stmt2 = (
            fixture_session()
            .query(literal_column("*"))
            .select_from(User)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_select_from_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(literal_column("*")).select_from(ua)
        stmt2 = (
            fixture_session()
            .query(literal_column("*"))
            .select_from(ua)
            ._final_statement(legacy_query_style=False)
        )

        expected = "SELECT * FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_correlate_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        expected = (
            "SELECT users.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users.id = addresses.user_id) AS anon_1 "
            "FROM users, addresses"
        )

        stmt1 = select(
            User.name,
            Address.id,
            select(func.count(Address.id))
            .where(User.id == Address.user_id)
            .correlate(User)
            .scalar_subquery(),
        )
        stmt2 = (
            fixture_session()
            .query(
                User.name,
                Address.id,
                select(func.count(Address.id))
                .where(User.id == Address.user_id)
                .correlate(User)
                .scalar_subquery(),
            )
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_correlate_aliased_entity(self):
        User = self.classes.User
        Address = self.classes.Address
        uu = aliased(User, name="uu")

        stmt1 = select(
            uu.name,
            Address.id,
            select(func.count(Address.id))
            .where(uu.id == Address.user_id)
            .correlate(uu)
            .scalar_subquery(),
        )

        stmt2 = (
            fixture_session()
            .query(
                uu.name,
                Address.id,
                select(func.count(Address.id))
                .where(uu.id == Address.user_id)
                .correlate(uu)
                .scalar_subquery(),
            )
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT uu.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE uu.id = addresses.user_id) AS anon_1 "
            "FROM users AS uu, addresses"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_entity(self):
        User = self.classes.User

        expected = "SELECT users.id, users.name FROM users"

        stmt1 = select(User)
        stmt2 = (
            fixture_session()
            .query(User)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_columns(self):
        User = self.classes.User

        expected = "SELECT users.id, users.name FROM users"

        stmt1 = select(User.id, User.name)
        stmt2 = (
            fixture_session()
            .query(User.id, User.name)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_aliased_columns(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(ua.id, ua.name)
        stmt2 = (
            fixture_session()
            .query(ua.id, ua.name)
            ._final_statement(legacy_query_style=False)
        )
        expected = "SELECT ua.id, ua.name FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(ua)
        stmt2 = (
            fixture_session()
            .query(ua)
            ._final_statement(legacy_query_style=False)
        )
        expected = "SELECT ua.id, ua.name FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_core_join_in_select_from_no_onclause(self):
        User = self.classes.User
        Address = self.classes.Address

        self.assert_compile(
            select(User).select_from(core_join(User, Address)),
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id",
        )

    def test_join_to_entity_no_onclause(self):
        User = self.classes.User
        Address = self.classes.Address

        self.assert_compile(
            select(User).join(Address),
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id",
        )

    def test_insert_from_query(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User.id, User.name).filter_by(name="ed")
        self.assert_compile(
            insert(Address).from_select(("id", "email_address"), q),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1",
        )

    def test_insert_from_query_col_attr(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User.id, User.name).filter_by(name="ed")
        self.assert_compile(
            insert(Address).from_select(
                (Address.id, Address.email_address), q
            ),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1",
        )

    def test_update_from_entity(self):
        User = self.classes.User
        self.assert_compile(
            update(User), "UPDATE users SET id=:id, name=:name"
        )

        self.assert_compile(
            update(User).values(name="ed").where(User.id == 5),
            "UPDATE users SET name=:name WHERE users.id = :id_1",
            checkparams={"id_1": 5, "name": "ed"},
        )

        self.assert_compile(
            update(User).values({User.name: "ed"}).where(User.id == 5),
            "UPDATE users SET name=:name WHERE users.id = :id_1",
            checkparams={"id_1": 5, "name": "ed"},
        )

    def test_delete_from_entity(self):
        User = self.classes.User
        self.assert_compile(delete(User), "DELETE FROM users")

        self.assert_compile(
            delete(User).where(User.id == 5),
            "DELETE FROM users WHERE users.id = :id_1",
            checkparams={"id_1": 5},
        )

    def test_insert_from_entity(self):
        User = self.classes.User
        self.assert_compile(
            insert(User), "INSERT INTO users (id, name) VALUES (:id, :name)"
        )

        self.assert_compile(
            insert(User).values(name="ed"),
            "INSERT INTO users (name) VALUES (:name)",
            checkparams={"name": "ed"},
        )

    def test_update_returning_star(self):
        User = self.classes.User
        self.assert_compile(
            update(User).returning(literal_column("*")),
            "UPDATE users SET id=:id, name=:name RETURNING *",
        )

    def test_delete_returning_star(self):
        User = self.classes.User
        self.assert_compile(
            delete(User).returning(literal_column("*")),
            "DELETE FROM users RETURNING *",
        )

    def test_insert_returning_star(self):
        User = self.classes.User
        self.assert_compile(
            insert(User).returning(literal_column("*")),
            "INSERT INTO users (id, name) VALUES (:id, :name) RETURNING *",
        )

    def test_col_prop_builtin_function(self):
        class Foo:
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            self.tables.users,
            properties={
                "foob": column_property(
                    func.coalesce(self.tables.users.c.name)
                )
            },
        )

        stmt1 = select(Foo).where(Foo.foob == "somename").order_by(Foo.foob)
        stmt2 = (
            fixture_session()
            .query(Foo)
            .filter(Foo.foob == "somename")
            .order_by(Foo.foob)
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT coalesce(users.name) AS coalesce_1, "
            "users.id, users.name FROM users "
            "WHERE coalesce(users.name) = :param_1 "
            "ORDER BY coalesce_1"
        )
        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)


class CorrelateTest(fixtures.DeclarativeMappedTest, _CoreCorrelateTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class T1(Base):
            __tablename__ = "t1"
            a = Column(Integer, primary_key=True)

            @hybridproperty
            def c(self):
                return self

        class T2(Base):
            __tablename__ = "t2"
            a = Column(Integer, primary_key=True)

            @hybridproperty
            def c(self):
                return self

    def _fixture(self):
        t1, t2 = self.classes("T1", "T2")
        return t1, t2, select(t1).where(t1.c.a == t2.c.a)


class CrudParamOverlapTest(test_compiler.CrudParamOverlapTest):
    @testing.fixture(
        params=Variation.generate_cases("type_", ["orm"]),
        ids=["orm"],
    )
    def crud_table_fixture(self, request):
        type_ = request.param

        if type_.orm:
            from sqlalchemy.orm import declarative_base

            Base = declarative_base()

            class Foo(Base):
                __tablename__ = "mytable"
                myid = Column(Integer, primary_key=True)
                name = Column(String)
                description = Column(String)

            table1 = Foo
        else:
            type_.fail()

        yield table1
