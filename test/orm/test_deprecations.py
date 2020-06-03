import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import collections
from sqlalchemy.orm import column_property
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_alias
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import create_session
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import eagerload
from sqlalchemy.orm import foreign
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relation
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from . import _fixtures
from .inheritance import _poly_fixtures
from .test_events import _RemoveListeners
from .test_options import PathTest as OptionsPathTest
from .test_query import QueryTest


class DeprecatedQueryTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    @classmethod
    def _expect_implicit_subquery(cls):
        return assertions.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs into "
            r"FROM clauses is deprecated; please call \.subquery\(\) on any "
            "Core select or ORM Query object in order to produce a "
            "subquery object."
        )

    def test_invalid_column(self):
        User = self.classes.User

        s = create_session()
        q = s.query(User.id)

        with testing.expect_deprecated(r"Query.add_column\(\) is deprecated"):
            q = q.add_column(User.name)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_via_textasfrom_select_from(self):
        User = self.classes.User
        s = create_session()

        with self._expect_implicit_subquery():
            eq_(
                s.query(User)
                .select_entity_from(
                    text("select * from users").columns(User.id, User.name)
                )
                .order_by(User.id)
                .all(),
                [User(id=7), User(id=8), User(id=9), User(id=10)],
            )

    def test_text_as_column(self):
        User = self.classes.User

        s = create_session()

        # TODO: this works as of "use rowproxy for ORM keyed tuple"
        # Ieb9085e9bcff564359095b754da9ae0af55679f0
        # but im not sure how this relates to things here
        q = s.query(User.id, text("users.name"))
        self.assert_compile(
            q, "SELECT users.id AS users_id, users.name FROM users"
        )
        eq_(q.all(), [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")])

        # same here, this was "passing string names to Query.columns"
        # deprecation message, that's gone here?
        assert_raises_message(
            sa.exc.ArgumentError,
            "Textual column expression 'name' should be explicitly",
            s.query,
            User.id,
            "name",
        )

    def test_query_as_scalar(self):
        User = self.classes.User

        s = Session()
        with assertions.expect_deprecated(
            r"The Query.as_scalar\(\) method is deprecated and will "
            "be removed in a future release."
        ):
            s.query(User).as_scalar()

    def test_select_entity_from_crit(self):
        User, users = self.classes.User, self.tables.users

        sel = users.select()
        sess = create_session()

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .filter(User.id.in_([7, 8]))
                .all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
            )

    def test_select_entity_from_select(self):
        User, users = self.classes.User, self.tables.users

        sess = create_session()
        with self._expect_implicit_subquery():
            self.assert_compile(
                sess.query(User.name).select_entity_from(
                    users.select().where(users.c.id > 5)
                ),
                "SELECT anon_1.name AS anon_1_name FROM "
                "(SELECT users.id AS id, users.name AS name FROM users "
                "WHERE users.id > :id_1) AS anon_1",
            )

    def test_select_entity_from_q_statement(self):
        User = self.classes.User

        sess = create_session()

        q = sess.query(User)
        with self._expect_implicit_subquery():
            q = sess.query(User).select_entity_from(q.statement)
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT users.id AS id, users.name AS name FROM "
            "users) AS anon_1 WHERE anon_1.name = :name_1",
        )

    def test_select_from_q_statement_no_aliasing(self):
        User = self.classes.User
        sess = create_session()

        q = sess.query(User)
        with self._expect_implicit_subquery():
            q = sess.query(User).select_from(q.statement)
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users, (SELECT users.id AS id, users.name AS name FROM "
            "users) AS anon_1 WHERE users.name = :name_1",
        )

    def test_from_alias_three(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(
                use_labels=True, order_by=[text("ulist.id"), addresses.c.id]
            )
        )
        sess = create_session()

        # better way.  use select_entity_from()
        def go():
            with self._expect_implicit_subquery():
                result = (
                    sess.query(User)
                    .select_entity_from(query)
                    .options(contains_eager("addresses"))
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_from_alias_four(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        sess = create_session()

        # same thing, but alias addresses, so that the adapter
        # generated by select_entity_from() is wrapped within
        # the adapter created by contains_eager()
        adalias = addresses.alias()
        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(adalias)
            .select(use_labels=True, order_by=[text("ulist.id"), adalias.c.id])
        )

        def go():
            with self._expect_implicit_subquery():
                result = (
                    sess.query(User)
                    .select_entity_from(query)
                    .options(contains_eager("addresses", alias=adalias))
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_select(self):
        users = self.tables.users

        sess = create_session()

        with self._expect_implicit_subquery():
            self.assert_compile(
                sess.query(users)
                .select_entity_from(users.select())
                .with_labels()
                .statement,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users, "
                "(SELECT users.id AS id, users.name AS name FROM users) "
                "AS anon_1",
            )

    def test_join(self):
        users, Address, User = (
            self.tables.users,
            self.classes.Address,
            self.classes.User,
        )

        # mapper(User, users, properties={"addresses": relationship(Address)})
        # mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        with self._expect_implicit_subquery():
            result = (
                sess.query(User)
                .select_entity_from(sel)
                .join("addresses")
                .add_entity(Address)
                .order_by(User.id)
                .order_by(Address.id)
                .all()
            )

        eq_(
            result,
            [
                (
                    User(name="jack", id=7),
                    Address(user_id=7, email_address="jack@bean.com", id=1),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@wood.com", id=2),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@bettyboop.com", id=3),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@lala.com", id=4),
                ),
            ],
        )

        adalias = aliased(Address)
        with self._expect_implicit_subquery():
            result = (
                sess.query(User)
                .select_entity_from(sel)
                .join(adalias, "addresses")
                .add_entity(adalias)
                .order_by(User.id)
                .order_by(adalias.id)
                .all()
            )
        eq_(
            result,
            [
                (
                    User(name="jack", id=7),
                    Address(user_id=7, email_address="jack@bean.com", id=1),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@wood.com", id=2),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@bettyboop.com", id=3),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@lala.com", id=4),
                ),
            ],
        )

    def test_more_joins(self):
        (users, Keyword, User) = (
            self.tables.users,
            self.classes.Keyword,
            self.classes.User,
        )

        sess = create_session()
        sel = users.select(users.c.id.in_([7, 8]))

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .join("orders", "items", "keywords")
                .filter(Keyword.name.in_(["red", "big", "round"]))
                .all(),
                [User(name="jack", id=7)],
            )

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .join("orders", "items", "keywords", aliased=True)
                .filter(Keyword.name.in_(["red", "big", "round"]))
                .all(),
                [User(name="jack", id=7)],
            )

    def test_join_no_order_by(self):
        User, users = self.classes.User, self.tables.users

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User).select_entity_from(sel).all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
            )

    def test_replace_with_eager(self):
        users, Address, User = (
            self.tables.users,
            self.classes.Address,
            self.classes.User,
        )

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .order_by(User.id)
                    .all(),
                    [
                        User(id=7, addresses=[Address(id=1)]),
                        User(
                            id=8,
                            addresses=[
                                Address(id=2),
                                Address(id=3),
                                Address(id=4),
                            ],
                        ),
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .filter(User.id == 8)
                    .order_by(User.id)
                    .all(),
                    [
                        User(
                            id=8,
                            addresses=[
                                Address(id=2),
                                Address(id=3),
                                Address(id=4),
                            ],
                        )
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .order_by(User.id)[1],
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                    ),
                )

        self.assert_sql_count(testing.db, go, 1)

    def test_onclause_conditional_adaption(self):
        Item, Order, orders, order_items, User = (
            self.classes.Item,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.User,
        )

        sess = Session()

        oalias = orders.select()

        with self._expect_implicit_subquery():
            self.assert_compile(
                sess.query(User)
                .join(oalias, User.orders)
                .join(
                    Item,
                    and_(
                        Order.id == order_items.c.order_id,
                        order_items.c.item_id == Item.id,
                    ),
                    from_joinpoint=True,
                ),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN "
                "(SELECT orders.id AS id, orders.user_id AS user_id, "
                "orders.address_id AS address_id, orders.description "
                "AS description, orders.isopen AS isopen FROM orders) "
                "AS anon_1 ON users.id = anon_1.user_id JOIN items "
                "ON anon_1.id = order_items.order_id "
                "AND order_items.item_id = items.id",
                use_default_dialect=True,
            )


class DeprecatedInhTest(_poly_fixtures._Polymorphic):
    def test_with_polymorphic(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer

        with DeprecatedQueryTest._expect_implicit_subquery():
            p_poly = with_polymorphic(Person, [Engineer], select([Person]))

        is_true(
            sa.inspect(p_poly).selectable.compare(select([Person]).subquery())
        )

    def test_multiple_adaption(self):
        """test that multiple filter() adapters get chained together "
        and work correctly within a multiple-entry join()."""

        Company = _poly_fixtures.Company
        Machine = _poly_fixtures.Machine
        Engineer = _poly_fixtures.Engineer

        people = self.tables.people
        engineers = self.tables.engineers
        machines = self.tables.machines

        sess = create_session()

        mach_alias = machines.select()
        with DeprecatedQueryTest._expect_implicit_subquery():
            self.assert_compile(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(mach_alias, Engineer.machines, from_joinpoint=True)
                .filter(Engineer.name == "dilbert")
                .filter(Machine.name == "foo"),
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name "
                "FROM companies JOIN (people "
                "JOIN engineers ON people.person_id = "
                "engineers.person_id) ON companies.company_id = "
                "people.company_id JOIN "
                "(SELECT machines.machine_id AS machine_id, "
                "machines.name AS name, "
                "machines.engineer_id AS engineer_id "
                "FROM machines) AS anon_1 "
                "ON engineers.person_id = anon_1.engineer_id "
                "WHERE people.name = :name_1 AND anon_1.name = :name_2",
                use_default_dialect=True,
            )


class DeprecatedMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_deferred_scalar_loader_name_change(self):
        class Foo(object):
            pass

        def myloader(*arg, **kw):
            pass

        instrumentation.register_class(Foo)
        manager = instrumentation.manager_of_class(Foo)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            manager.deferred_scalar_loader = myloader

        is_(manager.expired_attribute_loader, myloader)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            is_(manager.deferred_scalar_loader, myloader)

    def test_polymorphic_union_w_select(self):
        users, addresses = self.tables.users, self.tables.addresses

        with DeprecatedQueryTest._expect_implicit_subquery():
            dep = polymorphic_union(
                {"u": users.select(), "a": addresses.select()},
                "type",
                "bcjoin",
            )

        subq_version = polymorphic_union(
            {
                "u": users.select().subquery(),
                "a": addresses.select().subquery(),
            },
            "type",
            "bcjoin",
        )
        is_true(dep.compare(subq_version))

    def test_comparable_column(self):
        users, User = self.tables.users, self.classes.User

        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()) == func.lower(
                    other
                )

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op("&=")(other)

        mapper(
            User,
            users,
            properties={
                "name": sa.orm.column_property(
                    users.c.name, comparator_factory=MyComparator
                )
            },
        )

        assert_raises_message(
            AttributeError,
            "Neither 'InstrumentedAttribute' object nor "
            "'MyComparator' object associated with User.name has "
            "an attribute 'nonexistent'",
            getattr,
            User.name,
            "nonexistent",
        )

        eq_(
            str(
                (User.name == "ed").compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "lower(users.name) = lower(:lower_1)",
        )
        eq_(
            str(
                (User.name.intersects("ed")).compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "users.name &= :name_1",
        )

    def test_add_property(self):
        users = self.tables.users

        assert_col = []

        class User(fixtures.ComparableEntity):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

        m = mapper(User, users)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))

        sess = create_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(u.name, "jack")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(1, go)


class DeprecatedOptionAllTest(OptionsPathTest, _fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def _mapper_fixture_one(self):
        users, User, addresses, Address, orders, Order = (
            self.tables.users,
            self.classes.User,
            self.tables.addresses,
            self.classes.Address,
            self.tables.orders,
            self.classes.Order,
        )
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=self.tables.order_items)
            },
        )
        mapper(
            Keyword,
            keywords,
            properties={
                "keywords": column_property(keywords.c.name + "some keyword")
            },
        )
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, secondary=item_keywords)
            ),
        )

    def _assert_eager_with_entity_exception(
        self, entity_list, options, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            create_session()
            .query(*entity_list)
            .options(*options)
            ._compile_context,
        )

    def test_defer_addtl_attrs(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                )
            },
        )

        sess = create_session()

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.defer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(defer("addresses", "email_address"))

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.undefer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(undefer("addresses", "email_address"))


class InstrumentationTest(fixtures.ORMTest):
    def test_dict_subclass4(self):
        # tests #2654
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class MyDict(collections.MappedCollection):
                def __init__(self):
                    super(MyDict, self).__init__(lambda value: "k%d" % value)

                @collection.converter
                def _convert(self, dictlike):
                    for key, value in dictlike.items():
                        yield value + 5

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, "attr", uselist=True, typecallable=MyDict, useobject=True
        )

        f = Foo()
        f.attr = {"k1": 1, "k2": 2}

        eq_(f.attr, {"k7": 7, "k6": 6})

    def test_name_setup(self):
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Base(object):
                @collection.iterator
                def base_iterate(self, x):
                    return "base_iterate"

                @collection.appender
                def base_append(self, x):
                    return "base_append"

                @collection.converter
                def base_convert(self, x):
                    return "base_convert"

                @collection.remover
                def base_remove(self, x):
                    return "base_remove"

        from sqlalchemy.orm.collections import _instrument_class

        _instrument_class(Base)

        eq_(Base._sa_remover(Base(), 5), "base_remove")
        eq_(Base._sa_appender(Base(), 5), "base_append")
        eq_(Base._sa_iterator(Base(), 5), "base_iterate")
        eq_(Base._sa_converter(Base(), 5), "base_convert")

        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Sub(Base):
                @collection.converter
                def base_convert(self, x):
                    return "sub_convert"

                @collection.remover
                def sub_remove(self, x):
                    return "sub_remove"

        _instrument_class(Sub)

        eq_(Sub._sa_appender(Sub(), 5), "base_append")
        eq_(Sub._sa_remover(Sub(), 5), "sub_remove")
        eq_(Sub._sa_iterator(Sub(), 5), "base_iterate")
        eq_(Sub._sa_converter(Sub(), 5), "sub_convert")


class NonPrimaryRelationshipLoaderTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_selectload(self):
        """tests lazy loading with two relationships simultaneously,
        from the same table, using aliases.  """

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)

        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(Address, lazy=True),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="select",
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="select",
                ),
            ),
        )

        self._run_double_test(10)

    def test_joinedload(self):
        """Eager loading with two relationships simultaneously,
            from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="joined", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=closedorders.c.id,
                ),
            ),
        )
        self._run_double_test(1)

    def test_selectin(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def test_subqueryload(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def _run_double_test(self, count):
        User, Address, Order, Item = self.classes(
            "User", "Address", "Order", "Item"
        )
        q = create_session().query(User).order_by(User.id)

        def go():
            eq_(
                [
                    User(
                        id=7,
                        addresses=[Address(id=1)],
                        open_orders=[Order(id=3)],
                        closed_orders=[Order(id=1), Order(id=5)],
                    ),
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                        open_orders=[],
                        closed_orders=[],
                    ),
                    User(
                        id=9,
                        addresses=[Address(id=5)],
                        open_orders=[Order(id=4)],
                        closed_orders=[Order(id=2)],
                    ),
                    User(id=10),
                ],
                q.all(),
            )

        self.assert_sql_count(testing.db, go, count)

        sess = create_session()
        user = sess.query(User).get(7)

        closed_mapper = User.closed_orders.entity
        open_mapper = User.open_orders.entity
        eq_(
            [Order(id=1), Order(id=5)],
            create_session()
            .query(closed_mapper)
            .with_parent(user, property="closed_orders")
            .all(),
        )
        eq_(
            [Order(id=3)],
            create_session()
            .query(open_mapper)
            .with_parent(user, property="open_orders")
            .all(),
        )


class ViewonlyFlagWarningTest(fixtures.MappedTest):
    """test for #4993.

    In 1.4, this moves to test/orm/test_cascade, deprecation warnings
    become errors, will then be for #4994.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )
        Table(
            "orders",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("description", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    @testing.combinations(
        ("passive_deletes", True),
        ("passive_updates", False),
        ("enable_typechecks", False),
        ("active_history", True),
        ("cascade_backrefs", False),
    )
    def test_viewonly_warning(self, flag, value):
        Order = self.classes.Order

        with testing.expect_warnings(
            r"Setting %s on relationship\(\) while also setting "
            "viewonly=True does not make sense" % flag
        ):
            kw = {
                "viewonly": True,
                "primaryjoin": self.tables.users.c.id
                == foreign(self.tables.orders.c.user_id),
            }
            kw[flag] = value
            rel = relationship(Order, **kw)

            eq_(getattr(rel, flag), value)


class NonPrimaryMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_non_primary_identity_class(self):
        User = self.classes.User
        users, addresses = self.tables.users, self.tables.addresses

        class AddressUser(User):
            pass

        mapper(User, users, polymorphic_identity="user")
        m2 = mapper(
            AddressUser,
            addresses,
            inherits=User,
            polymorphic_identity="address",
            properties={"address_id": addresses.c.id},
        )
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m3 = mapper(AddressUser, addresses, non_primary=True)
        assert m3._identity_class is m2._identity_class
        eq_(
            m2.identity_key_from_instance(AddressUser()),
            m3.identity_key_from_instance(AddressUser()),
        )

    def test_illegal_non_primary(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            mapper(
                User,
                users,
                non_primary=True,
                properties={"addresses": relationship(Address)},
            )
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attempting to assign a new relationship 'addresses' "
            "to a non-primary mapper on class 'User'",
            configure_mappers,
        )

    def test_illegal_non_primary_2(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                User,
                users,
                non_primary=True,
            )

    def test_illegal_non_primary_3(self):
        users, addresses = self.tables.users, self.tables.addresses

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, users)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                Sub,
                addresses,
                non_primary=True,
            )


class InstancesTest(QueryTest, AssertsCompiledSQL):
    @testing.fails(
        "ORM refactor not allowing this yet, "
        "we may just abandon this use case"
    )
    def test_from_alias_one(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(
                use_labels=True, order_by=[text("ulist.id"), addresses.c.id]
            )
        )
        sess = create_session()
        q = sess.query(User)

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                "Retreiving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_alias("ulist"), contains_eager("addresses")
                    ).instances(query.execute())
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_from_alias_two_old_way(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(
                use_labels=True, order_by=[text("ulist.id"), addresses.c.id]
            )
        )
        sess = create_session()
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                "The AliasOption is not necessary for entities to be "
                "matched up to a query"
            ):
                result = (
                    q.options(
                        contains_alias("ulist"), contains_eager("addresses")
                    )
                    .from_statement(query)
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        sess = create_session()

        selectquery = users.outerjoin(addresses).select(
            users.c.id < 10,
            use_labels=True,
            order_by=[users.c.id, addresses.c.id],
        )
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(contains_eager("addresses")).instances(
                        selectquery.execute()
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(contains_eager(User.addresses)).instances(
                        selectquery.execute()
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_string_alias(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = create_session()
        q = sess.query(User)

        adalias = addresses.alias("adalias")
        selectquery = users.outerjoin(adalias).select(
            use_labels=True, order_by=[users.c.id, adalias.c.id]
        )

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"Passing a string name for the 'alias' argument to "
                r"'contains_eager\(\)` is deprecated",
                "Retreiving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_eager("addresses", alias="adalias")
                    ).instances(selectquery.execute())
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_aliased_instances(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = create_session()
        q = sess.query(User)

        adalias = addresses.alias("adalias")
        selectquery = users.outerjoin(adalias).select(
            use_labels=True, order_by=[users.c.id, adalias.c.id]
        )

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(
                        contains_eager("addresses", alias=adalias)
                    ).instances(selectquery.execute())
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_multi_string_alias(self):
        orders, items, users, order_items, User = (
            self.tables.orders,
            self.tables.items,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        sess = create_session()
        q = sess.query(User)

        oalias = orders.alias("o1")
        ialias = items.alias("i1")
        query = (
            users.outerjoin(oalias)
            .outerjoin(order_items)
            .outerjoin(ialias)
            .select(use_labels=True)
            .order_by(users.c.id, oalias.c.id, ialias.c.id)
        )

        # test using string alias with more than one level deep
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"Passing a string name for the 'alias' argument to "
                r"'contains_eager\(\)` is deprecated",
                "Retreiving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_eager("orders", alias="o1"),
                        contains_eager("orders.items", alias="i1"),
                    ).instances(query.execute())
                )
            assert self.static.user_order_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_multi_alias(self):
        orders, items, users, order_items, User = (
            self.tables.orders,
            self.tables.items,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        sess = create_session()
        q = sess.query(User)

        oalias = orders.alias("o1")
        ialias = items.alias("i1")
        query = (
            users.outerjoin(oalias)
            .outerjoin(order_items)
            .outerjoin(ialias)
            .select(use_labels=True)
            .order_by(users.c.id, oalias.c.id, ialias.c.id)
        )

        # test using Alias with more than one level deep

        # new way:
        # from sqlalchemy.orm.strategy_options import Load
        # opt = Load(User).contains_eager('orders', alias=oalias).
        #     contains_eager('items', alias=ialias)

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(
                        contains_eager("orders", alias=oalias),
                        contains_eager("orders.items", alias=ialias),
                    ).instances(query.execute())
                )
            assert self.static.user_order_result == result

        self.assert_sql_count(testing.db, go, 1)


class TestDeprecation20(fixtures.TestBase):
    def test_relation(self):
        with testing.expect_deprecated_20(".*relationship"):
            relation("foo")

    def test_eagerloading(self):
        with testing.expect_deprecated_20(".*joinedload"):
            eagerload("foo")


class DistinctOrderByImplicitTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_columns_augmented_roundtrip_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        q = (
            sess.query(User)
            .join("addresses")
            .distinct()
            .order_by(desc(Address.email_address))
        )
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_three(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .join(Address, true())
            .filter(User.name == "jack")
            .filter(User.id + Address.user_id > 0)
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
        )

        # even though columns are added, they aren't in the result
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
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

    def test_columns_augmented_sql_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
        )

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            self.assert_compile(
                q,
                "SELECT DISTINCT users.id AS users_id, users.name AS foo, "
                "addresses.id AS addresses_id, addresses.email_address AS "
                "addresses_email_address FROM users, addresses "
                "ORDER BY users.id, users.name, addresses.email_address",
            )


class SessionEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    def test_on_bulk_update_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = Session()
        canary = Mock()

        event.listen(sess, "after_bulk_update", canary.after_bulk_update)

        def legacy(ses, qry, ctx, res):
            canary.after_bulk_update_legacy(ses, qry, ctx, res)

        event.listen(sess, "after_bulk_update", legacy)

        mapper(User, users)

        with testing.expect_deprecated(
            'The argument signature for the "SessionEvents.after_bulk_update" '
            "event listener"
        ):
            sess.query(User).update({"name": "foo"})

        eq_(canary.after_bulk_update.call_count, 1)

        upd = canary.after_bulk_update.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(
            canary.after_bulk_update_legacy.mock_calls,
            [call(sess, upd.query, None, upd.result)],
        )

    def test_on_bulk_delete_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = Session()
        canary = Mock()

        event.listen(sess, "after_bulk_delete", canary.after_bulk_delete)

        def legacy(ses, qry, ctx, res):
            canary.after_bulk_delete_legacy(ses, qry, ctx, res)

        event.listen(sess, "after_bulk_delete", legacy)

        mapper(User, users)

        with testing.expect_deprecated(
            'The argument signature for the "SessionEvents.after_bulk_delete" '
            "event listener"
        ):
            sess.query(User).delete()

        eq_(canary.after_bulk_delete.call_count, 1)

        upd = canary.after_bulk_delete.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(
            canary.after_bulk_delete_legacy.mock_calls,
            [call(sess, upd.query, None, upd.result)],
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

    def test_value(self):
        User = self.classes.User

        sess = create_session()

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=7).value(User.id), 7)
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(
                sess.query(User.id, User.name).filter_by(id=7).value(User.id),
                7,
            )
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=0).value(User.id), None)

        sess.bind = testing.db
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query().value(sa.literal_column("1").label("x")), 1)

    def test_value_cancels_loader_opts(self):
        User = self.classes.User

        sess = create_session()

        q = (
            sess.query(User)
            .filter(User.name == "ed")
            .options(joinedload(User.addresses))
        )

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            q = q.value(func.count(literal_column("*")))


class MixedEntitiesTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_values(self):
        Address, users, User = (
            self.classes.Address,
            self.tables.users,
            self.classes.User,
        )

        sess = create_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.select_entity_from(sel).values(User.name)
        eq_(list(q2), [("jack",), ("ed",)])

        q = sess.query(User)

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(User.id).values(
                User.name, User.name + " " + cast(User.id, String(50))
            )
        eq_(
            list(q2),
            [
                ("jack", "jack 7"),
                ("ed", "ed 8"),
                ("fred", "fred 9"),
                ("chuck", "chuck 10"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join("addresses")
                .filter(User.name.like("%e%"))
                .order_by(User.id, Address.id)
                .values(User.name, Address.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@wood.com"),
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join("addresses")
                .filter(User.name.like("%e%"))
                .order_by(desc(Address.email_address))
                .slice(1, 3)
                .values(User.name, Address.email_address)
            )
        eq_(list(q2), [("ed", "ed@wood.com"), ("ed", "ed@lala.com")])

        adalias = aliased(Address)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join(adalias, "addresses")
                .filter(User.name.like("%e%"))
                .order_by(adalias.email_address)
                .values(User.name, adalias.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("ed", "ed@wood.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.values(func.count(User.name))
        assert next(q2) == (4,)

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(User.id == 8)
                .values(User.name, sel.c.name, User.name)
            )
        eq_(list(q2), [("ed", "ed", "ed")])

        # using User.xxx is alised against "sel", so this query returns nothing
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(User.id == 8)
                .filter(User.id > sel.c.id)
                .values(User.name, sel.c.name, User.name)
            )
        eq_(list(q2), [])

        # whereas this uses users.c.xxx, is not aliased and creates a new join
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(users.c.id == 8)
                .filter(users.c.id > sel.c.id)
                .values(users.c.name, sel.c.name, User.name)
            )
            eq_(list(q2), [("ed", "jack", "jack")])

    @testing.fails_on("mssql", "FIXME: unknown")
    def test_values_specific_order_by(self):
        users, User = self.tables.users, self.classes.User

        sess = create_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        u2 = aliased(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(u2.id > 1)
                .filter(or_(u2.id == User.id, u2.id != User.id))
                .order_by(User.id, sel.c.id, u2.id)
                .values(User.name, sel.c.name, u2.name)
            )
        eq_(
            list(q2),
            [
                ("jack", "jack", "jack"),
                ("jack", "jack", "ed"),
                ("jack", "jack", "fred"),
                ("jack", "jack", "chuck"),
                ("ed", "ed", "jack"),
                ("ed", "ed", "ed"),
                ("ed", "ed", "fred"),
                ("ed", "ed", "chuck"),
            ],
        )

    @testing.fails_on("mssql", "FIXME: unknown")
    @testing.fails_on(
        "oracle", "Oracle doesn't support boolean expressions as " "columns"
    )
    @testing.fails_on(
        "postgresql+pg8000",
        "pg8000 parses the SQL itself before passing on "
        "to PG, doesn't parse this",
    )
    @testing.fails_on("firebird", "unknown")
    def test_values_with_boolean_selects(self):
        """Tests a values clause that works with select boolean
        evaluations"""

        User = self.classes.User

        sess = create_session()

        q = sess.query(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.group_by(User.name.like("%j%"))
                .order_by(desc(User.name.like("%j%")))
                .values(
                    User.name.like("%j%"), func.count(User.name.like("%j%"))
                )
            )
        eq_(list(q2), [(True, 1), (False, 3)])

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(desc(User.name.like("%j%"))).values(
                User.name.like("%j%")
            )
        eq_(list(q2), [(True,), (False,), (False,), (False,)])
