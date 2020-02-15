import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy.ext.declarative import comparable_using
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import collections
from sqlalchemy.orm import column_property
from sqlalchemy.orm import comparable_property
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_alias
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import create_session
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import eagerload
from sqlalchemy.orm import foreign
from sqlalchemy.orm import identity
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import PropComparator
from sqlalchemy.orm import relation
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
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
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from . import _fixtures
from .inheritance import _poly_fixtures
from .test_options import PathTest as OptionsPathTest
from .test_query import QueryTest
from .test_transaction import _LocalFixture


class DeprecationWarningsTest(fixtures.DeclarativeMappedTest):
    run_setup_classes = "each"
    run_setup_mappers = "each"
    run_define_tables = "each"
    run_create_tables = None

    def test_session_weak_identity_map(self):
        with testing.expect_deprecated(
            ".*Session.weak_identity_map parameter as well as the"
        ):
            s = Session(weak_identity_map=True)

        is_(s._identity_cls, identity.WeakInstanceDict)

        with assertions.expect_deprecated(
            "The Session.weak_identity_map parameter as well as"
        ):
            s = Session(weak_identity_map=False)

            is_(s._identity_cls, identity.StrongInstanceDict)

        s = Session()
        is_(s._identity_cls, identity.WeakInstanceDict)

    def test_session_prune(self):
        s = Session()

        with assertions.expect_deprecated(
            r"The Session.prune\(\) method is deprecated along with "
            "Session.weak_identity_map"
        ):
            s.prune()

    def test_session_enable_transaction_accounting(self):
        with assertions.expect_deprecated(
            "the Session._enable_transaction_accounting parameter is "
            "deprecated"
        ):
            Session(_enable_transaction_accounting=False)

    def test_session_is_modified(self):
        class Foo(self.DeclarativeBasic):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)

        f1 = Foo()
        s = Session()
        with assertions.expect_deprecated(
            "The Session.is_modified.passive flag is deprecated"
        ):
            # this flag was for a long time documented as requiring
            # that it be set to True, so we've changed the default here
            # so that the warning emits
            s.is_modified(f1, passive=True)


class DeprecatedAccountingFlagsTest(_LocalFixture):
    def test_rollback_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name="ed")
        sess.add(u1)
        sess.commit()

        u1.name = "edwardo"
        sess.rollback()

        testing.db.execute(
            users.update(users.c.name == "ed").values(name="edward")
        )

        assert u1.name == "edwardo"
        sess.expire_all()
        assert u1.name == "edward"

    def test_commit_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name="ed")
        sess.add(u1)
        sess.commit()

        u1.name = "edwardo"
        sess.rollback()

        testing.db.execute(
            users.update(users.c.name == "ed").values(name="edward")
        )

        assert u1.name == "edwardo"
        sess.commit()

        assert testing.db.execute(select([users.c.name])).fetchall() == [
            ("edwardo",)
        ]
        assert u1.name == "edwardo"

        sess.delete(u1)
        sess.commit()

    def test_preflush_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = Session(
                _enable_transaction_accounting=False,
                autocommit=True,
                autoflush=False,
            )
        u1 = User(name="ed")
        sess.add(u1)
        sess.flush()

        sess.begin()
        u1.name = "edwardo"
        u2 = User(name="some other user")
        sess.add(u2)

        sess.rollback()

        sess.begin()
        assert testing.db.execute(select([users.c.name])).fetchall() == [
            ("ed",)
        ]


class DeprecatedSessionFeatureTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_fast_discard_race(self):
        # test issue #4068
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        with testing.expect_deprecated(".*identity map are deprecated"):
            sess = Session(weak_identity_map=False)

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        u1_state = u1._sa_instance_state
        sess.identity_map._dict.pop(u1_state.key)
        ref = u1_state.obj
        u1_state.obj = lambda: None

        u2 = sess.query(User).first()
        u1_state._cleanup(ref)

        u3 = sess.query(User).first()

        is_(u2, u3)

        u2_state = u2._sa_instance_state
        assert sess.identity_map.contains_state(u2._sa_instance_state)
        ref = u2_state.obj
        u2_state.obj = lambda: None
        u2_state._cleanup(ref)
        assert not sess.identity_map.contains_state(u2._sa_instance_state)

    def test_is_modified_passive_on(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        s = Session()
        u = User(name="fred", addresses=[Address(email_address="foo")])
        s.add(u)
        s.commit()

        u.id

        def go():
            assert not s.is_modified(u, passive=True)

        with testing.expect_deprecated(
            ".*Session.is_modified.passive flag is deprecated "
        ):
            self.assert_sql_count(testing.db, go, 0)

        u.name = "newname"

        def go():
            assert s.is_modified(u, passive=True)

        with testing.expect_deprecated(
            ".*Session.is_modified.passive flag is deprecated "
        ):
            self.assert_sql_count(testing.db, go, 0)


class StrongIdentityMapTest(_fixtures.FixtureTest):
    run_inserts = None

    def _strong_ident_fixture(self):
        with testing.expect_deprecated(
            ".*Session.weak_identity_map parameter as well as the"
        ):
            sess = create_session(weak_identity_map=False)

        def prune():
            with testing.expect_deprecated(".*Session.prune"):
                return sess.prune()

        return sess, prune

    def _event_fixture(self):
        session = create_session()

        @event.listens_for(session, "pending_to_persistent")
        @event.listens_for(session, "deleted_to_persistent")
        @event.listens_for(session, "detached_to_persistent")
        @event.listens_for(session, "loaded_as_persistent")
        def strong_ref_object(sess, instance):
            if "refs" not in sess.info:
                sess.info["refs"] = refs = set()
            else:
                refs = sess.info["refs"]

            refs.add(instance)

        @event.listens_for(session, "persistent_to_detached")
        @event.listens_for(session, "persistent_to_deleted")
        @event.listens_for(session, "persistent_to_transient")
        def deref_object(sess, instance):
            sess.info["refs"].discard(instance)

        def prune():
            if "refs" not in session.info:
                return 0

            sess_size = len(session.identity_map)
            session.info["refs"].clear()
            gc_collect()
            session.info["refs"] = set(
                s.obj() for s in session.identity_map.all_states()
            )
            return sess_size - len(session.identity_map)

        return session, prune

    def test_strong_ref_imap(self):
        self._test_strong_ref(self._strong_ident_fixture)

    def test_strong_ref_events(self):
        self._test_strong_ref(self._event_fixture)

    def _test_strong_ref(self, fixture):
        s, prune = fixture()

        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        # save user
        s.add(User(name="u1"))
        s.flush()
        user = s.query(User).one()
        user = None
        print(s.identity_map)
        gc_collect()
        assert len(s.identity_map) == 1

        user = s.query(User).one()
        assert not s.identity_map._modified
        user.name = "u2"
        assert s.identity_map._modified
        s.flush()
        eq_(users.select().execute().fetchall(), [(user.id, "u2")])

    def test_prune_imap(self):
        self._test_prune(self._strong_ident_fixture)

    def test_prune_events(self):
        self._test_prune(self._event_fixture)

    @testing.requires.cpython
    def _test_prune(self, fixture):
        s, prune = fixture()

        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        for o in [User(name="u%s" % x) for x in range(10)]:
            s.add(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        eq_(prune(), 0)
        s.flush()
        gc_collect()
        eq_(prune(), 9)
        # o is still in local scope here, so still present
        self.assert_(len(s.identity_map) == 1)

        id_ = o.id
        del o
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id_)
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        u.name = "squiznart"
        del u
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        s.add(User(name="x"))
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 0)
        s.flush()
        self.assert_(len(s.identity_map) == 1)
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id_)
        s.delete(u)
        del u
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 0)


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

    def test_cancel_order_by(self):
        users, User = self.tables.users, self.classes.User

        with testing.expect_deprecated(
            "The Mapper.order_by parameter is deprecated, and will be "
            "removed in a future release."
        ):
            mapper(User, users, order_by=users.c.name.desc())

        assert (
            "order by users.name desc"
            in str(create_session().query(User).statement).lower()
        )
        assert (
            "order by"
            not in str(
                create_session().query(User).order_by(None).statement
            ).lower()
        )
        assert (
            "order by users.name asc"
            in str(
                create_session()
                .query(User)
                .order_by(User.name.asc())
                .statement
            ).lower()
        )

        eq_(
            create_session().query(User).all(),
            [
                User(id=7, name="jack"),
                User(id=9, name="fred"),
                User(id=8, name="ed"),
                User(id=10, name="chuck"),
            ],
        )

        eq_(
            create_session().query(User).order_by(User.name).all(),
            [
                User(id=10, name="chuck"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=7, name="jack"),
            ],
        )

    def test_comparable(self):
        users = self.tables.users

        class extendedproperty(property):
            attribute = 123

            def method1(self):
                return "method1"

        from sqlalchemy.orm.properties import ColumnProperty

        class UCComparator(ColumnProperty.Comparator):
            __hash__ = None

            def method1(self):
                return "uccmethod1"

            def method2(self, other):
                return "method2"

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                if other is None:
                    return col is None
                else:
                    return sa.func.upper(col) == sa.func.upper(other)

        def map_(with_explicit_property):
            class User(object):
                @extendedproperty
                def uc_name(self):
                    if self.name is None:
                        return None
                    return self.name.upper()

            if with_explicit_property:
                args = (UCComparator, User.uc_name)
            else:
                args = (UCComparator,)

            with assertions.expect_deprecated(
                r"comparable_property\(\) is deprecated and will be "
                "removed in a future release."
            ):
                mapper(
                    User,
                    users,
                    properties=dict(uc_name=sa.orm.comparable_property(*args)),
                )
                return User

        for User in (map_(True), map_(False)):
            sess = create_session()
            sess.begin()
            q = sess.query(User)

            assert hasattr(User, "name")
            assert hasattr(User, "uc_name")

            eq_(User.uc_name.method1(), "method1")
            eq_(User.uc_name.method2("x"), "method2")

            assert_raises_message(
                AttributeError,
                "Neither 'extendedproperty' object nor 'UCComparator' "
                "object associated with User.uc_name has an attribute "
                "'nonexistent'",
                getattr,
                User.uc_name,
                "nonexistent",
            )

            # test compile
            assert not isinstance(User.uc_name == "jack", bool)
            u = q.filter(User.uc_name == "JACK").one()

            assert u.uc_name == "JACK"
            assert u not in sess.dirty

            u.name = "some user name"
            eq_(u.name, "some user name")
            assert u in sess.dirty
            eq_(u.uc_name, "SOME USER NAME")

            sess.flush()
            sess.expunge_all()

            q = sess.query(User)
            u2 = q.filter(User.name == "some user name").one()
            u3 = q.filter(User.uc_name == "SOME USER NAME").one()

            assert u2 is u3

            eq_(User.uc_name.attribute, 123)
            sess.rollback()

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

    def test_info(self):
        class MyComposite(object):
            pass

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            for constructor, args in [(comparable_property, "foo")]:
                obj = constructor(info={"x": "y"}, *args)
                eq_(obj.info, {"x": "y"})
                obj.info["q"] = "p"
                eq_(obj.info, {"x": "y", "q": "p"})

                obj = constructor(*args)
                eq_(obj.info, {})
                obj.info["q"] = "p"
                eq_(obj.info, {"q": "p"})

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

            def _uc_name(self):
                if self._name is None:
                    return None
                return self._name.upper()

            uc_name = property(_uc_name)
            uc_name2 = property(_uc_name)

        m = mapper(User, users)

        class UCComparator(PropComparator):
            __hash__ = None

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                if other is None:
                    return col is None
                else:
                    return func.upper(col) == func.upper(other)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))
        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            m.add_property("uc_name", comparable_property(UCComparator))
            m.add_property(
                "uc_name2", comparable_property(UCComparator, User.uc_name2)
            )

        sess = create_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(u.name, "jack")
            eq_(u.uc_name, "JACK")
            eq_(u.uc_name2, "JACK")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(1, go)

    def test_kwarg_accepted(self):
        class DummyComposite(object):
            def __init__(self, x, y):
                pass

        class MyFactory(PropComparator):
            pass

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            for args in ((comparable_property,),):
                fn = args[0]
                args = args[1:]
                fn(comparator_factory=MyFactory, *args)

    def test_merge_synonym_comparable(self):
        users = self.tables.users

        class User(object):
            class Comparator(PropComparator):
                pass

            def _getValue(self):
                return self._value

            def _setValue(self, value):
                setattr(self, "_value", value)

            value = property(_getValue, _setValue)

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            mapper(
                User,
                users,
                properties={
                    "uid": synonym("id"),
                    "foobar": comparable_property(User.Comparator, User.value),
                },
            )

        sess = create_session()
        u = User()
        u.name = "ed"
        sess.add(u)
        sess.flush()
        sess.expunge(u)
        sess.merge(u)


class DeprecatedDeclTest(fixtures.TestBase):
    @testing.provide_metadata
    def test_comparable_using(self):
        class NameComparator(sa.orm.PropComparator):
            @property
            def upperself(self):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                return sa.func.upper(col)

            def operate(self, op, other, **kw):
                return op(self.upperself, other, **kw)

        Base = declarative_base(metadata=self.metadata)

        with testing.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):

            class User(Base, fixtures.ComparableEntity):

                __tablename__ = "users"
                id = Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                )
                name = Column("name", String(50))

                @comparable_using(NameComparator)
                @property
                def uc_name(self):
                    return self.name is not None and self.name.upper() or None

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name="someuser")
        eq_(u1.name, "someuser", u1.name)
        eq_(u1.uc_name, "SOMEUSER", u1.uc_name)
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name == "SOMEUSER").one()
        eq_(rt, u1)
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name.startswith("SOMEUSE")).one()
        eq_(rt, u1)


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
            create_session().query(*entity_list).options,
            *options
        )

    def test_subqueryload_mapper_order_by(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)

        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, lazy="subquery", order_by=addresses.c.id
                    )
                },
                order_by=users.c.id.desc(),
            )

        sess = create_session()
        q = sess.query(User)

        result = q.limit(2).all()
        eq_(result, list(reversed(self.static.user_address_result[2:4])))

    def test_selectinload_mapper_order_by(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)
        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, lazy="selectin", order_by=addresses.c.id
                    )
                },
                order_by=users.c.id.desc(),
            )

        sess = create_session()
        q = sess.query(User)

        result = q.limit(2).all()
        eq_(result, list(reversed(self.static.user_address_result[2:4])))

    def test_join_mapper_order_by(self):
        """test that mapper-level order_by is adapted to a selectable."""

        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(User, users, order_by=users.c.id)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        with DeprecatedQueryTest._expect_implicit_subquery():
            eq_(
                sess.query(User).select_entity_from(sel).all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
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

    def test_link_event(self):
        canary = []

        with testing.expect_deprecated(
            r"The collection.linker\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Collection(list):
                @collection.linker
                def _on_link(self, obj):
                    canary.append(obj)

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, "attr", uselist=True, typecallable=Collection, useobject=True
        )

        f1 = Foo()
        f1.attr.append(3)

        eq_(canary, [f1.attr._sa_adapter])
        adapter_1 = f1.attr._sa_adapter

        l2 = Collection()
        f1.attr = l2
        eq_(canary, [adapter_1, f1.attr._sa_adapter, None])


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
