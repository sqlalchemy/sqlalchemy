import copy
import pickle
import sys

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import collections
from sqlalchemy.orm import defer
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import raiseload
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import state as sa_state
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.collections import column_keyed_dict
from sqlalchemy.orm.path_registry import PathRegistry
from sqlalchemy.orm.path_registry import PathToken
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_not_none
from sqlalchemy.testing import unpickle_in_subprocess
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.pickleable import Address
from sqlalchemy.testing.pickleable import AddressWMixin
from sqlalchemy.testing.pickleable import Child1
from sqlalchemy.testing.pickleable import Child2
from sqlalchemy.testing.pickleable import Dingaling
from sqlalchemy.testing.pickleable import EmailUser
from sqlalchemy.testing.pickleable import Mixin
from sqlalchemy.testing.pickleable import Order
from sqlalchemy.testing.pickleable import Parent
from sqlalchemy.testing.pickleable import Screen
from sqlalchemy.testing.pickleable import User
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import picklers
from test.orm import _fixtures
from .inheritance._poly_fixtures import _Polymorphic
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Engineer
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person


def no_ed_foo(cls):
    return cls.email_address != "ed@foo.com"


class PickleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "orders",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("description", String(30)),
            Column("isopen", Integer),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
            test_needs_acid=True,
            test_needs_fk=True,
        )

    def _option_test_fixture(self):
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingaling": relationship(Dingaling)},
        )
        self.mapper_registry.map_imperatively(Dingaling, dingalings)
        sess = fixture_session()
        u1 = User(name="ed")
        u1.addresses.append(Address(email_address="ed@bar.com"))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        return sess, User, Address, Dingaling

    def test_transient(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u1 = User(name="ed")
        u1.addresses.append(Address(email_address="ed@bar.com"))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.add(u2)
        sess.flush()

        sess.expunge_all()

        eq_(u1, sess.get(User, u2.id))

    def test_no_mappers(self):
        users = self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        u1 = User(name="ed")
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        assert_raises_message(
            orm_exc.UnmappedInstanceError,
            "Cannot deserialize object of type "
            "<class 'sqlalchemy.testing.pickleable.User'> - no mapper()",
            pickle.loads,
            u1_pickled,
        )

    def test_no_instrumentation(self):
        users = self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        u1 = User(name="ed")
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        self.mapper_registry.map_imperatively(User, users)

        u1 = pickle.loads(u1_pickled)
        # this fails unless the InstanceState
        # compiles the mapper
        eq_(str(u1), "User(name='ed')")

    def test_class_deferred_cols(self):
        addresses, users = (self.tables.addresses, self.tables.users)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name": sa.orm.deferred(users.c.name),
                "addresses": relationship(Address, backref="user"),
            },
        )
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "email_address": sa.orm.deferred(addresses.c.email_address)
            },
        )
        with fixture_session(expire_on_commit=False) as sess:
            u1 = User(name="ed")
            u1.addresses.append(Address(email_address="ed@bar.com"))
            sess.add(u1)
            sess.commit()

        with fixture_session() as sess:
            u1 = sess.get(User, u1.id)
            assert "name" not in u1.__dict__
            assert "addresses" not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        with fixture_session() as sess2:
            sess2.add(u2)
            eq_(u2.name, "ed")
            eq_(
                u2,
                User(
                    name="ed", addresses=[Address(email_address="ed@bar.com")]
                ),
            )

        u2 = pickle.loads(pickle.dumps(u1))
        with fixture_session() as sess2:
            u2 = sess2.merge(u2, load=False)
            eq_(u2.name, "ed")
            eq_(
                u2,
                User(
                    name="ed", addresses=[Address(email_address="ed@bar.com")]
                ),
            )

    def test_instance_lazy_relation_loaders(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="noload")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u1 = User(name="ed", addresses=[Address(email_address="ed@bar.com")])

        sess.add(u1)
        sess.commit()
        sess.close()

        u1 = sess.query(User).options(lazyload(User.addresses)).first()
        u2 = pickle.loads(pickle.dumps(u1))

        sess = fixture_session()
        sess.add(u2)
        assert u2.addresses

    def test_lazyload_extra_criteria_not_supported(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address)},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u1 = User(
            name="ed",
            addresses=[
                Address(email_address="ed@bar.com"),
                Address(email_address="ed@wood.com"),
            ],
        )

        sess.add(u1)
        sess.commit()
        sess.close()
        u1 = (
            sess.query(User)
            .options(
                lazyload(
                    User.addresses.and_(Address.email_address == "ed@bar.com")
                )
            )
            .first()
        )
        with testing.expect_warnings(
            r"Can't reliably serialize a lazyload\(\) option"
        ):
            u2 = pickle.loads(pickle.dumps(u1))

        eq_(len(u1.addresses), 1)

        sess = fixture_session()
        sess.add(u2)
        eq_(len(u2.addresses), 2)

    def test_invalidated_flag_pickle(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="noload")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        u1 = User()
        u1.addresses.append(Address())
        u2 = pickle.loads(pickle.dumps(u1))
        u2.addresses.append(Address())
        eq_(len(u2.addresses), 2)

    def test_invalidated_flag_deepcopy(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="noload")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        u1 = User()
        u1.addresses.append(Address())
        u2 = copy.deepcopy(u1)
        u2.addresses.append(Address())
        eq_(len(u2.addresses), 2)

    @testing.combinations(True, False, argnames="pickle_it")
    @testing.combinations(True, False, argnames="use_mixin")
    def test_loader_criteria(self, pickle_it, use_mixin):
        """test #8109"""

        users, addresses = (self.tables.users, self.tables.addresses)

        AddressCls = AddressWMixin if use_mixin else Address

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(AddressCls)},
        )

        self.mapper_registry.map_imperatively(AddressCls, addresses)

        with fixture_session(expire_on_commit=False) as sess:
            u1 = User(name="ed")
            u1.addresses = [
                AddressCls(email_address="ed@bar.com"),
                AddressCls(email_address="ed@foo.com"),
            ]
            sess.add(u1)
            sess.commit()

        with fixture_session(expire_on_commit=False) as sess:
            # note that non-lambda is not picklable right now as
            # SQL expressions usually can't be pickled.
            opt = with_loader_criteria(
                Mixin if use_mixin else Address,
                no_ed_foo,
                include_aliases=True,
            )

            u1 = sess.query(User).options(opt).first()

            if pickle_it:
                u1 = pickle.loads(pickle.dumps(u1))
                sess.close()
                sess.add(u1)

            eq_([ad.email_address for ad in u1.addresses], ["ed@bar.com"])

    def test_instance_deferred_cols(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        with fixture_session(expire_on_commit=False) as sess:
            u1 = User(name="ed")
            u1.addresses.append(Address(email_address="ed@bar.com"))
            sess.add(u1)
            sess.commit()

        with fixture_session(expire_on_commit=False) as sess:
            u1 = sess.get(
                User,
                u1.id,
                options=[
                    sa.orm.defer(User.name),
                    sa.orm.defaultload(User.addresses).defer(
                        Address.email_address
                    ),
                ],
            )
            assert "name" not in u1.__dict__
            assert "addresses" not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        with fixture_session() as sess2:
            sess2.add(u2)
            eq_(u2.name, "ed")
            assert "addresses" not in u2.__dict__
            ad = u2.addresses[0]
            assert "email_address" not in ad.__dict__
            eq_(ad.email_address, "ed@bar.com")
            eq_(
                u2,
                User(
                    name="ed", addresses=[Address(email_address="ed@bar.com")]
                ),
            )

        u2 = pickle.loads(pickle.dumps(u1))
        with fixture_session() as sess2:
            u2 = sess2.merge(u2, load=False)
            eq_(u2.name, "ed")
            assert "addresses" not in u2.__dict__
            ad = u2.addresses[0]

            # mapper options now transmit over merge(),
            # new as of 0.6, so email_address is deferred.
            assert "email_address" not in ad.__dict__

            eq_(ad.email_address, "ed@bar.com")
            eq_(
                u2,
                User(
                    name="ed", addresses=[Address(email_address="ed@bar.com")]
                ),
            )

    def test_pickle_protocols(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u1 = User(name="ed")
        u1.addresses.append(Address(email_address="ed@bar.com"))
        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()
        u1.addresses

        for loads, dumps in picklers():
            u2 = loads(dumps(u1))
            eq_(u1, u2)

    def test_state_info_pickle(self):
        users = self.tables.users
        self.mapper_registry.map_imperatively(User, users)

        u1 = User(id=1, name="ed")

        sa.inspect(u1).info["some_key"] = "value"

        state_dict = sa.inspect(u1).__getstate__()

        state = sa_state.InstanceState.__new__(sa_state.InstanceState)
        state.__setstate__(state_dict)

        u2 = state.obj()
        eq_(sa.inspect(u2).info["some_key"], "value")

    @testing.combinations(
        lambda User: sa.orm.joinedload(User.addresses),
        lambda User: sa.orm.defer(User.name),
        lambda Address: sa.orm.joinedload(User.addresses).joinedload(
            Address.dingaling
        ),
        lambda: sa.orm.joinedload(User.addresses).raiseload("*"),
        lambda: sa.orm.raiseload("*"),
    )
    def test_unbound_options(self, test_case):
        sess, User, Address, Dingaling = self._option_test_fixture()

        opt = testing.resolve_lambda(test_case, User=User, Address=Address)
        opt2 = pickle.loads(pickle.dumps(opt))
        eq_(opt.path, opt2.path)

        u1 = sess.query(User).options(opt).first()
        pickle.loads(pickle.dumps(u1))

    @testing.combinations(
        lambda User: sa.orm.Load(User).joinedload(User.addresses),
        lambda User: sa.orm.Load(User)
        .joinedload(User.addresses)
        .raiseload("*"),
        lambda User: sa.orm.Load(User).defer(User.name),
        lambda User, Address: sa.orm.Load(User)
        .joinedload(User.addresses)
        .joinedload(Address.dingaling),
        lambda User, Address: sa.orm.Load(User)
        .joinedload(User.addresses, innerjoin=True)
        .joinedload(Address.dingaling),
    )
    def test_bound_options(self, test_case):
        sess, User, Address, Dingaling = self._option_test_fixture()

        opt = testing.resolve_lambda(test_case, User=User, Address=Address)

        opt2 = pickle.loads(pickle.dumps(opt))
        eq_(opt.path, opt2.path)
        for v1, v2 in zip(opt.context, opt2.context):
            eq_(v1.local_opts, v2.local_opts)

        u1 = sess.query(User).options(opt).first()
        pickle.loads(pickle.dumps(u1))

    def test_collection_setstate(self):
        """test a particular cycle that requires CollectionAdapter
        to not rely upon InstanceState to deserialize."""

        m = MetaData()
        c1 = Table(
            "c1",
            m,
            Column("parent_id", String, ForeignKey("p.id"), primary_key=True),
        )
        c2 = Table(
            "c2",
            m,
            Column("parent_id", String, ForeignKey("p.id"), primary_key=True),
        )
        p = Table("p", m, Column("id", String, primary_key=True))

        self.mapper_registry.map_imperatively(
            Parent,
            p,
            properties={
                "children1": relationship(Child1),
                "children2": relationship(Child2),
            },
        )
        self.mapper_registry.map_imperatively(Child1, c1)
        self.mapper_registry.map_imperatively(Child2, c2)

        obj = Parent()
        screen1 = Screen(obj)
        screen1.errors = [obj.children1, obj.children2]
        screen2 = Screen(Child2(), screen1)
        pickle.loads(pickle.dumps(screen2))

    def test_exceptions(self):
        class Foo:
            pass

        users = self.tables.users
        self.mapper_registry.map_imperatively(User, users)

        for sa_exc in (
            orm_exc.UnmappedInstanceError(Foo()),
            orm_exc.UnmappedClassError(Foo),
            orm_exc.ObjectDeletedError(attributes.instance_state(User())),
        ):
            for loads, dumps in picklers():
                repickled = loads(dumps(sa_exc))
                eq_(repickled.args[0], sa_exc.args[0])

    def test_attribute_mapped_collection(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    collection_class=attribute_keyed_dict("email_address"),
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        u1 = User()
        u1.addresses = {"email1": Address(email_address="email1")}
        for loads, dumps in picklers():
            repickled = loads(dumps(u1))
            eq_(u1.addresses, repickled.addresses)
            eq_(repickled.addresses["email1"], Address(email_address="email1"))

            is_not_none(collections.collection_adapter(repickled.addresses))

    def test_column_mapped_collection(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    collection_class=column_keyed_dict(
                        addresses.c.email_address
                    ),
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        u1 = User()
        u1.addresses = {
            "email1": Address(email_address="email1"),
            "email2": Address(email_address="email2"),
        }
        for loads, dumps in picklers():
            repickled = loads(dumps(u1))
            eq_(u1.addresses, repickled.addresses)
            eq_(repickled.addresses["email1"], Address(email_address="email1"))

            is_not_none(collections.collection_adapter(repickled.addresses))

    def test_composite_column_mapped_collection(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    collection_class=column_keyed_dict(
                        [addresses.c.id, addresses.c.email_address]
                    ),
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        u1 = User()
        u1.addresses = {
            (1, "email1"): Address(id=1, email_address="email1"),
            (2, "email2"): Address(id=2, email_address="email2"),
        }
        for loads, dumps in picklers():
            repickled = loads(dumps(u1))
            eq_(u1.addresses, repickled.addresses)
            eq_(
                repickled.addresses[(1, "email1")],
                Address(id=1, email_address="email1"),
            )
            is_not_none(collections.collection_adapter(repickled.addresses))

    def test_bulk_save_objects_defaults_pickle(self):
        "Test for #11332"
        users = self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        pes = [User(name=f"foo{i}") for i in range(3)]
        s = fixture_session()
        s.bulk_save_objects(pes, return_defaults=True)
        state = pickle.dumps(pes)
        pickle.loads(state)


def _token_pickle_tables(metadata):
    """tables for :class:`.TokenPathPickleTest`."""

    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
        Column("name", String(30), nullable=False),
    )
    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
        Column("user_id", None, ForeignKey("users.id")),
        Column("email_address", String(50), nullable=False),
    )
    return users, addresses


def _token_pickle_mappers(reg, users, addresses):
    """mapping for :class:`.TokenPathPickleTest`."""

    reg.map_imperatively(
        User, users, properties={"addresses": relationship(Address)}
    )
    reg.map_imperatively(Address, addresses)


def _unpickle_token_path_main():
    """entry point for the interpreter spawned by
    :meth:`.TokenPathPickleTest.test_unpickle_other_process`.

    establishes the same mapping in a process which has not run any query,
    then unpickles the instance named in ``sys.argv[1]``.

    """

    _token_pickle_mappers(registry(), *_token_pickle_tables(MetaData()))

    with open(sys.argv[1], "rb") as file_:
        print(pickle.load(file_).name)


class TokenPathPickleTest(fixtures.MappedTest):
    """test #13493.

    loader paths may include wildcard / default tokens such as
    ``"column:*"``, which ride along with a pickled ORM instance and are
    reconstructed by :meth:`.PathRegistry.deserialize`.  As tokens are
    recognized there by their presence in ``PathToken._intern``, that
    collection must be fully populated up front, in every process,
    including one which has not yet run any query.

    Previously it was populated lazily, so unpickling in a fresh
    interpreter failed; this is routinely hit under the ``spawn`` and
    ``forkserver`` multiprocessing start methods, the latter of which
    became the POSIX default in Python 3.14.

    """

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        _token_pickle_tables(metadata)

    @classmethod
    def setup_mappers(cls):
        _token_pickle_mappers(
            cls.mapper_registry, cls.tables.users, cls.tables.addresses
        )

    @classmethod
    def insert_data(cls, connection):
        with Session(connection) as sess:
            sess.add(
                User(
                    name="ed",
                    addresses=[Address(email_address="ed@bar.com")],
                )
            )
            sess.commit()

    def test_intern_is_fully_populated(self):
        """the complete set of tokens is present at import time.

        this is the actual fix for #13493; the ``load_only()`` /
        ``raiseload("*")`` round trips below depend on it.

        """

        eq_(
            set(PathToken._intern),
            {
                "column:*",
                "column:_sa_default",
                "relationship:*",
                "relationship:_sa_default",
            },
        )

    def test_intern_does_not_grow(self):
        """no loader option adds to ``PathToken._intern``.

        the collection is process-global and never pruned, so it must not
        accept new entries at runtime, in particular not from paths
        arriving via deserialization.

        """

        before = dict(PathToken._intern)

        with fixture_session() as sess:
            for opt in (
                load_only(User.name),
                load_only(User.name, raiseload=True),
                defer(User.name),
                undefer(User.name),
                raiseload("*"),
                lazyload("*"),
                Load(User).defer("*"),
                Load(User).raiseload("*"),
                selectinload(User.addresses).load_only(Address.email_address),
                selectinload(User.addresses).raiseload("*"),
            ):
                u1 = sess.scalars(select(User).options(opt)).one()
                pickle.loads(pickle.dumps(u1))
                sess.expunge_all()

        eq_(PathToken._intern, before)

    def test_unknown_token_rejected(self):
        """a string that is not one of the fixed tokens can't create one."""

        with expect_raises_message(
            sa.exc.ArgumentError, "invalid token: column:not_a_token:\\*"
        ):
            inspect(User)._path_registry.token("column:not_a_token:*")

    @testing.combinations("column", "relationship", argnames="wildcard_key")
    @testing.combinations("*", "_sa_default", argnames="token_type")
    @testing.combinations(True, False, argnames="from_root")
    def test_deserialize_token_path(self, wildcard_key, token_type, from_root):
        """every token round trips through serialize() / deserialize()."""

        token = f"{wildcard_key}:{token_type}"

        parent = (
            PathRegistry.root if from_root else inspect(User)._path_registry
        )
        path = parent.token(token)

        eq_(PathRegistry.deserialize(path.serialize()).path, path.path)

    def test_pickle_load_only(self):
        """load_only() propagates a ``"column:*"`` token within
        InstanceState.load_options."""

        with fixture_session() as sess:
            u1 = sess.scalars(select(User).options(load_only(User.name))).one()
            sess.expunge_all()

        # prior to the #13493 fix, in a fresh interpreter this raised
        # KeyError: 'column:*'; see
        # test_unpickle_other_process() for that condition
        u2 = pickle.loads(pickle.dumps(u1))

        eq_(
            [
                elem.path.path
                for opt in u2._sa_instance_state.load_options
                for elem in opt.context
            ],
            [
                (inspect(User), inspect(User).attrs.name),
                (inspect(User), PathToken._intern["column:*"]),
            ],
        )

    def test_pickle_wildcard_raiseload(self):
        """raiseload("*") leaves a _LoadLazyAttribute in
        InstanceState.callables whose loader option has a root level
        token path."""

        with fixture_session() as sess:
            u1 = sess.scalars(select(User).options(raiseload("*"))).one()
            sess.expunge_all()

        # prior to the #13493 fix, in a fresh interpreter this raised
        # IndexError: invalid argument for RootRegistry.__getitem__: None;
        # see test_unpickle_other_process() for that condition
        u2 = pickle.loads(pickle.dumps(u1))

        eq_(
            u2._sa_instance_state.callables["addresses"].loadopt.path.path,
            (PathToken._intern["relationship:_sa_default"],),
        )

    @testing.combinations("load_only", "raiseload", argnames="option")
    def test_unpickle_other_process(self, option):
        """unpickle in an interpreter that has not itself run a query.

        this is the condition reported in #13493; the tests above run in
        the process that produced the pickle, where the tokens would have
        been set up by the query itself.

        """

        if option == "load_only":
            opt = load_only(User.name)
        else:
            opt = raiseload("*")

        with fixture_session() as sess:
            u1 = sess.scalars(select(User).options(opt)).one()
            sess.expunge_all()

        eq_(
            unpickle_in_subprocess(
                u1,
                "from test.orm.test_pickled import "
                "_unpickle_token_path_main; _unpickle_token_path_main()",
            ),
            b"ed",
        )


class OptionsTest(_Polymorphic):
    def test_options_of_type(self):
        with_poly = with_polymorphic(Person, [Engineer, Manager], flat=True)
        for opt, serialized_path, serialized_of_type in [
            (
                sa.orm.joinedload(Company.employees.of_type(Engineer)),
                [(Company, "employees"), (Engineer, None)],
                Engineer,
            ),
            (
                sa.orm.joinedload(Company.employees.of_type(with_poly)),
                [(Company, "employees"), (Person, None)],
                None,
            ),
        ]:
            opt2 = pickle.loads(pickle.dumps(opt))
            eq_(opt.__getstate__()["path"], serialized_path)
            eq_(opt2.__getstate__()["path"], serialized_path)

            for v1, v2 in zip(opt.context, opt2.context):
                eq_(v1.__getstate__()["_of_type"], serialized_of_type)
                eq_(v2.__getstate__()["_of_type"], serialized_of_type)

    def test_load(self):
        s = fixture_session()

        with_poly = with_polymorphic(Person, [Engineer, Manager], flat=True)
        emp = (
            s.query(Company)
            .options(subqueryload(Company.employees.of_type(with_poly)))
            .first()
        )

        pickle.loads(pickle.dumps(emp))


class PolymorphicDeferredTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("type", String(30)),
        )
        Table(
            "email_users",
            metadata,
            Column("id", Integer, ForeignKey("users.id"), primary_key=True),
            Column("email_address", String(30)),
        )

    def test_polymorphic_deferred(self):
        email_users, users = (self.tables.email_users, self.tables.users)

        self.mapper_registry.map_imperatively(
            User,
            users,
            polymorphic_identity="user",
            polymorphic_on=users.c.type,
        )
        self.mapper_registry.map_imperatively(
            EmailUser,
            email_users,
            inherits=User,
            polymorphic_identity="emailuser",
        )

        eu = EmailUser(name="user1", email_address="foo@bar.com")
        with fixture_session() as sess:
            sess.add(eu)
            sess.commit()

        with fixture_session() as sess:
            eu = sess.query(User).first()
            eu2 = pickle.loads(pickle.dumps(eu))
            sess2 = fixture_session()
            sess2.add(eu2)
            assert "email_address" not in eu2.__dict__
            eq_(eu2.email_address, "foo@bar.com")


class TupleLabelTest(_fixtures.FixtureTest):
    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        users, addresses, orders = (
            cls.tables.users,
            cls.tables.addresses,
            cls.tables.orders,
        )
        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", order_by=addresses.c.id
                ),
                # o2m, m2o
                "orders": relationship(
                    Order, backref="user", order_by=orders.c.id
                ),
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)
        cls.mapper_registry.map_imperatively(
            Order, orders, properties={"address": relationship(Address)}
        )  # m2o

    def test_tuple_labeling(self):
        sess = fixture_session()

        # test pickle + all the protocols !
        for pickled in False, -1, 0, 1, 2:
            for row in sess.query(User, Address).join(User.addresses).all():
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))

                eq_(list(row._fields), ["User", "Address"])
                eq_(row.User, row[0])
                eq_(row.Address, row[1])

            for row in sess.query(User.name, User.id.label("foobar")):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(list(row._fields), ["name", "foobar"])
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            for row in sess.query(User).with_entities(
                User.name, User.id.label("foobar")
            ):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(list(row._fields), ["name", "foobar"])
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            oalias = aliased(Order)
            for row in (
                sess.query(User, oalias)
                .join(User.orders.of_type(oalias))
                .all()
            ):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(list(row._fields), ["User"])
                eq_(row.User, row[0])

            oalias = aliased(Order, name="orders")
            for row in (
                sess.query(User, oalias).join(oalias, User.orders).all()
            ):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(list(row._fields), ["User", "orders"])
                eq_(row.User, row[0])
                eq_(row.orders, row[1])

            for row in sess.query(User.name + "hoho", User.name):
                eq_(list(row._fields), ["name"])
                eq_(row[0], row.name + "hoho")

            if pickled is not False:
                ret = sess.query(User, Address).join(User.addresses).all()
                pickle.loads(pickle.dumps(ret, pickled))


class CustomSetupTeardownTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )

    def test_rebuild_state(self):
        """not much of a 'test', but illustrate how to
        remove instance-level state before pickling.

        """

        users = self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        u1 = User()
        attributes.manager_of_class(User).teardown_instance(u1)
        assert not u1.__dict__
        u2 = pickle.loads(pickle.dumps(u1))
        attributes.manager_of_class(User).setup_instance(u2)
        assert attributes.instance_state(u2)
