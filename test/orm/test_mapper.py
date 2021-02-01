import logging
import logging.handlers

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import deferred
from sqlalchemy.orm import dynamic_loader
from sqlalchemy.orm import mapper
from sqlalchemy.orm import reconstructor
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm.persistence import _sort_states
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import ne_
from sqlalchemy.testing.fixtures import ComparableMixin
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class MapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_decl_attributes(self):
        """declarative mapper() now sets up some of the convenience
        attributes"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        am = self.mapper(Address, addresses)
        um = self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    "Address",
                    order_by="Address.id",
                    primaryjoin="User.id == remote(Address.user_id)",
                    backref="user",
                )
            },
        )

        assert not hasattr(User, "metadata")

        is_(um, User.__mapper__)
        is_(am, Address.__mapper__)

        is_(um.local_table, User.__table__)
        is_(am.local_table, Address.__table__)

        assert um.attrs.addresses.primaryjoin.compare(
            users.c.id == addresses.c.user_id
        )
        assert um.attrs.addresses.order_by[0].compare(Address.id)

        configure_mappers()

        is_(um.attrs.addresses.mapper, am)
        is_(am.attrs.user.mapper, um)

        sa.orm.clear_mappers()

        assert not hasattr(User, "__mapper__")
        assert not hasattr(User, "__table__")

    def test_default_constructor_imperative_map(self):
        class Plain(ComparableMixin):
            pass

        users = self.tables.users
        self.mapper(Plain, users)

        eq_(Plain(name="n1"), Plain(name="n1"))
        ne_(Plain(name="n1"), Plain(name="not1"))

        assert_raises_message(
            TypeError,
            "'foobar' is an invalid keyword argument for Plain",
            Plain,
            foobar="x",
        )

    def test_prop_shadow(self):
        """A backref name may not shadow an existing property name."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper(Address, addresses)
        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, backref="email_address")
            },
        )
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    def test_update_attr_keys(self, connection):
        """test that update()/insert() use the correct key when given
        InstrumentedAttributes."""

        User, users = self.classes.User, self.tables.users

        self.mapper(User, users, properties={"foobar": users.c.name})

        connection.execute(users.insert().values({User.foobar: "name1"}))
        eq_(
            connection.execute(
                sa.select(User.foobar).where(User.foobar == "name1")
            ).fetchall(),
            [("name1",)],
        )

        connection.execute(
            users.update().values({User.foobar: User.foobar + "foo"})
        )
        eq_(
            connection.execute(
                sa.select(User.foobar).where(User.foobar == "name1foo")
            ).fetchall(),
            [("name1foo",)],
        )

    def test_utils(self):
        users = self.tables.users
        addresses = self.tables.addresses
        Address = self.classes.Address

        from sqlalchemy.orm.base import _is_mapped_class, _is_aliased_class

        class Foo(object):
            x = "something"

            @property
            def y(self):
                return "something else"

        m = self.mapper(
            Foo, users, properties={"addresses": relationship(Address)}
        )
        self.mapper(Address, addresses)
        a1 = aliased(Foo)

        f = Foo()

        for fn, arg, ret in [
            (_is_mapped_class, Foo.x, False),
            (_is_mapped_class, Foo.y, False),
            (_is_mapped_class, Foo.name, False),
            (_is_mapped_class, Foo.addresses, False),
            (_is_mapped_class, Foo, True),
            (_is_mapped_class, f, False),
            (_is_mapped_class, a1, True),
            (_is_mapped_class, m, True),
            (_is_aliased_class, a1, True),
            (_is_aliased_class, Foo.x, False),
            (_is_aliased_class, Foo.y, False),
            (_is_aliased_class, Foo, False),
            (_is_aliased_class, f, False),
            (_is_aliased_class, a1, True),
            (_is_aliased_class, m, False),
        ]:
            assert fn(arg) == ret

    def test_entity_descriptor(self):
        users = self.tables.users

        from sqlalchemy.orm.base import _entity_descriptor

        class Foo(object):
            x = "something"

            @property
            def y(self):
                return "something else"

        m = self.mapper(Foo, users)
        a1 = aliased(Foo)

        for arg, key, ret in [
            (m, "x", Foo.x),
            (Foo, "x", Foo.x),
            (a1, "x", a1.x),
            (users, "name", users.c.name),
        ]:
            assert _entity_descriptor(arg, key) is ret

    def test_friendly_attribute_str_on_uncompiled_boom(self):
        User, users = self.classes.User, self.tables.users

        def boom():
            raise Exception("it broke")

        self.mapper(User, users, properties={"addresses": relationship(boom)})

        # test that QueryableAttribute.__str__() doesn't
        # cause a compile.
        eq_(str(User.addresses), "User.addresses")

    def test_exceptions_sticky(self):
        """test preservation of mapper compile errors raised during hasattr(),
        as well as for redundant mapper compile calls.  Test that
        repeated calls don't stack up error messages.

        """

        Address, addresses, User = (
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(
            Address, addresses, properties={"user": relationship(User)}
        )

        try:
            hasattr(Address.user, "property")
        except sa.orm.exc.UnmappedClassError:
            assert util.compat.py3k

        for i in range(3):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "One or more mappers failed to initialize - can't "
                "proceed with initialization of other mappers. "
                "Triggering mapper: 'mapped class Address->addresses'. "
                "Original exception was: Class 'test.orm._fixtures.User' "
                "is not mapped",
                configure_mappers,
            )

    def test_column_prefix(self):
        users, User = self.tables.users, self.classes.User

        self.mapper(
            User,
            users,
            column_prefix="_",
            properties={"user_name": synonym("_name")},
        )

        s = fixture_session()
        u = s.query(User).get(7)
        eq_(u._name, "jack")
        eq_(u._id, 7)
        u2 = s.query(User).filter_by(user_name="jack").one()
        assert u is u2

    def test_no_pks_1(self):
        User, users = self.classes.User, self.tables.users

        s = sa.select(users.c.name).alias("foo")
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    def test_no_pks_2(self):
        User, users = self.classes.User, self.tables.users

        s = sa.select(users.c.name).alias()
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    def test_reconfigure_on_other_mapper(self):
        """A configure trigger on an already-configured mapper
        still triggers a check against all mappers."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mp = self.mapper(User, users)
        sa.orm.configure_mappers()
        assert mp.registry._new_mappers is False

        m = self.mapper(
            Address,
            addresses,
            properties={"user": relationship(User, backref="addresses")},
        )

        assert m.configured is False
        assert m.registry._new_mappers is True
        User()
        assert User.addresses
        assert m.registry._new_mappers is False

    def test_configure_on_session(self):
        User, users = self.classes.User, self.tables.users

        m = self.mapper(User, users)
        session = fixture_session()
        session.connection(mapper=m)

    def test_incomplete_columns(self):
        """Loading from a select which does not contain all columns"""

        addresses, Address = self.tables.addresses, self.classes.Address

        self.mapper(Address, addresses)
        s = fixture_session()
        a = (
            s.query(Address)
            .from_statement(
                sa.select(addresses.c.id, addresses.c.user_id).order_by(
                    addresses.c.id
                )
            )
            .first()
        )
        eq_(a.user_id, 7)
        eq_(a.id, 1)
        # email address auto-defers
        assert "email_address" not in a.__dict__
        eq_(a.email_address, "jack@bean.com")

    def test_column_not_present(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        assert_raises_message(
            sa.exc.ArgumentError,
            "not represented in the mapper's table",
            mapper,
            User,
            users,
            properties={"foo": addresses.c.user_id},
        )

    def test_constructor_exc(self):
        """TypeError is raised for illegal constructor args,
        whether or not explicit __init__ is present [ticket:908]."""

        users, addresses = self.tables.users, self.tables.addresses

        class Foo(object):
            def __init__(self):
                pass

        class Bar(object):
            pass

        self.mapper(Foo, users)
        self.mapper(Bar, addresses)
        assert_raises(TypeError, Foo, x=5)
        assert_raises(TypeError, Bar, x=5)

    def test_sort_states_comparisons(self):
        """test that _sort_states() doesn't compare
        insert_order to state.key, for set of mixed
        persistent/pending.  In particular Python 3 disallows
        this.

        """

        class Foo(object):
            def __init__(self, id_):
                self.id = id_

        m = MetaData()
        foo_t = Table("foo", m, Column("id", String, primary_key=True))
        m = self.mapper(Foo, foo_t)

        class DontCompareMeToString(int):
            if util.py2k:

                def __lt__(self, other):
                    assert not isinstance(other, basestring)  # noqa
                    return int(self) < other

        foos = [Foo(id_="f%d" % i) for i in range(5)]
        states = [attributes.instance_state(f) for f in foos]

        for s in states[0:3]:
            s.key = m._identity_key_from_state(s)
        states[3].insert_order = DontCompareMeToString(5)
        states[4].insert_order = DontCompareMeToString(1)
        states[2].insert_order = DontCompareMeToString(3)
        eq_(
            _sort_states(m, states),
            [states[4], states[3], states[0], states[1], states[2]],
        )

    def test_props(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        m = self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(self.mapper(Address, addresses))
            },
        )
        assert User.addresses.property is m.get_property("addresses")

    def test_unicode_relationship_backref_names(self):
        # test [ticket:2901]
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(Address, addresses)
        self.mapper(
            User,
            users,
            properties={
                util.u("addresses"): relationship(
                    Address, backref=util.u("user")
                )
            },
        )
        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        assert a1.user is u1

    def test_configure_on_prop_1(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(self.mapper(Address, addresses))
            },
        )
        User.addresses.any(Address.email_address == "foo@bar.com")

    def test_configure_on_prop_2(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(self.mapper(Address, addresses))
            },
        )
        eq_(str(User.id == 3), str(users.c.id == 3))

    def test_configure_on_prop_3(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        class Foo(User):
            pass

        self.mapper(User, users)
        self.mapper(
            Foo,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )
        assert getattr(Foo().__class__, "name").impl is not None

    def test_deferred_subclass_attribute_instrument(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        class Foo(User):
            pass

        self.mapper(User, users)
        configure_mappers()
        self.mapper(
            Foo,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )
        assert getattr(Foo().__class__, "name").impl is not None

    def test_class_hier_only_instrument_once_multiple_configure(self):
        users, addresses = (self.tables.users, self.tables.addresses)

        class A(object):
            pass

        class ASub(A):
            pass

        class ASubSub(ASub):
            pass

        class B(object):
            pass

        from sqlalchemy.testing import mock
        from sqlalchemy.orm.attributes import register_attribute_impl

        with mock.patch(
            "sqlalchemy.orm.attributes.register_attribute_impl",
            side_effect=register_attribute_impl,
        ) as some_mock:

            self.mapper(A, users, properties={"bs": relationship(B)})
            self.mapper(B, addresses)

            configure_mappers()

            self.mapper(ASub, inherits=A)
            self.mapper(ASubSub, inherits=ASub)

            configure_mappers()

        b_calls = [c for c in some_mock.mock_calls if c[1][1] == "bs"]
        eq_(len(b_calls), 3)

    def test_check_descriptor_as_method(self):
        User, users = self.classes.User, self.tables.users

        m = self.mapper(User, users)

        class MyClass(User):
            def foo(self):
                pass

        assert m._is_userland_descriptor("foo", MyClass.foo)

    def test_configure_on_get_props_1(self):
        User, users = self.classes.User, self.tables.users

        m = self.mapper(User, users)
        assert not m.configured
        assert list(m.iterate_properties)
        assert m.configured

    def test_configure_on_get_props_2(self):
        User, users = self.classes.User, self.tables.users

        m = self.mapper(User, users)
        assert not m.configured
        assert m.get_property("name")
        assert m.configured

    def test_configure_on_get_props_3(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        m = self.mapper(User, users)
        assert not m.configured
        configure_mappers()

        self.mapper(
            Address,
            addresses,
            properties={"user": relationship(User, backref="addresses")},
        )
        assert m.get_property("addresses")

    def test_info(self):
        users = self.tables.users
        Address = self.classes.Address

        class MyComposite(object):
            pass

        for constructor, args in [
            (column_property, (users.c.name,)),
            (relationship, (Address,)),
            (composite, (MyComposite, "id", "name")),
            (synonym, "foo"),
        ]:
            obj = constructor(info={"x": "y"}, *args)
            eq_(obj.info, {"x": "y"})
            obj.info["q"] = "p"
            eq_(obj.info, {"x": "y", "q": "p"})

            obj = constructor(*args)
            eq_(obj.info, {})
            obj.info["q"] = "p"
            eq_(obj.info, {"q": "p"})

    def test_info_via_instrumented(self):
        m = MetaData()
        # create specific tables here as we don't want
        # users.c.id.info to be pre-initialized
        users = Table(
            "u",
            m,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        addresses = Table(
            "a",
            m,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("user_id", Integer, ForeignKey("u.id")),
        )
        Address = self.classes.Address
        User = self.classes.User

        self.mapper(
            User,
            users,
            properties={
                "name_lower": column_property(func.lower(users.c.name)),
                "addresses": relationship(Address),
            },
        )
        self.mapper(Address, addresses)

        # attr.info goes down to the original Column object
        # for the dictionary.  The annotated element needs to pass
        # this on.
        assert "info" not in users.c.id.__dict__
        is_(User.id.info, users.c.id.info)
        assert "info" in users.c.id.__dict__

        # for SQL expressions, ORM-level .info
        is_(User.name_lower.info, User.name_lower.property.info)

        # same for relationships
        is_(User.addresses.info, User.addresses.property.info)

    def test_add_property(self):
        users, addresses, Address = (
            self.tables.users,
            self.tables.addresses,
            self.classes.Address,
        )

        assert_col = []

        class User(fixtures.ComparableEntity):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

        m = self.mapper(User, users)
        self.mapper(Address, addresses)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))
        m.add_property("addresses", relationship(Address))

        sess = fixture_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(
                len(u.addresses),
                len(self.static.user_address_result[0].addresses),
            )
            eq_(u.name, "jack")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(2, go)

        u.name = "ed"
        u3 = User()
        u3.name = "some user"
        sess.add(u3)
        sess.flush()
        sess.rollback()

    def test_add_prop_via_backref_resets_memoizations_reconfigures(self):
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        m1 = self.mapper(User, users)
        User()

        self.mapper(
            Address,
            addresses,
            properties={"user": relationship(User, backref="addresses")},
        )
        # configure mappers takes place when User is generated
        User()
        assert hasattr(User, "addresses")
        assert "addresses" in [p.key for p in m1._polymorphic_properties]

    def test_replace_col_prop_w_syn(self):
        users, User = self.tables.users, self.classes.User

        m = self.mapper(User, users)
        m.add_property("_name", users.c.name)
        m.add_property("name", synonym("_name"))

        sess = fixture_session()
        u = sess.query(User).filter_by(name="jack").one()
        eq_(u._name, "jack")
        eq_(u.name, "jack")
        u.name = "jacko"
        assert m._columntoproperty[users.c.name] is m.get_property("_name")

        sa.orm.clear_mappers()

        m = self.mapper(User, users)
        m.add_property("name", synonym("_name", map_column=True))

        sess.expunge_all()
        u = sess.query(User).filter_by(name="jack").one()
        eq_(u._name, "jack")
        eq_(u.name, "jack")
        u.name = "jacko"
        assert m._columntoproperty[users.c.name] is m.get_property("_name")

    def test_replace_rel_prop_with_rel_warns(self):
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        m = self.mapper(
            User, users, properties={"addresses": relationship(Address)}
        )
        self.mapper(Address, addresses)

        assert_raises_message(
            sa.exc.SAWarning,
            "Property User.addresses on Mapper|User|users being replaced "
            "with new property User.addresses; the old property will "
            "be discarded",
            m.add_property,
            "addresses",
            relationship(Address),
        )

    def test_add_column_prop_deannotate(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses

        class SubUser(User):
            pass

        m = self.mapper(User, users)
        m2 = self.mapper(
            SubUser,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )
        self.mapper(Address, addresses, properties={"foo": relationship(m2)})
        # add property using annotated User.name,
        # needs to be deannotated
        m.add_property("x", column_property(User.name + "name"))
        s = fixture_session()
        q = s.query(m2).select_from(Address).join(Address.foo)
        self.assert_compile(
            q,
            "SELECT "
            "addresses_1.id AS addresses_1_id, "
            "users_1.id AS users_1_id, "
            "users_1.name AS users_1_name, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS "
            "addresses_1_email_address, "
            "users_1.name || :name_1 AS anon_1 "
            "FROM addresses JOIN (users AS users_1 JOIN addresses "
            "AS addresses_1 ON users_1.id = "
            "addresses_1.user_id) ON "
            "users_1.id = addresses.user_id",
        )

    def test_column_prop_deannotate(self):
        """test that column property deannotates,
        bringing expressions down to the original mapped columns.
        """
        User, users = self.classes.User, self.tables.users
        m = self.mapper(User, users)
        assert User.id.property.columns[0] is users.c.id
        assert User.name.property.columns[0] is users.c.name
        expr = User.name + "name"
        expr2 = sa.select(User.name, users.c.id)
        m.add_property("x", column_property(expr))
        m.add_property("y", column_property(expr2.scalar_subquery()))

        assert User.x.property.columns[0] is not expr
        assert User.x.property.columns[0].element.left is users.c.name
        # a deannotate needs to clone the base, in case
        # the original one referenced annotated elements.
        assert User.x.property.columns[0].element.right is not expr.right

        assert User.y.property.columns[0] is not expr2
        assert (
            User.y.property.columns[0].element._raw_columns[0] is users.c.name
        )
        assert User.y.property.columns[0].element._raw_columns[1] is users.c.id

    def test_synonym_replaces_backref(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        assert_calls = []

        class Address(object):
            def _get_user(self):
                assert_calls.append("get")
                return self._user

            def _set_user(self, user):
                assert_calls.append("set")
                self._user = user

            user = property(_get_user, _set_user)

        # synonym is created against nonexistent prop
        self.mapper(Address, addresses, properties={"user": synonym("_user")})
        sa.orm.configure_mappers()

        # later, backref sets up the prop
        self.mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="_user")},
        )

        sess = fixture_session()
        u1 = sess.query(User).get(7)
        u2 = sess.query(User).get(8)
        # comparaison ops need to work
        a1 = sess.query(Address).filter(Address.user == u1).one()
        eq_(a1.id, 1)
        a1.user = u2
        assert a1.user is u2
        eq_(assert_calls, ["set", "get"])

    def test_self_ref_synonym(self):
        t = Table(
            "nodes",
            MetaData(),
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
        )

        class Node(object):
            pass

        self.mapper(
            Node,
            t,
            properties={
                "_children": relationship(
                    Node, backref=backref("_parent", remote_side=t.c.id)
                ),
                "children": synonym("_children"),
                "parent": synonym("_parent"),
            },
        )

        n1 = Node()
        n2 = Node()
        n1.children.append(n2)
        assert n2.parent is n2._parent is n1
        assert n1.children[0] is n1._children[0] is n2
        eq_(str(Node.parent == n2), ":param_1 = nodes.parent_id")

    def test_reassign_polymorphic_identity_warns(self):
        User = self.classes.User
        users = self.tables.users

        class MyUser(User):
            pass

        self.mapper(
            User,
            users,
            polymorphic_on=users.c.name,
            polymorphic_identity="user",
        )
        assert_raises_message(
            sa.exc.SAWarning,
            "Reassigning polymorphic association for identity 'user'",
            mapper,
            MyUser,
            users,
            inherits=User,
            polymorphic_identity="user",
        )

    def test_prop_filters(self):
        t = Table(
            "person",
            MetaData(),
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(128)),
            Column("name", String(128)),
            Column("employee_number", Integer),
            Column("boss_id", Integer, ForeignKey("person.id")),
            Column("vendor_id", Integer),
        )

        class Person(object):
            pass

        class Vendor(Person):
            pass

        class Employee(Person):
            pass

        class Manager(Employee):
            pass

        class Hoho(object):
            pass

        class Lala(object):
            pass

        class Fub(object):
            pass

        class Frob(object):
            pass

        class HasDef(object):
            def name(self):
                pass

        class Empty(object):
            pass

        self.mapper(
            Empty, t, properties={"empty_id": t.c.id}, include_properties=[]
        )
        p_m = self.mapper(
            Person,
            t,
            polymorphic_on=t.c.type,
            include_properties=("id", "type", "name"),
        )
        e_m = self.mapper(
            Employee,
            inherits=p_m,
            polymorphic_identity="employee",
            properties={
                "boss": relationship(
                    Manager, backref=backref("peon"), remote_side=t.c.id
                )
            },
            exclude_properties=("vendor_id",),
        )

        self.mapper(
            Manager,
            inherits=e_m,
            polymorphic_identity="manager",
            include_properties=("id", "type"),
        )

        self.mapper(
            Vendor,
            inherits=p_m,
            polymorphic_identity="vendor",
            exclude_properties=("boss_id", "employee_number"),
        )
        self.mapper(Hoho, t, include_properties=("id", "type", "name"))
        self.mapper(
            Lala,
            t,
            exclude_properties=("vendor_id", "boss_id"),
            column_prefix="p_",
        )

        self.mapper(HasDef, t, column_prefix="h_")

        self.mapper(Fub, t, include_properties=(t.c.id, t.c.type))
        self.mapper(
            Frob,
            t,
            column_prefix="f_",
            exclude_properties=(t.c.boss_id, "employee_number", t.c.vendor_id),
        )

        configure_mappers()

        def assert_props(cls, want):
            have = set([n for n in dir(cls) if not n.startswith("_")])
            want = set(want)
            eq_(have, want)

        def assert_instrumented(cls, want):
            have = set([p.key for p in class_mapper(cls).iterate_properties])
            want = set(want)
            eq_(have, want)

        assert_props(
            HasDef,
            [
                "h_boss_id",
                "h_employee_number",
                "h_id",
                "name",
                "h_name",
                "h_vendor_id",
                "h_type",
            ],
        )
        assert_props(Person, ["id", "name", "type"])
        assert_instrumented(Person, ["id", "name", "type"])
        assert_props(
            Employee,
            ["boss", "boss_id", "employee_number", "id", "name", "type"],
        )
        assert_instrumented(
            Employee,
            ["boss", "boss_id", "employee_number", "id", "name", "type"],
        )
        assert_props(
            Manager,
            [
                "boss",
                "boss_id",
                "employee_number",
                "peon",
                "id",
                "name",
                "type",
            ],
        )

        # 'peon' and 'type' are both explicitly stated properties
        assert_instrumented(Manager, ["peon", "type", "id"])

        assert_props(Vendor, ["vendor_id", "id", "name", "type"])
        assert_props(Hoho, ["id", "name", "type"])
        assert_props(Lala, ["p_employee_number", "p_id", "p_name", "p_type"])
        assert_props(Fub, ["id", "type"])
        assert_props(Frob, ["f_id", "f_type", "f_name"])

        # putting the discriminator column in exclude_properties,
        # very weird.  As of 0.7.4 this re-maps it.
        class Foo(Person):
            pass

        assert_props(Empty, ["empty_id"])

        self.mapper(
            Foo,
            inherits=Person,
            polymorphic_identity="foo",
            exclude_properties=("type",),
        )
        assert hasattr(Foo, "type")
        assert Foo.type.property.columns[0] is t.c.type

    def test_prop_filters_defaults(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column(
                "id",
                Integer(),
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("x", Integer(), nullable=False, server_default="0"),
        )

        t.create(connection)

        class A(object):
            pass

        self.mapper(A, t, include_properties=["id"])
        s = Session(connection)
        s.add(A())
        s.commit()

    def test_we_dont_call_bool(self):
        class NoBoolAllowed(object):
            def __bool__(self):
                raise Exception("nope")

        self.mapper(NoBoolAllowed, self.tables.users)
        u1 = NoBoolAllowed()
        u1.name = "some name"
        s = Session(testing.db)
        s.add(u1)
        s.commit()
        assert s.query(NoBoolAllowed).get(u1.id) is u1

    def test_we_dont_call_eq(self):
        class NoEqAllowed(object):
            def __eq__(self, other):
                raise Exception("nope")

        addresses, users = self.tables.addresses, self.tables.users
        Address = self.classes.Address

        self.mapper(
            NoEqAllowed,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper(Address, addresses)

        u1 = NoEqAllowed()
        u1.name = "some name"
        u1.addresses = [Address(id=12, email_address="a1")]
        s = Session(testing.db)
        s.add(u1)
        s.commit()

        a1 = s.query(Address).filter_by(id=12).one()
        assert a1.user is u1

    def test_mapping_to_join_raises(self):
        """Test implicit merging of two cols raises."""

        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        usersaddresses = sa.join(
            users, addresses, users.c.id == addresses.c.user_id
        )
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Implicitly",
            mapper,
            User,
            usersaddresses,
            primary_key=[users.c.id],
        )

    def test_mapping_to_join_explicit_prop(self):
        """Mapping to a join"""

        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        usersaddresses = sa.join(
            users, addresses, users.c.id == addresses.c.user_id
        )
        self.mapper(
            User,
            usersaddresses,
            primary_key=[users.c.id],
            properties={"add_id": addresses.c.id},
        )
        result = fixture_session().query(User).order_by(users.c.id).all()
        eq_(result, self.static.user_result[:3])

    def test_mapping_to_join_exclude_prop(self):
        """Mapping to a join"""

        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        usersaddresses = sa.join(
            users, addresses, users.c.id == addresses.c.user_id
        )
        self.mapper(
            User,
            usersaddresses,
            primary_key=[users.c.id],
            exclude_properties=[addresses.c.id],
        )
        result = fixture_session().query(User).order_by(users.c.id).all()
        eq_(result, self.static.user_result[:3])

    def test_mapping_to_join_no_pk(self):
        email_bounces, addresses, Address = (
            self.tables.email_bounces,
            self.tables.addresses,
            self.classes.Address,
        )

        m = self.mapper(
            Address,
            addresses.join(email_bounces),
            properties={"id": [addresses.c.id, email_bounces.c.id]},
        )
        configure_mappers()
        assert addresses in m._pks_by_table
        assert email_bounces not in m._pks_by_table

        sess = fixture_session()
        a = Address(id=10, email_address="e1")
        sess.add(a)
        sess.flush()

        eq_(
            sess.connection().scalar(
                select(func.count("*")).select_from(addresses)
            ),
            6,
        )
        eq_(
            sess.connection().scalar(
                select(func.count("*")).select_from(email_bounces)
            ),
            5,
        )

    def test_mapping_to_outerjoin(self):
        """Mapping to an outer join with a nullable composite primary key."""

        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(
            User,
            users.outerjoin(addresses),
            primary_key=[users.c.id, addresses.c.id],
            properties=dict(address_id=addresses.c.id),
        )

        session = fixture_session()
        result = session.query(User).order_by(User.id, User.address_id).all()

        eq_(
            result,
            [
                User(id=7, address_id=1),
                User(id=8, address_id=2),
                User(id=8, address_id=3),
                User(id=8, address_id=4),
                User(id=9, address_id=5),
                User(id=10, address_id=None),
            ],
        )

    def test_mapping_to_outerjoin_no_partial_pks(self):
        """test the allow_partial_pks=False flag."""

        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper(
            User,
            users.outerjoin(addresses),
            allow_partial_pks=False,
            primary_key=[users.c.id, addresses.c.id],
            properties=dict(address_id=addresses.c.id),
        )

        session = fixture_session()
        result = session.query(User).order_by(User.id, User.address_id).all()

        eq_(
            result,
            [
                User(id=7, address_id=1),
                User(id=8, address_id=2),
                User(id=8, address_id=3),
                User(id=8, address_id=4),
                User(id=9, address_id=5),
                None,
            ],
        )

    def test_scalar_pk_arg(self):
        users, Keyword, items, Item, User, keywords = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.items,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
        )

        m1 = self.mapper(Item, items, primary_key=[items.c.id])
        m2 = self.mapper(Keyword, keywords, primary_key=keywords.c.id)
        m3 = self.mapper(User, users, primary_key=(users.c.id,))

        assert m1.primary_key[0] is items.c.id
        assert m2.primary_key[0] is keywords.c.id
        assert m3.primary_key[0] is users.c.id

    def test_custom_join(self):
        """select_from totally replace the FROM parameters."""

        users, items, order_items, orders, Item, User, Order = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        self.mapper(Item, items)

        self.mapper(
            Order,
            orders,
            properties=dict(items=relationship(Item, order_items)),
        )

        self.mapper(User, users, properties=dict(orders=relationship(Order)))

        session = fixture_session()
        result = (
            session.query(User)
            .select_from(users.join(orders).join(order_items).join(items))
            .filter(items.c.description == "item 4")
        ).all()

        eq_(result, [self.static.user_result[0]])

    # 'Raises a "expression evaluation not supported" error at prepare time
    @testing.fails_on("firebird", "FIXME: unknown")
    def test_function(self):
        """Mapping to a SELECT statement that has functions in it."""

        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        s = (
            sa.select(
                users,
                (users.c.id * 2).label("concat"),
                sa.func.count(addresses.c.id).label("count"),
            )
            .where(users.c.id == addresses.c.user_id)
            .group_by(*[c for c in users.c])
            .alias("myselect")
        )

        self.mapper(User, s)
        sess = fixture_session()
        result = sess.query(User).order_by(s.c.id).all()

        for idx, total in enumerate((14, 16)):
            eq_(result[idx].concat, result[idx].id * 2)
            eq_(result[idx].concat, total)

    def test_count(self):
        """The count function on Query."""

        User, users = self.classes.User, self.tables.users

        self.mapper(User, users)

        session = fixture_session()
        q = session.query(User)

        eq_(q.count(), 4)
        eq_(q.filter(User.id.in_([8, 9])).count(), 2)
        eq_(q.filter(users.c.id.in_([8, 9])).count(), 2)

        eq_(session.query(User.id).count(), 4)
        eq_(session.query(User.id).filter(User.id.in_((8, 9))).count(), 2)

    def test_many_to_many_count(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        self.mapper(Keyword, keywords)
        self.mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, item_keywords, lazy="select")
            ),
        )

        session = fixture_session()
        q = (
            session.query(Item)
            .join("keywords")
            .distinct()
            .filter(Keyword.name == "red")
        )
        eq_(q.count(), 2)

    def test_override_1(self):
        """Overriding a column raises an error."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        def go():
            self.mapper(
                User,
                users,
                properties=dict(
                    name=relationship(self.mapper(Address, addresses))
                ),
            )

        assert_raises(sa.exc.ArgumentError, go)

    def test_override_2(self):
        """exclude_properties cancels the error."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper(
            User,
            users,
            exclude_properties=["name"],
            properties=dict(
                name=relationship(self.mapper(Address, addresses))
            ),
        )

        assert bool(User.name)

    def test_override_3(self):
        """The column being named elsewhere also cancels the error,"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper(
            User,
            users,
            properties=dict(
                name=relationship(self.mapper(Address, addresses)),
                foo=users.c.name,
            ),
        )

    def test_synonym(self):
        users, addresses, Address = (
            self.tables.users,
            self.tables.addresses,
            self.classes.Address,
        )

        assert_col = []

        class extendedproperty(property):
            attribute = 123

        class User(object):
            def _get_name(self):
                assert_col.append(("get", self.name))
                return self.name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self.name = name

            uname = extendedproperty(_get_name, _set_name)

        self.mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper(Address, addresses), lazy="select"
                ),
                uname=synonym("name"),
                adlist=synonym("addresses"),
                adname=synonym("addresses"),
            ),
        )

        # ensure the synonym can get at the proxied comparators without
        # an explicit compile
        User.name == "ed"
        User.adname.any()

        assert hasattr(User, "adlist")
        # as of 0.4.2, synonyms always create a property
        assert hasattr(User, "adname")

        # test compile
        assert not isinstance(User.uname == "jack", bool)

        assert User.uname.property
        assert User.adlist.property

        sess = fixture_session()

        # test RowTuple names
        row = sess.query(User.id, User.uname).first()
        assert row.uname == row[1]

        u = sess.query(User).filter(User.uname == "jack").one()

        fixture = self.static.user_address_result[0].addresses
        eq_(u.adlist, fixture)

        addr = sess.query(Address).filter_by(id=fixture[0].id).one()
        u = sess.query(User).filter(User.adname.contains(addr)).one()
        u2 = sess.query(User).filter(User.adlist.contains(addr)).one()

        assert u is u2

        assert u not in sess.dirty
        u.uname = "some user name"
        assert len(assert_col) > 0
        eq_(assert_col, [("set", "some user name")])
        eq_(u.uname, "some user name")
        eq_(assert_col, [("set", "some user name"), ("get", "some user name")])
        eq_(u.name, "some user name")
        assert u in sess.dirty

        eq_(User.uname.attribute, 123)

    def test_synonym_of_synonym(self):
        users, User = (self.tables.users, self.classes.User)

        self.mapper(
            User, users, properties={"x": synonym("id"), "y": synonym("x")}
        )

        s = fixture_session()
        u = s.query(User).filter(User.y == 8).one()
        eq_(u.y, 8)

    def test_synonym_get_history(self):
        users, User = (self.tables.users, self.classes.User)

        self.mapper(
            User, users, properties={"x": synonym("id"), "y": synonym("x")}
        )

        u1 = User()
        eq_(attributes.instance_state(u1).attrs.x.history, (None, None, None))
        eq_(attributes.instance_state(u1).attrs.y.history, (None, None, None))

        u1.y = 5
        eq_(attributes.instance_state(u1).attrs.x.history, ([5], (), ()))
        eq_(attributes.instance_state(u1).attrs.y.history, ([5], (), ()))

    def test_synonym_nonexistent_attr(self):
        # test [ticket:4767].
        # synonym points to non-existent attrbute that hasn't been mapped yet.
        users = self.tables.users

        class User(object):
            def _x(self):
                return self.id

            x = property(_x)

        m = self.mapper(
            User,
            users,
            properties={"x": synonym("some_attr", descriptor=User.x)},
        )

        # object gracefully handles this condition
        assert not hasattr(User.x, "__name__")
        assert not hasattr(User.x, "comparator")

        m.add_property("some_attr", column_property(users.c.name))

        assert not hasattr(User.x, "__name__")
        assert hasattr(User.x, "comparator")

    def test_synonym_of_non_property_raises(self):
        from sqlalchemy.ext.associationproxy import association_proxy

        class User(object):
            pass

        users, Address, addresses = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
        )

        self.mapper(
            User,
            users,
            properties={"y": synonym("x"), "addresses": relationship(Address)},
        )
        self.mapper(Address, addresses)
        User.x = association_proxy("addresses", "email_address")

        assert_raises_message(
            sa.exc.InvalidRequestError,
            r'synonym\(\) attribute "User.x" only supports ORM mapped '
            "attributes, got .*AssociationProxy",
            getattr,
            User.y,
            "property",
        )

    def test_synonym_column_location(self):
        users, User = self.tables.users, self.classes.User

        def go():
            self.mapper(
                User,
                users,
                properties={"not_name": synonym("_name", map_column=True)},
            )

        assert_raises_message(
            sa.exc.ArgumentError,
            (
                "Can't compile synonym '_name': no column on table "
                "'users' named 'not_name'"
            ),
            go,
        )

    def test_column_synonyms(self):
        """Synonyms which automatically instrument properties,
        set up aliased column, etc."""

        addresses, users, Address = (
            self.tables.addresses,
            self.tables.users,
            self.classes.Address,
        )

        assert_col = []

        class User(object):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

        self.mapper(Address, addresses)
        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, lazy="select"),
                "name": synonym("_name", map_column=True),
            },
        )

        # test compile
        assert not isinstance(User.name == "jack", bool)

        assert hasattr(User, "name")
        assert hasattr(User, "_name")

        sess = fixture_session()
        u = sess.query(User).filter(User.name == "jack").one()
        eq_(u.name, "jack")
        u.name = "foo"
        eq_(u.name, "foo")
        eq_(assert_col, [("get", "jack"), ("set", "foo"), ("get", "foo")])

    def test_synonym_map_column_conflict(self):
        users, User = self.tables.users, self.classes.User

        assert_raises(
            sa.exc.ArgumentError,
            mapper,
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("_user_id", users.c.id),
                    ("id", synonym("_user_id", map_column=True)),
                ]
            ),
        )

        assert_raises(
            sa.exc.ArgumentError,
            mapper,
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("id", synonym("_user_id", map_column=True)),
                    ("_user_id", users.c.id),
                ]
            ),
        )

    def test_reentrant_compile(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        class MyFakeProperty(sa.orm.properties.ColumnProperty):
            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        self.mapper(
            User, users, properties={"name": MyFakeProperty(users.c.name)}
        )
        self.mapper(Address, addresses)
        configure_mappers()

        sa.orm.clear_mappers()

        class MyFakeProperty(sa.orm.properties.ColumnProperty):
            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        self.mapper(
            User, users, properties={"name": MyFakeProperty(users.c.name)}
        )
        self.mapper(Address, addresses)
        configure_mappers()

    @testing.combinations((True,), (False,))
    def test_registry_configure(self, cascade):
        User, users = self.classes.User, self.tables.users

        reg1 = registry()
        ump = reg1.map_imperatively(User, users)

        reg2 = registry()
        AnotherBase = reg2.generate_base()

        class Animal(AnotherBase):
            __tablename__ = "animal"
            species = Column(String(30), primary_key=True)
            __mapper_args__ = dict(
                polymorphic_on="species", polymorphic_identity="Animal"
            )
            user_id = Column("user_id", ForeignKey(users.c.id))

        ump.add_property("animal", relationship(Animal))

        if cascade:
            reg1.configure(cascade=True)
        else:
            with expect_raises_message(
                sa.exc.InvalidRequestError,
                "configure was called with cascade=False",
            ):
                reg1.configure()

    def test_reconstructor(self):
        users = self.tables.users

        recon = []

        class User(object):
            @reconstructor
            def reconstruct(self):
                recon.append("go")

        self.mapper(User, users)

        User()
        eq_(recon, [])
        fixture_session().query(User).first()
        eq_(recon, ["go"])

    def test_reconstructor_inheritance(self):
        users = self.tables.users

        recon = []

        class A(object):
            @reconstructor
            def reconstruct(self):
                assert isinstance(self, A)
                recon.append("A")

        class B(A):
            @reconstructor
            def reconstruct(self):
                assert isinstance(self, B)
                recon.append("B")

        class C(A):
            @reconstructor
            def reconstruct(self):
                assert isinstance(self, C)
                recon.append("C")

        self.mapper(
            A, users, polymorphic_on=users.c.name, polymorphic_identity="jack"
        )
        self.mapper(B, inherits=A, polymorphic_identity="ed")
        self.mapper(C, inherits=A, polymorphic_identity="chuck")

        A()
        B()
        C()
        eq_(recon, [])

        sess = fixture_session()
        sess.query(A).first()
        sess.query(B).first()
        sess.query(C).first()
        eq_(recon, ["A", "B", "C"])

    def test_reconstructor_init(self):

        users = self.tables.users

        recon = []

        class User(object):
            @reconstructor
            def __init__(self):
                recon.append("go")

        self.mapper(User, users)

        User()
        eq_(recon, ["go"])

        recon[:] = []
        fixture_session().query(User).first()
        eq_(recon, ["go"])

    def test_reconstructor_init_inheritance(self):
        users = self.tables.users

        recon = []

        class A(object):
            @reconstructor
            def __init__(self):
                assert isinstance(self, A)
                recon.append("A")

        class B(A):
            @reconstructor
            def __init__(self):
                assert isinstance(self, B)
                recon.append("B")

        class C(A):
            @reconstructor
            def __init__(self):
                assert isinstance(self, C)
                recon.append("C")

        self.mapper(
            A, users, polymorphic_on=users.c.name, polymorphic_identity="jack"
        )
        self.mapper(B, inherits=A, polymorphic_identity="ed")
        self.mapper(C, inherits=A, polymorphic_identity="chuck")

        A()
        B()
        C()
        eq_(recon, ["A", "B", "C"])

        recon[:] = []
        sess = fixture_session()
        sess.query(A).first()
        sess.query(B).first()
        sess.query(C).first()
        eq_(recon, ["A", "B", "C"])

    def test_unmapped_reconstructor_inheritance(self):
        users = self.tables.users

        recon = []

        class Base(object):
            @reconstructor
            def reconstruct(self):
                recon.append("go")

        class User(Base):
            pass

        self.mapper(User, users)

        User()
        eq_(recon, [])

        fixture_session().query(User).first()
        eq_(recon, ["go"])

    def test_unmapped_error(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper(Address, addresses)
        sa.orm.clear_mappers()

        self.mapper(
            User, users, properties={"addresses": relationship(Address)}
        )

        assert_raises_message(
            sa.orm.exc.UnmappedClassError,
            "Class 'test.orm._fixtures.Address' is not mapped",
            sa.orm.configure_mappers,
        )

    def test_unmapped_not_type_error(self):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Class object expected, got '5'.",
            class_mapper,
            5,
        )

    def test_unmapped_not_type_error_iter_ok(self):
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Class object expected, got '\(5, 6\)'.",
            class_mapper,
            (5, 6),
        )

    def test_attribute_error_raised_class_mapper(self):
        users = self.tables.users
        addresses = self.tables.addresses
        User = self.classes.User
        Address = self.classes.Address

        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    primaryjoin=lambda: users.c.id == addresses.wrong.user_id,
                )
            },
        )
        self.mapper(Address, addresses)
        assert_raises_message(
            AttributeError,
            "'Table' object has no attribute 'wrong'",
            class_mapper,
            Address,
        )

    def test_key_error_raised_class_mapper(self):
        users = self.tables.users
        addresses = self.tables.addresses
        User = self.classes.User
        Address = self.classes.Address

        self.mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    primaryjoin=lambda: users.c.id
                    == addresses.__dict__["wrong"].user_id,
                )
            },
        )
        self.mapper(Address, addresses)
        assert_raises_message(KeyError, "wrong", class_mapper, Address)

    def test_unmapped_subclass_error_postmap(self):
        users = self.tables.users

        class Base(object):
            pass

        class Sub(Base):
            pass

        self.mapper(Base, users)
        sa.orm.configure_mappers()

        # we can create new instances, set attributes.
        s = Sub()
        s.name = "foo"
        eq_(s.name, "foo")
        eq_(attributes.get_history(s, "name"), (["foo"], (), ()))

        # using it with an ORM operation, raises
        assert_raises(
            sa.orm.exc.UnmappedClassError, fixture_session().add, Sub()
        )

    def test_unmapped_subclass_error_premap(self):
        users = self.tables.users

        class Base(object):
            pass

        self.mapper(Base, users)

        class Sub(Base):
            pass

        sa.orm.configure_mappers()

        # we can create new instances, set attributes.
        s = Sub()
        s.name = "foo"
        eq_(s.name, "foo")
        eq_(attributes.get_history(s, "name"), (["foo"], (), ()))

        # using it with an ORM operation, raises
        assert_raises(
            sa.orm.exc.UnmappedClassError, fixture_session().add, Sub()
        )

    def test_oldstyle_mixin(self):
        users = self.tables.users

        class OldStyle:
            pass

        class NewStyle(object):
            pass

        class A(NewStyle, OldStyle):
            pass

        self.mapper(A, users)

        class B(OldStyle, NewStyle):
            pass

        self.mapper(B, users)


class RequirementsTest(fixtures.MappedTest):

    """Tests the contract for user classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "ht1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("ht1_id", Integer, ForeignKey("ht1.id")),
            Column("value", String(10)),
        )
        Table(
            "ht3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht4",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht3_id", Integer, ForeignKey("ht3.id"), primary_key=True),
        )
        Table(
            "ht5",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
        )
        Table(
            "ht6",
            metadata,
            Column("ht1a_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht1b_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("value", String(10)),
        )

    if util.py2k:

        def test_baseclass(self):
            ht1 = self.tables.ht1

            class OldStyle:
                pass

            assert_raises(sa.exc.ArgumentError, mapper, OldStyle, ht1)

            assert_raises(sa.exc.ArgumentError, mapper, 123)

            class NoWeakrefSupport(str):
                pass

            # TODO: is weakref support detectable without an instance?
            # self.assertRaises(
            #  sa.exc.ArgumentError, mapper, NoWeakrefSupport, t2)

    class _ValueBase(object):
        def __init__(self, value="abc", id_=None):
            self.id = id_
            self.value = value

        def __bool__(self):
            return False

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            if isinstance(other, type(self)):
                return self.value == other.value
            return False

    def test_comparison_overrides(self):
        """Simple tests to ensure users can supply comparison __methods__.

        The suite-level test --options are better suited to detect
        problems- they add selected __methods__ across the board on all
        ORM tests.  This test simply shoves a variety of operations
        through the ORM to catch basic regressions early in a standard
        test run.
        """

        ht6, ht5, ht4, ht3, ht2, ht1 = (
            self.tables.ht6,
            self.tables.ht5,
            self.tables.ht4,
            self.tables.ht3,
            self.tables.ht2,
            self.tables.ht1,
        )

        class H1(self._ValueBase):
            pass

        class H2(self._ValueBase):
            pass

        class H3(self._ValueBase):
            pass

        class H6(self._ValueBase):
            pass

        self.mapper(
            H1,
            ht1,
            properties={
                "h2s": relationship(H2, backref="h1"),
                "h3s": relationship(H3, secondary=ht4, backref="h1s"),
                "h1s": relationship(H1, secondary=ht5, backref="parent_h1"),
                "t6a": relationship(
                    H6, backref="h1a", primaryjoin=ht1.c.id == ht6.c.ht1a_id
                ),
                "t6b": relationship(
                    H6, backref="h1b", primaryjoin=ht1.c.id == ht6.c.ht1b_id
                ),
            },
        )
        self.mapper(H2, ht2)
        self.mapper(H3, ht3)
        self.mapper(H6, ht6)

        s = fixture_session()
        s.add_all([H1("abc"), H1("def")])
        h1 = H1("ghi")
        s.add(h1)
        h1.h2s.append(H2("abc"))
        h1.h3s.extend([H3(), H3()])
        h1.h1s.append(H1())

        s.flush()
        eq_(s.connection().scalar(select(func.count("*")).select_from(ht1)), 4)

        h6 = H6()
        h6.h1a = h1
        h6.h1b = h1

        h6 = H6()
        h6.h1a = h1
        h6.h1b = x = H1()
        assert x in s

        h6.h1b.h2s.append(H2("def"))

        s.flush()

        h1.h2s.extend([H2("abc"), H2("def")])
        s.flush()

        h1s = s.query(H1).options(sa.orm.joinedload("h2s")).all()
        eq_(len(h1s), 5)

        self.assert_unordered_result(
            h1s,
            H1,
            {"h2s": []},
            {"h2s": []},
            {
                "h2s": (
                    H2,
                    [{"value": "abc"}, {"value": "def"}, {"value": "abc"}],
                )
            },
            {"h2s": []},
            {"h2s": (H2, [{"value": "def"}])},
        )

        h1s = s.query(H1).options(sa.orm.joinedload("h3s")).all()

        eq_(len(h1s), 5)
        h1s = (
            s.query(H1)
            .options(
                sa.orm.joinedload("t6a").joinedload("h1b"),
                sa.orm.joinedload("h2s"),
                sa.orm.joinedload("h3s").joinedload("h1s"),
            )
            .all()
        )
        eq_(len(h1s), 5)

    def test_composite_results(self):
        ht2, ht1 = (self.tables.ht2, self.tables.ht1)

        class H1(self._ValueBase):
            def __init__(self, value, id_, h2s):
                self.value = value
                self.id = id_
                self.h2s = h2s

        class H2(self._ValueBase):
            def __init__(self, value, id_):
                self.value = value
                self.id = id_

        self.mapper(
            H1, ht1, properties={"h2s": relationship(H2, backref="h1")}
        )
        self.mapper(H2, ht2)
        s = fixture_session()
        s.add_all(
            [
                H1(
                    "abc",
                    1,
                    h2s=[H2("abc", id_=1), H2("def", id_=2), H2("def", id_=3)],
                ),
                H1(
                    "def",
                    2,
                    h2s=[H2("abc", id_=4), H2("abc", id_=5), H2("def", id_=6)],
                ),
            ]
        )
        s.commit()
        eq_(
            [
                (h1.value, h1.id, h2.value, h2.id)
                for h1, h2 in s.query(H1, H2)
                .join(H1.h2s)
                .order_by(H1.id, H2.id)
            ],
            [
                ("abc", 1, "abc", 1),
                ("abc", 1, "def", 2),
                ("abc", 1, "def", 3),
                ("def", 2, "abc", 4),
                ("def", 2, "abc", 5),
                ("def", 2, "def", 6),
            ],
        )

    def test_nonzero_len_recursion(self):
        ht1 = self.tables.ht1

        class H1(object):
            def __len__(self):
                return len(self.get_value())

            def get_value(self):
                self.value = "foobar"
                return self.value

        class H2(object):
            def __bool__(self):
                return bool(self.get_value())

            def get_value(self):
                self.value = "foobar"
                return self.value

        self.mapper(H1, ht1)
        self.mapper(H2, ht1)

        h1 = H1()
        h1.value = "Asdf"
        h1.value = "asdf asdf"  # ding

        h2 = H2()
        h2.value = "Asdf"
        h2.value = "asdf asdf"  # ding


class IsUserlandTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("someprop", Integer),
        )

    def _test(self, value, instancelevel=None):
        class Foo(object):
            someprop = value

        m = self.mapper(Foo, self.tables.foo)
        eq_(Foo.someprop, value)
        f1 = Foo()
        if instancelevel is not None:
            eq_(f1.someprop, instancelevel)
        else:
            eq_(f1.someprop, value)
        assert self.tables.foo.c.someprop not in m._columntoproperty

    def _test_not(self, value):
        class Foo(object):
            someprop = value

        m = self.mapper(Foo, self.tables.foo)
        is_(Foo.someprop.property.columns[0], self.tables.foo.c.someprop)
        assert self.tables.foo.c.someprop in m._columntoproperty

    def test_string(self):
        self._test("someprop")

    def test_unicode(self):
        self._test("someprop")

    def test_int(self):
        self._test(5)

    def test_dict(self):
        self._test({"bar": "bat"})

    def test_set(self):
        self._test(set([6]))

    def test_column(self):
        self._test_not(self.tables.foo.c.someprop)

    def test_relationship(self):
        self._test_not(relationship("bar"))

    def test_descriptor(self):
        def somefunc(self):
            return "hi"

        self._test(property(somefunc), "hi")


class MagicNamesTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "cartographers",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("alias", String(50)),
            Column("quip", String(100)),
        )
        Table(
            "maps",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("cart_id", Integer, ForeignKey("cartographers.id")),
            Column("state", String(2)),
            Column("data", sa.Text),
        )

    @classmethod
    def setup_classes(cls):
        class Cartographer(cls.Basic):
            pass

        class Map(cls.Basic):
            pass

    def test_mappish(self):
        maps, Cartographer, cartographers, Map = (
            self.tables.maps,
            self.classes.Cartographer,
            self.tables.cartographers,
            self.classes.Map,
        )

        self.mapper(
            Cartographer,
            cartographers,
            properties=dict(query=cartographers.c.quip),
        )
        self.mapper(
            Map,
            maps,
            properties=dict(mapper=relationship(Cartographer, backref="maps")),
        )

        c = Cartographer(
            name="Lenny", alias="The Dude", query="Where be dragons?"
        )
        Map(state="AK", mapper=c)

        sess = fixture_session()
        sess.add(c)
        sess.flush()
        sess.expunge_all()

        for C, M in (
            (Cartographer, Map),
            (sa.orm.aliased(Cartographer), sa.orm.aliased(Map)),
        ):
            c1 = (
                sess.query(C)
                .filter(C.alias == "The Dude")
                .filter(C.query == "Where be dragons?")
            ).one()
            sess.query(M).filter(M.mapper == c1).one()

    def test_direct_stateish(self):
        for reserved in (
            sa.orm.instrumentation.ClassManager.STATE_ATTR,
            sa.orm.instrumentation.ClassManager.MANAGER_ATTR,
        ):
            t = Table(
                "t",
                sa.MetaData(),
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
                Column(reserved, Integer),
            )

            class T(object):
                pass

            assert_raises_message(
                KeyError,
                (
                    "%r: requested attribute name conflicts with "
                    "instrumentation attribute of the same name." % reserved
                ),
                mapper,
                T,
                t,
            )

    def test_indirect_stateish(self):
        maps = self.tables.maps

        for reserved in (
            sa.orm.instrumentation.ClassManager.STATE_ATTR,
            sa.orm.instrumentation.ClassManager.MANAGER_ATTR,
        ):

            class M(object):
                pass

            assert_raises_message(
                KeyError,
                (
                    "requested attribute name conflicts with "
                    "instrumentation attribute of the same name"
                ),
                mapper,
                M,
                maps,
                properties={reserved: maps.c.state},
            )


class DocumentTest(fixtures.TestBase):
    def setup_test(self):

        self.mapper = registry().map_imperatively

    def test_doc_propagate(self):
        metadata = MetaData()
        t1 = Table(
            "t1",
            metadata,
            Column(
                "col1", Integer, primary_key=True, doc="primary key column"
            ),
            Column("col2", String, doc="data col"),
            Column("col3", String, doc="data col 2"),
            Column("col4", String, doc="data col 3"),
            Column("col5", String),
        )
        t2 = Table(
            "t2",
            metadata,
            Column(
                "col1", Integer, primary_key=True, doc="primary key column"
            ),
            Column("col2", String, doc="data col"),
            Column(
                "col3",
                Integer,
                ForeignKey("t1.col1"),
                doc="foreign key to t1.col1",
            ),
        )

        class Foo(object):
            pass

        class Bar(object):
            pass

        self.mapper(
            Foo,
            t1,
            properties={
                "bars": relationship(
                    Bar,
                    doc="bar relationship",
                    backref=backref("foo", doc="foo relationship"),
                ),
                "foober": column_property(t1.c.col3, doc="alternate data col"),
                "hoho": synonym("col4", doc="syn of col4"),
            },
        )
        self.mapper(Bar, t2)
        configure_mappers()
        eq_(Foo.col1.__doc__, "primary key column")
        eq_(Foo.col2.__doc__, "data col")
        eq_(Foo.col5.__doc__, None)
        eq_(Foo.foober.__doc__, "alternate data col")
        eq_(Foo.bars.__doc__, "bar relationship")
        eq_(Foo.hoho.__doc__, "syn of col4")
        eq_(Bar.col1.__doc__, "primary key column")
        eq_(Bar.foo.__doc__, "foo relationship")


class ORMLoggingTest(_fixtures.FixtureTest):
    def setup_test(self):
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [logging.getLogger("sqlalchemy.orm")]:
            log.addHandler(self.buf)

        self.mapper = registry().map_imperatively

    def teardown_test(self):
        for log in [logging.getLogger("sqlalchemy.orm")]:
            log.removeHandler(self.buf)

    def _current_messages(self):
        return [b.getMessage() for b in self.buf.buffer]

    def test_mapper_info_aliased(self):
        User, users = self.classes.User, self.tables.users
        tb = users.select().alias()
        self.mapper(User, tb)
        s = fixture_session()
        s.add(User(name="ed"))
        s.commit()

        for msg in self._current_messages():
            assert msg.startswith("(User|%%(%d anon)s) " % id(tb))


class ComparatorFactoryTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    def test_kwarg_accepted(self):
        users, Address = self.tables.users, self.classes.Address

        class DummyComposite(object):
            def __init__(self, x, y):
                pass

        from sqlalchemy.orm.interfaces import PropComparator

        class MyFactory(PropComparator):
            pass

        for args in (
            (column_property, users.c.name),
            (deferred, users.c.name),
            (synonym, "name"),
            (composite, DummyComposite, users.c.id, users.c.name),
            (relationship, Address),
            (backref, "address"),
            (dynamic_loader, Address),
        ):
            fn = args[0]
            args = args[1:]
            fn(comparator_factory=MyFactory, *args)

    def test_column(self):
        User, users = self.classes.User, self.tables.users

        from sqlalchemy.orm.properties import ColumnProperty

        class MyFactory(ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) == func.foobar(
                    other
                )

        self.mapper(
            User,
            users,
            properties={
                "name": column_property(
                    users.c.name, comparator_factory=MyFactory
                )
            },
        )
        self.assert_compile(
            User.name == "ed",
            "foobar(users.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )
        self.assert_compile(
            aliased(User).name == "ed",
            "foobar(users_1.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )

    def test_synonym(self):
        users, User = self.tables.users, self.classes.User

        from sqlalchemy.orm.properties import ColumnProperty

        class MyFactory(ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) == func.foobar(
                    other
                )

        self.mapper(
            User,
            users,
            properties={
                "name": synonym(
                    "_name", map_column=True, comparator_factory=MyFactory
                )
            },
        )
        self.assert_compile(
            User.name == "ed",
            "foobar(users.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )

        self.assert_compile(
            aliased(User).name == "ed",
            "foobar(users_1.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )

    def test_relationship(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        from sqlalchemy.orm.relationships import RelationshipProperty

        # NOTE: this API changed in 0.8, previously __clause_element__()
        # gave the parent selecatable, now it gives the
        # primaryjoin/secondaryjoin
        class MyFactory(RelationshipProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(
                    self._source_selectable().c.user_id
                ) == func.foobar(other.id)

        class MyFactory2(RelationshipProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(
                    self._source_selectable().c.id
                ) == func.foobar(other.user_id)

        self.mapper(User, users)
        self.mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                    comparator_factory=MyFactory,
                    backref=backref(
                        "addresses", comparator_factory=MyFactory2
                    ),
                )
            },
        )

        # these are kind of nonsensical tests.
        self.assert_compile(
            Address.user == User(id=5),
            "foobar(addresses.user_id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )
        self.assert_compile(
            User.addresses == Address(id=5, user_id=7),
            "foobar(users.id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )

        self.assert_compile(
            aliased(Address).user == User(id=5),
            "foobar(addresses_1.user_id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )

        self.assert_compile(
            aliased(User).addresses == Address(id=5, user_id=7),
            "foobar(users_1.id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect(),
        )


class RegistryConfigDisposeTest(fixtures.TestBase):
    """test the cascading behavior of registry configure / dispose."""

    @testing.fixture
    def threeway_fixture(self):
        reg1 = registry()
        reg2 = registry()
        reg3 = registry()

        ab = bc = True

        @reg1.mapped
        class A(object):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        @reg2.mapped
        class B(object):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey(A.id))

        @reg3.mapped
        class C(object):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey(B.id))

        if ab:
            A.__mapper__.add_property("b", relationship(B))

        if bc:
            B.__mapper__.add_property("c", relationship(C))

        yield reg1, reg2, reg3

        clear_mappers()

    @testing.fixture
    def threeway_configured_fixture(self, threeway_fixture):
        reg1, reg2, reg3 = threeway_fixture
        configure_mappers()

        return reg1, reg2, reg3

    @testing.combinations((True,), (False,), argnames="cascade")
    def test_configure_cascade_on_dependencies(
        self, threeway_fixture, cascade
    ):
        reg1, reg2, reg3 = threeway_fixture
        A, B, C = (
            reg1._class_registry["A"],
            reg2._class_registry["B"],
            reg3._class_registry["C"],
        )

        is_(reg3._new_mappers, True)
        is_(reg2._new_mappers, True)
        is_(reg1._new_mappers, True)

        if cascade:
            reg1.configure(cascade=True)

            is_(reg3._new_mappers, False)
            is_(reg2._new_mappers, False)
            is_(reg1._new_mappers, False)

            is_true(C.__mapper__.configured)
            is_true(B.__mapper__.configured)
            is_true(A.__mapper__.configured)
        else:
            with testing.expect_raises_message(
                sa.exc.InvalidRequestError,
                "configure was called with cascade=False but additional ",
            ):
                reg1.configure()

    @testing.combinations((True,), (False,), argnames="cascade")
    def test_configure_cascade_not_on_dependents(
        self, threeway_fixture, cascade
    ):
        reg1, reg2, reg3 = threeway_fixture
        A, B, C = (
            reg1._class_registry["A"],
            reg2._class_registry["B"],
            reg3._class_registry["C"],
        )

        is_(reg3._new_mappers, True)
        is_(reg2._new_mappers, True)
        is_(reg1._new_mappers, True)

        reg3.configure(cascade=cascade)

        is_(reg3._new_mappers, False)
        is_(reg2._new_mappers, True)
        is_(reg1._new_mappers, True)

        is_true(C.__mapper__.configured)
        is_false(B.__mapper__.configured)
        is_false(A.__mapper__.configured)

    @testing.combinations((True,), (False,), argnames="cascade")
    def test_dispose_cascade_not_on_dependencies(
        self, threeway_configured_fixture, cascade
    ):
        reg1, reg2, reg3 = threeway_configured_fixture
        A, B, C = (
            reg1._class_registry["A"],
            reg2._class_registry["B"],
            reg3._class_registry["C"],
        )
        am, bm, cm = A.__mapper__, B.__mapper__, C.__mapper__

        reg1.dispose(cascade=cascade)

        eq_(reg3.mappers, {cm})
        eq_(reg2.mappers, {bm})
        eq_(reg1.mappers, set())

        is_false(cm._dispose_called)
        is_false(bm._dispose_called)
        is_true(am._dispose_called)

    @testing.combinations((True,), (False,), argnames="cascade")
    def test_clear_cascade_not_on_dependents(
        self, threeway_configured_fixture, cascade
    ):
        reg1, reg2, reg3 = threeway_configured_fixture
        A, B, C = (
            reg1._class_registry["A"],
            reg2._class_registry["B"],
            reg3._class_registry["C"],
        )
        am, bm, cm = A.__mapper__, B.__mapper__, C.__mapper__

        if cascade:
            reg3.dispose(cascade=True)

            eq_(reg3.mappers, set())
            eq_(reg2.mappers, set())
            eq_(reg1.mappers, set())

            is_true(cm._dispose_called)
            is_true(bm._dispose_called)
            is_true(am._dispose_called)
        else:
            with testing.expect_raises_message(
                sa.exc.InvalidRequestError,
                "Registry has dependent registries that are not disposed; "
                "pass cascade=True to clear these also",
            ):
                reg3.dispose()
