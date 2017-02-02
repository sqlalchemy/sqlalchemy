"""General mapper operations with an emphasis on selecting/loading."""

from sqlalchemy.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import MetaData, Integer, String, \
    ForeignKey, func, util, select
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.engine import default
from sqlalchemy.orm import mapper, relationship, backref, \
    create_session, class_mapper, configure_mappers, reconstructor, \
    aliased, deferred, synonym, attributes, \
    column_property, composite, dynamic_loader, \
    comparable_property, Session
from sqlalchemy.orm.persistence import _sort_states
from sqlalchemy.testing import eq_, AssertsCompiledSQL, is_
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing.assertsql import CompiledSQL
import logging
import logging.handlers


class MapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_prop_shadow(self):
        """A backref name may not shadow an existing property name."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses)
        mapper(User, users,
               properties={
                   'addresses': relationship(Address, backref='email_address')
               })
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    def test_update_attr_keys(self):
        """test that update()/insert() use the correct key when given
        InstrumentedAttributes."""

        User, users = self.classes.User, self.tables.users

        mapper(User, users, properties={
            'foobar': users.c.name
        })

        users.insert().values({User.foobar: 'name1'}).execute()
        eq_(sa.select([User.foobar]).where(User.foobar == 'name1').
            execute().fetchall(), [('name1',)])

        users.update().values({User.foobar: User.foobar + 'foo'}).execute()
        eq_(sa.select([User.foobar]).where(User.foobar == 'name1foo').
            execute().fetchall(), [('name1foo',)])

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

        m = mapper(Foo, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)
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
        m = mapper(Foo, users)
        a1 = aliased(Foo)

        for arg, key, ret in [
            (m, "x", Foo.x),
            (Foo, "x", Foo.x),
            (a1, "x", a1.x),
            (users, "name", users.c.name)
        ]:
            assert _entity_descriptor(arg, key) is ret

    def test_friendly_attribute_str_on_uncompiled_boom(self):
        User, users = self.classes.User, self.tables.users

        def boom():
            raise Exception("it broke")
        mapper(User, users, properties={
            'addresses': relationship(boom)
        })

        # test that QueryableAttribute.__str__() doesn't
        # cause a compile.
        eq_(str(User.addresses), "User.addresses")

    def test_exceptions_sticky(self):
        """test preservation of mapper compile errors raised during hasattr(),
        as well as for redundant mapper compile calls.  Test that
        repeated calls don't stack up error messages.

        """

        Address, addresses, User = (self.classes.Address,
                                    self.tables.addresses,
                                    self.classes.User)

        mapper(Address, addresses, properties={
            'user': relationship(User)
        })

        try:
            hasattr(Address.user, 'property')
        except sa.orm.exc.UnmappedClassError:
            assert util.compat.py32

        for i in range(3):
            assert_raises_message(sa.exc.InvalidRequestError,
                                  "^One or more "
                                  "mappers failed to initialize - can't "
                                  "proceed with initialization of other "
                                  r"mappers. Triggering mapper\: "
                                  r"'Mapper\|Address\|addresses'."
                                  " Original exception was: Class "
                                  "'test.orm._fixtures.User' is not mapped$",
                                  configure_mappers)

    def test_column_prefix(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users, column_prefix='_', properties={
            'user_name': synonym('_name')
        })

        s = create_session()
        u = s.query(User).get(7)
        eq_(u._name, 'jack')
        eq_(u._id, 7)
        u2 = s.query(User).filter_by(user_name='jack').one()
        assert u is u2

    def test_no_pks_1(self):
        User, users = self.classes.User, self.tables.users

        s = sa.select([users.c.name]).alias('foo')
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    def test_no_pks_2(self):
        User, users = self.classes.User, self.tables.users

        s = sa.select([users.c.name]).alias()
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    def test_reconfigure_on_other_mapper(self):
        """A configure trigger on an already-configured mapper
        still triggers a check against all mappers."""

        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users)
        sa.orm.configure_mappers()
        assert sa.orm.mapperlib.Mapper._new_mappers is False

        m = mapper(Address, addresses, properties={
            'user': relationship(User, backref="addresses")})

        assert m.configured is False
        assert sa.orm.mapperlib.Mapper._new_mappers is True
        u = User()
        assert User.addresses
        assert sa.orm.mapperlib.Mapper._new_mappers is False

    def test_configure_on_session(self):
        User, users = self.classes.User, self.tables.users

        m = mapper(User, users)
        session = create_session()
        session.connection(m)

    def test_incomplete_columns(self):
        """Loading from a select which does not contain all columns"""

        addresses, Address = self.tables.addresses, self.classes.Address

        mapper(Address, addresses)
        s = create_session()
        a = s.query(Address).from_statement(
            sa.select([addresses.c.id, addresses.c.user_id]).
            order_by(addresses.c.id)).first()
        eq_(a.user_id, 7)
        eq_(a.id, 1)
        # email address auto-defers
        assert 'email_addres' not in a.__dict__
        eq_(a.email_address, 'jack@bean.com')

    def test_column_not_present(self):
        users, addresses, User = (self.tables.users,
                                  self.tables.addresses,
                                  self.classes.User)

        assert_raises_message(sa.exc.ArgumentError,
                              "not represented in the mapper's table",
                              mapper, User, users,
                              properties={'foo': addresses.c.user_id})

    def test_constructor_exc(self):
        """TypeError is raised for illegal constructor args,
        whether or not explicit __init__ is present [ticket:908]."""

        users, addresses = self.tables.users, self.tables.addresses

        class Foo(object):

            def __init__(self):
                pass

        class Bar(object):
            pass

        mapper(Foo, users)
        mapper(Bar, addresses)
        assert_raises(TypeError, Foo, x=5)
        assert_raises(TypeError, Bar, x=5)

    def test_sort_states_comparisons(self):
        """test that _sort_states() doesn't compare
        insert_order to state.key, for set of mixed
        persistent/pending.  In particular Python 3 disallows
        this.

        """
        class Foo(object):

            def __init__(self, id):
                self.id = id
        m = MetaData()
        foo_t = Table('foo', m,
                      Column('id', String, primary_key=True)
                      )
        m = mapper(Foo, foo_t)

        class DontCompareMeToString(int):
            if util.py2k:
                def __lt__(self, other):
                    assert not isinstance(other, basestring)
                    return int(self) < other

        foos = [Foo(id='f%d' % i) for i in range(5)]
        states = [attributes.instance_state(f) for f in foos]

        for s in states[0:3]:
            s.key = m._identity_key_from_state(s)
        states[3].insert_order = DontCompareMeToString(5)
        states[4].insert_order = DontCompareMeToString(1)
        states[2].insert_order = DontCompareMeToString(3)
        eq_(
            _sort_states(states),
            [states[4], states[3], states[0], states[1], states[2]]
        )

    def test_props(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        m = mapper(User, users, properties={
            'addresses': relationship(mapper(Address, addresses))
        })
        assert User.addresses.property is m.get_property('addresses')

    def test_unicode_relationship_backref_names(self):
        # test [ticket:2901]
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties={
            util.u('addresses'): relationship(Address, backref=util.u('user'))
        })
        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        assert a1.user is u1

    def test_configure_on_prop_1(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(mapper(Address, addresses))
        })
        User.addresses.any(Address.email_address == 'foo@bar.com')

    def test_configure_on_prop_2(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(mapper(Address, addresses))
        })
        eq_(str(User.id == 3), str(users.c.id == 3))

    def test_configure_on_prop_3(self):
        users, addresses, User = (self.tables.users,
                                  self.tables.addresses,
                                  self.classes.User)

        class Foo(User):
            pass

        mapper(User, users)
        mapper(Foo, addresses, inherits=User, properties={
            'address_id': addresses.c.id
        })
        assert getattr(Foo().__class__, 'name').impl is not None

    def test_deferred_subclass_attribute_instrument(self):
        users, addresses, User = (self.tables.users,
                                  self.tables.addresses,
                                  self.classes.User)

        class Foo(User):
            pass

        mapper(User, users)
        configure_mappers()
        mapper(Foo, addresses, inherits=User, properties={
            'address_id': addresses.c.id
        })
        assert getattr(Foo().__class__, 'name').impl is not None

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
            side_effect=register_attribute_impl
        ) as some_mock:

            mapper(A, users, properties={
                'bs': relationship(B)
            })
            mapper(B, addresses)

            configure_mappers()

            mapper(ASub, inherits=A)
            mapper(ASubSub, inherits=ASub)

            configure_mappers()

        b_calls = [
            c for c in some_mock.mock_calls if c[1][1] == 'bs'
        ]
        eq_(len(b_calls), 3)

    def test_check_descriptor_as_method(self):
        User, users = self.classes.User, self.tables.users

        m = mapper(User, users)

        class MyClass(User):

            def foo(self):
                pass
        m._is_userland_descriptor(MyClass.foo)

    def test_configure_on_get_props_1(self):
        User, users = self.classes.User, self.tables.users

        m = mapper(User, users)
        assert not m.configured
        assert list(m.iterate_properties)
        assert m.configured

    def test_configure_on_get_props_2(self):
        User, users = self.classes.User, self.tables.users

        m = mapper(User, users)
        assert not m.configured
        assert m.get_property('name')
        assert m.configured

    def test_configure_on_get_props_3(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        m = mapper(User, users)
        assert not m.configured
        configure_mappers()

        m2 = mapper(Address, addresses, properties={
            'user': relationship(User, backref='addresses')
        })
        assert m.get_property('addresses')

    def test_info(self):
        users = self.tables.users
        Address = self.classes.Address

        class MyComposite(object):
            pass
        for constructor, args in [
            (column_property, (users.c.name,)),
            (relationship, (Address,)),
            (composite, (MyComposite, 'id', 'name')),
            (synonym, 'foo'),
            (comparable_property, 'foo')
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
        users = Table('u', m, Column('id', Integer, primary_key=True),
                      Column('name', String))
        addresses = Table('a', m, Column('id', Integer, primary_key=True),
                          Column('name', String),
                          Column('user_id', Integer, ForeignKey('u.id')))
        Address = self.classes.Address
        User = self.classes.User

        mapper(User, users, properties={
            "name_lower": column_property(func.lower(users.c.name)),
            "addresses": relationship(Address)
        })
        mapper(Address, addresses)

        # attr.info goes down to the original Column object
        # for the dictionary.  The annotated element needs to pass
        # this on.
        assert 'info' not in users.c.id.__dict__
        is_(User.id.info, users.c.id.info)
        assert 'info' in users.c.id.__dict__

        # for SQL expressions, ORM-level .info
        is_(User.name_lower.info, User.name_lower.property.info)

        # same for relationships
        is_(User.addresses.info, User.addresses.property.info)

    def test_add_property(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)

        assert_col = []

        class User(fixtures.ComparableEntity):

            def _get_name(self):
                assert_col.append(('get', self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(('set', name))
                self._name = name
            name = property(_get_name, _set_name)

            def _uc_name(self):
                if self._name is None:
                    return None
                return self._name.upper()
            uc_name = property(_uc_name)
            uc_name2 = property(_uc_name)

        m = mapper(User, users)
        mapper(Address, addresses)

        class UCComparator(sa.orm.PropComparator):
            __hash__ = None

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, 'name')
                if other is None:
                    return col is None
                else:
                    return sa.func.upper(col) == sa.func.upper(other)

        m.add_property('_name', deferred(users.c.name))
        m.add_property('name', synonym('_name'))
        m.add_property('addresses', relationship(Address))
        m.add_property('uc_name', sa.orm.comparable_property(UCComparator))
        m.add_property('uc_name2', sa.orm.comparable_property(
            UCComparator, User.uc_name2))

        sess = create_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name='jack').one()

        def go():
            eq_(len(u.addresses),
                len(self.static.user_address_result[0].addresses))
            eq_(u.name, 'jack')
            eq_(u.uc_name, 'JACK')
            eq_(u.uc_name2, 'JACK')
            eq_(assert_col, [('get', 'jack')], str(assert_col))
        self.sql_count_(2, go)

        u.name = 'ed'
        u3 = User()
        u3.name = 'some user'
        sess.add(u3)
        sess.flush()
        sess.rollback()

    def test_add_prop_via_backref_resets_memoizations_reconfigures(self):
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        m1 = mapper(User, users)
        User()

        m2 = mapper(Address, addresses, properties={
            'user': relationship(User, backref="addresses")
        })
        # configure mappers takes place when User is generated
        User()
        assert hasattr(User, 'addresses')
        assert "addresses" in [p.key for p in m1._polymorphic_properties]

    def test_replace_col_prop_w_syn(self):
        users, User = self.tables.users, self.classes.User

        m = mapper(User, users)
        m.add_property('_name', users.c.name)
        m.add_property('name', synonym('_name'))

        sess = create_session()
        u = sess.query(User).filter_by(name='jack').one()
        eq_(u._name, 'jack')
        eq_(u.name, 'jack')
        u.name = 'jacko'
        assert m._columntoproperty[users.c.name] is m.get_property('_name')

        sa.orm.clear_mappers()

        m = mapper(User, users)
        m.add_property('name', synonym('_name', map_column=True))

        sess.expunge_all()
        u = sess.query(User).filter_by(name='jack').one()
        eq_(u._name, 'jack')
        eq_(u.name, 'jack')
        u.name = 'jacko'
        assert m._columntoproperty[users.c.name] is m.get_property('_name')

    def test_replace_rel_prop_with_rel_warns(self):
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        m = mapper(User, users, properties={
            "addresses": relationship(Address)
        })
        mapper(Address, addresses)

        assert_raises_message(
            sa.exc.SAWarning,
            "Property User.addresses on Mapper|User|users being replaced "
            "with new property User.addresses; the old property will "
            "be discarded",
            m.add_property,
            "addresses", relationship(Address)
        )

    def test_add_column_prop_deannotate(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses

        class SubUser(User):
            pass
        m = mapper(User, users)
        m2 = mapper(SubUser, addresses, inherits=User, properties={
            'address_id': addresses.c.id
        })
        m3 = mapper(Address, addresses, properties={
            'foo': relationship(m2)
        })
        # add property using annotated User.name,
        # needs to be deannotated
        m.add_property("x", column_property(User.name + "name"))
        s = create_session()
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
            "users_1.id = addresses.user_id"
        )

    def test_column_prop_deannotate(self):
        """test that column property deannotates,
        bringing expressions down to the original mapped columns.
        """
        User, users = self.classes.User, self.tables.users
        m = mapper(User, users)
        assert User.id.property.columns[0] is users.c.id
        assert User.name.property.columns[0] is users.c.name
        expr = User.name + "name"
        expr2 = sa.select([User.name, users.c.id])
        m.add_property("x", column_property(expr))
        m.add_property("y", column_property(expr2))

        assert User.x.property.columns[0] is not expr
        assert User.x.property.columns[0].element.left is users.c.name
        # a deannotate needs to clone the base, in case
        # the original one referenced annotated elements.
        assert User.x.property.columns[0].element.right is not expr.right

        assert User.y.property.columns[0] is not expr2
        assert User.y.property.columns[0].element.\
            _raw_columns[0] is users.c.name
        assert User.y.property.columns[0].element.\
            _raw_columns[1] is users.c.id

    def test_synonym_replaces_backref(self):
        addresses, users, User = (self.tables.addresses,
                                  self.tables.users,
                                  self.classes.User)

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
        mapper(Address, addresses, properties={
            'user': synonym('_user')
        })
        sa.orm.configure_mappers()

        # later, backref sets up the prop
        mapper(User, users, properties={
            'addresses': relationship(Address, backref='_user')
        })

        sess = create_session()
        u1 = sess.query(User).get(7)
        u2 = sess.query(User).get(8)
        # comparaison ops need to work
        a1 = sess.query(Address).filter(Address.user == u1).one()
        eq_(a1.id, 1)
        a1.user = u2
        assert a1.user is u2
        eq_(assert_calls, ["set", "get"])

    def test_self_ref_synonym(self):
        t = Table('nodes', MetaData(),
                  Column(
                      'id', Integer, primary_key=True,
                      test_needs_autoincrement=True),
                  Column('parent_id', Integer, ForeignKey('nodes.id')))

        class Node(object):
            pass

        mapper(Node, t, properties={
            '_children': relationship(
                Node, backref=backref('_parent', remote_side=t.c.id)),
            'children': synonym('_children'),
            'parent': synonym('_parent')
        })

        n1 = Node()
        n2 = Node()
        n1.children.append(n2)
        assert n2.parent is n2._parent is n1
        assert n1.children[0] is n1._children[0] is n2
        eq_(str(Node.parent == n2), ":param_1 = nodes.parent_id")

    def test_non_primary_identity_class(self):
        User = self.classes.User
        users, addresses = self.tables.users, self.tables.addresses

        class AddressUser(User):
            pass
        m1 = mapper(User, users, polymorphic_identity='user')
        m2 = mapper(AddressUser, addresses, inherits=User,
                    polymorphic_identity='address', properties={
                        'address_id': addresses.c.id
                    })
        m3 = mapper(AddressUser, addresses, non_primary=True)
        assert m3._identity_class is m2._identity_class
        eq_(
            m2.identity_key_from_instance(AddressUser()),
            m3.identity_key_from_instance(AddressUser())
        )

    def test_reassign_polymorphic_identity_warns(self):
        User = self.classes.User
        users = self.tables.users

        class MyUser(User):
            pass
        m1 = mapper(User, users, polymorphic_on=users.c.name,
                    polymorphic_identity='user')
        assert_raises_message(
            sa.exc.SAWarning,
            "Reassigning polymorphic association for identity 'user'",
            mapper,
            MyUser, users, inherits=User, polymorphic_identity='user'
        )

    def test_illegal_non_primary(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users)
        mapper(Address, addresses)
        mapper(User, users, non_primary=True, properties={
            'addresses': relationship(Address)
        })
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attempting to assign a new relationship 'addresses' "
            "to a non-primary mapper on class 'User'",
            configure_mappers
        )

    def test_illegal_non_primary_2(self):
        User, users = self.classes.User, self.tables.users

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Configure a primary mapper first",
            mapper, User, users, non_primary=True)

    def test_illegal_non_primary_3(self):
        users, addresses = self.tables.users, self.tables.addresses

        class Base(object):
            pass

        class Sub(Base):
            pass
        mapper(Base, users)
        assert_raises_message(sa.exc.InvalidRequestError,
                              "Configure a primary mapper first",
                              mapper, Sub, addresses, non_primary=True
                              )

    def test_prop_filters(self):
        t = Table('person', MetaData(),
                  Column('id', Integer, primary_key=True,
                         test_needs_autoincrement=True),
                  Column('type', String(128)),
                  Column('name', String(128)),
                  Column('employee_number', Integer),
                  Column('boss_id', Integer, ForeignKey('person.id')),
                  Column('vendor_id', Integer))

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

        mapper(
            Empty, t, properties={'empty_id': t.c.id},
            include_properties=[])
        p_m = mapper(Person, t, polymorphic_on=t.c.type,
                     include_properties=('id', 'type', 'name'))
        e_m = mapper(Employee, inherits=p_m,
                     polymorphic_identity='employee',
                     properties={
                         'boss': relationship(
                             Manager, backref=backref('peon'),
                             remote_side=t.c.id)},
                     exclude_properties=('vendor_id', ))

        mapper(
            Manager, inherits=e_m, polymorphic_identity='manager',
            include_properties=('id', 'type'))

        mapper(
            Vendor, inherits=p_m, polymorphic_identity='vendor',
            exclude_properties=('boss_id', 'employee_number'))
        mapper(Hoho, t, include_properties=('id', 'type', 'name'))
        mapper(
            Lala, t, exclude_properties=('vendor_id', 'boss_id'),
            column_prefix="p_")

        mapper(HasDef, t, column_prefix="h_")

        mapper(Fub, t, include_properties=(t.c.id, t.c.type))
        mapper(
            Frob, t, column_prefix='f_',
            exclude_properties=(
                t.c.boss_id,
                'employee_number', t.c.vendor_id))

        configure_mappers()

        def assert_props(cls, want):
            have = set([n for n in dir(cls) if not n.startswith('_')])
            want = set(want)
            eq_(have, want)

        def assert_instrumented(cls, want):
            have = set([p.key for p in class_mapper(cls).iterate_properties])
            want = set(want)
            eq_(have, want)

        assert_props(HasDef, ['h_boss_id', 'h_employee_number', 'h_id',
                              'name', 'h_name', 'h_vendor_id', 'h_type'])
        assert_props(Person, ['id', 'name', 'type'])
        assert_instrumented(Person, ['id', 'name', 'type'])
        assert_props(Employee, ['boss', 'boss_id', 'employee_number',
                                'id', 'name', 'type'])
        assert_instrumented(Employee, ['boss', 'boss_id', 'employee_number',
                                       'id', 'name', 'type'])
        assert_props(Manager, ['boss', 'boss_id', 'employee_number', 'peon',
                               'id', 'name', 'type'])

        # 'peon' and 'type' are both explicitly stated properties
        assert_instrumented(Manager, ['peon', 'type', 'id'])

        assert_props(Vendor, ['vendor_id', 'id', 'name', 'type'])
        assert_props(Hoho, ['id', 'name', 'type'])
        assert_props(Lala, ['p_employee_number', 'p_id', 'p_name', 'p_type'])
        assert_props(Fub, ['id', 'type'])
        assert_props(Frob, ['f_id', 'f_type', 'f_name', ])

        # putting the discriminator column in exclude_properties,
        # very weird.  As of 0.7.4 this re-maps it.
        class Foo(Person):
            pass
        assert_props(Empty, ['empty_id'])

        mapper(
            Foo, inherits=Person, polymorphic_identity='foo',
            exclude_properties=('type', ),
        )
        assert hasattr(Foo, "type")
        assert Foo.type.property.columns[0] is t.c.type

    @testing.provide_metadata
    def test_prop_filters_defaults(self):
        metadata = self.metadata
        t = Table('t', metadata,
                  Column(
                      'id', Integer(), primary_key=True,
                      test_needs_autoincrement=True),
                  Column('x', Integer(), nullable=False, server_default='0')
                  )
        t.create()

        class A(object):
            pass
        mapper(A, t, include_properties=['id'])
        s = Session()
        s.add(A())
        s.commit()

    def test_we_dont_call_bool(self):
        class NoBoolAllowed(object):

            def __bool__(self):
                raise Exception("nope")
        mapper(NoBoolAllowed, self.tables.users)
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

        mapper(NoEqAllowed, users, properties={
            'addresses': relationship(Address, backref='user')
        })
        mapper(Address, addresses)

        u1 = NoEqAllowed()
        u1.name = "some name"
        u1.addresses = [Address(id=12, email_address='a1')]
        s = Session(testing.db)
        s.add(u1)
        s.commit()

        a1 = s.query(Address).filter_by(id=12).one()
        assert a1.user is u1

    def test_mapping_to_join_raises(self):
        """Test implicit merging of two cols raises."""

        addresses, users, User = (self.tables.addresses,
                                  self.tables.users,
                                  self.classes.User)

        usersaddresses = sa.join(users, addresses,
                                 users.c.id == addresses.c.user_id)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Implicitly",
            mapper, User, usersaddresses, primary_key=[users.c.id]
        )

    def test_mapping_to_join_explicit_prop(self):
        """Mapping to a join"""

        User, addresses, users = (self.classes.User,
                                  self.tables.addresses,
                                  self.tables.users)

        usersaddresses = sa.join(users, addresses, users.c.id
                                 == addresses.c.user_id)
        mapper(User, usersaddresses, primary_key=[users.c.id],
               properties={'add_id': addresses.c.id}
               )
        result = create_session().query(User).order_by(users.c.id).all()
        eq_(result, self.static.user_result[:3])

    def test_mapping_to_join_exclude_prop(self):
        """Mapping to a join"""

        User, addresses, users = (self.classes.User,
                                  self.tables.addresses,
                                  self.tables.users)

        usersaddresses = sa.join(users, addresses, users.c.id
                                 == addresses.c.user_id)
        mapper(User, usersaddresses, primary_key=[users.c.id],
               exclude_properties=[addresses.c.id]
               )
        result = create_session().query(User).order_by(users.c.id).all()
        eq_(result, self.static.user_result[:3])

    def test_mapping_to_join_no_pk(self):
        email_bounces, addresses, Address = (self.tables.email_bounces,
                                             self.tables.addresses,
                                             self.classes.Address)

        m = mapper(Address,
                   addresses.join(email_bounces),
                   properties={'id': [addresses.c.id, email_bounces.c.id]}
                   )
        configure_mappers()
        assert addresses in m._pks_by_table
        assert email_bounces not in m._pks_by_table

        sess = create_session()
        a = Address(id=10, email_address='e1')
        sess.add(a)
        sess.flush()

        eq_(
            select([func.count('*')]).select_from(addresses).scalar(), 6)
        eq_(
            select([func.count('*')]).select_from(email_bounces).scalar(), 5)

    def test_mapping_to_outerjoin(self):
        """Mapping to an outer join with a nullable composite primary key."""

        users, addresses, User = (self.tables.users,
                                  self.tables.addresses,
                                  self.classes.User)

        mapper(User, users.outerjoin(addresses),
               primary_key=[users.c.id, addresses.c.id],
               properties=dict(
            address_id=addresses.c.id))

        session = create_session()
        result = session.query(User).order_by(User.id, User.address_id).all()

        eq_(result, [
            User(id=7, address_id=1),
            User(id=8, address_id=2),
            User(id=8, address_id=3),
            User(id=8, address_id=4),
            User(id=9, address_id=5),
            User(id=10, address_id=None)])

    def test_mapping_to_outerjoin_no_partial_pks(self):
        """test the allow_partial_pks=False flag."""

        users, addresses, User = (self.tables.users,
                                  self.tables.addresses,
                                  self.classes.User)

        mapper(User, users.outerjoin(addresses),
               allow_partial_pks=False,
               primary_key=[users.c.id, addresses.c.id],
               properties=dict(
            address_id=addresses.c.id))

        session = create_session()
        result = session.query(User).order_by(User.id, User.address_id).all()

        eq_(result, [
            User(id=7, address_id=1),
            User(id=8, address_id=2),
            User(id=8, address_id=3),
            User(id=8, address_id=4),
            User(id=9, address_id=5),
            None])

    def test_scalar_pk_arg(self):
        users, Keyword, items, Item, User, keywords = (self.tables.users,
                                                       self.classes.Keyword,
                                                       self.tables.items,
                                                       self.classes.Item,
                                                       self.classes.User,
                                                       self.tables.keywords)

        m1 = mapper(Item, items, primary_key=[items.c.id])
        m2 = mapper(Keyword, keywords, primary_key=keywords.c.id)
        m3 = mapper(User, users, primary_key=(users.c.id,))

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
            self.classes.Order)

        mapper(Item, items)

        mapper(Order, orders, properties=dict(
            items=relationship(Item, order_items)))

        mapper(User, users, properties=dict(
            orders=relationship(Order)))

        session = create_session()
        result = (session.query(User).
                  select_from(users.join(orders).
                              join(order_items).
                              join(items)).
                  filter(items.c.description == 'item 4')).all()

        eq_(result, [self.static.user_result[0]])

    @testing.uses_deprecated("Mapper.order_by")
    def test_cancel_order_by(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users, order_by=users.c.name.desc())

        assert "order by users.name desc" in \
            str(create_session().query(User).statement).lower()
        assert "order by" not in \
            str(create_session().query(User).order_by(None).statement).lower()
        assert "order by users.name asc" in \
            str(create_session().query(User).order_by(
                User.name.asc()).statement).lower()

        eq_(
            create_session().query(User).all(),
            [User(id=7, name='jack'), User(id=9, name='fred'),
             User(id=8, name='ed'), User(id=10, name='chuck')]
        )

        eq_(
            create_session().query(User).order_by(User.name).all(),
            [User(id=10, name='chuck'), User(id=8, name='ed'),
             User(id=9, name='fred'), User(id=7, name='jack')]
        )

    # 'Raises a "expression evaluation not supported" error at prepare time
    @testing.fails_on('firebird', 'FIXME: unknown')
    def test_function(self):
        """Mapping to a SELECT statement that has functions in it."""

        addresses, users, User = (self.tables.addresses,
                                  self.tables.users,
                                  self.classes.User)

        s = sa.select([users,
                       (users.c.id * 2).label('concat'),
                       sa.func.count(addresses.c.id).label('count')],
                      users.c.id == addresses.c.user_id,
                      group_by=[c for c in users.c]).alias('myselect')

        mapper(User, s)
        sess = create_session()
        result = sess.query(User).order_by(s.c.id).all()

        for idx, total in enumerate((14, 16)):
            eq_(result[idx].concat, result[idx].id * 2)
            eq_(result[idx].concat, total)

    def test_count(self):
        """The count function on Query."""

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        session = create_session()
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
            self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, item_keywords, lazy='select')))

        session = create_session()
        q = (session.query(Item).
             join('keywords').
             distinct().
             filter(Keyword.name == "red"))
        eq_(q.count(), 2)

    def test_override_1(self):
        """Overriding a column raises an error."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        def go():
            mapper(User, users,
                   properties=dict(
                       name=relationship(mapper(Address, addresses))))

        assert_raises(sa.exc.ArgumentError, go)

    def test_override_2(self):
        """exclude_properties cancels the error."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users,
               exclude_properties=['name'],
               properties=dict(
                   name=relationship(mapper(Address, addresses))))

        assert bool(User.name)

    def test_override_3(self):
        """The column being named elsewhere also cancels the error,"""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users,
               properties=dict(
                   name=relationship(mapper(Address, addresses)),
                   foo=users.c.name))

    def test_synonym(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)

        assert_col = []

        class extendedproperty(property):
            attribute = 123

        class User(object):

            def _get_name(self):
                assert_col.append(('get', self.name))
                return self.name

            def _set_name(self, name):
                assert_col.append(('set', name))
                self.name = name
            uname = extendedproperty(_get_name, _set_name)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='select'),
            uname=synonym('name'),
            adlist=synonym('addresses'),
            adname=synonym('addresses')
        ))

        # ensure the synonym can get at the proxied comparators without
        # an explicit compile
        User.name == 'ed'
        User.adname.any()

        assert hasattr(User, 'adlist')
        # as of 0.4.2, synonyms always create a property
        assert hasattr(User, 'adname')

        # test compile
        assert not isinstance(User.uname == 'jack', bool)

        assert User.uname.property
        assert User.adlist.property

        sess = create_session()

        # test RowTuple names
        row = sess.query(User.id, User.uname).first()
        assert row.uname == row[1]

        u = sess.query(User).filter(User.uname == 'jack').one()

        fixture = self.static.user_address_result[0].addresses
        eq_(u.adlist, fixture)

        addr = sess.query(Address).filter_by(id=fixture[0].id).one()
        u = sess.query(User).filter(User.adname.contains(addr)).one()
        u2 = sess.query(User).filter(User.adlist.contains(addr)).one()

        assert u is u2

        assert u not in sess.dirty
        u.uname = "some user name"
        assert len(assert_col) > 0
        eq_(assert_col, [('set', 'some user name')])
        eq_(u.uname, "some user name")
        eq_(assert_col, [('set', 'some user name'), ('get', 'some user name')])
        eq_(u.name, "some user name")
        assert u in sess.dirty

        eq_(User.uname.attribute, 123)

    def test_synonym_of_synonym(self):
        users, User = (self.tables.users,
                       self.classes.User)

        mapper(User, users, properties={
            'x': synonym('id'),
            'y': synonym('x')
        })

        s = Session()
        u = s.query(User).filter(User.y == 8).one()
        eq_(u.y, 8)

    def test_synonym_column_location(self):
        users, User = self.tables.users, self.classes.User

        def go():
            mapper(User, users, properties={
                'not_name': synonym('_name', map_column=True)})

        assert_raises_message(
            sa.exc.ArgumentError,
            ("Can't compile synonym '_name': no column on table "
             "'users' named 'not_name'"),
            go)

    def test_column_synonyms(self):
        """Synonyms which automatically instrument properties,
        set up aliased column, etc."""

        addresses, users, Address = (self.tables.addresses,
                                     self.tables.users,
                                     self.classes.Address)

        assert_col = []

        class User(object):

            def _get_name(self):
                assert_col.append(('get', self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(('set', name))
                self._name = name
            name = property(_get_name, _set_name)

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses': relationship(Address, lazy='select'),
            'name': synonym('_name', map_column=True)
        })

        # test compile
        assert not isinstance(User.name == 'jack', bool)

        assert hasattr(User, 'name')
        assert hasattr(User, '_name')

        sess = create_session()
        u = sess.query(User).filter(User.name == 'jack').one()
        eq_(u.name, 'jack')
        u.name = 'foo'
        eq_(u.name, 'foo')
        eq_(assert_col, [('get', 'jack'), ('set', 'foo'), ('get', 'foo')])

    def test_synonym_map_column_conflict(self):
        users, User = self.tables.users, self.classes.User

        assert_raises(
            sa.exc.ArgumentError,
            mapper,
            User, users, properties=util.OrderedDict([
                ('_user_id', users.c.id),
                ('id', synonym('_user_id', map_column=True)),
            ])
        )

        assert_raises(
            sa.exc.ArgumentError,
            mapper,
            User, users, properties=util.OrderedDict([
                ('id', synonym('_user_id', map_column=True)),
                ('_user_id', users.c.id),
            ])
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
                col = getattr(cls, 'name')
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
            mapper(User, users, properties=dict(
                uc_name=sa.orm.comparable_property(*args)))
            return User

        for User in (map_(True), map_(False)):
            sess = create_session()
            sess.begin()
            q = sess.query(User)

            assert hasattr(User, 'name')
            assert hasattr(User, 'uc_name')

            eq_(User.uc_name.method1(), "method1")
            eq_(User.uc_name.method2('x'), "method2")

            assert_raises_message(
                AttributeError,
                "Neither 'extendedproperty' object nor 'UCComparator' "
                "object associated with User.uc_name has an attribute "
                "'nonexistent'",
                getattr, User.uc_name, 'nonexistent')

            # test compile
            assert not isinstance(User.uc_name == 'jack', bool)
            u = q.filter(User.uc_name == 'JACK').one()

            assert u.uc_name == "JACK"
            assert u not in sess.dirty

            u.name = "some user name"
            eq_(u.name, "some user name")
            assert u in sess.dirty
            eq_(u.uc_name, "SOME USER NAME")

            sess.flush()
            sess.expunge_all()

            q = sess.query(User)
            u2 = q.filter(User.name == 'some user name').one()
            u3 = q.filter(User.uc_name == 'SOME USER NAME').one()

            assert u2 is u3

            eq_(User.uc_name.attribute, 123)
            sess.rollback()

    def test_comparable_column(self):
        users, User = self.tables.users, self.classes.User

        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()
                                  ) == func.lower(other)

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op('&=')(other)

        mapper(User, users, properties={
            'name': sa.orm.column_property(users.c.name,
                                           comparator_factory=MyComparator)
        })

        assert_raises_message(
            AttributeError,
            "Neither 'InstrumentedAttribute' object nor "
            "'MyComparator' object associated with User.name has "
            "an attribute 'nonexistent'",
            getattr, User.name, "nonexistent")

        eq_(
            str((User.name == 'ed').compile(
                dialect=sa.engine.default.DefaultDialect())),
            "lower(users.name) = lower(:lower_1)")
        eq_(
            str((User.name.intersects('ed')).compile(
                dialect=sa.engine.default.DefaultDialect())),
            "users.name &= :name_1")

    def test_reentrant_compile(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        class MyFakeProperty(sa.orm.properties.ColumnProperty):

            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        m1 = mapper(User, users, properties={
            'name': MyFakeProperty(users.c.name)
        })
        m2 = mapper(Address, addresses)
        configure_mappers()

        sa.orm.clear_mappers()

        class MyFakeProperty(sa.orm.properties.ColumnProperty):

            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        m1 = mapper(User, users, properties={
            'name': MyFakeProperty(users.c.name)
        })
        m2 = mapper(Address, addresses)
        configure_mappers()

    def test_reconstructor(self):
        users = self.tables.users

        recon = []

        class User(object):

            @reconstructor
            def reconstruct(self):
                recon.append('go')

        mapper(User, users)

        User()
        eq_(recon, [])
        create_session().query(User).first()
        eq_(recon, ['go'])

    def test_reconstructor_inheritance(self):
        users = self.tables.users

        recon = []

        class A(object):

            @reconstructor
            def reconstruct(self):
                assert isinstance(self, A)
                recon.append('A')

        class B(A):

            @reconstructor
            def reconstruct(self):
                assert isinstance(self, B)
                recon.append('B')

        class C(A):

            @reconstructor
            def reconstruct(self):
                assert isinstance(self, C)
                recon.append('C')

        mapper(A, users, polymorphic_on=users.c.name,
               polymorphic_identity='jack')
        mapper(B, inherits=A, polymorphic_identity='ed')
        mapper(C, inherits=A, polymorphic_identity='chuck')

        A()
        B()
        C()
        eq_(recon, [])

        sess = create_session()
        sess.query(A).first()
        sess.query(B).first()
        sess.query(C).first()
        eq_(recon, ['A', 'B', 'C'])

    def test_unmapped_reconstructor_inheritance(self):
        users = self.tables.users

        recon = []

        class Base(object):

            @reconstructor
            def reconstruct(self):
                recon.append('go')

        class User(Base):
            pass

        mapper(User, users)

        User()
        eq_(recon, [])

        create_session().query(User).first()
        eq_(recon, ['go'])

    def test_unmapped_error(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses)
        sa.orm.clear_mappers()

        mapper(User, users, properties={
            'addresses': relationship(Address)
        })

        assert_raises_message(
            sa.orm.exc.UnmappedClassError,
            "Class 'test.orm._fixtures.Address' is not mapped",
            sa.orm.configure_mappers)

    def test_unmapped_not_type_error(self):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Class object expected, got '5'.",
            class_mapper, 5
        )

    def test_unmapped_not_type_error_iter_ok(self):
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Class object expected, got '\(5, 6\)'.",
            class_mapper, (5, 6)
        )

    def test_attribute_error_raised_class_mapper(self):
        users = self.tables.users
        addresses = self.tables.addresses
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, users, properties={
            "addresses": relationship(
                Address,
                primaryjoin=lambda: users.c.id == addresses.wrong.user_id)
        })
        mapper(Address, addresses)
        assert_raises_message(
            AttributeError,
            "'Table' object has no attribute 'wrong'",
            class_mapper, Address
        )

    def test_key_error_raised_class_mapper(self):
        users = self.tables.users
        addresses = self.tables.addresses
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, users, properties={
            "addresses": relationship(Address,
                                      primaryjoin=lambda: users.c.id ==
                                      addresses.__dict__['wrong'].user_id)
        })
        mapper(Address, addresses)
        assert_raises_message(
            KeyError,
            "wrong",
            class_mapper, Address
        )

    def test_unmapped_subclass_error_postmap(self):
        users = self.tables.users

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, users)
        sa.orm.configure_mappers()

        # we can create new instances, set attributes.
        s = Sub()
        s.name = 'foo'
        eq_(s.name, 'foo')
        eq_(
            attributes.get_history(s, 'name'),
            (['foo'], (), ())
        )

        # using it with an ORM operation, raises
        assert_raises(sa.orm.exc.UnmappedClassError,
                      create_session().add, Sub())

    def test_unmapped_subclass_error_premap(self):
        users = self.tables.users

        class Base(object):
            pass

        mapper(Base, users)

        class Sub(Base):
            pass

        sa.orm.configure_mappers()

        # we can create new instances, set attributes.
        s = Sub()
        s.name = 'foo'
        eq_(s.name, 'foo')
        eq_(
            attributes.get_history(s, 'name'),
            (['foo'], (), ())
        )

        # using it with an ORM operation, raises
        assert_raises(sa.orm.exc.UnmappedClassError,
                      create_session().add, Sub())

    def test_oldstyle_mixin(self):
        users = self.tables.users

        class OldStyle:
            pass

        class NewStyle(object):
            pass

        class A(NewStyle, OldStyle):
            pass

        mapper(A, users)

        class B(OldStyle, NewStyle):
            pass

        mapper(B, users)


class DocumentTest(fixtures.TestBase):

    def test_doc_propagate(self):
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer, primary_key=True,
                          doc="primary key column"),
                   Column('col2', String, doc="data col"),
                   Column('col3', String, doc="data col 2"),
                   Column('col4', String, doc="data col 3"),
                   Column('col5', String),
                   )
        t2 = Table('t2', metadata,
                   Column('col1', Integer, primary_key=True,
                          doc="primary key column"),
                   Column('col2', String, doc="data col"),
                   Column('col3', Integer, ForeignKey('t1.col1'),
                          doc="foreign key to t1.col1")
                   )

        class Foo(object):
            pass

        class Bar(object):
            pass

        mapper(Foo, t1, properties={
            'bars': relationship(Bar,
                                 doc="bar relationship",
                                 backref=backref('foo', doc='foo relationship')
                                 ),
            'foober': column_property(t1.c.col3, doc='alternate data col'),
            'hoho': synonym("col4", doc="syn of col4")
        })
        mapper(Bar, t2)
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

    def setup(self):
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger('sqlalchemy.orm'),
        ]:
            log.addHandler(self.buf)

    def teardown(self):
        for log in [
            logging.getLogger('sqlalchemy.orm'),
        ]:
            log.removeHandler(self.buf)

    def _current_messages(self):
        return [b.getMessage() for b in self.buf.buffer]

    def test_mapper_info_aliased(self):
        User, users = self.classes.User, self.tables.users
        tb = users.select().alias()
        mapper(User, tb)
        s = Session()
        s.add(User(name='ed'))
        s.commit()

        for msg in self._current_messages():
            assert msg.startswith('(User|%%(%d anon)s) ' % id(tb))


class OptionsTest(_fixtures.FixtureTest):

    def test_synonym_options(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='select',
                                   order_by=addresses.c.id),
            adlist=synonym('addresses')))

        def go():
            sess = create_session()
            u = (sess.query(User).
                 order_by(User.id).
                 options(sa.orm.joinedload('adlist')).
                 filter_by(name='jack')).one()
            eq_(u.adlist,
                [self.static.user_address_result[0].addresses[0]])
        self.assert_sql_count(testing.db, go, 1)

    def test_eager_options(self):
        """A lazy relationship can be upgraded to an eager relationship."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses),
                                   order_by=addresses.c.id)))

        sess = create_session()
        result = (sess.query(User).
                  order_by(User.id).
                  options(sa.orm.joinedload('addresses'))).all()

        def go():
            eq_(result, self.static.user_address_result)
        self.sql_count_(0, go)

    def test_eager_options_with_limit(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='select')))

        sess = create_session()
        u = (sess.query(User).
             options(sa.orm.joinedload('addresses')).
             filter_by(id=8)).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)
        self.sql_count_(0, go)

        sess.expunge_all()

        u = sess.query(User).filter_by(id=8).one()
        eq_(u.id, 8)
        eq_(len(u.addresses), 3)

    def test_lazy_options_with_limit(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='joined')))

        sess = create_session()
        u = (sess.query(User).
             options(sa.orm.lazyload('addresses')).
             filter_by(id=8)).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)
        self.sql_count_(1, go)

    def test_eager_degrade(self):
        """An eager relationship automatically degrades to a lazy relationship
        if eager columns are not available"""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses),
                                   lazy='joined', order_by=addresses.c.id)))

        sess = create_session()
        # first test straight eager load, 1 statement

        def go():
            result = sess.query(User).order_by(User.id).all()
            eq_(result, self.static.user_address_result)
        self.sql_count_(1, go)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        # (previous users in session fell out of scope and were removed from
        # session's identity map)
        r = users.select().order_by(users.c.id).execute()

        def go():
            result = list(sess.query(User).instances(r))
            eq_(result, self.static.user_address_result)
        self.sql_count_(4, go)

    def test_eager_degrade_deep(self):
        users, Keyword, items, order_items, orders, \
            Item, User, Address, keywords, item_keywords, Order, addresses = (
                self.tables.users,
                self.classes.Keyword,
                self.tables.items,
                self.tables.order_items,
                self.tables.orders,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.tables.keywords,
                self.tables.item_keywords,
                self.classes.Order,
                self.tables.addresses)

        # test with a deeper set of eager loads.  when we first load the three
        # users, they will have no addresses or orders.  the number of lazy
        # loads when traversing the whole thing will be three for the
        # addresses and three for the orders.
        mapper(Address, addresses)

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, secondary=item_keywords,
                                  lazy='joined',
                                  order_by=item_keywords.c.keyword_id)))

        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy='joined',
                               order_by=order_items.c.item_id)))

        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='joined',
                                   order_by=addresses.c.id),
            orders=relationship(Order, lazy='joined',
                                order_by=orders.c.id)))

        sess = create_session()

        # first test straight eager load, 1 statement
        def go():
            result = sess.query(User).order_by(User.id).all()
            eq_(result, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        r = users.select().execute()

        def go():
            result = list(sess.query(User).instances(r))
            eq_(result, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 6)

    def test_lazy_options(self):
        """An eager relationship can be upgraded to a lazy relationship."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='joined')
        ))

        sess = create_session()
        result = (sess.query(User).
                  order_by(User.id).
                  options(sa.orm.lazyload('addresses'))).all()

        def go():
            eq_(result, self.static.user_address_result)
        self.sql_count_(4, go)

    def test_option_propagate(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items)
        ))
        mapper(Item, items)

        sess = create_session()

        oalias = aliased(Order)
        opt1 = sa.orm.joinedload(User.orders, Order.items)
        opt2 = sa.orm.contains_eager(User.orders, Order.items, alias=oalias)
        u1 = sess.query(User).join(oalias, User.orders).\
            options(opt1, opt2).first()
        ustate = attributes.instance_state(u1)
        assert opt1 in ustate.load_options
        assert opt2 not in ustate.load_options


class DeepOptionsTest(_fixtures.FixtureTest):

    @classmethod
    def setup_mappers(cls):
        users, Keyword, items, order_items, Order, Item, User, \
            keywords, item_keywords, orders = (
                cls.tables.users,
                cls.classes.Keyword,
                cls.tables.items,
                cls.tables.order_items,
                cls.classes.Order,
                cls.classes.Item,
                cls.classes.User,
                cls.tables.keywords,
                cls.tables.item_keywords,
                cls.tables.orders)

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, item_keywords,
                                  order_by=item_keywords.c.item_id)))

        mapper(Order, orders, properties=dict(
            items=relationship(Item, order_items,
                               order_by=items.c.id)))

        mapper(User, users, properties=dict(
            orders=relationship(Order, order_by=orders.c.id)))

    def test_deep_options_1(self):
        User = self.classes.User

        sess = create_session()

        # joinedload nothing.
        u = sess.query(User).order_by(User.id).all()

        def go():
            u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(testing.db, go, 3)

    def test_deep_options_2(self):
        """test (joined|subquery)load_all() options"""

        User = self.classes.User

        sess = create_session()

        result = (sess.query(User).
                  order_by(User.id).
                  options(
                      sa.orm.joinedload_all('orders.items.keywords'))).all()

        def go():
            result[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

        sess = create_session()

        result = (sess.query(User).
                  options(
                      sa.orm.subqueryload_all('orders.items.keywords'))).all()

        def go():
            result[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

    def test_deep_options_3(self):
        User = self.classes.User

        sess = create_session()

        # same thing, with separate options calls
        q2 = (sess.query(User).
              order_by(User.id).
              options(sa.orm.joinedload('orders')).
              options(sa.orm.joinedload('orders.items')).
              options(sa.orm.joinedload('orders.items.keywords')))
        u = q2.all()

        def go():
            u[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

    def test_deep_options_4(self):
        Item, User, Order = (self.classes.Item,
                             self.classes.User,
                             self.classes.Order)

        sess = create_session()

        assert_raises_message(
            sa.exc.ArgumentError,
            "Can't find property 'items' on any entity "
            "specified in this Query.",
            sess.query(User).options, sa.orm.joinedload(Order.items))

        # joinedload "keywords" on items.  it will lazy load "orders", then
        # lazy load the "items" on the order, but on "items" it will eager
        # load the "keywords"
        q3 = sess.query(User).order_by(User.id).options(
            sa.orm.joinedload('orders.items.keywords'))
        u = q3.all()

        def go():
            u[0].orders[1].items[0].keywords[1]
        self.sql_count_(2, go)

        sess = create_session()
        q3 = sess.query(User).order_by(User.id).options(
            sa.orm.joinedload(User.orders, Order.items, Item.keywords))
        u = q3.all()

        def go():
            u[0].orders[1].items[0].keywords[1]
        self.sql_count_(2, go)


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
            (synonym, 'name'),
            (composite, DummyComposite, users.c.id, users.c.name),
            (relationship, Address),
            (backref, 'address'),
            (comparable_property, ),
            (dynamic_loader, Address)
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
                return func.foobar(self.__clause_element__()) == \
                    func.foobar(other)
        mapper(
            User, users,
            properties={
                'name': column_property(
                    users.c.name, comparator_factory=MyFactory)})
        self.assert_compile(
            User.name == 'ed',
            "foobar(users.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect()
        )
        self.assert_compile(
            aliased(User).name == 'ed',
            "foobar(users_1.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect())

    def test_synonym(self):
        users, User = self.tables.users, self.classes.User

        from sqlalchemy.orm.properties import ColumnProperty

        class MyFactory(ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) ==\
                    func.foobar(other)

        mapper(User, users, properties={
            'name': synonym('_name', map_column=True,
                            comparator_factory=MyFactory)
        })
        self.assert_compile(
            User.name == 'ed',
            "foobar(users.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect())

        self.assert_compile(
            aliased(User).name == 'ed',
            "foobar(users_1.name) = foobar(:foobar_1)",
            dialect=default.DefaultDialect())

    def test_relationship(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        from sqlalchemy.orm.properties import RelationshipProperty

        # NOTE: this API changed in 0.8, previously __clause_element__()
        # gave the parent selecatable, now it gives the
        # primaryjoin/secondaryjoin
        class MyFactory(RelationshipProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(self._source_selectable().c.user_id) == \
                    func.foobar(other.id)

        class MyFactory2(RelationshipProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return func.foobar(self._source_selectable().c.id) == \
                    func.foobar(other.user_id)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(
                User, comparator_factory=MyFactory,
                backref=backref("addresses", comparator_factory=MyFactory2)
            )
        }
        )

        # these are kind of nonsensical tests.
        self.assert_compile(Address.user == User(id=5),
                            "foobar(addresses.user_id) = foobar(:foobar_1)",
                            dialect=default.DefaultDialect())
        self.assert_compile(User.addresses == Address(id=5, user_id=7),
                            "foobar(users.id) = foobar(:foobar_1)",
                            dialect=default.DefaultDialect())

        self.assert_compile(
            aliased(Address).user == User(id=5),
            "foobar(addresses_1.user_id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect())

        self.assert_compile(
            aliased(User).addresses == Address(id=5, user_id=7),
            "foobar(users_1.id) = foobar(:foobar_1)",
            dialect=default.DefaultDialect())


class SecondaryOptionsTest(fixtures.MappedTest):

    """test that the contains_eager() option doesn't bleed
    into a secondary load."""

    run_inserts = 'once'

    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table("base", metadata,
              Column('id', Integer, primary_key=True),
              Column('type', String(50), nullable=False)
              )
        Table("child1", metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True),
              Column(
                  'child2id', Integer, ForeignKey('child2.id'), nullable=False)
              )
        Table("child2", metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True),
              )
        Table('related', metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True),
              )

    @classmethod
    def setup_mappers(cls):
        child1, child2, base, related = (cls.tables.child1,
                                         cls.tables.child2,
                                         cls.tables.base,
                                         cls.tables.related)

        class Base(cls.Comparable):
            pass

        class Child1(Base):
            pass

        class Child2(Base):
            pass

        class Related(cls.Comparable):
            pass
        mapper(Base, base, polymorphic_on=base.c.type, properties={
            'related': relationship(Related, uselist=False)
        })
        mapper(Child1, child1, inherits=Base,
               polymorphic_identity='child1',
               properties={
                   'child2': relationship(
                       Child2,
                       primaryjoin=child1.c.child2id == base.c.id,
                       foreign_keys=child1.c.child2id)
               })
        mapper(Child2, child2, inherits=Base, polymorphic_identity='child2')
        mapper(Related, related)

    @classmethod
    def insert_data(cls):
        child1, child2, base, related = (cls.tables.child1,
                                         cls.tables.child2,
                                         cls.tables.base,
                                         cls.tables.related)

        base.insert().execute([
            {'id': 1, 'type': 'child1'},
            {'id': 2, 'type': 'child1'},
            {'id': 3, 'type': 'child1'},
            {'id': 4, 'type': 'child2'},
            {'id': 5, 'type': 'child2'},
            {'id': 6, 'type': 'child2'},
        ])
        child2.insert().execute([
            {'id': 4},
            {'id': 5},
            {'id': 6},
        ])
        child1.insert().execute([
            {'id': 1, 'child2id': 4},
            {'id': 2, 'child2id': 5},
            {'id': 3, 'child2id': 6},
        ])
        related.insert().execute([
            {'id': 1},
            {'id': 2},
            {'id': 3},
            {'id': 4},
            {'id': 5},
            {'id': 6},
        ])

    def test_contains_eager(self):
        Child1, Related = self.classes.Child1, self.classes.Related

        sess = create_session()

        child1s = sess.query(Child1).\
            join(Child1.related).\
            options(sa.orm.contains_eager(Child1.related)).\
            order_by(Child1.id)

        def go():
            eq_(
                child1s.all(),
                [
                    Child1(id=1, related=Related(id=1)),
                    Child1(id=2, related=Related(id=2)),
                    Child1(id=3, related=Related(id=3))
                ]
            )
        self.assert_sql_count(testing.db, go, 1)

        c1 = child1s[0]

        self.assert_sql_execution(
            testing.db,
            lambda: c1.child2,
            CompiledSQL(
                "SELECT child2.id AS child2_id, base.id AS base_id, "
                "base.type AS base_type "
                "FROM base JOIN child2 ON base.id = child2.id "
                "WHERE base.id = :param_1",
                {'param_1': 4}
            )
        )

    def test_joinedload_on_other(self):
        Child1, Related = self.classes.Child1, self.classes.Related

        sess = create_session()

        child1s = sess.query(Child1).join(Child1.related).options(
            sa.orm.joinedload(Child1.related)).order_by(Child1.id)

        def go():
            eq_(
                child1s.all(),
                [Child1(id=1, related=Related(id=1)),
                 Child1(id=2, related=Related(id=2)),
                 Child1(id=3, related=Related(id=3))]
            )
        self.assert_sql_count(testing.db, go, 1)

        c1 = child1s[0]

        self.assert_sql_execution(
            testing.db,
            lambda: c1.child2,
            CompiledSQL(
                "SELECT child2.id AS child2_id, base.id AS base_id, "
                "base.type AS base_type "
                "FROM base JOIN child2 ON base.id = child2.id "
                "WHERE base.id = :param_1",

                {'param_1': 4}
            )
        )

    def test_joinedload_on_same(self):
        Child1, Child2, Related = (self.classes.Child1,
                                   self.classes.Child2,
                                   self.classes.Related)

        sess = create_session()

        child1s = sess.query(Child1).join(Child1.related).options(
            sa.orm.joinedload(Child1.child2, Child2.related)
        ).order_by(Child1.id)

        def go():
            eq_(
                child1s.all(),
                [Child1(id=1, related=Related(id=1)),
                 Child1(id=2, related=Related(id=2)),
                 Child1(id=3, related=Related(id=3))]
            )
        self.assert_sql_count(testing.db, go, 4)

        c1 = child1s[0]

        # this *does* joinedload
        self.assert_sql_execution(
            testing.db,
            lambda: c1.child2,
            CompiledSQL(
                "SELECT child2.id AS child2_id, base.id AS base_id, "
                "base.type AS base_type, "
                "related_1.id AS related_1_id FROM base JOIN child2 "
                "ON base.id = child2.id "
                "LEFT OUTER JOIN related AS related_1 "
                "ON base.id = related_1.id WHERE base.id = :param_1",
                {'param_1': 4}
            )
        )


class DeferredPopulationTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("thing", metadata,
              Column(
                  "id", Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column("name", String(20)))

        Table("human", metadata,
              Column(
                  "id", Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column("thing_id", Integer, ForeignKey("thing.id")),
              Column("name", String(20)))

    @classmethod
    def setup_mappers(cls):
        thing, human = cls.tables.thing, cls.tables.human

        class Human(cls.Basic):
            pass

        class Thing(cls.Basic):
            pass

        mapper(Human, human, properties={"thing": relationship(Thing)})
        mapper(Thing, thing, properties={"name": deferred(thing.c.name)})

    @classmethod
    def insert_data(cls):
        thing, human = cls.tables.thing, cls.tables.human

        thing.insert().execute([
            {"id": 1, "name": "Chair"},
        ])

        human.insert().execute([
            {"id": 1, "thing_id": 1, "name": "Clark Kent"},
        ])

    def _test(self, thing):
        assert "name" in attributes.instance_state(thing).dict

    def test_no_previous_query(self):
        Thing = self.classes.Thing

        session = create_session()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_query_twice_with_clear(self):
        Thing = self.classes.Thing

        session = create_session()
        result = session.query(Thing).first()  # noqa
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_query_twice_no_clear(self):
        Thing = self.classes.Thing

        session = create_session()
        result = session.query(Thing).first()    # noqa
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_joinedload_with_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = create_session()
        human = session.query(Human).options(    # noqa
            sa.orm.joinedload("thing")).first()
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_joinedload_no_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = create_session()
        human = session.query(Human).options(    # noqa
            sa.orm.joinedload("thing")).first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_join_with_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = create_session()
        result = session.query(Human).add_entity(  # noqa
            Thing).join("thing").first()
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_join_no_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = create_session()
        result = session.query(Human).add_entity(  # noqa
            Thing).join("thing").first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)


class NoLoadTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    def test_o2m_noload(self):

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        m = mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='noload')
        ))
        q = create_session().query(m)
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x
        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(
            result[0], User,
            {'id': 7, 'addresses': (Address, [])},
        )

    def test_upgrade_o2m_noload_lazyload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        m = mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='noload')
        ))
        q = create_session().query(m).options(sa.orm.lazyload('addresses'))
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x
        self.sql_count_(2, go)

        self.assert_result(
            result[0], User,
            {'id': 7, 'addresses': (Address, [{'id': 1}])},
        )

    def test_m2o_noload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)
        mapper(Address, addresses, properties={
            'user': relationship(User)
        })
        mapper(User, users)
        s = Session()
        a1 = s.query(Address).filter_by(id=1).options(
            sa.orm.noload('user')).first()

        def go():
            eq_(a1.user, None)
        self.sql_count_(0, go)


class RaiseLoadTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    def test_o2m_raiseload_mapper(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='raise')
        ))
        q = create_session().query(User)
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'User.addresses' is not available due to lazy='raise'",
                lambda: x[0].addresses)
            result[0] = x
        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(
            result[0], User,
            {'id': 7},
        )

    def test_o2m_raiseload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address)
        ))
        q = create_session().query(User)
        result = [None]

        def go():
            x = q.options(
                sa.orm.raiseload(User.addresses)).filter(User.id == 7).all()
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'User.addresses' is not available due to lazy='raise'",
                lambda: x[0].addresses)
            result[0] = x
        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(
            result[0], User,
            {'id': 7},
        )

    def test_o2m_raiseload_lazyload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='raise')
        ))
        q = create_session().query(User).options(sa.orm.lazyload('addresses'))
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x
        self.sql_count_(2, go)

        self.assert_result(
            result[0], User,
            {'id': 7, 'addresses': (Address, [{'id': 1}])},
        )

    def test_m2o_raiseload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)
        mapper(Address, addresses, properties={
            'user': relationship(User)
        })
        mapper(User, users)
        s = Session()
        a1 = s.query(Address).filter_by(id=1).options(
            sa.orm.raiseload('user')).first()

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise'",
                lambda: a1.user)

        self.sql_count_(0, go)

    def test_m2o_raise_on_sql_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)
        mapper(Address, addresses, properties={
            'user': relationship(User)
        })
        mapper(User, users)
        s = Session()
        a1 = s.query(Address).filter_by(id=1).options(
            sa.orm.raiseload('user', sql_only=True)).first()

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise_on_sql'",
                lambda: a1.user)

        self.sql_count_(0, go)

        s.close()

        u1 = s.query(User).first()
        a1 = s.query(Address).filter_by(id=1).options(
            sa.orm.raiseload('user', sql_only=True)).first()
        assert 'user' not in a1.__dict__
        is_(a1.user, u1)

    def test_m2o_non_use_get_raise_on_sql_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)
        mapper(Address, addresses, properties={
            'user': relationship(
                User,
                primaryjoin=sa.and_(
                    addresses.c.user_id == users.c.id,
                    users.c.name != None  # noqa
                )
            )
        })
        mapper(User, users)
        s = Session()
        u1 = s.query(User).first()
        a1 = s.query(Address).filter_by(id=1).options(
            sa.orm.raiseload('user', sql_only=True)).first()

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise_on_sql'",
                lambda: a1.user)


class RequirementsTest(fixtures.MappedTest):

    """Tests the contract for user classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table('ht1', metadata,
              Column(
                  'id', Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column('value', String(10)))
        Table('ht2', metadata,
              Column(
                  'id', Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column('ht1_id', Integer, ForeignKey('ht1.id')),
              Column('value', String(10)))
        Table('ht3', metadata,
              Column(
                  'id', Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column('value', String(10)))
        Table('ht4', metadata,
              Column('ht1_id', Integer, ForeignKey('ht1.id'),
                     primary_key=True),
              Column('ht3_id', Integer, ForeignKey('ht3.id'),
                     primary_key=True))
        Table('ht5', metadata,
              Column('ht1_id', Integer, ForeignKey('ht1.id'),
                     primary_key=True))
        Table('ht6', metadata,
              Column('ht1a_id', Integer, ForeignKey('ht1.id'),
                     primary_key=True),
              Column('ht1b_id', Integer, ForeignKey('ht1.id'),
                     primary_key=True),
              Column('value', String(10)))

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

        def __init__(self, value='abc', id=None):
            self.id = id
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

        ht6, ht5, ht4, ht3, ht2, ht1 = (self.tables.ht6,
                                        self.tables.ht5,
                                        self.tables.ht4,
                                        self.tables.ht3,
                                        self.tables.ht2,
                                        self.tables.ht1)

        class H1(self._ValueBase):
            pass

        class H2(self._ValueBase):
            pass

        class H3(self._ValueBase):
            pass

        class H6(self._ValueBase):
            pass

        mapper(H1, ht1, properties={
            'h2s': relationship(H2, backref='h1'),
            'h3s': relationship(H3, secondary=ht4, backref='h1s'),
            'h1s': relationship(H1, secondary=ht5, backref='parent_h1'),
            't6a': relationship(H6, backref='h1a',
                                primaryjoin=ht1.c.id == ht6.c.ht1a_id),
            't6b': relationship(H6, backref='h1b',
                                primaryjoin=ht1.c.id == ht6.c.ht1b_id),
        })
        mapper(H2, ht2)
        mapper(H3, ht3)
        mapper(H6, ht6)

        s = create_session()
        s.add_all([
            H1('abc'),
            H1('def'),
        ])
        h1 = H1('ghi')
        s.add(h1)
        h1.h2s.append(H2('abc'))
        h1.h3s.extend([H3(), H3()])
        h1.h1s.append(H1())

        s.flush()
        eq_(select([func.count('*')]).select_from(ht1).scalar(), 4)

        h6 = H6()
        h6.h1a = h1
        h6.h1b = h1

        h6 = H6()
        h6.h1a = h1
        h6.h1b = x = H1()
        assert x in s

        h6.h1b.h2s.append(H2('def'))

        s.flush()

        h1.h2s.extend([H2('abc'), H2('def')])
        s.flush()

        h1s = s.query(H1).options(sa.orm.joinedload('h2s')).all()
        eq_(len(h1s), 5)

        self.assert_unordered_result(h1s, H1,
                                     {'h2s': []},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'},
                                                   {'value': 'def'},
                                                   {'value': 'abc'}])},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'def'}])})

        h1s = s.query(H1).options(sa.orm.joinedload('h3s')).all()

        eq_(len(h1s), 5)
        h1s = s.query(H1).options(sa.orm.joinedload_all('t6a.h1b'),
                                  sa.orm.joinedload('h2s'),
                                  sa.orm.joinedload_all('h3s.h1s')).all()
        eq_(len(h1s), 5)

    def test_composite_results(self):
        ht2, ht1 = (self.tables.ht2,
                    self.tables.ht1)

        class H1(self._ValueBase):

            def __init__(self, value, id, h2s):
                self.value = value
                self.id = id
                self.h2s = h2s

        class H2(self._ValueBase):

            def __init__(self, value, id):
                self.value = value
                self.id = id

        mapper(H1, ht1, properties={
            'h2s': relationship(H2, backref='h1'),
        })
        mapper(H2, ht2)
        s = Session()
        s.add_all([
            H1('abc', 1, h2s=[
                H2('abc', id=1),
                H2('def', id=2),
                H2('def', id=3),
            ]),
            H1('def', 2, h2s=[
                H2('abc', id=4),
                H2('abc', id=5),
                H2('def', id=6),
            ]),
        ])
        s.commit()
        eq_(
            [(h1.value, h1.id, h2.value, h2.id)
             for h1, h2 in
             s.query(H1, H2).join(H1.h2s).order_by(H1.id, H2.id)],
            [
                ('abc', 1, 'abc', 1),
                ('abc', 1, 'def', 2),
                ('abc', 1, 'def', 3),
                ('def', 2, 'abc', 4),
                ('def', 2, 'abc', 5),
                ('def', 2, 'def', 6),
            ]
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

        mapper(H1, ht1)
        mapper(H2, ht1)

        h1 = H1()
        h1.value = "Asdf"
        h1.value = "asdf asdf"  # ding

        h2 = H2()
        h2.value = "Asdf"
        h2.value = "asdf asdf"  # ding


class IsUserlandTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
              Column('id', Integer, primary_key=True),
              Column('someprop', Integer)
              )

    def _test(self, value, instancelevel=None):
        class Foo(object):
            someprop = value

        m = mapper(Foo, self.tables.foo)
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

        m = mapper(Foo, self.tables.foo)
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
        Table('cartographers', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50)),
              Column('alias', String(50)),
              Column('quip', String(100)))
        Table('maps', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('cart_id', Integer,
                     ForeignKey('cartographers.id')),
              Column('state', String(2)),
              Column('data', sa.Text))

    @classmethod
    def setup_classes(cls):
        class Cartographer(cls.Basic):
            pass

        class Map(cls.Basic):
            pass

    def test_mappish(self):
        maps, Cartographer, cartographers, Map = (self.tables.maps,
                                                  self.classes.Cartographer,
                                                  self.tables.cartographers,
                                                  self.classes.Map)

        mapper(Cartographer, cartographers, properties=dict(
            query=cartographers.c.quip))
        mapper(Map, maps, properties=dict(
            mapper=relationship(Cartographer, backref='maps')))

        c = Cartographer(name='Lenny', alias='The Dude',
                         query='Where be dragons?')
        Map(state='AK', mapper=c)

        sess = create_session()
        sess.add(c)
        sess.flush()
        sess.expunge_all()

        for C, M in ((Cartographer, Map),
                     (sa.orm.aliased(Cartographer), sa.orm.aliased(Map))):
            c1 = (sess.query(C).
                  filter(C.alias == 'The Dude').
                  filter(C.query == 'Where be dragons?')).one()
            sess.query(M).filter(M.mapper == c1).one()

    def test_direct_stateish(self):
        for reserved in (sa.orm.instrumentation.ClassManager.STATE_ATTR,
                         sa.orm.instrumentation.ClassManager.MANAGER_ATTR):
            t = Table('t', sa.MetaData(),
                      Column('id', Integer, primary_key=True,
                             test_needs_autoincrement=True),
                      Column(reserved, Integer))

            class T(object):
                pass
            assert_raises_message(
                KeyError,
                ('%r: requested attribute name conflicts with '
                 'instrumentation attribute of the same name.' % reserved),
                mapper, T, t)

    def test_indirect_stateish(self):
        maps = self.tables.maps

        for reserved in (sa.orm.instrumentation.ClassManager.STATE_ATTR,
                         sa.orm.instrumentation.ClassManager.MANAGER_ATTR):
            class M(object):
                pass

            assert_raises_message(
                KeyError,
                ('requested attribute name conflicts with '
                 'instrumentation attribute of the same name'),
                mapper, M, maps, properties={
                    reserved: maps.c.state})
