"""General mapper operations with an emphasis on selecting/loading."""

from test.lib.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from test.lib import testing
from sqlalchemy import MetaData, Integer, String, ForeignKey, func, util
from test.lib.schema import Table, Column
from sqlalchemy.engine import default
from sqlalchemy.orm import mapper, relationship, backref, \
    create_session, class_mapper, configure_mappers, reconstructor, \
    validates, aliased, defer, deferred, synonym, attributes, \
    column_property, composite, dynamic_loader, \
    comparable_property, Session
from test.lib.testing import eq_, AssertsCompiledSQL
from test.orm import _base, _fixtures
from test.lib.assertsql import CompiledSQL

class MapperTest(_fixtures.FixtureTest):

    @testing.resolve_artifact_names
    def test_prop_shadow(self):
        """A backref name may not shadow an existing property name."""

        mapper(Address, addresses)
        mapper(User, users,
            properties={
            'addresses':relationship(Address, backref='email_address')
        })
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    @testing.resolve_artifact_names
    def test_update_attr_keys(self):
        """test that update()/insert() use the correct key when given InstrumentedAttributes."""

        mapper(User, users, properties={
            'foobar':users.c.name
        })

        users.insert().values({User.foobar:'name1'}).execute()
        eq_(sa.select([User.foobar]).where(User.foobar=='name1').execute().fetchall(), [('name1',)])

        users.update().values({User.foobar:User.foobar + 'foo'}).execute()
        eq_(sa.select([User.foobar]).where(User.foobar=='name1foo').execute().fetchall(), [('name1foo',)])

    @testing.resolve_artifact_names
    def test_utils(self):
        from sqlalchemy.orm.util import _is_mapped_class, _is_aliased_class

        class Foo(object):
            x = "something"
            @property
            def y(self):
                return "somethign else"
        m = mapper(Foo, users)
        a1 = aliased(Foo)

        f = Foo()

        for fn, arg, ret in [
            (_is_mapped_class, Foo.x, False),
            (_is_mapped_class, Foo.y, False),
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



    @testing.resolve_artifact_names
    def test_prop_accessor(self):
        mapper(User, users)
        assert_raises(NotImplementedError,
                          getattr, sa.orm.class_mapper(User), 'properties')


    @testing.resolve_artifact_names
    def test_bad_cascade(self):
        mapper(Address, addresses)
        assert_raises(sa.exc.ArgumentError,
                          relationship, Address, cascade="fake, all, delete-orphan")

    @testing.resolve_artifact_names
    def test_friendly_attribute_str_on_uncompiled_boom(self):
        def boom():
            raise Exception("it broke")
        mapper(User, users, properties={
            'addresses':relationship(boom)
        })

        # test that QueryableAttribute.__str__() doesn't 
        # cause a compile.  
        eq_(str(User.addresses), "User.addresses")

    @testing.resolve_artifact_names
    def test_exceptions_sticky(self):
        """test preservation of mapper compile errors raised during hasattr(),
        as well as for redundant mapper compile calls.  Test that 
        repeated calls don't stack up error messages.

        """

        mapper(Address, addresses, properties={
            'user':relationship(User)
        })

        try:
            hasattr(Address.user, 'property')
        except sa.orm.exc.UnmappedClassError:
            assert util.compat.py32

        for i in range(3):
            assert_raises_message(sa.exc.InvalidRequestError,
                                  "^One or more mappers failed to "
                                  "initialize - can't proceed with "
                                  "initialization of other mappers.  "
                                  "Original exception was: Class "
                                  "'test.orm._fixtures.User' is not mapped$"
                                  , configure_mappers)

    @testing.resolve_artifact_names
    def test_column_prefix(self):
        mapper(User, users, column_prefix='_', properties={
            'user_name': synonym('_name')
        })

        s = create_session()
        u = s.query(User).get(7)
        eq_(u._name, 'jack')
        eq_(u._id,7)
        u2 = s.query(User).filter_by(user_name='jack').one()
        assert u is u2

    @testing.resolve_artifact_names
    def test_no_pks_1(self):
        s = sa.select([users.c.name]).alias('foo')
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    @testing.resolve_artifact_names
    def test_no_pks_2(self):
        s = sa.select([users.c.name]).alias()
        assert_raises(sa.exc.ArgumentError, mapper, User, s)

    @testing.resolve_artifact_names
    def test_reconfigure_on_other_mapper(self):
        """A configure trigger on an already-configured mapper 
        still triggers a check against all mappers."""
        mapper(User, users)
        sa.orm.configure_mappers()
        assert sa.orm.mapperlib._new_mappers is False

        m = mapper(Address, addresses, properties={
                'user': relationship(User, backref="addresses")})

        assert m.configured is False
        assert sa.orm.mapperlib._new_mappers is True
        u = User()
        assert User.addresses
        assert sa.orm.mapperlib._new_mappers is False

    @testing.resolve_artifact_names
    def test_configure_on_session(self):
        m = mapper(User, users)
        session = create_session()
        session.connection(m)

    @testing.resolve_artifact_names
    def test_incomplete_columns(self):
        """Loading from a select which does not contain all columns"""
        mapper(Address, addresses)
        s = create_session()
        a = s.query(Address).from_statement(
            sa.select([addresses.c.id, addresses.c.user_id])).first()
        eq_(a.user_id, 7)
        eq_(a.id, 1)
        # email address auto-defers
        assert 'email_addres' not in a.__dict__
        eq_(a.email_address, 'jack@bean.com')

    @testing.resolve_artifact_names
    def test_column_not_present(self):
        assert_raises_message(sa.exc.ArgumentError,
                              "not represented in the mapper's table",
                              mapper, User, users, properties={'foo'
                              : addresses.c.user_id})

    @testing.resolve_artifact_names
    def test_constructor_exc(self):
        """TypeError is raised for illegal constructor args, 
        whether or not explicit __init__ is present [ticket:908]."""

        class Foo(object):
            def __init__(self):
                pass
        class Bar(object):
            pass

        mapper(Foo, users)
        mapper(Bar, addresses)
        assert_raises(TypeError, Foo, x=5)
        assert_raises(TypeError, Bar, x=5)

    @testing.resolve_artifact_names
    def test_props(self):
        m = mapper(User, users, properties = {
            'addresses' : relationship(mapper(Address, addresses))
        })
        assert User.addresses.property is m.get_property('addresses')

    @testing.resolve_artifact_names
    def test_configure_on_prop_1(self):
        mapper(User, users, properties = {
            'addresses' : relationship(mapper(Address, addresses))
        })
        User.addresses.any(Address.email_address=='foo@bar.com')

    @testing.resolve_artifact_names
    def test_configure_on_prop_2(self):
        mapper(User, users, properties = {
            'addresses' : relationship(mapper(Address, addresses))
        })
        eq_(str(User.id == 3), str(users.c.id==3))

    @testing.resolve_artifact_names
    def test_configure_on_prop_3(self):
        class Foo(User):pass
        mapper(User, users)
        mapper(Foo, addresses, inherits=User)
        assert getattr(Foo().__class__, 'name').impl is not None

    @testing.resolve_artifact_names
    def test_deferred_subclass_attribute_instrument(self):
        class Foo(User):pass
        mapper(User, users)
        configure_mappers()
        mapper(Foo, addresses, inherits=User)
        assert getattr(Foo().__class__, 'name').impl is not None


    @testing.resolve_artifact_names
    def test_configure_on_get_props_1(self):
        m =mapper(User, users)
        assert not m.configured
        assert list(m.iterate_properties)
        assert m.configured

    @testing.resolve_artifact_names
    def test_configure_on_get_props_2(self):
        m= mapper(User, users)
        assert not m.configured
        assert m.get_property('name')
        assert m.configured

    @testing.resolve_artifact_names
    def test_configure_on_get_props_3(self):
        m= mapper(User, users)
        assert not m.configured
        configure_mappers()

        m2 = mapper(Address, addresses, properties={
                                            'user':relationship(User, backref='addresses')
                                        })
        assert m.get_property('addresses')

    @testing.resolve_artifact_names
    def test_add_property(self):
        assert_col = []

        class User(_base.ComparableEntity):
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
                    return col == None
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

    @testing.resolve_artifact_names
    def test_replace_property(self):
        m = mapper(User, users)
        m.add_property('_name',users.c.name)
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

    @testing.resolve_artifact_names
    def test_synonym_replaces_backref(self):
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
            'user':synonym('_user')
        })
        sa.orm.configure_mappers()

        # later, backref sets up the prop
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='_user')
        })

        sess = create_session()
        u1 = sess.query(User).get(7)
        u2 = sess.query(User).get(8)
        # comparaison ops need to work
        a1 = sess.query(Address).filter(Address.user==u1).one()
        eq_(a1.id, 1)
        a1.user = u2
        assert a1.user is u2
        eq_(assert_calls, ["set", "get"])

    @testing.resolve_artifact_names
    def test_self_ref_synonym(self):
        t = Table('nodes', MetaData(),
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')))

        class Node(object):
            pass

        mapper(Node, t, properties={
            '_children':relationship(Node, backref=backref('_parent', remote_side=t.c.id)),
            'children':synonym('_children'),
            'parent':synonym('_parent')
        })

        n1 = Node()
        n2 = Node()
        n1.children.append(n2)
        assert n2.parent is n2._parent is n1
        assert n1.children[0] is n1._children[0] is n2
        eq_(str(Node.parent == n2), ":param_1 = nodes.parent_id")

    @testing.resolve_artifact_names
    def test_illegal_non_primary(self):
        mapper(User, users)
        mapper(Address, addresses)
        mapper(User, users, non_primary=True, properties={
            'addresses':relationship(Address)
        })
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attempting to assign a new relationship 'addresses' "
            "to a non-primary mapper on class 'User'",
            configure_mappers
        )

    @testing.resolve_artifact_names
    def test_illegal_non_primary_2(self):
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Configure a primary mapper first",
            mapper, User, users, non_primary=True)

    @testing.resolve_artifact_names
    def test_illegal_non_primary_3(self):
        class Base(object):
            pass
        class Sub(Base):
            pass
        mapper(Base, users)
        assert_raises_message(sa.exc.InvalidRequestError, 
                "Configure a primary mapper first",
                mapper, Sub, addresses, non_primary=True
            )

    @testing.resolve_artifact_names
    def test_prop_filters(self):
        t = Table('person', MetaData(),
                  Column('id', Integer, primary_key=True,
                                        test_needs_autoincrement=True),
                  Column('type', String(128)),
                  Column('name', String(128)),
                  Column('employee_number', Integer),
                  Column('boss_id', Integer, ForeignKey('person.id')),
                  Column('vendor_id', Integer))

        class Person(object): pass
        class Vendor(Person): pass
        class Employee(Person): pass
        class Manager(Employee): pass
        class Hoho(object): pass
        class Lala(object): pass
        class Fub(object):pass
        class Frob(object):pass
        class HasDef(object):
            def name(self):
                pass
        class Empty(object):pass

        empty = mapper(Empty, t, properties={'empty_id' : t.c.id}, 
                       include_properties=[])
        p_m = mapper(Person, t, polymorphic_on=t.c.type,
                     include_properties=('id', 'type', 'name'))
        e_m = mapper(Employee, inherits=p_m,
                     polymorphic_identity='employee', properties={'boss'
                     : relationship(Manager, backref=backref('peon'),
                     remote_side=t.c.id)},
                     exclude_properties=('vendor_id', ))

        m_m = mapper(Manager, inherits=e_m, polymorphic_identity='manager',
                     include_properties=('id', 'type'))

        v_m = mapper(Vendor, inherits=p_m, polymorphic_identity='vendor',
                     exclude_properties=('boss_id', 'employee_number'))
        h_m = mapper(Hoho, t, include_properties=('id', 'type', 'name'))
        l_m = mapper(Lala, t, exclude_properties=('vendor_id', 'boss_id'),
                     column_prefix="p_")

        hd_m = mapper(HasDef, t, column_prefix="h_")

        fb_m = mapper(Fub, t, include_properties=(t.c.id, t.c.type))
        frb_m = mapper(Frob, t, column_prefix='f_',
                       exclude_properties=(t.c.boss_id,
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
        assert_instrumented(Employee,['boss', 'boss_id', 'employee_number',
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
        # excluding the discriminator column is currently not allowed
        class Foo(Person):
            pass
        assert_props(Empty, ['empty_id'])

        assert_raises(
            sa.exc.InvalidRequestError,
            mapper,
            Foo, inherits=Person, polymorphic_identity='foo',
            exclude_properties=('type', ),
            )
    @testing.resolve_artifact_names
    @testing.provide_metadata
    def test_prop_filters_defaults(self):
        t = Table('t', metadata,
               Column('id', Integer(), primary_key=True, test_needs_autoincrement=True),
               Column('x', Integer(), nullable=False, server_default='0')
              )
        t.create()
        class A(object):
            pass
        mapper(A, t, include_properties=['id'])
        s = Session()
        s.add(A())
        s.commit()


    @testing.resolve_artifact_names
    def test_mapping_to_join_raises(self):
        """Test implicit merging of two cols raises."""

        usersaddresses = sa.join(users, addresses,
                                 users.c.id == addresses.c.user_id)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Implicitly",
            mapper, User, usersaddresses, primary_key=[users.c.id]
        )

    @testing.resolve_artifact_names
    def test_mapping_to_join_explicit_prop(self):
        """Mapping to a join"""

        usersaddresses = sa.join(users, addresses, users.c.id
                                 == addresses.c.user_id)
        mapper(User, usersaddresses, primary_key=[users.c.id],
               properties={'add_id':addresses.c.id}
               )
        l = create_session().query(User).order_by(users.c.id).all()
        eq_(l, self.static.user_result[:3])

    @testing.resolve_artifact_names
    def test_mapping_to_join_exclude_prop(self):
        """Mapping to a join"""

        usersaddresses = sa.join(users, addresses, users.c.id
                                 == addresses.c.user_id)
        mapper(User, usersaddresses, primary_key=[users.c.id],
               exclude_properties=[addresses.c.id]
               )
        l = create_session().query(User).order_by(users.c.id).all()
        eq_(l, self.static.user_result[:3])

    @testing.resolve_artifact_names
    def test_mapping_to_join_no_pk(self):
        m = mapper(Address, 
                    addresses.join(email_bounces), 
                    properties={'id':[addresses.c.id, email_bounces.c.id]}
                )
        configure_mappers()
        assert addresses in m._pks_by_table
        assert email_bounces not in m._pks_by_table

        sess = create_session()
        a = Address(id=10, email_address='e1')
        sess.add(a)
        sess.flush()

        eq_(addresses.count().scalar(), 6)
        eq_(email_bounces.count().scalar(), 5)

    @testing.resolve_artifact_names
    def test_mapping_to_outerjoin(self):
        """Mapping to an outer join with a nullable composite primary key."""


        mapper(User, users.outerjoin(addresses),
               primary_key=[users.c.id, addresses.c.id],
               properties=dict(
            address_id=addresses.c.id))

        session = create_session()
        l = session.query(User).order_by(User.id, User.address_id).all()

        eq_(l, [
            User(id=7, address_id=1),
            User(id=8, address_id=2),
            User(id=8, address_id=3),
            User(id=8, address_id=4),
            User(id=9, address_id=5),
            User(id=10, address_id=None)])

    @testing.resolve_artifact_names
    def test_mapping_to_outerjoin_no_partial_pks(self):
        """test the allow_partial_pks=False flag."""


        mapper(User, users.outerjoin(addresses),
                allow_partial_pks=False,
               primary_key=[users.c.id, addresses.c.id],
               properties=dict(
            address_id=addresses.c.id))

        session = create_session()
        l = session.query(User).order_by(User.id, User.address_id).all()

        eq_(l, [
            User(id=7, address_id=1),
            User(id=8, address_id=2),
            User(id=8, address_id=3),
            User(id=8, address_id=4),
            User(id=9, address_id=5),
            None])

    @testing.resolve_artifact_names
    def test_scalar_pk_arg(self):
        m1 = mapper(Item, items, primary_key=[items.c.id])
        m2 = mapper(Keyword, keywords, primary_key=keywords.c.id)
        m3 = mapper(User, users, primary_key=(users.c.id,))

        assert m1.primary_key[0] is items.c.id
        assert m2.primary_key[0] is keywords.c.id
        assert m3.primary_key[0] is users.c.id


    @testing.resolve_artifact_names
    def test_custom_join(self):
        """select_from totally replace the FROM parameters."""

        mapper(Item, items)

        mapper(Order, orders, properties=dict(
            items=relationship(Item, order_items)))

        mapper(User, users, properties=dict(
            orders=relationship(Order)))

        session = create_session()
        l = (session.query(User).
             select_from(users.join(orders).
                         join(order_items).
                         join(items)).
             filter(items.c.description == 'item 4')).all()

        eq_(l, [self.static.user_result[0]])

    @testing.resolve_artifact_names
    def test_cancel_order_by(self):
        mapper(User, users, order_by=users.c.name.desc())

        assert "order by users.name desc" in str(create_session().query(User).statement).lower()
        assert "order by" not in str(create_session().query(User).order_by(None).statement).lower()
        assert "order by users.name asc" in str(create_session().query(User).order_by(User.name.asc()).statement).lower()

        eq_(
            create_session().query(User).all(),
            [User(id=7, name=u'jack'), User(id=9, name=u'fred'), User(id=8, name=u'ed'), User(id=10, name=u'chuck')]
        )

        eq_(
            create_session().query(User).order_by(User.name).all(),
            [User(id=10, name=u'chuck'), User(id=8, name=u'ed'), User(id=9, name=u'fred'), User(id=7, name=u'jack')]
        )

    # 'Raises a "expression evaluation not supported" error at prepare time
    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_function(self):
        """Mapping to a SELECT statement that has functions in it."""

        s = sa.select([users,
                       (users.c.id * 2).label('concat'),
                       sa.func.count(addresses.c.id).label('count')],
                      users.c.id == addresses.c.user_id,
                      group_by=[c for c in users.c]).alias('myselect')

        mapper(User, s, order_by=s.c.id)
        sess = create_session()
        l = sess.query(User).all()

        for idx, total in enumerate((14, 16)):
            eq_(l[idx].concat, l[idx].id * 2)
            eq_(l[idx].concat, total)

    @testing.resolve_artifact_names
    def test_count(self):
        """The count function on Query."""

        mapper(User, users)

        session = create_session()
        q = session.query(User)

        eq_(q.count(), 4)
        eq_(q.filter(User.id.in_([8,9])).count(), 2)
        eq_(q.filter(users.c.id.in_([8,9])).count(), 2)

        eq_(session.query(User.id).count(), 4)
        eq_(session.query(User.id).filter(User.id.in_((8, 9))).count(), 2)

    @testing.resolve_artifact_names
    def test_many_to_many_count(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords = relationship(Keyword, item_keywords, lazy='select')))

        session = create_session()
        q = (session.query(Item).
             join('keywords').
             distinct().
             filter(Keyword.name == "red"))
        eq_(q.count(), 2)

    @testing.resolve_artifact_names
    def test_override_1(self):
        """Overriding a column raises an error."""
        def go():
            mapper(User, users,
                   properties=dict(
                       name=relationship(mapper(Address, addresses))))

        assert_raises(sa.exc.ArgumentError, go)

    @testing.resolve_artifact_names
    def test_override_2(self):
        """exclude_properties cancels the error."""

        mapper(User, users,
               exclude_properties=['name'],
               properties=dict(
                   name=relationship(mapper(Address, addresses))))

        assert bool(User.name)

    @testing.resolve_artifact_names
    def test_override_3(self):
        """The column being named elsewhere also cancels the error,"""
        mapper(User, users,
               properties=dict(
                   name=relationship(mapper(Address, addresses)),
                   foo=users.c.name))

    @testing.resolve_artifact_names
    def test_synonym(self):

        assert_col = []
        class extendedproperty(property):
            attribute = 123
            def __getitem__(self, key):
                return 'value'

        class User(object):
            def _get_name(self):
                assert_col.append(('get', self.name))
                return self.name
            def _set_name(self, name):
                assert_col.append(('set', name))
                self.name = name
            uname = extendedproperty(_get_name, _set_name)

        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select'),
            uname = synonym('name'),
            adlist = synonym('addresses'),
            adname = synonym('addresses')
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

        u = sess.query(User).filter(User.uname=='jack').one()

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
        eq_(User.uname['key'], 'value')

    @testing.resolve_artifact_names
    def test_synonym_column_location(self):
        def go():
            mapper(User, users, properties={
                'not_name':synonym('_name', map_column=True)})

        assert_raises_message(
            sa.exc.ArgumentError,
            ("Can't compile synonym '_name': no column on table "
             "'users' named 'not_name'"),
            go)

    @testing.resolve_artifact_names
    def test_column_synonyms(self):
        """Synonyms which automatically instrument properties, set up aliased column, etc."""


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
        mapper(User, users, properties = {
            'addresses':relationship(Address, lazy='select'),
            'name':synonym('_name', map_column=True)
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

    @testing.resolve_artifact_names
    def test_synonym_map_column_conflict(self):
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

    @testing.resolve_artifact_names
    def test_comparable(self):
        class extendedproperty(property):
            attribute = 123

            def method1(self):
                return "method1"

            def __getitem__(self, key):
                return 'value'

        class UCComparator(sa.orm.PropComparator):
            __hash__ = None

            def method1(self):
                return "uccmethod1"

            def method2(self, other):
                return "method2"

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, 'name')
                if other is None:
                    return col == None
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
                    uc_name = sa.orm.comparable_property(*args)))
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
                "Neither 'extendedproperty' object nor 'UCComparator' object has an attribute 'nonexistent'", 
                getattr, User.uc_name, 'nonexistent')

            # test compile
            assert not isinstance(User.uc_name == 'jack', bool)
            u = q.filter(User.uc_name=='JACK').one()

            assert u.uc_name == "JACK"
            assert u not in sess.dirty

            u.name = "some user name"
            eq_(u.name, "some user name")
            assert u in sess.dirty
            eq_(u.uc_name, "SOME USER NAME")

            sess.flush()
            sess.expunge_all()

            q = sess.query(User)
            u2 = q.filter(User.name=='some user name').one()
            u3 = q.filter(User.uc_name=='SOME USER NAME').one()

            assert u2 is u3

            eq_(User.uc_name.attribute, 123)
            eq_(User.uc_name['key'], 'value')
            sess.rollback()

    @testing.resolve_artifact_names
    def test_comparable_column(self):
        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None
            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()) == func.lower(other)

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op('&=')(other)

        mapper(User, users, properties={
            'name':sa.orm.column_property(users.c.name, comparator_factory=MyComparator)
        })

        assert_raises_message(
            AttributeError, 
            "Neither 'InstrumentedAttribute' object nor 'MyComparator' object has an attribute 'nonexistent'", 
            getattr, User.name, "nonexistent")

        eq_(str((User.name == 'ed').compile(dialect=sa.engine.default.DefaultDialect())) , "lower(users.name) = lower(:lower_1)")
        eq_(str((User.name.intersects('ed')).compile(dialect=sa.engine.default.DefaultDialect())), "users.name &= :name_1")


    @testing.resolve_artifact_names
    def test_reentrant_compile(self):
        class MyFakeProperty(sa.orm.properties.ColumnProperty):
            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        m1 = mapper(User, users, properties={
            'name':MyFakeProperty(users.c.name)
        })
        m2 = mapper(Address, addresses)
        configure_mappers()

        sa.orm.clear_mappers()
        class MyFakeProperty(sa.orm.properties.ColumnProperty):
            def post_instrument_class(self, mapper):
                super(MyFakeProperty, self).post_instrument_class(mapper)
                configure_mappers()

        m1 = mapper(User, users, properties={
            'name':MyFakeProperty(users.c.name)
        })
        m2 = mapper(Address, addresses)
        configure_mappers()

    @testing.resolve_artifact_names
    def test_reconstructor(self):
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

    @testing.resolve_artifact_names
    def test_reconstructor_inheritance(self):
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

    @testing.resolve_artifact_names
    def test_unmapped_reconstructor_inheritance(self):
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

    @testing.resolve_artifact_names
    def test_unmapped_error(self):
        mapper(Address, addresses)
        sa.orm.clear_mappers()

        mapper(User, users, properties={
            'addresses':relationship(Address)
        })

        assert_raises(sa.orm.exc.UnmappedClassError, sa.orm.configure_mappers)

    @testing.resolve_artifact_names
    def test_unmapped_subclass_error_postmap(self):
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

    @testing.resolve_artifact_names
    def test_unmapped_subclass_error_premap(self):
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

    @testing.resolve_artifact_names
    def test_oldstyle_mixin(self):
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

class DocumentTest(testing.TestBase):

    def test_doc_propagate(self):
        metadata = MetaData()
        t1 = Table('t1', metadata,
            Column('col1', Integer, primary_key=True, doc="primary key column"),
            Column('col2', String, doc="data col"),
            Column('col3', String, doc="data col 2"),
            Column('col4', String, doc="data col 3"),
            Column('col5', String),
        )
        t2 = Table('t2', metadata,
            Column('col1', Integer, primary_key=True, doc="primary key column"),
            Column('col2', String, doc="data col"),
            Column('col3', Integer, ForeignKey('t1.col1'), doc="foreign key to t1.col1")
        )

        class Foo(object):
            pass

        class Bar(object):
            pass

        mapper(Foo, t1, properties={
            'bars':relationship(Bar, 
                                    doc="bar relationship", 
                                    backref=backref('foo',doc='foo relationship')
                                ),
            'foober':column_property(t1.c.col3, doc='alternate data col'),
            'hoho':synonym("col4", doc="syn of col4")
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



class OptionsTest(_fixtures.FixtureTest):

    @testing.fails_if(lambda: True, "0.7 regression, may not support "
                                "synonyms for relationship")
    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_synonym_options(self):
        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select',
                                 order_by=addresses.c.id),
            adlist = synonym('addresses')))

        def go():
            sess = create_session()
            u = (sess.query(User).
                 order_by(User.id).
                 options(sa.orm.joinedload('adlist')).
                 filter_by(name='jack')).one()
            eq_(u.adlist,
                [self.static.user_address_result[0].addresses[0]])
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_eager_options(self):
        """A lazy relationship can be upgraded to an eager relationship."""
        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses),
                                 order_by=addresses.c.id)))

        sess = create_session()
        l = (sess.query(User).
             order_by(User.id).
             options(sa.orm.joinedload('addresses'))).all()

        def go():
            eq_(l, self.static.user_address_result)
        self.sql_count_(0, go)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_eager_options_with_limit(self):
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

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_lazy_options_with_limit(self):
        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='joined')))

        sess = create_session()
        u = (sess.query(User).
             options(sa.orm.lazyload('addresses')).
             filter_by(id=8)).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)
        self.sql_count_(1, go)

    @testing.resolve_artifact_names
    def test_eager_degrade(self):
        """An eager relationship automatically degrades to a lazy relationship 
        if eager columns are not available"""
        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), 
                                lazy='joined', order_by=addresses.c.id)))

        sess = create_session()
        # first test straight eager load, 1 statement
        def go():
            l = sess.query(User).order_by(User.id).all()
            eq_(l, self.static.user_address_result)
        self.sql_count_(1, go)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        # (previous users in session fell out of scope and were removed from
        # session's identity map)
        r = users.select().order_by(users.c.id).execute()
        def go():
            l = list(sess.query(User).instances(r))
            eq_(l, self.static.user_address_result)
        self.sql_count_(4, go)

    @testing.resolve_artifact_names
    def test_eager_degrade_deep(self):
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
            l = sess.query(User).order_by(User.id).all()
            eq_(l, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        r = users.select().execute()
        def go():
            l = list(sess.query(User).instances(r))
            eq_(l, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 6)

    @testing.resolve_artifact_names
    def test_lazy_options(self):
        """An eager relationship can be upgraded to a lazy relationship."""
        mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='joined')
        ))

        sess = create_session()
        l = (sess.query(User).
             order_by(User.id).
             options(sa.orm.lazyload('addresses'))).all()

        def go():
            eq_(l, self.static.user_address_result)
        self.sql_count_(4, go)

    @testing.resolve_artifact_names
    def test_option_propagate(self):
        mapper(User, users, properties=dict(
            orders = relationship(Order)
        ))
        mapper(Order, orders, properties=dict(
            items = relationship(Item, secondary=order_items)
        ))
        mapper(Item, items)

        sess = create_session()

        oalias = aliased(Order)
        opt1 = sa.orm.joinedload(User.orders, Order.items)
        opt2a, opt2b = sa.orm.contains_eager(User.orders, Order.items, alias=oalias)
        u1 = sess.query(User).join(oalias, User.orders).options(opt1, opt2a, opt2b).first()
        ustate = attributes.instance_state(u1)
        assert opt1 in ustate.load_options
        assert opt2a not in ustate.load_options
        assert opt2b not in ustate.load_options

        import pickle
        pickle.dumps(u1)

class DeepOptionsTest(_fixtures.FixtureTest):
    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, item_keywords,
                              order_by=item_keywords.c.item_id)))

        mapper(Order, orders, properties=dict(
            items=relationship(Item, order_items,
                           order_by=items.c.id)))

        mapper(User, users, order_by=users.c.id, properties=dict(
            orders=relationship(Order, order_by=orders.c.id)))

    @testing.resolve_artifact_names
    def test_deep_options_1(self):
        sess = create_session()

        # joinedload nothing.
        u = sess.query(User).all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(testing.db, go, 3)

    @testing.resolve_artifact_names
    def test_deep_options_2(self):
        """test (joined|subquery)load_all() options"""

        sess = create_session()

        l = (sess.query(User).
              options(sa.orm.joinedload_all('orders.items.keywords'))).all()
        def go():
            x = l[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

        sess = create_session()

        l = (sess.query(User).
              options(sa.orm.subqueryload_all('orders.items.keywords'))).all()
        def go():
            x = l[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)


    @testing.resolve_artifact_names
    def test_deep_options_3(self):
        sess = create_session()

        # same thing, with separate options calls
        q2 = (sess.query(User).
              options(sa.orm.joinedload('orders')).
              options(sa.orm.joinedload('orders.items')).
              options(sa.orm.joinedload('orders.items.keywords')))
        u = q2.all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_deep_options_4(self):
        sess = create_session()

        assert_raises_message(
            sa.exc.ArgumentError,
            "Can't find property 'items' on any entity "
            "specified in this Query.",
            sess.query(User).options, sa.orm.joinedload(Order.items))

        # joinedload "keywords" on items.  it will lazy load "orders", then
        # lazy load the "items" on the order, but on "items" it will eager
        # load the "keywords"
        q3 = sess.query(User).options(sa.orm.joinedload('orders.items.keywords'))
        u = q3.all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.sql_count_(2, go)

        sess = create_session()
        q3 = sess.query(User).options(
                    sa.orm.joinedload(User.orders, Order.items, Item.keywords))
        u = q3.all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.sql_count_(2, go)

class ValidatorTest(_fixtures.FixtureTest):
    @testing.resolve_artifact_names
    def test_scalar(self):
        class User(_base.ComparableEntity):
            @validates('name')
            def validate_name(self, key, name):
                assert name != 'fred'
                return name + ' modified'

        mapper(User, users)
        sess = create_session()
        u1 = User(name='ed')
        eq_(u1.name, 'ed modified')
        assert_raises(AssertionError, setattr, u1, "name", "fred")
        eq_(u1.name, 'ed modified')
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).filter_by(name='ed modified').one(), User(name='ed'))


    @testing.resolve_artifact_names
    def test_collection(self):
        class User(_base.ComparableEntity):
            @validates('addresses')
            def validate_address(self, key, ad):
                assert '@' in ad.email_address
                return ad

        mapper(User, users, properties={'addresses':relationship(Address)})
        mapper(Address, addresses)
        sess = create_session()
        u1 = User(name='edward')
        assert_raises(AssertionError, u1.addresses.append, Address(email_address='noemail'))
        u1.addresses.append(Address(id=15, email_address='foo@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).filter_by(name='edward').one(), 
            User(name='edward', addresses=[Address(email_address='foo@bar.com')])
        )

class ComparatorFactoryTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    @testing.resolve_artifact_names
    def test_kwarg_accepted(self):
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

    @testing.resolve_artifact_names
    def test_column(self):
        from sqlalchemy.orm.properties import ColumnProperty

        class MyFactory(ColumnProperty.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) == func.foobar(other)
        mapper(User, users, properties={'name':column_property(users.c.name, comparator_factory=MyFactory)})
        self.assert_compile(User.name == 'ed', "foobar(users.name) = foobar(:foobar_1)", dialect=default.DefaultDialect())
        self.assert_compile(aliased(User).name == 'ed', "foobar(users_1.name) = foobar(:foobar_1)", dialect=default.DefaultDialect())

    @testing.resolve_artifact_names
    def test_synonym(self):
        from sqlalchemy.orm.properties import ColumnProperty
        class MyFactory(ColumnProperty.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return func.foobar(self.__clause_element__()) ==\
                            func.foobar(other)

        mapper(User, users, properties={
                    'name':synonym('_name', map_column=True,
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

    @testing.resolve_artifact_names
    def test_relationship(self):
        from sqlalchemy.orm.properties import PropertyLoader

        class MyFactory(PropertyLoader.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return func.foobar(self.__clause_element__().c.user_id) == func.foobar(other.id)

        class MyFactory2(PropertyLoader.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return func.foobar(self.__clause_element__().c.id) == func.foobar(other.user_id)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User, comparator_factory=MyFactory, 
                backref=backref("addresses", comparator_factory=MyFactory2)
            )
            }
        )
        self.assert_compile(Address.user == User(id=5), "foobar(addresses.user_id) = foobar(:foobar_1)", dialect=default.DefaultDialect())
        self.assert_compile(User.addresses == Address(id=5, user_id=7), "foobar(users.id) = foobar(:foobar_1)", dialect=default.DefaultDialect())

        self.assert_compile(aliased(Address).user == User(id=5), "foobar(addresses_1.user_id) = foobar(:foobar_1)", dialect=default.DefaultDialect())
        self.assert_compile(aliased(User).addresses == Address(id=5, user_id=7), "foobar(users_1.id) = foobar(:foobar_1)", dialect=default.DefaultDialect())


class DeferredTest(_fixtures.FixtureTest):

    @testing.resolve_artifact_names
    def test_basic(self):
        """A basic deferred load."""

        mapper(Order, orders, order_by=orders.c.id, properties={
            'description': deferred(orders.c.description)})

        o = Order()
        self.assert_(o.description is None)

        q = create_session().query(Order)
        def go():
            l = q.all()
            o2 = l[2]
            x = o2.description

        self.sql_eq_(go, [
            ("SELECT orders.id AS orders_id, "
             "orders.user_id AS orders_user_id, "
             "orders.address_id AS orders_address_id, "
             "orders.isopen AS orders_isopen "
             "FROM orders ORDER BY orders.id", {}),
            ("SELECT orders.description AS orders_description "
             "FROM orders WHERE orders.id = :param_1",
             {'param_1':3})])

    @testing.resolve_artifact_names
    def test_unsaved(self):
        """Deferred loading does not kick in when just PK cols are set."""

        mapper(Order, orders, properties={
            'description': deferred(orders.c.description)})

        sess = create_session()
        o = Order()
        sess.add(o)
        o.id = 7
        def go():
            o.description = "some description"
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_synonym_group_bug(self):
        mapper(Order, orders, properties={
            'isopen':synonym('_isopen', map_column=True),
            'description':deferred(orders.c.description, group='foo')
        })

        sess = create_session()
        o1 = sess.query(Order).get(1)
        eq_(o1.description, "order 1")

    @testing.resolve_artifact_names
    def test_unsaved_2(self):
        mapper(Order, orders, properties={
            'description': deferred(orders.c.description)})

        sess = create_session()
        o = Order()
        sess.add(o)
        def go():
            o.description = "some description"
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_unsaved_group(self):
        """Deferred loading doesnt kick in when just PK cols are set"""

        mapper(Order, orders, order_by=orders.c.id, properties=dict(
            description=deferred(orders.c.description, group='primary'),
            opened=deferred(orders.c.isopen, group='primary')))

        sess = create_session()
        o = Order()
        sess.add(o)
        o.id = 7
        def go():
            o.description = "some description"
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_unsaved_group_2(self):
        mapper(Order, orders, order_by=orders.c.id, properties=dict(
            description=deferred(orders.c.description, group='primary'),
            opened=deferred(orders.c.isopen, group='primary')))

        sess = create_session()
        o = Order()
        sess.add(o)
        def go():
            o.description = "some description"
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_save(self):
        m = mapper(Order, orders, properties={
            'description': deferred(orders.c.description)})

        sess = create_session()
        o2 = sess.query(Order).get(2)
        o2.isopen = 1
        sess.flush()

    @testing.resolve_artifact_names
    def test_group(self):
        """Deferred load with a group"""
        mapper(Order, orders, properties=util.OrderedDict([
            ('userident', deferred(orders.c.user_id, group='primary')),
            ('addrident', deferred(orders.c.address_id, group='primary')),
            ('description', deferred(orders.c.description, group='primary')),
            ('opened', deferred(orders.c.isopen, group='primary'))
        ]))

        sess = create_session()
        q = sess.query(Order).order_by(Order.id)
        def go():
            l = q.all()
            o2 = l[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, 'order 3')

        self.sql_eq_(go, [
            ("SELECT orders.id AS orders_id "
             "FROM orders ORDER BY orders.id", {}),
            ("SELECT orders.user_id AS orders_user_id, "
             "orders.address_id AS orders_address_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen "
             "FROM orders WHERE orders.id = :param_1",
             {'param_1':3})])

        o2 = q.all()[2]
        eq_(o2.description, 'order 3')
        assert o2 not in sess.dirty
        o2.description = 'order 3'
        def go():
            sess.flush()
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_preserve_changes(self):
        """A deferred load operation doesn't revert modifications on attributes"""
        mapper(Order, orders, properties = {
            'userident': deferred(orders.c.user_id, group='primary'),
            'description': deferred(orders.c.description, group='primary'),
            'opened': deferred(orders.c.isopen, group='primary')
        })
        sess = create_session()
        o = sess.query(Order).get(3)
        assert 'userident' not in o.__dict__
        o.description = 'somenewdescription'
        eq_(o.description, 'somenewdescription')
        def go():
            eq_(o.opened, 1)
        self.assert_sql_count(testing.db, go, 1)
        eq_(o.description, 'somenewdescription')
        assert o in sess.dirty

    @testing.resolve_artifact_names
    def test_commits_state(self):
        """
        When deferred elements are loaded via a group, they get the proper
        CommittedState and don't result in changes being committed

        """
        mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')})

        sess = create_session()
        o2 = sess.query(Order).get(3)

        # this will load the group of attributes
        eq_(o2.description, 'order 3')
        assert o2 not in sess.dirty
        # this will mark it as 'dirty', but nothing actually changed
        o2.description = 'order 3'
        # therefore the flush() shouldnt actually issue any SQL
        self.assert_sql_count(testing.db, sess.flush, 0)

    @testing.resolve_artifact_names
    def test_options(self):
        """Options on a mapper to create deferred and undeferred columns"""

        mapper(Order, orders)

        sess = create_session()
        q = sess.query(Order).order_by(Order.id).options(defer('user_id'))

        def go():
            q.all()[0].user_id

        self.sql_eq_(go, [
            ("SELECT orders.id AS orders_id, "
             "orders.address_id AS orders_address_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen "
             "FROM orders ORDER BY orders.id", {}),
            ("SELECT orders.user_id AS orders_user_id "
             "FROM orders WHERE orders.id = :param_1",
             {'param_1':1})])
        sess.expunge_all()

        q2 = q.options(sa.orm.undefer('user_id'))
        self.sql_eq_(q2.all, [
            ("SELECT orders.id AS orders_id, "
             "orders.user_id AS orders_user_id, "
             "orders.address_id AS orders_address_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen "
             "FROM orders ORDER BY orders.id",
             {})])

    @testing.resolve_artifact_names
    def test_undefer_group(self):
        mapper(Order, orders, properties=util.OrderedDict([
            ('userident',deferred(orders.c.user_id, group='primary')),
            ('description',deferred(orders.c.description, group='primary')),
            ('opened',deferred(orders.c.isopen, group='primary'))
            ]
            ))

        sess = create_session()
        q = sess.query(Order).order_by(Order.id)
        def go():
            l = q.options(sa.orm.undefer_group('primary')).all()
            o2 = l[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, 'order 3')

        self.sql_eq_(go, [
            ("SELECT orders.user_id AS orders_user_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen, "
             "orders.id AS orders_id, "
             "orders.address_id AS orders_address_id "
             "FROM orders ORDER BY orders.id",
             {})])

    @testing.resolve_artifact_names
    def test_locates_col(self):
        """Manually adding a column to the result undefers the column."""

        mapper(Order, orders, properties={
            'description':deferred(orders.c.description)})

        sess = create_session()
        o1 = sess.query(Order).order_by(Order.id).first()
        def go():
            eq_(o1.description, 'order 1')
        self.sql_count_(1, go)

        sess = create_session()
        o1 = (sess.query(Order).
              order_by(Order.id).
              add_column(orders.c.description).first())[0]
        def go():
            eq_(o1.description, 'order 1')
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_map_selectable_wo_deferred(self):
        """test mapping to a selectable with deferred cols,
        the selectable doesn't include the deferred col.
        
        """

        order_select = sa.select([
                        orders.c.id, 
                        orders.c.user_id, 
                        orders.c.address_id, 
                        orders.c.description,
                        orders.c.isopen]).alias()
        mapper(Order, order_select, properties={
            'description':deferred(order_select.c.description)
        })

        sess = Session()
        o1 = sess.query(Order).order_by(Order.id).first()
        assert 'description' not in o1.__dict__
        eq_(o1.description, 'order 1')

    @testing.resolve_artifact_names
    def test_deep_options(self):
        mapper(Item, items, properties=dict(
            description=deferred(items.c.description)))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items)))
        mapper(User, users, properties=dict(
            orders=relationship(Order, order_by=orders.c.id)))

        sess = create_session()
        q = sess.query(User).order_by(User.id)
        l = q.all()
        item = l[0].orders[1].items[1]
        def go():
            eq_(item.description, 'item 4')
        self.sql_count_(1, go)
        eq_(item.description, 'item 4')

        sess.expunge_all()
        l = q.options(sa.orm.undefer('orders.items.description')).all()
        item = l[0].orders[1].items[1]
        def go():
            eq_(item.description, 'item 4')
        self.sql_count_(0, go)
        eq_(item.description, 'item 4')


class SecondaryOptionsTest(_base.MappedTest):
    """test that the contains_eager() option doesn't bleed into a secondary load."""

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
            Column('child2id', Integer, ForeignKey('child2.id'), nullable=False)
        )
        Table("child2", metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
        )
        Table('related', metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Base(_base.ComparableEntity):
            pass
        class Child1(Base):
            pass
        class Child2(Base):
            pass
        class Related(_base.ComparableEntity):
            pass
        mapper(Base, base, polymorphic_on=base.c.type, properties={
            'related':relationship(Related, uselist=False)
        })
        mapper(Child1, child1, inherits=Base, 
                polymorphic_identity='child1', 
            properties={
            'child2':relationship(Child2, 
                                    primaryjoin=child1.c.child2id==base.c.id, 
                                    foreign_keys=child1.c.child2id)
        })
        mapper(Child2, child2, inherits=Base, polymorphic_identity='child2')
        mapper(Related, related)

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        base.insert().execute([
            {'id':1, 'type':'child1'},
            {'id':2, 'type':'child1'},
            {'id':3, 'type':'child1'},
            {'id':4, 'type':'child2'},
            {'id':5, 'type':'child2'},
            {'id':6, 'type':'child2'},
        ])
        child2.insert().execute([
            {'id':4},
            {'id':5},
            {'id':6},
        ])
        child1.insert().execute([
            {'id':1, 'child2id':4},
            {'id':2, 'child2id':5},
            {'id':3, 'child2id':6},
        ])
        related.insert().execute([
            {'id':1},
            {'id':2},
            {'id':3},
            {'id':4},
            {'id':5},
            {'id':6},
        ])

    @testing.resolve_artifact_names
    def test_contains_eager(self):
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
                "SELECT child2.id AS child2_id, base.id AS base_id, base.type AS base_type "
                "FROM base JOIN child2 ON base.id = child2.id "
                "WHERE base.id = :param_1",
                {'param_1':4}
            )
        )

    @testing.resolve_artifact_names
    def test_joinedload_on_other(self):
        sess = create_session()

        child1s = sess.query(Child1).join(Child1.related).options(sa.orm.joinedload(Child1.related)).order_by(Child1.id)

        def go():
            eq_(
                child1s.all(),
                [Child1(id=1, related=Related(id=1)), Child1(id=2, related=Related(id=2)), Child1(id=3, related=Related(id=3))]
            )
        self.assert_sql_count(testing.db, go, 1)

        c1 = child1s[0]

        self.assert_sql_execution(
            testing.db, 
            lambda: c1.child2, 
            CompiledSQL(
            "SELECT child2.id AS child2_id, base.id AS base_id, base.type AS base_type "
            "FROM base JOIN child2 ON base.id = child2.id WHERE base.id = :param_1",

#   joinedload- this shouldn't happen
#            "SELECT base.id AS base_id, child2.id AS child2_id, base.type AS base_type, "
#            "related_1.id AS related_1_id FROM base JOIN child2 ON base.id = child2.id "
#            "LEFT OUTER JOIN related AS related_1 ON base.id = related_1.id WHERE base.id = :param_1",
                {'param_1':4}
            )
        )

    @testing.resolve_artifact_names
    def test_joinedload_on_same(self):
        sess = create_session()

        child1s = sess.query(Child1).join(Child1.related).options(sa.orm.joinedload(Child1.child2, Child2.related)).order_by(Child1.id)

        def go():
            eq_(
                child1s.all(),
                [Child1(id=1, related=Related(id=1)), Child1(id=2, related=Related(id=2)), Child1(id=3, related=Related(id=3))]
            )
        self.assert_sql_count(testing.db, go, 4)

        c1 = child1s[0]

        # this *does* joinedload
        self.assert_sql_execution(
            testing.db, 
            lambda: c1.child2, 
            CompiledSQL(
                "SELECT child2.id AS child2_id, base.id AS base_id, base.type AS base_type, "
                "related_1.id AS related_1_id FROM base JOIN child2 ON base.id = child2.id "
                "LEFT OUTER JOIN related AS related_1 ON base.id = related_1.id WHERE base.id = :param_1",
                {'param_1':4}
            )
        )


class DeferredPopulationTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("thing", metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("name", String(20)))

        Table("human", metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("thing_id", Integer, ForeignKey("thing.id")),
            Column("name", String(20)))

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Human(_base.BasicEntity): pass
        class Thing(_base.BasicEntity): pass

        mapper(Human, human, properties={"thing": relationship(Thing)})
        mapper(Thing, thing, properties={"name": deferred(thing.c.name)})

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        thing.insert().execute([
            {"id": 1, "name": "Chair"},
        ])

        human.insert().execute([
            {"id": 1, "thing_id": 1, "name": "Clark Kent"},
        ])

    def _test(self, thing):
        assert "name" in attributes.instance_state(thing).dict

    @testing.resolve_artifact_names
    def test_no_previous_query(self):
        session = create_session()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_query_twice_with_clear(self):
        session = create_session()
        result = session.query(Thing).first()
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_query_twice_no_clear(self):
        session = create_session()
        result = session.query(Thing).first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_joinedload_with_clear(self):
        session = create_session()
        human = session.query(Human).options(sa.orm.joinedload("thing")).first()
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_joinedload_no_clear(self):
        session = create_session()
        human = session.query(Human).options(sa.orm.joinedload("thing")).first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_join_with_clear(self):
        session = create_session()
        result = session.query(Human).add_entity(Thing).join("thing").first()
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_join_no_clear(self):
        session = create_session()
        result = session.query(Human).add_entity(Thing).join("thing").first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)




class NoLoadTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @testing.resolve_artifact_names
    def test_basic(self):
        """A basic one-to-many lazy load"""
        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='noload')
        ))
        q = create_session().query(m)
        l = [None]
        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            l[0] = x
        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(l[0], User,
            {'id' : 7, 'addresses' : (Address, [])},
            )

    @testing.resolve_artifact_names
    def test_options(self):
        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='noload')
        ))
        q = create_session().query(m).options(sa.orm.lazyload('addresses'))
        l = [None]
        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            l[0] = x
        self.sql_count_(2, go)

        self.assert_result(l[0], User,
            {'id' : 7, 'addresses' : (Address, [{'id' : 1}])},
            )





class RequirementsTest(_base.MappedTest):
    """Tests the contract for user classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table('ht1', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('value', String(10)))
        Table('ht2', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('ht1_id', Integer, ForeignKey('ht1.id')),
              Column('value', String(10)))
        Table('ht3', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('value', String(10)))
        Table('ht4', metadata,
              Column('ht1_id', Integer, ForeignKey('ht1.id'), primary_key=True),
              Column('ht3_id', Integer, ForeignKey('ht3.id'), primary_key=True))
        Table('ht5', metadata,
              Column('ht1_id', Integer, ForeignKey('ht1.id'), primary_key=True))
        Table('ht6', metadata,
              Column('ht1a_id', Integer, ForeignKey('ht1.id'), primary_key=True),
              Column('ht1b_id', Integer, ForeignKey('ht1.id'), primary_key=True),
              Column('value', String(10)))

    # Py2K
    @testing.resolve_artifact_names
    def test_baseclass(self):
        class OldStyle:
            pass

        assert_raises(sa.exc.ArgumentError, mapper, OldStyle, ht1)

        assert_raises(sa.exc.ArgumentError, mapper, 123)

        class NoWeakrefSupport(str):
            pass

        # TODO: is weakref support detectable without an instance?
        #self.assertRaises(sa.exc.ArgumentError, mapper, NoWeakrefSupport, t2)
    # end Py2K

    @testing.resolve_artifact_names
    def test_comparison_overrides(self):
        """Simple tests to ensure users can supply comparison __methods__.

        The suite-level test --options are better suited to detect
        problems- they add selected __methods__ across the board on all
        ORM tests.  This test simply shoves a variety of operations
        through the ORM to catch basic regressions early in a standard
        test run.
        """

        # adding these methods directly to each class to avoid decoration
        # by the testlib decorators.
        class _Base(object):
            def __init__(self, value='abc'):
                self.value = value
            def __nonzero__(self):
                return False
            def __hash__(self):
                return hash(self.value)
            def __eq__(self, other):
                if isinstance(other, type(self)):
                    return self.value == other.value
                return False

        class H1(_Base):
            pass
        class H2(_Base):
            pass
        class H3(_Base):
            pass
        class H6(_Base):
            pass

        mapper(H1, ht1, properties={
            'h2s': relationship(H2, backref='h1'),
            'h3s': relationship(H3, secondary=ht4, backref='h1s'),
            'h1s': relationship(H1, secondary=ht5, backref='parent_h1'),
            't6a': relationship(H6, backref='h1a',
                                primaryjoin=ht1.c.id==ht6.c.ht1a_id),
            't6b': relationship(H6, backref='h1b',
                                primaryjoin=ht1.c.id==ht6.c.ht1b_id),
            })
        mapper(H2, ht2)
        mapper(H3, ht3)
        mapper(H6, ht6)

        s = create_session()
        for i in range(3):
            h1 = H1()
            s.add(h1)

        h1.h2s.append(H2())
        h1.h3s.extend([H3(), H3()])
        h1.h1s.append(H1())

        s.flush()
        eq_(ht1.count().scalar(), 4)

        h6 = H6()
        h6.h1a = h1
        h6.h1b = h1

        h6 = H6()
        h6.h1a = h1
        h6.h1b = x = H1()
        assert x in s

        h6.h1b.h2s.append(H2())

        s.flush()

        h1.h2s.extend([H2(), H2()])
        s.flush()

        h1s = s.query(H1).options(sa.orm.joinedload('h2s')).all()
        eq_(len(h1s), 5)

        self.assert_unordered_result(h1s, H1,
                                     {'h2s': []},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'},
                                                   {'value': 'abc'},
                                                   {'value': 'abc'}])},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'}])})

        h1s = s.query(H1).options(sa.orm.joinedload('h3s')).all()

        eq_(len(h1s), 5)
        h1s = s.query(H1).options(sa.orm.joinedload_all('t6a.h1b'),
                                  sa.orm.joinedload('h2s'),
                                  sa.orm.joinedload_all('h3s.h1s')).all()
        eq_(len(h1s), 5)

    @testing.resolve_artifact_names
    def test_nonzero_len_recursion(self):
        class H1(object):
            def __len__(self):
                return len(self.get_value())

            def get_value(self):
                self.value = "foobar"
                return self.value

        class H2(object):
            def __nonzero__(self):
                return bool(self.get_value())

            def get_value(self):
                self.value = "foobar"
                return self.value

        mapper(H1, ht1)
        mapper(H2, ht1)

        h1 = H1()
        h1.value = "Asdf"
        h1.value = "asdf asdf" # ding

        h2 = H2()
        h2.value = "Asdf"
        h2.value = "asdf asdf" # ding

class MagicNamesTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('cartographers', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(50)),
              Column('alias', String(50)),
              Column('quip', String(100)))
        Table('maps', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('cart_id', Integer,
                     ForeignKey('cartographers.id')),
              Column('state', String(2)),
              Column('data', sa.Text))

    @classmethod
    def setup_classes(cls):
        class Cartographer(_base.BasicEntity):
            pass

        class Map(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_mappish(self):
        mapper(Cartographer, cartographers, properties=dict(
            query=cartographers.c.quip))
        mapper(Map, maps, properties=dict(
            mapper=relationship(Cartographer, backref='maps')))

        c = Cartographer(name='Lenny', alias='The Dude',
                         query='Where be dragons?')
        m = Map(state='AK', mapper=c)

        sess = create_session()
        sess.add(c)
        sess.flush()
        sess.expunge_all()

        for C, M in ((Cartographer, Map),
                     (sa.orm.aliased(Cartographer), sa.orm.aliased(Map))):
            c1 = (sess.query(C).
                  filter(C.alias=='The Dude').
                  filter(C.query=='Where be dragons?')).one()
            m1 = sess.query(M).filter(M.mapper==c1).one()

    @testing.resolve_artifact_names
    def test_direct_stateish(self):
        for reserved in (sa.orm.instrumentation.ClassManager.STATE_ATTR,
                         sa.orm.instrumentation.ClassManager.MANAGER_ATTR):
            t = Table('t', sa.MetaData(),
                      Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                      Column(reserved, Integer))
            class T(object):
                pass

            assert_raises_message(
                KeyError,
                ('%r: requested attribute name conflicts with '
                 'instrumentation attribute of the same name.' % reserved),
                mapper, T, t)

    @testing.resolve_artifact_names
    def test_indirect_stateish(self):
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



