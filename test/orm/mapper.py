"""General mapper operations with an emphasis on selecting/loading."""

import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import MetaData, Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, backref, create_session, class_mapper, reconstructor, validates
from testlib.sa.orm import defer, deferred, synonym, attributes
from testlib.testing import eq_
import pickleable
from orm import _base, _fixtures


class MapperTest(_fixtures.FixtureTest):

    @testing.resolve_artifact_names
    def test_prop_shadow(self):
        """A backref name may not shadow an existing property name."""

        mapper(Address, addresses)
        mapper(User, users,
            properties={
            'addresses':relation(Address, backref='email_address')
        })
        self.assertRaises(sa.exc.ArgumentError, sa.orm.compile_mappers)

    @testing.resolve_artifact_names
    def test_prop_accessor(self):
        mapper(User, users)
        self.assertRaises(NotImplementedError,
                          getattr, sa.orm.class_mapper(User), 'properties')


    @testing.resolve_artifact_names
    def test_bad_cascade(self):
        mapper(Address, addresses)
        self.assertRaises(sa.exc.ArgumentError,
                          relation, Address, cascade="fake, all, delete-orphan")

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
        self.assertRaises(sa.exc.ArgumentError, mapper, User, s)

    @testing.emits_warning(
        'mapper Mapper|User|Select object creating an alias for '
        'the given selectable - use Class attributes for queries')
    @testing.resolve_artifact_names
    def test_no_pks_2(self):
        s = sa.select([users.c.name])
        self.assertRaises(sa.exc.ArgumentError, mapper, User, s)

    @testing.resolve_artifact_names
    def test_recompile_on_other_mapper(self):
        """A compile trigger on an already-compiled mapper still triggers a check against all mappers."""
        mapper(User, users)
        sa.orm.compile_mappers()
        assert sa.orm.mapperlib._new_mappers is False

        m = mapper(Address, addresses, properties={
                'user': relation(User, backref="addresses")})

        assert m.compiled is False
        assert sa.orm.mapperlib._new_mappers is True
        u = User()
        assert User.addresses
        assert sa.orm.mapperlib._new_mappers is False

    @testing.resolve_artifact_names
    def test_compile_on_session(self):
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
    def test_bad_constructor(self):
        """If the construction of a mapped class fails, the instance does not get placed in the session"""
        class Foo(object):
            def __init__(self, one, two, _sa_session=None):
                pass

        mapper(Foo, users, extension=sa.orm.scoped_session(
            create_session).extension)

        sess = create_session()
        self.assertRaises(TypeError, Foo, 'one', _sa_session=sess)
        eq_(len(list(sess)), 0)
        self.assertRaises(TypeError, Foo, 'one')
        Foo('one', 'two', _sa_session=sess)
        eq_(len(list(sess)), 1)

    @testing.resolve_artifact_names
    def test_constructor_exc_1(self):
        """Exceptions raised in the mapped class are not masked by sa decorations"""
        ex = AssertionError('oops')
        sess = create_session()

        class Foo(object):
            def __init__(self, **kw):
                raise ex
        mapper(Foo, users)

        try:
            Foo()
            assert False
        except Exception, e:
            assert e is ex

        sa.orm.clear_mappers()
        mapper(Foo, users, extension=sa.orm.scoped_session(
            create_session).extension)
        def bad_expunge(foo):
            raise Exception("this exception should be stated as a warning")

        sess.expunge = bad_expunge
        self.assertRaises(sa.exc.SAWarning, Foo, _sa_session=sess)

    @testing.resolve_artifact_names
    def test_constructor_exc_2(self):
        """TypeError is raised for illegal constructor args, whether or not explicit __init__ is present [ticket:908]."""

        class Foo(object):
            def __init__(self):
                pass
        class Bar(object):
            pass

        mapper(Foo, users)
        mapper(Bar, addresses)
        self.assertRaises(TypeError, Foo, x=5)
        self.assertRaises(TypeError, Bar, x=5)

    @testing.resolve_artifact_names
    def test_props(self):
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        }).compile()
        assert User.addresses.property is m.get_property('addresses')

    @testing.resolve_artifact_names
    def test_compile_on_prop_1(self):
        mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        User.addresses.any(Address.email_address=='foo@bar.com')

    @testing.resolve_artifact_names
    def test_compile_on_prop_2(self):
        mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        eq_(str(User.id == 3), str(users.c.id==3))

    @testing.resolve_artifact_names
    def test_compile_on_prop_3(self):
        class Foo(User):pass
        mapper(User, users)
        mapper(Foo, addresses, inherits=User)
        assert getattr(Foo().__class__, 'name').impl is not None

    @testing.resolve_artifact_names
    def test_compile_on_get_props_1(self):
        m =mapper(User, users)
        assert not m.compiled
        assert list(m.iterate_properties)
        assert m.compiled

    @testing.resolve_artifact_names
    def test_compile_on_get_props_2(self):
        m= mapper(User, users)
        assert not m.compiled
        assert m.get_property('name')
        assert m.compiled

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
            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, 'name')
                if other is None:
                    return col == None
                else:
                    return sa.func.upper(col) == sa.func.upper(other)

        m.add_property('_name', deferred(users.c.name))
        m.add_property('name', synonym('_name'))
        m.add_property('addresses', relation(Address))
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
        sess.save(u3)
        sess.flush()
        sess.rollback()

    @testing.resolve_artifact_names
    def test_replace_property(self):
        m = mapper(User, users)
        m.add_property('_name',users.c.name)
        m.add_property('name', synonym('_name', proxy=True))

        sess = create_session()
        u = sess.query(User).filter_by(name='jack').one()
        eq_(u._name, 'jack')
        eq_(u.name, 'jack')
        u.name = 'jacko'
        assert m._columntoproperty[users.c.name] is m.get_property('_name')

        sa.orm.clear_mappers()

        m = mapper(User, users)
        m.add_property('name', synonym('_name', map_column=True))

        sess.clear()
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
        sa.orm.compile_mappers()

        # later, backref sets up the prop
        mapper(User, users, properties={
            'addresses':relation(Address, backref='_user')
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
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')))

        class Node(object):
            pass

        mapper(Node, t, properties={
            '_children':relation(Node, backref=backref('_parent', remote_side=t.c.id)),
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
        try:
            mapper(User, users, non_primary=True, properties={
                'addresses':relation(Address)
            }).compile()
            assert False
        except sa.exc.ArgumentError, e:
            assert "Attempting to assign a new relation 'addresses' to a non-primary mapper on class 'User'" in str(e)

    @testing.resolve_artifact_names
    def test_illegal_non_primary_2(self):
        try:
            mapper(User, users, non_primary=True)
            assert False
        except sa.exc.InvalidRequestError, e:
            assert "Configure a primary mapper first" in str(e)

    @testing.resolve_artifact_names
    def test_prop_filters(self):
        t = Table('person', MetaData(),
                  Column('id', Integer, primary_key=True),
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

        p_m = mapper(Person, t, polymorphic_on=t.c.type,
                     include_properties=('id', 'type', 'name'))
        e_m = mapper(Employee, inherits=p_m, polymorphic_identity='employee',
          properties={
            'boss': relation(Manager, backref='peon')
          },
          exclude_properties=('vendor_id',))

        m_m = mapper(Manager, inherits=e_m, polymorphic_identity='manager',
                     include_properties=('id', 'type'))

        v_m = mapper(Vendor, inherits=p_m, polymorphic_identity='vendor',
                     exclude_properties=('boss_id', 'employee_number'))
        h_m = mapper(Hoho, t, include_properties=('id', 'type', 'name'))
        l_m = mapper(Lala, t, exclude_properties=('vendor_id', 'boss_id'),
                     column_prefix="p_")

        p_m.compile()
        #sa.orm.compile_mappers()

        def assert_props(cls, want):
            have = set([n for n in dir(cls) if not n.startswith('_')])
            want = set(want)
            eq_(have, want)

        def assert_instrumented(cls, want):
            have = set([p.key for p in class_mapper(cls).iterate_properties])
            want = set(want)
            eq_(have, want)
            
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

        # excluding the discriminator column is currently not allowed
        class Foo(Person):
            pass
        self.assertRaises(sa.exc.InvalidRequestError, mapper, Foo, inherits=Person, polymorphic_identity='foo', exclude_properties=('type',) )
    
    @testing.resolve_artifact_names
    def test_mapping_to_join(self):
        """Mapping to a join"""
        usersaddresses = sa.join(users, addresses,
                                 users.c.id == addresses.c.user_id)
        mapper(User, usersaddresses, primary_key=[users.c.id])
        l = create_session().query(User).order_by(users.c.id).all()
        eq_(l, self.static.user_result[:3])

    @testing.resolve_artifact_names
    def test_mapping_to_join_no_pk(self):
        m = mapper(Address, addresses.join(email_bounces))
        m.compile()
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
               allow_null_pks=True,
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
    def test_custom_join(self):
        """select_from totally replace the FROM parameters."""

        mapper(Item, items)

        mapper(Order, orders, properties=dict(
            items=relation(Item, order_items)))

        mapper(User, users, properties=dict(
            orders=relation(Order)))

        session = create_session()
        l = (session.query(User).
             select_from(users.join(orders).
                         join(order_items).
                         join(items)).
             filter(items.c.description == 'item 4')).all()

        eq_(l, [self.static.user_result[0]])

    @testing.resolve_artifact_names
    def test_order_by(self):
        """Ordering at the mapper and query level"""

        # TODO: make a unit test out of these various combinations
        #m = mapper(User, users, order_by=desc(users.c.name))
        mapper(User, users, order_by=None)
        #mapper(User, users)

        #l = create_session().query(User).select(order_by=[desc(users.c.name), asc(users.c.user_id)])
        l = create_session().query(User).all()
        #l = create_session().query(User).select(order_by=[])
        #l = create_session().query(User).select(order_by=None)


    # 'Raises a "expression evaluation not supported" error at prepare time
    @testing.fails_on('firebird')
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
            keywords = relation(Keyword, item_keywords, lazy=True)))

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
                       name=relation(mapper(Address, addresses))))

        self.assertRaises(sa.exc.ArgumentError, go)

    @testing.resolve_artifact_names
    def test_override_2(self):
        """exclude_properties cancels the error."""
        
        mapper(User, users,
               exclude_properties=['name'],
               properties=dict(
                   name=relation(mapper(Address, addresses))))
        
        assert bool(User.name)
        
    @testing.resolve_artifact_names
    def test_override_3(self):
        """The column being named elsewhere also cancels the error,"""
        mapper(User, users,
               properties=dict(
                   name=relation(mapper(Address, addresses)),
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
            addresses = relation(mapper(Address, addresses), lazy=True),
            uname = synonym('name'),
            adlist = synonym('addresses', proxy=True),
            adname = synonym('addresses')
        ))

        assert hasattr(User, 'adlist')
        # as of 0.4.2, synonyms always create a property
        assert hasattr(User, 'adname')

        # test compile
        assert not isinstance(User.uname == 'jack', bool)

        sess = create_session()
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

        self.assertRaisesMessage(
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
            'addresses':relation(Address, lazy=True),
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
    def test_comparable(self):
        class extendedproperty(property):
            attribute = 123
            def __getitem__(self, key):
                return 'value'

        class UCComparator(sa.orm.PropComparator):
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
            sess.clear()

            q = sess.query(User)
            u2 = q.filter(User.name=='some user name').one()
            u3 = q.filter(User.uc_name=='SOME USER NAME').one()

            assert u2 is u3

            eq_(User.uc_name.attribute, 123)
            eq_(User.uc_name['key'], 'value')
            sess.rollback()

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
                recon.append('A')

        class B(A):
            @reconstructor
            def reconstruct(self):
                recon.append('B')

        class C(A):
            @reconstructor
            def reconstruct(self):
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
            'addresses':relation(Address)
        })

        self.assertRaises(sa.orm.exc.UnmappedClassError, sa.orm.compile_mappers)

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


class OptionsTest(_fixtures.FixtureTest):

    @testing.fails_on('maxdb')
    @testing.resolve_artifact_names
    def test_synonym_options(self):
        mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=True,
                                 order_by=addresses.c.id),
            adlist = synonym('addresses', proxy=True)))


        def go():
            sess = create_session()
            u = (sess.query(User).
                 order_by(User.id).
                 options(sa.orm.eagerload('adlist')).
                 filter_by(name='jack')).one()
            eq_(u.adlist,
                [self.static.user_address_result[0].addresses[0]])
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_eager_options(self):
        """A lazy relation can be upgraded to an eager relation."""
        mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses),
                                 order_by=addresses.c.id)))

        sess = create_session()
        l = (sess.query(User).
             order_by(User.id).
             options(sa.orm.eagerload('addresses'))).all()

        def go():
            eq_(l, self.static.user_address_result)
        self.sql_count_(0, go)

    @testing.fails_on('maxdb')
    @testing.resolve_artifact_names
    def test_eager_options_with_limit(self):
        mapper(User, users, properties=dict(
            addresses=relation(mapper(Address, addresses), lazy=True)))

        sess = create_session()
        u = (sess.query(User).
             options(sa.orm.eagerload('addresses')).
             filter_by(id=8)).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)
        self.sql_count_(0, go)

        sess.clear()

        # test that eager loading doesnt modify parent mapper
        def go():
            u = sess.query(User).filter_by(id=8).one()
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)
        assert "tbl_row_count" not in self.capture_sql(testing.db, go)

    @testing.fails_on('maxdb')
    @testing.resolve_artifact_names
    def test_lazy_options_with_limit(self):
        mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=False)))

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
        """An eager relation automatically degrades to a lazy relation if eager columns are not available"""
        mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=False)))

        sess = create_session()
        # first test straight eager load, 1 statement
        def go():
            l = sess.query(User).order_by(User.id).all()
            eq_(l, self.static.user_address_result)
        self.sql_count_(1, go)

        sess.clear()

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
            keywords=relation(Keyword, secondary=item_keywords,
                              lazy=False,
                              order_by=item_keywords.c.keyword_id)))

        mapper(Order, orders, properties=dict(
            items=relation(Item, secondary=order_items, lazy=False,
                           order_by=order_items.c.item_id)))

        mapper(User, users, properties=dict(
            addresses=relation(Address, lazy=False,
                               order_by=addresses.c.id),
            orders=relation(Order, lazy=False,
                            order_by=orders.c.id)))

        sess = create_session()

        # first test straight eager load, 1 statement
        def go():
            l = sess.query(User).order_by(User.id).all()
            eq_(l, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        r = users.select().execute()
        def go():
            l = list(sess.query(User).instances(r))
            eq_(l, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 6)

    @testing.resolve_artifact_names
    def test_lazy_options(self):
        """An eager relation can be upgraded to a lazy relation."""
        mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=False)
        ))

        sess = create_session()
        l = (sess.query(User).
             order_by(User.id).
             options(sa.orm.lazyload('addresses'))).all()

        def go():
            eq_(l, self.static.user_address_result)
        self.sql_count_(4, go)


class DeepOptionsTest(_fixtures.FixtureTest):
    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relation(Keyword, item_keywords,
                              order_by=item_keywords.c.item_id)))

        mapper(Order, orders, properties=dict(
            items=relation(Item, order_items,
                           order_by=items.c.id)))

        mapper(User, users, order_by=users.c.id, properties=dict(
            orders=relation(Order, order_by=orders.c.id)))

    @testing.resolve_artifact_names
    def test_deep_options_1(self):
        sess = create_session()

        # eagerload nothing.
        u = sess.query(User).all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(testing.db, go, 3)

    @testing.resolve_artifact_names
    def test_deep_options_2(self):
        sess = create_session()

        # eagerload orders.items.keywords; eagerload_all() implies eager load
        # of orders, orders.items
        l = (sess.query(User).
              options(sa.orm.eagerload_all('orders.items.keywords'))).all()
        def go():
            x = l[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)


    @testing.resolve_artifact_names
    def test_deep_options_3(self):
        sess = create_session()

        # same thing, with separate options calls
        q2 = (sess.query(User).
              options(sa.orm.eagerload('orders')).
              options(sa.orm.eagerload('orders.items')).
              options(sa.orm.eagerload('orders.items.keywords')))
        u = q2.all()
        def go():
            x = u[0].orders[1].items[0].keywords[1]
        self.sql_count_(0, go)

    @testing.resolve_artifact_names
    def test_deep_options_4(self):
        sess = create_session()

        self.assertRaisesMessage(
            sa.exc.ArgumentError,
            r"Can't find entity Mapper\|Order\|orders in Query.  "
            r"Current list: \['Mapper\|User\|users'\]",
            sess.query(User).options, sa.orm.eagerload(Order.items))

        # eagerload "keywords" on items.  it will lazy load "orders", then
        # lazy load the "items" on the order, but on "items" it will eager
        # load the "keywords"
        q3 = sess.query(User).options(sa.orm.eagerload('orders.items.keywords'))
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
        self.assertRaises(AssertionError, setattr, u1, "name", "fred")
        eq_(u1.name, 'ed modified')
        sess.add(u1)
        sess.flush()
        sess.clear()
        eq_(sess.query(User).filter_by(name='ed modified').one(), User(name='ed'))
        

    @testing.resolve_artifact_names
    def test_collection(self):
        class User(_base.ComparableEntity):
            @validates('addresses')
            def validate_address(self, key, ad):
                assert '@' in ad.email_address
                return ad
                
        mapper(User, users, properties={'addresses':relation(Address)})
        mapper(Address, addresses)
        sess = create_session()
        u1 = User(name='edward')
        self.assertRaises(AssertionError, u1.addresses.append, Address(email_address='noemail'))
        u1.addresses.append(Address(id=15, email_address='foo@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.clear()
        eq_(
            sess.query(User).filter_by(name='edward').one(), 
            User(name='edward', addresses=[Address(email_address='foo@bar.com')])
        )
        
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
        sess.save(o)
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
        sess.save(o)
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
        sess.save(o)
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
        mapper(Order, orders, properties={
            'userident': deferred(orders.c.user_id, group='primary'),
            'addrident': deferred(orders.c.address_id, group='primary'),
            'description': deferred(orders.c.description, group='primary'),
            'opened': deferred(orders.c.isopen, group='primary')
        })

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

        self.sql_eq_(q.all, [
            ("SELECT orders.id AS orders_id, "
             "orders.address_id AS orders_address_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen "
             "FROM orders ORDER BY orders.id", {}),
            ("SELECT orders.user_id AS orders_user_id "
             "FROM orders WHERE orders.id = :param_1",
             {'param_1':3})])
        sess.clear()

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
        mapper(Order, orders, properties={
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')})

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
    def test_deep_options(self):
        mapper(Item, items, properties=dict(
            description=deferred(items.c.description)))
        mapper(Order, orders, properties=dict(
            items=relation(Item, secondary=order_items)))
        mapper(User, users, properties=dict(
            orders=relation(Order, order_by=orders.c.id)))

        sess = create_session()
        q = sess.query(User).order_by(User.id)
        l = q.all()
        item = l[0].orders[1].items[1]
        def go():
            eq_(item.description, 'item 4')
        self.sql_count_(1, go)
        eq_(item.description, 'item 4')

        sess.clear()
        l = q.options(sa.orm.undefer('orders.items.description')).all()
        item = l[0].orders[1].items[1]
        def go():
            eq_(item.description, 'item 4')
        self.sql_count_(0, go)
        eq_(item.description, 'item 4')

class DeferredPopulationTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table("thing", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(20)))

        Table("human", metadata,
            Column("id", Integer, primary_key=True),
            Column("thing_id", Integer, ForeignKey("thing.id")),
            Column("name", String(20)))

    @testing.resolve_artifact_names
    def setup_mappers(self):
        class Human(_base.BasicEntity): pass
        class Thing(_base.BasicEntity): pass

        mapper(Human, human, properties={"thing": relation(Thing)})
        mapper(Thing, thing, properties={"name": deferred(thing.c.name)})
    
    @testing.resolve_artifact_names
    def insert_data(self):
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
        session.clear()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_query_twice_no_clear(self):
        session = create_session()
        result = session.query(Thing).first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)
    
    @testing.resolve_artifact_names
    def test_eagerload_with_clear(self):
        session = create_session()
        human = session.query(Human).options(sa.orm.eagerload("thing")).first()
        session.clear()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_eagerload_no_clear(self):
        session = create_session()
        human = session.query(Human).options(sa.orm.eagerload("thing")).first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_join_with_clear(self):
        session = create_session()
        result = session.query(Human).add_entity(Thing).join("thing").first()
        session.clear()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    @testing.resolve_artifact_names
    def test_join_no_clear(self):
        session = create_session()
        result = session.query(Human).add_entity(Thing).join("thing").first()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)
    
        
class CompositeTypesTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('graphs', metadata,
            Column('id', Integer, primary_key=True),
            Column('version_id', Integer, primary_key=True, nullable=True),
            Column('name', String(30)))

        Table('edges', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('graph_id', Integer, nullable=False),
            Column('graph_version_id', Integer, nullable=False),
            Column('x1', Integer),
            Column('y1', Integer),
            Column('x2', Integer),
            Column('y2', Integer),
            sa.ForeignKeyConstraint(
            ['graph_id', 'graph_version_id'],
            ['graphs.id', 'graphs.version_id']))

        Table('foobars', metadata,
            Column('id', Integer, primary_key=True),
            Column('x1', Integer, default=2),
            Column('x2', Integer),
            Column('x3', Integer, default=15),
            Column('x4', Integer)
        )
        
    @testing.resolve_artifact_names
    def test_basic(self):
        class Point(object):
            def __init__(self, x, y):
                self.x = x
                self.y = y
            def __composite_values__(self):
                return [self.x, self.y]
            def __eq__(self, other):
                return other.x == self.x and other.y == self.y
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(object):
            pass
        class Edge(object):
            def __init__(self, start, end):
                self.start = start
                self.end = end

        mapper(Graph, graphs, properties={
            'edges':relation(Edge)
        })
        mapper(Edge, edges, properties={
            'start':sa.orm.composite(Point, edges.c.x1, edges.c.y1),
            'end': sa.orm.composite(Point, edges.c.x2, edges.c.y2)
        })

        sess = create_session()
        g = Graph()
        g.id = 1
        g.version_id=1
        g.edges.append(Edge(Point(3, 4), Point(5, 6)))
        g.edges.append(Edge(Point(14, 5), Point(2, 7)))
        sess.save(g)
        sess.flush()

        sess.clear()
        g2 = sess.query(Graph).get([g.id, g.version_id])
        for e1, e2 in zip(g.edges, g2.edges):
            eq_(e1.start, e2.start)
            eq_(e1.end, e2.end)

        g2.edges[1].end = Point(18, 4)
        sess.flush()
        sess.clear()
        e = sess.query(Edge).get(g2.edges[1].id)
        eq_(e.end, Point(18, 4))

        e.end.x = 19
        e.end.y = 5
        sess.flush()
        sess.clear()
        eq_(sess.query(Edge).get(g2.edges[1].id).end, Point(19, 5))

        g.edges[1].end = Point(19, 5)

        sess.clear()
        def go():
            g2 = (sess.query(Graph).
                  options(sa.orm.eagerload('edges'))).get([g.id, g.version_id])
            for e1, e2 in zip(g.edges, g2.edges):
                eq_(e1.start, e2.start)
                eq_(e1.end, e2.end)
        self.assert_sql_count(testing.db, go, 1)

        # test comparison of CompositeProperties to their object instances
        g = sess.query(Graph).get([1, 1])
        assert sess.query(Edge).filter(Edge.start==Point(3, 4)).one() is g.edges[0]

        assert sess.query(Edge).filter(Edge.start!=Point(3, 4)).first() is g.edges[1]

        eq_(sess.query(Edge).filter(Edge.start==None).all(), [])

    @testing.resolve_artifact_names
    def test_pk(self):
        """Using a composite type as a primary key"""

        class Version(object):
            def __init__(self, id, version):
                self.id = id
                self.version = version
            def __composite_values__(self):
                return (self.id, self.version)
            def __eq__(self, other):
                return other.id == self.id and other.version == self.version
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(object):
            def __init__(self, version):
                self.version = version

        mapper(Graph, graphs, allow_null_pks=True, properties={
            'version':sa.orm.composite(Version, graphs.c.id,
                                       graphs.c.version_id)})

        sess = create_session()
        g = Graph(Version(1, 1))
        sess.save(g)
        sess.flush()

        sess.clear()
        g2 = sess.query(Graph).get([1, 1])
        eq_(g.version, g2.version)
        sess.clear()

        g2 = sess.query(Graph).get(Version(1, 1))
        eq_(g.version, g2.version)

        # test pk mutation
        g2.version = Version(2, 1)
        sess.flush()
        g3 = sess.query(Graph).get(Version(2, 1))
        eq_(g2.version, g3.version)
        
        # test pk with one column NULL
        # TODO: can't seem to get NULL in for a PK value
        # in either mysql or postgres, autoincrement=False etc.
        # notwithstanding
        @testing.fails_on_everything_except("sqlite")
        def go():
            g = Graph(Version(2, None))
            sess.save(g)
            sess.flush()
            sess.clear()
            g2 = sess.query(Graph).filter_by(version=Version(2, None)).one()
            eq_(g.version, g2.version)
        go()
        
    @testing.resolve_artifact_names
    def test_attributes_with_defaults(self):
        class Foobar(object):
            pass

        class FBComposite(object):
            def __init__(self, x1, x2, x3, x4):
                self.x1 = x1
                self.x2 = x2
                self.x3 = x3
                self.x4 = x4
            def __composite_values__(self):
                return self.x1, self.x2, self.x3, self.x4
            def __eq__(self, other):
                return other.x1 == self.x1 and other.x2 == self.x2 and other.x3 == self.x3 and other.x4 == self.x4
            def __ne__(self, other):
                return not self.__eq__(other)

        mapper(Foobar, foobars, properties=dict(
            foob=sa.orm.composite(FBComposite, foobars.c.x1, foobars.c.x2, foobars.c.x3, foobars.c.x4)
        ))

        sess = create_session()
        f1 = Foobar()
        f1.foob = FBComposite(None, 5, None, None)
        sess.save(f1)
        sess.flush()

        assert f1.foob == FBComposite(2, 5, 15, None)
    
    @testing.resolve_artifact_names
    def test_set_composite_values(self):
        class Foobar(object):
            pass
        
        class FBComposite(object):
            def __init__(self, x1, x2, x3, x4):
                self.x1val = x1
                self.x2val = x2
                self.x3 = x3
                self.x4 = x4
            def __composite_values__(self):
                return self.x1val, self.x2val, self.x3, self.x4
            def __set_composite_values__(self, x1, x2, x3, x4):
                self.x1val = x1
                self.x2val = x2
                self.x3 = x3
                self.x4 = x4
            def __eq__(self, other):
                return other.x1val == self.x1val and other.x2val == self.x2val and other.x3 == self.x3 and other.x4 == self.x4
            def __ne__(self, other):
                return not self.__eq__(other)
        
        mapper(Foobar, foobars, properties=dict(
            foob=sa.orm.composite(FBComposite, foobars.c.x1, foobars.c.x2, foobars.c.x3, foobars.c.x4)
        ))
        
        sess = create_session()
        f1 = Foobar()
        f1.foob = FBComposite(None, 5, None, None)
        sess.save(f1)
        sess.flush()
        
        assert f1.foob == FBComposite(2, 5, 15, None)
    
    @testing.resolve_artifact_names
    def test_save_null(self):
        """test saving a null composite value
        
        See google groups thread for more context:
        http://groups.google.com/group/sqlalchemy/browse_thread/thread/0c6580a1761b2c29
        
        """
        class Point(object):
            def __init__(self, x, y):
                self.x = x
                self.y = y
            def __composite_values__(self):
                return [self.x, self.y]
            def __eq__(self, other):
                return other.x == self.x and other.y == self.y
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(object):
            pass
        class Edge(object):
            def __init__(self, start, end):
                self.start = start
                self.end = end

        mapper(Graph, graphs, properties={
            'edges':relation(Edge)
        })
        mapper(Edge, edges, properties={
            'start':sa.orm.composite(Point, edges.c.x1, edges.c.y1),
            'end':sa.orm.composite(Point, edges.c.x2, edges.c.y2)
        })

        sess = create_session()
        g = Graph()
        g.id = 1
        g.version_id=1
        e = Edge(None, None)
        g.edges.append(e)
        
        sess.save(g)
        sess.flush()
        
        sess.clear()
        
        g2 = sess.query(Graph).get([1, 1])
        assert g2.edges[-1].start.x is None
        assert g2.edges[-1].start.y is None


class NoLoadTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @testing.resolve_artifact_names
    def test_basic(self):
        """A basic one-to-many lazy load"""
        m = mapper(User, users, properties=dict(
            addresses = relation(mapper(Address, addresses), lazy=None)
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
            addresses = relation(mapper(Address, addresses), lazy=None)
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


class MapperExtensionTest(_fixtures.FixtureTest):
    run_inserts = None
    
    def extension(self):
        methods = []

        class Ext(sa.orm.MapperExtension):
            def instrument_class(self, mapper, cls):
                methods.append('instrument_class')
                return sa.orm.EXT_CONTINUE

            def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
                methods.append('init_instance')
                return sa.orm.EXT_CONTINUE

            def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
                methods.append('init_failed')
                return sa.orm.EXT_CONTINUE

            def translate_row(self, mapper, context, row):
                methods.append('translate_row')
                return sa.orm.EXT_CONTINUE

            def create_instance(self, mapper, selectcontext, row, class_):
                methods.append('create_instance')
                return sa.orm.EXT_CONTINUE

            def reconstruct_instance(self, mapper, instance):
                methods.append('reconstruct_instance')
                return sa.orm.EXT_CONTINUE

            def append_result(self, mapper, selectcontext, row, instance, result, **flags):
                methods.append('append_result')
                return sa.orm.EXT_CONTINUE

            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                methods.append('populate_instance')
                return sa.orm.EXT_CONTINUE

            def before_insert(self, mapper, connection, instance):
                methods.append('before_insert')
                return sa.orm.EXT_CONTINUE

            def after_insert(self, mapper, connection, instance):
                methods.append('after_insert')
                return sa.orm.EXT_CONTINUE

            def before_update(self, mapper, connection, instance):
                methods.append('before_update')
                return sa.orm.EXT_CONTINUE

            def after_update(self, mapper, connection, instance):
                methods.append('after_update')
                return sa.orm.EXT_CONTINUE

            def before_delete(self, mapper, connection, instance):
                methods.append('before_delete')
                return sa.orm.EXT_CONTINUE

            def after_delete(self, mapper, connection, instance):
                methods.append('after_delete')
                return sa.orm.EXT_CONTINUE

        return Ext, methods

    @testing.resolve_artifact_names
    def test_basic(self):
        """test that common user-defined methods get called."""
        Ext, methods = self.extension()

        mapper(User, users, extension=Ext())
        sess = create_session()
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        u = sess.query(User).populate_existing().get(u.id)
        sess.clear()
        u = sess.query(User).get(u.id)
        u.name = 'u1 changed'
        sess.flush()
        sess.delete(u)
        sess.flush()
        eq_(methods,
            ['instrument_class', 'init_instance', 'before_insert',
             'after_insert', 'translate_row', 'populate_instance',
             'append_result', 'translate_row', 'create_instance',
             'populate_instance', 'reconstruct_instance', 'append_result',
             'before_update', 'after_update', 'before_delete', 'after_delete'])

    @testing.resolve_artifact_names
    def test_inheritance(self):
        Ext, methods = self.extension()

        class AdminUser(User):
            pass

        mapper(User, users, extension=Ext())
        mapper(AdminUser, addresses, inherits=User)

        sess = create_session()
        am = AdminUser(name='au1', email_address='au1@e1')
        sess.save(am)
        sess.flush()
        am = sess.query(AdminUser).populate_existing().get(am.id)
        sess.clear()
        am = sess.query(AdminUser).get(am.id)
        am.name = 'au1 changed'
        sess.flush()
        sess.delete(am)
        sess.flush()
        eq_(methods,
            ['instrument_class', 'instrument_class', 'init_instance',
             'before_insert', 'after_insert', 'translate_row',
             'populate_instance', 'append_result', 'translate_row',
             'create_instance', 'populate_instance', 'reconstruct_instance',
             'append_result', 'before_update', 'after_update', 'before_delete',
             'after_delete'])

    @testing.resolve_artifact_names
    def test_after_with_no_changes(self):
        """after_update is called even if no columns were updated."""

        Ext, methods = self.extension()

        mapper(Item, items, extension=Ext() , properties={
            'keywords': relation(Keyword, secondary=item_keywords)})
        mapper(Keyword, keywords, extension=Ext())

        sess = create_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.save(i1)
        sess.save(k1)
        sess.flush()
        eq_(methods,
            ['instrument_class', 'instrument_class', 'init_instance',
             'init_instance', 'before_insert', 'after_insert',
             'before_insert', 'after_insert'])

        del methods[:]
        i1.keywords.append(k1)
        sess.flush()
        eq_(methods, ['before_update', 'after_update'])


    @testing.resolve_artifact_names
    def test_inheritance_with_dupes(self):
        """Inheritance with the same extension instance on both mappers."""
        Ext, methods = self.extension()

        class AdminUser(User):
            pass

        ext = Ext()
        mapper(User, users, extension=ext)
        mapper(AdminUser, addresses, inherits=User, extension=ext)

        sess = create_session()
        am = AdminUser(name="au1", email_address="au1@e1")
        sess.save(am)
        sess.flush()
        am = sess.query(AdminUser).populate_existing().get(am.id)
        sess.clear()
        am = sess.query(AdminUser).get(am.id)
        am.name = 'au1 changed'
        sess.flush()
        sess.delete(am)
        sess.flush()
        eq_(methods,
            ['instrument_class', 'instrument_class', 'init_instance',
             'before_insert', 'after_insert', 'translate_row',
             'populate_instance', 'append_result', 'translate_row',
             'create_instance', 'populate_instance', 'reconstruct_instance',
             'append_result', 'before_update', 'after_update', 'before_delete',
             'after_delete'])


class RequirementsTest(_base.MappedTest):
    """Tests the contract for user classes."""

    def define_tables(self, metadata):
        Table('ht1', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('value', String(10)))
        Table('ht2', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('ht1_id', Integer, ForeignKey('ht1.id')),
              Column('value', String(10)))
        Table('ht3', metadata,
              Column('id', Integer, primary_key=True,
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

    @testing.resolve_artifact_names
    def test_baseclass(self):
        class OldStyle:
            pass

        self.assertRaises(sa.exc.ArgumentError, mapper, OldStyle, ht1)

        class NoWeakrefSupport(str):
            pass

        # TODO: is weakref support detectable without an instance?
        #self.assertRaises(sa.exc.ArgumentError, mapper, NoWeakrefSupport, t2)

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
            'h2s': relation(H2, backref='h1'),
            'h3s': relation(H3, secondary=ht4, backref='h1s'),
            'h1s': relation(H1, secondary=ht5, backref='parent_h1'),
            't6a': relation(H6, backref='h1a',
                            primaryjoin=ht1.c.id==ht6.c.ht1a_id),
            't6b': relation(H6, backref='h1b',
                            primaryjoin=ht1.c.id==ht6.c.ht1b_id),
            })
        mapper(H2, ht2)
        mapper(H3, ht3)
        mapper(H6, ht6)

        s = create_session()
        for i in range(3):
            h1 = H1()
            s.save(h1)

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

        h1s = s.query(H1).options(sa.orm.eagerload('h2s')).all()
        eq_(len(h1s), 5)

        self.assert_unordered_result(h1s, H1,
                                     {'h2s': []},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'},
                                                   {'value': 'abc'},
                                                   {'value': 'abc'}])},
                                     {'h2s': []},
                                     {'h2s': (H2, [{'value': 'abc'}])})

        h1s = s.query(H1).options(sa.orm.eagerload('h3s')).all()

        eq_(len(h1s), 5)
        h1s = s.query(H1).options(sa.orm.eagerload_all('t6a.h1b'),
                                  sa.orm.eagerload('h2s'),
                                  sa.orm.eagerload_all('h3s.h1s')).all()
        eq_(len(h1s), 5)


class MagicNamesTest(_base.MappedTest):

    def define_tables(self, metadata):
        Table('cartographers', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)),
              Column('alias', String(50)),
              Column('quip', String(100)))
        Table('maps', metadata,
              Column('id', Integer, primary_key=True),
              Column('cart_id', Integer,
                     ForeignKey('cartographers.id')),
              Column('state', String(2)),
              Column('data', sa.Text))

    def setup_classes(self):
        class Cartographer(_base.BasicEntity):
            pass

        class Map(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_mappish(self):
        mapper(Cartographer, cartographers, properties=dict(
            query=cartographers.c.quip))
        mapper(Map, maps, properties=dict(
            mapper=relation(Cartographer, backref='maps')))

        c = Cartographer(name='Lenny', alias='The Dude',
                         query='Where be dragons?')
        m = Map(state='AK', mapper=c)

        sess = create_session()
        sess.save(c)
        sess.flush()
        sess.clear()
    
        for C, M in ((Cartographer, Map),
                     (sa.orm.aliased(Cartographer), sa.orm.aliased(Map))):
            c1 = (sess.query(C).
                  filter(C.alias=='The Dude').
                  filter(C.query=='Where be dragons?')).one()
            m1 = sess.query(M).filter(M.mapper==c1).one()

    @testing.resolve_artifact_names
    def test_direct_stateish(self):
        for reserved in (sa.orm.attributes.ClassManager.STATE_ATTR,
                         sa.orm.attributes.ClassManager.MANAGER_ATTR):
            t = Table('t', sa.MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column(reserved, Integer))
            class T(object):
                pass

            self.assertRaisesMessage(
                KeyError,
                ('%r: requested attribute name conflicts with '
                 'instrumentation attribute of the same name.' % reserved),
                mapper, T, t)

    @testing.resolve_artifact_names
    def test_indirect_stateish(self):
        for reserved in (sa.orm.attributes.ClassManager.STATE_ATTR,
                         sa.orm.attributes.ClassManager.MANAGER_ATTR):
            class M(object):
                pass

            self.assertRaisesMessage(
                KeyError,
                ('requested attribute name conflicts with '
                 'instrumentation attribute of the same name'),
                mapper, M, maps, properties={
                  reserved: maps.c.state})


class ScalarRequirementsTest(_base.MappedTest):

    # TODO: is this needed here?
    # what does this suite excercise that unitofwork doesn't?

    def define_tables(self, metadata):
        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', sa.PickleType()))

    def setup_classes(self):
        class Foo(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_correct_comparison(self):
        mapper(Foo, t1)

        f1 = Foo(data=pickleable.NotComparable('12345'))

        session = create_session()
        session.add(f1)
        session.flush()
        session.clear()

        f1 = session.query(Foo).get(f1.id)
        eq_(f1.data.data, '12345')

        f2 = Foo(data=pickleable.BrokenComparable('abc'))

        session.add(f2)
        session.flush()
        session.clear()

        f2 = session.query(Foo).get(f2.id)
        eq_(f2.data.data, 'abc')


if __name__ == "__main__":
    testenv.main()
