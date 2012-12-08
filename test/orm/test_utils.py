from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.orm import util
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import aliased, with_polymorphic
from sqlalchemy.orm import mapper, create_session
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing import eq_, is_
from sqlalchemy.orm.util import PathRegistry
from sqlalchemy import inspect

class AliasedClassTest(fixtures.TestBase):
    def point_map(self, cls):
        table = Table('point', MetaData(),
                    Column('id', Integer(), primary_key=True),
                    Column('x', Integer),
                    Column('y', Integer))
        mapper(cls, table)
        return table

    def test_simple(self):
        class Point(object):
            pass
        table = self.point_map(Point)

        alias = aliased(Point)

        assert alias.id
        assert alias.x
        assert alias.y

        assert Point.id.__clause_element__().table is table
        assert alias.id.__clause_element__().table is not table

    def test_notcallable(self):
        class Point(object):
            pass
        table = self.point_map(Point)
        alias = aliased(Point)

        assert_raises(TypeError, alias)

    def test_instancemethods(self):
        class Point(object):
            def zero(self):
                self.x, self.y = 0, 0

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.zero
        # Py2K
        # TODO: what is this testing ??
        assert not getattr(alias, 'zero')
        # end Py2K

    def test_classmethods(self):
        class Point(object):
            @classmethod
            def max_x(cls):
                return 100

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert alias.max_x
        assert Point.max_x() == alias.max_x()

    def test_simpleproperties(self):
        class Point(object):
            @property
            def max_x(self):
                return 100

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert Point.max_x != 100
        assert alias.max_x
        assert Point.max_x is alias.max_x

    def test_descriptors(self):
        """Tortured..."""

        class descriptor(object):
            def __init__(self, fn):
                self.fn = fn
            def __get__(self, obj, owner):
                if obj is not None:
                    return self.fn(obj, obj)
                else:
                    return self
            def method(self):
                return 'method'

        class Point(object):
            center = (0, 0)
            @descriptor
            def thing(self, arg):
                return arg.center

        table = self.point_map(Point)
        alias = aliased(Point)

        assert Point.thing != (0, 0)
        assert Point().thing == (0, 0)
        assert Point.thing.method() == 'method'

        assert alias.thing != (0, 0)
        assert alias.thing.method() == 'method'

    def test_hybrid_descriptors(self):
        from sqlalchemy import Column  # override testlib's override
        import types

        class MethodDescriptor(object):
            def __init__(self, func):
                self.func = func
            def __get__(self, instance, owner):
                if instance is None:
                    # Py3K
                    #args = (self.func, owner)
                    # Py2K
                    args = (self.func, owner, owner.__class__)
                    # end Py2K
                else:
                    # Py3K
                    #args = (self.func, instance)
                    # Py2K
                    args = (self.func, instance, owner)
                    # end Py2K
                return types.MethodType(*args)

        class PropertyDescriptor(object):
            def __init__(self, fget, fset, fdel):
                self.fget = fget
                self.fset = fset
                self.fdel = fdel
            def __get__(self, instance, owner):
                if instance is None:
                    return self.fget(owner)
                else:
                    return self.fget(instance)
            def __set__(self, instance, value):
                self.fset(instance, value)
            def __delete__(self, instance):
                self.fdel(instance)
        hybrid = MethodDescriptor
        def hybrid_property(fget, fset=None, fdel=None):
            return PropertyDescriptor(fget, fset, fdel)

        def assert_table(expr, table):
            for child in expr.get_children():
                if isinstance(child, Column):
                    assert child.table is table

        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y
            @hybrid
            def left_of(self, other):
                return self.x < other.x

            double_x = hybrid_property(lambda self: self.x * 2)

        table = self.point_map(Point)
        alias = aliased(Point)
        alias_table = alias.x.__clause_element__().table
        assert table is not alias_table

        p1 = Point(-10, -10)
        p2 = Point(20, 20)

        assert p1.left_of(p2)
        assert p1.double_x == -20

        assert_table(Point.double_x, table)
        assert_table(alias.double_x, alias_table)

        assert_table(Point.left_of(p2), table)
        assert_table(alias.left_of(p2), alias_table)

class IdentityKeyTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_identity_key_1(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        key = util.identity_key(User, [1])
        eq_(key, (User, (1,)))
        key = util.identity_key(User, ident=[1])
        eq_(key, (User, (1,)))

    def test_identity_key_scalar(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        key = util.identity_key(User, 1)
        eq_(key, (User, (1,)))
        key = util.identity_key(User, ident=1)
        eq_(key, (User, (1,)))

    def test_identity_key_2(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        s = create_session()
        u = User(name='u1')
        s.add(u)
        s.flush()
        key = util.identity_key(instance=u)
        eq_(key, (User, (u.id,)))

    def test_identity_key_3(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        row = {users.c.id: 1, users.c.name: "Frank"}
        key = util.identity_key(User, row=row)
        eq_(key, (User, (1,)))


class PathRegistryTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = None
    run_deletes = None


    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_root_registry(self):
        umapper = inspect(self.classes.User)
        is_(
            util.RootRegistry()[umapper],
            umapper._path_registry
        )
        eq_(
            util.RootRegistry()[umapper],
            util.PathRegistry.coerce((umapper,))
        )

    def test_expand(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce((umapper,))

        eq_(
            path[umapper.attrs.addresses][amapper]
                [amapper.attrs.email_address],
            PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))
        )

    def test_entity_boolean(self):
        umapper = inspect(self.classes.User)
        path = PathRegistry.coerce((umapper,))
        is_(bool(path), True)

    def test_key_boolean(self):
        umapper = inspect(self.classes.User)
        path = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        is_(bool(path), True)

    def test_aliased_class(self):
        User = self.classes.User
        ua = aliased(User)
        ua_insp = inspect(ua)
        path = PathRegistry.coerce((ua_insp, ua_insp.mapper.attrs.addresses))
        assert path.parent.is_aliased_class

    def test_indexed_entity(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))
        is_(path[0], umapper)
        is_(path[2], amapper)

    def test_indexed_key(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))
        eq_(path[1], umapper.attrs.addresses)
        eq_(path[3], amapper.attrs.email_address)

    def test_slice(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))
        eq_(path[1:3], (umapper.attrs.addresses, amapper))

    def test_addition(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((amapper, amapper.attrs.email_address))
        eq_(
            p1 + p2,
            PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))
        )

    def test_length(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        pneg1 = PathRegistry.coerce(())
        p0 = PathRegistry.coerce((umapper,))
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))

        eq_(len(pneg1), 0)
        eq_(len(p0), 1)
        eq_(len(p1), 2)
        eq_(len(p2), 3)
        eq_(len(p3), 4)
        eq_(pneg1.length, 0)
        eq_(p0.length, 1)
        eq_(p1.length, 2)
        eq_(p2.length, 3)
        eq_(p3.length, 4)

    def test_eq(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        u_alias = inspect(aliased(self.classes.User))
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.name))
        p4 = PathRegistry.coerce((u_alias, umapper.attrs.addresses))
        p5 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p6 = PathRegistry.coerce((amapper, amapper.attrs.user, umapper,
                                umapper.attrs.addresses))
        p7 = PathRegistry.coerce((amapper, amapper.attrs.user, umapper,
                                umapper.attrs.addresses,
                                amapper, amapper.attrs.email_address))

        is_(p1 == p2, True)
        is_(p1 == p3, False)
        is_(p1 == p4, False)
        is_(p1 == p5, False)
        is_(p6 == p7, False)
        is_(p6 == p7.parent.parent, True)

        is_(p1 != p2, False)
        is_(p1 != p3, True)
        is_(p1 != p4, True)
        is_(p1 != p5, True)

    def test_contains_mapper(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        assert p1.contains_mapper(umapper)
        assert not p1.contains_mapper(amapper)

    def _registry(self):
        class Reg(dict):
            @property
            def _attributes(self):
                return self
        return Reg()

    def test_path(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((amapper, amapper.attrs.email_address))

        eq_(
            p1.path, (umapper, umapper.attrs.addresses)
        )
        eq_(
            p2.path, (umapper, umapper.attrs.addresses, amapper)
        )
        eq_(
            p3.path, (amapper, amapper.attrs.email_address)
        )

    def test_registry_set(self):
        reg = self._registry()
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((amapper, amapper.attrs.email_address))

        p1.set(reg, "p1key", "p1value")
        p2.set(reg, "p2key", "p2value")
        p3.set(reg, "p3key", "p3value")
        eq_(
            reg,
            {
                ('p1key', p1.path): 'p1value',
                ('p2key', p2.path): 'p2value',
                ('p3key', p3.path): 'p3value',
            }
        )

    def test_registry_get(self):
        reg = self._registry()
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((amapper, amapper.attrs.email_address))
        reg.update(
            {
                ('p1key', p1.path): 'p1value',
                ('p2key', p2.path): 'p2value',
                ('p3key', p3.path): 'p3value',
            }
        )

        eq_(p1.get(reg, "p1key"), "p1value")
        eq_(p2.get(reg, "p2key"), "p2value")
        eq_(p2.get(reg, "p1key"), None)
        eq_(p3.get(reg, "p3key"), "p3value")
        eq_(p3.get(reg, "p1key"), None)

    def test_registry_contains(self):
        reg = self._registry()
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((amapper, amapper.attrs.email_address))
        reg.update(
            {
                ('p1key', p1.path): 'p1value',
                ('p2key', p2.path): 'p2value',
                ('p3key', p3.path): 'p3value',
            }
        )
        assert p1.contains(reg, "p1key")
        assert not p1.contains(reg, "p2key")
        assert p3.contains(reg, "p3key")
        assert not p2.contains(reg, "fake")

    def test_registry_setdefault(self):
        reg = self._registry()
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        reg.update(
            {
                ('p1key', p1.path): 'p1value',
            }
        )

        p1.setdefault(reg, "p1key", "p1newvalue_a")
        p1.setdefault(reg, "p1key_new", "p1newvalue_b")
        p2.setdefault(reg, "p2key", "p2newvalue")
        eq_(
            reg,
            {
                ('p1key', p1.path): 'p1value',
                ('p1key_new', p1.path): 'p1newvalue_b',
                ('p2key', p2.path): 'p2newvalue',
            }
        )

    def test_serialize(self):
        User = self.classes.User
        Address = self.classes.Address
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper,
                            amapper.attrs.email_address))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        eq_(
            p1.serialize(),
            [(User, "addresses"), (Address, "email_address")]
        )
        eq_(
            p2.serialize(),
            [(User, "addresses"), (Address, None)]
        )
        eq_(
            p3.serialize(),
            [(User, "addresses")]
        )

    def test_deseralize(self):
        User = self.classes.User
        Address = self.classes.Address
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)


        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper,
                            amapper.attrs.email_address))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.addresses))

        eq_(
            PathRegistry.deserialize([(User, "addresses"),
                        (Address, "email_address")]),
            p1
        )
        eq_(
            PathRegistry.deserialize([(User, "addresses"), (Address, None)]),
            p2
        )
        eq_(
            PathRegistry.deserialize([(User, "addresses")]),
            p3
        )

from .inheritance import _poly_fixtures
class PathRegistryInhTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = 'once'
    run_inserts = None
    run_deletes = None

    def test_plain(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        pmapper = inspect(Person)
        emapper = inspect(Engineer)

        p1 = PathRegistry.coerce((pmapper, emapper.attrs.machines))

        # given a mapper and an attribute on a subclass,
        # the path converts what you get to be against that subclass
        eq_(
            p1.path,
            (emapper, emapper.attrs.machines)
        )

    def test_plain_compound(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        cmapper = inspect(Company)
        pmapper = inspect(Person)
        emapper = inspect(Engineer)

        p1 = PathRegistry.coerce((cmapper, cmapper.attrs.employees,
                        pmapper, emapper.attrs.machines))

        # given a mapper and an attribute on a subclass,
        # the path converts what you get to be against that subclass
        eq_(
            p1.path,
            (cmapper, cmapper.attrs.employees, emapper, emapper.attrs.machines)
        )

    def test_plain_aliased(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)

        p_alias = aliased(Person)
        p_alias = inspect(p_alias)

        p1 = PathRegistry.coerce((p_alias, emapper.attrs.machines))
        # plain AliasedClass - the path keeps that AliasedClass directly
        # as is in the path
        eq_(
            p1.path,
            (p_alias, emapper.attrs.machines)
        )

    def test_plain_aliased_compound(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        cmapper = inspect(Company)
        emapper = inspect(Engineer)

        c_alias = aliased(Company)
        p_alias = aliased(Person)

        c_alias = inspect(c_alias)
        p_alias = inspect(p_alias)

        p1 = PathRegistry.coerce((c_alias, cmapper.attrs.employees,
                    p_alias, emapper.attrs.machines))
        # plain AliasedClass - the path keeps that AliasedClass directly
        # as is in the path
        eq_(
            p1.path,
            (c_alias, cmapper.attrs.employees, p_alias, emapper.attrs.machines)
        )

    def test_with_poly_sub(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)

        p_poly = with_polymorphic(Person, [Engineer])
        e_poly = inspect(p_poly.Engineer)
        p_poly = inspect(p_poly)

        p1 = PathRegistry.coerce((p_poly, emapper.attrs.machines))

        # polymorphic AliasedClass - the path uses _entity_for_mapper()
        # to get the most specific sub-entity
        eq_(
            p1.path,
            (e_poly, emapper.attrs.machines)
        )

    def test_with_poly_base(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        pmapper = inspect(Person)
        emapper = inspect(Engineer)

        p_poly = with_polymorphic(Person, [Engineer])
        p_poly = inspect(p_poly)

        # "name" is actually on Person, not Engineer
        p1 = PathRegistry.coerce((p_poly, emapper.attrs.name))

        # polymorphic AliasedClass - because "name" is on Person,
        # we get Person, not Engineer
        eq_(
            p1.path,
            (p_poly, pmapper.attrs.name)
        )

    def test_with_poly_use_mapper(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)

        p_poly = with_polymorphic(Person, [Engineer], _use_mapper_path=True)
        p_poly = inspect(p_poly)

        p1 = PathRegistry.coerce((p_poly, emapper.attrs.machines))

        # polymorphic AliasedClass with the "use_mapper_path" flag -
        # the AliasedClass acts just like the base mapper
        eq_(
            p1.path,
            (emapper, emapper.attrs.machines)
        )

