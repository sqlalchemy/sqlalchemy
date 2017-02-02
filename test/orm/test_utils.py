from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.orm import util as orm_util
from sqlalchemy import Column
from sqlalchemy import util
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import aliased, with_polymorphic, synonym
from sqlalchemy.orm import mapper, create_session, Session
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing import eq_, is_
from sqlalchemy.orm.path_registry import PathRegistry, RootRegistry
from sqlalchemy import inspect
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.testing import AssertsCompiledSQL

from .inheritance import _poly_fixtures


class AliasedClassTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _fixture(self, cls, properties={}):
        table = Table('point', MetaData(),
                      Column('id', Integer(), primary_key=True),
                      Column('x', Integer),
                      Column('y', Integer))
        mapper(cls, table, properties=properties)
        return table

    def test_simple(self):
        class Point(object):
            pass
        table = self._fixture(Point)

        alias = aliased(Point)

        assert alias.id
        assert alias.x
        assert alias.y

        assert Point.id.__clause_element__().table is table
        assert alias.id.__clause_element__().table is not table

    def test_not_instantiatable(self):
        class Point(object):
            pass
        table = self._fixture(Point)
        alias = aliased(Point)

        assert_raises(TypeError, alias)

    def test_instancemethod(self):
        class Point(object):
            def zero(self):
                self.x, self.y = 0, 0

        table = self._fixture(Point)
        alias = aliased(Point)

        assert Point.zero

        # TODO: I don't quite understand this
        # still
        if util.py2k:
            assert not getattr(alias, 'zero')
        else:
            assert getattr(alias, 'zero')

    def test_classmethod(self):
        class Point(object):
            @classmethod
            def max_x(cls):
                return 100

        table = self._fixture(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert alias.max_x
        assert Point.max_x() == alias.max_x() == 100

    def test_simple_property(self):
        class Point(object):
            @property
            def max_x(self):
                return 100

        table = self._fixture(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert Point.max_x != 100
        assert alias.max_x
        assert Point.max_x is alias.max_x

    def test_descriptors(self):

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

        table = self._fixture(Point)
        alias = aliased(Point)

        assert Point.thing != (0, 0)
        assert Point().thing == (0, 0)
        assert Point.thing.method() == 'method'

        assert alias.thing != (0, 0)
        assert alias.thing.method() == 'method'

    def _assert_has_table(self, expr, table):
        from sqlalchemy import Column  # override testlib's override
        for child in expr.get_children():
            if isinstance(child, Column):
                assert child.table is table

    def test_hybrid_descriptor_one(self):
        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_method
            def left_of(self, other):
                return self.x < other.x

        self._fixture(Point)
        alias = aliased(Point)
        sess = Session()

        self.assert_compile(
            sess.query(alias).filter(alias.left_of(Point)),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x < point.x"
        )

    def test_hybrid_descriptor_two(self):
        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_property
            def double_x(self):
                return self.x * 2

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.double_x), "Point.double_x")
        eq_(str(alias.double_x), "AliasedClass_Point.double_x")
        eq_(str(Point.double_x.__clause_element__()), "point.x * :x_1")
        eq_(str(alias.double_x.__clause_element__()), "point_1.x * :x_1")

        sess = Session()

        self.assert_compile(
            sess.query(alias).filter(alias.double_x > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x * :x_1 > point.x"
        )

    def test_hybrid_descriptor_three(self):
        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_property
            def x_alone(self):
                return self.x

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.x_alone), "Point.x_alone")
        eq_(str(alias.x_alone), "AliasedClass_Point.x_alone")

        # from __clause_element__() perspective, Point.x_alone
        # and Point.x return the same thing, so that's good
        eq_(str(Point.x.__clause_element__()), "point.x")
        eq_(str(Point.x_alone.__clause_element__()), "point.x")

        # same for the alias
        eq_(str(alias.x + 1), "point_1.x + :x_1")
        eq_(str(alias.x_alone + 1), "point_1.x + :x_1")

        is_(
            Point.x_alone.__clause_element__(),
            Point.x.__clause_element__()
        )

        eq_(str(alias.x_alone == alias.x), "point_1.x = point_1.x")

        a2 = aliased(Point)
        eq_(str(a2.x_alone == alias.x), "point_1.x = point_2.x")

        sess = Session()

        self.assert_compile(
            sess.query(alias).filter(alias.x_alone > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x > point.x"
        )

    def test_proxy_descriptor_one(self):
        class Point(object):
            def __init__(self, x, y):
                self.x, self.y = x, y

        self._fixture(Point, properties={
            'x_syn': synonym("x")
        })
        alias = aliased(Point)

        eq_(str(Point.x_syn), "Point.x_syn")
        eq_(str(alias.x_syn), "AliasedClass_Point.x_syn")

        sess = Session()
        self.assert_compile(
            sess.query(alias.x_syn).filter(alias.x_syn > Point.x_syn),
            "SELECT point_1.x AS point_1_x FROM point AS point_1, point "
            "WHERE point_1.x > point.x"
        )

    def test_parententity_vs_parentmapper(self):
        class Point(object):
            pass

        self._fixture(Point, properties={
            'x_syn': synonym("x")
        })
        pa = aliased(Point)

        is_(Point.x_syn._parententity, inspect(Point))
        is_(Point.x._parententity, inspect(Point))
        is_(Point.x_syn._parentmapper, inspect(Point))
        is_(Point.x._parentmapper, inspect(Point))

        is_(
            Point.x_syn.__clause_element__()._annotations['parententity'],
            inspect(Point))
        is_(
            Point.x.__clause_element__()._annotations['parententity'],
            inspect(Point))
        is_(
            Point.x_syn.__clause_element__()._annotations['parentmapper'],
            inspect(Point))
        is_(
            Point.x.__clause_element__()._annotations['parentmapper'],
            inspect(Point))

        pa = aliased(Point)

        is_(pa.x_syn._parententity, inspect(pa))
        is_(pa.x._parententity, inspect(pa))
        is_(pa.x_syn._parentmapper, inspect(Point))
        is_(pa.x._parentmapper, inspect(Point))

        is_(
            pa.x_syn.__clause_element__()._annotations['parententity'],
            inspect(pa)
        )
        is_(
            pa.x.__clause_element__()._annotations['parententity'],
            inspect(pa)
        )
        is_(
            pa.x_syn.__clause_element__()._annotations['parentmapper'],
            inspect(Point))
        is_(
            pa.x.__clause_element__()._annotations['parentmapper'],
            inspect(Point))


class IdentityKeyTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_identity_key_1(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        key = orm_util.identity_key(User, [1])
        eq_(key, (User, (1,)))
        key = orm_util.identity_key(User, ident=[1])
        eq_(key, (User, (1,)))

    def test_identity_key_scalar(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        key = orm_util.identity_key(User, 1)
        eq_(key, (User, (1,)))
        key = orm_util.identity_key(User, ident=1)
        eq_(key, (User, (1,)))

    def test_identity_key_2(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        s = create_session()
        u = User(name='u1')
        s.add(u)
        s.flush()
        key = orm_util.identity_key(instance=u)
        eq_(key, (User, (u.id,)))

    def test_identity_key_3(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        row = {users.c.id: 1, users.c.name: "Frank"}
        key = orm_util.identity_key(User, row=row)
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
            RootRegistry()[umapper],
            umapper._path_registry
        )
        eq_(
            RootRegistry()[umapper],
            PathRegistry.coerce((umapper,))
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
        reg = {}
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
        reg = {}
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
        reg = {}
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
        reg = {}
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
