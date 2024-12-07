import re

from sqlalchemy import Column
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.engine import result
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import aliased
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm import util as orm_util
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.path_registry import PathRegistry
from sqlalchemy.orm.path_registry import PathToken
from sqlalchemy.orm.path_registry import RootRegistry
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import is_true
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures
from .inheritance import _poly_fixtures


class ContextualWarningsTest(fixtures.TestBase):
    """
    Test for #7305

    """

    @testing.fixture
    def plain_fixture(cls, decl_base):
        class Foo(decl_base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)

        decl_base.metadata.create_all(testing.db)
        return Foo

    @testing.fixture
    def overlap_fixture(cls, decl_base):
        class Foo(decl_base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)
            bars = relationship(
                "Bar",
                primaryjoin="Foo.id==Bar.foo_id",
            )

        class Bar(decl_base):
            __tablename__ = "bar"
            id = Column(Integer, primary_key=True)
            foo_id = Column(Integer, ForeignKey("foo.id"))
            foos = relationship(
                "Foo",
                primaryjoin="Bar.foo_id==Foo.id",
            )

        return Foo, Bar

    def test_configure_mappers_explicit(self, overlap_fixture, decl_base):
        with expect_warnings(
            re.escape(
                "relationship 'Bar.foos' will copy column foo.id to column "
                "bar.foo_id, which conflicts with relationship(s): 'Foo.bars' "
                "(copies foo.id to bar.foo_id). "
            ),
        ):
            decl_base.registry.configure()

    def test_configure_mappers_implicit_aliased(self, overlap_fixture):
        Foo, Bar = overlap_fixture
        with expect_warnings(
            re.escape(
                "relationship 'Bar.foos' will copy column foo.id "
                "to column bar.foo_id, which conflicts with"
            )
            + ".*"
            + re.escape(
                "(This warning originated from the `configure_mappers()` "
                "process, which was "
                "invoked automatically in response to a user-initiated "
                "operation.)"
            ),
        ):
            FooAlias = aliased(Foo)
            assert hasattr(FooAlias, "bars")

    def test_configure_mappers_implicit_instantiate(self, overlap_fixture):
        Foo, Bar = overlap_fixture
        with expect_warnings(
            re.escape(
                "relationship 'Bar.foos' will copy column foo.id "
                "to column bar.foo_id, which conflicts with"
            )
            + ".*"
            + re.escape(
                "(This warning originated from the `configure_mappers()` "
                "process, which was "
                "invoked automatically in response to a user-initiated "
                "operation.)"
            ),
        ):
            foo = Foo()
            assert hasattr(foo, "bars")

    def test_autoflush_implicit(self, plain_fixture):
        Foo = plain_fixture

        sess = fixture_session()

        @event.listens_for(Foo, "before_insert")
        def emit_a_warning(mapper, connection, state):
            sess.add(Foo())

        sess.add(Foo())

        with expect_warnings(
            re.escape(
                "Usage of the 'Session.add()' operation is not "
                "currently supported within the execution stage of the flush "
                "process. Results may not be consistent.  Consider using "
                "alternative event listeners or connection-level operations "
                "instead."
            )
            + ".*"
            + re.escape(
                "(This warning originated from the Session 'autoflush' "
                "process, which was invoked automatically in response to a "
                "user-initiated operation. Consider using ``no_autoflush`` "
                "context manager if this warning happended while "
                "initializing objects.)"
            ),
        ):
            sess.execute(select(Foo))


class AliasedClassTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self, cls, properties={}):
        table = Table(
            "point",
            MetaData(),
            Column("id", Integer(), primary_key=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        clear_mappers()
        self.mapper_registry.map_imperatively(
            cls, table, properties=properties
        )
        return table

    def test_simple(self):
        class Point:
            pass

        table = self._fixture(Point)

        alias = aliased(Point)

        assert alias.id
        assert alias.x
        assert alias.y

        assert Point.id.__clause_element__().table is table
        assert alias.id.__clause_element__().table is not table

    def test_named_entity(self):
        class Point:
            pass

        self._fixture(Point)

        alias = aliased(Point, name="pp")

        self.assert_compile(
            select(alias), "SELECT pp.id, pp.x, pp.y FROM point AS pp"
        )

    def test_named_selectable(self):
        class Point:
            pass

        table = self._fixture(Point)

        alias = aliased(table, name="pp")

        self.assert_compile(
            select(alias), "SELECT pp.id, pp.x, pp.y FROM point AS pp"
        )

    def test_not_instantiatable(self):
        class Point:
            pass

        self._fixture(Point)
        alias = aliased(Point)

        assert_raises(TypeError, alias)

    def test_instancemethod(self):
        class Point:
            def zero(self):
                self.x, self.y = 0, 0

        self._fixture(Point)
        alias = aliased(Point)

        assert Point.zero

        assert getattr(alias, "zero")

    def test_classmethod(self):
        class Point:
            @classmethod
            def max_x(cls):
                return 100

        self._fixture(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert alias.max_x
        assert Point.max_x() == alias.max_x() == 100

    def test_simple_property(self):
        class Point:
            @property
            def max_x(self):
                return 100

        self._fixture(Point)
        alias = aliased(Point)

        assert Point.max_x
        assert Point.max_x != 100
        assert alias.max_x
        assert Point.max_x is alias.max_x

    def test_descriptors(self):
        class descriptor:
            def __init__(self, fn):
                self.fn = fn

            def __get__(self, obj, owner):
                if obj is not None:
                    return self.fn(obj, obj)
                else:
                    return self

            def method(self):
                return "method"

        class Point:
            center = (0, 0)

            @descriptor
            def thing(self, arg):
                return arg.center

        self._fixture(Point)
        alias = aliased(Point)

        assert Point.thing != (0, 0)
        assert Point().thing == (0, 0)
        assert Point.thing.method() == "method"

        assert alias.thing != (0, 0)
        assert alias.thing.method() == "method"

    def _assert_has_table(self, expr, table):
        from sqlalchemy import Column  # override testlib's override

        for child in expr.get_children():
            if isinstance(child, Column):
                assert child.table is table

    def test_hybrid_descriptor_one(self):
        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_method
            def left_of(self, other):
                return self.x < other.x

        self._fixture(Point)
        alias = aliased(Point)
        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.left_of(Point)),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x < point.x",
        )

    def test_hybrid_descriptor_two(self):
        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_property
            def double_x(self):
                return self.x * 2

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.double_x), "Point.double_x")
        eq_(str(alias.double_x), "aliased(Point).double_x")
        eq_(str(Point.double_x.__clause_element__()), "point.x * :x_1")
        eq_(str(alias.double_x.__clause_element__()), "point_1.x * :x_1")

        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.double_x > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x * :x_1 > point.x",
        )

    def test_hybrid_descriptor_three(self):
        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            @hybrid_property
            def x_alone(self):
                return self.x

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.x_alone), "Point.x_alone")
        eq_(str(alias.x_alone), "aliased(Point).x_alone")

        # from __clause_element__() perspective, Point.x_alone
        # and Point.x return the same thing, so that's good
        eq_(str(Point.x.__clause_element__()), "point.x")
        eq_(str(Point.x_alone.__clause_element__()), "point.x")

        # same for the alias
        eq_(str(alias.x + 1), "point_1.x + :x_1")
        eq_(str(alias.x_alone + 1), "point_1.x + :x_1")

        point_mapper = inspect(Point)

        eq_(
            Point.x_alone._annotations,
            {
                "entity_namespace": point_mapper,
                "parententity": point_mapper,
                "parentmapper": point_mapper,
                "proxy_key": "x_alone",
                "proxy_owner": point_mapper,
            },
        )
        eq_(
            Point.x._annotations,
            {
                "entity_namespace": point_mapper,
                "parententity": point_mapper,
                "parentmapper": point_mapper,
                "proxy_key": "x",
                "proxy_owner": point_mapper,
            },
        )

        eq_(str(alias.x_alone == alias.x), "point_1.x = point_1.x")

        a2 = aliased(Point)
        eq_(str(a2.x_alone == alias.x), "point_1.x = point_2.x")

        eq_(
            a2.x._annotations,
            {
                "entity_namespace": inspect(a2),
                "parententity": inspect(a2),
                "parentmapper": point_mapper,
                "proxy_key": "x",
                "proxy_owner": inspect(a2),
            },
        )

        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.x_alone > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x > point.x",
        )

    def test_proxy_descriptor_one(self):
        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

        self._fixture(Point, properties={"x_syn": synonym("x")})
        alias = aliased(Point)

        eq_(str(Point.x_syn), "Point.x_syn")
        eq_(str(alias.x_syn), "aliased(Point).x_syn")

        sess = fixture_session()
        self.assert_compile(
            sess.query(alias.x_syn).filter(alias.x_syn > Point.x_syn),
            "SELECT point_1.x AS point_1_x FROM point AS point_1, point "
            "WHERE point_1.x > point.x",
        )

    def test_meta_getattr_one(self):
        class MetaPoint(type):
            def __getattr__(cls, key):
                if key == "x_syn":
                    return cls.x
                raise AttributeError(key)

        class Point(metaclass=MetaPoint):
            pass

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.x_syn), "Point.x")
        eq_(str(alias.x_syn), "aliased(Point).x")

        # from __clause_element__() perspective, Point.x_syn
        # and Point.x return the same thing, so that's good
        eq_(str(Point.x.__clause_element__()), "point.x")
        eq_(str(Point.x_syn.__clause_element__()), "point.x")

        # same for the alias
        eq_(str(alias.x + 1), "point_1.x + :x_1")
        eq_(str(alias.x_syn + 1), "point_1.x + :x_1")

        is_(Point.x_syn.__clause_element__(), Point.x.__clause_element__())

        eq_(str(alias.x_syn == alias.x), "point_1.x = point_1.x")

        a2 = aliased(Point)
        eq_(str(a2.x_syn == alias.x), "point_1.x = point_2.x")

        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.x_syn > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x > point.x",
        )

    def test_meta_getattr_two(self):
        class MetaPoint(type):
            def __getattr__(cls, key):
                if key == "double_x":
                    return cls._impl_double_x
                raise AttributeError(key)

        class Point(metaclass=MetaPoint):
            @hybrid_property
            def _impl_double_x(self):
                return self.x * 2

        self._fixture(Point)
        alias = aliased(Point)

        eq_(str(Point.double_x), "Point._impl_double_x")
        eq_(str(alias.double_x), "aliased(Point)._impl_double_x")
        eq_(str(Point.double_x.__clause_element__()), "point.x * :x_1")
        eq_(str(alias.double_x.__clause_element__()), "point_1.x * :x_1")

        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.double_x > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x * :x_1 > point.x",
        )

    def test_meta_getattr_three(self):
        class MetaPoint(type):
            def __getattr__(cls, key):
                @hybrid_property
                def double_x(me):
                    return me.x * 2

                if key == "double_x":
                    return double_x.__get__(None, cls)
                raise AttributeError(key)

        class Point(metaclass=MetaPoint):
            pass

        self._fixture(Point)

        alias = aliased(Point)

        eq_(str(Point.double_x.__clause_element__()), "point.x * :x_1")
        eq_(str(alias.double_x.__clause_element__()), "point_1.x * :x_1")

        sess = fixture_session()

        self.assert_compile(
            sess.query(alias).filter(alias.double_x > Point.x),
            "SELECT point_1.id AS point_1_id, point_1.x AS point_1_x, "
            "point_1.y AS point_1_y FROM point AS point_1, point "
            "WHERE point_1.x * :x_1 > point.x",
        )

    def test_parententity_vs_parentmapper(self):
        class Point:
            pass

        self._fixture(Point, properties={"x_syn": synonym("x")})
        pa = aliased(Point)

        is_(Point.x_syn._parententity, inspect(Point))
        is_(Point.x._parententity, inspect(Point))
        is_(Point.x_syn._parentmapper, inspect(Point))
        is_(Point.x._parentmapper, inspect(Point))

        is_(
            Point.x_syn.__clause_element__()._annotations["parententity"],
            inspect(Point),
        )
        is_(
            Point.x.__clause_element__()._annotations["parententity"],
            inspect(Point),
        )
        is_(
            Point.x_syn.__clause_element__()._annotations["parentmapper"],
            inspect(Point),
        )
        is_(
            Point.x.__clause_element__()._annotations["parentmapper"],
            inspect(Point),
        )

        pa = aliased(Point)

        is_(pa.x_syn._parententity, inspect(pa))
        is_(pa.x._parententity, inspect(pa))
        is_(pa.x_syn._parentmapper, inspect(Point))
        is_(pa.x._parentmapper, inspect(Point))

        is_(
            pa.x_syn.__clause_element__()._annotations["parententity"],
            inspect(pa),
        )
        is_(
            pa.x.__clause_element__()._annotations["parententity"], inspect(pa)
        )
        is_(
            pa.x_syn.__clause_element__()._annotations["parentmapper"],
            inspect(Point),
        )
        is_(
            pa.x.__clause_element__()._annotations["parentmapper"],
            inspect(Point),
        )


class IdentityKeyTest(_fixtures.FixtureTest):
    run_inserts = None

    def _cases():
        return testing.combinations(
            (orm_util,), (Session,), argnames="ormutil"
        )

    @_cases()
    def test_identity_key_1(self, ormutil):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        key = ormutil.identity_key(User, [1])
        eq_(key, (User, (1,), None))
        key = ormutil.identity_key(User, ident=[1])
        eq_(key, (User, (1,), None))

    @_cases()
    def test_identity_key_scalar(self, ormutil):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        key = ormutil.identity_key(User, 1)
        eq_(key, (User, (1,), None))
        key = ormutil.identity_key(User, ident=1)
        eq_(key, (User, (1,), None))

    @_cases()
    def test_identity_key_2(self, ormutil):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = User(name="u1")
        s.add(u)
        s.flush()
        key = ormutil.identity_key(instance=u)
        eq_(key, (User, (u.id,), None))

    @_cases()
    @testing.combinations("dict", "row", "mapping", argnames="rowtype")
    def test_identity_key_3(self, ormutil, rowtype):
        """test a real Row works with identity_key.

        this was broken w/ 1.4 future mode as we are assuming a mapping
        here.  to prevent regressions, identity_key now accepts any of
        dict, RowMapping, Row for the "row".

        found_during_type_annotation


        """
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        if rowtype == "dict":
            row = {users.c.id: 1, users.c.name: "Frank"}
        elif rowtype in ("mapping", "row"):
            row = result.result_tuple([users.c.id, users.c.name])((1, "Frank"))
            if rowtype == "mapping":
                row = row._mapping

        key = ormutil.identity_key(User, row=row)
        eq_(key, (User, (1,), None))

    def test_identity_key_token(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        key = orm_util.identity_key(User, [1], identity_token="token")
        eq_(key, (User, (1,), "token"))
        key = orm_util.identity_key(User, ident=[1], identity_token="token")
        eq_(key, (User, (1,), "token"))


class PathRegistryTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_root_registry(self):
        umapper = inspect(self.classes.User)
        is_(RootRegistry()[umapper], umapper._path_registry)
        eq_(RootRegistry()[umapper], PathRegistry.coerce((umapper,)))

    def test_expand(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce((umapper,))

        eq_(
            path[umapper.attrs.addresses][amapper][
                amapper.attrs.email_address
            ],
            PathRegistry.coerce(
                (
                    umapper,
                    umapper.attrs.addresses,
                    amapper,
                    amapper.attrs.email_address,
                )
            ),
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
        path = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )
        is_(path[0], umapper)
        is_(path[2], amapper)

    def test_indexed_key_token(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                PathToken.intern(":*"),
            )
        )
        is_true(path.is_token)
        eq_(path[1], umapper.attrs.addresses)
        eq_(path[3], ":*")

        with expect_raises(IndexError):
            path[amapper]

    def test_slice_token(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                PathToken.intern(":*"),
            )
        )
        is_true(path.is_token)
        eq_(path[1:3], (umapper.attrs.addresses, amapper))

    def test_indexed_key(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )
        eq_(path[1], umapper.attrs.addresses)
        eq_(path[3], amapper.attrs.email_address)

    def test_slice(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        path = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )
        eq_(path[1:3], (umapper.attrs.addresses, amapper))

    def test_addition(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((amapper, amapper.attrs.email_address))
        eq_(
            p1 + p2,
            PathRegistry.coerce(
                (
                    umapper,
                    umapper.attrs.addresses,
                    amapper,
                    amapper.attrs.email_address,
                )
            ),
        )

    def test_length(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        pneg1 = PathRegistry.coerce(())
        p0 = PathRegistry.coerce((umapper,))
        p1 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )

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
        p6 = PathRegistry.coerce(
            (amapper, amapper.attrs.user, umapper, umapper.attrs.addresses)
        )
        p7 = PathRegistry.coerce(
            (
                amapper,
                amapper.attrs.user,
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )

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

    def test_eq_non_path(self):
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)
        u_alias = inspect(aliased(self.classes.User))
        p1 = PathRegistry.coerce((umapper,))
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        p3 = PathRegistry.coerce((u_alias, umapper.attrs.addresses))
        p4 = PathRegistry.coerce((u_alias, umapper.attrs.addresses, amapper))
        p5 = PathRegistry.coerce((u_alias,)).token(":*")

        non_object = 54.1432

        for obj in [p1, p2, p3, p4, p5]:
            with expect_warnings(
                "Comparison of PathRegistry to "
                "<.* 'float'> is not supported"
            ):
                is_(obj == non_object, False)

            with expect_warnings(
                "Comparison of PathRegistry to "
                "<.* 'float'> is not supported"
            ):
                is_(obj != non_object, True)

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

        eq_(p1.path, (umapper, umapper.attrs.addresses))
        eq_(p2.path, (umapper, umapper.attrs.addresses, amapper))
        eq_(p3.path, (amapper, amapper.attrs.email_address))

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
                ("p1key", p1.path): "p1value",
                ("p2key", p2.path): "p2value",
                ("p3key", p3.path): "p3value",
            },
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
                ("p1key", p1.path): "p1value",
                ("p2key", p2.path): "p2value",
                ("p3key", p3.path): "p3value",
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
                ("p1key", p1.path): "p1value",
                ("p2key", p2.path): "p2value",
                ("p3key", p3.path): "p3value",
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
        reg.update({("p1key", p1.path): "p1value"})

        p1.setdefault(reg, "p1key", "p1newvalue_a")
        p1.setdefault(reg, "p1key_new", "p1newvalue_b")
        p2.setdefault(reg, "p2key", "p2newvalue")
        eq_(
            reg,
            {
                ("p1key", p1.path): "p1value",
                ("p1key_new", p1.path): "p1newvalue_b",
                ("p2key", p2.path): "p2newvalue",
            },
        )

    def test_serialize(self):
        User = self.classes.User
        Address = self.classes.Address
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.addresses))
        eq_(p1.serialize(), [(User, "addresses"), (Address, "email_address")])
        eq_(p2.serialize(), [(User, "addresses"), (Address, None)])
        eq_(p3.serialize(), [(User, "addresses")])

    def test_deseralize(self):
        User = self.classes.User
        Address = self.classes.Address
        umapper = inspect(self.classes.User)
        amapper = inspect(self.classes.Address)

        p1 = PathRegistry.coerce(
            (
                umapper,
                umapper.attrs.addresses,
                amapper,
                amapper.attrs.email_address,
            )
        )
        p2 = PathRegistry.coerce((umapper, umapper.attrs.addresses, amapper))
        p3 = PathRegistry.coerce((umapper, umapper.attrs.addresses))

        eq_(
            PathRegistry.deserialize(
                [(User, "addresses"), (Address, "email_address")]
            ),
            p1,
        )
        eq_(
            PathRegistry.deserialize([(User, "addresses"), (Address, None)]),
            p2,
        )
        eq_(PathRegistry.deserialize([(User, "addresses")]), p3)


class PathRegistryInhTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = "once"
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
        eq_(p1.path, (emapper, emapper.attrs.machines))

    def test_plain_compound(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        cmapper = inspect(Company)
        pmapper = inspect(Person)
        emapper = inspect(Engineer)

        p1 = PathRegistry.coerce(
            (cmapper, cmapper.attrs.employees, pmapper, emapper.attrs.machines)
        )

        # given a mapper and an attribute on a subclass,
        # the path converts what you get to be against that subclass
        eq_(
            p1.path,
            (
                cmapper,
                cmapper.attrs.employees,
                emapper,
                emapper.attrs.machines,
            ),
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
        eq_(p1.path, (p_alias, emapper.attrs.machines))

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

        p1 = PathRegistry.coerce(
            (c_alias, cmapper.attrs.employees, p_alias, emapper.attrs.machines)
        )
        # plain AliasedClass - the path keeps that AliasedClass directly
        # as is in the path
        eq_(
            p1.path,
            (
                c_alias,
                cmapper.attrs.employees,
                p_alias,
                emapper.attrs.machines,
            ),
        )

    def test_with_poly_sub(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)
        cmapper = inspect(Company)

        p_poly = with_polymorphic(Person, [Engineer])
        e_poly_insp = inspect(p_poly.Engineer)  # noqa - used by comment below
        p_poly_insp = inspect(p_poly)

        p1 = PathRegistry.coerce((p_poly_insp, emapper.attrs.machines))

        # changes as of #5082: when a with_polymorphic is in the middle
        # of a path, the natural path makes sure it uses the base mappers,
        # however when it's at the root, the with_polymorphic stays in
        # the natural path

        # this behavior is the same as pre #5082, it was temporarily changed
        # but this proved to be incorrect.   The path starts on a
        # with_polymorphic(), so a Query will "naturally" construct a path
        # that comes from that wp.
        eq_(p1.path, (e_poly_insp, emapper.attrs.machines))
        eq_(p1.natural_path, (e_poly_insp, emapper.attrs.machines))

        # this behavior is new as of the final version of #5082.
        # the path starts on a normal entity and has a with_polymorphic
        # in the middle, for this to match what Query will generate it needs
        # to use the non aliased mappers in the natural path.
        p2 = PathRegistry.coerce(
            (
                cmapper,
                cmapper.attrs.employees,
                p_poly_insp,
                emapper.attrs.machines,
            )
        )
        eq_(
            p2.path,
            (
                cmapper,
                cmapper.attrs.employees,
                e_poly_insp,
                emapper.attrs.machines,
            ),
        )
        eq_(
            p2.natural_path,
            (
                cmapper,
                cmapper.attrs.employees,
                emapper,
                emapper.attrs.machines,
            ),
        )

    def test_with_poly_base_two(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        cmapper = inspect(Company)
        pmapper = inspect(Person)

        p_poly = with_polymorphic(Person, [Engineer])
        e_poly_insp = inspect(p_poly.Engineer)  # noqa - used by comment below
        p_poly_insp = inspect(p_poly)

        p1 = PathRegistry.coerce(
            (
                cmapper,
                cmapper.attrs.employees,
                p_poly_insp,
                pmapper.attrs.paperwork,
            )
        )

        eq_(
            p1.path,
            (
                cmapper,
                cmapper.attrs.employees,
                p_poly_insp,
                pmapper.attrs.paperwork,
            ),
        )
        eq_(
            p1.natural_path,
            (
                cmapper,
                cmapper.attrs.employees,
                pmapper,
                pmapper.attrs.paperwork,
            ),
        )

    def test_nonpoly_oftype_aliased_subclass_onroot(self):
        Engineer = _poly_fixtures.Engineer
        eng_alias = aliased(Engineer)
        ea_insp = inspect(eng_alias)

        p1 = PathRegistry.coerce((ea_insp, ea_insp.mapper.attrs.paperwork))

        eq_(p1.path, (ea_insp, ea_insp.mapper.attrs.paperwork))
        eq_(p1.natural_path, (ea_insp, ea_insp.mapper.attrs.paperwork))

    def test_nonpoly_oftype_aliased_subclass(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        cmapper = inspect(Company)
        pmapper = inspect(Person)
        eng_alias = aliased(Engineer)
        ea_insp = inspect(eng_alias)

        p1 = PathRegistry.coerce(
            (
                cmapper,
                cmapper.attrs.employees,
                ea_insp,
                ea_insp.mapper.attrs.paperwork,
            )
        )

        eq_(
            p1.path,
            (
                cmapper,
                cmapper.attrs.employees,
                ea_insp,
                ea_insp.mapper.attrs.paperwork,
            ),
        )
        eq_(
            p1.natural_path,
            (
                cmapper,
                cmapper.attrs.employees,
                pmapper,
                pmapper.attrs.paperwork,
            ),
        )

    def test_nonpoly_oftype_subclass(self):
        Company = _poly_fixtures.Company
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)
        cmapper = inspect(Company)
        pmapper = inspect(Person)

        p1 = PathRegistry.coerce(
            (
                cmapper,
                cmapper.attrs.employees,
                emapper,
                emapper.attrs.paperwork,
            )
        )

        eq_(
            p1.path,
            (
                cmapper,
                cmapper.attrs.employees,
                pmapper,
                pmapper.attrs.paperwork,
            ),
        )
        eq_(
            p1.natural_path,
            (
                cmapper,
                cmapper.attrs.employees,
                pmapper,
                pmapper.attrs.paperwork,
            ),
        )

    def test_with_poly_base_one(self):
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
        eq_(p1.path, (p_poly, pmapper.attrs.name))

    def test_with_poly_use_mapper(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer
        emapper = inspect(Engineer)

        p_poly = with_polymorphic(Person, [Engineer], _use_mapper_path=True)
        p_poly = inspect(p_poly)

        p1 = PathRegistry.coerce((p_poly, emapper.attrs.machines))

        # polymorphic AliasedClass with the "use_mapper_path" flag -
        # the AliasedClass acts just like the base mapper
        eq_(p1.path, (emapper, emapper.attrs.machines))
