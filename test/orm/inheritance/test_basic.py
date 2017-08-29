import warnings
from sqlalchemy.testing import eq_, is_, assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util, event
from sqlalchemy.orm import *
from sqlalchemy.orm.util import instance_str
from sqlalchemy.orm import exc as orm_exc, attributes
from sqlalchemy.testing.assertsql import AllOf, CompiledSQL, RegexSQL, Or
from sqlalchemy.sql import table, column
from sqlalchemy import testing
from sqlalchemy.testing import engines
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.testing.util import gc_collect


class O2MTest(fixtures.MappedTest):
    """deals with inheritance and one-to-many relationships"""
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
                    Column('id', Integer, primary_key=True,
                           test_needs_autoincrement=True),
                    Column('data', String(20)))

        bar = Table('bar', metadata,
                    Column('id', Integer, ForeignKey(
                        'foo.id'), primary_key=True),
                    Column('bar_data', String(20)))

        blub = Table('blub', metadata,
                     Column('id', Integer, ForeignKey(
                         'bar.id'), primary_key=True),
                     Column('foo_id', Integer, ForeignKey(
                         'foo.id'), nullable=False),
                     Column('blub_data', String(20)))

    def test_basic(self):
        class Foo(object):
            def __init__(self, data=None):
                self.data = data

            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)

        mapper(Bar, bar, inherits=Foo)

        class Blub(Bar):
            def __repr__(self):
                return "Blub id %d, data %s" % (self.id, self.data)

        mapper(Blub, blub, inherits=Bar, properties={
            'parent_foo': relationship(Foo)
        })

        sess = create_session()
        b1 = Blub("blub #1")
        b2 = Blub("blub #2")
        f = Foo("foo #1")
        sess.add(b1)
        sess.add(b2)
        sess.add(f)
        b1.parent_foo = f
        b2.parent_foo = f
        sess.flush()
        compare = ','.join([repr(b1), repr(b2), repr(b1.parent_foo),
                            repr(b2.parent_foo)])
        sess.expunge_all()
        result = sess.query(Blub).all()
        result_str = ','.join([repr(result[0]), repr(result[1]),
                               repr(result[0].parent_foo),
                               repr(result[1].parent_foo)])
        eq_(compare, result_str)
        eq_(result[0].parent_foo.data, 'foo #1')
        eq_(result[1].parent_foo.data, 'foo #1')


class PolyExpressionEagerLoad(fixtures.DeclarativeMappedTest):
    run_setup_mappers = 'once'
    __dialect__ = 'default'

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, Base):
            __tablename__ = 'a'

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            discriminator = Column(String(50), nullable=False)
            child_id = Column(Integer, ForeignKey('a.id'))
            child = relationship('A')

            p_a = case([
                (discriminator == "a", "a"),
            ], else_="b")

            __mapper_args__ = {
                'polymorphic_identity': 'a',
                "polymorphic_on": p_a,
            }

        class B(A):
            __mapper_args__ = {
                'polymorphic_identity': 'b'
            }

    @classmethod
    def insert_data(cls):
        A = cls.classes.A

        session = Session(testing.db)
        session.add_all([
            A(id=1, discriminator='a'),
            A(id=2, discriminator='b', child_id=1),
            A(id=3, discriminator='c', child_id=1),
        ])
        session.commit()

    def test_joinedload(self):
        A = self.classes.A
        B = self.classes.B

        session = Session(testing.db)
        result = session.query(A).filter_by(child_id=None).\
            options(joinedload('child')).one()

        eq_(
            result,
            A(id=1, discriminator='a', child=[B(id=2), B(id=3)]),
        )


class PolymorphicResolutionMultiLevel(fixtures.DeclarativeMappedTest,
                                      testing.AssertsCompiledSQL):
    run_setup_mappers = 'once'
    __dialect__ = 'default'

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)

        class B(A):
            __tablename__ = 'b'
            id = Column(Integer, ForeignKey('a.id'), primary_key=True)

        class C(A):
            __tablename__ = 'c'
            id = Column(Integer, ForeignKey('a.id'), primary_key=True)

        class D(B):
            __tablename__ = 'd'
            id = Column(Integer, ForeignKey('b.id'), primary_key=True)

    def test_ordered_b_d(self):
        a_mapper = inspect(self.classes.A)
        eq_(
            a_mapper._mappers_from_spec(
                [self.classes.B, self.classes.D], None),
            [a_mapper, inspect(self.classes.B), inspect(self.classes.D)]
        )

    def test_a(self):
        a_mapper = inspect(self.classes.A)
        eq_(a_mapper._mappers_from_spec([self.classes.A], None), [a_mapper])

    def test_b_d_selectable(self):
        a_mapper = inspect(self.classes.A)
        spec = [self.classes.D, self.classes.B]
        eq_(
            a_mapper._mappers_from_spec(
                spec,
                self.classes.B.__table__.join(self.classes.D.__table__)
            ),
            [inspect(self.classes.B), inspect(self.classes.D)]
        )

    def test_d_selectable(self):
        a_mapper = inspect(self.classes.A)
        spec = [self.classes.D]
        eq_(
            a_mapper._mappers_from_spec(
                spec,
                self.classes.B.__table__.join(self.classes.D.__table__)
            ),
            [inspect(self.classes.D)]
        )

    def test_reverse_d_b(self):
        a_mapper = inspect(self.classes.A)
        spec = [self.classes.D, self.classes.B]
        eq_(
            a_mapper._mappers_from_spec(spec, None),
            [a_mapper, inspect(self.classes.B), inspect(self.classes.D)]
        )
        mappers, selectable = a_mapper._with_polymorphic_args(spec=spec)
        self.assert_compile(selectable,
                            "a LEFT OUTER JOIN b ON a.id = b.id "
                            "LEFT OUTER JOIN d ON b.id = d.id")

    def test_d_b_missing(self):
        a_mapper = inspect(self.classes.A)
        spec = [self.classes.D]
        eq_(
            a_mapper._mappers_from_spec(spec, None),
            [a_mapper, inspect(self.classes.B), inspect(self.classes.D)]
        )
        mappers, selectable = a_mapper._with_polymorphic_args(spec=spec)
        self.assert_compile(selectable,
                            "a LEFT OUTER JOIN b ON a.id = b.id "
                            "LEFT OUTER JOIN d ON b.id = d.id")

    def test_d_c_b(self):
        a_mapper = inspect(self.classes.A)
        spec = [self.classes.D, self.classes.C, self.classes.B]
        ms = a_mapper._mappers_from_spec(spec, None)

        eq_(
            ms[-1], inspect(self.classes.D)
        )
        eq_(ms[0], a_mapper)
        eq_(
            set(ms[1:3]), set(a_mapper._inheriting_mappers)
        )


class PolymorphicOnNotLocalTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('x', String(10)),
                   Column('q', String(10)))
        t2 = Table('t2', metadata,
                   Column('t2id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('y', String(10)),
                   Column('xid', ForeignKey('t1.id')))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Child(Parent):
            pass

    def test_non_col_polymorphic_on(self):
        Parent = self.classes.Parent
        t2 = self.tables.t2
        assert_raises_message(
            sa_exc.ArgumentError,
            "Can't determine polymorphic_on "
            "value 'im not a column' - no "
            "attribute is mapped to this name.",
            mapper,
            Parent, t2, polymorphic_on="im not a column"
        )

    def test_polymorphic_on_non_expr_prop(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent = self.classes.Parent

        t1t2_join = select([t1.c.x], from_obj=[t1.join(t2)]).alias()

        def go():
            interface_m = mapper(Parent, t2,
                                 polymorphic_on=lambda: "hi",
                                 polymorphic_identity=0)

        assert_raises_message(
            sa_exc.ArgumentError,
            "Only direct column-mapped property or "
            "SQL expression can be passed for polymorphic_on",
            go
        )

    def test_polymorphic_on_not_present_col(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent = self.classes.Parent
        t1t2_join = select([t1.c.x], from_obj=[t1.join(t2)]).alias()

        def go():
            t1t2_join_2 = select([t1.c.q], from_obj=[t1.join(t2)]).alias()
            interface_m = mapper(Parent, t2,
                                 polymorphic_on=t1t2_join.c.x,
                                 with_polymorphic=('*', t1t2_join_2),
                                 polymorphic_identity=0)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not map polymorphic_on column 'x' to the mapped table - "
            "polymorphic loads will not function properly",
            go
        )

    def test_polymorphic_on_only_in_with_poly(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent = self.classes.Parent
        t1t2_join = select([t1.c.x], from_obj=[t1.join(t2)]).alias()
        # if its in the with_polymorphic, then its OK
        mapper(Parent, t2,
               polymorphic_on=t1t2_join.c.x,
               with_polymorphic=('*', t1t2_join),
               polymorphic_identity=0)

    def test_polymorpic_on_not_in_with_poly(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent = self.classes.Parent

        t1t2_join = select([t1.c.x], from_obj=[t1.join(t2)]).alias()

        # if with_polymorphic, but its not present, not OK
        def go():
            t1t2_join_2 = select([t1.c.q], from_obj=[t1.join(t2)]).alias()
            interface_m = mapper(Parent, t2,
                                 polymorphic_on=t1t2_join.c.x,
                                 with_polymorphic=('*', t1t2_join_2),
                                 polymorphic_identity=0)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not map polymorphic_on column 'x' "
            "to the mapped table - "
            "polymorphic loads will not function properly",
            go
        )

    def test_polymorphic_on_expr_explicit_map(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ])
        mapper(Parent, t1, properties={
            "discriminator": column_property(expr)
        }, polymorphic_identity="parent",
            polymorphic_on=expr)
        mapper(Child, t2, inherits=Parent,
               polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_expr_implicit_map_no_label_joined(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ])
        mapper(Parent, t1, polymorphic_identity="parent",
               polymorphic_on=expr)
        mapper(Child, t2, inherits=Parent, polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_expr_implicit_map_w_label_joined(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ]).label(None)
        mapper(Parent, t1, polymorphic_identity="parent",
               polymorphic_on=expr)
        mapper(Child, t2, inherits=Parent, polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_expr_implicit_map_no_label_single(self):
        """test that single_table_criterion is propagated
        with a standalone expr"""
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ])
        mapper(Parent, t1, polymorphic_identity="parent",
               polymorphic_on=expr)
        mapper(Child, inherits=Parent, polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_expr_implicit_map_w_label_single(self):
        """test that single_table_criterion is propagated
        with a standalone expr"""
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ]).label(None)
        mapper(Parent, t1, polymorphic_identity="parent",
               polymorphic_on=expr)
        mapper(Child, inherits=Parent, polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_column_prop(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ])
        cprop = column_property(expr)
        mapper(Parent, t1, properties={
            "discriminator": cprop
        }, polymorphic_identity="parent",
            polymorphic_on=cprop)
        mapper(Child, t2, inherits=Parent,
               polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_column_str_prop(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        expr = case([
            (t1.c.x == "p", "parent"),
            (t1.c.x == "c", "child"),
        ])
        cprop = column_property(expr)
        mapper(Parent, t1, properties={
            "discriminator": cprop
        }, polymorphic_identity="parent",
            polymorphic_on="discriminator")
        mapper(Child, t2, inherits=Parent,
               polymorphic_identity="child")

        self._roundtrip(parent_ident='p', child_ident='c')

    def test_polymorphic_on_synonym(self):
        t2, t1 = self.tables.t2, self.tables.t1
        Parent, Child = self.classes.Parent, self.classes.Child
        cprop = column_property(t1.c.x)
        assert_raises_message(
            sa_exc.ArgumentError,
            "Only direct column-mapped property or "
            "SQL expression can be passed for polymorphic_on",
            mapper, Parent, t1, properties={
                "discriminator": cprop,
                "discrim_syn": synonym(cprop)
            }, polymorphic_identity="parent",
            polymorphic_on="discrim_syn")

    def _roundtrip(self, set_event=True, parent_ident='parent',
                   child_ident='child'):
        Parent, Child = self.classes.Parent, self.classes.Child

        # locate the "polymorphic_on" ColumnProperty.   This isn't
        # "officially" stored at the moment so do some heuristics to find it.
        parent_mapper = inspect(Parent)
        for prop in parent_mapper.column_attrs:
            if not prop.instrument:
                break
        else:
            prop = parent_mapper._columntoproperty[
                parent_mapper.polymorphic_on]

        # then make sure the column we will query on matches.
        is_(
            parent_mapper.polymorphic_on,
            prop.columns[0]
        )

        if set_event:
            @event.listens_for(Parent, "init", propagate=True)
            def set_identity(instance, *arg, **kw):
                ident = object_mapper(instance).polymorphic_identity
                if ident == 'parent':
                    instance.x = parent_ident
                elif ident == 'child':
                    instance.x = child_ident
                else:
                    assert False, "Got unexpected identity %r" % ident

        s = Session(testing.db)
        s.add_all([
            Parent(q="p1"),
            Child(q="c1", y="c1"),
            Parent(q="p2"),
        ])
        s.commit()
        s.close()

        eq_(
            [type(t) for t in s.query(Parent).order_by(Parent.id)],
            [Parent, Child, Parent]
        )

        eq_(
            [type(t) for t in s.query(Child).all()],
            [Child]
        )


class SortOnlyOnImportantFKsTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('b_id', Integer,
                     ForeignKey('b.id', use_alter=True, name='b_fk')))
        Table('b', metadata,
              Column('id', Integer, ForeignKey('a.id'), primary_key=True))

    @classmethod
    def setup_classes(cls):
        Base = declarative_base()

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            b_id = Column(Integer, ForeignKey('b.id'))

        class B(A):
            __tablename__ = "b"

            id = Column(Integer, ForeignKey('a.id'), primary_key=True)

            __mapper_args__ = {'inherit_condition': id == A.id}

        cls.classes.A = A
        cls.classes.B = B

    def test_flush(self):
        s = Session(testing.db)
        s.add(self.classes.B())
        s.flush()


class FalseDiscriminatorTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('type', Boolean, nullable=False))

    def test_false_on_sub(self):
        class Foo(object):
            pass

        class Bar(Foo):
            pass
        mapper(Foo, t1, polymorphic_on=t1.c.type, polymorphic_identity=True)
        mapper(Bar, inherits=Foo, polymorphic_identity=False)
        sess = create_session()
        b1 = Bar()
        sess.add(b1)
        sess.flush()
        assert b1.type is False
        sess.expunge_all()
        assert isinstance(sess.query(Foo).one(), Bar)

    def test_false_on_base(self):
        class Ding(object):
            pass

        class Bat(Ding):
            pass
        mapper(Ding, t1, polymorphic_on=t1.c.type, polymorphic_identity=False)
        mapper(Bat, inherits=Ding, polymorphic_identity=True)
        sess = create_session()
        d1 = Ding()
        sess.add(d1)
        sess.flush()
        assert d1.type is False
        sess.expunge_all()
        assert sess.query(Ding).one() is not None


class PolymorphicSynonymTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('type', String(10), nullable=False),
                   Column('info', String(255)))
        t2 = Table('t2', metadata,
                   Column('id', Integer, ForeignKey('t1.id'),
                          primary_key=True),
                   Column('data', String(10), nullable=False))

    def test_polymorphic_synonym(self):
        class T1(fixtures.ComparableEntity):
            def info(self):
                return "THE INFO IS:" + self._info

            def _set_info(self, x):
                self._info = x
            info = property(info, _set_info)

        class T2(T1):
            pass

        mapper(T1, t1, polymorphic_on=t1.c.type, polymorphic_identity='t1',
               properties={'info': synonym('_info', map_column=True)})
        mapper(T2, t2, inherits=T1, polymorphic_identity='t2')
        sess = create_session()
        at1 = T1(info='at1')
        at2 = T2(info='at2', data='t2 data')
        sess.add(at1)
        sess.add(at2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(T2).filter(T2.info == 'at2').one(), at2)
        eq_(at2.info, "THE INFO IS:at2")


class PolymorphicAttributeManagementTest(fixtures.MappedTest):
    """Test polymorphic_on can be assigned, can be mirrored, etc."""

    run_setup_mappers = 'once'

    @classmethod
    def define_tables(cls, metadata):
        Table('table_a', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('class_name', String(50)))
        Table('table_b', metadata,
              Column('id', Integer, ForeignKey('table_a.id'),
                     primary_key=True),
              Column('class_name', String(50)))
        Table('table_c', metadata,
              Column('id', Integer, ForeignKey('table_b.id'),
                     primary_key=True),
              Column('data', String(10)))

    @classmethod
    def setup_classes(cls):
        table_b, table_c, table_a = (cls.tables.table_b,
                                     cls.tables.table_c,
                                     cls.tables.table_a)

        class A(cls.Basic):
            pass

        class B(A):
            pass

        class C(B):
            pass

        class D(B):
            pass

        mapper(A, table_a,
               polymorphic_on=table_a.c.class_name,
               polymorphic_identity='a')
        mapper(B, table_b, inherits=A, polymorphic_on=table_b.c.class_name,
               polymorphic_identity='b',
               properties=dict(
                   class_name=[table_a.c.class_name, table_b.c.class_name]))
        mapper(C, table_c, inherits=B, polymorphic_identity='c')
        mapper(D, inherits=B, polymorphic_identity='d')

    def test_poly_configured_immediate(self):
        A, C, B = (self.classes.A, self.classes.C, self.classes.B)

        a = A()
        b = B()
        c = C()
        eq_(a.class_name, 'a')
        eq_(b.class_name, 'b')
        eq_(c.class_name, 'c')

    def test_base_class(self):
        A, C, B = (self.classes.A, self.classes.C, self.classes.B)

        sess = Session()
        c1 = C()
        sess.add(c1)
        sess.commit()

        assert isinstance(sess.query(B).first(), C)

        sess.close()

        assert isinstance(sess.query(A).first(), C)

    def test_valid_assignment_upwards(self):
        """test that we can assign 'd' to a B, since B/D
        both involve the same set of tables.
        """
        D, B = self.classes.D, self.classes.B

        sess = Session()
        b1 = B()
        b1.class_name = 'd'
        sess.add(b1)
        sess.commit()
        sess.close()
        assert isinstance(sess.query(B).first(), D)

    def test_invalid_assignment_downwards(self):
        """test that we warn on assign of 'b' to a C, since this adds
        a row to the C table we'd never load.
        """
        C = self.classes.C

        sess = Session()
        c1 = C()
        c1.class_name = 'b'
        sess.add(c1)
        assert_raises_message(
            sa_exc.SAWarning,
            "Flushing object %s with incompatible "
            "polymorphic identity 'b'; the object may not "
            "refresh and/or load correctly" % instance_str(c1),
            sess.flush
        )

    def test_invalid_assignment_upwards(self):
        """test that we warn on assign of 'c' to a B, since we will have a
        "C" row that has no joined row, which will cause object
        deleted errors.
        """
        B = self.classes.B

        sess = Session()
        b1 = B()
        b1.class_name = 'c'
        sess.add(b1)
        assert_raises_message(
            sa_exc.SAWarning,
            "Flushing object %s with incompatible "
            "polymorphic identity 'c'; the object may not "
            "refresh and/or load correctly" % instance_str(b1),
            sess.flush
        )

    def test_entirely_oob_assignment(self):
        """test warn on an unknown polymorphic identity.
        """
        B = self.classes.B

        sess = Session()
        b1 = B()
        b1.class_name = 'xyz'
        sess.add(b1)
        assert_raises_message(
            sa_exc.SAWarning,
            "Flushing object %s with incompatible "
            "polymorphic identity 'xyz'; the object may not "
            "refresh and/or load correctly" % instance_str(b1),
            sess.flush
        )

    def test_not_set_on_upate(self):
        C = self.classes.C

        sess = Session()
        c1 = C()
        sess.add(c1)
        sess.commit()
        sess.expire(c1)

        c1.data = 'foo'
        sess.flush()

    def test_validate_on_upate(self):
        C = self.classes.C

        sess = Session()
        c1 = C()
        sess.add(c1)
        sess.commit()
        sess.expire(c1)

        c1.class_name = 'b'
        assert_raises_message(
            sa_exc.SAWarning,
            "Flushing object %s with incompatible "
            "polymorphic identity 'b'; the object may not "
            "refresh and/or load correctly" % instance_str(c1),
            sess.flush
        )


class CascadeTest(fixtures.MappedTest):
    """that cascades on polymorphic relationships continue
    cascading along the path of the instance's mapper, not
    the base mapper."""

    @classmethod
    def define_tables(cls, metadata):
        global t1, t2, t3, t4
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('data', String(30)))

        t2 = Table('t2', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('t1id', Integer, ForeignKey('t1.id')),
                   Column('type', String(30)),
                   Column('data', String(30)))
        t3 = Table('t3', metadata,
                   Column('id', Integer, ForeignKey('t2.id'),
                          primary_key=True),
                   Column('moredata', String(30)))

        t4 = Table('t4', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('t3id', Integer, ForeignKey('t3.id')),
                   Column('data', String(30)))

    def test_cascade(self):
        class T1(fixtures.BasicEntity):
            pass

        class T2(fixtures.BasicEntity):
            pass

        class T3(T2):
            pass

        class T4(fixtures.BasicEntity):
            pass

        mapper(T1, t1, properties={
            't2s': relationship(T2, cascade="all")
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(T3, t3, inherits=T2, polymorphic_identity='t3', properties={
            't4s': relationship(T4, cascade="all")
        })
        mapper(T4, t4)

        sess = create_session()
        t1_1 = T1(data='t1')

        t3_1 = T3(data='t3', moredata='t3')
        t2_1 = T2(data='t2')

        t1_1.t2s.append(t2_1)
        t1_1.t2s.append(t3_1)

        t4_1 = T4(data='t4')
        t3_1.t4s.append(t4_1)

        sess.add(t1_1)

        assert t4_1 in sess.new
        sess.flush()

        sess.delete(t1_1)
        assert t4_1 in sess.deleted
        sess.flush()


class M2OUseGetTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('type', String(30)))
        Table('sub', metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True))
        Table('related', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('sub_id', Integer, ForeignKey('sub.id')))

    def test_use_get(self):
        base, sub, related = (self.tables.base,
                              self.tables.sub,
                              self.tables.related)

        # test [ticket:1186]
        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass

        class Related(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='b')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='s')
        mapper(Related, related, properties={
            # previously, this was needed for the comparison to occur:
            # the 'primaryjoin' looks just like "Sub"'s "get" clause
            # (based on the Base id), and foreign_keys since that join
            # condition doesn't actually have any fks in it
            # 'sub':relationship(Sub, primaryjoin=base.c.id==related.c.sub_id,
            # foreign_keys=related.c.sub_id)

            # now we can use this:
            'sub': relationship(Sub)
        })

        assert class_mapper(Related).get_property('sub').strategy.use_get

        sess = create_session()
        s1 = Sub()
        r1 = Related(sub=s1)
        sess.add(r1)
        sess.flush()
        sess.expunge_all()

        r1 = sess.query(Related).first()
        s1 = sess.query(Sub).first()

        def go():
            assert r1.sub
        self.assert_sql_count(testing.db, go, 0)


class GetTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
                    Column('id', Integer, primary_key=True,
                           test_needs_autoincrement=True),
                    Column('type', String(30)),
                    Column('data', String(20)))

        bar = Table('bar', metadata,
                    Column('id', Integer, ForeignKey(
                        'foo.id'), primary_key=True),
                    Column('bar_data', String(20)))

        blub = Table('blub', metadata,
                     Column('blub_id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('foo_id', Integer, ForeignKey('foo.id')),
                     Column('bar_id', Integer, ForeignKey('bar.id')),
                     Column('blub_data', String(20)))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(Foo):
            pass

        class Blub(Bar):
            pass

    def test_get_polymorphic(self):
        self._do_get_test(True)

    def test_get_nonpolymorphic(self):
        self._do_get_test(False)

    def _do_get_test(self, polymorphic):
        foo, Bar, Blub, blub, bar, Foo = (self.tables.foo,
                                          self.classes.Bar,
                                          self.classes.Blub,
                                          self.tables.blub,
                                          self.tables.bar,
                                          self.classes.Foo)

        if polymorphic:
            mapper(Foo, foo, polymorphic_on=foo.c.type,
                   polymorphic_identity='foo')
            mapper(Bar, bar, inherits=Foo, polymorphic_identity='bar')
            mapper(Blub, blub, inherits=Bar, polymorphic_identity='blub')
        else:
            mapper(Foo, foo)
            mapper(Bar, bar, inherits=Foo)
            mapper(Blub, blub, inherits=Bar)

        sess = create_session()
        f = Foo()
        b = Bar()
        bl = Blub()
        sess.add(f)
        sess.add(b)
        sess.add(bl)
        sess.flush()

        if polymorphic:
            def go():
                assert sess.query(Foo).get(f.id) is f
                assert sess.query(Foo).get(b.id) is b
                assert sess.query(Foo).get(bl.id) is bl
                assert sess.query(Bar).get(b.id) is b
                assert sess.query(Bar).get(bl.id) is bl
                assert sess.query(Blub).get(bl.id) is bl

                # test class mismatches - item is present
                # in the identity map but we requested a subclass
                assert sess.query(Blub).get(f.id) is None
                assert sess.query(Blub).get(b.id) is None
                assert sess.query(Bar).get(f.id) is None

            self.assert_sql_count(testing.db, go, 0)
        else:
            # this is testing the 'wrong' behavior of using get()
            # polymorphically with mappers that are not configured to be
            # polymorphic.  the important part being that get() always
            # returns an instance of the query's type.
            def go():
                assert sess.query(Foo).get(f.id) is f

                bb = sess.query(Foo).get(b.id)
                assert isinstance(b, Foo) and bb.id == b.id

                bll = sess.query(Foo).get(bl.id)
                assert isinstance(bll, Foo) and bll.id == bl.id

                assert sess.query(Bar).get(b.id) is b

                bll = sess.query(Bar).get(bl.id)
                assert isinstance(bll, Bar) and bll.id == bl.id

                assert sess.query(Blub).get(bl.id) is bl

            self.assert_sql_count(testing.db, go, 3)


class EagerLazyTest(fixtures.MappedTest):
    """tests eager load/lazy load of child items off inheritance mappers, tests
    that LazyLoader constructs the right query condition."""

    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, bar_foo
        foo = Table('foo', metadata,
                    Column('id', Integer, primary_key=True,
                           test_needs_autoincrement=True),
                    Column('data', String(30)))
        bar = Table('bar', metadata,
                    Column('id', Integer, ForeignKey(
                        'foo.id'), primary_key=True),
                    Column('bar_data', String(30)))

        bar_foo = Table('bar_foo', metadata,
                        Column('bar_id', Integer, ForeignKey('bar.id')),
                        Column('foo_id', Integer, ForeignKey('foo.id')))

    def test_basic(self):
        class Foo(object):
            pass

        class Bar(Foo):
            pass

        foos = mapper(Foo, foo)
        bars = mapper(Bar, bar, inherits=foos)
        bars.add_property('lazy', relationship(foos, bar_foo, lazy='select'))
        bars.add_property('eager', relationship(foos, bar_foo, lazy='joined'))

        foo.insert().execute(data='foo1')
        bar.insert().execute(id=1, data='bar1')

        foo.insert().execute(data='foo2')
        bar.insert().execute(id=2, data='bar2')

        foo.insert().execute(data='foo3')  # 3
        foo.insert().execute(data='foo4')  # 4

        bar_foo.insert().execute(bar_id=1, foo_id=3)
        bar_foo.insert().execute(bar_id=2, foo_id=4)

        sess = create_session()
        q = sess.query(Bar)
        self.assert_(len(q.first().lazy) == 1)
        self.assert_(len(q.first().eager) == 1)


class EagerTargetingTest(fixtures.MappedTest):
    """test a scenario where joined table inheritance might be
    confused as an eagerly loaded joined table."""

    @classmethod
    def define_tables(cls, metadata):
        Table('a_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)),
              Column('type', String(30), nullable=False),
              Column('parent_id', Integer, ForeignKey('a_table.id')))

        Table('b_table', metadata,
              Column('id', Integer, ForeignKey(
                  'a_table.id'), primary_key=True),
              Column('b_data', String(50)))

    def test_adapt_stringency(self):
        b_table, a_table = self.tables.b_table, self.tables.a_table

        class A(fixtures.ComparableEntity):
            pass

        class B(A):
            pass

        mapper(
            A, a_table, polymorphic_on=a_table.c.type,
            polymorphic_identity='A',
            properties={'children': relationship(A, order_by=a_table.c.name)})

        mapper(B, b_table, inherits=A, polymorphic_identity='B', properties={
            'b_derived': column_property(b_table.c.b_data + "DATA")
        })

        sess = create_session()

        b1 = B(id=1, name='b1', b_data='i')
        sess.add(b1)
        sess.flush()

        b2 = B(id=2, name='b2', b_data='l', parent_id=1)
        sess.add(b2)
        sess.flush()

        bid = b1.id

        sess.expunge_all()
        node = sess.query(B).filter(B.id == bid).all()[0]
        eq_(node, B(id=1, name='b1', b_data='i'))
        eq_(node.children[0], B(id=2, name='b2', b_data='l'))

        sess.expunge_all()
        node = sess.query(B).options(joinedload(B.children))\
            .filter(B.id == bid).all()[0]
        eq_(node, B(id=1, name='b1', b_data='i'))
        eq_(node.children[0], B(id=2, name='b2', b_data='l'))


class FlushTest(fixtures.MappedTest):
    """test dependency sorting among inheriting mappers"""

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('email', String(128)),
              Column('password', String(16)))

        Table('roles', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('description', String(32)))

        Table('user_roles', metadata,
              Column('user_id', Integer, ForeignKey(
                  'users.id'), primary_key=True),
              Column('role_id', Integer, ForeignKey(
                  'roles.id'), primary_key=True)
              )

        Table('admins', metadata,
              Column('admin_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', Integer, ForeignKey('users.id')))

    def test_one(self):
        admins, users, roles, user_roles = (self.tables.admins,
                                            self.tables.users,
                                            self.tables.roles,
                                            self.tables.user_roles)

        class User(object):
            pass

        class Role(object):
            pass

        class Admin(User):
            pass
        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties={
            'roles': relationship(Role, secondary=user_roles, lazy='joined')})
        admin_mapper = mapper(Admin, admins, inherits=user_mapper)
        sess = create_session()
        adminrole = Role()
        sess.add(adminrole)
        sess.flush()

        # create an Admin, and append a Role.  the dependency processors
        # corresponding to the "roles" attribute for the Admin mapper and the
        # User mapper have to ensure that two dependency processors don't fire
        # off and insert the many to many row twice.
        a = Admin()
        a.roles.append(adminrole)
        a.password = 'admin'
        sess.add(a)
        sess.flush()

        eq_(select([func.count('*')]).select_from(user_roles).scalar(), 1)

    def test_two(self):
        admins, users, roles, user_roles = (self.tables.admins,
                                            self.tables.users,
                                            self.tables.roles,
                                            self.tables.user_roles)

        class User(object):
            def __init__(self, email=None, password=None):
                self.email = email
                self.password = password

        class Role(object):
            def __init__(self, description=None):
                self.description = description

        class Admin(User):
            pass

        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties={
            'roles': relationship(Role, secondary=user_roles, lazy='joined')})

        admin_mapper = mapper(Admin, admins, inherits=user_mapper)

        # create roles
        adminrole = Role('admin')

        sess = create_session()
        sess.add(adminrole)
        sess.flush()

        # create admin user
        a = Admin(email='tim', password='admin')
        a.roles.append(adminrole)
        sess.add(a)
        sess.flush()

        a.password = 'sadmin'
        sess.flush()
        eq_(select([func.count('*')]).select_from(user_roles).scalar(), 1)


class PassiveDeletesTest(fixtures.MappedTest):
    __requires__ = ('foreign_keys',)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a", metadata,
            Column('id', Integer, primary_key=True),
            Column('type', String(30))
        )
        Table(
            "b", metadata,
            Column(
                'id', Integer, ForeignKey('a.id', ondelete="CASCADE"),
                primary_key=True),
            Column('data', String(10))
        )

        Table(
            "c", metadata,
            Column('cid', Integer, primary_key=True),
            Column('bid', ForeignKey('b.id', ondelete="CASCADE"))
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(A):
            pass

        class C(B):
            pass

    def _fixture(self, a_p=False, b_p=False, c_p=False):
        A, B, C = self.classes("A", "B", "C")
        a, b, c = self.tables("a", "b", "c")

        mapper(
            A, a, passive_deletes=a_p,
            polymorphic_on=a.c.type, polymorphic_identity='a')
        mapper(
            B, b, inherits=A, passive_deletes=b_p, polymorphic_identity='b')
        mapper(
            C, c, inherits=B, passive_deletes=c_p, polymorphic_identity='c')

    def test_none(self):
        A, B, C = self.classes("A", "B", "C")
        self._fixture()

        s = Session()
        a1, b1, c1 = A(id=1), B(id=2), C(cid=1, id=3)
        s.add_all([a1, b1, c1])
        s.commit()

        # want to see if the 'C' table loads even though
        # a and b are loaded
        c1 = s.query(B).filter_by(id=3).first()
        s.delete(c1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            RegexSQL(
                "SELECT .* "
                "FROM c WHERE :param_1 = c.bid",
                [{'param_1': 3}]
            ),
            CompiledSQL(
                "DELETE FROM c WHERE c.cid = :cid",
                [{'cid': 1}]
            ),
            CompiledSQL(
                "DELETE FROM b WHERE b.id = :id",
                [{'id': 3}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 3}]
            )
        )

    def test_c_only(self):
        A, B, C = self.classes("A", "B", "C")
        self._fixture(c_p=True)

        s = Session()
        a1, b1, c1 = A(id=1), B(id=2), C(cid=1, id=3)
        s.add_all([a1, b1, c1])
        s.commit()

        s.delete(a1)

        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type "
                "FROM a WHERE a.id = :param_1",
                [{'param_1': 1}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 1}]
            )
        )

        b1.id
        s.delete(b1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM b WHERE b.id = :id",
                [{'id': 2}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 2}]
            )
        )

        # want to see if the 'C' table loads even though
        # a and b are loaded
        c1 = s.query(A).filter_by(id=3).first()
        s.delete(c1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM b WHERE b.id = :id",
                [{'id': 3}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 3}]
            )
        )

    def test_b_only(self):
        A, B, C = self.classes("A", "B", "C")
        self._fixture(b_p=True)

        s = Session()
        a1, b1, c1 = A(id=1), B(id=2), C(cid=1, id=3)
        s.add_all([a1, b1, c1])
        s.commit()

        s.delete(a1)

        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type "
                "FROM a WHERE a.id = :param_1",
                [{'param_1': 1}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 1}]
            )
        )

        b1.id
        s.delete(b1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 2}]
            )
        )

        c1.id
        s.delete(c1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 3}]
            )
        )

    def test_a_only(self):
        A, B, C = self.classes("A", "B", "C")
        self._fixture(a_p=True)

        s = Session()
        a1, b1, c1 = A(id=1), B(id=2), C(cid=1, id=3)
        s.add_all([a1, b1, c1])
        s.commit()

        s.delete(a1)

        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type "
                "FROM a WHERE a.id = :param_1",
                [{'param_1': 1}]
            ),
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 1}]
            )
        )

        b1.id
        s.delete(b1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 2}]
            )
        )

        # want to see if the 'C' table loads even though
        # a and b are loaded
        c1 = s.query(A).filter_by(id=3).first()
        s.delete(c1)
        with self.sql_execution_asserter(testing.db) as asserter:
            s.flush()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM a WHERE a.id = :id",
                [{'id': 3}]
            )
        )


class OptimizedGetOnDeferredTest(fixtures.MappedTest):
    """test that the 'optimized get' path accommodates deferred columns."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a", metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True)
        )
        Table(
            "b", metadata,
            Column('id', Integer, ForeignKey('a.id'), primary_key=True),
            Column('data', String(10))
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(A):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B = cls.classes("A", "B")
        a, b = cls.tables("a", "b")

        mapper(A, a)
        mapper(B, b, inherits=A, properties={
            'data': deferred(b.c.data),
            'expr': column_property(b.c.data + 'q', deferred=True)
        })

    def test_column_property(self):
        A, B = self.classes("A", "B")
        sess = Session()
        b1 = B(data='x')
        sess.add(b1)
        sess.flush()

        eq_(b1.expr, 'xq')

    def test_expired_column(self):
        A, B = self.classes("A", "B")
        sess = Session()
        b1 = B(data='x')
        sess.add(b1)
        sess.flush()
        sess.expire(b1, ['data'])

        eq_(b1.data, 'x')


class JoinedNoFKSortingTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True))
        Table("b", metadata, Column('id', Integer, primary_key=True))
        Table("c", metadata, Column('id', Integer, primary_key=True))

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(A):
            pass

        class C(A):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C = cls.classes.A, cls.classes.B, cls.classes.C
        mapper(A, cls.tables.a)
        mapper(B, cls.tables.b, inherits=A,
               inherit_condition=cls.tables.a.c.id == cls.tables.b.c.id,
               inherit_foreign_keys=cls.tables.b.c.id)
        mapper(C, cls.tables.c, inherits=A,
               inherit_condition=cls.tables.a.c.id == cls.tables.c.c.id,
               inherit_foreign_keys=cls.tables.c.c.id)

    def test_ordering(self):
        B, C = self.classes.B, self.classes.C
        sess = Session()
        sess.add_all([B(), C(), B(), C()])
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL("INSERT INTO a () VALUES ()", {}),
            CompiledSQL("INSERT INTO a () VALUES ()", {}),
            CompiledSQL("INSERT INTO a () VALUES ()", {}),
            CompiledSQL("INSERT INTO a () VALUES ()", {}),
            AllOf(
                CompiledSQL(
                    "INSERT INTO b (id) VALUES (:id)",
                    [{"id": 1}, {"id": 3}]
                ),
                CompiledSQL(
                    "INSERT INTO c (id) VALUES (:id)",
                    [{"id": 2}, {"id": 4}]
                )
            )
        )


class VersioningTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('version_id', Integer, nullable=False),
              Column('value', String(40)),
              Column('discriminator', Integer, nullable=False))
        Table('subtable', metadata,
              Column('id', None, ForeignKey('base.id'), primary_key=True),
              Column('subdata', String(50)))
        Table('stuff', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('parent', Integer, ForeignKey('base.id')))

    @testing.emits_warning(r".*updated rowcount")
    @testing.requires.sane_rowcount_w_returning
    @engines.close_open_connections
    def test_save_update(self):
        subtable, base, stuff = (self.tables.subtable,
                                 self.tables.base,
                                 self.tables.stuff)

        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass

        class Stuff(Base):
            pass
        mapper(Stuff, stuff)
        mapper(Base, base,
               polymorphic_on=base.c.discriminator,
               version_id_col=base.c.version_id,
               polymorphic_identity=1, properties={
                   'stuff': relationship(Stuff)
               })
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        sess.add(b1)
        sess.add(s1)

        sess.flush()

        sess2 = create_session()
        s2 = sess2.query(Base).get(s1.id)
        s2.subdata = 'sess2 subdata'

        s1.subdata = 'sess1 subdata'

        sess.flush()

        assert_raises(orm_exc.StaleDataError,
                      sess2.query(Base).with_lockmode('read').get,
                      s1.id)

        if not testing.db.dialect.supports_sane_rowcount:
            sess2.flush()
        else:
            assert_raises(orm_exc.StaleDataError, sess2.flush)

        sess2.refresh(s2)
        if testing.db.dialect.supports_sane_rowcount:
            assert s2.subdata == 'sess1 subdata'
        s2.subdata = 'sess2 subdata'
        sess2.flush()

    @testing.emits_warning(r".*(update|delete)d rowcount")
    @testing.requires.sane_rowcount_w_returning
    def test_delete(self):
        subtable, base = self.tables.subtable, self.tables.base

        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass

        mapper(Base, base,
               polymorphic_on=base.c.discriminator,
               version_id_col=base.c.version_id, polymorphic_identity=1)
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        s2 = Sub(value='sub2', subdata='some other subdata')
        sess.add(b1)
        sess.add(s1)
        sess.add(s2)

        sess.flush()

        sess2 = create_session()
        s3 = sess2.query(Base).get(s1.id)
        sess2.delete(s3)
        sess2.flush()

        s2.subdata = 'some new subdata'
        sess.flush()

        s1.subdata = 'some new subdata'
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises(
                orm_exc.StaleDataError,
                sess.flush
            )
        else:
            sess.flush()


class DistinctPKTest(fixtures.MappedTest):
    """test the construction of mapper.primary_key when an inheriting relationship
    joins on a column other than primary key column."""

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global person_table, employee_table, Person, Employee

        person_table = Table("persons", metadata,
                             Column("id", Integer, primary_key=True,
                                    test_needs_autoincrement=True),
                             Column("name", String(80)))

        employee_table = Table("employees", metadata,
                               Column("eid", Integer, primary_key=True,
                                      test_needs_autoincrement=True),
                               Column("salary", Integer),
                               Column("person_id", Integer,
                                      ForeignKey("persons.id")))

        class Person(object):
            def __init__(self, name):
                self.name = name

        class Employee(Person):
            pass

    @classmethod
    def insert_data(cls):
        person_insert = person_table.insert()
        person_insert.execute(id=1, name='alice')
        person_insert.execute(id=2, name='bob')

        employee_insert = employee_table.insert()
        employee_insert.execute(id=2, salary=250, person_id=1)  # alice
        employee_insert.execute(id=3, salary=200, person_id=2)  # bob

    def test_implicit(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper)
        assert list(class_mapper(Employee).primary_key) == [person_table.c.id]

    def test_explicit_props(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper,
               properties={'pid': person_table.c.id,
                           'eid': employee_table.c.eid})
        self._do_test(False)

    def test_explicit_composite_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table,
               inherits=person_mapper,
               properties=dict(id=[employee_table.c.eid, person_table.c.id]),
               primary_key=[person_table.c.id, employee_table.c.eid])
        assert_raises_message(
            sa_exc.SAWarning,
            r"On mapper Mapper\|Employee\|employees, "
            "primary key column 'persons.id' is being "
            "combined with distinct primary key column 'employees.eid' "
            "in attribute 'id'.  Use explicit properties to give "
            "each column its own mapped attribute name.", self._do_test, True)

    def test_explicit_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper,
               primary_key=[person_table.c.id])
        self._do_test(False)

    def _do_test(self, composite):
        session = create_session()
        query = session.query(Employee)

        if composite:
            alice1 = query.get([1, 2])
            bob = query.get([2, 3])
            alice2 = query.get([1, 2])
        else:
            alice1 = query.get(1)
            bob = query.get(2)
            alice2 = query.get(1)

            assert alice1.name == alice2.name == 'alice'
            assert bob.name == 'bob'


class SyncCompileTest(fixtures.MappedTest):
    """test that syncrules compile properly on custom inherit conds"""

    @classmethod
    def define_tables(cls, metadata):
        global _a_table, _b_table, _c_table

        _a_table = Table('a', metadata,
                         Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
                         Column('data1', String(128)))

        _b_table = Table('b', metadata,
                         Column('a_id', Integer, ForeignKey(
                             'a.id'), primary_key=True),
                         Column('data2', String(128)))

        _c_table = Table('c', metadata,
                         # Column('a_id', Integer, ForeignKey('b.a_id'),
                         # primary_key=True), #works
                         Column('b_a_id', Integer, ForeignKey(
                             'b.a_id'), primary_key=True),
                         Column('data3', String(128)))

    def test_joins(self):
        for j1 in (None, _b_table.c.a_id == _a_table.c.id, _a_table.c.id ==
                   _b_table.c.a_id):
            for j2 in (None, _b_table.c.a_id == _c_table.c.b_a_id,
                       _c_table.c.b_a_id == _b_table.c.a_id):
                self._do_test(j1, j2)
                for t in reversed(_a_table.metadata.sorted_tables):
                    t.delete().execute().close()

    def _do_test(self, j1, j2):
        class A(object):
            def __init__(self, **kwargs):
                for key, value in list(kwargs.items()):
                    setattr(self, key, value)

        class B(A):
            pass

        class C(B):
            pass

        mapper(A, _a_table)
        mapper(B, _b_table, inherits=A,
               inherit_condition=j1
               )
        mapper(C, _c_table, inherits=B,
               inherit_condition=j2
               )

        session = create_session()

        a = A(data1='a1')
        session.add(a)

        b = B(data1='b1', data2='b2')
        session.add(b)

        c = C(data1='c1', data2='c2', data3='c3')
        session.add(c)

        session.flush()
        session.expunge_all()

        assert len(session.query(A).all()) == 3
        assert len(session.query(B).all()) == 2
        assert len(session.query(C).all()) == 1


class OverrideColKeyTest(fixtures.MappedTest):
    """test overriding of column attributes."""

    @classmethod
    def define_tables(cls, metadata):
        global base, subtable, subtable_two

        base = Table('base', metadata,
                     Column('base_id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('data', String(255)),
                     Column('sqlite_fixer', String(10)))

        subtable = Table('subtable', metadata,
                         Column('base_id', Integer, ForeignKey(
                             'base.base_id'), primary_key=True),
                         Column('subdata', String(255)))
        subtable_two = Table('subtable_two', metadata,
                             Column('base_id', Integer, primary_key=True),
                             Column('fk_base_id', Integer,
                                    ForeignKey('base.base_id')),
                             Column('subdata', String(255)))

    def test_plain(self):
        # control case
        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        # Sub gets a "base_id" property using the "base_id"
        # column of both tables.
        eq_(
            class_mapper(Sub).get_property('base_id').columns,
            [subtable.c.base_id, base.c.base_id]
        )

    def test_override_explicit(self):
        # this pattern is what you see when using declarative
        # in particular, here we do a "manual" version of
        # what we'd like the mapper to do.

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, base, properties={
            'id': base.c.base_id
        })
        mapper(Sub, subtable, inherits=Base, properties={
            # this is the manual way to do it, is not really
            # possible in declarative
            'id': [base.c.base_id, subtable.c.base_id]
        })

        eq_(
            class_mapper(Sub).get_property('id').columns,
            [base.c.base_id, subtable.c.base_id]
        )

        s1 = Sub()
        s1.id = 10
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).get(10) is s1

    def test_override_onlyinparent(self):
        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, base, properties={
            'id': base.c.base_id
        })
        mapper(Sub, subtable, inherits=Base)

        eq_(
            class_mapper(Sub).get_property('id').columns,
            [base.c.base_id]
        )

        eq_(
            class_mapper(Sub).get_property('base_id').columns,
            [subtable.c.base_id]
        )

        s1 = Sub()
        s1.id = 10

        s2 = Sub()
        s2.base_id = 15

        sess = create_session()
        sess.add_all([s1, s2])
        sess.flush()

        # s1 gets '10'
        assert sess.query(Sub).get(10) is s1

        # s2 gets a new id, base_id is overwritten by the ultimate
        # PK col
        assert s2.id == s2.base_id != 15

    def test_override_implicit(self):
        # this is originally [ticket:1111].
        # the pattern here is now disallowed by [ticket:1892]

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, base, properties={
            'id': base.c.base_id
        })

        def go():
            mapper(Sub, subtable, inherits=Base, properties={
                'id': subtable.c.base_id
            })
        # Sub mapper compilation needs to detect that "base.c.base_id"
        # is renamed in the inherited mapper as "id", even though
        # it has its own "id" property.  It then generates
        # an exception in 0.7 due to the implicit conflict.
        assert_raises(sa_exc.InvalidRequestError, go)

    def test_pk_fk_different(self):
        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, base)

        def go():
            mapper(Sub, subtable_two, inherits=Base)
        assert_raises_message(
            sa_exc.SAWarning,
            "Implicitly combining column base.base_id with "
            "column subtable_two.base_id under attribute 'base_id'",
            go
        )

    def test_plain_descriptor(self):
        """test that descriptors prevent inheritance from propigating
        properties to subclasses."""

        class Base(object):
            pass

        class Sub(Base):
            @property
            def data(self):
                return "im the data"

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        s1 = Sub()
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).one().data == "im the data"

    def test_custom_descriptor(self):
        """test that descriptors prevent inheritance from propigating
        properties to subclasses."""

        class MyDesc(object):
            def __get__(self, instance, owner):
                if instance is None:
                    return self
                return "im the data"

        class Base(object):
            pass

        class Sub(Base):
            data = MyDesc()

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        s1 = Sub()
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).one().data == "im the data"

    def test_sub_columns_over_base_descriptors(self):
        class Base(object):
            @property
            def subdata(self):
                return "this is base"

        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        sess = create_session()
        b1 = Base()
        assert b1.subdata == "this is base"
        s1 = Sub()
        s1.subdata = "this is sub"
        assert s1.subdata == "this is sub"

        sess.add_all([s1, b1])
        sess.flush()
        sess.expunge_all()

        assert sess.query(Base).get(b1.base_id).subdata == "this is base"
        assert sess.query(Sub).get(s1.base_id).subdata == "this is sub"

    def test_base_descriptors_over_base_cols(self):
        class Base(object):
            @property
            def data(self):
                return "this is base"

        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        sess = create_session()
        b1 = Base()
        assert b1.data == "this is base"
        s1 = Sub()
        assert s1.data == "this is base"

        sess.add_all([s1, b1])
        sess.flush()
        sess.expunge_all()

        assert sess.query(Base).get(b1.base_id).data == "this is base"
        assert sess.query(Sub).get(s1.base_id).data == "this is base"


class OptimizedLoadTest(fixtures.MappedTest):
    """tests for the "optimized load" routine."""

    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)),
              Column('type', String(50)),
              Column('counter', Integer, server_default="1"))
        Table('sub', metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True),
              Column('sub', String(50)),
              Column('subcounter', Integer, server_default="1"),
              Column('subcounter2', Integer, server_default="1"))
        Table('subsub', metadata,
              Column('id', Integer, ForeignKey('sub.id'), primary_key=True),
              Column('subsubcounter2', Integer, server_default="1"))
        Table('with_comp', metadata,
              Column('id', Integer, ForeignKey('base.id'), primary_key=True),
              Column('a', String(10)),
              Column('b', String(10)))

    def test_no_optimize_on_map_to_join(self):
        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.ComparableEntity):
            pass

        class JoinBase(fixtures.ComparableEntity):
            pass

        class SubJoinBase(JoinBase):
            pass

        mapper(Base, base)
        mapper(JoinBase, base.outerjoin(sub), properties=util.OrderedDict(
            [('id', [base.c.id, sub.c.id]),
             ('counter', [base.c.counter, sub.c.subcounter])]))
        mapper(SubJoinBase, inherits=JoinBase)

        sess = Session()
        sess.add(Base(data='data'))
        sess.commit()

        sjb = sess.query(SubJoinBase).one()
        sjb_id = sjb.id
        sess.expire(sjb)

        # this should not use the optimized load,
        # which assumes discrete tables
        def go():
            eq_(sjb.data, 'data')

        self.assert_sql_execution(
            testing.db, go,
            CompiledSQL(
                "SELECT base.id AS base_id, sub.id AS sub_id, "
                "base.counter AS base_counter, "
                "sub.subcounter AS sub_subcounter, "
                "base.data AS base_data, base.type AS base_type, "
                "sub.sub AS sub_sub, sub.subcounter2 AS sub_subcounter2 "
                "FROM base LEFT OUTER JOIN sub ON base.id = sub.id "
                "WHERE base.id = :param_1", {'param_1': sjb_id}))

    def test_optimized_passes(self):
        """"test that the 'optimized load' routine doesn't crash when
        a column in the join condition is not available."""

        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.ComparableEntity):
            pass

        class Sub(Base):
            pass

        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')

        # redefine Sub's "id" to favor the "id" col in the subtable.
        # "id" is also part of the primary join condition
        mapper(Sub, sub, inherits=Base,
               polymorphic_identity='sub',
               properties={'id': [sub.c.id, base.c.id]})
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        sess.add(s1)
        sess.commit()
        sess.expunge_all()

        # load s1 via Base.  s1.id won't populate since it's relative to
        # the "sub" table.  The optimized load kicks in and tries to
        # generate on the primary join, but cannot since "id" is itself
        # unloaded. the optimized load needs to return "None" so regular
        # full-row loading proceeds
        s1 = sess.query(Base).first()
        assert s1.sub == 's1sub'

    def test_column_expression(self):
        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.ComparableEntity):
            pass

        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub',
               properties={
                   'concat': column_property(sub.c.sub + "|" + sub.c.sub)})
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        sess.add(s1)
        sess.commit()
        sess.expunge_all()
        s1 = sess.query(Base).first()
        assert s1.concat == 's1sub|s1sub'

    def test_column_expression_joined(self):
        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.ComparableEntity):
            pass

        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub',
               properties={
                   'concat': column_property(base.c.data + "|" + sub.c.sub)})
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        s2 = Sub(data='s2data', sub='s2sub')
        s3 = Sub(data='s3data', sub='s3sub')
        sess.add_all([s1, s2, s3])
        sess.commit()
        sess.expunge_all()
        # query a bunch of rows to ensure there's no cartesian
        # product against "base" occurring, it is in fact
        # detecting that "base" needs to be in the join
        # criterion
        eq_(
            sess.query(Base).order_by(Base.id).all(),
            [
                Sub(data='s1data', sub='s1sub', concat='s1data|s1sub'),
                Sub(data='s2data', sub='s2sub', concat='s2data|s2sub'),
                Sub(data='s3data', sub='s3sub', concat='s3data|s3sub')
            ]
        )

    def test_composite_column_joined(self):
        base, with_comp = self.tables.base, self.tables.with_comp

        class Base(fixtures.BasicEntity):
            pass

        class WithComp(Base):
            pass

        class Comp(object):
            def __init__(self, a, b):
                self.a = a
                self.b = b

            def __composite_values__(self):
                return self.a, self.b

            def __eq__(self, other):
                return (self.a == other.a) and (self.b == other.b)
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        mapper(WithComp, with_comp, inherits=Base, polymorphic_identity='wc',
               properties={'comp': composite(Comp,
                                             with_comp.c.a, with_comp.c.b)})
        sess = sessionmaker()()
        s1 = WithComp(data='s1data', comp=Comp('ham', 'cheese'))
        s2 = WithComp(data='s2data', comp=Comp('bacon', 'eggs'))
        sess.add_all([s1, s2])
        sess.commit()
        sess.expunge_all()
        s1test, s2test = sess.query(Base).order_by(Base.id).all()
        assert s1test.comp
        assert s2test.comp
        eq_(s1test.comp, Comp('ham', 'cheese'))
        eq_(s2test.comp, Comp('bacon', 'eggs'))

    def test_load_expired_on_pending(self):
        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub')
        sess = Session()
        s1 = Sub(data='s1')
        sess.add(s1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO base (data, type) VALUES (:data, :type)",
                [{'data': 's1', 'type': 'sub'}]
            ),
            CompiledSQL(
                "INSERT INTO sub (id, sub) VALUES (:id, :sub)",
                lambda ctx: {'id': s1.id, 'sub': None}
            ),
        )

        def go():
            eq_(s1.subcounter2, 1)
        self.assert_sql_execution(
            testing.db, go,
            CompiledSQL(
                "SELECT base.counter AS base_counter, "
                "sub.subcounter AS sub_subcounter, "
                "sub.subcounter2 AS sub_subcounter2 FROM base JOIN sub "
                "ON base.id = sub.id WHERE base.id = :param_1",
                lambda ctx: {'param_1': s1.id}))

    def test_dont_generate_on_none(self):
        base, sub = self.tables.base, self.tables.sub

        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        m = mapper(Sub, sub, inherits=Base, polymorphic_identity='sub')

        s1 = Sub()
        assert m._optimized_get_statement(attributes.instance_state(s1),
                                          ['subcounter2']) is None

        # loads s1.id as None
        eq_(s1.id, None)

        # this now will come up with a value of None for id - should reject
        assert m._optimized_get_statement(attributes.instance_state(s1),
                                          ['subcounter2']) is None

        s1.id = 1
        attributes.instance_state(s1)._commit_all(s1.__dict__, None)
        assert m._optimized_get_statement(attributes.instance_state(s1),
                                          ['subcounter2']) is not None

    def test_load_expired_on_pending_twolevel(self):
        base, sub, subsub = (self.tables.base,
                             self.tables.sub,
                             self.tables.subsub)

        class Base(fixtures.BasicEntity):
            pass

        class Sub(Base):
            pass

        class SubSub(Sub):
            pass

        mapper(Base, base, polymorphic_on=base.c.type,
               polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub')
        mapper(SubSub, subsub, inherits=Sub, polymorphic_identity='subsub')
        sess = Session()
        s1 = SubSub(data='s1', counter=1, subcounter=2)
        sess.add(s1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO base (data, type, counter) VALUES "
                "(:data, :type, :counter)",
                [{'data': 's1', 'type': 'subsub', 'counter': 1}]
            ),
            CompiledSQL(
                "INSERT INTO sub (id, sub, subcounter) VALUES "
                "(:id, :sub, :subcounter)",
                lambda ctx: [{'subcounter': 2, 'sub': None, 'id': s1.id}]
            ),
            CompiledSQL(
                "INSERT INTO subsub (id) VALUES (:id)",
                lambda ctx: {'id': s1.id}
            ),
        )

        def go():
            eq_(
                s1.subcounter2, 1
            )
        self.assert_sql_execution(
            testing.db,
            go,
            Or(
                CompiledSQL(
                    "SELECT subsub.subsubcounter2 AS subsub_subsubcounter2, "
                    "sub.subcounter2 AS sub_subcounter2 FROM subsub, sub "
                    "WHERE :param_1 = sub.id AND sub.id = subsub.id",
                    lambda ctx: {'param_1': s1.id}
                ),
                CompiledSQL(
                    "SELECT sub.subcounter2 AS sub_subcounter2, "
                    "subsub.subsubcounter2 AS subsub_subsubcounter2 "
                    "FROM sub, subsub "
                    "WHERE :param_1 = sub.id AND sub.id = subsub.id",
                    lambda ctx: {'param_1': s1.id}
                ),
            )
        )


class NoPKOnSubTableWarningTest(fixtures.TestBase):

    def _fixture(self):
        metadata = MetaData()
        parent = Table('parent', metadata,
                       Column('id', Integer, primary_key=True))
        child = Table('child', metadata,
                      Column('id', Integer, ForeignKey('parent.id')))
        return parent, child

    def tearDown(self):
        clear_mappers()

    def test_warning_on_sub(self):
        parent, child = self._fixture()

        class P(object):
            pass

        class C(P):
            pass

        mapper(P, parent)
        assert_raises_message(
            sa_exc.SAWarning,
            "Could not assemble any primary keys for locally mapped "
            "table 'child' - no rows will be persisted in this Table.",
            mapper, C, child, inherits=P
        )

    def test_no_warning_with_explicit(self):
        parent, child = self._fixture()

        class P(object):
            pass

        class C(P):
            pass

        mapper(P, parent)
        mc = mapper(C, child, inherits=P, primary_key=[parent.c.id])
        eq_(mc.primary_key, (parent.c.id,))


class InhCondTest(fixtures.TestBase):
    def test_inh_cond_nonexistent_table_unrelated(self):
        metadata = MetaData()
        base_table = Table("base", metadata,
                           Column("id", Integer, primary_key=True))
        derived_table = Table("derived", metadata,
                              Column("id", Integer, ForeignKey(
                                  "base.id"), primary_key=True),
                              Column("owner_id", Integer,
                                     ForeignKey("owner.owner_id")))

        class Base(object):
            pass

        class Derived(Base):
            pass

        mapper(Base, base_table)
        # succeeds, despite "owner" table not configured yet
        m2 = mapper(Derived, derived_table,
                    inherits=Base)
        assert m2.inherit_condition.compare(
            base_table.c.id == derived_table.c.id
        )

    def test_inh_cond_nonexistent_col_unrelated(self):
        m = MetaData()
        base_table = Table("base", m,
                           Column("id", Integer, primary_key=True))
        derived_table = Table("derived", m,
                              Column("id", Integer, ForeignKey('base.id'),
                                     primary_key=True),
                              Column('order_id', Integer,
                                     ForeignKey('order.foo')))
        order_table = Table('order', m, Column(
            'id', Integer, primary_key=True))

        class Base(object):
            pass

        class Derived(Base):
            pass

        mapper(Base, base_table)

        # succeeds, despite "order.foo" doesn't exist
        m2 = mapper(Derived, derived_table, inherits=Base)
        assert m2.inherit_condition.compare(
            base_table.c.id == derived_table.c.id
        )

    def test_inh_cond_no_fk(self):
        metadata = MetaData()
        base_table = Table("base", metadata,
                           Column("id", Integer, primary_key=True))
        derived_table = Table("derived", metadata,
                              Column("id", Integer, primary_key=True))

        class Base(object):
            pass

        class Derived(Base):
            pass

        mapper(Base, base_table)
        assert_raises_message(
            sa_exc.ArgumentError,
            "Can't find any foreign key relationships between "
            "'base' and 'derived'.",
            mapper,
            Derived, derived_table,  inherits=Base
        )

    def test_inh_cond_nonexistent_table_related(self):
        m1 = MetaData()
        m2 = MetaData()
        base_table = Table("base", m1,
                           Column("id", Integer, primary_key=True))
        derived_table = Table("derived", m2,
                              Column("id", Integer, ForeignKey('base.id'),
                                     primary_key=True))

        class Base(object):
            pass

        class Derived(Base):
            pass

        mapper(Base, base_table)

        # the ForeignKey def is correct but there are two
        # different metadatas.  Would like the traditional
        # "noreferencedtable" error to raise so that the
        # user is directed towards the FK definition in question.
        assert_raises_message(
            sa_exc.NoReferencedTableError,
            "Foreign key associated with column 'derived.id' "
            "could not find table 'base' with which to generate "
            "a foreign key to target column 'id'",
            mapper,
            Derived, derived_table,  inherits=Base
        )

    def test_inh_cond_nonexistent_col_related(self):
        m = MetaData()
        base_table = Table("base", m,
                           Column("id", Integer, primary_key=True))
        derived_table = Table("derived", m,
                              Column("id", Integer, ForeignKey('base.q'),
                                     primary_key=True))

        class Base(object):
            pass

        class Derived(Base):
            pass

        mapper(Base, base_table)

        assert_raises_message(
            sa_exc.NoReferencedColumnError,
            "Could not initialize target column for ForeignKey "
            "'base.q' on table "
            "'derived': table 'base' has no column named 'q'",
            mapper,
            Derived, derived_table,  inherits=Base
        )


class PKDiscriminatorTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        parents = Table('parents', metadata,
                        Column('id', Integer, primary_key=True,
                               test_needs_autoincrement=True),
                        Column('name', String(60)))

        children = Table('children', metadata,
                         Column('id', Integer, ForeignKey('parents.id'),
                                primary_key=True),
                         Column('type', Integer, primary_key=True),
                         Column('name', String(60)))

    def test_pk_as_discriminator(self):
        parents, children = self.tables.parents, self.tables.children

        class Parent(object):
            def __init__(self, name=None):
                self.name = name

        class Child(object):
            def __init__(self, name=None):
                self.name = name

        class A(Child):
            pass

        mapper(Parent, parents, properties={
            'children': relationship(Child, backref='parent'),
        })
        mapper(Child, children, polymorphic_on=children.c.type,
               polymorphic_identity=1)

        mapper(A, inherits=Child, polymorphic_identity=2)

        s = create_session()
        p = Parent('p1')
        a = A('a1')
        p.children.append(a)
        s.add(p)
        s.flush()

        assert a.id
        assert a.type == 2

        p.name = 'p1new'
        a.name = 'a1new'
        s.flush()

        s.expire_all()
        assert a.name == 'a1new'
        assert p.name == 'p1new'


class NoPolyIdentInMiddleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('type', String(50), nullable=False))

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(A):
            pass

        class C(B):
            pass

        class D(B):
            pass

        class E(A):
            pass

    @classmethod
    def setup_mappers(cls):
        A, C, B, E, D, base = (cls.classes.A,
                               cls.classes.C,
                               cls.classes.B,
                               cls.classes.E,
                               cls.classes.D,
                               cls.tables.base)

        mapper(A, base, polymorphic_on=base.c.type)
        mapper(B, inherits=A, )
        mapper(C, inherits=B, polymorphic_identity='c')
        mapper(D, inherits=B, polymorphic_identity='d')
        mapper(E, inherits=A, polymorphic_identity='e')

    def test_load_from_middle(self):
        C, B = self.classes.C, self.classes.B

        s = Session()
        s.add(C())
        o = s.query(B).first()
        eq_(o.type, 'c')
        assert isinstance(o, C)

    def test_load_from_base(self):
        A, C = self.classes.A, self.classes.C

        s = Session()
        s.add(C())
        o = s.query(A).first()
        eq_(o.type, 'c')
        assert isinstance(o, C)

    def test_discriminator(self):
        C, B, base = (self.classes.C,
                      self.classes.B,
                      self.tables.base)

        assert class_mapper(B).polymorphic_on is base.c.type
        assert class_mapper(C).polymorphic_on is base.c.type

    def test_load_multiple_from_middle(self):
        C, B, E, D, base = (self.classes.C,
                            self.classes.B,
                            self.classes.E,
                            self.classes.D,
                            self.tables.base)

        s = Session()
        s.add_all([C(), D(), E()])
        eq_(
            s.query(B).order_by(base.c.type).all(),
            [C(), D()]
        )


class DeleteOrphanTest(fixtures.MappedTest):
    """Test the fairly obvious, that an error is raised
    when attempting to insert an orphan.

    Previous SQLA versions would check this constraint
    in memory which is the original rationale for this test.

    """

    @classmethod
    def define_tables(cls, metadata):
        global single, parent
        single = Table('single', metadata,
                       Column('id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('type', String(50), nullable=False),
                       Column('data', String(50)),
                       Column('parent_id', Integer, ForeignKey(
                           'parent.id'), nullable=False),
                       )

        parent = Table('parent', metadata,
                       Column('id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('data', String(50)))

    def test_orphan_message(self):
        class Base(fixtures.BasicEntity):
            pass

        class SubClass(Base):
            pass

        class Parent(fixtures.BasicEntity):
            pass

        mapper(Base, single, polymorphic_on=single.c.type,
               polymorphic_identity='base')
        mapper(SubClass, inherits=Base, polymorphic_identity='sub')
        mapper(Parent, parent, properties={
            'related': relationship(Base, cascade="all, delete-orphan")
        })

        sess = create_session()
        s1 = SubClass(data='s1')
        sess.add(s1)
        assert_raises(sa_exc.DBAPIError, sess.flush)


class PolymorphicUnionTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def _fixture(self):
        t1 = table('t1', column('c1', Integer),
                   column('c2', Integer),
                   column('c3', Integer))
        t2 = table('t2', column('c1', Integer), column('c2', Integer),
                   column('c3', Integer),
                   column('c4', Integer))
        t3 = table('t3', column('c1', Integer),
                   column('c3', Integer),
                   column('c5', Integer))
        return t1, t2, t3

    def test_type_col_present(self):
        t1, t2, t3 = self._fixture()
        self.assert_compile(
            polymorphic_union(
                util.OrderedDict([("a", t1), ("b", t2), ("c", t3)]),
                'q1'
            ),
            "SELECT t1.c1, t1.c2, t1.c3, CAST(NULL AS INTEGER) AS c4, "
            "CAST(NULL AS INTEGER) AS c5, 'a' AS q1 FROM t1 UNION ALL "
            "SELECT t2.c1, t2.c2, t2.c3, t2.c4, CAST(NULL AS INTEGER) AS c5, "
            "'b' AS q1 FROM t2 UNION ALL SELECT t3.c1, "
            "CAST(NULL AS INTEGER) AS c2, t3.c3, CAST(NULL AS INTEGER) AS c4, "
            "t3.c5, 'c' AS q1 FROM t3"
        )

    def test_type_col_non_present(self):
        t1, t2, t3 = self._fixture()
        self.assert_compile(
            polymorphic_union(
                util.OrderedDict([("a", t1), ("b", t2), ("c", t3)]),
                None
            ),
            "SELECT t1.c1, t1.c2, t1.c3, CAST(NULL AS INTEGER) AS c4, "
            "CAST(NULL AS INTEGER) AS c5 FROM t1 UNION ALL SELECT t2.c1, "
            "t2.c2, t2.c3, t2.c4, CAST(NULL AS INTEGER) AS c5 FROM t2 "
            "UNION ALL SELECT t3.c1, CAST(NULL AS INTEGER) AS c2, t3.c3, "
            "CAST(NULL AS INTEGER) AS c4, t3.c5 FROM t3"
        )

    def test_no_cast_null(self):
        t1, t2, t3 = self._fixture()
        self.assert_compile(
            polymorphic_union(
                util.OrderedDict([("a", t1), ("b", t2), ("c", t3)]),
                'q1', cast_nulls=False
            ),
            "SELECT t1.c1, t1.c2, t1.c3, NULL AS c4, NULL AS c5, 'a' AS q1 "
            "FROM t1 UNION ALL SELECT t2.c1, t2.c2, t2.c3, t2.c4, NULL AS c5, "
            "'b' AS q1 FROM t2 UNION ALL SELECT t3.c1, NULL AS c2, t3.c3, "
            "NULL AS c4, t3.c5, 'c' AS q1 FROM t3"
        )


class NameConflictTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        content = Table('content', metadata,
                        Column('id', Integer, primary_key=True,
                               test_needs_autoincrement=True),
                        Column('type', String(30)))
        foo = Table('foo', metadata,
                    Column('id', Integer, ForeignKey('content.id'),
                           primary_key=True),
                    Column('content_type', String(30)))

    def test_name_conflict(self):
        class Content(object):
            pass

        class Foo(Content):
            pass
        mapper(Content, self.tables.content,
               polymorphic_on=self.tables.content.c.type)
        mapper(Foo, self.tables.foo, inherits=Content,
               polymorphic_identity='foo')
        sess = create_session()
        f = Foo()
        f.content_type = 'bar'
        sess.add(f)
        sess.flush()
        f_id = f.id
        sess.expunge_all()
        assert sess.query(Content).get(f_id).content_type == 'bar'
