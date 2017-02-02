
from sqlalchemy.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy import MetaData, Integer, ForeignKey, util, event
from sqlalchemy.orm import mapper, relationship, create_session, \
    attributes, class_mapper, clear_mappers, instrumentation, events
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing import eq_, ne_
from sqlalchemy.testing import fixtures
from sqlalchemy import testing


class InitTest(fixtures.ORMTest):
    def fixture(self):
        return Table('t', MetaData(),
                     Column('id', Integer, primary_key=True),
                     Column('type', Integer),
                     Column('x', Integer),
                     Column('y', Integer))

    def register(self, cls, canary):
        original_init = cls.__init__
        instrumentation.register_class(cls)
        ne_(cls.__init__, original_init)
        manager = instrumentation.manager_of_class(cls)

        def init(state, args, kwargs):
            canary.append((cls, 'init', state.class_))
        event.listen(manager, 'init', init, raw=True)

    def test_ai(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        obj = A()
        eq_(inits, [(A, '__init__')])

    def test_A(self):
        inits = []

        class A(object):
            pass
        self.register(A, inits)

        obj = A()
        eq_(inits, [(A, 'init', A)])

    def test_Ai(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

    def test_ai_B(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        class B(A):
            pass
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (A, '__init__')])

    def test_ai_Bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (B, '__init__'), (A, '__init__')])

    def test_Ai_bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, '__init__'), (A, 'init', B), (A, '__init__')])

    def test_Ai_Bi(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (B, '__init__'), (A, '__init__')])

    def test_Ai_B(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            pass
        self.register(B, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (A, '__init__')])

    def test_Ai_Bi_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (B, '__init__'), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (C, '__init__'), (B, '__init__'),
                    (A, '__init__')])

    def test_Ai_bi_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
                super(B, self).__init__()

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, '__init__'), (A, 'init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (C, '__init__'),  (B, '__init__'),
                    (A, '__init__')])

    def test_Ai_b_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            pass

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(A, 'init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (C, '__init__'), (A, '__init__')])

    def test_Ai_B_Ci(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            pass
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
                super(C, self).__init__()
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (C, '__init__'), (A, '__init__')])

    def test_Ai_B_C(self):
        inits = []

        class A(object):
            def __init__(self):
                inits.append((A, '__init__'))
        self.register(A, inits)

        class B(A):
            pass
        self.register(B, inits)

        class C(B):
            pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A), (A, '__init__')])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (A, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (A, '__init__')])

    def test_A_Bi_C(self):
        inits = []

        class A(object):
            pass
        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, '__init__'))
        self.register(B, inits)

        class C(B):
            pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B), (B, '__init__')])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (B, '__init__')])

    def test_A_B_Ci(self):
        inits = []

        class A(object):
            pass
        self.register(A, inits)

        class B(A):
            pass
        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, '__init__'))
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B)])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C), (C, '__init__')])

    def test_A_B_C(self):
        inits = []

        class A(object):
            pass
        self.register(A, inits)

        class B(A):
            pass
        self.register(B, inits)

        class C(B):
            pass
        self.register(C, inits)

        obj = A()
        eq_(inits, [(A, 'init', A)])

        del inits[:]

        obj = B()
        eq_(inits, [(B, 'init', B)])

        del inits[:]
        obj = C()
        eq_(inits, [(C, 'init', C)])

    def test_defaulted_init(self):
        class X(object):
            def __init__(self_, a, b=123, c='abc'):
                self_.a = a
                self_.b = b
                self_.c = c
        instrumentation.register_class(X)

        o = X('foo')
        eq_(o.a, 'foo')
        eq_(o.b, 123)
        eq_(o.c, 'abc')

        class Y(object):
            unique = object()

            class OutOfScopeForEval(object):
                def __repr__(self_):
                    # misleading repr
                    return '123'

            outofscope = OutOfScopeForEval()

            def __init__(self_, u=unique, o=outofscope):
                self_.u = u
                self_.o = o

        instrumentation.register_class(Y)

        o = Y()
        assert o.u is Y.unique
        assert o.o is Y.outofscope


class MapperInitTest(fixtures.ORMTest):

    def fixture(self):
        return Table('t', MetaData(),
                     Column('id', Integer, primary_key=True),
                     Column('type', Integer),
                     Column('x', Integer),
                     Column('y', Integer))

    def test_partially_mapped_inheritance(self):
        class A(object):
            pass

        class B(A):
            pass

        class C(B):
            def __init__(self, x):
                pass

        m = mapper(A, self.fixture())

        # B is not mapped in the current implementation
        assert_raises(sa.orm.exc.UnmappedClassError, class_mapper, B)

        # C is not mapped in the current implementation
        assert_raises(sa.orm.exc.UnmappedClassError, class_mapper, C)

    def test_del_warning(self):
        class A(object):
            def __del__(self):
                pass

        assert_raises_message(
            sa.exc.SAWarning,
            r"__del__\(\) method on class "
            r"<class '.*\.A'> will cause "
            r"unreachable cycles and memory leaks, as SQLAlchemy "
            r"instrumentation often creates reference cycles.  "
            r"Please remove this method.",
            mapper, A, self.fixture()
        )


class OnLoadTest(fixtures.ORMTest):
    """Check that Events.load is not hit in regular attributes operations."""

    def test_basic(self):
        import pickle

        global A

        class A(object):
            pass

        def canary(instance):
            assert False

        try:
            instrumentation.register_class(A)
            manager = instrumentation.manager_of_class(A)
            event.listen(manager, 'load', canary)

            a = A()
            p_a = pickle.dumps(a)
            re_a = pickle.loads(p_a)
        finally:
            del A


class NativeInstrumentationTest(fixtures.ORMTest):
    def test_register_reserved_attribute(self):
        class T(object):
            pass

        instrumentation.register_class(T)
        manager = instrumentation.manager_of_class(T)

        sa = instrumentation.ClassManager.STATE_ATTR
        ma = instrumentation.ClassManager.MANAGER_ATTR

        def fails(method, attr): return assert_raises(
            KeyError, getattr(manager, method), attr, property())

        fails('install_member', sa)
        fails('install_member', ma)
        fails('install_descriptor', sa)
        fails('install_descriptor', ma)

    def test_mapped_stateattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column(instrumentation.ClassManager.STATE_ATTR, Integer))

        class T(object):
            pass

        assert_raises(KeyError, mapper, T, t)

    def test_mapped_managerattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column(instrumentation.ClassManager.MANAGER_ATTR, Integer))

        class T(object):
            pass
        assert_raises(KeyError, mapper, T, t)


class Py3KFunctionInstTest(fixtures.ORMTest):
    __requires__ = ("python3", )

    def _instrument(self, cls):
        manager = instrumentation.register_class(cls)
        canary = []

        def check(target, args, kwargs):
            canary.append((args, kwargs))
        event.listen(manager, "init", check)
        return cls, canary

    def test_kw_only_args(self):
        cls, canary = self._kw_only_fixture()

        a = cls("a", b="b", c="c")
        eq_(canary, [(('a', ), {'b': 'b', 'c': 'c'})])

    def test_kw_plus_posn_args(self):
        cls, canary = self._kw_plus_posn_fixture()

        a = cls("a", 1, 2, 3, b="b", c="c")
        eq_(canary, [(('a', 1, 2, 3), {'b': 'b', 'c': 'c'})])

    def test_kw_only_args_plus_opt(self):
        cls, canary = self._kw_opt_fixture()

        a = cls("a", b="b")
        eq_(canary, [(('a', ), {'b': 'b', 'c': 'c'})])

        canary[:] = []
        a = cls("a", b="b", c="d")
        eq_(canary, [(('a', ), {'b': 'b', 'c': 'd'})])

    def test_kw_only_sig(self):
        cls, canary = self._kw_only_fixture()
        assert_raises(
            TypeError,
            cls, "a", "b", "c"
        )

    def test_kw_plus_opt_sig(self):
        cls, canary = self._kw_only_fixture()
        assert_raises(
            TypeError,
            cls, "a", "b", "c"
        )

        assert_raises(
            TypeError,
            cls, "a", "b", c="c"
        )


if util.py3k:
    _locals = {}
    exec("""
def _kw_only_fixture(self):
    class A(object):
        def __init__(self, a, *, b, c):
            self.a = a
            self.b = b
            self.c = c
    return self._instrument(A)

def _kw_plus_posn_fixture(self):
    class A(object):
        def __init__(self, a, *args, b, c):
            self.a = a
            self.b = b
            self.c = c
    return self._instrument(A)

def _kw_opt_fixture(self):
    class A(object):
        def __init__(self, a, *, b, c="c"):
            self.a = a
            self.b = b
            self.c = c
    return self._instrument(A)
""", _locals)
    for k in _locals:
        setattr(Py3KFunctionInstTest, k, _locals[k])


class MiscTest(fixtures.ORMTest):
    """Seems basic, but not directly covered elsewhere!"""

    def test_compileonattr(self):
        t = Table('t', MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column('x', Integer))

        class A(object):
            pass
        mapper(A, t)

        a = A()
        assert a.id is None

    def test_compileonattr_rel(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))

        class A(object):
            pass

        class B(object):
            pass
        mapper(A, t1, properties=dict(bs=relationship(B)))
        mapper(B, t2)

        a = A()
        assert not a.bs

    def test_uninstrument(self):
        class A(object):
            pass

        manager = instrumentation.register_class(A)
        attributes.register_attribute(A, 'x', uselist=False, useobject=False)

        assert instrumentation.manager_of_class(A) is manager
        instrumentation.unregister_class(A)
        assert instrumentation.manager_of_class(A) is None
        assert not hasattr(A, 'x')

        # I prefer 'is' here but on pypy
        # it seems only == works
        assert A.__init__ == object.__init__

    def test_compileonattr_rel_backref_a(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))

        class Base(object):
            def __init__(self, *args, **kwargs):
                pass

        for base in object, Base:
            class A(base):
                pass

            class B(base):
                pass
            mapper(A, t1, properties=dict(bs=relationship(B, backref='a')))
            mapper(B, t2)

            b = B()
            assert b.a is None
            a = A()
            b.a = a

            session = create_session()
            session.add(b)
            assert a in session, "base is %s" % base

    def test_compileonattr_rel_backref_b(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('x', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1_id', Integer, ForeignKey('t1.id')))

        class Base(object):
            def __init__(self):
                pass

        class Base_AKW(object):
            def __init__(self, *args, **kwargs):
                pass

        for base in object, Base, Base_AKW:
            class A(base):
                pass

            class B(base):
                pass
            mapper(A, t1)
            mapper(B, t2, properties=dict(a=relationship(A, backref='bs')))

            a = A()
            b = B()
            b.a = a

            session = create_session()
            session.add(a)
            assert b in session, 'base: %s' % base
