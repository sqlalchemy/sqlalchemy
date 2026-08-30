import inspect
import linecache
import sys
import traceback

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import relationship
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_warns_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import ne_
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class InitTest(fixtures.ORMTest):
    def fixture(self):
        return Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("type", Integer),
            Column("x", Integer),
            Column("y", Integer),
        )

    def register(self, cls, canary):
        original_init = cls.__init__
        instrumentation.register_class(cls)
        ne_(cls.__init__, original_init)
        manager = instrumentation.manager_of_class(cls)

        def init(state, args, kwargs):
            canary.append((cls, "init", state.class_))

        event.listen(manager, "init", init, raw=True)

    def test_ai(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        A()
        eq_(inits, [(A, "__init__")])

    def test_A(self):
        inits = []

        class A:
            pass

        self.register(A, inits)

        A()
        eq_(inits, [(A, "init", A)])

    def test_Ai(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

    def test_ai_B(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        class B(A):
            pass

        self.register(B, inits)

        A()
        eq_(inits, [(A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (A, "__init__")])

    def test_ai_Bi(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))
                super().__init__()

        self.register(B, inits)

        A()
        eq_(inits, [(A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (B, "__init__"), (A, "__init__")])

    def test_Ai_bi(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))
                super().__init__()

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "__init__"), (A, "init", B), (A, "__init__")])

    def test_Ai_Bi(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))
                super().__init__()

        self.register(B, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (B, "__init__"), (A, "__init__")])

    def test_Ai_B(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            pass

        self.register(B, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (A, "__init__")])

    def test_Ai_Bi_Ci(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))
                super().__init__()

        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, "__init__"))
                super().__init__()

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (B, "__init__"), (A, "__init__")])

        del inits[:]
        C()
        eq_(
            inits,
            [
                (C, "init", C),
                (C, "__init__"),
                (B, "__init__"),
                (A, "__init__"),
            ],
        )

    def test_Ai_bi_Ci(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))
                super().__init__()

        class C(B):
            def __init__(self):
                inits.append((C, "__init__"))
                super().__init__()

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "__init__"), (A, "init", B), (A, "__init__")])

        del inits[:]
        C()
        eq_(
            inits,
            [
                (C, "init", C),
                (C, "__init__"),
                (B, "__init__"),
                (A, "__init__"),
            ],
        )

    def test_Ai_b_Ci(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            pass

        class C(B):
            def __init__(self):
                inits.append((C, "__init__"))
                super().__init__()

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(A, "init", B), (A, "__init__")])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C), (C, "__init__"), (A, "__init__")])

    def test_Ai_B_Ci(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            pass

        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, "__init__"))
                super().__init__()

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (A, "__init__")])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C), (C, "__init__"), (A, "__init__")])

    def test_Ai_B_C(self):
        inits = []

        class A:
            def __init__(self):
                inits.append((A, "__init__"))

        self.register(A, inits)

        class B(A):
            pass

        self.register(B, inits)

        class C(B):
            pass

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A), (A, "__init__")])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (A, "__init__")])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C), (A, "__init__")])

    def test_A_Bi_C(self):
        inits = []

        class A:
            pass

        self.register(A, inits)

        class B(A):
            def __init__(self):
                inits.append((B, "__init__"))

        self.register(B, inits)

        class C(B):
            pass

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A)])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B), (B, "__init__")])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C), (B, "__init__")])

    def test_A_B_Ci(self):
        inits = []

        class A:
            pass

        self.register(A, inits)

        class B(A):
            pass

        self.register(B, inits)

        class C(B):
            def __init__(self):
                inits.append((C, "__init__"))

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A)])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B)])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C), (C, "__init__")])

    def test_A_B_C(self):
        inits = []

        class A:
            pass

        self.register(A, inits)

        class B(A):
            pass

        self.register(B, inits)

        class C(B):
            pass

        self.register(C, inits)

        A()
        eq_(inits, [(A, "init", A)])

        del inits[:]

        B()
        eq_(inits, [(B, "init", B)])

        del inits[:]
        C()
        eq_(inits, [(C, "init", C)])

    def test_defaulted_init(self):
        class X:
            def __init__(self_, a, b=123, c="abc"):
                self_.a = a
                self_.b = b
                self_.c = c

        instrumentation.register_class(X)

        o = X("foo")
        eq_(o.a, "foo")
        eq_(o.b, 123)
        eq_(o.c, "abc")

        class Y:
            unique = object()

            class OutOfScopeForEval:
                def __repr__(self_):
                    # misleading repr
                    return "123"

            outofscope = OutOfScopeForEval()

            def __init__(self_, u=unique, o=outofscope):
                self_.u = u
                self_.o = o

        instrumentation.register_class(Y)

        o = Y()
        assert o.u is Y.unique
        assert o.o is Y.outofscope


class MapperInitTest(fixtures.MappedTest):
    def fixture(self):
        return Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("type", Integer),
            Column("x", Integer),
            Column("y", Integer),
        )

    def test_partially_mapped_inheritance(self):
        class A:
            pass

        class B(A):
            pass

        class C(B):
            def __init__(self, x):
                pass

        self.mapper_registry.map_imperatively(A, self.fixture())

        # B is not mapped in the current implementation
        assert_raises(sa.orm.exc.UnmappedClassError, class_mapper, B)

        # C is not mapped in the current implementation
        assert_raises(sa.orm.exc.UnmappedClassError, class_mapper, C)

    def test_del_warning(self):
        class A:
            def __del__(self):
                pass

        assert_warns_message(
            sa.exc.SAWarning,
            r"__del__\(\) method on class "
            r"<class '.*\.A'> will cause "
            r"unreachable cycles and memory leaks, as SQLAlchemy "
            r"instrumentation often creates reference cycles.  "
            r"Please remove this method.",
            self.mapper_registry.map_imperatively,
            A,
            self.fixture(),
        )


class OnLoadTest(fixtures.ORMTest):
    """Check that Events.load is not hit in regular attributes operations."""

    def test_basic(self):
        import pickle

        global A

        class A:
            pass

        def canary(instance):
            assert False

        try:
            instrumentation.register_class(A)
            manager = instrumentation.manager_of_class(A)
            event.listen(manager, "load", canary)

            a = A()
            p_a = pickle.dumps(a)
            pickle.loads(p_a)
        finally:
            del A


class NativeInstrumentationTest(fixtures.MappedTest):
    def test_register_reserved_attribute(self):
        class T:
            pass

        instrumentation.register_class(T)
        manager = instrumentation.manager_of_class(T)

        sa = instrumentation.ClassManager.STATE_ATTR
        ma = instrumentation.ClassManager.MANAGER_ATTR

        def fails(method, attr):
            return assert_raises(
                KeyError, getattr(manager, method), attr, property()
            )

        fails("install_member", sa)
        fails("install_member", ma)
        fails("install_descriptor", sa)
        fails("install_descriptor", ma)

    def test_mapped_stateattr(self):
        t = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column(instrumentation.ClassManager.STATE_ATTR, Integer),
        )

        class T:
            pass

        assert_raises(KeyError, self.mapper_registry.map_imperatively, T, t)

    def test_mapped_managerattr(self):
        t = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column(instrumentation.ClassManager.MANAGER_ATTR, Integer),
        )

        class T:
            pass

        assert_raises(KeyError, self.mapper_registry.map_imperatively, T, t)


class Py3KFunctionInstTest(fixtures.ORMTest):
    def _instrument(self, cls):
        manager = instrumentation.register_class(cls)
        canary = []

        def check(target, args, kwargs):
            canary.append((args, kwargs))

        event.listen(manager, "init", check)
        return cls, canary

    def test_kw_only_args(self):
        cls, canary = self._kw_only_fixture()

        cls("a", b="b", c="c")
        eq_(canary, [(("a",), {"b": "b", "c": "c"})])

    def test_kw_plus_posn_args(self):
        cls, canary = self._kw_plus_posn_fixture()

        cls("a", 1, 2, 3, b="b", c="c")
        eq_(canary, [(("a", 1, 2, 3), {"b": "b", "c": "c"})])

    def test_kw_only_args_plus_opt(self):
        cls, canary = self._kw_opt_fixture()

        cls("a", b="b")
        eq_(canary, [(("a",), {"b": "b", "c": "c"})])

        canary[:] = []
        cls("a", b="b", c="d")
        eq_(canary, [(("a",), {"b": "b", "c": "d"})])

    def test_kw_only_sig(self):
        cls, canary = self._kw_only_fixture()
        assert_raises(TypeError, cls, "a", "b", "c")

    def test_kw_plus_opt_sig(self):
        cls, canary = self._kw_only_fixture()
        assert_raises(TypeError, cls, "a", "b", "c")

        assert_raises(TypeError, cls, "a", "b", c="c")

    def _kw_only_fixture(self):
        class A:
            def __init__(self, a, *, b, c):
                self.a = a
                self.b = b
                self.c = c

        return self._instrument(A)

    def _kw_plus_posn_fixture(self):
        class A:
            def __init__(self, a, *args, b, c):
                self.a = a
                self.b = b
                self.c = c

        return self._instrument(A)

    def _kw_opt_fixture(self):
        class A:
            def __init__(self, a, *, b, c="c"):
                self.a = a
                self.b = b
                self.c = c

        return self._instrument(A)


class MiscTest(fixtures.MappedTest):
    """Seems basic, but not directly covered elsewhere!"""

    def test_compileonattr(self):
        t = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )

        class A:
            pass

        self.mapper_registry.map_imperatively(A, t)

        a = A()
        assert a.id is None

    def test_compileonattr_rel(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("t1.id")),
        )

        class A:
            pass

        class B:
            pass

        self.mapper_registry.map_imperatively(
            A, t1, properties=dict(bs=relationship(B))
        )
        self.mapper_registry.map_imperatively(B, t2)

        a = A()
        assert not a.bs

    def test_uninstrument(self):
        class A:
            pass

        manager = instrumentation.register_class(A)
        attributes._register_attribute(
            A,
            "x",
            comparator=object(),
            parententity=object(),
            uselist=False,
            useobject=False,
        )

        assert instrumentation.manager_of_class(A) is manager
        instrumentation.unregister_class(A)
        assert instrumentation.opt_manager_of_class(A) is None
        assert not hasattr(A, "x")

        with expect_raises_message(
            sa.orm.exc.UnmappedClassError,
            r"Can't locate an instrumentation manager for class .*A",
        ):
            instrumentation.manager_of_class(A)

        assert A.__init__ == object.__init__

    def test_compileonattr_rel_backref_a(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("t1.id")),
        )

        class Base:
            def __init__(self, *args, **kwargs):
                pass

        for base in object, Base:

            class A(base):
                pass

            class B(base):
                pass

            self.mapper_registry.map_imperatively(
                A, t1, properties=dict(bs=relationship(B, backref="a"))
            )
            self.mapper_registry.map_imperatively(B, t2)

            b = B()
            assert b.a is None
            a = A()
            b.a = a

            session = fixture_session()
            session.add(b)
            assert a in session, "base is %s" % base

            clear_mappers()

    def test_compileonattr_rel_backref_b(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("t1.id")),
        )

        class Base:
            def __init__(self):
                pass

        class Base_AKW:
            def __init__(self, *args, **kwargs):
                pass

        for base in object, Base, Base_AKW:

            class A(base):
                pass

            class B(base):
                pass

            self.mapper_registry.map_imperatively(A, t1)
            self.mapper_registry.map_imperatively(
                B, t2, properties=dict(a=relationship(A, backref="bs"))
            )

            a = A()
            b = B()
            b.a = a

            session = fixture_session()
            session.add(a)
            assert b in session, "base: %s" % base
            clear_mappers()


class GeneratedInitSourceTest(fixtures.ORMTest):
    """test that the __init__ generated by _generate_init() has retrievable
    source, so that it renders in tracebacks and is visible to pdb and
    inspect.getsource().

    """

    def _fixture(self):
        class A:
            def __init__(self, x):
                self.x = x.upper()

        instrumentation.register_class(A)
        return A

    def test_descriptive_filename(self):
        """each mapped class names its own filename; two classes must not
        share a linecache key, else one would render the other's source.

        """

        class A:
            def __init__(self, x):
                self.x = x

        class B:
            def __init__(self, y, z):
                self.y = y

        for cls in (A, B):
            instrumentation.register_class(cls)

        prefix = (
            f"<sqlalchemy generated __init__ for class {__name__}."
            "GeneratedInitSourceTest.test_descriptive_filename.<locals>."
        )
        eq_(
            [
                A.__init__.__code__.co_filename,
                B.__init__.__code__.co_filename,
            ],
            [f"{prefix}A>", f"{prefix}B>"],
        )

    def test_survives_checkcache(self):
        """the entry is registered with a mtime of None, which
        checkcache() is documented to skip; were it any other value the
        entry would be dropped as its filename has no file behind it.

        """
        A = self._fixture()
        filename = A.__init__.__code__.co_filename

        linecache.checkcache()

        eq_(linecache.getlines(filename)[0], "def __init__(self, x):\n")

    @testing.variation("retrieval", ["linecache", "getsource"])
    def test_source_retrievable(self, retrieval):
        A = self._fixture()

        lines = [
            "def __init__(self, x):\n",
            "    new_state = class_manager._new_state_if_none(self)\n",
            "    if new_state:\n",
            "        return new_state._initialize_instance(self, x)\n",
            "    else:\n",
            "        return original_init(self, x)\n",
        ]

        if retrieval.linecache:
            eq_(linecache.getlines(A.__init__.__code__.co_filename), lines)
        elif retrieval.getsource:
            eq_(inspect.getsource(A.__init__), "".join(lines))
        else:
            retrieval.fail()

    def test_traceback_renders_generated_frame(self):
        """the whole point: an error raised through a mapped class'
        __init__ shows our frame with its source line rather than a bare
        File "<string>".

        """
        A = self._fixture()

        try:
            A(42)
        except AttributeError:
            frames = traceback.extract_tb(sys.exc_info()[2])
        else:
            assert False, "expected AttributeError"

        generated = [
            f for f in frames if f.filename.startswith("<sqlalchemy generated")
        ]
        eq_(
            [(f.name, f.line) for f in generated],
            [("__init__", "return new_state._initialize_instance(self, x)")],
        )

    def test_discarded_class_drops_entry(self):
        """classes built on the fly and then thrown away, e.g. by
        ``type("A", (Base,), {})``, must not leave their generated source
        behind in linecache.

        """
        filenames = []
        for i in range(3):
            cls = type(f"A{i}", (object,), {"__init__": lambda self, x: None})
            instrumentation.register_class(cls)
            filenames.append(cls.__init__.__code__.co_filename)
            eq_(
                linecache.getlines(filenames[-1])[0],
                "def __init__(self, x):\n",
            )
            del cls

        gc_collect()

        eq_([f for f in filenames if f in linecache.cache], [])
