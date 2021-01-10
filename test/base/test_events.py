"""Test event registration and listening."""

from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import testing
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.util import gc_collect


class TearDownLocalEventsFixture(object):
    def teardown_test(self):
        classes = set()
        for entry in event.base._registrars.values():
            for evt_cls in entry:
                if evt_cls.__module__ == __name__:
                    classes.add(evt_cls)

        for evt_cls in classes:
            event.base._remove_dispatcher(evt_cls)


class EventsTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """Test class- and instance-level event registration."""

    def setup_test(self):
        class TargetEvents(event.Events):
            def event_one(self, x, y):
                pass

            def event_two(self, x):
                pass

            def event_three(self, x):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        self.Target = Target

    def test_register_class(self):
        def listen(x, y):
            pass

        event.listen(self.Target, "event_one", listen)

        eq_(len(self.Target().dispatch.event_one), 1)
        eq_(len(self.Target().dispatch.event_two), 0)

    def test_register_instance(self):
        def listen(x, y):
            pass

        t1 = self.Target()
        event.listen(t1, "event_one", listen)

        eq_(len(self.Target().dispatch.event_one), 0)
        eq_(len(t1.dispatch.event_one), 1)
        eq_(len(self.Target().dispatch.event_two), 0)
        eq_(len(t1.dispatch.event_two), 0)

    def test_bool_clslevel(self):
        def listen_one(x, y):
            pass

        event.listen(self.Target, "event_one", listen_one)
        t = self.Target()
        assert t.dispatch.event_one

    def test_register_class_instance(self):
        def listen_one(x, y):
            pass

        def listen_two(x, y):
            pass

        event.listen(self.Target, "event_one", listen_one)

        t1 = self.Target()
        event.listen(t1, "event_one", listen_two)

        eq_(len(self.Target().dispatch.event_one), 1)
        eq_(len(t1.dispatch.event_one), 2)
        eq_(len(self.Target().dispatch.event_two), 0)
        eq_(len(t1.dispatch.event_two), 0)

        def listen_three(x, y):
            pass

        event.listen(self.Target, "event_one", listen_three)
        eq_(len(self.Target().dispatch.event_one), 2)
        eq_(len(t1.dispatch.event_one), 3)

    def test_append_vs_insert_cls(self):
        def listen_one(x, y):
            pass

        def listen_two(x, y):
            pass

        def listen_three(x, y):
            pass

        event.listen(self.Target, "event_one", listen_one)
        event.listen(self.Target, "event_one", listen_two)
        event.listen(self.Target, "event_one", listen_three, insert=True)

        eq_(
            list(self.Target().dispatch.event_one),
            [listen_three, listen_one, listen_two],
        )

    def test_append_vs_insert_instance(self):
        def listen_one(x, y):
            pass

        def listen_two(x, y):
            pass

        def listen_three(x, y):
            pass

        target = self.Target()
        event.listen(target, "event_one", listen_one)
        event.listen(target, "event_one", listen_two)
        event.listen(target, "event_one", listen_three, insert=True)

        eq_(
            list(target.dispatch.event_one),
            [listen_three, listen_one, listen_two],
        )

    def test_decorator(self):
        @event.listens_for(self.Target, "event_one")
        def listen_one(x, y):
            pass

        @event.listens_for(self.Target, "event_two")
        @event.listens_for(self.Target, "event_three")
        def listen_two(x, y):
            pass

        eq_(list(self.Target().dispatch.event_one), [listen_one])

        eq_(list(self.Target().dispatch.event_two), [listen_two])

        eq_(list(self.Target().dispatch.event_three), [listen_two])

    def test_no_instance_level_collections(self):
        @event.listens_for(self.Target, "event_one")
        def listen_one(x, y):
            pass

        t1 = self.Target()
        t2 = self.Target()
        t1.dispatch.event_one(5, 6)
        t2.dispatch.event_one(5, 6)
        is_(
            self.Target.dispatch._empty_listener_reg[self.Target]["event_one"],
            t1.dispatch.event_one,
        )

        @event.listens_for(t1, "event_one")
        def listen_two(x, y):
            pass

        is_not(
            self.Target.dispatch._empty_listener_reg[self.Target]["event_one"],
            t1.dispatch.event_one,
        )
        is_(
            self.Target.dispatch._empty_listener_reg[self.Target]["event_one"],
            t2.dispatch.event_one,
        )

    def test_exec_once(self):
        m1 = Mock()

        event.listen(self.Target, "event_one", m1)

        t1 = self.Target()
        t2 = self.Target()

        t1.dispatch.event_one.for_modify(t1.dispatch).exec_once(5, 6)

        t1.dispatch.event_one.for_modify(t1.dispatch).exec_once(7, 8)

        t2.dispatch.event_one.for_modify(t2.dispatch).exec_once(9, 10)

        eq_(m1.mock_calls, [call(5, 6), call(9, 10)])

    def test_exec_once_exception(self):
        m1 = Mock()
        m1.side_effect = ValueError

        event.listen(self.Target, "event_one", m1)

        t1 = self.Target()

        assert_raises(
            ValueError,
            t1.dispatch.event_one.for_modify(t1.dispatch).exec_once,
            5,
            6,
        )

        t1.dispatch.event_one.for_modify(t1.dispatch).exec_once(7, 8)

        eq_(m1.mock_calls, [call(5, 6)])

    def test_exec_once_unless_exception(self):
        m1 = Mock()
        m1.side_effect = ValueError

        event.listen(self.Target, "event_one", m1)

        t1 = self.Target()

        assert_raises(
            ValueError,
            t1.dispatch.event_one.for_modify(
                t1.dispatch
            ).exec_once_unless_exception,
            5,
            6,
        )

        assert_raises(
            ValueError,
            t1.dispatch.event_one.for_modify(
                t1.dispatch
            ).exec_once_unless_exception,
            7,
            8,
        )

        m1.side_effect = None
        t1.dispatch.event_one.for_modify(
            t1.dispatch
        ).exec_once_unless_exception(9, 10)

        t1.dispatch.event_one.for_modify(
            t1.dispatch
        ).exec_once_unless_exception(11, 12)

        eq_(m1.mock_calls, [call(5, 6), call(7, 8), call(9, 10)])

    def test_immutable_methods(self):
        t1 = self.Target()
        for meth in [
            t1.dispatch.event_one.exec_once,
            t1.dispatch.event_one.insert,
            t1.dispatch.event_one.append,
            t1.dispatch.event_one.remove,
            t1.dispatch.event_one.clear,
        ]:
            assert_raises_message(
                NotImplementedError, r"need to call for_modify\(\)", meth
            )


class SlotsEventsTest(fixtures.TestBase):
    @testing.requires.python3
    def test_no_slots_dispatch(self):
        class Target(object):
            __slots__ = ()

        class TargetEvents(event.Events):
            _dispatch_target = Target

            def event_one(self, x, y):
                pass

            def event_two(self, x):
                pass

            def event_three(self, x):
                pass

        t1 = Target()

        with testing.expect_raises_message(
            TypeError,
            r"target .*Target.* doesn't have __dict__, should it "
            "be defining _slots_dispatch",
        ):
            event.listen(t1, "event_one", Mock())

    def test_slots_dispatch(self):
        class Target(object):
            __slots__ = ("_slots_dispatch",)

        class TargetEvents(event.Events):
            _dispatch_target = Target

            def event_one(self, x, y):
                pass

            def event_two(self, x):
                pass

            def event_three(self, x):
                pass

        t1 = Target()

        m1 = Mock()
        event.listen(t1, "event_one", m1)

        t1.dispatch.event_one(2, 4)

        eq_(m1.mock_calls, [call(2, 4)])


class NamedCallTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def _fixture(self):
        class TargetEventsOne(event.Events):
            def event_one(self, x, y):
                pass

            def event_two(self, x, y, **kw):
                pass

            def event_five(self, x, y, z, q):
                pass

        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)

        return TargetOne

    def _wrapped_fixture(self):
        class TargetEvents(event.Events):
            @classmethod
            def _listen(cls, event_key):
                fn = event_key._listen_fn

                def adapt(*args):
                    fn(*["adapted %s" % arg for arg in args])

                event_key = event_key.with_wrapper(adapt)

                event_key.base_listen()

            def event_one(self, x, y):
                pass

            def event_five(self, x, y, z, q):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        return Target

    def test_kw_accept(self):
        TargetOne = self._fixture()

        canary = Mock()

        @event.listens_for(TargetOne, "event_one", named=True)
        def handler1(**kw):
            canary(kw)

        TargetOne().dispatch.event_one(4, 5)

        eq_(canary.mock_calls, [call({"x": 4, "y": 5})])

    def test_kw_accept_wrapped(self):
        TargetOne = self._wrapped_fixture()

        canary = Mock()

        @event.listens_for(TargetOne, "event_one", named=True)
        def handler1(**kw):
            canary(kw)

        TargetOne().dispatch.event_one(4, 5)

        eq_(canary.mock_calls, [call({"y": "adapted 5", "x": "adapted 4"})])

    def test_partial_kw_accept(self):
        TargetOne = self._fixture()

        canary = Mock()

        @event.listens_for(TargetOne, "event_five", named=True)
        def handler1(z, y, **kw):
            canary(z, y, kw)

        TargetOne().dispatch.event_five(4, 5, 6, 7)

        eq_(canary.mock_calls, [call(6, 5, {"x": 4, "q": 7})])

    def test_partial_kw_accept_wrapped(self):
        TargetOne = self._wrapped_fixture()

        canary = Mock()

        @event.listens_for(TargetOne, "event_five", named=True)
        def handler1(z, y, **kw):
            canary(z, y, kw)

        TargetOne().dispatch.event_five(4, 5, 6, 7)

        eq_(
            canary.mock_calls,
            [
                call(
                    "adapted 6",
                    "adapted 5",
                    {"q": "adapted 7", "x": "adapted 4"},
                )
            ],
        )

    def test_kw_accept_plus_kw(self):
        TargetOne = self._fixture()
        canary = Mock()

        @event.listens_for(TargetOne, "event_two", named=True)
        def handler1(**kw):
            canary(kw)

        TargetOne().dispatch.event_two(4, 5, z=8, q=5)

        eq_(canary.mock_calls, [call({"x": 4, "y": 5, "z": 8, "q": 5})])


class LegacySignatureTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """test adaption of legacy args"""

    def setup_test(self):
        class TargetEventsOne(event.Events):
            @event._legacy_signature("0.9", ["x", "y"])
            def event_three(self, x, y, z, q):
                pass

            @event._legacy_signature("0.9", ["x", "y", "**kw"])
            def event_four(self, x, y, z, q, **kw):
                pass

            @event._legacy_signature(
                "0.9", ["x", "y", "z", "q"], lambda x, y: (x, y, x + y, x * y)
            )
            def event_six(self, x, y):
                pass

        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)

        self.TargetOne = TargetOne

    def test_legacy_accept(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_three")
        def handler1(x, y):
            canary(x, y)

        with expect_deprecated(
            'The argument signature for the "TargetEventsOne.event_three" '
            "event listener has changed as of version 0.9, and conversion "
            "for the old argument signature will be removed in a future "
            r'release.  The new signature is "def event_three\(x, y, z, q\)"'
        ):
            self.TargetOne().dispatch.event_three(4, 5, 6, 7)

        eq_(canary.mock_calls, [call(4, 5)])

    def test_legacy_accept_kw_cls(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_four")
        def handler1(x, y, **kw):
            canary(x, y, kw)

        self._test_legacy_accept_kw(self.TargetOne(), canary)

    def test_legacy_accept_kw_instance(self):
        canary = Mock()

        inst = self.TargetOne()

        @event.listens_for(inst, "event_four")
        def handler1(x, y, **kw):
            canary(x, y, kw)

        self._test_legacy_accept_kw(inst, canary)

    def test_legacy_accept_partial(self):
        canary = Mock()

        def evt(a, x, y, **kw):
            canary(a, x, y, **kw)

        from functools import partial

        evt_partial = partial(evt, 5)
        target = self.TargetOne()
        event.listen(target, "event_four", evt_partial)
        # can't do legacy accept on a partial; we can't inspect it
        assert_raises(
            TypeError, target.dispatch.event_four, 4, 5, 6, 7, foo="bar"
        )
        target.dispatch.event_four(4, 5, foo="bar")
        eq_(canary.mock_calls, [call(5, 4, 5, foo="bar")])

    def _test_legacy_accept_kw(self, target, canary):
        with expect_deprecated(
            'The argument signature for the "TargetEventsOne.event_four" '
            "event listener has changed as of version 0.9, and conversion "
            "for the old argument signature will be removed in a future "
            r"release.  The new signature is "
            r'"def event_four\(x, y, z, q, \*\*kw\)"'
        ):
            target.dispatch.event_four(4, 5, 6, 7, foo="bar")

        eq_(canary.mock_calls, [call(4, 5, {"foo": "bar"})])

    def test_complex_legacy_accept(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_six")
        def handler1(x, y, z, q):
            canary(x, y, z, q)

        with expect_deprecated(
            'The argument signature for the "TargetEventsOne.event_six" '
            "event listener has changed as of version 0.9, and "
            "conversion for the old argument signature will be removed in "
            "a future release.  The new signature is "
            r'"def event_six\(x, y\)'
        ):
            self.TargetOne().dispatch.event_six(4, 5)
        eq_(canary.mock_calls, [call(4, 5, 9, 20)])

    def test_complex_new_accept(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_six")
        def handler1(x, y):
            canary(x, y)

        # new version does not emit a warning
        self.TargetOne().dispatch.event_six(4, 5)
        eq_(canary.mock_calls, [call(4, 5)])

    def test_legacy_accept_from_method(self):
        canary = Mock()

        class MyClass(object):
            def handler1(self, x, y):
                canary(x, y)

        event.listen(self.TargetOne, "event_three", MyClass().handler1)

        with expect_deprecated(
            'The argument signature for the "TargetEventsOne.event_three" '
            "event listener has changed as of version 0.9, and conversion "
            "for the old argument signature will be removed in a future "
            r'release.  The new signature is "def event_three\(x, y, z, q\)"'
        ):
            self.TargetOne().dispatch.event_three(4, 5, 6, 7)
        eq_(canary.mock_calls, [call(4, 5)])

    def test_standard_accept_has_legacies(self):
        canary = Mock()

        event.listen(self.TargetOne, "event_three", canary)

        self.TargetOne().dispatch.event_three(4, 5)

        eq_(canary.mock_calls, [call(4, 5)])

    def test_kw_accept_has_legacies(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_three", named=True)
        def handler1(**kw):
            canary(kw)

        self.TargetOne().dispatch.event_three(4, 5, 6, 7)

        eq_(canary.mock_calls, [call({"x": 4, "y": 5, "z": 6, "q": 7})])

    def test_kw_accept_plus_kw_has_legacies(self):
        canary = Mock()

        @event.listens_for(self.TargetOne, "event_four", named=True)
        def handler1(**kw):
            canary(kw)

        self.TargetOne().dispatch.event_four(4, 5, 6, 7, foo="bar")

        eq_(
            canary.mock_calls,
            [call({"x": 4, "y": 5, "z": 6, "q": 7, "foo": "bar"})],
        )


class ClsLevelListenTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def setup_test(self):
        class TargetEventsOne(event.Events):
            def event_one(self, x, y):
                pass

        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)

        self.TargetOne = TargetOne

    def test_lis_subcalss_lis(self):
        @event.listens_for(self.TargetOne, "event_one")
        def handler1(x, y):
            pass

        class SubTarget(self.TargetOne):
            pass

        @event.listens_for(self.TargetOne, "event_one")
        def handler2(x, y):
            pass

        eq_(len(SubTarget().dispatch.event_one), 2)

    def test_lis_multisub_lis(self):
        @event.listens_for(self.TargetOne, "event_one")
        def handler1(x, y):
            pass

        class SubTarget(self.TargetOne):
            pass

        class SubSubTarget(SubTarget):
            pass

        @event.listens_for(self.TargetOne, "event_one")
        def handler2(x, y):
            pass

        eq_(len(SubTarget().dispatch.event_one), 2)
        eq_(len(SubSubTarget().dispatch.event_one), 2)

    def test_two_sub_lis(self):
        class SubTarget1(self.TargetOne):
            pass

        class SubTarget2(self.TargetOne):
            pass

        @event.listens_for(self.TargetOne, "event_one")
        def handler1(x, y):
            pass

        @event.listens_for(SubTarget1, "event_one")
        def handler2(x, y):
            pass

        s1 = SubTarget1()
        assert handler1 in s1.dispatch.event_one
        assert handler2 in s1.dispatch.event_one

        s2 = SubTarget2()
        assert handler1 in s2.dispatch.event_one
        assert handler2 not in s2.dispatch.event_one


class AcceptTargetsTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """Test default target acceptance."""

    def setup_test(self):
        class TargetEventsOne(event.Events):
            def event_one(self, x, y):
                pass

        class TargetEventsTwo(event.Events):
            def event_one(self, x, y):
                pass

        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)

        class TargetTwo(object):
            dispatch = event.dispatcher(TargetEventsTwo)

        self.TargetOne = TargetOne
        self.TargetTwo = TargetTwo

    def test_target_accept(self):
        """Test that events of the same name are routed to the correct
        collection based on the type of target given.

        """

        def listen_one(x, y):
            pass

        def listen_two(x, y):
            pass

        def listen_three(x, y):
            pass

        def listen_four(x, y):
            pass

        event.listen(self.TargetOne, "event_one", listen_one)
        event.listen(self.TargetTwo, "event_one", listen_two)

        eq_(list(self.TargetOne().dispatch.event_one), [listen_one])

        eq_(list(self.TargetTwo().dispatch.event_one), [listen_two])

        t1 = self.TargetOne()
        t2 = self.TargetTwo()

        event.listen(t1, "event_one", listen_three)
        event.listen(t2, "event_one", listen_four)

        eq_(list(t1.dispatch.event_one), [listen_one, listen_three])

        eq_(list(t2.dispatch.event_one), [listen_two, listen_four])


class CustomTargetsTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """Test custom target acceptance."""

    def setup_test(self):
        class TargetEvents(event.Events):
            @classmethod
            def _accept_with(cls, target):
                if target == "one":
                    return Target
                else:
                    return None

            def event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        self.Target = Target

    def test_indirect(self):
        def listen(x, y):
            pass

        event.listen("one", "event_one", listen)

        eq_(list(self.Target().dispatch.event_one), [listen])

        assert_raises(
            exc.InvalidRequestError,
            event.listen,
            listen,
            "event_one",
            self.Target,
        )


class SubclassGrowthTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """test that ad-hoc subclasses are garbage collected."""

    def setup_test(self):
        class TargetEvents(event.Events):
            def some_event(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        self.Target = Target

    def test_subclass(self):
        class SubTarget(self.Target):
            pass

        st = SubTarget()
        st.dispatch.some_event(1, 2)
        del st
        del SubTarget
        gc_collect()
        eq_(self.Target.__subclasses__(), [])


class ListenOverrideTest(TearDownLocalEventsFixture, fixtures.TestBase):
    """Test custom listen functions which change the listener function
    signature."""

    def setup_test(self):
        class TargetEvents(event.Events):
            @classmethod
            def _listen(cls, event_key, add=False):
                fn = event_key.fn
                if add:

                    def adapt(x, y):
                        fn(x + y)

                    event_key = event_key.with_wrapper(adapt)

                event_key.base_listen()

            def event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        self.Target = Target

    def test_listen_override(self):
        listen_one = Mock()
        listen_two = Mock()

        event.listen(self.Target, "event_one", listen_one, add=True)
        event.listen(self.Target, "event_one", listen_two)

        t1 = self.Target()
        t1.dispatch.event_one(5, 7)
        t1.dispatch.event_one(10, 5)

        eq_(listen_one.mock_calls, [call(12), call(15)])
        eq_(listen_two.mock_calls, [call(5, 7), call(10, 5)])

    def test_remove_clslevel(self):
        listen_one = Mock()
        event.listen(self.Target, "event_one", listen_one, add=True)
        t1 = self.Target()
        t1.dispatch.event_one(5, 7)
        eq_(listen_one.mock_calls, [call(12)])
        event.remove(self.Target, "event_one", listen_one)
        t1.dispatch.event_one(10, 5)
        eq_(listen_one.mock_calls, [call(12)])

    def test_remove_instancelevel(self):
        listen_one = Mock()
        t1 = self.Target()
        event.listen(t1, "event_one", listen_one, add=True)
        t1.dispatch.event_one(5, 7)
        eq_(listen_one.mock_calls, [call(12)])
        event.remove(t1, "event_one", listen_one)
        t1.dispatch.event_one(10, 5)
        eq_(listen_one.mock_calls, [call(12)])


class PropagateTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def setup_test(self):
        class TargetEvents(event.Events):
            def event_one(self, arg):
                pass

            def event_two(self, arg):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        self.Target = Target

    def test_propagate(self):
        listen_one = Mock()
        listen_two = Mock()

        t1 = self.Target()

        event.listen(t1, "event_one", listen_one, propagate=True)
        event.listen(t1, "event_two", listen_two)

        t2 = self.Target()

        t2.dispatch._update(t1.dispatch)

        t2.dispatch.event_one(t2, 1)
        t2.dispatch.event_two(t2, 2)

        eq_(listen_one.mock_calls, [call(t2, 1)])
        eq_(listen_two.mock_calls, [])


class JoinTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def setup_test(self):
        class TargetEvents(event.Events):
            def event_one(self, target, arg):
                pass

        class BaseTarget(object):
            dispatch = event.dispatcher(TargetEvents)

        class TargetFactory(BaseTarget):
            def create(self):
                return TargetElement(self)

        class TargetElement(BaseTarget):
            def __init__(self, parent):
                self.dispatch = self.dispatch._join(parent.dispatch)

            def run_event(self, arg):
                list(self.dispatch.event_one)
                self.dispatch.event_one(self, arg)

        self.BaseTarget = BaseTarget
        self.TargetFactory = TargetFactory
        self.TargetElement = TargetElement

    def test_neither(self):
        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

    def test_kw_ok(self):
        l1 = Mock()

        def listen(**kw):
            l1(kw)

        event.listen(self.TargetFactory, "event_one", listen, named=True)
        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        eq_(
            l1.mock_calls,
            [
                call({"target": element, "arg": 1}),
                call({"target": element, "arg": 2}),
            ],
        )

    def test_parent_class_only(self):
        l1 = Mock()

        event.listen(self.TargetFactory, "event_one", l1)

        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        element.run_event(3)
        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_class_child_class(self):
        l1 = Mock()
        l2 = Mock()

        event.listen(self.TargetFactory, "event_one", l1)
        event.listen(self.TargetElement, "event_one", l2)

        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        element.run_event(3)
        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_class_child_instance_apply_after(self):
        l1 = Mock()
        l2 = Mock()

        event.listen(self.TargetFactory, "event_one", l1)
        element = self.TargetFactory().create()

        element.run_event(1)

        event.listen(element, "event_one", l2)
        element.run_event(2)
        element.run_event(3)

        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )
        eq_(l2.mock_calls, [call(element, 2), call(element, 3)])

    def test_parent_class_child_instance_apply_before(self):
        l1 = Mock()
        l2 = Mock()

        event.listen(self.TargetFactory, "event_one", l1)
        element = self.TargetFactory().create()

        event.listen(element, "event_one", l2)

        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_instance_child_class_apply_before(self):
        l1 = Mock()
        l2 = Mock()

        event.listen(self.TargetElement, "event_one", l2)

        factory = self.TargetFactory()
        event.listen(factory, "event_one", l1)

        element = factory.create()

        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_instance_child_class_apply_after(self):
        l1 = Mock()
        l2 = Mock()

        event.listen(self.TargetElement, "event_one", l2)

        factory = self.TargetFactory()
        element = factory.create()

        element.run_event(1)

        event.listen(factory, "event_one", l1)

        element.run_event(2)
        element.run_event(3)

        # if _JoinedListener fixes .listeners
        # at construction time, then we don't get
        # the new listeners.
        # eq_(l1.mock_calls, [])

        # alternatively, if _JoinedListener shares the list
        # using a @property, then we get them, at the arguable
        # expense of the extra method call to access the .listeners
        # collection
        eq_(l1.mock_calls, [call(element, 2), call(element, 3)])

        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_instance_child_instance_apply_before(self):
        l1 = Mock()
        l2 = Mock()
        factory = self.TargetFactory()

        event.listen(factory, "event_one", l1)

        element = factory.create()
        event.listen(element, "event_one", l2)

        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )

    def test_parent_events_child_no_events(self):
        l1 = Mock()
        factory = self.TargetFactory()

        event.listen(self.TargetElement, "event_one", l1)
        element = factory.create()

        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)],
        )


class DisableClsPropagateTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def setup_test(self):
        class TargetEvents(event.Events):
            def event_one(self, target, arg):
                pass

        class BaseTarget(object):
            dispatch = event.dispatcher(TargetEvents)

        class SubTarget(BaseTarget):
            _sa_propagate_class_events = False

            def __init__(self, parent):
                self.dispatch = self.dispatch._join(parent.dispatch)

        self.BaseTarget = BaseTarget
        self.SubTarget = SubTarget

    def test_listen_invoke_clslevel(self):
        canary = Mock()

        event.listen(self.BaseTarget, "event_one", canary)

        s1 = self.SubTarget(self.BaseTarget())
        s1.dispatch.event_one()

        eq_(canary.mock_calls, [call.event_one()])

    def test_insert_invoke_clslevel(self):
        canary = Mock()

        event.listen(self.BaseTarget, "event_one", canary, insert=True)

        s1 = self.SubTarget(self.BaseTarget())
        s1.dispatch.event_one()

        eq_(canary.mock_calls, [call.event_one()])

    def test_remove_invoke_clslevel(self):
        canary = Mock()

        event.listen(self.BaseTarget, "event_one", canary)

        s1 = self.SubTarget(self.BaseTarget())

        event.remove(self.BaseTarget, "event_one", canary)

        s1.dispatch.event_one()

        eq_(canary.mock_calls, [])


class RemovalTest(TearDownLocalEventsFixture, fixtures.TestBase):
    def _fixture(self):
        class TargetEvents(event.Events):
            def event_one(self, x, y):
                pass

            def event_two(self, x):
                pass

            def event_three(self, x):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        return Target

    def _wrapped_fixture(self):
        class TargetEvents(event.Events):
            @classmethod
            def _listen(cls, event_key):
                fn = event_key._listen_fn

                def adapt(value):
                    fn("adapted " + value)

                event_key = event_key.with_wrapper(adapt)

                event_key.base_listen()

            def event_one(self, x):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

        return Target

    def test_clslevel(self):
        Target = self._fixture()

        m1 = Mock()

        event.listen(Target, "event_two", m1)

        t1 = Target()
        t1.dispatch.event_two("x")

        event.remove(Target, "event_two", m1)

        t1.dispatch.event_two("y")

        eq_(m1.mock_calls, [call("x")])

    def test_clslevel_subclass(self):
        Target = self._fixture()

        class SubTarget(Target):
            pass

        m1 = Mock()

        event.listen(Target, "event_two", m1)

        t1 = SubTarget()
        t1.dispatch.event_two("x")

        event.remove(Target, "event_two", m1)

        t1.dispatch.event_two("y")

        eq_(m1.mock_calls, [call("x")])

    def test_instance(self):
        Target = self._fixture()

        class Foo(object):
            def __init__(self):
                self.mock = Mock()

            def evt(self, arg):
                self.mock(arg)

        f1 = Foo()
        f2 = Foo()

        event.listen(Target, "event_one", f1.evt)
        event.listen(Target, "event_one", f2.evt)

        t1 = Target()
        t1.dispatch.event_one("x")

        event.remove(Target, "event_one", f1.evt)

        t1.dispatch.event_one("y")

        eq_(f1.mock.mock_calls, [call("x")])
        eq_(f2.mock.mock_calls, [call("x"), call("y")])

    def test_once(self):
        Target = self._fixture()

        m1 = Mock()
        m2 = Mock()
        m3 = Mock()
        m4 = Mock()

        event.listen(Target, "event_one", m1)
        event.listen(Target, "event_one", m2, once=True)
        event.listen(Target, "event_one", m3, once=True)

        t1 = Target()
        t1.dispatch.event_one("x")
        t1.dispatch.event_one("y")

        event.listen(Target, "event_one", m4, once=True)
        t1.dispatch.event_one("z")
        t1.dispatch.event_one("q")

        eq_(m1.mock_calls, [call("x"), call("y"), call("z"), call("q")])
        eq_(m2.mock_calls, [call("x")])
        eq_(m3.mock_calls, [call("x")])
        eq_(m4.mock_calls, [call("z")])

    def test_once_unless_exception(self):
        Target = self._fixture()

        m1 = Mock()
        m2 = Mock()
        m3 = Mock()
        m4 = Mock()

        m1.side_effect = ValueError
        m2.side_effect = ValueError
        m3.side_effect = ValueError

        event.listen(Target, "event_one", m1)
        event.listen(Target, "event_one", m2, _once_unless_exception=True)
        event.listen(Target, "event_one", m3, _once_unless_exception=True)

        t1 = Target()

        # only m1 is called, raises
        assert_raises(ValueError, t1.dispatch.event_one, "x")

        # now m1 and m2 can be called but not m3
        m1.side_effect = None

        assert_raises(ValueError, t1.dispatch.event_one, "y")

        # now m3 can be called
        m2.side_effect = None

        event.listen(Target, "event_one", m4, _once_unless_exception=True)
        assert_raises(ValueError, t1.dispatch.event_one, "z")

        assert_raises(ValueError, t1.dispatch.event_one, "q")

        eq_(m1.mock_calls, [call("x"), call("y"), call("z"), call("q")])
        eq_(m2.mock_calls, [call("y"), call("z")])
        eq_(m3.mock_calls, [call("z"), call("q")])
        eq_(m4.mock_calls, [])  # m4 never got called because m3 blocked it

        # now m4 can be called
        m3.side_effect = None

        t1.dispatch.event_one("p")
        eq_(
            m1.mock_calls,
            [call("x"), call("y"), call("z"), call("q"), call("p")],
        )

        # m2 already got called, so no "p"
        eq_(m2.mock_calls, [call("y"), call("z")])
        eq_(m3.mock_calls, [call("z"), call("q"), call("p")])
        eq_(m4.mock_calls, [call("p")])

        t1.dispatch.event_one("j")
        eq_(
            m1.mock_calls,
            [call("x"), call("y"), call("z"), call("q"), call("p"), call("j")],
        )

        # nobody got "j" because they've all been successful
        eq_(m2.mock_calls, [call("y"), call("z")])
        eq_(m3.mock_calls, [call("z"), call("q"), call("p")])
        eq_(m4.mock_calls, [call("p")])

    def test_once_doesnt_dereference_listener(self):
        # test for [ticket:4794]

        Target = self._fixture()

        canary = Mock()

        def go(target, given_id):
            def anonymous(run_id):
                canary(run_id, given_id)

            event.listen(target, "event_one", anonymous, once=True)

        t1 = Target()

        assert_calls = []
        given_ids = []
        for given_id in range(100):
            given_ids.append(given_id)
            go(t1, given_id)
            if given_id % 10 == 0:
                t1.dispatch.event_one(given_id)
                assert_calls.extend(call(given_id, i) for i in given_ids)
                given_ids[:] = []

        eq_(canary.mock_calls, assert_calls)

    def test_propagate(self):
        Target = self._fixture()

        m1 = Mock()

        t1 = Target()
        t2 = Target()

        event.listen(t1, "event_one", m1, propagate=True)
        event.listen(t1, "event_two", m1, propagate=False)

        t2.dispatch._update(t1.dispatch)

        t1.dispatch.event_one("t1e1x")
        t1.dispatch.event_two("t1e2x")
        t2.dispatch.event_one("t2e1x")
        t2.dispatch.event_two("t2e2x")

        event.remove(t1, "event_one", m1)
        event.remove(t1, "event_two", m1)

        t1.dispatch.event_one("t1e1y")
        t1.dispatch.event_two("t1e2y")
        t2.dispatch.event_one("t2e1y")
        t2.dispatch.event_two("t2e2y")

        eq_(m1.mock_calls, [call("t1e1x"), call("t1e2x"), call("t2e1x")])

    @testing.requires.predictable_gc
    def test_listener_collection_removed_cleanup(self):
        from sqlalchemy.event import registry

        Target = self._fixture()

        m1 = Mock()

        t1 = Target()

        event.listen(t1, "event_one", m1)

        key = (id(t1), "event_one", id(m1))

        assert key in registry._key_to_collection
        collection_ref = list(registry._key_to_collection[key])[0]
        assert collection_ref in registry._collection_to_key

        t1.dispatch.event_one("t1")

        del t1

        gc_collect()

        assert key not in registry._key_to_collection
        assert collection_ref not in registry._collection_to_key

    def test_remove_not_listened(self):
        Target = self._fixture()

        m1 = Mock()

        t1 = Target()

        event.listen(t1, "event_one", m1, propagate=True)
        event.listen(t1, "event_three", m1)

        event.remove(t1, "event_one", m1)
        assert_raises_message(
            exc.InvalidRequestError,
            r"No listeners found for event <.*Target.*> / "
            r"'event_two' / <Mock.*> ",
            event.remove,
            t1,
            "event_two",
            m1,
        )

        event.remove(t1, "event_three", m1)

    def test_no_remove_in_event(self):
        Target = self._fixture()

        t1 = Target()

        def evt():
            event.remove(t1, "event_one", evt)

        event.listen(t1, "event_one", evt)

        assert_raises_message(
            Exception, "deque mutated during iteration", t1.dispatch.event_one
        )

    def test_no_add_in_event(self):
        Target = self._fixture()

        t1 = Target()

        m1 = Mock()

        def evt():
            event.listen(t1, "event_one", m1)

        event.listen(t1, "event_one", evt)

        assert_raises_message(
            Exception, "deque mutated during iteration", t1.dispatch.event_one
        )

    def test_remove_plain_named(self):
        Target = self._fixture()

        listen_one = Mock()
        t1 = Target()
        event.listen(t1, "event_one", listen_one, named=True)
        t1.dispatch.event_one("t1")

        eq_(listen_one.mock_calls, [call(x="t1")])
        event.remove(t1, "event_one", listen_one)
        t1.dispatch.event_one("t2")

        eq_(listen_one.mock_calls, [call(x="t1")])

    def test_remove_wrapped_named(self):
        Target = self._wrapped_fixture()

        listen_one = Mock()
        t1 = Target()
        event.listen(t1, "event_one", listen_one, named=True)
        t1.dispatch.event_one("t1")

        eq_(listen_one.mock_calls, [call(x="adapted t1")])
        event.remove(t1, "event_one", listen_one)
        t1.dispatch.event_one("t2")

        eq_(listen_one.mock_calls, [call(x="adapted t1")])

    def test_double_event_nonwrapped(self):
        Target = self._fixture()

        listen_one = Mock()
        t1 = Target()
        event.listen(t1, "event_one", listen_one)
        event.listen(t1, "event_one", listen_one)

        t1.dispatch.event_one("t1")

        # doubles are eliminated
        eq_(listen_one.mock_calls, [call("t1")])

        # only one remove needed
        event.remove(t1, "event_one", listen_one)
        t1.dispatch.event_one("t2")

        eq_(listen_one.mock_calls, [call("t1")])

    def test_double_event_wrapped(self):
        # this is issue #3199
        Target = self._wrapped_fixture()

        listen_one = Mock()
        t1 = Target()

        event.listen(t1, "event_one", listen_one)
        event.listen(t1, "event_one", listen_one)

        t1.dispatch.event_one("t1")

        # doubles are eliminated
        eq_(listen_one.mock_calls, [call("adapted t1")])

        # only one remove needed
        event.remove(t1, "event_one", listen_one)
        t1.dispatch.event_one("t2")

        eq_(listen_one.mock_calls, [call("adapted t1")])
