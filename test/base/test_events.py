"""Test event registration and listening."""

from sqlalchemy.testing import eq_, assert_raises, assert_raises_message, \
    is_, is_not_
from sqlalchemy import event, exc
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.testing.mock import Mock, call


class EventsTest(fixtures.TestBase):
    """Test class- and instance-level event registration."""

    def setUp(self):
        assert 'event_one' not in event._registrars
        assert 'event_two' not in event._registrars

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

    def tearDown(self):
        event._remove_dispatcher(self.Target.__dict__['dispatch'].events)

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

    def test_append_vs_insert(self):
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
            [listen_three, listen_one, listen_two]
        )

    def test_decorator(self):
        @event.listens_for(self.Target, "event_one")
        def listen_one(x, y):
            pass

        @event.listens_for(self.Target, "event_two")
        @event.listens_for(self.Target, "event_three")
        def listen_two(x, y):
            pass

        eq_(
            list(self.Target().dispatch.event_one),
            [listen_one]
        )

        eq_(
            list(self.Target().dispatch.event_two),
            [listen_two]
        )

        eq_(
            list(self.Target().dispatch.event_three),
            [listen_two]
        )

    def test_no_instance_level_collections(self):
        @event.listens_for(self.Target, "event_one")
        def listen_one(x, y):
            pass
        t1 = self.Target()
        t2 = self.Target()
        t1.dispatch.event_one(5, 6)
        t2.dispatch.event_one(5, 6)
        is_(
            t1.dispatch.__dict__['event_one'],
            self.Target.dispatch.event_one.\
                _empty_listeners[self.Target]
        )

        @event.listens_for(t1, "event_one")
        def listen_two(x, y):
            pass
        is_not_(
            t1.dispatch.__dict__['event_one'],
            self.Target.dispatch.event_one.\
                _empty_listeners[self.Target]
        )
        is_(
            t2.dispatch.__dict__['event_one'],
            self.Target.dispatch.event_one.\
                _empty_listeners[self.Target]
        )

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
                NotImplementedError,
                r"need to call for_modify\(\)",
                meth
            )

class ClsLevelListenTest(fixtures.TestBase):


    def tearDown(self):
        event._remove_dispatcher(self.TargetOne.__dict__['dispatch'].events)

    def setUp(self):
        class TargetEventsOne(event.Events):
            def event_one(self, x, y):
                pass
        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)
        self.TargetOne = TargetOne

    def tearDown(self):
        event._remove_dispatcher(
            self.TargetOne.__dict__['dispatch'].events)

    def test_lis_subcalss_lis(self):
        @event.listens_for(self.TargetOne, "event_one")
        def handler1(x, y):
            pass

        class SubTarget(self.TargetOne):
            pass

        @event.listens_for(self.TargetOne, "event_one")
        def handler2(x, y):
            pass

        eq_(
            len(SubTarget().dispatch.event_one),
            2
        )

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

        eq_(
            len(SubTarget().dispatch.event_one),
            2
        )
        eq_(
            len(SubSubTarget().dispatch.event_one),
            2
        )

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


class AcceptTargetsTest(fixtures.TestBase):
    """Test default target acceptance."""

    def setUp(self):
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

    def tearDown(self):
        event._remove_dispatcher(self.TargetOne.__dict__['dispatch'].events)
        event._remove_dispatcher(self.TargetTwo.__dict__['dispatch'].events)

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

        eq_(
            list(self.TargetOne().dispatch.event_one),
            [listen_one]
        )

        eq_(
            list(self.TargetTwo().dispatch.event_one),
            [listen_two]
        )

        t1 = self.TargetOne()
        t2 = self.TargetTwo()

        event.listen(t1, "event_one", listen_three)
        event.listen(t2, "event_one", listen_four)

        eq_(
            list(t1.dispatch.event_one),
            [listen_one, listen_three]
        )

        eq_(
            list(t2.dispatch.event_one),
            [listen_two, listen_four]
        )

class CustomTargetsTest(fixtures.TestBase):
    """Test custom target acceptance."""

    def setUp(self):
        class TargetEvents(event.Events):
            @classmethod
            def _accept_with(cls, target):
                if target == 'one':
                    return Target
                else:
                    return None

            def event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)
        self.Target = Target

    def tearDown(self):
        event._remove_dispatcher(self.Target.__dict__['dispatch'].events)

    def test_indirect(self):
        def listen(x, y):
            pass

        event.listen("one", "event_one", listen)

        eq_(
            list(self.Target().dispatch.event_one),
            [listen]
        )

        assert_raises(
            exc.InvalidRequestError,
            event.listen,
            listen, "event_one", self.Target
        )

class SubclassGrowthTest(fixtures.TestBase):
    """test that ad-hoc subclasses are garbage collected."""

    def setUp(self):
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


class ListenOverrideTest(fixtures.TestBase):
    """Test custom listen functions which change the listener function signature."""

    def setUp(self):
        class TargetEvents(event.Events):
            @classmethod
            def _listen(cls, target, identifier, fn, add=False):
                if add:
                    def adapt(x, y):
                        fn(x + y)
                else:
                    adapt = fn

                event.Events._listen(target, identifier, adapt)

            def event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)
        self.Target = Target

    def tearDown(self):
        event._remove_dispatcher(self.Target.__dict__['dispatch'].events)

    def test_listen_override(self):
        listen_one = Mock()
        listen_two = Mock()

        event.listen(self.Target, "event_one", listen_one, add=True)
        event.listen(self.Target, "event_one", listen_two)

        t1 = self.Target()
        t1.dispatch.event_one(5, 7)
        t1.dispatch.event_one(10, 5)

        eq_(
            listen_one.mock_calls,
            [call(12), call(15)]
        )
        eq_(
            listen_two.mock_calls,
            [call(5, 7), call(10, 5)]
        )

class PropagateTest(fixtures.TestBase):
    def setUp(self):
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

        eq_(
            listen_one.mock_calls,
            [call(t2, 1)]
        )
        eq_(
            listen_two.mock_calls,
            []
        )

class JoinTest(fixtures.TestBase):
    def setUp(self):
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

    def tearDown(self):
        for cls in (self.TargetElement,
                self.TargetFactory, self.BaseTarget):
            if 'dispatch' in cls.__dict__:
                event._remove_dispatcher(cls.__dict__['dispatch'].events)

    def test_neither(self):
        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        element.run_event(3)

    def test_parent_class_only(self):
        l1 = Mock()

        event.listen(self.TargetFactory, "event_one", l1)

        element = self.TargetFactory().create()
        element.run_event(1)
        element.run_event(2)
        element.run_event(3)
        eq_(
            l1.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
        eq_(
            l2.mock_calls,
            [call(element, 2), call(element, 3)]
        )

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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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

        # c1 gets no events due to _JoinedListener
        # fixing the "parent" at construction time.
        # this can be changed to be "live" at the cost
        # of performance.
        eq_(
            l1.mock_calls, []
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
        eq_(
            l2.mock_calls,
            [call(element, 1), call(element, 2), call(element, 3)]
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
            [call(element, 1), call(element, 2), call(element, 3)]
        )
