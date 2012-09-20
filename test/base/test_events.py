"""Test event registration and listening."""

from test.lib.testing import eq_, assert_raises, assert_raises_message, \
    is_, is_not_
from sqlalchemy import event, exc, util
from test.lib import fixtures

class TestEvents(fixtures.TestBase):
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

class TestClsLevelListen(fixtures.TestBase):


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
            print 'handler1'

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
            print 'handler1'

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


class TestAcceptTargets(fixtures.TestBase):
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

class TestCustomTargets(fixtures.TestBase):
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

class TestListenOverride(fixtures.TestBase):
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
        result = []
        def listen_one(x):
            result.append(x)

        def listen_two(x, y):
            result.append((x, y))

        event.listen(self.Target, "event_one", listen_one, add=True)
        event.listen(self.Target, "event_one", listen_two)

        t1 = self.Target()
        t1.dispatch.event_one(5, 7)
        t1.dispatch.event_one(10, 5)

        eq_(result,
            [
                12, (5, 7), 15, (10, 5)
            ]
        )

class TestPropagate(fixtures.TestBase):
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
        result = []
        def listen_one(target, arg):
            result.append((target, arg))

        def listen_two(target, arg):
            result.append((target, arg))

        t1 = self.Target()

        event.listen(t1, "event_one", listen_one, propagate=True)
        event.listen(t1, "event_two", listen_two)

        t2 = self.Target()

        t2.dispatch._update(t1.dispatch)

        t2.dispatch.event_one(t2, 1)
        t2.dispatch.event_two(t2, 2)
        eq_(result, [(t2, 1)])
