"""Test event registration and listening."""

from sqlalchemy.test.testing import TestBase, eq_, assert_raises
from sqlalchemy import event, exc, util

class TestEvents(TestBase):
    """Test class- and instance-level event registration."""
    
    def setUp(self):
        global Target
        
        assert 'on_event_one' not in event._registrars
        assert 'on_event_two' not in event._registrars
        
        class TargetEvents(event.Events):
            def on_event_one(self, x, y):
                pass
            
            def on_event_two(self, x):
                pass
                
        class Target(object):
            dispatch = event.dispatcher(TargetEvents)
    
    def tearDown(self):
        event._remove_dispatcher(Target.__dict__['dispatch'].events)
    
    def test_register_class(self):
        def listen(x, y):
            pass
        
        event.listen(listen, "on_event_one", Target)
        
        eq_(len(Target().dispatch.on_event_one), 1)
        eq_(len(Target().dispatch.on_event_two), 0)

    def test_register_instance(self):
        def listen(x, y):
            pass
        
        t1 = Target()
        event.listen(listen, "on_event_one", t1)

        eq_(len(Target().dispatch.on_event_one), 0)
        eq_(len(t1.dispatch.on_event_one), 1)
        eq_(len(Target().dispatch.on_event_two), 0)
        eq_(len(t1.dispatch.on_event_two), 0)
    
    def test_register_class_instance(self):
        def listen_one(x, y):
            pass

        def listen_two(x, y):
            pass

        event.listen(listen_one, "on_event_one", Target)
        
        t1 = Target()
        event.listen(listen_two, "on_event_one", t1)

        eq_(len(Target().dispatch.on_event_one), 1)
        eq_(len(t1.dispatch.on_event_one), 2)
        eq_(len(Target().dispatch.on_event_two), 0)
        eq_(len(t1.dispatch.on_event_two), 0)
        
        def listen_three(x, y):
            pass
        
        event.listen(listen_three, "on_event_one", Target)
        eq_(len(Target().dispatch.on_event_one), 2)
        eq_(len(t1.dispatch.on_event_one), 3)
        
class TestAcceptTargets(TestBase):
    """Test default target acceptance."""
    
    def setUp(self):
        global TargetOne, TargetTwo
        
        class TargetEventsOne(event.Events):
            def on_event_one(self, x, y):
                pass

        class TargetEventsTwo(event.Events):
            def on_event_one(self, x, y):
                pass
                
        class TargetOne(object):
            dispatch = event.dispatcher(TargetEventsOne)

        class TargetTwo(object):
            dispatch = event.dispatcher(TargetEventsTwo)
    
    def tearDown(self):
        event._remove_dispatcher(TargetOne.__dict__['dispatch'].events)
        event._remove_dispatcher(TargetTwo.__dict__['dispatch'].events)
        
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
            
        event.listen(listen_one, "on_event_one", TargetOne)
        event.listen(listen_two, "on_event_one", TargetTwo)
        
        eq_(
            list(TargetOne().dispatch.on_event_one),
            [listen_one]
        )

        eq_(
            list(TargetTwo().dispatch.on_event_one),
            [listen_two]
        )

        t1 = TargetOne()
        t2 = TargetTwo()

        event.listen(listen_three, "on_event_one", t1)
        event.listen(listen_four, "on_event_one", t2)
        
        eq_(
            list(t1.dispatch.on_event_one),
            [listen_one, listen_three]
        )

        eq_(
            list(t2.dispatch.on_event_one),
            [listen_two, listen_four]
        )
        
class TestCustomTargets(TestBase):
    """Test custom target acceptance."""

    def setUp(self):
        global Target
        
        class TargetEvents(event.Events):
            @classmethod
            def accept_with(cls, target):
                if target == 'one':
                    return Target
                else:
                    return None
                    
            def on_event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

    def tearDown(self):
        event._remove_dispatcher(Target.__dict__['dispatch'].events)
    
    def test_indirect(self):
        def listen(x, y):
            pass
        
        event.listen(listen, "on_event_one", "one")

        eq_(
            list(Target().dispatch.on_event_one),
            [listen]
        )
        
        assert_raises(
            exc.InvalidRequestError, 
            event.listen,
            listen, "on_event_one", Target
        )
        
class TestListenOverride(TestBase):
    """Test custom listen functions which change the listener function signature."""
    
    def setUp(self):
        global Target
        
        class TargetEvents(event.Events):
            @classmethod
            def listen(cls, fn, identifier, target, add=False):
                if add:
                    def adapt(x, y):
                        fn(x + y)
                else:
                    adapt = fn
                    
                event.Events.listen(adapt, identifier, target)
                    
            def on_event_one(self, x, y):
                pass

        class Target(object):
            dispatch = event.dispatcher(TargetEvents)

    def tearDown(self):
        event._remove_dispatcher(Target.__dict__['dispatch'].events)
    
    def test_listen_override(self):
        result = []
        def listen_one(x):
            result.append(x)
            
        def listen_two(x, y):
            result.append((x, y))
        
        event.listen(listen_one, "on_event_one", Target, add=True)
        event.listen(listen_two, "on_event_one", Target)

        t1 = Target()
        t1.dispatch.on_event_one(5, 7)
        t1.dispatch.on_event_one(10, 5)
        
        eq_(result,
            [
                12, (5, 7), 15, (10, 5)
            ]
        )
        
class TestPropagate(TestBase):
    def setUp(self):
        global Target
        
        class TargetEvents(event.Events):
            def on_event_one(self, arg):
                pass
            
            def on_event_two(self, arg):
                pass
                
        class Target(object):
            dispatch = event.dispatcher(TargetEvents)
            
    
    def test_propagate(self):
        result = []
        def listen_one(target, arg):
            result.append((target, arg))

        def listen_two(target, arg):
            result.append((target, arg))
        
        t1 = Target()
        
        event.listen(listen_one, "on_event_one", t1, propagate=True)
        event.listen(listen_two, "on_event_two", t1)

        t2 = Target()
        
        t2.dispatch.update(t1.dispatch)
        
        t2.dispatch.on_event_one(t2, 1)
        t2.dispatch.on_event_two(t2, 2)
        eq_(result, [(t2, 1)])
        
        
        
            
        
        
        
        
        
        
        
    
    
        