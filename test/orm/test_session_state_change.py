from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import state_changes
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures


class StateTestChange(state_changes._StateChangeState):
    a = 1
    b = 2
    c = 3


class StateMachineTest(fixtures.TestBase):
    def test_single_change(self):
        """test single method that declares and invokes a state change"""
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                self._state = StateTestChange.b

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m.move_to_b()
        eq_(m._state, StateTestChange.b)

    def test_single_incorrect_change(self):
        """test single method that declares a state change but changes to the
        wrong state."""
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                self._state = StateTestChange.c

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Method 'move_to_b\(\)' "
            r"caused an unexpected state change to <StateTestChange.c: 3>",
        ):
            m.move_to_b()

    def test_single_failed_to_change(self):
        """test single method that declares a state change but didn't do
        the change."""
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                pass

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Method 'move_to_b\(\)' failed to change state "
            "to <StateTestChange.b: 2> as "
            "expected",
        ):
            m.move_to_b()

    def test_change_from_sub_method_with_declaration(self):
        """test successful state change by one method calling another that
        does the change.

        """
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def _inner_move_to_b(self):
                self._state = StateTestChange.b

            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                with self._expect_state(StateTestChange.b):
                    self._inner_move_to_b()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m.move_to_b()
        eq_(m._state, StateTestChange.b)

    def test_method_and_sub_method_no_change(self):
        """test methods that declare the state should not change"""
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a,), _NO_CHANGE
            )
            def _inner_do_nothing(self):
                pass

            @state_changes._StateChange.declare_states(
                (StateTestChange.a,), _NO_CHANGE
            )
            def do_nothing(self):
                self._inner_do_nothing()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m._state = StateTestChange.a
        m.do_nothing()
        eq_(m._state, StateTestChange.a)

    def test_method_w_no_change_illegal_inner_change(self):
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.c
            )
            def _inner_move_to_c(self):
                self._state = StateTestChange.c

            @state_changes._StateChange.declare_states(
                (StateTestChange.a,), _NO_CHANGE
            )
            def do_nothing(self):
                self._inner_move_to_c()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m._state = StateTestChange.a

        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Method '_inner_move_to_c\(\)' can't be called here; "
            r"method 'do_nothing\(\)' is already in progress and this "
            r"would cause an unexpected state change to "
            "<StateTestChange.c: 3>",
        ):
            m.do_nothing()
        eq_(m._state, StateTestChange.a)

    def test_change_from_method_sub_w_no_change(self):
        """test methods that declare the state should not change"""
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a,), _NO_CHANGE
            )
            def _inner_do_nothing(self):
                pass

            @state_changes._StateChange.declare_states(
                (StateTestChange.a,), StateTestChange.b
            )
            def move_to_b(self):
                self._inner_do_nothing()
                self._state = StateTestChange.b

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m._state = StateTestChange.a
        m.move_to_b()
        eq_(m._state, StateTestChange.b)

    def test_invalid_change_from_declared_sub_method_with_declaration(self):
        """A method uses _expect_state() to call a sub-method, which must
        declare that state as its destination if no exceptions are raised.

        """
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            # method declares StateTestChange.c so can't be called under
            # expect_state(StateTestChange.b)
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.c
            )
            def _inner_move_to_c(self):
                self._state = StateTestChange.c

            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                with self._expect_state(StateTestChange.b):
                    self._inner_move_to_c()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Can't run operation '_inner_move_to_c\(\)' here; will move "
            r"to state <StateTestChange.c: 3> where we are "
            "expecting <StateTestChange.b: 2>",
        ):
            m.move_to_b()

    def test_invalid_change_from_invalid_sub_method_with_declaration(self):
        """A method uses _expect_state() to call a sub-method, which must
        declare that state as its destination if no exceptions are raised.

        Test an error is raised if the sub-method doesn't change to the
        correct state.

        """
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            # method declares StateTestChange.b, but is doing the wrong
            # change, so should fail under expect_state(StateTestChange.b)
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def _inner_move_to_c(self):
                self._state = StateTestChange.c

            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                with self._expect_state(StateTestChange.b):
                    self._inner_move_to_c()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"While method 'move_to_b\(\)' was running, method "
            r"'_inner_move_to_c\(\)' caused an unexpected state change "
            "to <StateTestChange.c: 3>",
        ):
            m.move_to_b()

    def test_invalid_prereq_state(self):
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                self._state = StateTestChange.b

            @state_changes._StateChange.declare_states(
                (StateTestChange.c,), "d"
            )
            def move_to_d(self):
                self._state = "d"

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        m.move_to_b()
        eq_(m._state, StateTestChange.b)
        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Can't run operation 'move_to_d\(\)' when "
            "Session is in state <StateTestChange.b: 2>",
        ):
            m.move_to_d()

    def test_declare_only(self):
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                state_changes._StateChangeStates.ANY, StateTestChange.b
            )
            def _inner_move_to_b(self):
                self._state = StateTestChange.b

            def move_to_b(self):
                with self._expect_state(StateTestChange.b):
                    self._move_to_b()

        m = Machine()
        eq_(m._state, _NO_CHANGE)
        with expect_raises_message(
            AssertionError,
            "Unexpected call to _expect_state outside of "
            "state-changing method",
        ):
            m.move_to_b()

    def test_sibling_calls_maintain_correct_state(self):
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                state_changes._StateChangeStates.ANY, StateTestChange.c
            )
            def move_to_c(self):
                self._state = StateTestChange.c

            @state_changes._StateChange.declare_states(
                state_changes._StateChangeStates.ANY, _NO_CHANGE
            )
            def do_nothing(self):
                pass

        m = Machine()
        m.do_nothing()
        eq_(m._state, _NO_CHANGE)
        m.move_to_c()
        eq_(m._state, StateTestChange.c)

    def test_change_from_sub_method_requires_declaration(self):
        """A method can't call another state-changing method without using
        _expect_state() to allow the state change to occur.

        """
        _NO_CHANGE = state_changes._StateChangeStates.NO_CHANGE

        class Machine(state_changes._StateChange):
            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def _inner_move_to_b(self):
                self._state = StateTestChange.b

            @state_changes._StateChange.declare_states(
                (StateTestChange.a, _NO_CHANGE), StateTestChange.b
            )
            def move_to_b(self):
                self._inner_move_to_b()

        m = Machine()

        with expect_raises_message(
            sa_exc.IllegalStateChangeError,
            r"Method '_inner_move_to_b\(\)' can't be called here; "
            r"method 'move_to_b\(\)' is already in progress and this would "
            r"cause an unexpected state change to <StateTestChange.b: 2>",
        ):
            m.move_to_b()
