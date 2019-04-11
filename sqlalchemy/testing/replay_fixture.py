from collections import deque
import contextlib
import types

from . import config
from . import fixtures
from . import profiling
from .. import create_engine
from .. import MetaData
from .. import util
from ..orm import Session


class ReplayFixtureTest(fixtures.TestBase):
    @contextlib.contextmanager
    def _dummy_ctx(self, *arg, **kw):
        yield

    def test_invocation(self):

        dbapi_session = ReplayableSession()
        creator = config.db.pool._creator

        def recorder():
            return dbapi_session.recorder(creator())

        engine = create_engine(
            config.db.url, creator=recorder, use_native_hstore=False
        )
        self.metadata = MetaData(engine)
        self.engine = engine
        self.session = Session(engine)

        self.setup_engine()
        try:
            self._run_steps(ctx=self._dummy_ctx)
        finally:
            self.teardown_engine()
            engine.dispose()

        def player():
            return dbapi_session.player()

        engine = create_engine(
            config.db.url, creator=player, use_native_hstore=False
        )

        self.metadata = MetaData(engine)
        self.engine = engine
        self.session = Session(engine)

        self.setup_engine()
        try:
            self._run_steps(ctx=profiling.count_functions)
        finally:
            self.session.close()
            engine.dispose()

    def setup_engine(self):
        pass

    def teardown_engine(self):
        pass

    def _run_steps(self, ctx):
        raise NotImplementedError()


class ReplayableSession(object):
    """A simple record/playback tool.

    This is *not* a mock testing class.  It only records a session for later
    playback and makes no assertions on call consistency whatsoever.  It's
    unlikely to be suitable for anything other than DB-API recording.

    """

    Callable = object()
    NoAttribute = object()

    if util.py2k:
        Natives = set(
            [getattr(types, t) for t in dir(types) if not t.startswith("_")]
        ).difference(
            [
                getattr(types, t)
                for t in (
                    "FunctionType",
                    "BuiltinFunctionType",
                    "MethodType",
                    "BuiltinMethodType",
                    "LambdaType",
                    "UnboundMethodType",
                )
            ]
        )
    else:
        Natives = (
            set(
                [
                    getattr(types, t)
                    for t in dir(types)
                    if not t.startswith("_")
                ]
            )
            .union(
                [
                    type(t) if not isinstance(t, type) else t
                    for t in __builtins__.values()
                ]
            )
            .difference(
                [
                    getattr(types, t)
                    for t in (
                        "FunctionType",
                        "BuiltinFunctionType",
                        "MethodType",
                        "BuiltinMethodType",
                        "LambdaType",
                    )
                ]
            )
        )

    def __init__(self):
        self.buffer = deque()

    def recorder(self, base):
        return self.Recorder(self.buffer, base)

    def player(self):
        return self.Player(self.buffer)

    class Recorder(object):
        def __init__(self, buffer, subject):
            self._buffer = buffer
            self._subject = subject

        def __call__(self, *args, **kw):
            subject, buffer = [
                object.__getattribute__(self, x)
                for x in ("_subject", "_buffer")
            ]

            result = subject(*args, **kw)
            if type(result) not in ReplayableSession.Natives:
                buffer.append(ReplayableSession.Callable)
                return type(self)(buffer, result)
            else:
                buffer.append(result)
                return result

        @property
        def _sqla_unwrap(self):
            return self._subject

        def __getattribute__(self, key):
            try:
                return object.__getattribute__(self, key)
            except AttributeError:
                pass

            subject, buffer = [
                object.__getattribute__(self, x)
                for x in ("_subject", "_buffer")
            ]
            try:
                result = type(subject).__getattribute__(subject, key)
            except AttributeError:
                buffer.append(ReplayableSession.NoAttribute)
                raise
            else:
                if type(result) not in ReplayableSession.Natives:
                    buffer.append(ReplayableSession.Callable)
                    return type(self)(buffer, result)
                else:
                    buffer.append(result)
                    return result

    class Player(object):
        def __init__(self, buffer):
            self._buffer = buffer

        def __call__(self, *args, **kw):
            buffer = object.__getattribute__(self, "_buffer")
            result = buffer.popleft()
            if result is ReplayableSession.Callable:
                return self
            else:
                return result

        @property
        def _sqla_unwrap(self):
            return None

        def __getattribute__(self, key):
            try:
                return object.__getattribute__(self, key)
            except AttributeError:
                pass
            buffer = object.__getattribute__(self, "_buffer")
            result = buffer.popleft()
            if result is ReplayableSession.Callable:
                return self
            elif result is ReplayableSession.NoAttribute:
                raise AttributeError(key)
            else:
                return result
