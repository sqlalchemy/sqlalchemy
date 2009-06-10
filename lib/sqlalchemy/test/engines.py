import sys, types, weakref
from collections import deque
import config
from sqlalchemy.util import function_named, callable

class ConnectionKiller(object):
    def __init__(self):
        self.proxy_refs = weakref.WeakKeyDictionary()

    def checkout(self, dbapi_con, con_record, con_proxy):
        self.proxy_refs[con_proxy] = True

    def _apply_all(self, methods):
        for rec in self.proxy_refs:
            if rec is not None and rec.is_valid:
                try:
                    for name in methods:
                        if callable(name):
                            name(rec)
                        else:
                            getattr(rec, name)()
                except (SystemExit, KeyboardInterrupt):
                    raise
                except Exception, e:
                    # fixme
                    sys.stderr.write("\n" + str(e) + "\n")

    def rollback_all(self):
        self._apply_all(('rollback',))

    def close_all(self):
        self._apply_all(('rollback', 'close'))

    def assert_all_closed(self):
        for rec in self.proxy_refs:
            if rec.is_valid:
                assert False

testing_reaper = ConnectionKiller()

def assert_conns_closed(fn):
    def decorated(*args, **kw):
        try:
            fn(*args, **kw)
        finally:
            testing_reaper.assert_all_closed()
    return function_named(decorated, fn.__name__)

def rollback_open_connections(fn):
    """Decorator that rolls back all open connections after fn execution."""

    def decorated(*args, **kw):
        try:
            fn(*args, **kw)
        finally:
            testing_reaper.rollback_all()
    return function_named(decorated, fn.__name__)

def close_open_connections(fn):
    """Decorator that closes all connections after fn execution."""

    def decorated(*args, **kw):
        try:
            fn(*args, **kw)
        finally:
            testing_reaper.close_all()
    return function_named(decorated, fn.__name__)

def all_dialects():
    import sqlalchemy.databases as d
    for name in d.__all__:
        mod = getattr(__import__('sqlalchemy.databases.%s' % name).databases, name)
        yield mod.dialect()
        
class ReconnectFixture(object):
    def __init__(self, dbapi):
        self.dbapi = dbapi
        self.connections = []

    def __getattr__(self, key):
        return getattr(self.dbapi, key)

    def connect(self, *args, **kwargs):
        conn = self.dbapi.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def shutdown(self):
        for c in list(self.connections):
            c.close()
        self.connections = []

def reconnecting_engine(url=None, options=None):
    url = url or config.db_url
    dbapi = config.db.dialect.dbapi
    if not options:
        options = {}
    options['module'] = ReconnectFixture(dbapi)
    engine = testing_engine(url, options)
    engine.test_shutdown = engine.dialect.dbapi.shutdown
    return engine

def testing_engine(url=None, options=None):
    """Produce an engine configured by --options with optional overrides."""

    from sqlalchemy import create_engine
    from sqlalchemy.test.assertsql import asserter

    url = url or config.db_url
    options = options or config.db_opts

    options.setdefault('proxy', asserter)
    
    listeners = options.setdefault('listeners', [])
    listeners.append(testing_reaper)

    engine = create_engine(url, **options)

    return engine

def utf8_engine(url=None, options=None):
    """Hook for dialects or drivers that don't handle utf8 by default."""

    from sqlalchemy.engine import url as engine_url

    if config.db.name == 'mysql':
        dbapi_ver = config.db.dialect.dbapi.version_info
        if (dbapi_ver < (1, 2, 1) or
            dbapi_ver in ((1, 2, 1, 'gamma', 1), (1, 2, 1, 'gamma', 2),
                          (1, 2, 1, 'gamma', 3), (1, 2, 1, 'gamma', 5))):
            raise RuntimeError('Character set support unavailable with this '
                               'driver version: %s' % repr(dbapi_ver))
        else:
            url = url or config.db_url
            url = engine_url.make_url(url)
            url.query['charset'] = 'utf8'
            url.query['use_unicode'] = '0'
            url = str(url)

    return testing_engine(url, options)

def mock_engine(db=None):
    """Provides a mocking engine based on the current testing.db."""
    
    from sqlalchemy import create_engine
    
    dbi = db or config.db
    buffer = []
    def executor(sql, *a, **kw):
        buffer.append(sql)
    engine = create_engine(dbi.name + '://',
                           strategy='mock', executor=executor)
    assert not hasattr(engine, 'mock')
    engine.mock = buffer
    return engine

class ReplayableSession(object):
    """A simple record/playback tool.

    This is *not* a mock testing class.  It only records a session for later
    playback and makes no assertions on call consistency whatsoever.  It's
    unlikely to be suitable for anything other than DB-API recording.

    """

    Callable = object()
    NoAttribute = object()
    Natives = set([getattr(types, t)
                   for t in dir(types) if not t.startswith('_')]). \
                   difference([getattr(types, t)
                               for t in ('FunctionType', 'BuiltinFunctionType',
                                         'MethodType', 'BuiltinMethodType',
                                         'LambdaType', 'UnboundMethodType',)])
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
            subject, buffer = [object.__getattribute__(self, x)
                               for x in ('_subject', '_buffer')]

            result = subject(*args, **kw)
            if type(result) not in ReplayableSession.Natives:
                buffer.append(ReplayableSession.Callable)
                return type(self)(buffer, result)
            else:
                buffer.append(result)
                return result

        def __getattribute__(self, key):
            try:
                return object.__getattribute__(self, key)
            except AttributeError:
                pass

            subject, buffer = [object.__getattribute__(self, x)
                               for x in ('_subject', '_buffer')]
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
            buffer = object.__getattribute__(self, '_buffer')
            result = buffer.popleft()
            if result is ReplayableSession.Callable:
                return self
            else:
                return result

        def __getattribute__(self, key):
            try:
                return object.__getattribute__(self, key)
            except AttributeError:
                pass
            buffer = object.__getattribute__(self, '_buffer')
            result = buffer.popleft()
            if result is ReplayableSession.Callable:
                return self
            elif result is ReplayableSession.NoAttribute:
                raise AttributeError(key)
            else:
                return result
