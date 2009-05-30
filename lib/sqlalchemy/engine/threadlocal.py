"""Provides a thread-local transactional wrapper around the root Engine class.

The ``threadlocal`` module is invoked when using the ``strategy="threadlocal"`` flag
with :func:`~sqlalchemy.engine.create_engine`.  This module is semi-private and is 
invoked automatically when the threadlocal engine strategy is used.
"""

from sqlalchemy import util
from sqlalchemy.engine import base

class TLSession(object):
    def __init__(self, engine):
        self.engine = engine
        self.__tcount = 0

    def get_connection(self, close_with_result=False):
        try:
            return self.__transaction._increment_connect()
        except AttributeError:
            return self.engine.TLConnection(self, self.engine.pool.connect(), close_with_result=close_with_result)

    def reset(self):
        try:
            self.__transaction._force_close()
            del self.__transaction
            del self.__trans
        except AttributeError:
            pass
        self.__tcount = 0

    def _conn_closed(self):
        if self.__tcount == 1:
            self.__trans._trans.rollback()
            self.reset()

    def in_transaction(self):
        return self.__tcount > 0

    def prepare(self):
        if self.__tcount == 1:
            self.__trans._trans.prepare()

    def begin_twophase(self, xid=None):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin_twophase(xid=xid)
        self.__tcount += 1
        return self.__trans

    def begin(self, **kwargs):
        if self.__tcount == 0:
            self.__transaction = self.get_connection()
            self.__trans = self.__transaction._begin(**kwargs)
        self.__tcount += 1
        return self.__trans

    def rollback(self):
        if self.__tcount > 0:
            try:
                self.__trans._trans.rollback()
            finally:
                self.reset()

    def commit(self):
        if self.__tcount == 1:
            try:
                self.__trans._trans.commit()
            finally:
                self.reset()
        elif self.__tcount > 1:
            self.__tcount -= 1
            
    def close(self):
        if self.__tcount == 1:
            self.rollback()
        elif self.__tcount > 1:
            self.__tcount -= 1
        
    def is_begun(self):
        return self.__tcount > 0


class TLConnection(base.Connection):
    def __init__(self, session, connection, **kwargs):
        base.Connection.__init__(self, session.engine, connection, **kwargs)
        self.__session = session
        self.__opencount = 1

    def _branch(self):
        return self.engine.Connection(self.engine, self.connection, _branch=True)

    def session(self):
        return self.__session
    session = property(session)

    def _increment_connect(self):
        self.__opencount += 1
        return self

    def _begin(self, **kwargs):
        return TLTransaction(
            super(TLConnection, self).begin(**kwargs), self.__session)

    def _begin_twophase(self, xid=None):
        return TLTransaction(
            super(TLConnection, self).begin_twophase(xid=xid), self.__session)

    def in_transaction(self):
        return self.session.in_transaction()

    def begin(self, **kwargs):
        return self.session.begin(**kwargs)

    def begin_twophase(self, xid=None):
        return self.session.begin_twophase(xid=xid)
    
    def begin_nested(self):
        raise NotImplementedError("SAVEPOINT transactions with the 'threadlocal' strategy")
        
    def close(self):
        if self.__opencount == 1:
            base.Connection.close(self)
            self.__session._conn_closed()
        self.__opencount -= 1

    def _force_close(self):
        self.__opencount = 0
        base.Connection.close(self)


class TLTransaction(base.Transaction):
    def __init__(self, trans, session):
        self._trans = trans
        self._session = session

    def connection(self):
        return self._trans.connection
    connection = property(connection)
    
    def is_active(self):
        return self._trans.is_active
    is_active = property(is_active)

    def rollback(self):
        self._session.rollback()

    def prepare(self):
        self._session.prepare()

    def commit(self):
        self._session.commit()

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._trans.__exit__(type, value, traceback)


class TLEngine(base.Engine):
    """An Engine that includes support for thread-local managed transactions.

    The TLEngine relies upon its Pool having "threadlocal" behavior,
    so that once a connection is checked out for the current thread,
    you get that same connection repeatedly.
    """

    def __init__(self, *args, **kwargs):
        """Construct a new TLEngine."""

        super(TLEngine, self).__init__(*args, **kwargs)
        self.context = util.threading.local()

        proxy = kwargs.get('proxy')
        if proxy:
            self.TLConnection = base._proxy_connection_cls(TLConnection, proxy)
        else:
            self.TLConnection = TLConnection

    def session(self):
        "Returns the current thread's TLSession"
        if not hasattr(self.context, 'session'):
            self.context.session = TLSession(self)
        return self.context.session

    session = property(session)

    def contextual_connect(self, **kwargs):
        """Return a TLConnection which is thread-locally scoped."""

        return self.session.get_connection(**kwargs)

    def begin_twophase(self, **kwargs):
        return self.session.begin_twophase(**kwargs)

    def begin_nested(self):
        raise NotImplementedError("SAVEPOINT transactions with the 'threadlocal' strategy")
        
    def begin(self, **kwargs):
        return self.session.begin(**kwargs)

    def prepare(self):
        self.session.prepare()
        
    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def __repr__(self):
        return 'TLEngine(%s)' % str(self.url)
