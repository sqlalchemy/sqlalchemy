from sqlalchemy import schema, exceptions, util, sql, types
import StringIO, sys, re
import base, default

"""provides a thread-local transactional wrapper around the basic ComposedSQLEngine.  multiple calls to engine.connect()
will return the same connection for the same thread. also provides begin/commit methods on the engine itself
which correspond to a thread-local transaction."""

class TLTransaction(base.Transaction):
    def rollback(self):
        try:
            base.Transaction.rollback(self)
        finally:
            try:
                del self.connection.engine.context.transaction
            except AttributeError:
                pass
    def commit(self):
        try:
            base.Transaction.commit(self)
            stack = self.connection.engine.context.transaction
            stack.pop()
            if len(stack) == 0:
                del self.connection.engine.context.transaction
        except:
            try:
                del self.connection.engine.context.transaction
            except AttributeError:
                pass
            raise
            
class TLConnection(base.Connection):
    def _create_transaction(self, parent):
        return TLTransaction(self, parent)
    def begin(self):
        t = base.Connection.begin(self)
        if not hasattr(self.engine.context, 'transaction'):
            self.engine.context.transaction = []
        self.engine.context.transaction.append(t)
        return t
        
class TLEngine(base.ComposedSQLEngine):
    """a ComposedSQLEngine that includes support for thread-local managed transactions.  This engine
    is better suited to be used with threadlocal Pool object."""
    def __init__(self, *args, **kwargs):
        """the TLEngine relies upon the ConnectionProvider having "threadlocal" behavior,
        so that once a connection is checked out for the current thread, you get that same connection
        repeatedly."""
        base.ComposedSQLEngine.__init__(self, *args, **kwargs)
        self.context = util.ThreadLocal()
    def raw_connection(self):
        """returns a DBAPI connection."""
        return self.connection_provider.get_connection()
    def connect(self, **kwargs):
        """returns a Connection that is not thread-locally scoped.  this is the equilvalent to calling
        "connect()" on a ComposedSQLEngine."""
        return base.Connection(self, self.connection_provider.unique_connection())
    def contextual_connect(self, **kwargs):
        """returns a TLConnection which is thread-locally scoped."""
        return TLConnection(self, **kwargs)
    def begin(self):
        return self.connect().begin()
    def commit(self):
        if hasattr(self.context, 'transaction'):
            self.context.transaction[-1].commit()
    def rollback(self):
        if hasattr(self.context, 'transaction'):
            self.context.transaction[-1].rollback()
    def transaction(self, func, *args, **kwargs):
           """executes the given function within a transaction boundary.  this is a shortcut for
           explicitly calling begin() and commit() and optionally rollback() when execptions are raised.
           The given *args and **kwargs will be passed to the function as well, which could be handy
           in constructing decorators."""
           trans = self.begin()
           try:
               func(*args, **kwargs)
           except:
               trans.rollback()
               raise
           trans.commit()

class TLocalConnectionProvider(default.PoolConnectionProvider):
    def unique_connection(self):
        return self._pool.unique_connection()
