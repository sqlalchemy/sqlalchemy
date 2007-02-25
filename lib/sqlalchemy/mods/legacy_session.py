"""A plugin that emulates 0.1 Session behavior."""

import sqlalchemy.orm.objectstore as objectstore
import sqlalchemy.orm.unitofwork as unitofwork
import sqlalchemy.util as util
import sqlalchemy

import sqlalchemy.mods.threadlocal

class LegacySession(objectstore.Session):
    def __init__(self, nest_on=None, hash_key=None, **kwargs):
        super(LegacySession, self).__init__(**kwargs)
        self.parent_uow = None
        self.begin_count = 0
        self.nest_on = util.to_list(nest_on)
        self.__pushed_count = 0

    def was_pushed(self):
        if self.nest_on is None:
            return
        self.__pushed_count += 1
        if self.__pushed_count == 1:
            for n in self.nest_on:
                n.push_session()

    def was_popped(self):
        if self.nest_on is None or self.__pushed_count == 0:
            return
        self.__pushed_count -= 1
        if self.__pushed_count == 0:
            for n in self.nest_on:
                n.pop_session()

    class SessionTrans(object):
        """Returned by ``Session.begin()``, denotes a
        transactionalized UnitOfWork instance.  Call ``commit()`
        on this to commit the transaction.
        """

        def __init__(self, parent, uow, isactive):
            self.__parent = parent
            self.__isactive = isactive
            self.__uow = uow

        isactive = property(lambda s:s.__isactive, doc="True if this SessionTrans is the 'active' transaction marker, else its a no-op.")
        parent = property(lambda s:s.__parent, doc="The parent Session of this SessionTrans object.")
        uow = property(lambda s:s.__uow, doc="The parent UnitOfWork corresponding to this transaction.")

        def begin(self):
            """Call ``begin()`` on the underlying ``Session`` object,
            returning a new no-op ``SessionTrans`` object.
            """

            if self.parent.uow is not self.uow:
                raise InvalidRequestError("This SessionTrans is no longer valid")
            return self.parent.begin()

        def commit(self):
            """Commit the transaction noted by this ``SessionTrans`` object."""

            self.__parent._trans_commit(self)
            self.__isactive = False

        def rollback(self):
            """Roll back the current UnitOfWork transaction, in the
            case that ``begin()`` has been called.

            The changes logged since the begin() call are discarded.
            """

            self.__parent._trans_rollback(self)
            self.__isactive = False

    def begin(self):
        """Begin a new UnitOfWork transaction and return a
        transaction-holding object.

        ``commit()`` or ``rollback()`` should be called on the returned object.

        ``commit()`` on the ``Session`` will do nothing while a
        transaction is pending, and further calls to ``begin()`` will
        return no-op transactional objects.
        """

        if self.parent_uow is not None:
            return LegacySession.SessionTrans(self, self.uow, False)
        self.parent_uow = self.uow
        self.uow = unitofwork.UnitOfWork(identity_map = self.uow.identity_map)
        return LegacySession.SessionTrans(self, self.uow, True)

    def commit(self, *objects):
        """Commit the current UnitOfWork transaction.

        Called with no arguments, this is only used for *implicit*
        transactions when there was no ``begin()``.

        If individual objects are submitted, then only those objects
        are committed, and the begin/commit cycle is not affected.
        """

        # if an object list is given, commit just those but dont
        # change begin/commit status
        if len(objects):
            self._commit_uow(*objects)
            self.uow.flush(self, *objects)
            return
        if self.parent_uow is None:
            self._commit_uow()

    def _trans_commit(self, trans):
        if trans.uow is self.uow and trans.isactive:
            try:
                self._commit_uow()
            finally:
                self.uow = self.parent_uow
                self.parent_uow = None

    def _trans_rollback(self, trans):
        if trans.uow is self.uow:
            self.uow = self.parent_uow
            self.parent_uow = None

    def _commit_uow(self, *obj):
        self.was_pushed()
        try:
            self.uow.flush(self, *obj)
        finally:
            self.was_popped()

def begin():
    """Deprecated. Use ``s = Session(new_imap=False)``."""

    return objectstore.get_session().begin()

def commit(*obj):
    """Deprecated. Use ``flush(*obj)``."""

    objectstore.get_session().flush(*obj)

def uow():
    return objectstore.get_session()

def push_session(sess):
    old = get_session()
    if getattr(sess, '_previous', None) is not None:
        raise InvalidRequestError("Given Session is already pushed onto some thread's stack")
    sess._previous = old
    session_registry.set(sess)
    sess.was_pushed()

def pop_session():
    sess = get_session()
    old = sess._previous
    sess._previous = None
    session_registry.set(old)
    sess.was_popped()
    return old

def using_session(sess, func):
    push_session(sess)
    try:
        return func()
    finally:
        pop_session()

def install_plugin():
    objectstore.Session = LegacySession
    objectstore.session_registry = util.ScopedRegistry(objectstore.Session)
    objectstore.begin = begin
    objectstore.commit = commit
    objectstore.uow = uow
    objectstore.push_session = push_session
    objectstore.pop_session = pop_session
    objectstore.using_session = using_session

install_plugin()
