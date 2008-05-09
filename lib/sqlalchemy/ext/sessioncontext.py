from sqlalchemy.orm.scoping import ScopedSession, _ScopedExt
from sqlalchemy.util import warn_deprecated
from sqlalchemy.orm import create_session

__all__ = ['SessionContext', 'SessionContextExt']


class SessionContext(ScopedSession):
    """Provides thread-local management of Sessions.

    Usage::

      context = SessionContext(sessionmaker(autoflush=True))

    """

    def __init__(self, session_factory=None, scopefunc=None):
        warn_deprecated("SessionContext is deprecated.  Use scoped_session().")
        if session_factory is None:
            session_factory=create_session
        super(SessionContext, self).__init__(session_factory, scopefunc=scopefunc)

    def get_current(self):
        return self.registry()

    def set_current(self, session):
        self.registry.set(session)

    def del_current(self):
        self.registry.clear()

    current = property(get_current, set_current, del_current,
                       """Property used to get/set/del the session in the current scope.""")

    def _get_mapper_extension(self):
        try:
            return self._extension
        except AttributeError:
            self._extension = ext = SessionContextExt(self)
            return ext

    mapper_extension = property(_get_mapper_extension,
                                doc="""Get a mapper extension that implements `get_session` using this context.  Deprecated.""")


class SessionContextExt(_ScopedExt):
    def __init__(self, *args, **kwargs):
        warn_deprecated("SessionContextExt is deprecated.  Use ScopedSession(enhance_classes=True)")
        super(SessionContextExt, self).__init__(*args, **kwargs)

