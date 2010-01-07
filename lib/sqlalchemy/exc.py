# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Exceptions used with SQLAlchemy.

The base exception class is SQLAlchemyError.  Exceptions which are raised as a
result of DBAPI exceptions are all subclasses of
:class:`~sqlalchemy.exc.DBAPIError`.

"""


class SQLAlchemyError(Exception):
    """Generic error class."""


class ArgumentError(SQLAlchemyError):
    """Raised when an invalid or conflicting function argument is supplied.

    This error generally corresponds to construction time state errors.

    """


class CircularDependencyError(SQLAlchemyError):
    """Raised by topological sorts when a circular dependency is detected"""


class CompileError(SQLAlchemyError):
    """Raised when an error occurs during SQL compilation"""

class IdentifierError(SQLAlchemyError):
    """Raised when a schema name is beyond the max character limit"""

# Moved to orm.exc; compatability definition installed by orm import until 0.6
ConcurrentModificationError = None

class DisconnectionError(SQLAlchemyError):
    """A disconnect is detected on a raw DB-API connection.

    This error is raised and consumed internally by a connection pool.  It can
    be raised by a ``PoolListener`` so that the host pool forces a disconnect.

    """


# Moved to orm.exc; compatability definition installed by orm import until 0.6
FlushError = None

class TimeoutError(SQLAlchemyError):
    """Raised when a connection pool times out on getting a connection."""


class InvalidRequestError(SQLAlchemyError):
    """SQLAlchemy was asked to do something it can't do.

    This error generally corresponds to runtime state errors.

    """

class NoSuchColumnError(KeyError, InvalidRequestError):
    """A nonexistent column is requested from a ``RowProxy``."""

class NoReferenceError(InvalidRequestError):
    """Raised by ``ForeignKey`` to indicate a reference cannot be resolved."""
    
class NoReferencedTableError(NoReferenceError):
    """Raised by ``ForeignKey`` when the referred ``Table`` cannot be located."""

class NoReferencedColumnError(NoReferenceError):
    """Raised by ``ForeignKey`` when the referred ``Column`` cannot be located."""

class NoSuchTableError(InvalidRequestError):
    """Table does not exist or is not visible to a connection."""


class UnboundExecutionError(InvalidRequestError):
    """SQL was attempted without a database connection to execute it on."""


# Moved to orm.exc; compatability definition installed by orm import until 0.6
UnmappedColumnError = None

class DBAPIError(SQLAlchemyError):
    """Raised when the execution of a database operation fails.

    ``DBAPIError`` wraps exceptions raised by the DB-API underlying the
    database operation.  Driver-specific implementations of the standard
    DB-API exception types are wrapped by matching sub-types of SQLAlchemy's
    ``DBAPIError`` when possible.  DB-API's ``Error`` type maps to
    ``DBAPIError`` in SQLAlchemy, otherwise the names are identical.  Note
    that there is no guarantee that different DB-API implementations will
    raise the same exception type for any given error condition.

    If the error-raising operation occured in the execution of a SQL
    statement, that statement and its parameters will be available on
    the exception object in the ``statement`` and ``params`` attributes.

    The wrapped exception object is available in the ``orig`` attribute.
    Its type and properties are DB-API implementation specific.

    """

    @classmethod
    def instance(cls, statement, params, orig, connection_invalidated=False):
        # Don't ever wrap these, just return them directly as if
        # DBAPIError didn't exist.
        if isinstance(orig, (KeyboardInterrupt, SystemExit)):
            return orig

        if orig is not None:
            name, glob = orig.__class__.__name__, globals()
            if name in glob and issubclass(glob[name], DBAPIError):
                cls = glob[name]

        return cls(statement, params, orig, connection_invalidated)

    def __init__(self, statement, params, orig, connection_invalidated=False):
        try:
            text = str(orig)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, e:
            text = 'Error in str() of DB-API-generated exception: ' + str(e)
        SQLAlchemyError.__init__(
            self, '(%s) %s' % (orig.__class__.__name__, text))
        self.statement = statement
        self.params = params
        self.orig = orig
        self.connection_invalidated = connection_invalidated

    def __str__(self):
        if isinstance(self.params, (list, tuple)) and len(self.params) > 10 and isinstance(self.params[0], (list, dict, tuple)):
            return ' '.join((SQLAlchemyError.__str__(self),
                             repr(self.statement),
                             repr(self.params[:2]),
                             '... and a total of %i bound parameter sets' % len(self.params)))
        return ' '.join((SQLAlchemyError.__str__(self),
                         repr(self.statement), repr(self.params)))


# As of 0.4, SQLError is now DBAPIError.
# SQLError alias will be removed in 0.6.
SQLError = DBAPIError

class InterfaceError(DBAPIError):
    """Wraps a DB-API InterfaceError."""


class DatabaseError(DBAPIError):
    """Wraps a DB-API DatabaseError."""


class DataError(DatabaseError):
    """Wraps a DB-API DataError."""


class OperationalError(DatabaseError):
    """Wraps a DB-API OperationalError."""


class IntegrityError(DatabaseError):
    """Wraps a DB-API IntegrityError."""


class InternalError(DatabaseError):
    """Wraps a DB-API InternalError."""


class ProgrammingError(DatabaseError):
    """Wraps a DB-API ProgrammingError."""


class NotSupportedError(DatabaseError):
    """Wraps a DB-API NotSupportedError."""


# Warnings

class SADeprecationWarning(DeprecationWarning):
    """Issued once per usage of a deprecated API."""


class SAPendingDeprecationWarning(PendingDeprecationWarning):
    """Issued once per usage of a deprecated API."""


class SAWarning(RuntimeWarning):
    """Issued at runtime."""
