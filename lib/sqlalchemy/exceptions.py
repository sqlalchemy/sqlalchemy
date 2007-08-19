# exceptions.py - exceptions for SQLAlchemy
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


class SQLAlchemyError(Exception):
    """Generic error class."""


class ArgumentError(SQLAlchemyError):
    """Raised for all those conditions where invalid arguments are
    sent to constructed objects.  This error generally corresponds to
    construction time state errors.
    """


class CompileError(SQLAlchemyError):
    """Raised when an error occurs during SQL compilation"""
        

class TimeoutError(SQLAlchemyError):
    """Raised when a connection pool times out on getting a connection."""


class ConcurrentModificationError(SQLAlchemyError):
    """Raised when a concurrent modification condition is detected."""


class CircularDependencyError(SQLAlchemyError):
    """Raised by topological sorts when a circular dependency is detected"""
    

class FlushError(SQLAlchemyError):
    """Raised when an invalid condition is detected upon a ``flush()``."""


class InvalidRequestError(SQLAlchemyError):
    """SQLAlchemy was asked to do something it can't do, return
    nonexistent data, etc.

    This error generally corresponds to runtime state errors.
    """


class NoSuchTableError(InvalidRequestError):
    """SQLAlchemy was asked to load a table's definition from the
    database, but the table doesn't exist.
    """


class AssertionError(SQLAlchemyError):
    """Corresponds to internal state being detected in an invalid state."""


class NoSuchColumnError(KeyError, SQLAlchemyError):
    """Raised by ``RowProxy`` when a nonexistent column is requested from a row."""


class DisconnectionError(SQLAlchemyError):
    """Raised within ``Pool`` when a disconnect is detected on a raw DB-API connection."""


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

    def __new__(cls, statement, params, orig, *args, **kw):
        # Don't ever wrap these, just return them directly as if
        # DBAPIError didn't exist.
        if isinstance(orig, (KeyboardInterrupt, SystemExit)):
            return orig
        
        if orig is not None:
            name, glob = type(orig).__name__, globals()
            if name in glob and issubclass(glob[name], DBAPIError):
                cls = glob[name]
            
        return SQLAlchemyError.__new__(cls, statement, params, orig,
                                       *args, **kw)

    def __init__(self, statement, params, orig):
        SQLAlchemyError.__init__(self, "(%s) %s" %
                                 (orig.__class__.__name__, str(orig)))
        self.statement = statement
        self.params = params
        self.orig = orig

    def __str__(self):
        return ' '.join([SQLAlchemyError.__str__(self),
                         repr(self.statement), repr(self.params)])


# As of 0.4, SQLError is now DBAPIError
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

class InterfaceError(DatabaseError):
    """Wraps a DB-API InterfaceError."""

class ProgrammingError(DatabaseError):
    """Wraps a DB-API ProgrammingError."""

class NotSupportedError(DatabaseError):
    """Wraps a DB-API NotSupportedError."""
