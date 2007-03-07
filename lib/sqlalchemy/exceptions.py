# exceptions.py - exceptions for SQLAlchemy
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


class SQLAlchemyError(Exception):
    """Generic error class."""

    pass

class SQLError(SQLAlchemyError):
    """Raised when the execution of a SQL statement fails.

    Includes accessors for the underlying exception, as well as the
    SQL and bind parameters.
    """

    def __init__(self, statement, params, orig):
        SQLAlchemyError.__init__(self, "(%s) %s"% (orig.__class__.__name__, str(orig)))
        self.statement = statement
        self.params = params
        self.orig = orig

    def __str__(self):
        return SQLAlchemyError.__str__(self) + " " + repr(self.statement) + " " + repr(self.params)

class ArgumentError(SQLAlchemyError):
    """Raised for all those conditions where invalid arguments are
    sent to constructed objects.  This error generally corresponds to
    construction time state errors.
    """

    pass

class CompileError(SQLAlchemyError):
    """Raised when an error occurs during SQL compilation"""
    
    pass
    
class TimeoutError(SQLAlchemyError):
    """Raised when a connection pool times out on getting a connection."""

    pass

class ConcurrentModificationError(SQLAlchemyError):
    """Raised when a concurrent modification condition is detected."""

    pass

class CircularDependencyError(SQLAlchemyError):
    """Raised by topological sorts when a circular dependency is detected"""
    pass
    
class FlushError(SQLAlchemyError):
    """Raised when an invalid condition is detected upon a ``flush()``."""
    pass

class InvalidRequestError(SQLAlchemyError):
    """SQLAlchemy was asked to do something it can't do, return
    nonexistent data, etc.

    This error generally corresponds to runtime state errors.
    """

    pass

class NoSuchTableError(InvalidRequestError):
    """SQLAlchemy was asked to load a table's definition from the
    database, but the table doesn't exist.
    """

    pass

class AssertionError(SQLAlchemyError):
    """Corresponds to internal state being detected in an invalid state."""

    pass

class NoSuchColumnError(KeyError, SQLAlchemyError):
    """Raised by ``RowProxy`` when a nonexistent column is requested from a row."""

    pass

class DBAPIError(SQLAlchemyError):
    """Something weird happened with a particular DBAPI version."""

    def __init__(self, message, orig):
        SQLAlchemyError.__init__(self, "(%s) (%s) %s"% (message, orig.__class__.__name__, str(orig)))
        self.orig = orig
