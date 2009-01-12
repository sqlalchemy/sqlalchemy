# log.py - adapt python logging module to SQLAlchemy
# Copyright (C) 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Logging control and utilities.

Control of logging for SA can be performed from the regular python logging
module.  The regular dotted module namespace is used, starting at
'sqlalchemy'.  For class-level logging, the class name is appended.

The "echo" keyword parameter which is available on SQLA ``Engine``
and ``Pool`` objects corresponds to a logger specific to that 
instance only.

E.g.::

    engine.echo = True

is equivalent to::

    import logging
    logger = logging.getLogger('sqlalchemy.engine.Engine.%s' % hex(id(engine)))
    logger.setLevel(logging.DEBUG)
    
"""

import logging
import sys


rootlogger = logging.getLogger('sqlalchemy')
if rootlogger.level == logging.NOTSET:
    rootlogger.setLevel(logging.WARN)

default_enabled = False
def default_logging(name):
    global default_enabled
    if logging.getLogger(name).getEffectiveLevel() < logging.WARN:
        default_enabled = True
    if not default_enabled:
        default_enabled = True
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s'))
        rootlogger.addHandler(handler)

def class_logger(cls, enable=False):
    logger = logging.getLogger(cls.__module__ + "." + cls.__name__)
    if enable == 'debug':
        logger.setLevel(logging.DEBUG)
    elif enable == 'info':
        logger.setLevel(logging.INFO)
    cls._should_log_debug = logger.isEnabledFor(logging.DEBUG)
    cls._should_log_info = logger.isEnabledFor(logging.INFO)
    cls.logger = logger

def instance_logger(instance, echoflag=None):
    """create a logger for an instance.
    
    Warning: this is an expensive call which also results in a permanent
    increase in memory overhead for each call.  Use only for 
    low-volume, long-time-spanning objects.
    
    """
    
    # limit the number of loggers by chopping off the hex(id).
    # many novice users unfortunately create an unlimited number 
    # of Engines in their applications which would otherwise
    # cause the app to run out of memory.
    name = "%s.%s.0x...%s" % (instance.__class__.__module__,
                             instance.__class__.__name__,
                             hex(id(instance))[-4:])
    
    if echoflag is not None:
        l = logging.getLogger(name)
        if echoflag == 'debug':
            default_logging(name)
            l.setLevel(logging.DEBUG)
        elif echoflag is True:
            default_logging(name)
            l.setLevel(logging.INFO)
        elif echoflag is False:
            l.setLevel(logging.NOTSET)
    else:
        l = logging.getLogger(name)
    instance._should_log_debug = l.isEnabledFor(logging.DEBUG)
    instance._should_log_info = l.isEnabledFor(logging.INFO)
    return l

class echo_property(object):
    __doc__ = """\
    When ``True``, enable log output for this element.

    This has the effect of setting the Python logging level for the namespace
    of this element's class and object reference.  A value of boolean ``True``
    indicates that the loglevel ``logging.INFO`` will be set for the logger,
    whereas the string value ``debug`` will set the loglevel to
    ``logging.DEBUG``.
    """

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            return instance._should_log_debug and 'debug' or (instance._should_log_info and True or False)

    def __set__(self, instance, value):
        instance_logger(instance, echoflag=value)
