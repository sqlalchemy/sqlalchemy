# logging.py - adapt python logging module to SQLAlchemy
# Copyright (C) 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Logging control and utilities.

Provides a few functions used by instances to turn on/off their logging,
including support for the usual "echo" parameter.

Control of logging for SA can be performed from the regular python logging
module.  The regular dotted module namespace is used, starting at
'sqlalchemy'.  For class-level logging, the class name is appended, and for
instance-level logging, the hex id of the instance is appended.

The "echo" keyword parameter which is available on some SA objects corresponds
to an instance-level logger for that instance.

E.g.::

    engine.echo = True

is equivalent to::

    import logging
    logger = logging.getLogger('sqlalchemy.engine.Engine.%s' % hex(id(engine)))
    logger.setLevel(logging.DEBUG)
"""

import sys, warnings
import sqlalchemy.exceptions as sa_exc

# py2.5 absolute imports will fix....
logging = __import__('logging')

# moved to sqlalchemy.exceptions.  this alias will be removed in 0.5.
SADeprecationWarning = sa_exc.SADeprecationWarning

rootlogger = logging.getLogger('sqlalchemy')
if rootlogger.level == logging.NOTSET:
    rootlogger.setLevel(logging.WARN)
warnings.filterwarnings("once", category=sa_exc.SADeprecationWarning)

default_enabled = False
def default_logging(name):
    global default_enabled
    if logging.getLogger(name).getEffectiveLevel() < logging.WARN:
        default_enabled=True
    if not default_enabled:
        default_enabled = True
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s'))
        rootlogger.addHandler(handler)

def _get_instance_name(instance):
    # since getLogger() does not have any way of removing logger objects from
    # memory, instance logging displays the instance id as a modulus of 16 to
    # prevent endless memory growth also speeds performance as logger
    # initialization is apparently slow
    return "%s.%s.0x..%s" % (instance.__class__.__module__,
                             instance.__class__.__name__,
                             hex(id(instance))[-2:])
    return (instance.__class__.__module__ + "." + instance.__class__.__name__ +
            ".0x.." + hex(id(instance))[-2:])

def class_logger(cls):
    return logging.getLogger(cls.__module__ + "." + cls.__name__)

def is_debug_enabled(logger):
    return logger.isEnabledFor(logging.DEBUG)

def is_info_enabled(logger):
    return logger.isEnabledFor(logging.INFO)

def instance_logger(instance, echoflag=None):
    if echoflag is not None:
        l = logging.getLogger(_get_instance_name(instance))
        if echoflag == 'debug':
            default_logging(_get_instance_name(instance))
            l.setLevel(logging.DEBUG)
        elif echoflag is True:
            default_logging(_get_instance_name(instance))
            l.setLevel(logging.INFO)
        elif echoflag is False:
            l.setLevel(logging.NOTSET)
    else:
        l = logging.getLogger(_get_instance_name(instance))
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
