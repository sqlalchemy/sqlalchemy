# logging.py - adapt python logging module to SQLAlchemy
# Copyright (C) 2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""provides a few functions used by instances to turn on/off their logging, including support
for the usual "echo" parameter.  Control of logging for SA can be performed from the regular
python logging module.  The regular dotted module namespace is used, starting at 'sqlalchemy'.  
For class-level logging, the class name is appended, and for instance-level logging, the hex
id of the instance is appended.

The "echo" keyword parameter which is available on some SA objects corresponds to an instance-level
logger for that instance.

E.g.:

    engine.echo = True
    
is equivalent to:

    import logging
    logging.getLogger('sqlalchemy.engine.ComposedSQLEngine.%s' % hex(id(engine))).setLevel(logging.DEBUG)
    
"""

import sys

# py2.5 absolute imports will fix....
logging = __import__('logging')

default_enabled = False
def default_logging():
    global default_enabled
    if not default_enabled:
        default_enabled = True
        rootlogger = logging.getLogger('sqlalchemy')
        rootlogger.setLevel(logging.NOTSET)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
        rootlogger.addHandler(handler)

def _get_instance_name(instance):                    
    return instance.__class__.__module__ + "." + instance.__class__.__name__ + "." + hex(id(instance))
    
def instance_logger(instance):
    return logging.getLogger(_get_instance_name(instance))

def class_logger(cls):
    return logging.getLogger(cls.__module__ + "." + cls.__name__)

def is_debug_enabled(logger):
    return logger.isEnabledFor(logging.DEBUG)
def is_info_enabled(logger):
    return logger.isEnabledFor(logging.INFO)
        
class echo_property(object):
    level_map={logging.DEBUG : "debug", logging.NOTSET : False}
    def __get__(self, instance, owner):
        level = logging.getLogger(_get_instance_name(instance)).getEffectiveLevel()
        return echo_property.level_map.get(level, True)
    def __set__(self, instance, value):
        if value:
            default_logging()
            logging.getLogger(_get_instance_name(instance)).setLevel(value == 'debug' and logging.DEBUG or logging.INFO)
        else:
            logging.getLogger(_get_instance_name(instance)).setLevel(logging.NOTSET)
    
    
