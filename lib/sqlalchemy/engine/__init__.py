# engine/__init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sqlalchemy.databases

from base import *
import strategies
import re

def engine_descriptors():
    """provides a listing of all the database implementations supported.  this data
    is provided as a list of dictionaries, where each dictionary contains the following
    key/value pairs:
    
    name :       the name of the engine, suitable for use in the create_engine function

    description: a plain description of the engine.

    arguments :  a dictionary describing the name and description of each parameter
                 used to connect to this engine's underlying DBAPI.
    
    This function is meant for usage in automated configuration tools that wish to 
    query the user for database and connection information.
    """
    result = []
    #for module in sqlalchemy.databases.__all__:
    for module in ['sqlite', 'postgres', 'mysql']:
        module = getattr(__import__('sqlalchemy.databases.%s' % module).databases, module)
        result.append(module.descriptor())
    return result
    
default_strategy = 'plain'
def create_engine(*args, **kwargs):
    """creates a new Engine instance.  Using the given strategy name,
    locates that strategy and invokes its create() method to produce the Engine.
    The strategies themselves are instances of EngineStrategy, and the built in 
    ones are present in the sqlalchemy.engine.strategies module.  Current implementations
    include "plain" and "threadlocal".  The default used by this function is "threadlocal".
    
    "plain" provides support for a Connection object which can be used to execute SQL queries 
    with a specific underlying DBAPI connection.
    
    "threadlocal" is similar to "plain" except that it adds support for a thread-local connection and
    transaction context, which allows a group of engine operations to participate using the same
    connection and transaction without the need for explicit passing of a Connection object.
    
    The standard method of specifying the engine is via URL as the first positional
    argument, to indicate the appropriate database dialect and connection arguments, with additional
    keyword arguments sent as options to the dialect and resulting Engine.
    
    The URL is in the form <dialect>://opt1=val1&opt2=val2.  
    Where <dialect> is a name such as "mysql", "oracle", "postgres", and the options indicate
    username, password, database, etc.  Supported keynames include "username", "user", "password",
    "pw", "db", "database", "host", "filename".

    **kwargs represents options to be sent to the Engine itself as well as the components of the Engine,
    including the Dialect, the ConnectionProvider, and the Pool.  A list of common options is as follows:

    pool=None : an instance of sqlalchemy.pool.DBProxy or sqlalchemy.pool.Pool to be used as the
    underlying source for connections (DBProxy/Pool is described in the previous section). If None,
    a default DBProxy will be created using the engine's own database module with the given
    arguments.

    echo=False : if True, the Engine will log all statements as well as a repr() of their 
    parameter lists to the engines logger, which defaults to sys.stdout.  A Engine instances' 
    "echo" data member can be modified at any time to turn logging on and off.  If set to the string 
    'debug', result rows will be printed to the standard output as well.

    logger=None : a file-like object where logging output can be sent, if echo is set to True.  
    This defaults to sys.stdout.

    encoding='utf-8' : the encoding to be used when encoding/decoding Unicode strings
    
    convert_unicode=False : True if unicode conversion should be applied to all str types
    
    module=None : used by Oracle and Postgres, this is a reference to a DBAPI2 module to be used 
    instead of the engine's default module.  For Postgres, the default is psycopg2, or psycopg1 if 
    2 cannot be found.  For Oracle, its cx_Oracle.  For mysql, MySQLdb.

    use_ansi=True : used only by Oracle;  when False, the Oracle driver attempts to support a 
    particular "quirk" of some Oracle databases, that the LEFT OUTER JOIN SQL syntax is not 
    supported, and the "Oracle join" syntax of using <column1>(+)=<column2> must be used 
    in order to achieve a LEFT OUTER JOIN.  Its advised that the Oracle database be configured to 
    have full ANSI support instead of using this feature.

    """
    strategy = kwargs.pop('strategy', default_strategy)
    strategy = strategies.strategies[strategy]
    return strategy.create(*args, **kwargs)
