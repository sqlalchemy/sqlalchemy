# __init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from engine import *
from types import *
from sql import *
from schema import *
from exceptions import *
import mapping as mapperlib
from mapping import *

import sqlalchemy.schema
import sqlalchemy.ext.proxy
sqlalchemy.schema.default_engine = sqlalchemy.ext.proxy.ProxyEngine()

def global_connect(*args, **kwargs):
    sqlalchemy.schema.default_engine.connect(*args, **kwargs)
    