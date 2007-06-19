# __init__.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.types import *
from sqlalchemy.sql import *
from sqlalchemy.schema import *
from sqlalchemy.orm import *

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import default_metadata

def __figure_version():
    try:
        from pkg_resources import require
        import os
        # NOTE: this only works when the package is either installed,
        # or has an .egg-info directory present (i.e. wont work with raw SVN checkout)
        info = require('sqlalchemy')[0]
        if os.path.dirname(os.path.dirname(__file__)) == info.location:
            return info.version
        else:
            return '(not installed)'
    except:
        return '(not installed)'
        
__version__ = __figure_version()
    
def global_connect(*args, **kwargs):
    default_metadata.connect(*args, **kwargs)
    