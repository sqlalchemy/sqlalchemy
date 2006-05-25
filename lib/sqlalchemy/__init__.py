# __init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from types import *
from sql import *
from schema import *
from sqlalchemy.orm import *

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import default_metadata

def global_connect(*args, **kwargs):
    default_metadata.connect(*args, **kwargs)
    