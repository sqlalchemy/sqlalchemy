# Copyright (C) 2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base import ischema_names
from ... import types as sqltypes

__all__ = ('INT4RANGE', 'INT8RANGE', 'NUMRANGE')

class INT4RANGE(sqltypes.TypeEngine):
    "Represent the Postgresql INT4RANGE type."
    
    __visit_name__ = 'INT4RANGE'

ischema_names['int4range'] = INT4RANGE

class INT8RANGE(sqltypes.TypeEngine):
    "Represent the Postgresql INT8RANGE type."
    
    __visit_name__ = 'INT8RANGE'

ischema_names['int8range'] = INT8RANGE

class NUMRANGE(sqltypes.TypeEngine):
    "Represent the Postgresql NUMRANGE type."
    
    __visit_name__ = 'NUMRANGE'

ischema_names['numrange'] = NUMRANGE

class DATERANGE(sqltypes.TypeEngine):
    "Represent the Postgresql DATERANGE type."
    
    __visit_name__ = 'DATERANGE'

ischema_names['daterange'] = DATERANGE

class TSRANGE(sqltypes.TypeEngine):
    "Represent the Postgresql TSRANGE type."
    
    __visit_name__ = 'TSRANGE'

ischema_names['tsrange'] = TSRANGE

class TSTZRANGE(sqltypes.TypeEngine):
    "Represent the Postgresql TSTZRANGE type."
    
    __visit_name__ = 'TSTZRANGE'

ischema_names['tstzrange'] = TSTZRANGE
