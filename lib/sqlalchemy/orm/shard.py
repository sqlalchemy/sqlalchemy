# orm/shard.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util

util.warn_deprecated(
    "Horizontal sharding is now importable via "
    "'import sqlalchemy.ext.horizontal_shard"
)

from sqlalchemy.ext.horizontal_shard import *

