# testing/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from . import config
from .exclusions import against as _against
from .exclusions import skip


def against(*queries):
    return _against(config._current, *queries)

crashes = skip
