# sql/future/__init__.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Future 2.0 API features.

"""
from .engine import Connection  # noqa
from .engine import create_engine  # noqa
from .engine import Engine  # noqa
from ..sql.selectable import Select  # noqa
from ..util.langhelpers import public_factory


select = public_factory(Select._create_future_select, ".future.select")
