# maxdb/sapdb.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects.maxdb.base import MaxDBDialect

class MaxDBDialect_sapdb(MaxDBDialect):
    driver = 'sapdb'

    @classmethod
    def dbapi(cls):
        from sapdb import dbapi as _dbapi
        return _dbapi

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [], opts


dialect = MaxDBDialect_sapdb