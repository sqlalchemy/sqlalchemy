# sql/_util_cy.pxd
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

cdef class anon_map(dict):
    cdef unsigned int _index

    cdef inline object _add_missing(self, object key)
