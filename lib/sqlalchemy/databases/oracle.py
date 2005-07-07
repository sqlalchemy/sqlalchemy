# oracle.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


import sys, StringIO, string

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy.ansisql import *


def engine(**params):
    return OracleSQLEngine(**params)
    
class OracleSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, use_ansi = True, **params):
        self._use_ansi = use_ansi
        ansisql.ANSISQLEngine.__init__(self, **params)
        
    def compile(self, statement):
        compiler = OracleCompiler(statement, use_ansi = self._use_ansi)
        
        statement.accept_visitor(compiler)
        return compiler

    def create_connection(self):
        raise NotImplementedError()

class OracleCompiler(ansisql.ANSICompiler):
    """oracle compiler modifies the lexical structure of Select statements to work under 
    non-ANSI configured Oracle databases, if the use_ansi flag is False."""
    
    def __init__(self, parent, use_ansi = True):
        self._outertable = None
        self._use_ansi = use_ansi
        ansisql.ANSICompiler.__init__(self, parent)
        
    def visit_join(self, join):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_join(self, join)
            
        self.froms[join] = self.get_from_text(join.left) + ", " + self.get_from_text(join.right)
        self.wheres[join] = join.onclause
        
        if join.isouter:
            # if outer join, push on the right side table as the current "outertable"
            outertable = self._outertable
            self._outertable = join.right

            # now re-visit the onclause, which will be used as a where clause
            # (the first visit occured via the Join object itself right before it called visit_join())
            join.onclause.accept_visitor(self)

            self._outertable = outertable
        
    def visit_column(self, column):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_column(self, column)
            
        if column.table is self._outertable:
            self.strings[column] = "%s.%s(+)" % (column.table.name, column.name)
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)
        

