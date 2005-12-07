# ansisql.py
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

"""defines ANSI SQL operations."""

import sqlalchemy.schema as schema

from sqlalchemy.schema import *
import sqlalchemy.sql as sql
import sqlalchemy.engine
from sqlalchemy.sql import *
from sqlalchemy.util import *
import string, re

def engine(**params):
    return ANSISQLEngine(**params)

class ANSISQLEngine(sqlalchemy.engine.SQLEngine):

    def schemagenerator(self, proxy, **params):
        return ANSISchemaGenerator(proxy, **params)
    
    def schemadropper(self, proxy, **params):
        return ANSISchemaDropper(proxy, **params)

    def compiler(self, statement, bindparams, **kwargs):
        return ANSICompiler(self, statement, bindparams, **kwargs)
    
    def connect_args(self):
        return ([],{})

    def dbapi(self):
        return None

class ANSICompiler(sql.Compiled):
    def __init__(self, engine, statement, bindparams, typemap=None, paramstyle=None,**kwargs):
        sql.Compiled.__init__(self, engine, statement, bindparams)
        self.binds = {}
        self.froms = {}
        self.wheres = {}
        self.strings = {}
        self.select_stack = []
        self.typemap = typemap or {}
        self.isinsert = False
        
        if paramstyle is None:
            db = self.engine.dbapi()
            if db is not None:
                paramstyle = db.paramstyle
            else:
                paramstyle = 'named'

        if paramstyle == 'named':
            self.bindtemplate = ':%s'
            self.positional=False
        elif paramstyle =='pyformat':
            self.bindtemplate = "%%(%s)s"
            self.positional=False
        else:
            # for positional, use pyformat until the end
            self.bindtemplate = "%%(%s)s"
            self.positional=True
        self.paramstyle=paramstyle
        
    def after_compile(self):
        if self.positional:
            self.positiontup = []
            match = r'%\(([\w_]+)\)s'
            params = re.finditer(match, self.strings[self.statement])
            for p in params:
                self.positiontup.append(p.group(1))
            if self.paramstyle=='qmark':
                self.strings[self.statement] = re.sub(match, '?', self.strings[self.statement])
            elif self.paramstyle=='format':
                self.strings[self.statement] = re.sub(match, '%s', self.strings[self.statement])
            elif self.paramstyle=='numeric':
                i = 0
                def getnum(x):
                    i += 1
                    return i
                self.strings[self.statement] = re.sub(match, getnum(s), self.strings[self.statement])

    def get_from_text(self, obj):
        return self.froms[obj]

    def get_str(self, obj):
        return self.strings[obj]

    def get_whereclause(self, obj):
        return self.wheres.get(obj, None)

    def get_params(self, **params):
        """returns the bind params for this compiled object, with values overridden by 
        those given in the **params dictionary"""
        d = {}
        if self.bindparams is not None:
            bindparams = self.bindparams.copy()
        else:
            bindparams = {}
        bindparams.update(params)
        for key, value in bindparams.iteritems():
            try:
                b = self.binds[key]
            except KeyError:
                continue
            d[b.key] = b.typeprocess(value)

        for b in self.binds.values():
            d.setdefault(b.key, b.typeprocess(b.value))

        if self.positional:
            return [d[key] for key in self.positiontup]
        else:
            return d

    def visit_column(self, column):
        if len(self.select_stack):
            # if we are within a visit to a Select, set up the "typemap"
            # for this column which is used to translate result set values
            if self.select_stack[-1].use_labels:
                self.typemap.setdefault(column.label, column.type)
            else:
                self.typemap.setdefault(column.key, column.type)
        if column.table.name is None:
            self.strings[column] = column.name
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)

    def visit_columnclause(self, column):
        if column.table is not None and column.table.name is not None:
            self.strings[column] = "%s.%s" % (column.table.name, column.text)
        else:
            self.strings[column] = column.text

    def visit_fromclause(self, fromclause):
        self.froms[fromclause] = fromclause.from_name

    def visit_textclause(self, textclause):
        if textclause.parens and len(textclause.text):
            self.strings[textclause] = "(" + textclause.text + ")"
        else:
            self.strings[textclause] = textclause.text
        self.froms[textclause] = textclause.text
        
    def visit_null(self, null):
        self.strings[null] = 'NULL'
       
    def visit_compound(self, compound):
        if compound.operator is None:
            sep = " "
        else:
            sep = " " + compound.operator + " "
        
        if compound.parens:
            self.strings[compound] = "(" + string.join([self.get_str(c) for c in compound.clauses], sep) + ")"
        else:
            self.strings[compound] = string.join([self.get_str(c) for c in compound.clauses], sep)
        
    def visit_clauselist(self, list):
        if list.parens:
            self.strings[list] = "(" + string.join([self.get_str(c) for c in list.clauses], ', ') + ")"
        else:
            self.strings[list] = string.join([self.get_str(c) for c in list.clauses], ', ')

    def visit_function(self, func):
        self.strings[func] = func.name + "(" + string.join([self.get_str(c) for c in func.clauses], ', ') + ")"
    
    def visit_compound_select(self, cs):
        text = string.join([self.get_str(c) for c in cs.selects], " " + cs.keyword + " ")
        for tup in cs.clauses:
            text += " " + tup[0] + " " + self.get_str(tup[1])
        self.strings[cs] = text
            
    def visit_binary(self, binary):
        result = self.get_str(binary.left)
        if binary.operator is not None:
            result += " " + binary.operator
        result += " " + self.get_str(binary.right)
        if binary.parens:
            result = "(" + result + ")"
        self.strings[binary] = result

    def visit_bindparam(self, bindparam):
        if bindparam.shortname != bindparam.key:
            self.binds[bindparam.shortname] = bindparam
        count = 1
        key = bindparam.key

        # redefine the generated name of the bind param in the case
        # that we have multiple conflicting bind parameters.
        while self.binds.setdefault(key, bindparam) is not bindparam:
            key = "%s_%d" % (bindparam.key, count)
            count += 1
        bindparam.key = key
        self.strings[bindparam] = self.bindparam_string(key)

    def bindparam_string(self, name):
        return self.bindtemplate % name
        
    def visit_alias(self, alias):
        self.froms[alias] = self.get_from_text(alias.selectable) + " AS " + alias.name
        self.strings[alias] = self.get_str(alias.selectable)

    def visit_select(self, select):
        inner_columns = []

        self.select_stack.append(select)
        for c in select._raw_columns:
            if c.is_selectable():
                for co in c.columns:
                    co.accept_visitor(self)
                    inner_columns.append(co)
            else:
                c.accept_visitor(self)
                inner_columns.append(c)
        self.select_stack.pop(-1)
        
        if select.use_labels:
            collist = string.join(["%s AS %s" % (self.get_str(c), c.label) for c in inner_columns], ', ')
        else:
            collist = string.join([self.get_str(c) for c in inner_columns], ', ')

        text = "SELECT "
        if select.distinct:
            text += "DISTINCT "
        text += collist + " \nFROM "
        
        whereclause = select.whereclause
        
        froms = []
        for f in select.froms:

            # special thingy used by oracle to redefine a join
            w = self.get_whereclause(f)
            if w is not None:
                # TODO: move this more into the oracle module
                whereclause = sql.and_(w, whereclause)
                self.visit_compound(whereclause)
                
            t = self.get_from_text(f)
            if t is not None:
                froms.append(t)

        text += string.join(froms, ', ')

        if whereclause is not None:
            t = self.get_str(whereclause)
            if t:
                text += " \nWHERE " + t

        for tup in select.clauses:
            text += " " + tup[0] + " " + self.get_str(tup[1])

        if select.having is not None:
            t = self.get_str(select.having)
            if t:
                text += " \nHAVING " + t

        if select.limit is not None or select.offset is not None:
            # TODO: ok, so this is a simple limit/offset thing.
            # need to make this DB neutral for mysql, oracle
            text += self.limit_clause(select)
            
        if getattr(select, 'issubquery', False):
            self.strings[select] = "(" + text + ")"
        else:
            self.strings[select] = text

        self.froms[select] = "(" + text + ")"

    def limit_clause(self, select):
        if select.limit is not None:
            return  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                return " \n LIMIT -1"
            return " OFFSET " + str(select.offset)

    def visit_table(self, table):
        self.froms[table] = table.fullname
        self.strings[table] = ""

    def visit_join(self, join):
        # TODO: ppl are going to want RIGHT, FULL OUTER and NATURAL joins.
        righttext = self.get_from_text(join.right)
        if join.right._group_parenthesized():
            righttext = "(" + righttext + ")"
        if join.isouter:
            self.froms[join] = (self.get_from_text(join.left) + " LEFT OUTER JOIN " + righttext + 
            " ON " + self.get_str(join.onclause))
        else:
            self.froms[join] = (self.get_from_text(join.left) + " JOIN " + righttext +
            " ON " + self.get_str(join.onclause))
        self.strings[join] = self.froms[join]
        
    def visit_insert(self, insert_stmt):
        self.isinsert = True
        colparams = insert_stmt.get_colparams(self.bindparams)
        for c in colparams:
            b = c[1]
            self.binds[b.key] = b
            self.binds[b.shortname] = b
            
        text = ("INSERT INTO " + insert_stmt.table.fullname + " (" + string.join([c[0].name for c in colparams], ', ') + ")" +
         " VALUES (" + string.join([self.bindparam_string(c[1].key) for c in colparams], ', ') + ")")
         
        self.strings[insert_stmt] = text

    def visit_update(self, update_stmt):
        colparams = update_stmt.get_colparams(self.bindparams)
        def create_param(p):
            if isinstance(p, sql.BindParamClause):
                self.binds[p.key] = p
                self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                p.accept_visitor(self)
                if isinstance(p, sql.ClauseElement):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)
                
        text = "UPDATE " + update_stmt.table.fullname + " SET " + string.join(["%s=%s" % (c[0].name, create_param(c[1])) for c in colparams], ', ')
        
        if update_stmt.whereclause:
            text += " WHERE " + self.get_str(update_stmt.whereclause)
         
        self.strings[update_stmt] = text

    def visit_delete(self, delete_stmt):
        text = "DELETE FROM " + delete_stmt.table.fullname
        
        if delete_stmt.whereclause:
            text += " WHERE " + self.get_str(delete_stmt.whereclause)
         
        self.strings[delete_stmt] = text
        
    def __str__(self):
        return self.get_str(self.statement)


class ANSISchemaGenerator(sqlalchemy.engine.SchemaIterator):

    def get_column_specification(self, column, override_pk=False, first_pk=False):
        raise NotImplementedError()
        
    def visit_table(self, table):
        self.append("\nCREATE TABLE " + table.fullname + "(")
        
        separator = "\n"
        
        # if only one primary key, specify it along with the column
        pks = table.primary_key
        first_pk = False
        for column in table.columns:
            self.append(separator)
            separator = ", \n"
            self.append("\t" + self.get_column_specification(column, override_pk=len(pks)>1, first_pk=column.primary_key and not first_pk))
            if column.primary_key:
                first_pk = True
        # if multiple primary keys, specify it at the bottom
        if len(pks) > 1:
            self.append(", \n")
            self.append("\tPRIMARY KEY (%s)" % string.join([c.name for c in pks],', '))
                    
        self.append("\n)\n\n")
        self.execute()

    def visit_column(self, column):
        pass
    
class ANSISchemaDropper(sqlalchemy.engine.SchemaIterator):
    def visit_table(self, table):
        self.append("\nDROP TABLE " + table.fullname)
        self.execute()


