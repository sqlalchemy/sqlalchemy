# ansisql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines ANSI SQL operations.  Contains default implementations for the abstract objects 
in the sql module."""

from sqlalchemy import schema, sql, engine, util, sql_util
from  sqlalchemy.engine import default
import string, re, sets, weakref

ANSI_FUNCS = sets.ImmutableSet([
'CURRENT_TIME',
'CURRENT_TIMESTAMP',
'CURRENT_DATE',
'LOCALTIME',
'LOCALTIMESTAMP',
'CURRENT_USER',
'SESSION_USER',
'USER'
])


RESERVED_WORDS = util.Set(['all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric', 'authorization', 'between', 'binary', 'both', 'case', 'cast', 'check', 'collate', 'column', 'constraint', 'create', 'cross', 'current_date', 'current_role', 'current_time', 'current_timestamp', 'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end', 'except', 'false', 'for', 'foreign', 'freeze', 'from', 'full', 'grant', 'group', 'having', 'ilike', 'in', 'initially', 'inner', 'intersect', 'into', 'is', 'isnull', 'join', 'leading', 'left', 'like', 'limit', 'localtime', 'localtimestamp', 'natural', 'new', 'not', 'notnull', 'null', 'off', 'offset', 'old', 'on', 'only', 'or', 'order', 'outer', 'overlaps', 'placing', 'primary', 'references', 'right', 'select', 'session_user', 'similar', 'some', 'symmetric', 'table', 'then', 'to', 'trailing', 'true', 'union', 'unique', 'user', 'using', 'verbose', 'when', 'where'])

LEGAL_CHARACTERS = util.Set(string.ascii_lowercase + string.ascii_uppercase + string.digits + '_$')
ILLEGAL_INITIAL_CHARACTERS = util.Set(string.digits + '$')

class ANSIDialect(default.DefaultDialect):
    def __init__(self, cache_identifiers=True, **kwargs):
        super(ANSIDialect,self).__init__(**kwargs)
        self.identifier_preparer = self.preparer()
        self.cache_identifiers = cache_identifiers
        
    def create_connect_args(self):
        return ([],{})

    def dbapi(self):
        return None

    def schemagenerator(self, *args, **params):
        return ANSISchemaGenerator(*args, **params)

    def schemadropper(self, *args, **params):
        return ANSISchemaDropper(*args, **params)

    def compiler(self, statement, parameters, **kwargs):
        return ANSICompiler(self, statement, parameters, **kwargs)

    def preparer(self):
        """return an IdenfifierPreparer.
        
        This object is used to format table and column names including proper quoting and case conventions."""
        return ANSIIdentifierPreparer(self)

class ANSICompiler(sql.Compiled):
    """default implementation of Compiled, which compiles ClauseElements into ANSI-compliant SQL strings."""
    def __init__(self, dialect, statement, parameters=None, **kwargs):
        """constructs a new ANSICompiler object.
        
        dialect - Dialect to be used
        
        statement - ClauseElement to be compiled
        
        parameters - optional dictionary indicating a set of bind parameters
        specified with this Compiled object.  These parameters are the "default"
        key/value pairs when the Compiled is executed, and also may affect the 
        actual compilation, as in the case of an INSERT where the actual columns
        inserted will correspond to the keys present in the parameters."""
        sql.Compiled.__init__(self, dialect, statement, parameters, **kwargs)
        
        # a dictionary of bind parameter keys to _BindParamClause instances.
        self.binds = {}

        # a dictionary which stores the string representation for every ClauseElement
        # processed by this compiler.
        self.strings = {}
        
        # a dictionary which stores the string representation for ClauseElements
        # processed by this compiler, which are to be used in the FROM clause
        # of a select.  items are often placed in "froms" as well as "strings"
        # and sometimes with different representations.
        self.froms = {}
        
        # slightly hacky.  maps FROM clauses to WHERE clauses, and used in select 
        # generation to modify the WHERE clause of the select.  currently a hack
        # used by the oracle module.
        self.wheres = {}
        
        # when the compiler visits a SELECT statement, the clause object is appended
        # to this stack.  various visit operations will check this stack to determine
        # additional choices (TODO: it seems to be all typemap stuff.  shouldnt this only
        # apply to the topmost-level SELECT statement ?)
        self.select_stack = []
        
        # a dictionary of result-set column names (strings) to TypeEngine instances,
        # which will be passed to a ResultProxy and used for resultset-level value conversion
        self.typemap = {}
        
        # a dictionary of select columns mapped to their name or key
        self.columns = {}
        
        # True if this compiled represents an INSERT
        self.isinsert = False
        
        # True if this compiled represents an UPDATE
        self.isupdate = False
        
        # default formatting style for bind parameters
        self.bindtemplate = ":%s"
        
        # paramstyle from the dialect (comes from DBAPI)
        self.paramstyle = dialect.paramstyle
        
        # true if the paramstyle is positional
        self.positional = dialect.positional
        
        # a list of the compiled's bind parameter names, used to help
        # formulate a positional argument list
        self.positiontup = []
        
        # an ANSIIdentifierPreparer that formats the quoting of identifiers
        self.preparer = dialect.identifier_preparer
        
        # for UPDATE and INSERT statements, a set of columns whos values are being set
        # from a SQL expression (i.e., not one of the bind parameter values).  if present,
        # default-value logic in the Dialect knows not to fire off column defaults
        # and also knows postfetching will be needed to get the values represented by these
        # parameters.
        self.inline_params = None
        
    def after_compile(self):
        # this re will search for params like :param
        # it has a negative lookbehind for an extra ':' so that it doesnt match
        # postgres '::text' tokens
        match = r'(?<!:):([\w_]+)'
        if self.paramstyle=='pyformat':
            self.strings[self.statement] = re.sub(match, lambda m:'%(' + m.group(1) +')s', self.strings[self.statement])
        elif self.positional:
            params = re.finditer(match, self.strings[self.statement])
            for p in params:
                self.positiontup.append(p.group(1))
            if self.paramstyle=='qmark':
                self.strings[self.statement] = re.sub(match, '?', self.strings[self.statement])
            elif self.paramstyle=='format':
                self.strings[self.statement] = re.sub(match, '%s', self.strings[self.statement])
            elif self.paramstyle=='numeric':
                i = [0]
                def getnum(x):
                    i[0] += 1
                    return str(i[0])
                self.strings[self.statement] = re.sub(match, getnum, self.strings[self.statement])

    def get_from_text(self, obj):
        return self.froms.get(obj, None)

    def get_str(self, obj):
        return self.strings[obj]

    def get_whereclause(self, obj):
        return self.wheres.get(obj, None)

    def get_params(self, **params):
        """returns a structure of bind parameters for this compiled object.
        This includes bind parameters that might be compiled in via the "values"
        argument of an Insert or Update statement object, and also the given **params.
        The keys inside of **params can be any key that matches the BindParameterClause
        objects compiled within this object.  The output is dependent on the paramstyle
        of the DBAPI being used; if a named style, the return result will be a dictionary
        with keynames matching the compiled statement.  If a positional style, the output
        will be a list, with an iterator that will return parameter 
        values in an order corresponding to the bind positions in the compiled statement.
        
        for an executemany style of call, this method should be called for each element
        in the list of parameter groups that will ultimately be executed.
        """
        if self.parameters is not None:
            bindparams = self.parameters.copy()
        else:
            bindparams = {}
        bindparams.update(params)

        d = sql.ClauseParameters(self.dialect, self.positiontup)
        for b in self.binds.values():
            d.set_parameter(b, b.value)

        for key, value in bindparams.iteritems():
            try:
                b = self.binds[key]
            except KeyError:
                continue
            d.set_parameter(b, value)

        return d

    def default_from(self):
        """called when a SELECT statement has no froms, and no FROM clause is to be appended.  
        gives Oracle a chance to tack on a "FROM DUAL" to the string output. """
        return ""

    def visit_label(self, label):
        if len(self.select_stack):
            self.typemap.setdefault(label.name.lower(), label.obj.type)
        self.strings[label] = self.strings[label.obj] + " AS "  + self.preparer.format_label(label)
        
    def visit_column(self, column):
        if len(self.select_stack):
            # if we are within a visit to a Select, set up the "typemap"
            # for this column which is used to translate result set values
            self.typemap.setdefault(column.name.lower(), column.type)
            self.columns.setdefault(column.key, column)
        if column.table is None or not column.table.named_with_column():
            self.strings[column] = self.preparer.format_column(column)
        else:
            if column.table.oid_column is column:
                n = self.dialect.oid_column_name()
                if n is not None:
                    self.strings[column] = "%s.%s" % (self.preparer.format_table(column.table, use_schema=False), n)
                elif len(column.table.primary_key) != 0:
                    self.strings[column] = self.preparer.format_column_with_table(list(column.table.primary_key)[0])
                else:
                    self.strings[column] = None
            else:
                self.strings[column] = self.preparer.format_column_with_table(column)

    def visit_fromclause(self, fromclause):
        self.froms[fromclause] = fromclause.name

    def visit_index(self, index):
        self.strings[index] = index.name
    
    def visit_typeclause(self, typeclause):
        self.strings[typeclause] = typeclause.type.dialect_impl(self.dialect).get_col_spec()
            
    def visit_textclause(self, textclause):
        if textclause.parens and len(textclause.text):
            self.strings[textclause] = "(" + textclause.text + ")"
        else:
            self.strings[textclause] = textclause.text
        self.froms[textclause] = textclause.text
        if textclause.typemap is not None:
            self.typemap.update(textclause.typemap)
        
    def visit_null(self, null):
        self.strings[null] = 'NULL'
       
    def visit_compound(self, compound):
        if compound.operator is None:
            sep = " "
        else:
            sep = " " + compound.operator + " "
        
        s = string.join([self.get_str(c) for c in compound.clauses], sep)
        if compound.parens:
            self.strings[compound] = "(" + s + ")"
        else:
            self.strings[compound] = s
        
    def visit_clauselist(self, list):
        if list.parens:
            self.strings[list] = "(" + string.join([s for s in [self.get_str(c) for c in list.clauses] if s is not None], ', ') + ")"
        else:
            self.strings[list] = string.join([s for s in [self.get_str(c) for c in list.clauses] if s is not None], ', ')

    def apply_function_parens(self, func):
        return func.name.upper() not in ANSI_FUNCS or len(func.clauses) > 0

    def visit_calculatedclause(self, list):
        if list.parens:
            self.strings[list] = "(" + string.join([self.get_str(c) for c in list.clauses], ' ') + ")"
        else:
            self.strings[list] = string.join([self.get_str(c) for c in list.clauses], ' ')
      
    def visit_cast(self, cast):
        if len(self.select_stack):
            # not sure if we want to set the typemap here...
            self.typemap.setdefault("CAST", cast.type)
        self.strings[cast] = "CAST(%s AS %s)" % (self.strings[cast.clause],self.strings[cast.typeclause])
         
    def visit_function(self, func):
        if len(self.select_stack):
            self.typemap.setdefault(func.name, func.type)
        if not self.apply_function_parens(func):
            self.strings[func] = ".".join(func.packagenames + [func.name])
            self.froms[func] = self.strings[func]
        else:
            self.strings[func] = ".".join(func.packagenames + [func.name]) + "(" + string.join([self.get_str(c) for c in func.clauses], ', ') + ")"
            self.froms[func] = self.strings[func]
        
    def visit_compound_select(self, cs):
        text = string.join([self.get_str(c) for c in cs.selects], " " + cs.keyword + " ")
        group_by = self.get_str(cs.group_by_clause)
        if group_by:
            text += " GROUP BY " + group_by
        order_by = self.get_str(cs.order_by_clause)
        if order_by:
            text += " ORDER BY " + order_by
        text += self.visit_select_postclauses(cs)
        if cs.parens:
            self.strings[cs] = "(" + text + ")"
        else:
            self.strings[cs] = text
        self.froms[cs] = "(" + text + ")"

    def visit_binary(self, binary):
        result = self.get_str(binary.left)
        if binary.operator is not None:
            result += " " + self.binary_operator_string(binary)
        result += " " + self.get_str(binary.right)
        if binary.parens:
            result = "(" + result + ")"
        self.strings[binary] = result

    def binary_operator_string(self, binary):
        return binary.operator

    def visit_bindparam(self, bindparam):
        if bindparam.shortname != bindparam.key:
            self.binds.setdefault(bindparam.shortname, bindparam)
        count = 1
        key = bindparam.key

        # redefine the generated name of the bind param in the case
        # that we have multiple conflicting bind parameters.
        while self.binds.setdefault(key, bindparam) is not bindparam:
            # ensure the name doesn't expand the length of the string
            # in case we're at the edge of max identifier length
            tag = "_%d" % count
            key = bindparam.key[0 : len(bindparam.key) - len(tag)] + tag
            count += 1
        bindparam.key = key
        self.strings[bindparam] = self.bindparam_string(key)

    def bindparam_string(self, name):
        return self.bindtemplate % name
        
    def visit_alias(self, alias):
        self.froms[alias] = self.get_from_text(alias.original) + " AS " + self.preparer.format_alias(alias)
        self.strings[alias] = self.get_str(alias.original)

    def visit_select(self, select):
        
        # the actual list of columns to print in the SELECT column list.
        inner_columns = util.OrderedDict()

        self.select_stack.append(select)
        for c in select._raw_columns:
            if isinstance(c, sql.Select) and c.is_scalar:
                c.accept_visitor(self)
                inner_columns[self.get_str(c)] = c
                continue
            try:
                s = c._selectable()
            except AttributeError:
                c.accept_visitor(self)
                inner_columns[self.get_str(c)] = c
                continue
            for co in s.columns:
                if select.use_labels:
                    l = co.label(co._label)
                    l.accept_visitor(self)
                    inner_columns[co._label] = l
                # TODO: figure this out, a ColumnClause with a select as a parent
                # is different from any other kind of parent
                elif select.is_subquery and isinstance(co, sql._ColumnClause) and co.table is not None and not isinstance(co.table, sql.Select):
                    # SQLite doesnt like selecting from a subquery where the column
                    # names look like table.colname, so add a label synonomous with
                    # the column name
                    l = co.label(co.name)
                    l.accept_visitor(self)
                    inner_columns[self.get_str(l.obj)] = l
                else:
                    co.accept_visitor(self)
                    inner_columns[self.get_str(co)] = co
        self.select_stack.pop(-1)
        
        collist = string.join([self.get_str(v) for v in inner_columns.values()], ', ')

        text = "SELECT "
        text += self.visit_select_precolumns(select)
        text += collist
        
        whereclause = select.whereclause
        
        froms = []
        for f in select.froms:

            if self.parameters is not None:
                # look at our own parameters, see if they
                # are all present in the form of BindParamClauses.  if
                # not, then append to the above whereclause column conditions
                # matching those keys
                for c in f.columns:
                    if sql.is_column(c) and self.parameters.has_key(c.key) and not self.binds.has_key(c.key):
                        value = self.parameters[c.key]
                    else:
                        continue
                    clause = c==value
                    clause.accept_visitor(self)
                    whereclause = sql.and_(clause, whereclause)
                    self.visit_compound(whereclause)

            # special thingy used by oracle to redefine a join
            w = self.get_whereclause(f)
            if w is not None:
                # TODO: move this more into the oracle module
                whereclause = sql.and_(w, whereclause)
                self.visit_compound(whereclause)
                
            t = self.get_from_text(f)
            if t is not None:
                froms.append(t)
        
        if len(froms):
            text += " \nFROM "
            text += string.join(froms, ', ')
        else:
            text += self.default_from()
            
        if whereclause is not None:
            t = self.get_str(whereclause)
            if t:
                text += " \nWHERE " + t

        group_by = self.get_str(select.group_by_clause)
        if group_by:
            text += " GROUP BY " + group_by

        if select.having is not None:
            t = self.get_str(select.having)
            if t:
                text += " \nHAVING " + t

        order_by = self.get_str(select.order_by_clause)
        if order_by:
            text += " ORDER BY " + order_by

        text += self.visit_select_postclauses(select)

        text += self.for_update_clause(select)

        if getattr(select, 'parens', False):
            self.strings[select] = "(" + text + ")"
        else:
            self.strings[select] = text
        self.froms[select] = "(" + text + ")"

    def visit_select_precolumns(self, select):
        """ called when building a SELECT statment, position is just before column list """
        return select.distinct and "DISTINCT " or ""

    def visit_select_postclauses(self, select):
        """ called when building a SELECT statement, position is after all other SELECT clauses. Most DB syntaxes put LIMIT/OFFSET here """
        return (select.limit or select.offset) and self.limit_clause(select) or ""

    def for_update_clause(self, select):
        if select.for_update:
            return " FOR UPDATE"
        else:
            return ""

    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select.offset)
        return text

    def visit_table(self, table):
        self.froms[table] = self.preparer.format_table(table)
        self.strings[table] = ""

    def visit_join(self, join):
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

    def visit_insert_column_default(self, column, default, parameters):
        """called when visiting an Insert statement, for each column in the table that
        contains a ColumnDefault object.  adds a blank 'placeholder' parameter so the 
        Insert gets compiled with this column's name in its column and VALUES clauses."""
        parameters.setdefault(column.key, None)

    def visit_update_column_default(self, column, default, parameters):
        """called when visiting an Update statement, for each column in the table that
        contains a ColumnDefault object as an onupdate. adds a blank 'placeholder' parameter so the 
        Update gets compiled with this column's name as one of its SET clauses."""
        parameters.setdefault(column.key, None)
        
    def visit_insert_sequence(self, column, sequence, parameters):
        """called when visiting an Insert statement, for each column in the table that
        contains a Sequence object.  Overridden by compilers that support sequences to place
        a blank 'placeholder' parameter, so the Insert gets compiled with this column's
        name in its column and VALUES clauses."""
        pass
    
    def visit_insert_column(self, column, parameters):
        """called when visiting an Insert statement, for each column in the table
        that is a NULL insert into the table.  Overridden by compilers who disallow
        NULL columns being set in an Insert where there is a default value on the column
        (i.e. postgres), to remove the column from the parameter list."""
        pass
        
    def visit_insert(self, insert_stmt):
        # scan the table's columns for defaults that have to be pre-set for an INSERT
        # add these columns to the parameter list via visit_insert_XXX methods
        default_params = {}
        class DefaultVisitor(schema.SchemaVisitor):
            def visit_column(s, c):
                self.visit_insert_column(c, default_params)
            def visit_column_default(s, cd):
                self.visit_insert_column_default(c, cd, default_params)
            def visit_sequence(s, seq):
                self.visit_insert_sequence(c, seq, default_params)
        vis = DefaultVisitor()
        for c in insert_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                c.accept_schema_visitor(vis)
        
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt, default_params)

        self.inline_params = util.Set()
        def create_param(col, p):
            if isinstance(p, sql._BindParamClause):
                self.binds[p.key] = p
                if p.shortname is not None:
                    self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                self.inline_params.add(col)
                p.accept_visitor(self)
                if isinstance(p, sql.ClauseElement) and not isinstance(p, sql.ColumnElement):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)

        text = ("INSERT INTO " + self.preparer.format_table(insert_stmt.table) + " (" + string.join([self.preparer.format_column(c[0]) for c in colparams], ', ') + ")" +
         " VALUES (" + string.join([create_param(*c) for c in colparams], ', ') + ")")

        self.strings[insert_stmt] = text

    def visit_update(self, update_stmt):
        # scan the table's columns for onupdates that have to be pre-set for an UPDATE
        # add these columns to the parameter list via visit_update_XXX methods
        default_params = {}
        class OnUpdateVisitor(schema.SchemaVisitor):
            def visit_column_onupdate(s, cd):
                self.visit_update_column_default(c, cd, default_params)
        vis = OnUpdateVisitor()
        for c in update_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                c.accept_schema_visitor(vis)

        self.isupdate = True
        colparams = self._get_colparams(update_stmt, default_params)

        self.inline_params = util.Set()
        def create_param(col, p):
            if isinstance(p, sql._BindParamClause):
                self.binds[p.key] = p
                self.binds[p.shortname] = p
                return self.bindparam_string(p.key)
            else:
                p.accept_visitor(self)
                self.inline_params.add(col)
                if isinstance(p, sql.ClauseElement) and not isinstance(p, sql.ColumnElement):
                    return "(" + self.get_str(p) + ")"
                else:
                    return self.get_str(p)
                
        text = "UPDATE " + self.preparer.format_table(update_stmt.table) + " SET " + string.join(["%s=%s" % (self.preparer.format_column(c[0]), create_param(*c)) for c in colparams], ', ')
        
        if update_stmt.whereclause:
            text += " WHERE " + self.get_str(update_stmt.whereclause)
         
        self.strings[update_stmt] = text


    def _get_colparams(self, stmt, default_params):
        """organize UPDATE/INSERT SET/VALUES parameters into a list of tuples, 
        each tuple containing the Column and a ClauseElement representing the
        value to be set (usually a _BindParamClause, but could also be other
        SQL expressions.)

        the list of tuples will determine the columns that are actually rendered
        into the SET/VALUES clause of the rendered UPDATE/INSERT statement.  It will
        also determine how to generate the list/dictionary of bind parameters at 
        execution time (i.e. get_params()).
        
        this list takes into account the "values" keyword specified to the statement,
        the parameters sent to this Compiled instance, and the default bind parameter
        values corresponding to the dialect's behavior for otherwise unspecified 
        primary key columns.
        """
        # no parameters in the statement, no parameters in the 
        # compiled params - return binds for all columns
        if self.parameters is None and stmt.parameters is None:
            return [(c, sql.bindparam(c.key, type=c.type)) for c in stmt.table.columns]

        def to_col(key):
            if not isinstance(key, sql._ColumnClause):
                return stmt.table.columns.get(str(key), key)
            else:
                return key
                
        # if we have statement parameters - set defaults in the 
        # compiled params
        if self.parameters is None:
            parameters = {}
        else:
            parameters = dict([(to_col(k), v) for k, v in self.parameters.iteritems()])

        if stmt.parameters is not None:
            for k, v in stmt.parameters.iteritems():
                parameters.setdefault(to_col(k), v)

        for k, v in default_params.iteritems():
            parameters.setdefault(to_col(k), v)

        # create a list of column assignment clauses as tuples
        values = []
        for c in stmt.table.columns:
            if parameters.has_key(c):
                value = parameters[c]
                if sql._is_literal(value):
                    value = sql.bindparam(c.key, value, type=c.type)
                values.append((c, value))
        return values

    def visit_delete(self, delete_stmt):
        text = "DELETE FROM " + self.preparer.format_table(delete_stmt.table)
        
        if delete_stmt.whereclause:
            text += " WHERE " + self.get_str(delete_stmt.whereclause)
         
        self.strings[delete_stmt] = text
        
    def __str__(self):
        return self.get_str(self.statement)

class ANSISchemaBase(engine.SchemaIterator):
    def find_alterables(self, tables):
        alterables = []
        class FindAlterables(schema.SchemaVisitor):
            def visit_foreign_key_constraint(self, constraint):
                if constraint.use_alter and constraint.table in tables:
                    alterables.append(constraint)
        findalterables = FindAlterables()
        for table in tables:
            for c in table.constraints:
                c.accept_schema_visitor(findalterables)
        return alterables
        
class ANSISchemaGenerator(ANSISchemaBase):
    def __init__(self, engine, proxy, connection, checkfirst=False, tables=None, **kwargs):
        super(ANSISchemaGenerator, self).__init__(engine, proxy, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables and util.Set(tables) or None
        self.connection = connection
        self.preparer = self.engine.dialect.preparer()
        self.dialect = self.engine.dialect
        
    def get_column_specification(self, column, first_pk=False):
        raise NotImplementedError()
    
    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=False, tables=self.tables) if (not self.checkfirst or not self.dialect.has_table(self.connection, t.name))]
        for table in collection:
            table.accept_schema_visitor(self, traverse=False)
        if self.supports_alter():
            for alterable in self.find_alterables(collection):
                self.add_foreignkey(alterable)
                
    def visit_table(self, table):
        for column in table.columns:
            if column.default is not None:
                column.default.accept_schema_visitor(self, traverse=False)
            #if column.onupdate is not None:
            #    column.onupdate.accept_schema_visitor(visitor, traverse=False)
        
        self.append("\nCREATE TABLE " + self.preparer.format_table(table) + " (")
        
        separator = "\n"
        
        # if only one primary key, specify it along with the column
        first_pk = False
        for column in table.columns:
            self.append(separator)
            separator = ", \n"
            self.append("\t" + self.get_column_specification(column, first_pk=column.primary_key and not first_pk))
            if column.primary_key:
                first_pk = True
            for constraint in column.constraints:
                constraint.accept_schema_visitor(self, traverse=False)

        # On some DB order is significant: visit PK first, then the
        # other constraints (engine.ReflectionTest.testbasic failed on FB2)
        if len(table.primary_key):
            table.primary_key.accept_schema_visitor(self, traverse=False)
        for constraint in [c for c in table.constraints if c is not table.primary_key]:
            constraint.accept_schema_visitor(self, traverse=False)

        self.append("\n)%s\n\n" % self.post_create_table(table))
        self.execute()
        if hasattr(table, 'indexes'):
            for index in table.indexes:
                index.accept_schema_visitor(self, traverse=False)
        
    def post_create_table(self, table):
        return ''

    def get_column_default_string(self, column):
        if isinstance(column.default, schema.PassiveDefault):
            if isinstance(column.default.arg, str):
                return repr(column.default.arg)
            else:
                return str(self._compile(column.default.arg, None))
        else:
            return None

    def _compile(self, tocompile, parameters):
        """compile the given string/parameters using this SchemaGenerator's dialect."""
        compiler = self.engine.dialect.compiler(tocompile, parameters)
        compiler.compile()
        return compiler

    def visit_check_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " % constraint.name)
        self.append(" CHECK (%s)" % constraint.sqltext)
        
    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return
        self.append(", \n\tPRIMARY KEY ")
        if constraint.name is not None:
            self.append("%s " % constraint.name)
        self.append("(%s)" % (string.join([self.preparer.format_column(c) for c in constraint],', ')))
    
    def supports_alter(self):
        return True
                        
    def visit_foreign_key_constraint(self, constraint):
        if constraint.use_alter and self.supports_alter():
            return
        self.append(", \n\t ")
        self.define_foreign_key(constraint)
    
    def add_foreignkey(self, constraint):
        self.append("ALTER TABLE %s ADD " % self.preparer.format_table(constraint.table))
        self.define_foreign_key(constraint)
        self.execute()
        
    def define_foreign_key(self, constraint):
        if constraint.name is not None:
            self.append("CONSTRAINT %s " % constraint.name)
        self.append("FOREIGN KEY(%s) REFERENCES %s (%s)" % (
            string.join([self.preparer.format_column(f.parent) for f in constraint.elements], ', '),
            self.preparer.format_table(list(constraint.elements)[0].column.table),
            string.join([self.preparer.format_column(f.column) for f in constraint.elements], ', ')
        ))
        if constraint.ondelete is not None:
            self.append(" ON DELETE %s" % constraint.ondelete)
        if constraint.onupdate is not None:
            self.append(" ON UPDATE %s" % constraint.onupdate)

    def visit_unique_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " % constraint.name)
        self.append(" UNIQUE ")
        self.append("(%s)" % (string.join([self.preparer.format_column(c) for c in constraint],', ')))

    def visit_column(self, column):
        pass

    def visit_index(self, index):
        self.append('CREATE ')
        if index.unique:
            self.append('UNIQUE ')
        self.append('INDEX %s ON %s (%s)' \
                    % (index.name, self.preparer.format_table(index.table),
                       string.join([self.preparer.format_column(c) for c in index.columns], ', ')))
        self.execute()
        
class ANSISchemaDropper(ANSISchemaBase):
    def __init__(self, engine, proxy, connection, checkfirst=False, tables=None, **kwargs):
        super(ANSISchemaDropper, self).__init__(engine, proxy, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.connection = connection
        self.preparer = self.engine.dialect.preparer()
        self.dialect = self.engine.dialect

    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=True, tables=self.tables) if (not self.checkfirst or  self.dialect.has_table(self.connection, t.name))]
        if self.supports_alter():
            for alterable in self.find_alterables(collection):
                self.drop_foreignkey(alterable)
        for table in collection:
            table.accept_schema_visitor(self, traverse=False)

    def supports_alter(self):
        return True

    def visit_index(self, index):
        self.append("\nDROP INDEX " + index.name)
        self.execute()

    def drop_foreignkey(self, constraint):
        self.append("ALTER TABLE %s DROP CONSTRAINT %s" % (self.preparer.format_table(constraint.table), constraint.name))
        self.execute()
        
    def visit_table(self, table):
        for column in table.columns:
            if column.default is not None:
                column.default.accept_schema_visitor(self, traverse=False)

        self.append("\nDROP TABLE " + self.preparer.format_table(table))
        self.execute()

class ANSIDefaultRunner(engine.DefaultRunner):
    pass

class ANSIIdentifierPreparer(object):
    """handles quoting and case-folding of identifiers based on options"""
    def __init__(self, dialect, initial_quote='"', final_quote=None, omit_schema=False):
        """Constructs a new ANSIIdentifierPreparer object.
        
        initial_quote - Character that begins a delimited identifier
        final_quote - Caracter that ends a delimited identifier. defaults to initial_quote.
        
        omit_schema - prevent prepending schema name. useful for databases that do not support schemae
        """
        self.dialect = dialect
        self.initial_quote = initial_quote
        self.final_quote = final_quote or self.initial_quote
        self.omit_schema = omit_schema
        self.__strings = {}
    def _escape_identifier(self, value):
        """escape an identifier.
        
        subclasses should override this to provide database-dependent escaping behavior."""
        return value.replace('"', '""')
    
    def _quote_identifier(self, value):
        """quote an identifier.
        
        subclasses should override this to provide database-dependent quoting behavior."""
        return self.initial_quote + self._escape_identifier(value) + self.final_quote
    
    def _fold_identifier_case(self, value):
        """fold the case of an identifier.
        
        subclassses should override this to provide database-dependent case folding behavior."""
        return value
        # ANSI SQL calls for the case of all unquoted identifiers to be folded to UPPER.
        # some tests would need to be rewritten if this is done.
        #return value.upper()
    
    def _reserved_words(self):
        return RESERVED_WORDS

    def _legal_characters(self):
        return LEGAL_CHARACTERS
    
    def _illegal_initial_characters(self):
        return ILLEGAL_INITIAL_CHARACTERS
        
    def _requires_quotes(self, value, case_sensitive):
        """return true if the given identifier requires quoting."""
        return \
            value in self._reserved_words() \
            or (value[0] in self._illegal_initial_characters()) \
            or bool(len([x for x in str(value) if x not in self._legal_characters()])) \
            or (case_sensitive and value.lower() != value)
    
    def __generic_obj_format(self, obj, ident):
        if getattr(obj, 'quote', False):
            return self._quote_identifier(ident)
        if self.dialect.cache_identifiers:
            case_sens = getattr(obj, 'case_sensitive', None)
            try:
                return self.__strings[(ident, case_sens)]
            except KeyError:
                if self._requires_quotes(ident, getattr(obj, 'case_sensitive', ident == ident.lower())):
                    self.__strings[(ident, case_sens)] = self._quote_identifier(ident)
                else:
                    self.__strings[(ident, case_sens)] = ident
                return self.__strings[(ident, case_sens)]
        else:
            if self._requires_quotes(ident, getattr(obj, 'case_sensitive', ident == ident.lower())):
                return self._quote_identifier(ident)
            else:
                return ident
            
    def should_quote(self, object):
        return object.quote or self._requires_quotes(object.name, object.case_sensitive) 
 
    def is_natural_case(self, object):
        return object.quote or self._requires_quotes(object.name, object.case_sensitive)
        
    def format_sequence(self, sequence):
        return self.__generic_obj_format(sequence, sequence.name)
    
    def format_label(self, label):
        return self.__generic_obj_format(label, label.name)

    def format_alias(self, alias):
        return self.__generic_obj_format(alias, alias.name)
        
    def format_table(self, table, use_schema=True):
        """Prepare a quoted table and schema name"""
        result = self.__generic_obj_format(table, table.name)
        if use_schema and getattr(table, "schema", None):
            result = self.__generic_obj_format(table, table.schema) + "." + result
        return result
    
    def format_column(self, column, use_table=False):
        """Prepare a quoted column name """
        # TODO: isinstance alert !  get ColumnClause and Column to better
        # differentiate themselves
        if isinstance(column, schema.SchemaItem):
            if use_table:
                return self.format_table(column.table, use_schema=False) + "." + self.__generic_obj_format(column, column.name)
            else:
                return self.__generic_obj_format(column, column.name)
        else:
            # literal textual elements get stuck into ColumnClause alot, which shouldnt get quoted
            if use_table:
                return column.table.name + "." + column.name
            else:
                return column.name
            
    def format_column_with_table(self, column):
        """Prepare a quoted column name with table name"""
        return self.format_column(column, use_table=True)

