# ansisql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines ANSI SQL operations.

Contains default implementations for the abstract objects in the sql
module.
"""

import string, re, sets, operator

from sqlalchemy import schema, sql, engine, util, exceptions
from  sqlalchemy.engine import default


ANSI_FUNCS = sets.ImmutableSet([
    'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
    'CURRENT_USER', 'LOCALTIME', 'LOCALTIMESTAMP',
    'SESSION_USER', 'USER'])

RESERVED_WORDS = util.Set([
    'all', 'analyse', 'analyze', 'and', 'any', 'array',
    'as', 'asc', 'asymmetric', 'authorization', 'between',
    'binary', 'both', 'case', 'cast', 'check', 'collate',
    'column', 'constraint', 'create', 'cross', 'current_date',
    'current_role', 'current_time', 'current_timestamp',
    'current_user', 'default', 'deferrable', 'desc',
    'distinct', 'do', 'else', 'end', 'except', 'false',
    'for', 'foreign', 'freeze', 'from', 'full', 'grant',
    'group', 'having', 'ilike', 'in', 'initially', 'inner',
    'intersect', 'into', 'is', 'isnull', 'join', 'leading',
    'left', 'like', 'limit', 'localtime', 'localtimestamp',
    'natural', 'new', 'not', 'notnull', 'null', 'off', 'offset',
    'old', 'on', 'only', 'or', 'order', 'outer', 'overlaps',
    'placing', 'primary', 'references', 'right', 'select',
    'session_user', 'set', 'similar', 'some', 'symmetric', 'table',
    'then', 'to', 'trailing', 'true', 'union', 'unique', 'user',
    'using', 'verbose', 'when', 'where'])

LEGAL_CHARACTERS = util.Set(string.ascii_lowercase +
                            string.ascii_uppercase +
                            string.digits + '_$')
ILLEGAL_INITIAL_CHARACTERS = util.Set(string.digits + '$')

BIND_PARAMS = re.compile(r'(?<![:\w\$\x5c]):([\w\$]+)(?![:\w\$])', re.UNICODE)
BIND_PARAMS_ESC = re.compile(r'\x5c(:[\w\$]+)(?![:\w\$])', re.UNICODE)
ANONYMOUS_LABEL = re.compile(r'{ANON (-?\d+) (.*)}')

OPERATORS =  {
    operator.and_ : 'AND',
    operator.or_ : 'OR',
    operator.inv : 'NOT',
    operator.add : '+',
    operator.mul : '*',
    operator.sub : '-',
    operator.div : '/',
    operator.mod : '%',
    operator.truediv : '/',
    operator.lt : '<',
    operator.le : '<=',
    operator.ne : '!=',
    operator.gt : '>',
    operator.ge : '>=',
    operator.eq : '=',
    sql.ColumnOperators.distinct_op : 'DISTINCT',
    sql.ColumnOperators.concat_op : '||',
    sql.ColumnOperators.like_op : 'LIKE',
    sql.ColumnOperators.notlike_op : 'NOT LIKE',
    sql.ColumnOperators.ilike_op : 'ILIKE',
    sql.ColumnOperators.notilike_op : 'NOT ILIKE',
    sql.ColumnOperators.between_op : 'BETWEEN',
    sql.ColumnOperators.in_op : 'IN',
    sql.ColumnOperators.notin_op : 'NOT IN',
    sql.ColumnOperators.comma_op : ', ',
    sql.ColumnOperators.desc_op : 'DESC',
    sql.ColumnOperators.asc_op : 'ASC',
    
    sql.Operators.from_ : 'FROM',
    sql.Operators.as_ : 'AS',
    sql.Operators.exists : 'EXISTS',
    sql.Operators.is_ : 'IS',
    sql.Operators.isnot : 'IS NOT'
}

class ANSIDialect(default.DefaultDialect):
    def __init__(self, cache_identifiers=True, **kwargs):
        super(ANSIDialect,self).__init__(**kwargs)
        self.identifier_preparer = self.preparer()
        self.cache_identifiers = cache_identifiers

    def create_connect_args(self):
        return ([],{})

    def schemagenerator(self, *args, **kwargs):
        return ANSISchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return ANSISchemaDropper(self, *args, **kwargs)

    def compiler(self, statement, parameters, **kwargs):
        return ANSICompiler(self, statement, parameters, **kwargs)

    def preparer(self):
        """Return an IdentifierPreparer.

        This object is used to format table and column names including
        proper quoting and case conventions.
        """
        return ANSIIdentifierPreparer(self)

class ANSICompiler(engine.Compiled, sql.ClauseVisitor):
    """Default implementation of Compiled.

    Compiles ClauseElements into ANSI-compliant SQL strings.
    """

    __traverse_options__ = {'column_collections':False, 'entry':True}

    operators = OPERATORS
    
    def __init__(self, dialect, statement, parameters=None, **kwargs):
        """Construct a new ``ANSICompiler`` object.

        dialect
          Dialect to be used

        statement
          ClauseElement to be compiled

        parameters
          optional dictionary indicating a set of bind parameters
          specified with this Compiled object.  These parameters are
          the *default* key/value pairs when the Compiled is executed,
          and also may affect the actual compilation, as in the case
          of an INSERT where the actual columns inserted will
          correspond to the keys present in the parameters.
        """
        
        super(ANSICompiler, self).__init__(dialect, statement, parameters, **kwargs)

        # if we are insert/update.  set to true when we visit an INSERT or UPDATE
        self.isinsert = self.isupdate = False
        
        # a dictionary of bind parameter keys to _BindParamClause instances.
        self.binds = {}
        
        # a dictionary of _BindParamClause instances to "compiled" names that are
        # actually present in the generated SQL
        self.bind_names = {}

        # a stack.  what recursive compiler doesn't have a stack ? :)
        self.stack = []
        
        # a dictionary of result-set column names (strings) to TypeEngine instances,
        # which will be passed to a ResultProxy and used for resultset-level value conversion
        self.typemap = {}

        # a dictionary of select columns labels mapped to their "generated" label
        self.column_labels = {}

        # a dictionary of ClauseElement subclasses to counters, which are used to
        # generate truncated identifier names or "anonymous" identifiers such as
        # for aliases
        self.generated_ids = {}
        
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
        text = self.string
        if ':' not in text:
            return
        
        if self.paramstyle=='pyformat':
            text = BIND_PARAMS.sub(lambda m:'%(' + m.group(1) +')s', text)
        elif self.positional:
            params = BIND_PARAMS.finditer(text)
            for p in params:
                self.positiontup.append(p.group(1))
            if self.paramstyle=='qmark':
                text = BIND_PARAMS.sub('?', text)
            elif self.paramstyle=='format':
                text = BIND_PARAMS.sub('%s', text)
            elif self.paramstyle=='numeric':
                i = [0]
                def getnum(x):
                    i[0] += 1
                    return str(i[0])
                text = BIND_PARAMS.sub(getnum, text)
        # un-escape any \:params
        text = BIND_PARAMS_ESC.sub(lambda m: m.group(1), text)
        self.string = text

    def compile(self):
        self.string = self.process(self.statement)
        self.after_compile()
    
    def process(self, obj, stack=None, **kwargs):
        if stack:
            self.stack.append(stack)
        try:
            return self.traverse_single(obj, **kwargs)
        finally:
            if stack:
                self.stack.pop(-1)
        
    def is_subquery(self, select):
        return self.stack and self.stack[-1].get('is_subquery')
        
    def get_whereclause(self, obj):
        """given a FROM clause, return an additional WHERE condition that should be 
        applied to a SELECT. 
        
        Currently used by Oracle to provide WHERE criterion for JOIN and OUTER JOIN
        constructs in non-ansi mode.
        """
        
        return None

    def construct_params(self, params):
        """Return a sql.ClauseParameters object.
        
        Combines the given bind parameter dictionary (string keys to object values)
        with the _BindParamClause objects stored within this Compiled object
        to produce a ClauseParameters structure, representing the bind arguments
        for a single statement execution, or one element of an executemany execution.
        """
        
        if self.parameters is not None:
            bindparams = self.parameters.copy()
        else:
            bindparams = {}
        bindparams.update(params)
        d = sql.ClauseParameters(self.dialect, self.positiontup)
        for b in self.binds.values():
            name = self.bind_names[b]
            d.set_parameter(b, b.value, name)

        for key, value in bindparams.iteritems():
            try:
                b = self.binds[key]
            except KeyError:
                continue
            name = self.bind_names[b]
            d.set_parameter(b, value, name)

        return d

    params = property(lambda self:self.construct_params({}), doc="""Return the `ClauseParameters` corresponding to this compiled object.  
        A shortcut for `construct_params()`.""")
        
    def default_from(self):
        """Called when a SELECT statement has no froms, and no FROM clause is to be appended.

        Gives Oracle a chance to tack on a ``FROM DUAL`` to the string output.
        """

        return ""
    
    def visit_grouping(self, grouping, **kwargs):
        return "(" + self.process(grouping.elem) + ")"
        
    def visit_label(self, label):
        labelname = self._truncated_identifier("colident", label.name)
        
        if self.stack and self.stack[-1].get('select'):
            self.typemap.setdefault(labelname.lower(), label.obj.type)
            if isinstance(label.obj, sql._ColumnClause):
                self.column_labels[label.obj._label] = labelname
            self.column_labels[label.name] = labelname
        return " ".join([self.process(label.obj), self.operator_string(sql.ColumnOperators.as_), self.preparer.format_label(label, labelname)])
        
    def visit_column(self, column, **kwargs):
        # there is actually somewhat of a ruleset when you would *not* necessarily
        # want to truncate a column identifier, if its mapped to the name of a 
        # physical column.  but thats very hard to identify at this point, and 
        # the identifier length should be greater than the id lengths of any physical
        # columns so should not matter.
        if not column.is_literal:
            name = self._truncated_identifier("colident", column.name)
        else:
            name = column.name

        if self.stack and self.stack[-1].get('select'):
            # if we are within a visit to a Select, set up the "typemap"
            # for this column which is used to translate result set values
            self.typemap.setdefault(name.lower(), column.type)
            self.column_labels.setdefault(column._label, name.lower())

        if column.table is None or not column.table.named_with_column():
            return self.preparer.format_column(column, name=name)
        else:
            if column.table.oid_column is column:
                n = self.dialect.oid_column_name(column)
                if n is not None:
                    return "%s.%s" % (self.preparer.format_table(column.table, use_schema=False, name=self._anonymize(column.table.name)), n)
                elif len(column.table.primary_key) != 0:
                    pk = list(column.table.primary_key)[0]
                    pkname = (pk.is_literal and name or self._truncated_identifier("colident", pk.name))
                    return self.preparer.format_column_with_table(list(column.table.primary_key)[0], column_name=pkname, table_name=self._anonymize(column.table.name))
                else:
                    return None
            else:
                return self.preparer.format_column_with_table(column, column_name=name, table_name=self._anonymize(column.table.name))


    def visit_fromclause(self, fromclause, **kwargs):
        return fromclause.name

    def visit_index(self, index, **kwargs):
        return index.name

    def visit_typeclause(self, typeclause, **kwargs):
        return typeclause.type.dialect_impl(self.dialect).get_col_spec()

    def visit_textclause(self, textclause, **kwargs):
        for bind in textclause.bindparams.values():
            self.process(bind)
        if textclause.typemap is not None:
            self.typemap.update(textclause.typemap)
        return textclause.text

    def visit_null(self, null, **kwargs):
        return 'NULL'

    def visit_clauselist(self, clauselist, **kwargs):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        elif sep == sql.ColumnOperators.comma_op:
            sep = ', '
        else:
            sep = " " + self.operator_string(clauselist.operator) + " "
        return string.join([s for s in [self.process(c) for c in clauselist.clauses] if s is not None], sep)

    def apply_function_parens(self, func):
        return func.name.upper() not in ANSI_FUNCS or len(func.clauses) > 0

    def visit_calculatedclause(self, clause, **kwargs):
        return self.process(clause.clause_expr)

    def visit_cast(self, cast, **kwargs):
        if self.stack and self.stack[-1].get('select'):
            # not sure if we want to set the typemap here...
            self.typemap.setdefault("CAST", cast.type)
        return "CAST(%s AS %s)" % (self.process(cast.clause), self.process(cast.typeclause))

    def visit_function(self, func, **kwargs):
        if self.stack and self.stack[-1].get('select'):
            self.typemap.setdefault(func.name, func.type)
        if not self.apply_function_parens(func):
            return ".".join(func.packagenames + [func.name])
        else:
            return ".".join(func.packagenames + [func.name]) + (not func.group and " " or "") + self.process(func.clause_expr)

    def visit_compound_select(self, cs, asfrom=False, parens=True, **kwargs):
        stack_entry = {'select':cs}
        
        if asfrom:
            stack_entry['is_selected_from'] = stack_entry['is_subquery'] = True
        elif self.stack and self.stack[-1].get('select'):
            stack_entry['is_subquery'] = True
        self.stack.append(stack_entry)
        
        text = string.join([self.process(c, asfrom=asfrom, parens=False) for c in cs.selects], " " + cs.keyword + " ")
        group_by = self.process(cs._group_by_clause, asfrom=asfrom)
        if group_by:
            text += " GROUP BY " + group_by

        text += self.order_by_clause(cs)            
        text += (cs._limit or cs._offset) and self.limit_clause(cs) or ""
        
        self.stack.pop(-1)
        
        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def visit_unary(self, unary, **kwargs):
        s = self.process(unary.element)
        if unary.operator:
            s = self.operator_string(unary.operator) + " " + s
        if unary.modifier:
            s = s + " " + self.operator_string(unary.modifier)
        return s
        
    def visit_binary(self, binary, **kwargs):
        op = self.operator_string(binary.operator)
        if callable(op):
            return op(self.process(binary.left), self.process(binary.right))
        else:
            return self.process(binary.left) + " " + op + " " + self.process(binary.right)
        
    def operator_string(self, operator):
        return self.operators.get(operator, str(operator))

    def visit_bindparam(self, bindparam, **kwargs):
        # apply truncation to the ultimate generated name

        if bindparam.shortname != bindparam.key:
            self.binds.setdefault(bindparam.shortname, bindparam)

        if bindparam.unique:
            count = 1
            key = bindparam.key
            # redefine the generated name of the bind param in the case
            # that we have multiple conflicting bind parameters.
            while self.binds.setdefault(key, bindparam) is not bindparam:
                tag = "_%d" % count
                key = bindparam.key + tag
                count += 1
            bindparam.key = key
            return self.bindparam_string(self._truncate_bindparam(bindparam))
        else:
            existing = self.binds.get(bindparam.key)
            if existing is not None and existing.unique:
                raise exceptions.CompileError("Bind parameter '%s' conflicts with unique bind parameter of the same name" % bindparam.key)
            self.binds[bindparam.key] = bindparam
            return self.bindparam_string(self._truncate_bindparam(bindparam))
    
    def _truncate_bindparam(self, bindparam):
        if bindparam in self.bind_names:
            return self.bind_names[bindparam]
            
        bind_name = bindparam.key
        bind_name = self._truncated_identifier("bindparam", bind_name)
        # add to bind_names for translation
        self.bind_names[bindparam] = bind_name
            
        return bind_name
    
    def _truncated_identifier(self, ident_class, name):
        if (ident_class, name) in self.generated_ids:
            return self.generated_ids[(ident_class, name)]
        
        anonname = ANONYMOUS_LABEL.sub(self._process_anon, name)

        if len(anonname) > self.dialect.max_identifier_length():
            counter = self.generated_ids.get(ident_class, 1)
            truncname = name[0:self.dialect.max_identifier_length() - 6] + "_" + hex(counter)[2:]
            self.generated_ids[ident_class] = counter + 1
        else:
            truncname = anonname
        self.generated_ids[(ident_class, name)] = truncname
        return truncname

    def _process_anon(self, match):
        (ident, derived) = match.group(1,2)
        if ('anonymous', ident) in self.generated_ids:
            return self.generated_ids[('anonymous', ident)]
        else:
            anonymous_counter = self.generated_ids.get('anonymous', 1)
            newname = derived + "_" + str(anonymous_counter)
            self.generated_ids['anonymous'] = anonymous_counter + 1
            self.generated_ids[('anonymous', ident)] = newname
            return newname
    
    def _anonymize(self, name):
        return ANONYMOUS_LABEL.sub(self._process_anon, name)
            
    def bindparam_string(self, name):
        return self.bindtemplate % name

    def visit_alias(self, alias, asfrom=False, **kwargs):
        if asfrom:
            return self.process(alias.original, asfrom=True, **kwargs) + " AS " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    def label_select_column(self, select, column):
        """convert a column from a select's "columns" clause.
        
        given a select() and a column element from its inner_columns collection, return a
        Label object if this column should be labeled in the columns clause.  Otherwise,
        return None and the column will be used as-is.
        
        The calling method will traverse the returned label to acquire its string
        representation.
        """
        
        # SQLite doesnt like selecting from a subquery where the column
        # names look like table.colname. so if column is in a "selected from"
        # subquery, label it synoymously with its column name
        if \
            (self.stack and self.stack[-1].get('is_selected_from')) and \
            isinstance(column, sql._ColumnClause) and \
            not column.is_literal and \
            column.table is not None and \
            not isinstance(column.table, sql.Select):
            return column.label(column.name)
        else:
            return None

    def visit_select(self, select, asfrom=False, parens=True, **kwargs):

        stack_entry = {'select':select}
        
        if asfrom:
            stack_entry['is_selected_from'] = stack_entry['is_subquery'] = True
        elif self.stack and self.stack[-1].get('select'):
            stack_entry['is_subquery'] = True

        if self.stack and self.stack[-1].get('from'):
            existingfroms = self.stack[-1]['from']
        else:
            existingfroms = None
        froms = select._get_display_froms(existingfroms)

        correlate_froms = util.Set()
        for f in froms:
            correlate_froms.add(f)
            for f2 in f._get_from_objects():
                correlate_froms.add(f2)

        # TODO: might want to propigate existing froms for select(select(select))
        # where innermost select should correlate to outermost
#        if existingfroms:
#            correlate_froms = correlate_froms.union(existingfroms)    
        stack_entry['from'] = correlate_froms
        self.stack.append(stack_entry)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = util.OrderedSet()
                
        for co in select.inner_columns:
            if select.use_labels:
                labelname = co._label
                if labelname is not None:
                    l = co.label(labelname)
                    inner_columns.add(self.process(l))
                else:
                    self.traverse(co)
                    inner_columns.add(self.process(co))
            else:
                l = self.label_select_column(select, co)
                if l is not None:
                    inner_columns.add(self.process(l))
                else:
                    inner_columns.add(self.process(co))
            
        collist = string.join(inner_columns.difference(util.Set([None])), ', ')

        text = " ".join(["SELECT"] + [self.process(x) for x in select._prefixes]) + " "
        text += self.get_select_precolumns(select)
        text += collist

        whereclause = select._whereclause

        from_strings = []
        for f in froms:
            from_strings.append(self.process(f, asfrom=True))

            w = self.get_whereclause(f)
            if w is not None:
                if whereclause is not None:
                    whereclause = sql.and_(w, whereclause)
                else:
                    whereclause = w

        if froms:
            text += " \nFROM "
            text += string.join(from_strings, ', ')
        else:
            text += self.default_from()

        if whereclause is not None:
            t = self.process(whereclause)
            if t:
                text += " \nWHERE " + t

        group_by = self.process(select._group_by_clause)
        if group_by:
            text += " GROUP BY " + group_by

        if select._having is not None:
            t = self.process(select._having)
            if t:
                text += " \nHAVING " + t
        
        text += self.order_by_clause(select)
        text += (select._limit or select._offset) and self.limit_clause(select) or ""
        text += self.for_update_clause(select)

        self.stack.pop(-1)

        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def get_select_precolumns(self, select):
        """Called when building a ``SELECT`` statement, position is just before column list."""
        return select._distinct and "DISTINCT " or ""

    def order_by_clause(self, select):
        order_by = self.process(select._order_by_clause)
        if order_by:
            return " ORDER BY " + order_by
        else:
            return ""

    def for_update_clause(self, select):
        if select.for_update:
            return " FOR UPDATE"
        else:
            return ""

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select._offset)
        return text

    def visit_table(self, table, asfrom=False, **kwargs):
        if asfrom:
            return self.preparer.format_table(table)
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return (self.process(join.left, asfrom=True) + (join.isouter and " LEFT OUTER JOIN " or " JOIN ") + \
            self.process(join.right, asfrom=True) + " ON " + self.process(join.onclause))

    def uses_sequences_for_inserts(self):
        return False
        
    def visit_insert(self, insert_stmt):

        # search for columns who will be required to have an explicit bound value.
        # for inserts, this includes Python-side defaults, columns with sequences for dialects
        # that support sequences, and primary key columns for dialects that explicitly insert
        # pre-generated primary key values
        required_cols = util.Set()
        class DefaultVisitor(schema.SchemaVisitor):
            def visit_column(s, cd):
                if c.primary_key and self.uses_sequences_for_inserts():
                    required_cols.add(c)
            def visit_column_default(s, cd):
                required_cols.add(c)
            def visit_sequence(s, seq):
                if self.uses_sequences_for_inserts():
                    required_cols.add(c)
        vis = DefaultVisitor()
        for c in insert_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                vis.traverse(c)

        self.isinsert = True
        colparams = self._get_colparams(insert_stmt, required_cols)

        return ("INSERT INTO " + self.preparer.format_table(insert_stmt.table) + " (" + string.join([self.preparer.format_column(c[0]) for c in colparams], ', ') + ")" +
         " VALUES (" + string.join([c[1] for c in colparams], ', ') + ")")

    def visit_update(self, update_stmt):
        self.stack.append({'from':util.Set([update_stmt.table])})
        
        # search for columns who will be required to have an explicit bound value.
        # for updates, this includes Python-side "onupdate" defaults.
        required_cols = util.Set()
        class OnUpdateVisitor(schema.SchemaVisitor):
            def visit_column_onupdate(s, cd):
                required_cols.add(c)
        vis = OnUpdateVisitor()
        for c in update_stmt.table.c:
            if (isinstance(c, schema.SchemaItem) and (self.parameters is None or self.parameters.get(c.key, None) is None)):
                vis.traverse(c)

        self.isupdate = True
        colparams = self._get_colparams(update_stmt, required_cols)

        text = "UPDATE " + self.preparer.format_table(update_stmt.table) + " SET " + string.join(["%s=%s" % (self.preparer.format_column(c[0]), c[1]) for c in colparams], ', ')

        if update_stmt._whereclause:
            text += " WHERE " + self.process(update_stmt._whereclause)
        
        self.stack.pop(-1)
        
        return text

    def _get_colparams(self, stmt, required_cols):
        """create a set of tuples representing column/string pairs for use 
        in an INSERT or UPDATE statement.
        
        This method may generate new bind params within this compiled
        based on the given set of "required columns", which are required
        to have a value set in the statement.
        """

        def create_bind_param(col, value):
            bindparam = sql.bindparam(col.key, value, type_=col.type, unique=True)
            self.binds[col.key] = bindparam
            return self.bindparam_string(self._truncate_bindparam(bindparam))

        # no parameters in the statement, no parameters in the
        # compiled params - return binds for all columns
        if self.parameters is None and stmt.parameters is None:
            return [(c, create_bind_param(c, None)) for c in stmt.table.columns]

        def create_clause_param(col, value):
            self.traverse(value)
            self.inline_params.add(col)
            return self.process(value)

        self.inline_params = util.Set()

        def to_col(key):
            if not isinstance(key, sql._ColumnClause):
                return stmt.table.columns.get(unicode(key), key)
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

        for col in required_cols:
            parameters.setdefault(col, None)

        # create a list of column assignment clauses as tuples
        values = []
        for c in stmt.table.columns:
            if c in parameters:
                value = parameters[c]
                if sql._is_literal(value):
                    value = create_bind_param(c, value)
                else:
                    value = create_clause_param(c, value)
                values.append((c, value))
        
        return values

    def visit_delete(self, delete_stmt):
        self.stack.append({'from':util.Set([delete_stmt.table])})

        text = "DELETE FROM " + self.preparer.format_table(delete_stmt.table)

        if delete_stmt._whereclause:
            text += " WHERE " + self.process(delete_stmt._whereclause)

        self.stack.pop(-1)
        
        return text
        
    def visit_savepoint(self, savepoint_stmt):
        return "SAVEPOINT %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TO SAVEPOINT %s" % self.preparer.format_savepoint(savepoint_stmt)
    
    def visit_release_savepoint(self, savepoint_stmt):
        return "RELEASE SAVEPOINT %s" % self.preparer.format_savepoint(savepoint_stmt)
    
    def __str__(self):
        return self.string

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
                findalterables.traverse(c)
        return alterables

class ANSISchemaGenerator(ANSISchemaBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(ANSISchemaGenerator, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables and util.Set(tables) or None
        self.preparer = dialect.preparer()
        self.dialect = dialect

    def get_column_specification(self, column, first_pk=False):
        raise NotImplementedError()

    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=False, tables=self.tables) if (not self.checkfirst or not self.dialect.has_table(self.connection, t.name, schema=t.schema))]
        for table in collection:
            self.traverse_single(table)
        if self.dialect.supports_alter():
            for alterable in self.find_alterables(collection):
                self.add_foreignkey(alterable)

    def visit_table(self, table):
        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

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
                self.traverse_single(constraint)

        # On some DB order is significant: visit PK first, then the
        # other constraints (engine.ReflectionTest.testbasic failed on FB2)
        if table.primary_key:
            self.traverse_single(table.primary_key)
        for constraint in [c for c in table.constraints if c is not table.primary_key]:
            self.traverse_single(constraint)

        self.append("\n)%s\n\n" % self.post_create_table(table))
        self.execute()
        if hasattr(table, 'indexes'):
            for index in table.indexes:
                self.traverse_single(index)

    def post_create_table(self, table):
        return ''

    def get_column_default_string(self, column):
        if isinstance(column.default, schema.PassiveDefault):
            if isinstance(column.default.arg, basestring):
                return "'%s'" % column.default.arg
            else:
                return unicode(self._compile(column.default.arg, None))
        else:
            return None

    def _compile(self, tocompile, parameters):
        """compile the given string/parameters using this SchemaGenerator's dialect."""
        compiler = self.dialect.compiler(tocompile, parameters)
        compiler.compile()
        return compiler

    def visit_check_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " %
                        self.preparer.format_constraint(constraint))
        self.append(" CHECK (%s)" % constraint.sqltext)

    def visit_column_check_constraint(self, constraint):
        self.append(" CHECK (%s)" % constraint.sqltext)

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " % self.preparer.format_constraint(constraint))
        self.append("PRIMARY KEY ")
        self.append("(%s)" % ', '.join([self.preparer.format_column(c) for c in constraint]))

    def visit_foreign_key_constraint(self, constraint):
        if constraint.use_alter and self.dialect.supports_alter():
            return
        self.append(", \n\t ")
        self.define_foreign_key(constraint)

    def add_foreignkey(self, constraint):
        self.append("ALTER TABLE %s ADD " % self.preparer.format_table(constraint.table))
        self.define_foreign_key(constraint)
        self.execute()

    def define_foreign_key(self, constraint):
        preparer = self.preparer
        if constraint.name is not None:
            self.append("CONSTRAINT %s " %
                        preparer.format_constraint(constraint))
        self.append("FOREIGN KEY(%s) REFERENCES %s (%s)" % (
            ', '.join([preparer.format_column(f.parent) for f in constraint.elements]),
            preparer.format_table(list(constraint.elements)[0].column.table),
            ', '.join([preparer.format_column(f.column) for f in constraint.elements])
        ))
        if constraint.ondelete is not None:
            self.append(" ON DELETE %s" % constraint.ondelete)
        if constraint.onupdate is not None:
            self.append(" ON UPDATE %s" % constraint.onupdate)

    def visit_unique_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " %
                        self.preparer.format_constraint(constraint))
        self.append(" UNIQUE (%s)" % (', '.join([self.preparer.format_column(c) for c in constraint])))

    def visit_column(self, column):
        pass

    def visit_index(self, index):
        preparer = self.preparer
        self.append("CREATE ")
        if index.unique:
            self.append("UNIQUE ")
        self.append("INDEX %s ON %s (%s)" \
                    % (preparer.format_index(index),
                       preparer.format_table(index.table),
                       string.join([preparer.format_column(c) for c in index.columns], ', ')))
        self.execute()

class ANSISchemaDropper(ANSISchemaBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(ANSISchemaDropper, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.preparer()
        self.dialect = dialect

    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=True, tables=self.tables) if (not self.checkfirst or  self.dialect.has_table(self.connection, t.name, schema=t.schema))]
        if self.dialect.supports_alter():
            for alterable in self.find_alterables(collection):
                self.drop_foreignkey(alterable)
        for table in collection:
            self.traverse_single(table)

    def visit_index(self, index):
        self.append("\nDROP INDEX " + self.preparer.format_index(index))
        self.execute()

    def drop_foreignkey(self, constraint):
        self.append("ALTER TABLE %s DROP CONSTRAINT %s" % (
            self.preparer.format_table(constraint.table),
            self.preparer.format_constraint(constraint)))
        self.execute()

    def visit_table(self, table):
        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.append("\nDROP TABLE " + self.preparer.format_table(table))
        self.execute()

class ANSIDefaultRunner(engine.DefaultRunner):
    pass

class ANSIIdentifierPreparer(object):
    """Handle quoting and case-folding of identifiers based on options."""

    def __init__(self, dialect, initial_quote='"', final_quote=None, omit_schema=False):
        """Construct a new ``ANSIIdentifierPreparer`` object.

        initial_quote
          Character that begins a delimited identifier.

        final_quote
          Character that ends a delimited identifier. Defaults to `initial_quote`.

        omit_schema
          Prevent prepending schema name. Useful for databases that do
          not support schemae.
        """

        self.dialect = dialect
        self.initial_quote = initial_quote
        self.final_quote = final_quote or self.initial_quote
        self.omit_schema = omit_schema
        self.__strings = {}

    def _escape_identifier(self, value):
        """Escape an identifier.

        Subclasses should override this to provide database-dependent
        escaping behavior.
        """

        return value.replace('"', '""')

    def _unescape_identifier(self, value):
        """Canonicalize an escaped identifier.

        Subclasses should override this to provide database-dependent
        unescaping behavior that reverses _escape_identifier.
        """

        return value.replace('""', '"')

    def quote_identifier(self, value):
        """Quote an identifier.

        Subclasses should override this to provide database-dependent
        quoting behavior.
        """

        return self.initial_quote + self._escape_identifier(value) + self.final_quote

    def _fold_identifier_case(self, value):
        """Fold the case of an identifier.

        Subclasses should override this to provide database-dependent
        case folding behavior.
        """

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

    def _requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        return \
            value in self._reserved_words() \
            or (value[0] in self._illegal_initial_characters()) \
            or bool(len([x for x in unicode(value) if x not in self._legal_characters()])) \
            or (value.lower() != value)

    def __generic_obj_format(self, obj, ident):
        if getattr(obj, 'quote', False):
            return self.quote_identifier(ident)
        if self.dialect.cache_identifiers:
            try:
                return self.__strings[ident]
            except KeyError:
                if self._requires_quotes(ident):
                    self.__strings[ident] = self.quote_identifier(ident)
                else:
                    self.__strings[ident] = ident
                return self.__strings[ident]
        else:
            if self._requires_quotes(ident):
                return self.quote_identifier(ident)
            else:
                return ident

    def should_quote(self, object):
        return object.quote or self._requires_quotes(object.name)

    def format_sequence(self, sequence):
        return self.__generic_obj_format(sequence, sequence.name)

    def format_label(self, label, name=None):
        return self.__generic_obj_format(label, name or label.name)

    def format_alias(self, alias, name=None):
        return self.__generic_obj_format(alias, name or alias.name)

    def format_savepoint(self, savepoint, name=None):
        return self.__generic_obj_format(savepoint, name or savepoint.ident)

    def format_constraint(self, constraint):
        return self.__generic_obj_format(constraint, constraint.name)

    def format_index(self, index):
        return self.__generic_obj_format(index, index.name)

    def format_table(self, table, use_schema=True, name=None):
        """Prepare a quoted table and schema name."""

        if name is None:
            name = table.name
        result = self.__generic_obj_format(table, name)
        if use_schema and getattr(table, "schema", None):
            result = self.__generic_obj_format(table, table.schema) + "." + result
        return result

    def format_column(self, column, use_table=False, name=None, table_name=None):
        """Prepare a quoted column name."""
        if name is None:
            name = column.name
        if not getattr(column, 'is_literal', False):
            if use_table:
                return self.format_table(column.table, use_schema=False, name=table_name) + "." + self.__generic_obj_format(column, name)
            else:
                return self.__generic_obj_format(column, name)
        else:
            # literal textual elements get stuck into ColumnClause alot, which shouldnt get quoted
            if use_table:
                return self.format_table(column.table, use_schema=False, name=table_name) + "." + name
            else:
                return name

    def format_column_with_table(self, column, column_name=None, table_name=None):
        """Prepare a quoted column name with table name."""
        
        return self.format_column(column, use_table=True, name=column_name, table_name=table_name)


    def format_table_seq(self, table, use_schema=True):
        """Format table name and schema as a tuple."""

        # Dialects with more levels in their fully qualified references
        # ('database', 'owner', etc.) could override this and return
        # a longer sequence.

        if use_schema and getattr(table, 'schema', None):
            return (self.quote_identifier(table.schema),
                    self.format_table(table, use_schema=False))
        else:
            return (self.format_table(table, use_schema=False), )

    def unformat_identifiers(self, identifiers):
        """Unpack 'schema.table.column'-like strings into components."""

        try:
            r = self._r_identifiers
        except AttributeError:
            initial, final, escaped_final = \
                     [re.escape(s) for s in
                      (self.initial_quote, self.final_quote,
                       self._escape_identifier(self.final_quote))]
            r = re.compile(
                r'(?:'
                r'(?:%(initial)s((?:%(escaped)s|[^%(final)s])+)%(final)s'
                r'|([^\.]+))(?=\.|$))+' %
                { 'initial': initial,
                  'final': final,
                  'escaped': escaped_final })
            self._r_identifiers = r
        
        return [self._unescape_identifier(i)
                for i in [a or b for a, b in r.findall(identifiers)]]


dialect = ANSIDialect
