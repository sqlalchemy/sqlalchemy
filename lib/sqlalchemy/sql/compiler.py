# compiler.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base SQL and DDL compiler implementations.

Provides the [sqlalchemy.sql.compiler#DefaultCompiler] class, which is
responsible for generating all SQL query strings, as well as
[sqlalchemy.sql.compiler#SchemaGenerator] and [sqlalchemy.sql.compiler#SchemaDropper]
which issue CREATE and DROP DDL for tables, sequences, and indexes.

The elements in this module are used by public-facing constructs like
[sqlalchemy.sql.expression#ClauseElement] and [sqlalchemy.engine#Engine].
While dialect authors will want to be familiar with this module for the purpose of
creating database-specific compilers and schema generators, the module
is otherwise internal to SQLAlchemy.
"""

import string, re, itertools
from sqlalchemy import schema, engine, util, exceptions
from sqlalchemy.sql import operators, functions
from sqlalchemy.sql import expression as sql

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

LEGAL_CHARACTERS = re.compile(r'^[A-Z0-9_$]+$', re.I)
ILLEGAL_INITIAL_CHARACTERS = re.compile(r'[0-9$]')

BIND_PARAMS = re.compile(r'(?<![:\w\$\x5c]):([\w\$]+)(?![:\w\$])', re.UNICODE)
BIND_PARAMS_ESC = re.compile(r'\x5c(:[\w\$]+)(?![:\w\$])', re.UNICODE)
ANONYMOUS_LABEL = re.compile(r'{ANON (-?\d+) ([^{}]+)}')

BIND_TEMPLATES = {
    'pyformat':"%%(%(name)s)s",
    'qmark':"?",
    'format':"%%s",
    'numeric':"%(position)s",
    'named':":%(name)s"
}


OPERATORS =  {
    operators.and_ : 'AND',
    operators.or_ : 'OR',
    operators.inv : 'NOT',
    operators.add : '+',
    operators.mul : '*',
    operators.sub : '-',
    operators.div : '/',
    operators.mod : '%',
    operators.truediv : '/',
    operators.lt : '<',
    operators.le : '<=',
    operators.ne : '!=',
    operators.gt : '>',
    operators.ge : '>=',
    operators.eq : '=',
    operators.distinct_op : 'DISTINCT',
    operators.concat_op : '||',
    operators.like_op : lambda x, y, escape=None: '%s LIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
    operators.notlike_op : lambda x, y, escape=None: '%s NOT LIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
    operators.ilike_op : lambda x, y, escape=None: "lower(%s) LIKE lower(%s)" % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
    operators.notilike_op : lambda x, y, escape=None: "lower(%s) NOT LIKE lower(%s)" % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
    operators.between_op : 'BETWEEN',
    operators.in_op : 'IN',
    operators.notin_op : 'NOT IN',
    operators.comma_op : ', ',
    operators.desc_op : 'DESC',
    operators.asc_op : 'ASC',
    operators.from_ : 'FROM',
    operators.as_ : 'AS',
    operators.exists : 'EXISTS',
    operators.is_ : 'IS',
    operators.isnot : 'IS NOT',
    operators.collate : 'COLLATE',
}

FUNCTIONS = {
    functions.coalesce : 'coalesce%(expr)s',
    functions.current_date: 'CURRENT_DATE',
    functions.current_time: 'CURRENT_TIME',
    functions.current_timestamp: 'CURRENT_TIMESTAMP',
    functions.current_user: 'CURRENT_USER',
    functions.localtime: 'LOCALTIME',
    functions.localtimestamp: 'LOCALTIMESTAMP',
    functions.random: 'random%(expr)s',
    functions.sysdate: 'sysdate',
    functions.session_user :'SESSION_USER',
    functions.user: 'USER'
}

class DefaultCompiler(engine.Compiled):
    """Default implementation of Compiled.

    Compiles ClauseElements into SQL strings.   Uses a similar visit
    paradigm as visitors.ClauseVisitor but implements its own traversal.
    """

    __traverse_options__ = {'column_collections':False, 'entry':True}

    operators = OPERATORS
    functions = FUNCTIONS

    def __init__(self, dialect, statement, column_keys=None, inline=False, **kwargs):
        """Construct a new ``DefaultCompiler`` object.

        dialect
          Dialect to be used

        statement
          ClauseElement to be compiled

        column_keys
          a list of column names to be compiled into an INSERT or UPDATE
          statement.
        """

        super(DefaultCompiler, self).__init__(dialect, statement, column_keys, **kwargs)

        # if we are insert/update/delete.  set to true when we visit an INSERT, UPDATE or DELETE
        self.isdelete = self.isinsert = self.isupdate = False

        # compile INSERT/UPDATE defaults/sequences inlined (no pre-execute)
        self.inline = inline or getattr(statement, 'inline', False)

        # a dictionary of bind parameter keys to _BindParamClause instances.
        self.binds = {}

        # a dictionary of _BindParamClause instances to "compiled" names that are
        # actually present in the generated SQL
        self.bind_names = {}

        # a stack.  what recursive compiler doesn't have a stack ? :)
        self.stack = []

        # relates label names in the final SQL to
        # a tuple of local column/label name, ColumnElement object (if any) and TypeEngine.
        # ResultProxy uses this for type processing and column targeting
        self.result_map = {}

        # a dictionary of ClauseElement subclasses to counters, which are used to
        # generate truncated identifier names or "anonymous" identifiers such as
        # for aliases
        self.generated_ids = {}

        # paramstyle from the dialect (comes from DB-API)
        self.paramstyle = self.dialect.paramstyle

        # true if the paramstyle is positional
        self.positional = self.dialect.positional

        self.bindtemplate = BIND_TEMPLATES[self.paramstyle]

        # a list of the compiled's bind parameter names, used to help
        # formulate a positional argument list
        self.positiontup = []

        # an IdentifierPreparer that formats the quoting of identifiers
        self.preparer = self.dialect.identifier_preparer

    def compile(self):
        self.string = self.process(self.statement)

    def process(self, obj, stack=None, **kwargs):
        if stack:
            self.stack.append(stack)
        try:
            meth = getattr(self, "visit_%s" % obj.__visit_name__, None)
            if meth:
                return meth(obj, **kwargs)
        finally:
            if stack:
                self.stack.pop(-1)

    def is_subquery(self, select):
        return self.stack and self.stack[-1].get('is_subquery')

    def construct_params(self, params=None):
        """return a dictionary of bind parameter keys and values"""

        if params:
            pd = {}
            for bindparam, name in self.bind_names.iteritems():
                for paramname in (bindparam, bindparam.key, bindparam.shortname, name):
                    if paramname in params:
                        pd[name] = params[paramname]
                        break
                else:
                    if callable(bindparam.value):
                        pd[name] = bindparam.value()
                    else:
                        pd[name] = bindparam.value
            return pd
        else:
            pd = {}
            for bindparam in self.bind_names:
                if callable(bindparam.value):
                    pd[self.bind_names[bindparam]] = bindparam.value()
                else:
                    pd[self.bind_names[bindparam]] = bindparam.value
            return pd

    params = property(construct_params)

    def default_from(self):
        """Called when a SELECT statement has no froms, and no FROM clause is to be appended.

        Gives Oracle a chance to tack on a ``FROM DUAL`` to the string output.
        """

        return ""

    def visit_grouping(self, grouping, **kwargs):
        return "(" + self.process(grouping.elem) + ")"

    def visit_label(self, label, result_map=None):
        labelname = self._truncated_identifier("colident", label.name)

        if result_map is not None:
            result_map[labelname.lower()] = (label.name, (label, label.obj, labelname), label.obj.type)

        return " ".join([self.process(label.obj), self.operator_string(operators.as_), self.preparer.format_label(label, labelname)])

    def visit_column(self, column, result_map=None, **kwargs):

        if column._is_oid:
            name = self.dialect.oid_column_name(column)
            if name is None:
                if len(column.table.primary_key) != 0:
                    pk = list(column.table.primary_key)[0]
                    return self.visit_column(pk, result_map=result_map, **kwargs)
                else:
                    return None
        elif not column.is_literal:
            name = self._truncated_identifier("colident", column.name)
        else:
            name = column.name

        if result_map is not None:
            result_map[name.lower()] = (name, (column, ), column.type)

        if getattr(column, "is_literal", False):
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(column, name)

        if column.table is None or not column.table.named_with_column:
            return name
        else:
            if getattr(column.table, 'schema', None):
                schema_prefix = self.preparer.quote(column.table, column.table.schema) + '.'
            else:
                schema_prefix = ''
            return schema_prefix + self.preparer.quote(column.table, ANONYMOUS_LABEL.sub(self._process_anon, column.table.name)) + "." + name

    def escape_literal_column(self, text):
        """provide escaping for the literal_column() construct."""

        # TODO: some dialects might need different behavior here
        return text.replace('%', '%%')

    def visit_fromclause(self, fromclause, **kwargs):
        return fromclause.name

    def visit_index(self, index, **kwargs):
        return index.name

    def visit_typeclause(self, typeclause, **kwargs):
        return typeclause.type.dialect_impl(self.dialect).get_col_spec()

    def visit_textclause(self, textclause, **kwargs):
        if textclause.typemap is not None:
            for colname, type_ in textclause.typemap.iteritems():
                self.result_map[colname.lower()] = (colname, None, type_)

        def do_bindparam(m):
            name = m.group(1)
            if name in textclause.bindparams:
                return self.process(textclause.bindparams[name])
            else:
                return self.bindparam_string(name)

        # un-escape any \:params
        return BIND_PARAMS_ESC.sub(lambda m: m.group(1),
            BIND_PARAMS.sub(do_bindparam, textclause.text)
        )

    def visit_null(self, null, **kwargs):
        return 'NULL'

    def visit_clauselist(self, clauselist, **kwargs):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        elif sep == operators.comma_op:
            sep = ', '
        else:
            sep = " " + self.operator_string(clauselist.operator) + " "
        return sep.join([s for s in [self.process(c) for c in clauselist.clauses] if s is not None])

    def visit_calculatedclause(self, clause, **kwargs):
        return self.process(clause.clause_expr)

    def visit_cast(self, cast, **kwargs):
        return "CAST(%s AS %s)" % (self.process(cast.clause), self.process(cast.typeclause))

    def visit_function(self, func, result_map=None, **kwargs):
        if result_map is not None:
            result_map[func.name.lower()] = (func.name, None, func.type)

        name = self.function_string(func)

        if callable(name):
            return name(*[self.process(x) for x in func.clauses])
        else:
            return ".".join(func.packagenames + [name]) % {'expr':self.function_argspec(func)}

    def function_argspec(self, func):
        return self.process(func.clause_expr)

    def function_string(self, func):
        return self.functions.get(func.__class__, self.functions.get(func.name, func.name + "%(expr)s"))

    def visit_compound_select(self, cs, asfrom=False, parens=True, **kwargs):
        stack_entry = {'select':cs}

        if asfrom:
            stack_entry['is_subquery'] = True
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
            return op(self.process(binary.left), self.process(binary.right), **binary.modifiers)
        else:
            return self.process(binary.left) + " " + op + " " + self.process(binary.right)

    def operator_string(self, operator):
        return self.operators.get(operator, str(operator))

    def visit_bindparam(self, bindparam, **kwargs):
        name = self._truncate_bindparam(bindparam)
        if name in self.binds:
            existing = self.binds[name]
            if existing is not bindparam and (existing.unique or bindparam.unique):
                raise exceptions.CompileError("Bind parameter '%s' conflicts with unique bind parameter of the same name" % bindparam.key)
        self.binds[bindparam.key] = self.binds[name] = bindparam
        return self.bindparam_string(name)

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

        if len(anonname) > self.dialect.max_identifier_length:
            counter = self.generated_ids.get(ident_class, 1)
            truncname = anonname[0:self.dialect.max_identifier_length - 6] + "_" + hex(counter)[2:]
            self.generated_ids[ident_class] = counter + 1
        else:
            truncname = anonname
        self.generated_ids[(ident_class, name)] = truncname
        return truncname
    
    def _process_anon(self, match):
        (ident, derived) = match.group(1,2)

        key = ('anonymous', ident)
        if key in self.generated_ids:
            return self.generated_ids[key]
        else:
            anonymous_counter = self.generated_ids.get(('anon_counter', derived), 1)
            newname = derived + "_" + str(anonymous_counter)
            self.generated_ids[('anon_counter', derived)] = anonymous_counter + 1
            self.generated_ids[key] = newname
            return newname

    def _anonymize(self, name):
        return ANONYMOUS_LABEL.sub(self._process_anon, name)

    def bindparam_string(self, name):
        if self.positional:
            self.positiontup.append(name)

        return self.bindtemplate % {'name':name, 'position':len(self.positiontup)}

    def visit_alias(self, alias, asfrom=False, **kwargs):
        if asfrom:
            return self.process(alias.original, asfrom=True, **kwargs) + " AS " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    def label_select_column(self, select, column, asfrom):
        """label columns present in a select()."""

        if isinstance(column, sql._Label):
            return column

        if select.use_labels and getattr(column, '_label', None):
            return column.label(column._label)

        if \
            asfrom and \
            isinstance(column, sql._ColumnClause) and \
            not column.is_literal and \
            column.table is not None and \
            not isinstance(column.table, sql.Select):
            return column.label(column.name)
        elif not isinstance(column, (sql._UnaryExpression, sql._TextClause, sql._BindParamClause)) and (not hasattr(column, 'name') or isinstance(column, sql._Function)):
            return column.label(column.anon_label)
        else:
            return column

    def visit_select(self, select, asfrom=False, parens=True, iswrapper=False, **kwargs):

        stack_entry = {'select':select}
        prev_entry = self.stack and self.stack[-1] or None

        if asfrom or (prev_entry and 'select' in prev_entry):
            stack_entry['is_subquery'] = True
            if prev_entry and 'iswrapper' in prev_entry:
                column_clause_args = {'result_map':self.result_map}
            else:
                column_clause_args = {}
        elif iswrapper:
            column_clause_args = {}
            stack_entry['iswrapper'] = True
        else:
            column_clause_args = {'result_map':self.result_map}

        if self.stack and 'from' in self.stack[-1]:
            existingfroms = self.stack[-1]['from']
        else:
            existingfroms = None

        froms = select._get_display_froms(existingfroms)

        correlate_froms = util.Set(itertools.chain(*([froms] + [f._get_from_objects() for f in froms])))

        # TODO: might want to propigate existing froms for select(select(select))
        # where innermost select should correlate to outermost
#        if existingfroms:
#            correlate_froms = correlate_froms.union(existingfroms)
        stack_entry['from'] = correlate_froms
        self.stack.append(stack_entry)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = util.OrderedSet(
            [c for c in [
                self.process(
                    self.label_select_column(select, co, asfrom=asfrom), 
                    **column_clause_args) 
                for co in select.inner_columns
            ]
            if c is not None]
        )

        text = " ".join(["SELECT"] + [self.process(x) for x in select._prefixes]) + " "
        text += self.get_select_precolumns(select)
        text += ', '.join(inner_columns)

        from_strings = []
        for f in froms:
            from_strings.append(self.process(f, asfrom=True))

        if froms:
            text += " \nFROM "
            text += string.join(from_strings, ', ')
        else:
            text += self.default_from()

        if select._whereclause is not None:
            t = self.process(select._whereclause)
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
            if getattr(table, "schema", None):
                return self.preparer.quote(table, table.schema) + "." + self.preparer.quote(table, table.name)
            else:
                return self.preparer.quote(table, table.name)
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return (self.process(join.left, asfrom=True) + (join.isouter and " LEFT OUTER JOIN " or " JOIN ") + \
            self.process(join.right, asfrom=True) + " ON " + self.process(join.onclause))

    def visit_sequence(self, seq):
        return None

    def visit_insert(self, insert_stmt):
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt)
        preparer = self.preparer

        insert = ' '.join(["INSERT"] +
                          [self.process(x) for x in insert_stmt._prefixes])

        return (insert + " INTO %s (%s) VALUES (%s)" %
                (preparer.format_table(insert_stmt.table),
                 ', '.join([preparer.quote(c[0], c[0].name)
                            for c in colparams]),
                 ', '.join([c[1] for c in colparams])))

    def visit_update(self, update_stmt):
        self.stack.append({'from':util.Set([update_stmt.table])})

        self.isupdate = True
        colparams = self._get_colparams(update_stmt)

        text = "UPDATE " + self.preparer.format_table(update_stmt.table) + " SET " + string.join(["%s=%s" % (self.preparer.quote(c[0], c[0].name), c[1]) for c in colparams], ', ')

        if update_stmt._whereclause:
            text += " WHERE " + self.process(update_stmt._whereclause)

        self.stack.pop(-1)

        return text

    def _get_colparams(self, stmt):
        """create a set of tuples representing column/string pairs for use
        in an INSERT or UPDATE statement.

        """

        def create_bind_param(col, value):
            bindparam = sql.bindparam(col.key, value, type_=col.type)
            self.binds[col.key] = bindparam
            return self.bindparam_string(self._truncate_bindparam(bindparam))

        self.postfetch = []
        self.prefetch = []

        # no parameters in the statement, no parameters in the
        # compiled params - return binds for all columns
        if self.column_keys is None and stmt.parameters is None:
            return [(c, create_bind_param(c, None)) for c in stmt.table.columns]

        # if we have statement parameters - set defaults in the
        # compiled params
        if self.column_keys is None:
            parameters = {}
        else:
            parameters = dict([(getattr(key, 'key', key), None) for key in self.column_keys])

        if stmt.parameters is not None:
            for k, v in stmt.parameters.iteritems():
                parameters.setdefault(getattr(k, 'key', k), v)

        # create a list of column assignment clauses as tuples
        values = []
        for c in stmt.table.columns:
            if c.key in parameters:
                value = parameters[c.key]
                if sql._is_literal(value):
                    value = create_bind_param(c, value)
                else:
                    self.postfetch.append(c)
                    value = self.process(value.self_group())
                values.append((c, value))
            elif isinstance(c, schema.Column):
                if self.isinsert:
                    if (c.primary_key and self.dialect.preexecute_pk_sequences and not self.inline):
                        if (((isinstance(c.default, schema.Sequence) and
                              not c.default.optional) or
                             not self.dialect.supports_pk_autoincrement) or
                            (c.default is not None and
                             not isinstance(c.default, schema.Sequence))):
                            values.append((c, create_bind_param(c, None)))
                            self.prefetch.append(c)
                    elif isinstance(c.default, schema.ColumnDefault):
                        if isinstance(c.default.arg, sql.ClauseElement):
                            values.append((c, self.process(c.default.arg.self_group())))
                            if not c.primary_key:
                                # dont add primary key column to postfetch
                                self.postfetch.append(c)
                        else:
                            values.append((c, create_bind_param(c, None)))
                            self.prefetch.append(c)
                    elif isinstance(c.default, schema.PassiveDefault):
                        if not c.primary_key:
                            self.postfetch.append(c)
                    elif isinstance(c.default, schema.Sequence):
                        proc = self.process(c.default)
                        if proc is not None:
                            values.append((c, proc))
                            if not c.primary_key:
                                self.postfetch.append(c)
                elif self.isupdate:
                    if isinstance(c.onupdate, schema.ColumnDefault):
                        if isinstance(c.onupdate.arg, sql.ClauseElement):
                            values.append((c, self.process(c.onupdate.arg.self_group())))
                            self.postfetch.append(c)
                        else:
                            values.append((c, create_bind_param(c, None)))
                            self.prefetch.append(c)
                    elif isinstance(c.onupdate, schema.PassiveDefault):
                        self.postfetch.append(c)
        return values

    def visit_delete(self, delete_stmt):
        self.stack.append({'from':util.Set([delete_stmt.table])})
        self.isdelete = True

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
        return self.string or ''

class DDLBase(engine.SchemaIterator):
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

    def _validate_identifier(self, ident, truncate):
        if truncate:
            if len(ident) > self.dialect.max_identifier_length:
                counter = getattr(self, 'counter', 0)
                self.counter = counter + 1
                return ident[0:self.dialect.max_identifier_length - 6] + "_" + hex(self.counter)[2:]
            else:
                return ident
        else:
            self.dialect.validate_identifier(ident)
            return ident


class SchemaGenerator(DDLBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(SchemaGenerator, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables and util.Set(tables) or None
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect

    def get_column_specification(self, column, first_pk=False):
        raise NotImplementedError()

    def _can_create(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or not self.dialect.has_table(self.connection, table.name, schema=table.schema)

    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=False, tables=self.tables) if self._can_create(t)]
        for table in collection:
            self.traverse_single(table)
        if self.dialect.supports_alter:
            for alterable in self.find_alterables(collection):
                self.add_foreignkey(alterable)

    def visit_table(self, table):
        for listener in table.ddl_listeners['before-create']:
            listener('before-create', table, self.connection)

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

        for listener in table.ddl_listeners['after-create']:
            listener('after-create', table, self.connection)

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
        compiler = self.dialect.statement_compiler(self.dialect, tocompile, parameters)
        compiler.compile()
        return compiler

    def visit_check_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " %
                        self.preparer.format_constraint(constraint))
        self.append(" CHECK (%s)" % constraint.sqltext)
        self.define_constraint_deferrability(constraint)

    def visit_column_check_constraint(self, constraint):
        self.append(" CHECK (%s)" % constraint.sqltext)
        self.define_constraint_deferrability(constraint)

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " % self.preparer.format_constraint(constraint))
        self.append("PRIMARY KEY ")
        self.append("(%s)" % ', '.join([self.preparer.quote(c, c.name) for c in constraint]))
        self.define_constraint_deferrability(constraint)

    def visit_foreign_key_constraint(self, constraint):
        if constraint.use_alter and self.dialect.supports_alter:
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
        table = list(constraint.elements)[0].column.table
        self.append("FOREIGN KEY(%s) REFERENCES %s (%s)" % (
            ', '.join([preparer.quote(f.parent, f.parent.name) for f in constraint.elements]),
            preparer.format_table(table),
            ', '.join([preparer.quote(f.column, f.column.name) for f in constraint.elements])
        ))
        if constraint.ondelete is not None:
            self.append(" ON DELETE %s" % constraint.ondelete)
        if constraint.onupdate is not None:
            self.append(" ON UPDATE %s" % constraint.onupdate)
        self.define_constraint_deferrability(constraint)

    def visit_unique_constraint(self, constraint):
        self.append(", \n\t")
        if constraint.name is not None:
            self.append("CONSTRAINT %s " %
                        self.preparer.format_constraint(constraint))
        self.append(" UNIQUE (%s)" % (', '.join([self.preparer.quote(c, c.name) for c in constraint])))
        self.define_constraint_deferrability(constraint)

    def define_constraint_deferrability(self, constraint):
        if constraint.deferrable is not None:
            if constraint.deferrable:
                self.append(" DEFERRABLE")
            else:
                self.append(" NOT DEFERRABLE")
        if constraint.initially is not None:
            self.append(" INITIALLY %s" % constraint.initially)

    def visit_column(self, column):
        pass

    def visit_index(self, index):
        preparer = self.preparer
        self.append("CREATE ")
        if index.unique:
            self.append("UNIQUE ")
        self.append("INDEX %s ON %s (%s)" \
                    % (preparer.quote(index, self._validate_identifier(index.name, True)),
                       preparer.format_table(index.table),
                       string.join([preparer.quote(c, c.name) for c in index.columns], ', ')))
        self.execute()


class SchemaDropper(DDLBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(SchemaDropper, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect

    def visit_metadata(self, metadata):
        collection = [t for t in metadata.table_iterator(reverse=True, tables=self.tables) if self._can_drop(t)]
        if self.dialect.supports_alter:
            for alterable in self.find_alterables(collection):
                self.drop_foreignkey(alterable)
        for table in collection:
            self.traverse_single(table)

    def _can_drop(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or self.dialect.has_table(self.connection, table.name, schema=table.schema)

    def visit_index(self, index):
        self.append("\nDROP INDEX " + self.preparer.quote(index, self._validate_identifier(index.name, False)))
        self.execute()

    def drop_foreignkey(self, constraint):
        self.append("ALTER TABLE %s DROP CONSTRAINT %s" % (
            self.preparer.format_table(constraint.table),
            self.preparer.format_constraint(constraint)))
        self.execute()

    def visit_table(self, table):
        for listener in table.ddl_listeners['before-drop']:
            listener('before-drop', table, self.connection)

        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.append("\nDROP TABLE " + self.preparer.format_table(table))
        self.execute()

        for listener in table.ddl_listeners['after-drop']:
            listener('after-drop', table, self.connection)


class IdentifierPreparer(object):
    """Handle quoting and case-folding of identifiers based on options."""

    reserved_words = RESERVED_WORDS

    legal_characters = LEGAL_CHARACTERS

    illegal_initial_characters = ILLEGAL_INITIAL_CHARACTERS

    def __init__(self, dialect, initial_quote='"', final_quote=None, omit_schema=False):
        """Construct a new ``IdentifierPreparer`` object.

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

    def _requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (lc_value in self.reserved_words
                or self.illegal_initial_characters.match(value[0])
                or not self.legal_characters.match(unicode(value))
                or (lc_value != value))

    def quote(self, obj, ident):
        if getattr(obj, 'quote', False):
            return self.quote_identifier(ident)
        if ident in self.__strings:
            return self.__strings[ident]
        else:
            if self._requires_quotes(ident):
                self.__strings[ident] = self.quote_identifier(ident)
            else:
                self.__strings[ident] = ident
            return self.__strings[ident]

    def should_quote(self, object):
        return object.quote or self._requires_quotes(object.name)

    def format_sequence(self, sequence, use_schema=True):
        name = self.quote(sequence, sequence.name)
        if not self.omit_schema and use_schema and sequence.schema is not None:
            name = self.quote(sequence, sequence.schema) + "." + name
        return name

    def format_label(self, label, name=None):
        return self.quote(label, name or label.name)

    def format_alias(self, alias, name=None):
        return self.quote(alias, name or alias.name)

    def format_savepoint(self, savepoint, name=None):
        return self.quote(savepoint, name or savepoint.ident)

    def format_constraint(self, constraint):
        return self.quote(constraint, constraint.name)

    def format_table(self, table, use_schema=True, name=None):
        """Prepare a quoted table and schema name."""

        if name is None:
            name = table.name
        result = self.quote(table, name)
        if not self.omit_schema and use_schema and getattr(table, "schema", None):
            result = self.quote(table, table.schema) + "." + result
        return result

    def format_column(self, column, use_table=False, name=None, table_name=None):
        """Prepare a quoted column name.

        deprecated.  use preparer.quote(col, column.name) or combine with format_table()
        """

        if name is None:
            name = column.name
        if not getattr(column, 'is_literal', False):
            if use_table:
                return self.format_table(column.table, use_schema=False, name=table_name) + "." + self.quote(column, name)
            else:
                return self.quote(column, name)
        else:
            # literal textual elements get stuck into ColumnClause alot, which shouldnt get quoted
            if use_table:
                return self.format_table(column.table, use_schema=False, name=table_name) + "." + name
            else:
                return name

    def format_table_seq(self, table, use_schema=True):
        """Format table name and schema as a tuple."""

        # Dialects with more levels in their fully qualified references
        # ('database', 'owner', etc.) could override this and return
        # a longer sequence.

        if not self.omit_schema and use_schema and getattr(table, 'schema', None):
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
