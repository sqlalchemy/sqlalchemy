# sql/compiler.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base SQL and DDL compiler implementations.

Classes provided include:

:class:`.compiler.SQLCompiler` - renders SQL
strings

:class:`.compiler.DDLCompiler` - renders DDL
(data definition language) strings

:class:`.compiler.GenericTypeCompiler` - renders
type specification strings.

To generate user-defined SQL strings, see
:doc:`/ext/compiler`.

"""

import contextlib
import re
from . import schema, sqltypes, operators, functions, visitors, \
    elements, selectable, crud
from .. import util, exc
import itertools

RESERVED_WORDS = set([
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
ILLEGAL_INITIAL_CHARACTERS = set([str(x) for x in range(0, 10)]).union(['$'])

BIND_PARAMS = re.compile(r'(?<![:\w\$\x5c]):([\w\$]+)(?![:\w\$])', re.UNICODE)
BIND_PARAMS_ESC = re.compile(r'\x5c(:[\w\$]+)(?![:\w\$])', re.UNICODE)

BIND_TEMPLATES = {
    'pyformat': "%%(%(name)s)s",
    'qmark': "?",
    'format': "%%s",
    'numeric': ":[_POSITION]",
    'named': ":%(name)s"
}


OPERATORS = {
    # binary
    operators.and_: ' AND ',
    operators.or_: ' OR ',
    operators.add: ' + ',
    operators.mul: ' * ',
    operators.sub: ' - ',
    operators.div: ' / ',
    operators.mod: ' % ',
    operators.truediv: ' / ',
    operators.neg: '-',
    operators.lt: ' < ',
    operators.le: ' <= ',
    operators.ne: ' != ',
    operators.gt: ' > ',
    operators.ge: ' >= ',
    operators.eq: ' = ',
    operators.concat_op: ' || ',
    operators.match_op: ' MATCH ',
    operators.notmatch_op: ' NOT MATCH ',
    operators.in_op: ' IN ',
    operators.notin_op: ' NOT IN ',
    operators.comma_op: ', ',
    operators.from_: ' FROM ',
    operators.as_: ' AS ',
    operators.is_: ' IS ',
    operators.isnot: ' IS NOT ',
    operators.collate: ' COLLATE ',

    # unary
    operators.exists: 'EXISTS ',
    operators.distinct_op: 'DISTINCT ',
    operators.inv: 'NOT ',

    # modifiers
    operators.desc_op: ' DESC',
    operators.asc_op: ' ASC',
    operators.nullsfirst_op: ' NULLS FIRST',
    operators.nullslast_op: ' NULLS LAST',

}

FUNCTIONS = {
    functions.coalesce: 'coalesce%(expr)s',
    functions.current_date: 'CURRENT_DATE',
    functions.current_time: 'CURRENT_TIME',
    functions.current_timestamp: 'CURRENT_TIMESTAMP',
    functions.current_user: 'CURRENT_USER',
    functions.localtime: 'LOCALTIME',
    functions.localtimestamp: 'LOCALTIMESTAMP',
    functions.random: 'random%(expr)s',
    functions.sysdate: 'sysdate',
    functions.session_user: 'SESSION_USER',
    functions.user: 'USER'
}

EXTRACT_MAP = {
    'month': 'month',
    'day': 'day',
    'year': 'year',
    'second': 'second',
    'hour': 'hour',
    'doy': 'doy',
    'minute': 'minute',
    'quarter': 'quarter',
    'dow': 'dow',
    'week': 'week',
    'epoch': 'epoch',
    'milliseconds': 'milliseconds',
    'microseconds': 'microseconds',
    'timezone_hour': 'timezone_hour',
    'timezone_minute': 'timezone_minute'
}

COMPOUND_KEYWORDS = {
    selectable.CompoundSelect.UNION: 'UNION',
    selectable.CompoundSelect.UNION_ALL: 'UNION ALL',
    selectable.CompoundSelect.EXCEPT: 'EXCEPT',
    selectable.CompoundSelect.EXCEPT_ALL: 'EXCEPT ALL',
    selectable.CompoundSelect.INTERSECT: 'INTERSECT',
    selectable.CompoundSelect.INTERSECT_ALL: 'INTERSECT ALL'
}


class Compiled(object):

    """Represent a compiled SQL or DDL expression.

    The ``__str__`` method of the ``Compiled`` object should produce
    the actual text of the statement.  ``Compiled`` objects are
    specific to their underlying database dialect, and also may
    or may not be specific to the columns referenced within a
    particular set of bind parameters.  In no case should the
    ``Compiled`` object be dependent on the actual values of those
    bind parameters, even though it may reference those values as
    defaults.
    """

    _cached_metadata = None

    def __init__(self, dialect, statement, bind=None,
                 compile_kwargs=util.immutabledict()):
        """Construct a new ``Compiled`` object.

        :param dialect: ``Dialect`` to compile against.

        :param statement: ``ClauseElement`` to be compiled.

        :param bind: Optional Engine or Connection to compile this
          statement against.

        :param compile_kwargs: additional kwargs that will be
         passed to the initial call to :meth:`.Compiled.process`.

         .. versionadded:: 0.8

        """

        self.dialect = dialect
        self.bind = bind
        if statement is not None:
            self.statement = statement
            self.can_execute = statement.supports_execution
            self.string = self.process(self.statement, **compile_kwargs)

    @util.deprecated("0.7", ":class:`.Compiled` objects now compile "
                     "within the constructor.")
    def compile(self):
        """Produce the internal string representation of this element.
        """
        pass

    def _execute_on_connection(self, connection, multiparams, params):
        return connection._execute_compiled(self, multiparams, params)

    @property
    def sql_compiler(self):
        """Return a Compiled that is capable of processing SQL expressions.

        If this compiler is one, it would likely just return 'self'.

        """

        raise NotImplementedError()

    def process(self, obj, **kwargs):
        return obj._compiler_dispatch(self, **kwargs)

    def __str__(self):
        """Return the string text of the generated SQL or DDL."""

        return self.string or ''

    def construct_params(self, params=None):
        """Return the bind params for this compiled object.

        :param params: a dict of string/object pairs whose values will
                       override bind values compiled in to the
                       statement.
        """

        raise NotImplementedError()

    @property
    def params(self):
        """Return the bind params for this compiled object."""
        return self.construct_params()

    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exc.UnboundExecutionError(
                "This Compiled object is not bound to any Engine "
                "or Connection.")
        return e._execute_compiled(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's
        scalar value."""

        return self.execute(*multiparams, **params).scalar()


class TypeCompiler(util.with_metaclass(util.EnsureKWArgType, object)):
    """Produces DDL specification for TypeEngine objects."""

    ensure_kwarg = 'visit_\w+'

    def __init__(self, dialect):
        self.dialect = dialect

    def process(self, type_, **kw):
        return type_._compiler_dispatch(self, **kw)


class _CompileLabel(visitors.Visitable):

    """lightweight label object which acts as an expression.Label."""

    __visit_name__ = 'label'
    __slots__ = 'element', 'name'

    def __init__(self, col, name, alt_names=()):
        self.element = col
        self.name = name
        self._alt_names = (col,) + alt_names

    @property
    def proxy_set(self):
        return self.element.proxy_set

    @property
    def type(self):
        return self.element.type


class SQLCompiler(Compiled):

    """Default implementation of Compiled.

    Compiles ClauseElements into SQL strings.   Uses a similar visit
    paradigm as visitors.ClauseVisitor but implements its own traversal.

    """

    extract_map = EXTRACT_MAP

    compound_keywords = COMPOUND_KEYWORDS

    isdelete = isinsert = isupdate = False
    """class-level defaults which can be set at the instance
    level to define if this Compiled instance represents
    INSERT/UPDATE/DELETE
    """

    returning = None
    """holds the "returning" collection of columns if
    the statement is CRUD and defines returning columns
    either implicitly or explicitly
    """

    returning_precedes_values = False
    """set to True classwide to generate RETURNING
    clauses before the VALUES or WHERE clause (i.e. MSSQL)
    """

    render_table_with_column_in_update_from = False
    """set to True classwide to indicate the SET clause
    in a multi-table UPDATE statement should qualify
    columns with the table name (i.e. MySQL only)
    """

    ansi_bind_rules = False
    """SQL 92 doesn't allow bind parameters to be used
    in the columns clause of a SELECT, nor does it allow
    ambiguous expressions like "? = ?".  A compiler
    subclass can set this flag to False if the target
    driver/DB enforces this
    """

    def __init__(self, dialect, statement, column_keys=None,
                 inline=False, **kwargs):
        """Construct a new ``DefaultCompiler`` object.

        dialect
          Dialect to be used

        statement
          ClauseElement to be compiled

        column_keys
          a list of column names to be compiled into an INSERT or UPDATE
          statement.

        """
        self.column_keys = column_keys

        # compile INSERT/UPDATE defaults/sequences inlined (no pre-
        # execute)
        self.inline = inline or getattr(statement, 'inline', False)

        # a dictionary of bind parameter keys to BindParameter
        # instances.
        self.binds = {}

        # a dictionary of BindParameter instances to "compiled" names
        # that are actually present in the generated SQL
        self.bind_names = util.column_dict()

        # stack which keeps track of nested SELECT statements
        self.stack = []

        # relates label names in the final SQL to a tuple of local
        # column/label name, ColumnElement object (if any) and
        # TypeEngine. ResultProxy uses this for type processing and
        # column targeting
        self._result_columns = []

        # if False, means we can't be sure the list of entries
        # in _result_columns is actually the rendered order.   This
        # gets flipped when we use TextAsFrom, for example.
        self._ordered_columns = True

        # true if the paramstyle is positional
        self.positional = dialect.positional
        if self.positional:
            self.positiontup = []
        self.bindtemplate = BIND_TEMPLATES[dialect.paramstyle]

        self.ctes = None

        # an IdentifierPreparer that formats the quoting of identifiers
        self.preparer = dialect.identifier_preparer
        self.label_length = dialect.label_length \
            or dialect.max_identifier_length

        # a map which tracks "anonymous" identifiers that are created on
        # the fly here
        self.anon_map = util.PopulateDict(self._process_anon)

        # a map which tracks "truncated" names based on
        # dialect.label_length or dialect.max_identifier_length
        self.truncated_names = {}
        Compiled.__init__(self, dialect, statement, **kwargs)

        if self.positional and dialect.paramstyle == 'numeric':
            self._apply_numbered_params()

    @util.memoized_instancemethod
    def _init_cte_state(self):
        """Initialize collections related to CTEs only if
        a CTE is located, to save on the overhead of
        these collections otherwise.

        """
        # collect CTEs to tack on top of a SELECT
        self.ctes = util.OrderedDict()
        self.ctes_by_name = {}
        self.ctes_recursive = False
        if self.positional:
            self.cte_positional = {}

    @contextlib.contextmanager
    def _nested_result(self):
        """special API to support the use case of 'nested result sets'"""
        result_columns, ordered_columns = (
            self._result_columns, self._ordered_columns)
        self._result_columns, self._ordered_columns = [], False

        try:
            if self.stack:
                entry = self.stack[-1]
                entry['need_result_map_for_nested'] = True
            else:
                entry = None
            yield self._result_columns, self._ordered_columns
        finally:
            if entry:
                entry.pop('need_result_map_for_nested')
            self._result_columns, self._ordered_columns = (
                result_columns, ordered_columns)

    def _apply_numbered_params(self):
        poscount = itertools.count(1)
        self.string = re.sub(
            r'\[_POSITION\]',
            lambda m: str(util.next(poscount)),
            self.string)

    @util.memoized_property
    def _bind_processors(self):
        return dict(
            (key, value) for key, value in
            ((self.bind_names[bindparam],
              bindparam.type._cached_bind_processor(self.dialect))
             for bindparam in self.bind_names)
            if value is not None
        )

    def is_subquery(self):
        return len(self.stack) > 1

    @property
    def sql_compiler(self):
        return self

    def construct_params(self, params=None, _group_number=None, _check=True):
        """return a dictionary of bind parameter keys and values"""

        if params:
            pd = {}
            for bindparam in self.bind_names:
                name = self.bind_names[bindparam]
                if bindparam.key in params:
                    pd[name] = params[bindparam.key]
                elif name in params:
                    pd[name] = params[name]

                elif _check and bindparam.required:
                    if _group_number:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r, "
                            "in parameter group %d" %
                            (bindparam.key, _group_number))
                    else:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r"
                            % bindparam.key)

                elif bindparam.callable:
                    pd[name] = bindparam.effective_value
                else:
                    pd[name] = bindparam.value
            return pd
        else:
            pd = {}
            for bindparam in self.bind_names:
                if _check and bindparam.required:
                    if _group_number:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r, "
                            "in parameter group %d" %
                            (bindparam.key, _group_number))
                    else:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r"
                            % bindparam.key)

                if bindparam.callable:
                    pd[self.bind_names[bindparam]] = bindparam.effective_value
                else:
                    pd[self.bind_names[bindparam]] = bindparam.value
            return pd

    @property
    def params(self):
        """Return the bind param dictionary embedded into this
        compiled object, for those values that are present."""
        return self.construct_params(_check=False)

    @util.dependencies("sqlalchemy.engine.result")
    def _create_result_map(self, result):
        """utility method used for unit tests only."""
        return result.ResultMetaData._create_result_map(self._result_columns)

    def default_from(self):
        """Called when a SELECT statement has no froms, and no FROM clause is
        to be appended.

        Gives Oracle a chance to tack on a ``FROM DUAL`` to the string output.

        """
        return ""

    def visit_grouping(self, grouping, asfrom=False, **kwargs):
        return "(" + grouping.element._compiler_dispatch(self, **kwargs) + ")"

    def visit_label_reference(
            self, element, within_columns_clause=False, **kwargs):
        if self.stack and self.dialect.supports_simple_order_by_label:
            selectable = self.stack[-1]['selectable']

            with_cols, only_froms = selectable._label_resolve_dict
            if within_columns_clause:
                resolve_dict = only_froms
            else:
                resolve_dict = with_cols

            # this can be None in the case that a _label_reference()
            # were subject to a replacement operation, in which case
            # the replacement of the Label element may have changed
            # to something else like a ColumnClause expression.
            order_by_elem = element.element._order_by_label_element

            if order_by_elem is not None and order_by_elem.name in \
                    resolve_dict:

                kwargs['render_label_as_label'] = \
                    element.element._order_by_label_element

        return self.process(
            element.element, within_columns_clause=within_columns_clause,
            **kwargs)

    def visit_textual_label_reference(
            self, element, within_columns_clause=False, **kwargs):
        if not self.stack:
            # compiling the element outside of the context of a SELECT
            return self.process(
                element._text_clause
            )

        selectable = self.stack[-1]['selectable']
        with_cols, only_froms = selectable._label_resolve_dict
        try:
            if within_columns_clause:
                col = only_froms[element.element]
            else:
                col = with_cols[element.element]
        except KeyError:
            # treat it like text()
            util.warn_limited(
                "Can't resolve label reference %r; converting to text()",
                util.ellipses_string(element.element))
            return self.process(
                element._text_clause
            )
        else:
            kwargs['render_label_as_label'] = col
            return self.process(
                col, within_columns_clause=within_columns_clause, **kwargs)

    def visit_label(self, label,
                    add_to_result_map=None,
                    within_label_clause=False,
                    within_columns_clause=False,
                    render_label_as_label=None,
                    **kw):
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        render_label_with_as = (within_columns_clause and not
                                within_label_clause)
        render_label_only = render_label_as_label is label

        if render_label_only or render_label_with_as:
            if isinstance(label.name, elements._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name

        if render_label_with_as:
            if add_to_result_map is not None:
                add_to_result_map(
                    labelname,
                    label.name,
                    (label, labelname, ) + label._alt_names,
                    label.type
                )

            return label.element._compiler_dispatch(
                self, within_columns_clause=True,
                within_label_clause=True, **kw) + \
                OPERATORS[operators.as_] + \
                self.preparer.format_label(label, labelname)
        elif render_label_only:
            return self.preparer.format_label(label, labelname)
        else:
            return label.element._compiler_dispatch(
                self, within_columns_clause=False, **kw)

    def visit_column(self, column, add_to_result_map=None,
                     include_table=True, **kwargs):
        name = orig_name = column.name
        if name is None:
            raise exc.CompileError("Cannot compile Column object until "
                                   "its 'name' is assigned.")

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(
                name,
                orig_name,
                (column, name, column.key),
                column.type
            )

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name)

        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return name
        else:
            if table.schema:
                schema_prefix = self.preparer.quote_schema(table.schema) + '.'
            else:
                schema_prefix = ''
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)

            return schema_prefix + \
                self.preparer.quote(tablename) + \
                "." + name

    def escape_literal_column(self, text):
        """provide escaping for the literal_column() construct."""

        # TODO: some dialects might need different behavior here
        return text.replace('%', '%%')

    def visit_fromclause(self, fromclause, **kwargs):
        return fromclause.name

    def visit_index(self, index, **kwargs):
        return index.name

    def visit_typeclause(self, typeclause, **kw):
        kw['type_expression'] = typeclause
        return self.dialect.type_compiler.process(typeclause.type, **kw)

    def post_process_text(self, text):
        return text

    def visit_textclause(self, textclause, **kw):
        def do_bindparam(m):
            name = m.group(1)
            if name in textclause._bindparams:
                return self.process(textclause._bindparams[name], **kw)
            else:
                return self.bindparam_string(name, **kw)

        # un-escape any \:params
        return BIND_PARAMS_ESC.sub(
            lambda m: m.group(1),
            BIND_PARAMS.sub(
                do_bindparam,
                self.post_process_text(textclause.text))
        )

    def visit_text_as_from(self, taf,
                           compound_index=None,
                           asfrom=False,
                           parens=True, **kw):

        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = toplevel or \
            (
                compound_index == 0 and entry.get(
                    'need_result_map_for_compound', False)
            ) or entry.get('need_result_map_for_nested', False)

        if populate_result_map:
            self._ordered_columns = False
            for c in taf.column_args:
                self.process(c, within_columns_clause=True,
                             add_to_result_map=self._add_to_result_map)

        text = self.process(taf.element, **kw)
        if asfrom and parens:
            text = "(%s)" % text
        return text

    def visit_null(self, expr, **kw):
        return 'NULL'

    def visit_true(self, expr, **kw):
        if self.dialect.supports_native_boolean:
            return 'true'
        else:
            return "1"

    def visit_false(self, expr, **kw):
        if self.dialect.supports_native_boolean:
            return 'false'
        else:
            return "0"

    def visit_clauselist(self, clauselist, **kw):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        else:
            sep = OPERATORS[clauselist.operator]
        return sep.join(
            s for s in
            (
                c._compiler_dispatch(self, **kw)
                for c in clauselist.clauses)
            if s)

    def visit_case(self, clause, **kwargs):
        x = "CASE "
        if clause.value is not None:
            x += clause.value._compiler_dispatch(self, **kwargs) + " "
        for cond, result in clause.whens:
            x += "WHEN " + cond._compiler_dispatch(
                self, **kwargs
            ) + " THEN " + result._compiler_dispatch(
                self, **kwargs) + " "
        if clause.else_ is not None:
            x += "ELSE " + clause.else_._compiler_dispatch(
                self, **kwargs
            ) + " "
        x += "END"
        return x

    def visit_cast(self, cast, **kwargs):
        return "CAST(%s AS %s)" % \
            (cast.clause._compiler_dispatch(self, **kwargs),
             cast.typeclause._compiler_dispatch(self, **kwargs))

    def visit_over(self, over, **kwargs):
        return "%s OVER (%s)" % (
            over.func._compiler_dispatch(self, **kwargs),
            ' '.join(
                '%s BY %s' % (word, clause._compiler_dispatch(self, **kwargs))
                for word, clause in (
                    ('PARTITION', over.partition_by),
                    ('ORDER', over.order_by)
                )
                if clause is not None and len(clause)
            )
        )

    def visit_funcfilter(self, funcfilter, **kwargs):
        return "%s FILTER (WHERE %s)" % (
            funcfilter.func._compiler_dispatch(self, **kwargs),
            funcfilter.criterion._compiler_dispatch(self, **kwargs)
        )

    def visit_extract(self, extract, **kwargs):
        field = self.extract_map.get(extract.field, extract.field)
        return "EXTRACT(%s FROM %s)" % (
            field, extract.expr._compiler_dispatch(self, **kwargs))

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        if add_to_result_map is not None:
            add_to_result_map(
                func.name, func.name, (), func.type
            )

        disp = getattr(self, "visit_%s_func" % func.name.lower(), None)
        if disp:
            return disp(func, **kwargs)
        else:
            name = FUNCTIONS.get(func.__class__, func.name + "%(expr)s")
            return ".".join(list(func.packagenames) + [name]) % \
                {'expr': self.function_argspec(func, **kwargs)}

    def visit_next_value_func(self, next_value, **kw):
        return self.visit_sequence(next_value.sequence)

    def visit_sequence(self, sequence):
        raise NotImplementedError(
            "Dialect '%s' does not support sequence increments." %
            self.dialect.name
        )

    def function_argspec(self, func, **kwargs):
        return func.clause_expr._compiler_dispatch(self, **kwargs)

    def visit_compound_select(self, cs, asfrom=False,
                              parens=True, compound_index=0, **kwargs):
        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]
        need_result_map = toplevel or \
            (compound_index == 0
                and entry.get('need_result_map_for_compound', False))

        self.stack.append(
            {
                'correlate_froms': entry['correlate_froms'],
                'asfrom_froms': entry['asfrom_froms'],
                'selectable': cs,
                'need_result_map_for_compound': need_result_map
            })

        keyword = self.compound_keywords.get(cs.keyword)

        text = (" " + keyword + " ").join(
            (c._compiler_dispatch(self,
                                  asfrom=asfrom, parens=False,
                                  compound_index=i, **kwargs)
             for i, c in enumerate(cs.selects))
        )

        group_by = cs._group_by_clause._compiler_dispatch(
            self, asfrom=asfrom, **kwargs)
        if group_by:
            text += " GROUP BY " + group_by

        text += self.order_by_clause(cs, **kwargs)
        text += (cs._limit_clause is not None
                 or cs._offset_clause is not None) and \
            self.limit_clause(cs, **kwargs) or ""

        if self.ctes and toplevel:
            text = self._render_cte_clause() + text

        self.stack.pop(-1)
        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def visit_unary(self, unary, **kw):
        if unary.operator:
            if unary.modifier:
                raise exc.CompileError(
                    "Unary expression does not support operator "
                    "and modifier simultaneously")
            disp = getattr(self, "visit_%s_unary_operator" %
                           unary.operator.__name__, None)
            if disp:
                return disp(unary, unary.operator, **kw)
            else:
                return self._generate_generic_unary_operator(
                    unary, OPERATORS[unary.operator], **kw)
        elif unary.modifier:
            disp = getattr(self, "visit_%s_unary_modifier" %
                           unary.modifier.__name__, None)
            if disp:
                return disp(unary, unary.modifier, **kw)
            else:
                return self._generate_generic_unary_modifier(
                    unary, OPERATORS[unary.modifier], **kw)
        else:
            raise exc.CompileError(
                "Unary expression has no operator or modifier")

    def visit_istrue_unary_operator(self, element, operator, **kw):
        if self.dialect.supports_native_boolean:
            return self.process(element.element, **kw)
        else:
            return "%s = 1" % self.process(element.element, **kw)

    def visit_isfalse_unary_operator(self, element, operator, **kw):
        if self.dialect.supports_native_boolean:
            return "NOT %s" % self.process(element.element, **kw)
        else:
            return "%s = 0" % self.process(element.element, **kw)

    def visit_notmatch_op_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_binary(
            binary, override_operator=operators.match_op)

    def visit_binary(self, binary, override_operator=None, **kw):
        # don't allow "? = ?" to render
        if self.ansi_bind_rules and \
                isinstance(binary.left, elements.BindParameter) and \
                isinstance(binary.right, elements.BindParameter):
            kw['literal_binds'] = True

        operator_ = override_operator or binary.operator
        disp = getattr(self, "visit_%s_binary" % operator_.__name__, None)
        if disp:
            return disp(binary, operator_, **kw)
        else:
            try:
                opstring = OPERATORS[operator_]
            except KeyError:
                raise exc.UnsupportedCompilationError(self, operator_)
            else:
                return self._generate_generic_binary(binary, opstring, **kw)

    def visit_custom_op_binary(self, element, operator, **kw):
        return self._generate_generic_binary(
            element, " " + operator.opstring + " ", **kw)

    def visit_custom_op_unary_operator(self, element, operator, **kw):
        return self._generate_generic_unary_operator(
            element, operator.opstring + " ", **kw)

    def visit_custom_op_unary_modifier(self, element, operator, **kw):
        return self._generate_generic_unary_modifier(
            element, " " + operator.opstring, **kw)

    def _generate_generic_binary(self, binary, opstring, **kw):
        return binary.left._compiler_dispatch(self, **kw) + \
            opstring + \
            binary.right._compiler_dispatch(self, **kw)

    def _generate_generic_unary_operator(self, unary, opstring, **kw):
        return opstring + unary.element._compiler_dispatch(self, **kw)

    def _generate_generic_unary_modifier(self, unary, opstring, **kw):
        return unary.element._compiler_dispatch(self, **kw) + opstring

    @util.memoized_property
    def _like_percent_literal(self):
        return elements.literal_column("'%'", type_=sqltypes.STRINGTYPE)

    def visit_contains_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__add__(binary.right).__add__(percent)
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_notcontains_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__add__(binary.right).__add__(percent)
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_startswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__radd__(
            binary.right
        )
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_notstartswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__radd__(
            binary.right
        )
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_endswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__add__(binary.right)
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_notendswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        percent = self._like_percent_literal
        binary.right = percent.__add__(binary.right)
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_like_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)

        # TODO: use ternary here, not "and"/ "or"
        return '%s LIKE %s' % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_notlike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        return '%s NOT LIKE %s' % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_ilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        return 'lower(%s) LIKE lower(%s)' % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_notilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        return 'lower(%s) NOT LIKE lower(%s)' % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_between_op_binary(self, binary, operator, **kw):
        symmetric = binary.modifiers.get("symmetric", False)
        return self._generate_generic_binary(
            binary, " BETWEEN SYMMETRIC "
            if symmetric else " BETWEEN ", **kw)

    def visit_notbetween_op_binary(self, binary, operator, **kw):
        symmetric = binary.modifiers.get("symmetric", False)
        return self._generate_generic_binary(
            binary, " NOT BETWEEN SYMMETRIC "
            if symmetric else " NOT BETWEEN ", **kw)

    def visit_bindparam(self, bindparam, within_columns_clause=False,
                        literal_binds=False,
                        skip_bind_expression=False,
                        **kwargs):
        if not skip_bind_expression and bindparam.type._has_bind_expression:
            bind_expression = bindparam.type.bind_expression(bindparam)
            return self.process(bind_expression,
                                skip_bind_expression=True)

        if literal_binds or \
            (within_columns_clause and
                self.ansi_bind_rules):
            if bindparam.value is None and bindparam.callable is None:
                raise exc.CompileError("Bind parameter '%s' without a "
                                       "renderable value not allowed here."
                                       % bindparam.key)
            return self.render_literal_bindparam(
                bindparam, within_columns_clause=True, **kwargs)

        name = self._truncate_bindparam(bindparam)

        if name in self.binds:
            existing = self.binds[name]
            if existing is not bindparam:
                if (existing.unique or bindparam.unique) and \
                    not existing.proxy_set.intersection(
                        bindparam.proxy_set):
                    raise exc.CompileError(
                        "Bind parameter '%s' conflicts with "
                        "unique bind parameter of the same name" %
                        bindparam.key
                    )
                elif existing._is_crud or bindparam._is_crud:
                    raise exc.CompileError(
                        "bindparam() name '%s' is reserved "
                        "for automatic usage in the VALUES or SET "
                        "clause of this "
                        "insert/update statement.   Please use a "
                        "name other than column name when using bindparam() "
                        "with insert() or update() (for example, 'b_%s')." %
                        (bindparam.key, bindparam.key)
                    )

        self.binds[bindparam.key] = self.binds[name] = bindparam

        return self.bindparam_string(name, **kwargs)

    def render_literal_bindparam(self, bindparam, **kw):
        value = bindparam.effective_value
        return self.render_literal_value(value, bindparam.type)

    def render_literal_value(self, value, type_):
        """Render the value of a bind parameter as a quoted literal.

        This is used for statement sections that do not accept bind parameters
        on the target driver/database.

        This should be implemented by subclasses using the quoting services
        of the DBAPI.

        """

        processor = type_._cached_literal_processor(self.dialect)
        if processor:
            return processor(value)
        else:
            raise NotImplementedError(
                "Don't know how to literal-quote value %r" % value)

    def _truncate_bindparam(self, bindparam):
        if bindparam in self.bind_names:
            return self.bind_names[bindparam]

        bind_name = bindparam.key
        if isinstance(bind_name, elements._truncated_label):
            bind_name = self._truncated_identifier("bindparam", bind_name)

        # add to bind_names for translation
        self.bind_names[bindparam] = bind_name

        return bind_name

    def _truncated_identifier(self, ident_class, name):
        if (ident_class, name) in self.truncated_names:
            return self.truncated_names[(ident_class, name)]

        anonname = name.apply_map(self.anon_map)

        if len(anonname) > self.label_length:
            counter = self.truncated_names.get(ident_class, 1)
            truncname = anonname[0:max(self.label_length - 6, 0)] + \
                "_" + hex(counter)[2:]
            self.truncated_names[ident_class] = counter + 1
        else:
            truncname = anonname
        self.truncated_names[(ident_class, name)] = truncname
        return truncname

    def _anonymize(self, name):
        return name % self.anon_map

    def _process_anon(self, key):
        (ident, derived) = key.split(' ', 1)
        anonymous_counter = self.anon_map.get(derived, 1)
        self.anon_map[derived] = anonymous_counter + 1
        return derived + "_" + str(anonymous_counter)

    def bindparam_string(self, name, positional_names=None, **kw):
        if self.positional:
            if positional_names is not None:
                positional_names.append(name)
            else:
                self.positiontup.append(name)
        return self.bindtemplate % {'name': name}

    def visit_cte(self, cte, asfrom=False, ashint=False,
                  fromhints=None,
                  **kwargs):
        self._init_cte_state()

        if isinstance(cte.name, elements._truncated_label):
            cte_name = self._truncated_identifier("alias", cte.name)
        else:
            cte_name = cte.name

        if cte_name in self.ctes_by_name:
            existing_cte = self.ctes_by_name[cte_name]
            # we've generated a same-named CTE that we are enclosed in,
            # or this is the same CTE.  just return the name.
            if cte in existing_cte._restates or cte is existing_cte:
                return self.preparer.format_alias(cte, cte_name)
            elif existing_cte in cte._restates:
                # we've generated a same-named CTE that is
                # enclosed in us - we take precedence, so
                # discard the text for the "inner".
                del self.ctes[existing_cte]
            else:
                raise exc.CompileError(
                    "Multiple, unrelated CTEs found with "
                    "the same name: %r" %
                    cte_name)

        self.ctes_by_name[cte_name] = cte

        if cte._cte_alias is not None:
            orig_cte = cte._cte_alias
            if orig_cte not in self.ctes:
                self.visit_cte(orig_cte, **kwargs)
            cte_alias_name = cte._cte_alias.name
            if isinstance(cte_alias_name, elements._truncated_label):
                cte_alias_name = self._truncated_identifier(
                    "alias", cte_alias_name)
        else:
            orig_cte = cte
            cte_alias_name = None
        if not cte_alias_name and cte not in self.ctes:
            if cte.recursive:
                self.ctes_recursive = True
            text = self.preparer.format_alias(cte, cte_name)
            if cte.recursive:
                if isinstance(cte.original, selectable.Select):
                    col_source = cte.original
                elif isinstance(cte.original, selectable.CompoundSelect):
                    col_source = cte.original.selects[0]
                else:
                    assert False
                recur_cols = [c for c in
                              util.unique_list(col_source.inner_columns)
                              if c is not None]

                text += "(%s)" % (", ".join(
                    self.preparer.format_column(ident)
                    for ident in recur_cols))

            if self.positional:
                kwargs['positional_names'] = self.cte_positional[cte] = []

            text += " AS \n" + \
                cte.original._compiler_dispatch(
                    self, asfrom=True, **kwargs
                )

            if cte._suffixes:
                text += " " + self._generate_prefixes(
                    cte, cte._suffixes, **kwargs)

            self.ctes[cte] = text

        if asfrom:
            if cte_alias_name:
                text = self.preparer.format_alias(cte, cte_alias_name)
                text += self.get_render_as_alias_suffix(cte_name)
            else:
                return self.preparer.format_alias(cte, cte_name)
            return text

    def visit_alias(self, alias, asfrom=False, ashint=False,
                    iscrud=False,
                    fromhints=None, **kwargs):
        if asfrom or ashint:
            if isinstance(alias.name, elements._truncated_label):
                alias_name = self._truncated_identifier("alias", alias.name)
            else:
                alias_name = alias.name

        if ashint:
            return self.preparer.format_alias(alias, alias_name)
        elif asfrom:
            ret = alias.original._compiler_dispatch(self,
                                                    asfrom=True, **kwargs) + \
                self.get_render_as_alias_suffix(
                    self.preparer.format_alias(alias, alias_name))

            if fromhints and alias in fromhints:
                ret = self.format_from_hint_text(ret, alias,
                                                 fromhints[alias], iscrud)

            return ret
        else:
            return alias.original._compiler_dispatch(self, **kwargs)

    def get_render_as_alias_suffix(self, alias_name_text):
        return " AS " + alias_name_text

    def _add_to_result_map(self, keyname, name, objects, type_):
        if not self.dialect.case_sensitive:
            keyname = keyname.lower()

        self._result_columns.append((keyname, name, objects, type_))

    def _label_select_column(self, select, column,
                             populate_result_map,
                             asfrom, column_clause_args,
                             name=None,
                             within_columns_clause=True):
        """produce labeled columns present in a select()."""

        if column.type._has_column_expression and \
                populate_result_map:
            col_expr = column.type.column_expression(column)
            add_to_result_map = lambda keyname, name, objects, type_: \
                self._add_to_result_map(
                    keyname, name,
                    objects + (column,), type_)
        else:
            col_expr = column
            if populate_result_map:
                add_to_result_map = self._add_to_result_map
            else:
                add_to_result_map = None

        if not within_columns_clause:
            result_expr = col_expr
        elif isinstance(column, elements.Label):
            if col_expr is not column:
                result_expr = _CompileLabel(
                    col_expr,
                    column.name,
                    alt_names=(column.element,)
                )
            else:
                result_expr = col_expr

        elif select is not None and name:
            result_expr = _CompileLabel(
                col_expr,
                name,
                alt_names=(column._key_label,)
            )

        elif \
            asfrom and \
            isinstance(column, elements.ColumnClause) and \
            not column.is_literal and \
            column.table is not None and \
                not isinstance(column.table, selectable.Select):
            result_expr = _CompileLabel(col_expr,
                                        elements._as_truncated(column.name),
                                        alt_names=(column.key,))
        elif not isinstance(column,
                            (elements.UnaryExpression, elements.TextClause)) \
                and (not hasattr(column, 'name') or
                     isinstance(column, functions.Function)):
            result_expr = _CompileLabel(col_expr, column.anon_label)
        elif col_expr is not column:
            # TODO: are we sure "column" has a .name and .key here ?
            # assert isinstance(column, elements.ColumnClause)
            result_expr = _CompileLabel(col_expr,
                                        elements._as_truncated(column.name),
                                        alt_names=(column.key,))
        else:
            result_expr = col_expr

        column_clause_args.update(
            within_columns_clause=within_columns_clause,
            add_to_result_map=add_to_result_map
        )
        return result_expr._compiler_dispatch(
            self,
            **column_clause_args
        )

    def format_from_hint_text(self, sqltext, table, hint, iscrud):
        hinttext = self.get_from_hint_text(table, hint)
        if hinttext:
            sqltext += " " + hinttext
        return sqltext

    def get_select_hint_text(self, byfroms):
        return None

    def get_from_hint_text(self, table, text):
        return None

    def get_crud_hint_text(self, table, text):
        return None

    def get_statement_hint_text(self, hint_texts):
        return " ".join(hint_texts)

    def _transform_select_for_nested_joins(self, select):
        """Rewrite any "a JOIN (b JOIN c)" expression as
        "a JOIN (select * from b JOIN c) AS anon", to support
        databases that can't parse a parenthesized join correctly
        (i.e. sqlite the main one).

        """
        cloned = {}
        column_translate = [{}]

        def visit(element, **kw):
            if element in column_translate[-1]:
                return column_translate[-1][element]

            elif element in cloned:
                return cloned[element]

            newelem = cloned[element] = element._clone()

            if newelem.is_selectable and newelem._is_join and \
                    isinstance(newelem.right, selectable.FromGrouping):

                newelem._reset_exported()
                newelem.left = visit(newelem.left, **kw)

                right = visit(newelem.right, **kw)

                selectable_ = selectable.Select(
                    [right.element],
                    use_labels=True).alias()

                for c in selectable_.c:
                    c._key_label = c.key
                    c._label = c.name

                translate_dict = dict(
                    zip(newelem.right.element.c, selectable_.c)
                )

                # translating from both the old and the new
                # because different select() structures will lead us
                # to traverse differently
                translate_dict[right.element.left] = selectable_
                translate_dict[right.element.right] = selectable_
                translate_dict[newelem.right.element.left] = selectable_
                translate_dict[newelem.right.element.right] = selectable_

                # propagate translations that we've gained
                # from nested visit(newelem.right) outwards
                # to the enclosing select here.  this happens
                # only when we have more than one level of right
                # join nesting, i.e. "a JOIN (b JOIN (c JOIN d))"
                for k, v in list(column_translate[-1].items()):
                    if v in translate_dict:
                        # remarkably, no current ORM tests (May 2013)
                        # hit this condition, only test_join_rewriting
                        # does.
                        column_translate[-1][k] = translate_dict[v]

                column_translate[-1].update(translate_dict)

                newelem.right = selectable_

                newelem.onclause = visit(newelem.onclause, **kw)

            elif newelem._is_from_container:
                # if we hit an Alias, CompoundSelect or ScalarSelect, put a
                # marker in the stack.
                kw['transform_clue'] = 'select_container'
                newelem._copy_internals(clone=visit, **kw)
            elif newelem.is_selectable and newelem._is_select:
                barrier_select = kw.get('transform_clue', None) == \
                    'select_container'
                # if we're still descended from an
                # Alias/CompoundSelect/ScalarSelect, we're
                # in a FROM clause, so start with a new translate collection
                if barrier_select:
                    column_translate.append({})
                kw['transform_clue'] = 'inside_select'
                newelem._copy_internals(clone=visit, **kw)
                if barrier_select:
                    del column_translate[-1]
            else:
                newelem._copy_internals(clone=visit, **kw)

            return newelem

        return visit(select)

    def _transform_result_map_for_nested_joins(
            self, select, transformed_select):
        inner_col = dict((c._key_label, c) for
                         c in transformed_select.inner_columns)

        d = dict(
            (inner_col[c._key_label], c)
            for c in select.inner_columns
        )

        self._result_columns = [
            (key, name, tuple([d.get(col, col) for col in objs]), typ)
            for key, name, objs, typ in self._result_columns
        ]

    _default_stack_entry = util.immutabledict([
        ('correlate_froms', frozenset()),
        ('asfrom_froms', frozenset())
    ])

    def _display_froms_for_select(self, select, asfrom):
        # utility method to help external dialects
        # get the correct from list for a select.
        # specifically the oracle dialect needs this feature
        # right now.
        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        correlate_froms = entry['correlate_froms']
        asfrom_froms = entry['asfrom_froms']

        if asfrom:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms.difference(
                    asfrom_froms),
                implicit_correlate_froms=())
        else:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms,
                implicit_correlate_froms=asfrom_froms)
        return froms

    def visit_select(self, select, asfrom=False, parens=True,
                     fromhints=None,
                     compound_index=0,
                     nested_join_translation=False,
                     select_wraps_for=None,
                     **kwargs):

        needs_nested_translation = \
            select.use_labels and \
            not nested_join_translation and \
            not self.stack and \
            not self.dialect.supports_right_nested_joins

        if needs_nested_translation:
            transformed_select = self._transform_select_for_nested_joins(
                select)
            text = self.visit_select(
                transformed_select, asfrom=asfrom, parens=parens,
                fromhints=fromhints,
                compound_index=compound_index,
                nested_join_translation=True, **kwargs
            )

        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = toplevel or \
            (
                compound_index == 0 and entry.get(
                    'need_result_map_for_compound', False)
            ) or entry.get('need_result_map_for_nested', False)

        if needs_nested_translation:
            if populate_result_map:
                self._transform_result_map_for_nested_joins(
                    select, transformed_select)
            return text

        froms = self._setup_select_stack(select, entry, asfrom)

        column_clause_args = kwargs.copy()
        column_clause_args.update({
            'within_label_clause': False,
            'within_columns_clause': False
        })

        text = "SELECT "  # we're off to a good start !

        if select._hints:
            hint_text, byfrom = self._setup_select_hints(select)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select._prefixes:
            text += self._generate_prefixes(
                select, select._prefixes, **kwargs)

        text += self.get_select_precolumns(select)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c for c in [
                self._label_select_column(
                    select,
                    column,
                    populate_result_map, asfrom,
                    column_clause_args,
                    name=name)
                for name, column in select._columns_plus_names
            ]
            if c is not None
        ]

        if populate_result_map and select_wraps_for is not None:
            # if this select is a compiler-generated wrapper,
            # rewrite the targeted columns in the result map
            wrapped_inner_columns = set(select_wraps_for.inner_columns)
            translate = dict(
                (outer, inner.pop()) for outer, inner in [
                    (
                        outer,
                        outer.proxy_set.intersection(wrapped_inner_columns))
                    for outer in select.inner_columns
                ] if inner
            )
            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
            ]

        text = self._compose_select_body(
            text, select, inner_columns, froms, byfrom, kwargs)

        if select._statement_hints:
            per_dialect = [
                ht for (dialect_name, ht)
                in select._statement_hints
                if dialect_name in ('*', self.dialect.name)
            ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        if self.ctes and toplevel:
            text = self._render_cte_clause() + text

        if select._suffixes:
            text += " " + self._generate_prefixes(
                select, select._suffixes, **kwargs)

        self.stack.pop(-1)

        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def _setup_select_hints(self, select):
        byfrom = dict([
            (from_, hinttext % {
                'name': from_._compiler_dispatch(
                    self, ashint=True)
            })
            for (from_, dialect), hinttext in
            select._hints.items()
            if dialect in ('*', self.dialect.name)
        ])
        hint_text = self.get_select_hint_text(byfrom)
        return hint_text, byfrom

    def _setup_select_stack(self, select, entry, asfrom):
        correlate_froms = entry['correlate_froms']
        asfrom_froms = entry['asfrom_froms']

        if asfrom:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms.difference(
                    asfrom_froms),
                implicit_correlate_froms=())
        else:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms,
                implicit_correlate_froms=asfrom_froms)

        new_correlate_froms = set(selectable._from_objects(*froms))
        all_correlate_froms = new_correlate_froms.union(correlate_froms)

        new_entry = {
            'asfrom_froms': new_correlate_froms,
            'correlate_froms': all_correlate_froms,
            'selectable': select,
        }
        self.stack.append(new_entry)
        return froms

    def _compose_select_body(
            self, text, select, inner_columns, froms, byfrom, kwargs):
        text += ', '.join(inner_columns)

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ', '.join(
                    [f._compiler_dispatch(self, asfrom=True,
                                          fromhints=byfrom, **kwargs)
                     for f in froms])
            else:
                text += ', '.join(
                    [f._compiler_dispatch(self, asfrom=True, **kwargs)
                     for f in froms])
        else:
            text += self.default_from()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nWHERE " + t

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(
                self, **kwargs)
            if group_by:
                text += " GROUP BY " + group_by

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nHAVING " + t

        if select._order_by_clause.clauses:
            text += self.order_by_clause(select, **kwargs)

        if (select._limit_clause is not None or
                select._offset_clause is not None):
            text += self.limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            text += self.for_update_clause(select, **kwargs)

        return text

    def _generate_prefixes(self, stmt, prefixes, **kw):
        clause = " ".join(
            prefix._compiler_dispatch(self, **kw)
            for prefix, dialect_name in prefixes
            if dialect_name is None or
            dialect_name == self.dialect.name
        )
        if clause:
            clause += " "
        return clause

    def _render_cte_clause(self):
        if self.positional:
            self.positiontup = sum([
                self.cte_positional[cte]
                for cte in self.ctes], []) + \
                self.positiontup
        cte_text = self.get_cte_preamble(self.ctes_recursive) + " "
        cte_text += ", \n".join(
            [txt for txt in self.ctes.values()]
        )
        cte_text += "\n "
        return cte_text

    def get_cte_preamble(self, recursive):
        if recursive:
            return "WITH RECURSIVE"
        else:
            return "WITH"

    def get_select_precolumns(self, select):
        """Called when building a ``SELECT`` statement, position is just
        before column list.

        """
        return select._distinct and "DISTINCT " or ""

    def order_by_clause(self, select, **kw):
        order_by = select._order_by_clause._compiler_dispatch(self, **kw)
        if order_by:
            return " ORDER BY " + order_by
        else:
            return ""

    def for_update_clause(self, select, **kw):
        return " FOR UPDATE"

    def returning_clause(self, stmt, returning_cols):
        raise exc.CompileError(
            "RETURNING is not supported by this "
            "dialect's statement compiler.")

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                text += "\n LIMIT -1"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                    fromhints=None, **kwargs):
        if asfrom or ashint:
            if getattr(table, "schema", None):
                ret = self.preparer.quote_schema(table.schema) + \
                    "." + self.preparer.quote(table.name)
            else:
                ret = self.preparer.quote(table.name)
            if fromhints and table in fromhints:
                ret = self.format_from_hint_text(ret, table,
                                                 fromhints[table], iscrud)
            return ret
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return (
            join.left._compiler_dispatch(self, asfrom=True, **kwargs) +
            (join.isouter and " LEFT OUTER JOIN " or " JOIN ") +
            join.right._compiler_dispatch(self, asfrom=True, **kwargs) +
            " ON " +
            join.onclause._compiler_dispatch(self, **kwargs)
        )

    def visit_insert(self, insert_stmt, **kw):
        self.stack.append(
            {'correlate_froms': set(),
             "asfrom_froms": set(),
             "selectable": insert_stmt})

        self.isinsert = True
        crud_params = crud._get_crud_params(self, insert_stmt, **kw)

        if not crud_params and \
                not self.dialect.supports_default_values and \
                not self.dialect.supports_empty_insert:
            raise exc.CompileError("The '%s' dialect with current database "
                                   "version settings does not support empty "
                                   "inserts." %
                                   self.dialect.name)

        if insert_stmt._has_multi_parameters:
            if not self.dialect.supports_multivalues_insert:
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support "
                    "in-place multirow inserts." %
                    self.dialect.name)
            crud_params_single = crud_params[0]
        else:
            crud_params_single = crud_params

        preparer = self.preparer
        supports_default_values = self.dialect.supports_default_values

        text = "INSERT "

        if insert_stmt._prefixes:
            text += self._generate_prefixes(insert_stmt,
                                            insert_stmt._prefixes, **kw)

        text += "INTO "
        table_text = preparer.format_table(insert_stmt.table)

        if insert_stmt._hints:
            dialect_hints = dict([
                (table, hint_text)
                for (table, dialect), hint_text in
                insert_stmt._hints.items()
                if dialect in ('*', self.dialect.name)
            ])
            if insert_stmt.table in dialect_hints:
                table_text = self.format_from_hint_text(
                    table_text,
                    insert_stmt.table,
                    dialect_hints[insert_stmt.table],
                    True
                )

        text += table_text

        if crud_params_single or not supports_default_values:
            text += " (%s)" % ', '.join([preparer.format_column(c[0])
                                         for c in crud_params_single])

        if self.returning or insert_stmt._returning:
            self.returning = self.returning or insert_stmt._returning
            returning_clause = self.returning_clause(
                insert_stmt, self.returning)

            if self.returning_precedes_values:
                text += " " + returning_clause

        if insert_stmt.select is not None:
            text += " %s" % self.process(self._insert_from_select, **kw)
        elif not crud_params and supports_default_values:
            text += " DEFAULT VALUES"
        elif insert_stmt._has_multi_parameters:
            text += " VALUES %s" % (
                ", ".join(
                    "(%s)" % (
                        ', '.join(c[1] for c in crud_param_set)
                    )
                    for crud_param_set in crud_params
                )
            )
        else:
            text += " VALUES (%s)" % \
                ', '.join([c[1] for c in crud_params])

        if self.returning and not self.returning_precedes_values:
            text += " " + returning_clause

        self.stack.pop(-1)

        return text

    def update_limit_clause(self, update_stmt):
        """Provide a hook for MySQL to add LIMIT to the UPDATE"""
        return None

    def update_tables_clause(self, update_stmt, from_table,
                             extra_froms, **kw):
        """Provide a hook to override the initial table clause
        in an UPDATE statement.

        MySQL overrides this.

        """
        return from_table._compiler_dispatch(self, asfrom=True,
                                             iscrud=True, **kw)

    def update_from_clause(self, update_stmt,
                           from_table, extra_froms,
                           from_hints,
                           **kw):
        """Provide a hook to override the generation of an
        UPDATE..FROM clause.

        MySQL and MSSQL override this.

        """
        return "FROM " + ', '.join(
            t._compiler_dispatch(self, asfrom=True,
                                 fromhints=from_hints, **kw)
            for t in extra_froms)

    def visit_update(self, update_stmt, **kw):
        self.stack.append(
            {'correlate_froms': set([update_stmt.table]),
             "asfrom_froms": set([update_stmt.table]),
             "selectable": update_stmt})

        self.isupdate = True

        extra_froms = update_stmt._extra_froms

        text = "UPDATE "

        if update_stmt._prefixes:
            text += self._generate_prefixes(update_stmt,
                                            update_stmt._prefixes, **kw)

        table_text = self.update_tables_clause(update_stmt, update_stmt.table,
                                               extra_froms, **kw)

        crud_params = crud._get_crud_params(self, update_stmt, **kw)

        if update_stmt._hints:
            dialect_hints = dict([
                (table, hint_text)
                for (table, dialect), hint_text in
                update_stmt._hints.items()
                if dialect in ('*', self.dialect.name)
            ])
            if update_stmt.table in dialect_hints:
                table_text = self.format_from_hint_text(
                    table_text,
                    update_stmt.table,
                    dialect_hints[update_stmt.table],
                    True
                )
        else:
            dialect_hints = None

        text += table_text

        text += ' SET '
        include_table = extra_froms and \
            self.render_table_with_column_in_update_from
        text += ', '.join(
            c[0]._compiler_dispatch(self,
                                    include_table=include_table) +
            '=' + c[1] for c in crud_params
        )

        if self.returning or update_stmt._returning:
            if not self.returning:
                self.returning = update_stmt._returning
            if self.returning_precedes_values:
                text += " " + self.returning_clause(
                    update_stmt, self.returning)

        if extra_froms:
            extra_from_text = self.update_from_clause(
                update_stmt,
                update_stmt.table,
                extra_froms,
                dialect_hints, **kw)
            if extra_from_text:
                text += " " + extra_from_text

        if update_stmt._whereclause is not None:
            t = self.process(update_stmt._whereclause)
            if t:
                text += " WHERE " + t

        limit_clause = self.update_limit_clause(update_stmt)
        if limit_clause:
            text += " " + limit_clause

        if self.returning and not self.returning_precedes_values:
            text += " " + self.returning_clause(
                update_stmt, self.returning)

        self.stack.pop(-1)

        return text

    @util.memoized_property
    def _key_getters_for_crud_column(self):
        return crud._key_getters_for_crud_column(self)

    def visit_delete(self, delete_stmt, **kw):
        self.stack.append({'correlate_froms': set([delete_stmt.table]),
                           "asfrom_froms": set([delete_stmt.table]),
                           "selectable": delete_stmt})
        self.isdelete = True

        text = "DELETE "

        if delete_stmt._prefixes:
            text += self._generate_prefixes(delete_stmt,
                                            delete_stmt._prefixes, **kw)

        text += "FROM "
        table_text = delete_stmt.table._compiler_dispatch(
            self, asfrom=True, iscrud=True)

        if delete_stmt._hints:
            dialect_hints = dict([
                (table, hint_text)
                for (table, dialect), hint_text in
                delete_stmt._hints.items()
                if dialect in ('*', self.dialect.name)
            ])
            if delete_stmt.table in dialect_hints:
                table_text = self.format_from_hint_text(
                    table_text,
                    delete_stmt.table,
                    dialect_hints[delete_stmt.table],
                    True
                )

        else:
            dialect_hints = None

        text += table_text

        if delete_stmt._returning:
            self.returning = delete_stmt._returning
            if self.returning_precedes_values:
                text += " " + self.returning_clause(
                    delete_stmt, delete_stmt._returning)

        if delete_stmt._whereclause is not None:
            t = delete_stmt._whereclause._compiler_dispatch(self)
            if t:
                text += " WHERE " + t

        if self.returning and not self.returning_precedes_values:
            text += " " + self.returning_clause(
                delete_stmt, delete_stmt._returning)

        self.stack.pop(-1)

        return text

    def visit_savepoint(self, savepoint_stmt):
        return "SAVEPOINT %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TO SAVEPOINT %s" % \
            self.preparer.format_savepoint(savepoint_stmt)

    def visit_release_savepoint(self, savepoint_stmt):
        return "RELEASE SAVEPOINT %s" % \
            self.preparer.format_savepoint(savepoint_stmt)


class DDLCompiler(Compiled):

    @util.memoized_property
    def sql_compiler(self):
        return self.dialect.statement_compiler(self.dialect, None)

    @util.memoized_property
    def type_compiler(self):
        return self.dialect.type_compiler

    @property
    def preparer(self):
        return self.dialect.identifier_preparer

    def construct_params(self, params=None):
        return None

    def visit_ddl(self, ddl, **kwargs):
        # table events can substitute table and schema name
        context = ddl.context
        if isinstance(ddl.target, schema.Table):
            context = context.copy()

            preparer = self.dialect.identifier_preparer
            path = preparer.format_table_seq(ddl.target)
            if len(path) == 1:
                table, sch = path[0], ''
            else:
                table, sch = path[-1], path[0]

            context.setdefault('table', table)
            context.setdefault('schema', sch)
            context.setdefault('fullname', preparer.format_table(ddl.target))

        return self.sql_compiler.post_process_text(ddl.statement % context)

    def visit_create_schema(self, create):
        schema = self.preparer.format_schema(create.element)
        return "CREATE SCHEMA " + schema

    def visit_drop_schema(self, drop):
        schema = self.preparer.format_schema(drop.element)
        text = "DROP SCHEMA " + schema
        if drop.cascade:
            text += " CASCADE"
        return text

    def visit_create_table(self, create):
        table = create.element
        preparer = self.dialect.identifier_preparer

        text = "\n" + " ".join(['CREATE'] +
                               table._prefixes +
                               ['TABLE',
                                preparer.format_table(table),
                                "("])
        separator = "\n"

        # if only one primary key, specify it along with the column
        first_pk = False
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column,
                                         first_pk=column.primary_key
                                         and not first_pk)
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '%s', column '%s'): %s") %
                        (table.description, column.name, ce.args[0])
                    ))

        const = self.create_table_constraints(
            table, _include_foreign_key_constraints=
            create.include_foreign_key_constraints)
        if const:
            text += ", \n\t" + const

        text += "\n)%s\n\n" % self.post_create_table(table)
        return text

    def visit_create_column(self, create, first_pk=False):
        column = create.element

        if column.system:
            return None

        text = self.get_column_specification(
            column,
            first_pk=first_pk
        )
        const = " ".join(self.process(constraint)
                         for constraint in column.constraints)
        if const:
            text += " " + const

        return text

    def create_table_constraints(
        self, table,
            _include_foreign_key_constraints=None):

        # On some DB order is significant: visit PK first, then the
        # other constraints (engine.ReflectionTest.testbasic failed on FB2)
        constraints = []
        if table.primary_key:
            constraints.append(table.primary_key)

        all_fkcs = table.foreign_key_constraints
        if _include_foreign_key_constraints is not None:
            omit_fkcs = all_fkcs.difference(_include_foreign_key_constraints)
        else:
            omit_fkcs = set()

        constraints.extend([c for c in table._sorted_constraints
                            if c is not table.primary_key and
                            c not in omit_fkcs])

        return ", \n\t".join(
            p for p in
            (self.process(constraint)
                for constraint in constraints
                if (
                    constraint._create_rule is None or
                    constraint._create_rule(self))
                and (
                    not self.dialect.supports_alter or
                    not getattr(constraint, 'use_alter', False)
                )) if p is not None
        )

    def visit_drop_table(self, drop):
        return "\nDROP TABLE " + self.preparer.format_table(drop.element)

    def visit_drop_view(self, drop):
        return "\nDROP VIEW " + self.preparer.format_table(drop.element)

    def _verify_index_table(self, index):
        if index.table is None:
            raise exc.CompileError("Index '%s' is not associated "
                                   "with any table." % index.name)

    def visit_create_index(self, create, include_schema=False,
                           include_table_schema=True):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX %s ON %s (%s)" \
            % (
                self._prepared_index_name(index,
                                          include_schema=include_schema),
                preparer.format_table(index.table,
                                      use_schema=include_table_schema),
                ', '.join(
                    self.sql_compiler.process(
                        expr, include_table=False, literal_binds=True) for
                    expr in index.expressions)
            )
        return text

    def visit_drop_index(self, drop):
        index = drop.element
        return "\nDROP INDEX " + self._prepared_index_name(
            index, include_schema=True)

    def _prepared_index_name(self, index, include_schema=False):
        if include_schema and index.table is not None and index.table.schema:
            schema = index.table.schema
            schema_name = self.preparer.quote_schema(schema)
        else:
            schema_name = None

        ident = index.name
        if isinstance(ident, elements._truncated_label):
            max_ = self.dialect.max_index_name_length or \
                self.dialect.max_identifier_length
            if len(ident) > max_:
                ident = ident[0:max_ - 8] + \
                    "_" + util.md5_hex(ident)[-4:]
        else:
            self.dialect.validate_identifier(ident)

        index_name = self.preparer.quote(ident)

        if schema_name:
            index_name = schema_name + "." + index_name
        return index_name

    def visit_add_constraint(self, create):
        return "ALTER TABLE %s ADD %s" % (
            self.preparer.format_table(create.element.table),
            self.process(create.element)
        )

    def visit_create_sequence(self, create):
        text = "CREATE SEQUENCE %s" % \
            self.preparer.format_sequence(create.element)
        if create.element.increment is not None:
            text += " INCREMENT BY %d" % create.element.increment
        if create.element.start is not None:
            text += " START WITH %d" % create.element.start
        return text

    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s" % \
            self.preparer.format_sequence(drop.element)

    def visit_drop_constraint(self, drop):
        constraint = drop.element
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
        else:
            formatted_name = None

        if formatted_name is None:
            raise exc.CompileError(
                "Can't emit DROP CONSTRAINT for constraint %r; "
                "it has no name" % drop.element)
        return "ALTER TABLE %s DROP CONSTRAINT %s%s" % (
            self.preparer.format_table(drop.element.table),
            formatted_name,
            drop.cascade and " CASCADE" or ""
        )

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + \
            self.dialect.type_compiler.process(
                column.type, type_expression=column)
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def post_create_table(self, table):
        return ''

    def get_column_default_string(self, column):
        if isinstance(column.server_default, schema.DefaultClause):
            if isinstance(column.server_default.arg, util.string_types):
                return "'%s'" % column.server_default.arg
            else:
                return self.sql_compiler.process(
                    column.server_default.arg, literal_binds=True)
        else:
            return None

    def visit_check_constraint(self, constraint):
        text = ""
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
            if formatted_name is not None:
                text += "CONSTRAINT %s " % formatted_name
        text += "CHECK (%s)" % self.sql_compiler.process(constraint.sqltext,
                                                         include_table=False,
                                                         literal_binds=True)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_column_check_constraint(self, constraint):
        text = ""
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
            if formatted_name is not None:
                text += "CONSTRAINT %s " % formatted_name
        text += "CHECK (%s)" % constraint.sqltext
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return ''
        text = ""
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
            if formatted_name is not None:
                text += "CONSTRAINT %s " % formatted_name
        text += "PRIMARY KEY "
        text += "(%s)" % ', '.join(self.preparer.quote(c.name)
                                   for c in constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_foreign_key_constraint(self, constraint):
        preparer = self.dialect.identifier_preparer
        text = ""
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
            if formatted_name is not None:
                text += "CONSTRAINT %s " % formatted_name
        remote_table = list(constraint.elements)[0].column.table
        text += "FOREIGN KEY(%s) REFERENCES %s (%s)" % (
            ', '.join(preparer.quote(f.parent.name)
                      for f in constraint.elements),
            self.define_constraint_remote_table(
                constraint, remote_table, preparer),
            ', '.join(preparer.quote(f.column.name)
                      for f in constraint.elements)
        )
        text += self.define_constraint_match(constraint)
        text += self.define_constraint_cascades(constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def define_constraint_remote_table(self, constraint, table, preparer):
        """Format the remote table clause of a CREATE CONSTRAINT clause."""

        return preparer.format_table(table)

    def visit_unique_constraint(self, constraint):
        if len(constraint) == 0:
            return ''
        text = ""
        if constraint.name is not None:
            formatted_name = self.preparer.format_constraint(constraint)
            text += "CONSTRAINT %s " % formatted_name
        text += "UNIQUE (%s)" % (
                ', '.join(self.preparer.quote(c.name)
                          for c in constraint))
        text += self.define_constraint_deferrability(constraint)
        return text

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete
        if constraint.onupdate is not None:
            text += " ON UPDATE %s" % constraint.onupdate
        return text

    def define_constraint_deferrability(self, constraint):
        text = ""
        if constraint.deferrable is not None:
            if constraint.deferrable:
                text += " DEFERRABLE"
            else:
                text += " NOT DEFERRABLE"
        if constraint.initially is not None:
            text += " INITIALLY %s" % constraint.initially
        return text

    def define_constraint_match(self, constraint):
        text = ""
        if constraint.match is not None:
            text += " MATCH %s" % constraint.match
        return text


class GenericTypeCompiler(TypeCompiler):

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_REAL(self, type_, **kw):
        return "REAL"

    def visit_NUMERIC(self, type_, **kw):
        if type_.precision is None:
            return "NUMERIC"
        elif type_.scale is None:
            return "NUMERIC(%(precision)s)" % \
                {'precision': type_.precision}
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % \
                {'precision': type_.precision,
                 'scale': type_.scale}

    def visit_DECIMAL(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % \
                {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % \
                {'precision': type_.precision,
                 'scale': type_.scale}

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_TIMESTAMP(self, type_, **kw):
        return 'TIMESTAMP'

    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        return "TIME"

    def visit_CLOB(self, type_, **kw):
        return "CLOB"

    def visit_NCLOB(self, type_, **kw):
        return "NCLOB"

    def _render_string_type(self, type_, name):

        text = name
        if type_.length:
            text += "(%d)" % type_.length
        if type_.collation:
            text += ' COLLATE "%s"' % type_.collation
        return text

    def visit_CHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR")

    def visit_NCHAR(self, type_, **kw):
        return self._render_string_type(type_, "NCHAR")

    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_NVARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "NVARCHAR")

    def visit_TEXT(self, type_, **kw):
        return self._render_string_type(type_, "TEXT")

    def visit_BLOB(self, type_, **kw):
        return "BLOB"

    def visit_BINARY(self, type_, **kw):
        return "BINARY" + (type_.length and "(%d)" % type_.length or "")

    def visit_VARBINARY(self, type_, **kw):
        return "VARBINARY" + (type_.length and "(%d)" % type_.length or "")

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_large_binary(self, type_, **kw):
        return self.visit_BLOB(type_, **kw)

    def visit_boolean(self, type_, **kw):
        return self.visit_BOOLEAN(type_, **kw)

    def visit_time(self, type_, **kw):
        return self.visit_TIME(type_, **kw)

    def visit_datetime(self, type_, **kw):
        return self.visit_DATETIME(type_, **kw)

    def visit_date(self, type_, **kw):
        return self.visit_DATE(type_, **kw)

    def visit_big_integer(self, type_, **kw):
        return self.visit_BIGINT(type_, **kw)

    def visit_small_integer(self, type_, **kw):
        return self.visit_SMALLINT(type_, **kw)

    def visit_integer(self, type_, **kw):
        return self.visit_INTEGER(type_, **kw)

    def visit_real(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_float(self, type_, **kw):
        return self.visit_FLOAT(type_, **kw)

    def visit_numeric(self, type_, **kw):
        return self.visit_NUMERIC(type_, **kw)

    def visit_string(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_unicode(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_text(self, type_, **kw):
        return self.visit_TEXT(type_, **kw)

    def visit_unicode_text(self, type_, **kw):
        return self.visit_TEXT(type_, **kw)

    def visit_enum(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_null(self, type_, **kw):
        raise exc.CompileError("Can't generate DDL for %r; "
                               "did you forget to specify a "
                               "type on this Column?" % type_)

    def visit_type_decorator(self, type_, **kw):
        return self.process(type_.type_engine(self.dialect), **kw)

    def visit_user_defined(self, type_, **kw):
        return type_.get_col_spec(**kw)


class IdentifierPreparer(object):

    """Handle quoting and case-folding of identifiers based on options."""

    reserved_words = RESERVED_WORDS

    legal_characters = LEGAL_CHARACTERS

    illegal_initial_characters = ILLEGAL_INITIAL_CHARACTERS

    def __init__(self, dialect, initial_quote='"',
                 final_quote=None, escape_quote='"', omit_schema=False):
        """Construct a new ``IdentifierPreparer`` object.

        initial_quote
          Character that begins a delimited identifier.

        final_quote
          Character that ends a delimited identifier. Defaults to
          `initial_quote`.

        omit_schema
          Prevent prepending schema name. Useful for databases that do
          not support schemae.
        """

        self.dialect = dialect
        self.initial_quote = initial_quote
        self.final_quote = final_quote or self.initial_quote
        self.escape_quote = escape_quote
        self.escape_to_quote = self.escape_quote * 2
        self.omit_schema = omit_schema
        self._strings = {}

    def _escape_identifier(self, value):
        """Escape an identifier.

        Subclasses should override this to provide database-dependent
        escaping behavior.
        """

        return value.replace(self.escape_quote, self.escape_to_quote)

    def _unescape_identifier(self, value):
        """Canonicalize an escaped identifier.

        Subclasses should override this to provide database-dependent
        unescaping behavior that reverses _escape_identifier.
        """

        return value.replace(self.escape_to_quote, self.escape_quote)

    def quote_identifier(self, value):
        """Quote an identifier.

        Subclasses should override this to provide database-dependent
        quoting behavior.
        """

        return self.initial_quote + \
            self._escape_identifier(value) + \
            self.final_quote

    def _requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (lc_value in self.reserved_words
                or value[0] in self.illegal_initial_characters
                or not self.legal_characters.match(util.text_type(value))
                or (lc_value != value))

    def quote_schema(self, schema, force=None):
        """Conditionally quote a schema.

        Subclasses can override this to provide database-dependent
        quoting behavior for schema names.

        the 'force' flag should be considered deprecated.

        """
        return self.quote(schema, force)

    def quote(self, ident, force=None):
        """Conditionally quote an identifier.

        the 'force' flag should be considered deprecated.
        """

        force = getattr(ident, "quote", None)

        if force is None:
            if ident in self._strings:
                return self._strings[ident]
            else:
                if self._requires_quotes(ident):
                    self._strings[ident] = self.quote_identifier(ident)
                else:
                    self._strings[ident] = ident
                return self._strings[ident]
        elif force:
            return self.quote_identifier(ident)
        else:
            return ident

    def format_sequence(self, sequence, use_schema=True):
        name = self.quote(sequence.name)
        if (not self.omit_schema and use_schema and
                sequence.schema is not None):
            name = self.quote_schema(sequence.schema) + "." + name
        return name

    def format_label(self, label, name=None):
        return self.quote(name or label.name)

    def format_alias(self, alias, name=None):
        return self.quote(name or alias.name)

    def format_savepoint(self, savepoint, name=None):
        return self.quote(name or savepoint.ident)

    @util.dependencies("sqlalchemy.sql.naming")
    def format_constraint(self, naming, constraint):
        if isinstance(constraint.name, elements._defer_name):
            name = naming._constraint_name_for_table(
                constraint, constraint.table)
            if name:
                return self.quote(name)
            elif isinstance(constraint.name, elements._defer_none_name):
                return None
        return self.quote(constraint.name)

    def format_table(self, table, use_schema=True, name=None):
        """Prepare a quoted table and schema name."""

        if name is None:
            name = table.name
        result = self.quote(name)
        if not self.omit_schema and use_schema \
                and getattr(table, "schema", None):
            result = self.quote_schema(table.schema) + "." + result
        return result

    def format_schema(self, name, quote=None):
        """Prepare a quoted schema name."""

        return self.quote(name, quote)

    def format_column(self, column, use_table=False,
                      name=None, table_name=None):
        """Prepare a quoted column name."""

        if name is None:
            name = column.name
        if not getattr(column, 'is_literal', False):
            if use_table:
                return self.format_table(
                    column.table, use_schema=False,
                    name=table_name) + "." + self.quote(name)
            else:
                return self.quote(name)
        else:
            # literal textual elements get stuck into ColumnClause a lot,
            # which shouldn't get quoted

            if use_table:
                return self.format_table(
                    column.table, use_schema=False,
                    name=table_name) + '.' + name
            else:
                return name

    def format_table_seq(self, table, use_schema=True):
        """Format table name and schema as a tuple."""

        # Dialects with more levels in their fully qualified references
        # ('database', 'owner', etc.) could override this and return
        # a longer sequence.

        if not self.omit_schema and use_schema and \
                getattr(table, 'schema', None):
            return (self.quote_schema(table.schema),
                    self.format_table(table, use_schema=False))
        else:
            return (self.format_table(table, use_schema=False), )

    @util.memoized_property
    def _r_identifiers(self):
        initial, final, escaped_final = \
            [re.escape(s) for s in
             (self.initial_quote, self.final_quote,
              self._escape_identifier(self.final_quote))]
        r = re.compile(
            r'(?:'
            r'(?:%(initial)s((?:%(escaped)s|[^%(final)s])+)%(final)s'
            r'|([^\.]+))(?=\.|$))+' %
            {'initial': initial,
             'final': final,
             'escaped': escaped_final})
        return r

    def unformat_identifiers(self, identifiers):
        """Unpack 'schema.table.column'-like strings into components."""

        r = self._r_identifiers
        return [self._unescape_identifier(i)
                for i in [a or b for a, b in r.findall(identifiers)]]
