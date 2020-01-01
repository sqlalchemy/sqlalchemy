# sql/crud.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Functions used by compiler.py to determine the parameters rendered
within INSERT and UPDATE statements.

"""
import operator

from . import dml
from . import elements
from .. import exc
from .. import util


REQUIRED = util.symbol(
    "REQUIRED",
    """
Placeholder for the value within a :class:`.BindParameter`
which is required to be present when the statement is passed
to :meth:`.Connection.execute`.

This symbol is typically used when a :func:`.expression.insert`
or :func:`.expression.update` statement is compiled without parameter
values present.

""",
)

ISINSERT = util.symbol("ISINSERT")
ISUPDATE = util.symbol("ISUPDATE")
ISDELETE = util.symbol("ISDELETE")


def _setup_crud_params(compiler, stmt, local_stmt_type, **kw):
    restore_isinsert = compiler.isinsert
    restore_isupdate = compiler.isupdate
    restore_isdelete = compiler.isdelete

    should_restore = (
        restore_isinsert or restore_isupdate or restore_isdelete
    ) or len(compiler.stack) > 1

    if local_stmt_type is ISINSERT:
        compiler.isupdate = False
        compiler.isinsert = True
    elif local_stmt_type is ISUPDATE:
        compiler.isupdate = True
        compiler.isinsert = False
    elif local_stmt_type is ISDELETE:
        if not should_restore:
            compiler.isdelete = True
    else:
        assert False, "ISINSERT, ISUPDATE, or ISDELETE expected"

    try:
        if local_stmt_type in (ISINSERT, ISUPDATE):
            return _get_crud_params(compiler, stmt, **kw)
    finally:
        if should_restore:
            compiler.isinsert = restore_isinsert
            compiler.isupdate = restore_isupdate
            compiler.isdelete = restore_isdelete


def _get_crud_params(compiler, stmt, **kw):
    """create a set of tuples representing column/string pairs for use
    in an INSERT or UPDATE statement.

    Also generates the Compiled object's postfetch, prefetch, and
    returning column collections, used for default handling and ultimately
    populating the ResultProxy's prefetch_cols() and postfetch_cols()
    collections.

    """

    compiler.postfetch = []
    compiler.insert_prefetch = []
    compiler.update_prefetch = []
    compiler.returning = []

    # no parameters in the statement, no parameters in the
    # compiled params - return binds for all columns
    if compiler.column_keys is None and stmt.parameters is None:
        return [
            (c, _create_bind_param(compiler, c, None, required=True))
            for c in stmt.table.columns
        ]

    if stmt._has_multi_parameters:
        stmt_parameters = stmt.parameters[0]
    else:
        stmt_parameters = stmt.parameters

    # getters - these are normally just column.key,
    # but in the case of mysql multi-table update, the rules for
    # .key must conditionally take tablename into account
    (
        _column_as_key,
        _getattr_col_key,
        _col_bind_name,
    ) = _key_getters_for_crud_column(compiler, stmt)

    # if we have statement parameters - set defaults in the
    # compiled params
    if compiler.column_keys is None:
        parameters = {}
    else:
        parameters = dict(
            (_column_as_key(key), REQUIRED)
            for key in compiler.column_keys
            if not stmt_parameters or key not in stmt_parameters
        )

    # create a list of column assignment clauses as tuples
    values = []

    if stmt_parameters is not None:
        _get_stmt_parameters_params(
            compiler, parameters, stmt_parameters, _column_as_key, values, kw
        )

    check_columns = {}

    # special logic that only occurs for multi-table UPDATE
    # statements
    if compiler.isupdate and stmt._extra_froms and stmt_parameters:
        _get_multitable_params(
            compiler,
            stmt,
            stmt_parameters,
            check_columns,
            _col_bind_name,
            _getattr_col_key,
            values,
            kw,
        )

    if compiler.isinsert and stmt.select_names:
        _scan_insert_from_select_cols(
            compiler,
            stmt,
            parameters,
            _getattr_col_key,
            _column_as_key,
            _col_bind_name,
            check_columns,
            values,
            kw,
        )
    else:
        _scan_cols(
            compiler,
            stmt,
            parameters,
            _getattr_col_key,
            _column_as_key,
            _col_bind_name,
            check_columns,
            values,
            kw,
        )

    if parameters and stmt_parameters:
        check = (
            set(parameters)
            .intersection(_column_as_key(k) for k in stmt_parameters)
            .difference(check_columns)
        )
        if check:
            raise exc.CompileError(
                "Unconsumed column names: %s"
                % (", ".join("%s" % c for c in check))
            )

    if stmt._has_multi_parameters:
        values = _extend_values_for_multiparams(compiler, stmt, values, kw)

    return values


def _create_bind_param(
    compiler, col, value, process=True, required=False, name=None, **kw
):
    if name is None:
        name = col.key
    bindparam = elements.BindParameter(
        name, value, type_=col.type, required=required
    )
    bindparam._is_crud = True
    if process:
        bindparam = bindparam._compiler_dispatch(compiler, **kw)
    return bindparam


def _key_getters_for_crud_column(compiler, stmt):
    if compiler.isupdate and stmt._extra_froms:
        # when extra tables are present, refer to the columns
        # in those extra tables as table-qualified, including in
        # dictionaries and when rendering bind param names.
        # the "main" table of the statement remains unqualified,
        # allowing the most compatibility with a non-multi-table
        # statement.
        _et = set(stmt._extra_froms)

        def _column_as_key(key):
            str_key = elements._column_as_key(key)
            if hasattr(key, "table") and key.table in _et:
                return (key.table.name, str_key)
            else:
                return str_key

        def _getattr_col_key(col):
            if col.table in _et:
                return (col.table.name, col.key)
            else:
                return col.key

        def _col_bind_name(col):
            if col.table in _et:
                return "%s_%s" % (col.table.name, col.key)
            else:
                return col.key

    else:
        _column_as_key = elements._column_as_key
        _getattr_col_key = _col_bind_name = operator.attrgetter("key")

    return _column_as_key, _getattr_col_key, _col_bind_name


def _scan_insert_from_select_cols(
    compiler,
    stmt,
    parameters,
    _getattr_col_key,
    _column_as_key,
    _col_bind_name,
    check_columns,
    values,
    kw,
):

    (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    ) = _get_returning_modifiers(compiler, stmt)

    cols = [stmt.table.c[_column_as_key(name)] for name in stmt.select_names]

    compiler._insert_from_select = stmt.select

    add_select_cols = []
    if stmt.include_insert_from_select_defaults:
        col_set = set(cols)
        for col in stmt.table.columns:
            if col not in col_set and col.default:
                cols.append(col)

    for c in cols:
        col_key = _getattr_col_key(c)
        if col_key in parameters and col_key not in check_columns:
            parameters.pop(col_key)
            values.append((c, None))
        else:
            _append_param_insert_select_hasdefault(
                compiler, stmt, c, add_select_cols, kw
            )

    if add_select_cols:
        values.extend(add_select_cols)
        compiler._insert_from_select = compiler._insert_from_select._generate()
        compiler._insert_from_select._raw_columns = tuple(
            compiler._insert_from_select._raw_columns
        ) + tuple(expr for col, expr in add_select_cols)


def _scan_cols(
    compiler,
    stmt,
    parameters,
    _getattr_col_key,
    _column_as_key,
    _col_bind_name,
    check_columns,
    values,
    kw,
):

    (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    ) = _get_returning_modifiers(compiler, stmt)

    if stmt._parameter_ordering:
        parameter_ordering = [
            _column_as_key(key) for key in stmt._parameter_ordering
        ]
        ordered_keys = set(parameter_ordering)
        cols = [stmt.table.c[key] for key in parameter_ordering] + [
            c for c in stmt.table.c if c.key not in ordered_keys
        ]
    else:
        cols = stmt.table.columns

    for c in cols:
        col_key = _getattr_col_key(c)

        if col_key in parameters and col_key not in check_columns:

            _append_param_parameter(
                compiler,
                stmt,
                c,
                col_key,
                parameters,
                _col_bind_name,
                implicit_returning,
                implicit_return_defaults,
                values,
                kw,
            )

        elif compiler.isinsert:
            if (
                c.primary_key
                and need_pks
                and (
                    implicit_returning
                    or not postfetch_lastrowid
                    or c is not stmt.table._autoincrement_column
                )
            ):

                if implicit_returning:
                    _append_param_insert_pk_returning(
                        compiler, stmt, c, values, kw
                    )
                else:
                    _append_param_insert_pk(compiler, stmt, c, values, kw)

            elif c.default is not None:

                _append_param_insert_hasdefault(
                    compiler, stmt, c, implicit_return_defaults, values, kw
                )

            elif c.server_default is not None:
                if implicit_return_defaults and c in implicit_return_defaults:
                    compiler.returning.append(c)
                elif not c.primary_key:
                    compiler.postfetch.append(c)
            elif implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            elif (
                c.primary_key
                and c is not stmt.table._autoincrement_column
                and not c.nullable
            ):
                _warn_pk_with_no_anticipated_value(c)

        elif compiler.isupdate:
            _append_param_update(
                compiler, stmt, c, implicit_return_defaults, values, kw
            )


def _append_param_parameter(
    compiler,
    stmt,
    c,
    col_key,
    parameters,
    _col_bind_name,
    implicit_returning,
    implicit_return_defaults,
    values,
    kw,
):
    value = parameters.pop(col_key)
    if elements._is_literal(value):
        value = _create_bind_param(
            compiler,
            c,
            value,
            required=value is REQUIRED,
            name=_col_bind_name(c)
            if not stmt._has_multi_parameters
            else "%s_m0" % _col_bind_name(c),
            **kw
        )
    else:
        if isinstance(value, elements.BindParameter) and value.type._isnull:
            value = value._clone()
            value.type = c.type

        if c.primary_key and implicit_returning:
            compiler.returning.append(c)
            value = compiler.process(value.self_group(), **kw)
        elif implicit_return_defaults and c in implicit_return_defaults:
            compiler.returning.append(c)
            value = compiler.process(value.self_group(), **kw)
        else:
            # postfetch specifically means, "we can SELECT the row we just
            # inserted by primary key to get back the server generated
            # defaults". so by definition this can't be used to get the primary
            # key value back, because we need to have it ahead of time.
            if not c.primary_key:
                compiler.postfetch.append(c)
            value = compiler.process(value.self_group(), **kw)
    values.append((c, value))


def _append_param_insert_pk_returning(compiler, stmt, c, values, kw):
    """Create a primary key expression in the INSERT statement and
    possibly a RETURNING clause for it.

    If the column has a Python-side default, we will create a bound
    parameter for it and "pre-execute" the Python function.  If
    the column has a SQL expression default, or is a sequence,
    we will add it directly into the INSERT statement and add a
    RETURNING element to get the new value.  If the column has a
    server side default or is marked as the "autoincrement" column,
    we will add a RETRUNING element to get at the value.

    If all the above tests fail, that indicates a primary key column with no
    noted default generation capabilities that has no parameter passed;
    raise an exception.

    """
    if c.default is not None:
        if c.default.is_sequence:
            if compiler.dialect.supports_sequences and (
                not c.default.optional
                or not compiler.dialect.sequences_optional
            ):
                proc = compiler.process(c.default, **kw)
                values.append((c, proc))
            compiler.returning.append(c)
        elif c.default.is_clause_element:
            values.append(
                (c, compiler.process(c.default.arg.self_group(), **kw))
            )
            compiler.returning.append(c)
        else:
            values.append((c, _create_insert_prefetch_bind_param(compiler, c)))
    elif c is stmt.table._autoincrement_column or c.server_default is not None:
        compiler.returning.append(c)
    elif not c.nullable:
        # no .default, no .server_default, not autoincrement, we have
        # no indication this primary key column will have any value
        _warn_pk_with_no_anticipated_value(c)


def _create_insert_prefetch_bind_param(compiler, c, process=True, name=None):
    param = _create_bind_param(compiler, c, None, process=process, name=name)
    compiler.insert_prefetch.append(c)
    return param


def _create_update_prefetch_bind_param(compiler, c, process=True, name=None):
    param = _create_bind_param(compiler, c, None, process=process, name=name)
    compiler.update_prefetch.append(c)
    return param


class _multiparam_column(elements.ColumnElement):
    _is_multiparam_column = True

    def __init__(self, original, index):
        self.index = index
        self.key = "%s_m%d" % (original.key, index + 1)
        self.original = original
        self.default = original.default
        self.type = original.type

    def __eq__(self, other):
        return (
            isinstance(other, _multiparam_column)
            and other.key == self.key
            and other.original == self.original
        )


def _process_multiparam_default_bind(compiler, stmt, c, index, kw):

    if not c.default:
        raise exc.CompileError(
            "INSERT value for column %s is explicitly rendered as a bound"
            "parameter in the VALUES clause; "
            "a Python-side value or SQL expression is required" % c
        )
    elif c.default.is_clause_element:
        return compiler.process(c.default.arg.self_group(), **kw)
    else:
        col = _multiparam_column(c, index)
        if isinstance(stmt, dml.Insert):
            return _create_insert_prefetch_bind_param(compiler, col)
        else:
            return _create_update_prefetch_bind_param(compiler, col)


def _append_param_insert_pk(compiler, stmt, c, values, kw):
    """Create a bound parameter in the INSERT statement to receive a
    'prefetched' default value.

    The 'prefetched' value indicates that we are to invoke a Python-side
    default function or expliclt SQL expression before the INSERT statement
    proceeds, so that we have a primary key value available.

    if the column has no noted default generation capabilities, it has
    no value passed in either; raise an exception.

    """
    if (
        # column has a Python-side default
        c.default is not None
        and (
            # and it won't be a Sequence
            not c.default.is_sequence
            or compiler.dialect.supports_sequences
        )
    ) or (
        # column is the "autoincrement column"
        c is stmt.table._autoincrement_column
        and (
            # and it's either a "sequence" or a
            # pre-executable "autoincrement" sequence
            compiler.dialect.supports_sequences
            or compiler.dialect.preexecute_autoincrement_sequences
        )
    ):
        values.append((c, _create_insert_prefetch_bind_param(compiler, c)))
    elif c.default is None and c.server_default is None and not c.nullable:
        # no .default, no .server_default, not autoincrement, we have
        # no indication this primary key column will have any value
        _warn_pk_with_no_anticipated_value(c)


def _append_param_insert_hasdefault(
    compiler, stmt, c, implicit_return_defaults, values, kw
):

    if c.default.is_sequence:
        if compiler.dialect.supports_sequences and (
            not c.default.optional or not compiler.dialect.sequences_optional
        ):
            proc = compiler.process(c.default, **kw)
            values.append((c, proc))
            if implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            elif not c.primary_key:
                compiler.postfetch.append(c)
    elif c.default.is_clause_element:
        proc = compiler.process(c.default.arg.self_group(), **kw)
        values.append((c, proc))

        if implicit_return_defaults and c in implicit_return_defaults:
            compiler.returning.append(c)
        elif not c.primary_key:
            # don't add primary key column to postfetch
            compiler.postfetch.append(c)
    else:
        values.append((c, _create_insert_prefetch_bind_param(compiler, c)))


def _append_param_insert_select_hasdefault(compiler, stmt, c, values, kw):

    if c.default.is_sequence:
        if compiler.dialect.supports_sequences and (
            not c.default.optional or not compiler.dialect.sequences_optional
        ):
            proc = c.default
            values.append((c, proc.next_value()))
    elif c.default.is_clause_element:
        proc = c.default.arg.self_group()
        values.append((c, proc))
    else:
        values.append(
            (c, _create_insert_prefetch_bind_param(compiler, c, process=False))
        )


def _append_param_update(
    compiler, stmt, c, implicit_return_defaults, values, kw
):

    if c.onupdate is not None and not c.onupdate.is_sequence:
        if c.onupdate.is_clause_element:
            values.append(
                (c, compiler.process(c.onupdate.arg.self_group(), **kw))
            )
            if implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            else:
                compiler.postfetch.append(c)
        else:
            values.append((c, _create_update_prefetch_bind_param(compiler, c)))
    elif c.server_onupdate is not None:
        if implicit_return_defaults and c in implicit_return_defaults:
            compiler.returning.append(c)
        else:
            compiler.postfetch.append(c)
    elif (
        implicit_return_defaults
        and stmt._return_defaults is not True
        and c in implicit_return_defaults
    ):
        compiler.returning.append(c)


def _get_multitable_params(
    compiler,
    stmt,
    stmt_parameters,
    check_columns,
    _col_bind_name,
    _getattr_col_key,
    values,
    kw,
):

    normalized_params = dict(
        (elements._clause_element_as_expr(c), param)
        for c, param in stmt_parameters.items()
    )
    affected_tables = set()
    for t in stmt._extra_froms:
        for c in t.c:
            if c in normalized_params:
                affected_tables.add(t)
                check_columns[_getattr_col_key(c)] = c
                value = normalized_params[c]
                if elements._is_literal(value):
                    value = _create_bind_param(
                        compiler,
                        c,
                        value,
                        required=value is REQUIRED,
                        name=_col_bind_name(c),
                    )
                else:
                    compiler.postfetch.append(c)
                    value = compiler.process(value.self_group(), **kw)
                values.append((c, value))
    # determine tables which are actually to be updated - process onupdate
    # and server_onupdate for these
    for t in affected_tables:
        for c in t.c:
            if c in normalized_params:
                continue
            elif c.onupdate is not None and not c.onupdate.is_sequence:
                if c.onupdate.is_clause_element:
                    values.append(
                        (
                            c,
                            compiler.process(
                                c.onupdate.arg.self_group(), **kw
                            ),
                        )
                    )
                    compiler.postfetch.append(c)
                else:
                    values.append(
                        (
                            c,
                            _create_update_prefetch_bind_param(
                                compiler, c, name=_col_bind_name(c)
                            ),
                        )
                    )
            elif c.server_onupdate is not None:
                compiler.postfetch.append(c)


def _extend_values_for_multiparams(compiler, stmt, values, kw):
    values_0 = values
    values = [values]

    for i, row in enumerate(stmt.parameters[1:]):
        extension = []
        for (col, param) in values_0:
            if col in row or col.key in row:
                key = col if col in row else col.key

                if elements._is_literal(row[key]):
                    new_param = _create_bind_param(
                        compiler,
                        col,
                        row[key],
                        name="%s_m%d" % (col.key, i + 1),
                        **kw
                    )
                else:
                    new_param = compiler.process(row[key].self_group(), **kw)
            else:
                new_param = _process_multiparam_default_bind(
                    compiler, stmt, col, i, kw
                )

            extension.append((col, new_param))

        values.append(extension)

    return values


def _get_stmt_parameters_params(
    compiler, parameters, stmt_parameters, _column_as_key, values, kw
):
    for k, v in stmt_parameters.items():
        colkey = _column_as_key(k)
        if colkey is not None:
            parameters.setdefault(colkey, v)
        else:
            # a non-Column expression on the left side;
            # add it to values() in an "as-is" state,
            # coercing right side to bound param
            if elements._is_literal(v):
                v = compiler.process(
                    elements.BindParameter(None, v, type_=k.type), **kw
                )
            else:
                v = compiler.process(v.self_group(), **kw)

            values.append((k, v))


def _get_returning_modifiers(compiler, stmt):
    need_pks = (
        compiler.isinsert
        and not compiler.inline
        and not stmt._returning
        and not stmt._has_multi_parameters
    )

    implicit_returning = (
        need_pks
        and compiler.dialect.implicit_returning
        and stmt.table.implicit_returning
    )

    if compiler.isinsert:
        implicit_return_defaults = implicit_returning and stmt._return_defaults
    elif compiler.isupdate:
        implicit_return_defaults = (
            compiler.dialect.implicit_returning
            and stmt.table.implicit_returning
            and stmt._return_defaults
        )
    else:
        # this line is unused, currently we are always
        # isinsert or isupdate
        implicit_return_defaults = False  # pragma: no cover

    if implicit_return_defaults:
        if stmt._return_defaults is True:
            implicit_return_defaults = set(stmt.table.c)
        else:
            implicit_return_defaults = set(stmt._return_defaults)

    postfetch_lastrowid = need_pks and compiler.dialect.postfetch_lastrowid

    return (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    )


def _warn_pk_with_no_anticipated_value(c):
    msg = (
        "Column '%s.%s' is marked as a member of the "
        "primary key for table '%s', "
        "but has no Python-side or server-side default generator indicated, "
        "nor does it indicate 'autoincrement=True' or 'nullable=True', "
        "and no explicit value is passed.  "
        "Primary key columns typically may not store NULL."
        % (c.table.fullname, c.name, c.table.fullname)
    )
    if len(c.table.primary_key) > 1:
        msg += (
            " Note that as of SQLAlchemy 1.1, 'autoincrement=True' must be "
            "indicated explicitly for composite (e.g. multicolumn) primary "
            "keys if AUTO_INCREMENT/SERIAL/IDENTITY "
            "behavior is expected for one of the columns in the primary key. "
            "CREATE TABLE statements are impacted by this change as well on "
            "most backends."
        )
    util.warn(msg)
