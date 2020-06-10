# orm/persistence.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""private module containing functions used to emit INSERT, UPDATE
and DELETE statements on behalf of a :class:`_orm.Mapper` and its descending
mappers.

The functions here are called only by the unit of work functions
in unitofwork.py.

"""

from itertools import chain
from itertools import groupby
import operator

from . import attributes
from . import evaluator
from . import exc as orm_exc
from . import loading
from . import sync
from .base import state_str
from .. import exc as sa_exc
from .. import future
from .. import sql
from .. import util
from ..future import select as future_select
from ..sql import coercions
from ..sql import expression
from ..sql import operators
from ..sql import roles
from ..sql.base import CompileState
from ..sql.base import Options
from ..sql.dml import DeleteDMLState
from ..sql.dml import UpdateDMLState
from ..sql.elements import BooleanClauseList
from ..sql.util import _entity_namespace_key


def _bulk_insert(
    mapper,
    mappings,
    session_transaction,
    isstates,
    return_defaults,
    render_nulls,
):
    base_mapper = mapper.base_mapper

    cached_connections = _cached_connection_dict(base_mapper)

    if session_transaction.session.connection_callable:
        raise NotImplementedError(
            "connection_callable / per-instance sharding "
            "not supported in bulk_insert()"
        )

    if isstates:
        if return_defaults:
            states = [(state, state.dict) for state in mappings]
            mappings = [dict_ for (state, dict_) in states]
        else:
            mappings = [state.dict for state in mappings]
    else:
        mappings = list(mappings)

    connection = session_transaction.connection(base_mapper)
    for table, super_mapper in base_mapper._sorted_tables.items():
        if not mapper.isa(super_mapper):
            continue

        records = (
            (
                None,
                state_dict,
                params,
                mapper,
                connection,
                value_params,
                has_all_pks,
                has_all_defaults,
            )
            for (
                state,
                state_dict,
                params,
                mp,
                conn,
                value_params,
                has_all_pks,
                has_all_defaults,
            ) in _collect_insert_commands(
                table,
                ((None, mapping, mapper, connection) for mapping in mappings),
                bulk=True,
                return_defaults=return_defaults,
                render_nulls=render_nulls,
            )
        )
        _emit_insert_statements(
            base_mapper,
            None,
            cached_connections,
            super_mapper,
            table,
            records,
            bookkeeping=return_defaults,
        )

    if return_defaults and isstates:
        identity_cls = mapper._identity_class
        identity_props = [p.key for p in mapper._identity_key_props]
        for state, dict_ in states:
            state.key = (
                identity_cls,
                tuple([dict_[key] for key in identity_props]),
            )


def _bulk_update(
    mapper, mappings, session_transaction, isstates, update_changed_only
):
    base_mapper = mapper.base_mapper

    cached_connections = _cached_connection_dict(base_mapper)

    search_keys = mapper._primary_key_propkeys
    if mapper._version_id_prop:
        search_keys = {mapper._version_id_prop.key}.union(search_keys)

    def _changed_dict(mapper, state):
        return dict(
            (k, v)
            for k, v in state.dict.items()
            if k in state.committed_state or k in search_keys
        )

    if isstates:
        if update_changed_only:
            mappings = [_changed_dict(mapper, state) for state in mappings]
        else:
            mappings = [state.dict for state in mappings]
    else:
        mappings = list(mappings)

    if session_transaction.session.connection_callable:
        raise NotImplementedError(
            "connection_callable / per-instance sharding "
            "not supported in bulk_update()"
        )

    connection = session_transaction.connection(base_mapper)

    for table, super_mapper in base_mapper._sorted_tables.items():
        if not mapper.isa(super_mapper):
            continue

        records = _collect_update_commands(
            None,
            table,
            (
                (
                    None,
                    mapping,
                    mapper,
                    connection,
                    (
                        mapping[mapper._version_id_prop.key]
                        if mapper._version_id_prop
                        else None
                    ),
                )
                for mapping in mappings
            ),
            bulk=True,
        )

        _emit_update_statements(
            base_mapper,
            None,
            cached_connections,
            super_mapper,
            table,
            records,
            bookkeeping=False,
        )


def save_obj(base_mapper, states, uowtransaction, single=False):
    """Issue ``INSERT`` and/or ``UPDATE`` statements for a list
    of objects.

    This is called within the context of a UOWTransaction during a
    flush operation, given a list of states to be flushed.  The
    base mapper in an inheritance hierarchy handles the inserts/
    updates for all descendant mappers.

    """

    # if batch=false, call _save_obj separately for each object
    if not single and not base_mapper.batch:
        for state in _sort_states(base_mapper, states):
            save_obj(base_mapper, [state], uowtransaction, single=True)
        return

    states_to_update = []
    states_to_insert = []
    cached_connections = _cached_connection_dict(base_mapper)

    for (
        state,
        dict_,
        mapper,
        connection,
        has_identity,
        row_switch,
        update_version_id,
    ) in _organize_states_for_save(base_mapper, states, uowtransaction):
        if has_identity or row_switch:
            states_to_update.append(
                (state, dict_, mapper, connection, update_version_id)
            )
        else:
            states_to_insert.append((state, dict_, mapper, connection))

    for table, mapper in base_mapper._sorted_tables.items():
        if table not in mapper._pks_by_table:
            continue
        insert = _collect_insert_commands(table, states_to_insert)

        update = _collect_update_commands(
            uowtransaction, table, states_to_update
        )

        _emit_update_statements(
            base_mapper,
            uowtransaction,
            cached_connections,
            mapper,
            table,
            update,
        )

        _emit_insert_statements(
            base_mapper,
            uowtransaction,
            cached_connections,
            mapper,
            table,
            insert,
        )

    _finalize_insert_update_commands(
        base_mapper,
        uowtransaction,
        chain(
            (
                (state, state_dict, mapper, connection, False)
                for (state, state_dict, mapper, connection) in states_to_insert
            ),
            (
                (state, state_dict, mapper, connection, True)
                for (
                    state,
                    state_dict,
                    mapper,
                    connection,
                    update_version_id,
                ) in states_to_update
            ),
        ),
    )


def post_update(base_mapper, states, uowtransaction, post_update_cols):
    """Issue UPDATE statements on behalf of a relationship() which
    specifies post_update.

    """
    cached_connections = _cached_connection_dict(base_mapper)

    states_to_update = list(
        _organize_states_for_post_update(base_mapper, states, uowtransaction)
    )

    for table, mapper in base_mapper._sorted_tables.items():
        if table not in mapper._pks_by_table:
            continue

        update = (
            (
                state,
                state_dict,
                sub_mapper,
                connection,
                mapper._get_committed_state_attr_by_column(
                    state, state_dict, mapper.version_id_col
                )
                if mapper.version_id_col is not None
                else None,
            )
            for state, state_dict, sub_mapper, connection in states_to_update
            if table in sub_mapper._pks_by_table
        )

        update = _collect_post_update_commands(
            base_mapper, uowtransaction, table, update, post_update_cols
        )

        _emit_post_update_statements(
            base_mapper,
            uowtransaction,
            cached_connections,
            mapper,
            table,
            update,
        )


def delete_obj(base_mapper, states, uowtransaction):
    """Issue ``DELETE`` statements for a list of objects.

    This is called within the context of a UOWTransaction during a
    flush operation.

    """

    cached_connections = _cached_connection_dict(base_mapper)

    states_to_delete = list(
        _organize_states_for_delete(base_mapper, states, uowtransaction)
    )

    table_to_mapper = base_mapper._sorted_tables

    for table in reversed(list(table_to_mapper.keys())):
        mapper = table_to_mapper[table]
        if table not in mapper._pks_by_table:
            continue
        elif mapper.inherits and mapper.passive_deletes:
            continue

        delete = _collect_delete_commands(
            base_mapper, uowtransaction, table, states_to_delete
        )

        _emit_delete_statements(
            base_mapper,
            uowtransaction,
            cached_connections,
            mapper,
            table,
            delete,
        )

    for (
        state,
        state_dict,
        mapper,
        connection,
        update_version_id,
    ) in states_to_delete:
        mapper.dispatch.after_delete(mapper, connection, state)


def _organize_states_for_save(base_mapper, states, uowtransaction):
    """Make an initial pass across a set of states for INSERT or
    UPDATE.

    This includes splitting out into distinct lists for
    each, calling before_insert/before_update, obtaining
    key information for each state including its dictionary,
    mapper, the connection to use for the execution per state,
    and the identity flag.

    """

    for state, dict_, mapper, connection in _connections_for_states(
        base_mapper, uowtransaction, states
    ):

        has_identity = bool(state.key)

        instance_key = state.key or mapper._identity_key_from_state(state)

        row_switch = update_version_id = None

        # call before_XXX extensions
        if not has_identity:
            mapper.dispatch.before_insert(mapper, connection, state)
        else:
            mapper.dispatch.before_update(mapper, connection, state)

        if mapper._validate_polymorphic_identity:
            mapper._validate_polymorphic_identity(mapper, state, dict_)

        # detect if we have a "pending" instance (i.e. has
        # no instance_key attached to it), and another instance
        # with the same identity key already exists as persistent.
        # convert to an UPDATE if so.
        if (
            not has_identity
            and instance_key in uowtransaction.session.identity_map
        ):
            instance = uowtransaction.session.identity_map[instance_key]
            existing = attributes.instance_state(instance)

            if not uowtransaction.was_already_deleted(existing):
                if not uowtransaction.is_deleted(existing):
                    util.warn(
                        "New instance %s with identity key %s conflicts "
                        "with persistent instance %s"
                        % (state_str(state), instance_key, state_str(existing))
                    )
                else:
                    base_mapper._log_debug(
                        "detected row switch for identity %s.  "
                        "will update %s, remove %s from "
                        "transaction",
                        instance_key,
                        state_str(state),
                        state_str(existing),
                    )

                    # remove the "delete" flag from the existing element
                    uowtransaction.remove_state_actions(existing)
                    row_switch = existing

        if (has_identity or row_switch) and mapper.version_id_col is not None:
            update_version_id = mapper._get_committed_state_attr_by_column(
                row_switch if row_switch else state,
                row_switch.dict if row_switch else dict_,
                mapper.version_id_col,
            )

        yield (
            state,
            dict_,
            mapper,
            connection,
            has_identity,
            row_switch,
            update_version_id,
        )


def _organize_states_for_post_update(base_mapper, states, uowtransaction):
    """Make an initial pass across a set of states for UPDATE
    corresponding to post_update.

    This includes obtaining key information for each state
    including its dictionary, mapper, the connection to use for
    the execution per state.

    """
    return _connections_for_states(base_mapper, uowtransaction, states)


def _organize_states_for_delete(base_mapper, states, uowtransaction):
    """Make an initial pass across a set of states for DELETE.

    This includes calling out before_delete and obtaining
    key information for each state including its dictionary,
    mapper, the connection to use for the execution per state.

    """
    for state, dict_, mapper, connection in _connections_for_states(
        base_mapper, uowtransaction, states
    ):

        mapper.dispatch.before_delete(mapper, connection, state)

        if mapper.version_id_col is not None:
            update_version_id = mapper._get_committed_state_attr_by_column(
                state, dict_, mapper.version_id_col
            )
        else:
            update_version_id = None

        yield (state, dict_, mapper, connection, update_version_id)


def _collect_insert_commands(
    table,
    states_to_insert,
    bulk=False,
    return_defaults=False,
    render_nulls=False,
):
    """Identify sets of values to use in INSERT statements for a
    list of states.

    """
    for state, state_dict, mapper, connection in states_to_insert:
        if table not in mapper._pks_by_table:
            continue

        params = {}
        value_params = {}

        propkey_to_col = mapper._propkey_to_col[table]

        eval_none = mapper._insert_cols_evaluating_none[table]

        for propkey in set(propkey_to_col).intersection(state_dict):
            value = state_dict[propkey]
            col = propkey_to_col[propkey]
            if value is None and col not in eval_none and not render_nulls:
                continue
            elif not bulk and (
                hasattr(value, "__clause_element__")
                or isinstance(value, sql.ClauseElement)
            ):
                value_params[col] = (
                    value.__clause_element__()
                    if hasattr(value, "__clause_element__")
                    else value
                )
            else:
                params[col.key] = value

        if not bulk:
            # for all the columns that have no default and we don't have
            # a value and where "None" is not a special value, add
            # explicit None to the INSERT.   This is a legacy behavior
            # which might be worth removing, as it should not be necessary
            # and also produces confusion, given that "missing" and None
            # now have distinct meanings
            for colkey in (
                mapper._insert_cols_as_none[table]
                .difference(params)
                .difference([c.key for c in value_params])
            ):
                params[colkey] = None

        if not bulk or return_defaults:
            # params are in terms of Column key objects, so
            # compare to pk_keys_by_table
            has_all_pks = mapper._pk_keys_by_table[table].issubset(params)

            if mapper.base_mapper.eager_defaults:
                has_all_defaults = mapper._server_default_cols[table].issubset(
                    params
                )
            else:
                has_all_defaults = True
        else:
            has_all_defaults = has_all_pks = True

        if (
            mapper.version_id_generator is not False
            and mapper.version_id_col is not None
            and mapper.version_id_col in mapper._cols_by_table[table]
        ):
            params[mapper.version_id_col.key] = mapper.version_id_generator(
                None
            )

        yield (
            state,
            state_dict,
            params,
            mapper,
            connection,
            value_params,
            has_all_pks,
            has_all_defaults,
        )


def _collect_update_commands(
    uowtransaction, table, states_to_update, bulk=False
):
    """Identify sets of values to use in UPDATE statements for a
    list of states.

    This function works intricately with the history system
    to determine exactly what values should be updated
    as well as how the row should be matched within an UPDATE
    statement.  Includes some tricky scenarios where the primary
    key of an object might have been changed.

    """

    for (
        state,
        state_dict,
        mapper,
        connection,
        update_version_id,
    ) in states_to_update:

        if table not in mapper._pks_by_table:
            continue

        pks = mapper._pks_by_table[table]

        value_params = {}

        propkey_to_col = mapper._propkey_to_col[table]

        if bulk:
            # keys here are mapped attribute keys, so
            # look at mapper attribute keys for pk
            params = dict(
                (propkey_to_col[propkey].key, state_dict[propkey])
                for propkey in set(propkey_to_col)
                .intersection(state_dict)
                .difference(mapper._pk_attr_keys_by_table[table])
            )
            has_all_defaults = True
        else:
            params = {}
            for propkey in set(propkey_to_col).intersection(
                state.committed_state
            ):
                value = state_dict[propkey]
                col = propkey_to_col[propkey]

                if hasattr(value, "__clause_element__") or isinstance(
                    value, sql.ClauseElement
                ):
                    value_params[col] = (
                        value.__clause_element__()
                        if hasattr(value, "__clause_element__")
                        else value
                    )
                # guard against values that generate non-__nonzero__
                # objects for __eq__()
                elif (
                    state.manager[propkey].impl.is_equal(
                        value, state.committed_state[propkey]
                    )
                    is not True
                ):
                    params[col.key] = value

            if mapper.base_mapper.eager_defaults:
                has_all_defaults = (
                    mapper._server_onupdate_default_cols[table]
                ).issubset(params)
            else:
                has_all_defaults = True

        if (
            update_version_id is not None
            and mapper.version_id_col in mapper._cols_by_table[table]
        ):

            if not bulk and not (params or value_params):
                # HACK: check for history in other tables, in case the
                # history is only in a different table than the one
                # where the version_id_col is.  This logic was lost
                # from 0.9 -> 1.0.0 and restored in 1.0.6.
                for prop in mapper._columntoproperty.values():
                    history = state.manager[prop.key].impl.get_history(
                        state, state_dict, attributes.PASSIVE_NO_INITIALIZE
                    )
                    if history.added:
                        break
                else:
                    # no net change, break
                    continue

            col = mapper.version_id_col
            no_params = not params and not value_params
            params[col._label] = update_version_id

            if (
                bulk or col.key not in params
            ) and mapper.version_id_generator is not False:
                val = mapper.version_id_generator(update_version_id)
                params[col.key] = val
            elif mapper.version_id_generator is False and no_params:
                # no version id generator, no values set on the table,
                # and version id wasn't manually incremented.
                # set version id to itself so we get an UPDATE
                # statement
                params[col.key] = update_version_id

        elif not (params or value_params):
            continue

        has_all_pks = True
        expect_pk_cascaded = False
        if bulk:
            # keys here are mapped attribute keys, so
            # look at mapper attribute keys for pk
            pk_params = dict(
                (propkey_to_col[propkey]._label, state_dict.get(propkey))
                for propkey in set(propkey_to_col).intersection(
                    mapper._pk_attr_keys_by_table[table]
                )
            )
        else:
            pk_params = {}
            for col in pks:
                propkey = mapper._columntoproperty[col].key

                history = state.manager[propkey].impl.get_history(
                    state, state_dict, attributes.PASSIVE_OFF
                )

                if history.added:
                    if (
                        not history.deleted
                        or ("pk_cascaded", state, col)
                        in uowtransaction.attributes
                    ):
                        expect_pk_cascaded = True
                        pk_params[col._label] = history.added[0]
                        params.pop(col.key, None)
                    else:
                        # else, use the old value to locate the row
                        pk_params[col._label] = history.deleted[0]
                        if col in value_params:
                            has_all_pks = False
                else:
                    pk_params[col._label] = history.unchanged[0]
                if pk_params[col._label] is None:
                    raise orm_exc.FlushError(
                        "Can't update table %s using NULL for primary "
                        "key value on column %s" % (table, col)
                    )

        if params or value_params:
            params.update(pk_params)
            yield (
                state,
                state_dict,
                params,
                mapper,
                connection,
                value_params,
                has_all_defaults,
                has_all_pks,
            )
        elif expect_pk_cascaded:
            # no UPDATE occurs on this table, but we expect that CASCADE rules
            # have changed the primary key of the row; propagate this event to
            # other columns that expect to have been modified. this normally
            # occurs after the UPDATE is emitted however we invoke it here
            # explicitly in the absence of our invoking an UPDATE
            for m, equated_pairs in mapper._table_to_equated[table]:
                sync.populate(
                    state,
                    m,
                    state,
                    m,
                    equated_pairs,
                    uowtransaction,
                    mapper.passive_updates,
                )


def _collect_post_update_commands(
    base_mapper, uowtransaction, table, states_to_update, post_update_cols
):
    """Identify sets of values to use in UPDATE statements for a
    list of states within a post_update operation.

    """

    for (
        state,
        state_dict,
        mapper,
        connection,
        update_version_id,
    ) in states_to_update:

        # assert table in mapper._pks_by_table

        pks = mapper._pks_by_table[table]
        params = {}
        hasdata = False

        for col in mapper._cols_by_table[table]:
            if col in pks:
                params[col._label] = mapper._get_state_attr_by_column(
                    state, state_dict, col, passive=attributes.PASSIVE_OFF
                )

            elif col in post_update_cols or col.onupdate is not None:
                prop = mapper._columntoproperty[col]
                history = state.manager[prop.key].impl.get_history(
                    state, state_dict, attributes.PASSIVE_NO_INITIALIZE
                )
                if history.added:
                    value = history.added[0]
                    params[col.key] = value
                    hasdata = True
        if hasdata:
            if (
                update_version_id is not None
                and mapper.version_id_col in mapper._cols_by_table[table]
            ):

                col = mapper.version_id_col
                params[col._label] = update_version_id

                if (
                    bool(state.key)
                    and col.key not in params
                    and mapper.version_id_generator is not False
                ):
                    val = mapper.version_id_generator(update_version_id)
                    params[col.key] = val
            yield state, state_dict, mapper, connection, params


def _collect_delete_commands(
    base_mapper, uowtransaction, table, states_to_delete
):
    """Identify values to use in DELETE statements for a list of
    states to be deleted."""

    for (
        state,
        state_dict,
        mapper,
        connection,
        update_version_id,
    ) in states_to_delete:

        if table not in mapper._pks_by_table:
            continue

        params = {}
        for col in mapper._pks_by_table[table]:
            params[
                col.key
            ] = value = mapper._get_committed_state_attr_by_column(
                state, state_dict, col
            )
            if value is None:
                raise orm_exc.FlushError(
                    "Can't delete from table %s "
                    "using NULL for primary "
                    "key value on column %s" % (table, col)
                )

        if (
            update_version_id is not None
            and mapper.version_id_col in mapper._cols_by_table[table]
        ):
            params[mapper.version_id_col.key] = update_version_id
        yield params, connection


def _emit_update_statements(
    base_mapper,
    uowtransaction,
    cached_connections,
    mapper,
    table,
    update,
    bookkeeping=True,
):
    """Emit UPDATE statements corresponding to value lists collected
    by _collect_update_commands()."""

    needs_version_id = (
        mapper.version_id_col is not None
        and mapper.version_id_col in mapper._cols_by_table[table]
    )

    def update_stmt():
        clauses = BooleanClauseList._construct_raw(operators.and_)

        for col in mapper._pks_by_table[table]:
            clauses.clauses.append(
                col == sql.bindparam(col._label, type_=col.type)
            )

        if needs_version_id:
            clauses.clauses.append(
                mapper.version_id_col
                == sql.bindparam(
                    mapper.version_id_col._label,
                    type_=mapper.version_id_col.type,
                )
            )

        stmt = table.update(clauses)
        return stmt

    cached_stmt = base_mapper._memo(("update", table), update_stmt)

    for (
        (connection, paramkeys, hasvalue, has_all_defaults, has_all_pks),
        records,
    ) in groupby(
        update,
        lambda rec: (
            rec[4],  # connection
            set(rec[2]),  # set of parameter keys
            bool(rec[5]),  # whether or not we have "value" parameters
            rec[6],  # has_all_defaults
            rec[7],  # has all pks
        ),
    ):
        rows = 0
        records = list(records)

        statement = cached_stmt
        return_defaults = False

        if not has_all_pks:
            statement = statement.return_defaults()
            return_defaults = True
        elif (
            bookkeeping
            and not has_all_defaults
            and mapper.base_mapper.eager_defaults
        ):
            statement = statement.return_defaults()
            return_defaults = True
        elif mapper.version_id_col is not None:
            statement = statement.return_defaults(mapper.version_id_col)
            return_defaults = True

        assert_singlerow = (
            connection.dialect.supports_sane_rowcount
            if not return_defaults
            else connection.dialect.supports_sane_rowcount_returning
        )

        assert_multirow = (
            assert_singlerow
            and connection.dialect.supports_sane_multi_rowcount
        )
        allow_multirow = has_all_defaults and not needs_version_id

        if hasvalue:
            for (
                state,
                state_dict,
                params,
                mapper,
                connection,
                value_params,
                has_all_defaults,
                has_all_pks,
            ) in records:
                c = connection.execute(statement.values(value_params), params)
                if bookkeeping:
                    _postfetch(
                        mapper,
                        uowtransaction,
                        table,
                        state,
                        state_dict,
                        c,
                        c.context.compiled_parameters[0],
                        value_params,
                        True,
                    )
                rows += c.rowcount
                check_rowcount = assert_singlerow
        else:
            if not allow_multirow:
                check_rowcount = assert_singlerow
                for (
                    state,
                    state_dict,
                    params,
                    mapper,
                    connection,
                    value_params,
                    has_all_defaults,
                    has_all_pks,
                ) in records:
                    c = cached_connections[connection].execute(
                        statement, params
                    )

                    # TODO: why with bookkeeping=False?
                    if bookkeeping:
                        _postfetch(
                            mapper,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            c,
                            c.context.compiled_parameters[0],
                            value_params,
                            True,
                        )
                    rows += c.rowcount
            else:
                multiparams = [rec[2] for rec in records]

                check_rowcount = assert_multirow or (
                    assert_singlerow and len(multiparams) == 1
                )

                c = cached_connections[connection].execute(
                    statement, multiparams
                )

                rows += c.rowcount

                for (
                    state,
                    state_dict,
                    params,
                    mapper,
                    connection,
                    value_params,
                    has_all_defaults,
                    has_all_pks,
                ) in records:
                    if bookkeeping:
                        _postfetch(
                            mapper,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            c,
                            c.context.compiled_parameters[0],
                            value_params,
                            True,
                        )

        if check_rowcount:
            if rows != len(records):
                raise orm_exc.StaleDataError(
                    "UPDATE statement on table '%s' expected to "
                    "update %d row(s); %d were matched."
                    % (table.description, len(records), rows)
                )

        elif needs_version_id:
            util.warn(
                "Dialect %s does not support updated rowcount "
                "- versioning cannot be verified."
                % c.dialect.dialect_description
            )


def _emit_insert_statements(
    base_mapper,
    uowtransaction,
    cached_connections,
    mapper,
    table,
    insert,
    bookkeeping=True,
):
    """Emit INSERT statements corresponding to value lists collected
    by _collect_insert_commands()."""

    cached_stmt = base_mapper._memo(("insert", table), table.insert)

    for (
        (connection, pkeys, hasvalue, has_all_pks, has_all_defaults),
        records,
    ) in groupby(
        insert,
        lambda rec: (
            rec[4],  # connection
            set(rec[2]),  # parameter keys
            bool(rec[5]),  # whether we have "value" parameters
            rec[6],
            rec[7],
        ),
    ):

        statement = cached_stmt

        if (
            not bookkeeping
            or (
                has_all_defaults
                or not base_mapper.eager_defaults
                or not connection.dialect.implicit_returning
            )
            and has_all_pks
            and not hasvalue
        ):

            records = list(records)
            multiparams = [rec[2] for rec in records]

            c = cached_connections[connection].execute(statement, multiparams)
            if bookkeeping:
                for (
                    (
                        state,
                        state_dict,
                        params,
                        mapper_rec,
                        conn,
                        value_params,
                        has_all_pks,
                        has_all_defaults,
                    ),
                    last_inserted_params,
                ) in zip(records, c.context.compiled_parameters):
                    if state:
                        _postfetch(
                            mapper_rec,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            c,
                            last_inserted_params,
                            value_params,
                            False,
                        )
                    else:
                        _postfetch_bulk_save(mapper_rec, state_dict, table)

        else:
            if not has_all_defaults and base_mapper.eager_defaults:
                statement = statement.return_defaults()
            elif mapper.version_id_col is not None:
                statement = statement.return_defaults(mapper.version_id_col)

            for (
                state,
                state_dict,
                params,
                mapper_rec,
                connection,
                value_params,
                has_all_pks,
                has_all_defaults,
            ) in records:

                if value_params:
                    result = connection.execute(
                        statement.values(value_params), params
                    )
                else:
                    result = cached_connections[connection].execute(
                        statement, params
                    )

                primary_key = result.context.inserted_primary_key
                if primary_key is not None:
                    # set primary key attributes
                    for pk, col in zip(
                        primary_key, mapper._pks_by_table[table]
                    ):
                        prop = mapper_rec._columntoproperty[col]
                        if pk is not None and (
                            col in value_params
                            or state_dict.get(prop.key) is None
                        ):
                            state_dict[prop.key] = pk
                if bookkeeping:
                    if state:
                        _postfetch(
                            mapper_rec,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            result,
                            result.context.compiled_parameters[0],
                            value_params,
                            False,
                        )
                    else:
                        _postfetch_bulk_save(mapper_rec, state_dict, table)


def _emit_post_update_statements(
    base_mapper, uowtransaction, cached_connections, mapper, table, update
):
    """Emit UPDATE statements corresponding to value lists collected
    by _collect_post_update_commands()."""

    needs_version_id = (
        mapper.version_id_col is not None
        and mapper.version_id_col in mapper._cols_by_table[table]
    )

    def update_stmt():
        clauses = BooleanClauseList._construct_raw(operators.and_)

        for col in mapper._pks_by_table[table]:
            clauses.clauses.append(
                col == sql.bindparam(col._label, type_=col.type)
            )

        if needs_version_id:
            clauses.clauses.append(
                mapper.version_id_col
                == sql.bindparam(
                    mapper.version_id_col._label,
                    type_=mapper.version_id_col.type,
                )
            )

        stmt = table.update(clauses)

        if mapper.version_id_col is not None:
            stmt = stmt.return_defaults(mapper.version_id_col)

        return stmt

    statement = base_mapper._memo(("post_update", table), update_stmt)

    # execute each UPDATE in the order according to the original
    # list of states to guarantee row access order, but
    # also group them into common (connection, cols) sets
    # to support executemany().
    for key, records in groupby(
        update,
        lambda rec: (rec[3], set(rec[4])),  # connection  # parameter keys
    ):
        rows = 0

        records = list(records)
        connection = key[0]

        assert_singlerow = (
            connection.dialect.supports_sane_rowcount
            if mapper.version_id_col is None
            else connection.dialect.supports_sane_rowcount_returning
        )
        assert_multirow = (
            assert_singlerow
            and connection.dialect.supports_sane_multi_rowcount
        )
        allow_multirow = not needs_version_id or assert_multirow

        if not allow_multirow:
            check_rowcount = assert_singlerow
            for state, state_dict, mapper_rec, connection, params in records:
                c = cached_connections[connection].execute(statement, params)
                _postfetch_post_update(
                    mapper_rec,
                    uowtransaction,
                    table,
                    state,
                    state_dict,
                    c,
                    c.context.compiled_parameters[0],
                )
                rows += c.rowcount
        else:
            multiparams = [
                params
                for state, state_dict, mapper_rec, conn, params in records
            ]

            check_rowcount = assert_multirow or (
                assert_singlerow and len(multiparams) == 1
            )

            c = cached_connections[connection].execute(statement, multiparams)

            rows += c.rowcount
            for state, state_dict, mapper_rec, connection, params in records:
                _postfetch_post_update(
                    mapper_rec,
                    uowtransaction,
                    table,
                    state,
                    state_dict,
                    c,
                    c.context.compiled_parameters[0],
                )

        if check_rowcount:
            if rows != len(records):
                raise orm_exc.StaleDataError(
                    "UPDATE statement on table '%s' expected to "
                    "update %d row(s); %d were matched."
                    % (table.description, len(records), rows)
                )

        elif needs_version_id:
            util.warn(
                "Dialect %s does not support updated rowcount "
                "- versioning cannot be verified."
                % c.dialect.dialect_description
            )


def _emit_delete_statements(
    base_mapper, uowtransaction, cached_connections, mapper, table, delete
):
    """Emit DELETE statements corresponding to value lists collected
    by _collect_delete_commands()."""

    need_version_id = (
        mapper.version_id_col is not None
        and mapper.version_id_col in mapper._cols_by_table[table]
    )

    def delete_stmt():
        clauses = BooleanClauseList._construct_raw(operators.and_)

        for col in mapper._pks_by_table[table]:
            clauses.clauses.append(
                col == sql.bindparam(col.key, type_=col.type)
            )

        if need_version_id:
            clauses.clauses.append(
                mapper.version_id_col
                == sql.bindparam(
                    mapper.version_id_col.key, type_=mapper.version_id_col.type
                )
            )

        return table.delete(clauses)

    statement = base_mapper._memo(("delete", table), delete_stmt)
    for connection, recs in groupby(delete, lambda rec: rec[1]):  # connection
        del_objects = [params for params, connection in recs]

        connection = cached_connections[connection]

        expected = len(del_objects)
        rows_matched = -1
        only_warn = False

        if (
            need_version_id
            and not connection.dialect.supports_sane_multi_rowcount
        ):
            if connection.dialect.supports_sane_rowcount:
                rows_matched = 0
                # execute deletes individually so that versioned
                # rows can be verified
                for params in del_objects:
                    c = connection.execute(statement, params)
                    rows_matched += c.rowcount
            else:
                util.warn(
                    "Dialect %s does not support deleted rowcount "
                    "- versioning cannot be verified."
                    % connection.dialect.dialect_description
                )
                connection.execute(statement, del_objects)
        else:
            c = connection.execute(statement, del_objects)

            if not need_version_id:
                only_warn = True

            rows_matched = c.rowcount

        if (
            base_mapper.confirm_deleted_rows
            and rows_matched > -1
            and expected != rows_matched
            and (
                connection.dialect.supports_sane_multi_rowcount
                or len(del_objects) == 1
            )
        ):
            # TODO: why does this "only warn" if versioning is turned off,
            # whereas the UPDATE raises?
            if only_warn:
                util.warn(
                    "DELETE statement on table '%s' expected to "
                    "delete %d row(s); %d were matched.  Please set "
                    "confirm_deleted_rows=False within the mapper "
                    "configuration to prevent this warning."
                    % (table.description, expected, rows_matched)
                )
            else:
                raise orm_exc.StaleDataError(
                    "DELETE statement on table '%s' expected to "
                    "delete %d row(s); %d were matched.  Please set "
                    "confirm_deleted_rows=False within the mapper "
                    "configuration to prevent this warning."
                    % (table.description, expected, rows_matched)
                )


def _finalize_insert_update_commands(base_mapper, uowtransaction, states):
    """finalize state on states that have been inserted or updated,
    including calling after_insert/after_update events.

    """
    for state, state_dict, mapper, connection, has_identity in states:

        if mapper._readonly_props:
            readonly = state.unmodified_intersection(
                [
                    p.key
                    for p in mapper._readonly_props
                    if (
                        p.expire_on_flush
                        and (not p.deferred or p.key in state.dict)
                    )
                    or (
                        not p.expire_on_flush
                        and not p.deferred
                        and p.key not in state.dict
                    )
                ]
            )
            if readonly:
                state._expire_attributes(state.dict, readonly)

        # if eager_defaults option is enabled, load
        # all expired cols.  Else if we have a version_id_col, make sure
        # it isn't expired.
        toload_now = []

        if base_mapper.eager_defaults:
            toload_now.extend(
                state._unloaded_non_object.intersection(
                    mapper._server_default_plus_onupdate_propkeys
                )
            )

        if (
            mapper.version_id_col is not None
            and mapper.version_id_generator is False
        ):
            if mapper._version_id_prop.key in state.unloaded:
                toload_now.extend([mapper._version_id_prop.key])

        if toload_now:
            state.key = base_mapper._identity_key_from_state(state)
            stmt = future.select(mapper).apply_labels()
            loading.load_on_ident(
                uowtransaction.session,
                stmt,
                state.key,
                refresh_state=state,
                only_load_props=toload_now,
            )

        # call after_XXX extensions
        if not has_identity:
            mapper.dispatch.after_insert(mapper, connection, state)
        else:
            mapper.dispatch.after_update(mapper, connection, state)

        if (
            mapper.version_id_generator is False
            and mapper.version_id_col is not None
        ):
            if state_dict[mapper._version_id_prop.key] is None:
                raise orm_exc.FlushError(
                    "Instance does not contain a non-NULL version value"
                )


def _postfetch_post_update(
    mapper, uowtransaction, table, state, dict_, result, params
):
    if uowtransaction.is_deleted(state):
        return

    prefetch_cols = result.context.compiled.prefetch
    postfetch_cols = result.context.compiled.postfetch

    if (
        mapper.version_id_col is not None
        and mapper.version_id_col in mapper._cols_by_table[table]
    ):
        prefetch_cols = list(prefetch_cols) + [mapper.version_id_col]

    refresh_flush = bool(mapper.class_manager.dispatch.refresh_flush)
    if refresh_flush:
        load_evt_attrs = []

    for c in prefetch_cols:
        if c.key in params and c in mapper._columntoproperty:
            dict_[mapper._columntoproperty[c].key] = params[c.key]
            if refresh_flush:
                load_evt_attrs.append(mapper._columntoproperty[c].key)

    if refresh_flush and load_evt_attrs:
        mapper.class_manager.dispatch.refresh_flush(
            state, uowtransaction, load_evt_attrs
        )

    if postfetch_cols:
        state._expire_attributes(
            state.dict,
            [
                mapper._columntoproperty[c].key
                for c in postfetch_cols
                if c in mapper._columntoproperty
            ],
        )


def _postfetch(
    mapper,
    uowtransaction,
    table,
    state,
    dict_,
    result,
    params,
    value_params,
    isupdate,
):
    """Expire attributes in need of newly persisted database state,
    after an INSERT or UPDATE statement has proceeded for that
    state."""

    prefetch_cols = result.context.compiled.prefetch
    postfetch_cols = result.context.compiled.postfetch
    returning_cols = result.context.compiled.returning

    if (
        mapper.version_id_col is not None
        and mapper.version_id_col in mapper._cols_by_table[table]
    ):
        prefetch_cols = list(prefetch_cols) + [mapper.version_id_col]

    refresh_flush = bool(mapper.class_manager.dispatch.refresh_flush)
    if refresh_flush:
        load_evt_attrs = []

    if returning_cols:
        row = result.context.returned_defaults
        if row is not None:
            for row_value, col in zip(row, returning_cols):
                # pk cols returned from insert are handled
                # distinctly, don't step on the values here
                if col.primary_key and result.context.isinsert:
                    continue

                # note that columns can be in the "return defaults" that are
                # not mapped to this mapper, typically because they are
                # "excluded", which can be specified directly or also occurs
                # when using declarative w/ single table inheritance
                prop = mapper._columntoproperty.get(col)
                if prop:
                    dict_[prop.key] = row_value
                    if refresh_flush:
                        load_evt_attrs.append(prop.key)

    for c in prefetch_cols:
        if c.key in params and c in mapper._columntoproperty:
            dict_[mapper._columntoproperty[c].key] = params[c.key]
            if refresh_flush:
                load_evt_attrs.append(mapper._columntoproperty[c].key)

    if refresh_flush and load_evt_attrs:
        mapper.class_manager.dispatch.refresh_flush(
            state, uowtransaction, load_evt_attrs
        )

    if isupdate and value_params:
        # explicitly suit the use case specified by
        # [ticket:3801], PK SQL expressions for UPDATE on non-RETURNING
        # database which are set to themselves in order to do a version bump.
        postfetch_cols.extend(
            [
                col
                for col in value_params
                if col.primary_key and col not in returning_cols
            ]
        )

    if postfetch_cols:
        state._expire_attributes(
            state.dict,
            [
                mapper._columntoproperty[c].key
                for c in postfetch_cols
                if c in mapper._columntoproperty
            ],
        )

    # synchronize newly inserted ids from one table to the next
    # TODO: this still goes a little too often.  would be nice to
    # have definitive list of "columns that changed" here
    for m, equated_pairs in mapper._table_to_equated[table]:
        sync.populate(
            state,
            m,
            state,
            m,
            equated_pairs,
            uowtransaction,
            mapper.passive_updates,
        )


def _postfetch_bulk_save(mapper, dict_, table):
    for m, equated_pairs in mapper._table_to_equated[table]:
        sync.bulk_populate_inherit_keys(dict_, m, equated_pairs)


def _connections_for_states(base_mapper, uowtransaction, states):
    """Return an iterator of (state, state.dict, mapper, connection).

    The states are sorted according to _sort_states, then paired
    with the connection they should be using for the given
    unit of work transaction.

    """
    # if session has a connection callable,
    # organize individual states with the connection
    # to use for update
    if uowtransaction.session.connection_callable:
        connection_callable = uowtransaction.session.connection_callable
    else:
        connection = uowtransaction.transaction.connection(base_mapper)
        connection_callable = None

    for state in _sort_states(base_mapper, states):
        if connection_callable:
            connection = connection_callable(base_mapper, state.obj())

        mapper = state.manager.mapper

        yield state, state.dict, mapper, connection


def _cached_connection_dict(base_mapper):
    # dictionary of connection->connection_with_cache_options.
    return util.PopulateDict(
        lambda conn: conn.execution_options(
            compiled_cache=base_mapper._compiled_cache
        )
    )


def _sort_states(mapper, states):
    pending = set(states)
    persistent = set(s for s in pending if s.key is not None)
    pending.difference_update(persistent)

    try:
        persistent_sorted = sorted(
            persistent, key=mapper._persistent_sortkey_fn
        )
    except TypeError as err:
        util.raise_(
            sa_exc.InvalidRequestError(
                "Could not sort objects by primary key; primary key "
                "values must be sortable in Python (was: %s)" % err
            ),
            replace_context=err,
        )
    return (
        sorted(pending, key=operator.attrgetter("insert_order"))
        + persistent_sorted
    )


_EMPTY_DICT = util.immutabledict()


class BulkUDCompileState(CompileState):
    class default_update_options(Options):
        _synchronize_session = "evaluate"
        _autoflush = True
        _subject_mapper = None
        _resolved_values = _EMPTY_DICT
        _resolved_keys_as_propnames = _EMPTY_DICT
        _value_evaluators = _EMPTY_DICT
        _matched_objects = None
        _matched_rows = None
        _refresh_identity_token = None

    @classmethod
    def orm_pre_session_exec(
        cls, session, statement, params, execution_options, bind_arguments
    ):
        sync = execution_options.get("synchronize_session", None)
        if sync is None:
            sync = statement._execution_options.get(
                "synchronize_session", None
            )

        update_options = execution_options.get(
            "_sa_orm_update_options",
            BulkUDCompileState.default_update_options,
        )

        if sync is not None:
            if sync not in ("evaluate", "fetch", False):
                raise sa_exc.ArgumentError(
                    "Valid strategies for session synchronization "
                    "are 'evaluate', 'fetch', False"
                )
            update_options += {"_synchronize_session": sync}

        bind_arguments["clause"] = statement
        try:
            plugin_subject = statement._propagate_attrs["plugin_subject"]
        except KeyError:
            assert False, "statement had 'orm' plugin but no plugin_subject"
        else:
            bind_arguments["mapper"] = plugin_subject.mapper

        update_options += {"_subject_mapper": plugin_subject.mapper}

        if update_options._autoflush:
            session._autoflush()

        if update_options._synchronize_session == "evaluate":
            update_options = cls._do_pre_synchronize_evaluate(
                session,
                statement,
                params,
                execution_options,
                bind_arguments,
                update_options,
            )
        elif update_options._synchronize_session == "fetch":
            update_options = cls._do_pre_synchronize_fetch(
                session,
                statement,
                params,
                execution_options,
                bind_arguments,
                update_options,
            )

        return util.immutabledict(execution_options).union(
            dict(_sa_orm_update_options=update_options)
        )

    @classmethod
    def orm_setup_cursor_result(
        cls, session, statement, execution_options, bind_arguments, result
    ):
        update_options = execution_options["_sa_orm_update_options"]
        if update_options._synchronize_session == "evaluate":
            cls._do_post_synchronize_evaluate(session, update_options)
        elif update_options._synchronize_session == "fetch":
            cls._do_post_synchronize_fetch(session, update_options)

        return result

    @classmethod
    def _do_pre_synchronize_evaluate(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        update_options,
    ):
        mapper = update_options._subject_mapper
        target_cls = mapper.class_

        value_evaluators = resolved_keys_as_propnames = _EMPTY_DICT

        try:
            evaluator_compiler = evaluator.EvaluatorCompiler(target_cls)
            if statement._where_criteria:
                eval_condition = evaluator_compiler.process(
                    *statement._where_criteria
                )
            else:

                def eval_condition(obj):
                    return True

            # TODO: something more robust for this conditional
            if statement.__visit_name__ == "update":
                resolved_values = cls._get_resolved_values(mapper, statement)
                value_evaluators = {}
                resolved_keys_as_propnames = cls._resolved_keys_as_propnames(
                    mapper, resolved_values
                )
                for key, value in resolved_keys_as_propnames:
                    value_evaluators[key] = evaluator_compiler.process(
                        coercions.expect(roles.ExpressionElementRole, value)
                    )

        except evaluator.UnevaluatableError as err:
            util.raise_(
                sa_exc.InvalidRequestError(
                    'Could not evaluate current criteria in Python: "%s". '
                    "Specify 'fetch' or False for the "
                    "synchronize_session execution option." % err
                ),
                from_=err,
            )

        # TODO: detect when the where clause is a trivial primary key match
        matched_objects = [
            obj
            for (cls, pk, identity_token,), obj in session.identity_map.items()
            if issubclass(cls, target_cls)
            and eval_condition(obj)
            and identity_token == update_options._refresh_identity_token
        ]
        return update_options + {
            "_matched_objects": matched_objects,
            "_value_evaluators": value_evaluators,
            "_resolved_keys_as_propnames": resolved_keys_as_propnames,
        }

    @classmethod
    def _get_resolved_values(cls, mapper, statement):
        if statement._multi_values:
            return []
        elif statement._ordered_values:
            iterator = statement._ordered_values
        elif statement._values:
            iterator = statement._values.items()
        else:
            return []

        values = []
        if iterator:
            for k, v in iterator:
                if mapper:
                    if isinstance(k, util.string_types):
                        desc = sql.util._entity_namespace_key(mapper, k)
                        values.extend(desc._bulk_update_tuples(v))
                    elif "entity_namespace" in k._annotations:
                        k_anno = k._annotations
                        attr = _entity_namespace_key(
                            k_anno["entity_namespace"], k_anno["orm_key"]
                        )
                        values.extend(attr._bulk_update_tuples(v))
                    else:
                        values.append((k, v))
                else:
                    values.append((k, v))
        return values

    @classmethod
    def _resolved_keys_as_propnames(cls, mapper, resolved_values):
        values = []
        for k, v in resolved_values:
            if isinstance(k, attributes.QueryableAttribute):
                values.append((k.key, v))
                continue
            elif hasattr(k, "__clause_element__"):
                k = k.__clause_element__()

            if mapper and isinstance(k, expression.ColumnElement):
                try:
                    attr = mapper._columntoproperty[k]
                except orm_exc.UnmappedColumnError:
                    pass
                else:
                    values.append((attr.key, v))
            else:
                raise sa_exc.InvalidRequestError(
                    "Invalid expression type: %r" % k
                )
        return values

    @classmethod
    def _do_pre_synchronize_fetch(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        update_options,
    ):
        mapper = update_options._subject_mapper

        if mapper:
            primary_table = mapper.local_table
        else:
            primary_table = statement._raw_columns[0]

        # note this creates a Select() *without* the ORM plugin.
        # we don't want that here.
        select_stmt = future_select(*primary_table.primary_key)
        select_stmt._where_criteria = statement._where_criteria

        matched_rows = session.execute(
            select_stmt, params, execution_options, bind_arguments
        ).fetchall()

        if statement.__visit_name__ == "update":
            resolved_values = cls._get_resolved_values(mapper, statement)
            resolved_keys_as_propnames = cls._resolved_keys_as_propnames(
                mapper, resolved_values
            )
        else:
            resolved_keys_as_propnames = _EMPTY_DICT

        return update_options + {
            "_matched_rows": matched_rows,
            "_resolved_keys_as_propnames": resolved_keys_as_propnames,
        }


@CompileState.plugin_for("orm", "update")
class BulkORMUpdate(UpdateDMLState, BulkUDCompileState):
    @classmethod
    def create_for_statement(cls, statement, compiler, **kw):

        self = cls.__new__(cls)

        self.mapper = mapper = statement.table._annotations.get(
            "parentmapper", None
        )

        self._resolved_values = cls._get_resolved_values(mapper, statement)

        if not statement._preserve_parameter_order and statement._values:
            self._resolved_values = dict(self._resolved_values)

        new_stmt = sql.Update.__new__(sql.Update)
        new_stmt.__dict__.update(statement.__dict__)
        new_stmt.table = mapper.local_table

        # note if the statement has _multi_values, these
        # are passed through to the new statement, which will then raise
        # InvalidRequestError because UPDATE doesn't support multi_values
        # right now.
        if statement._ordered_values:
            new_stmt._ordered_values = self._resolved_values
        elif statement._values:
            new_stmt._values = self._resolved_values

        UpdateDMLState.__init__(self, new_stmt, compiler, **kw)

        return self

    @classmethod
    def _do_post_synchronize_evaluate(cls, session, update_options):

        states = set()
        evaluated_keys = list(update_options._value_evaluators.keys())
        for obj in update_options._matched_objects:

            state, dict_ = (
                attributes.instance_state(obj),
                attributes.instance_dict(obj),
            )

            assert (
                state.identity_token == update_options._refresh_identity_token
            )

            # only evaluate unmodified attributes
            to_evaluate = state.unmodified.intersection(evaluated_keys)
            for key in to_evaluate:
                dict_[key] = update_options._value_evaluators[key](obj)

            state.manager.dispatch.refresh(state, None, to_evaluate)

            state._commit(dict_, list(to_evaluate))

            # expire attributes with pending changes
            # (there was no autoflush, so they are overwritten)
            state._expire_attributes(
                dict_, set(evaluated_keys).difference(to_evaluate)
            )
            states.add(state)
        session._register_altered(states)

    @classmethod
    def _do_post_synchronize_fetch(cls, session, update_options):
        target_mapper = update_options._subject_mapper

        states = set(
            [
                attributes.instance_state(session.identity_map[identity_key])
                for identity_key in [
                    target_mapper.identity_key_from_primary_key(
                        list(primary_key),
                        identity_token=update_options._refresh_identity_token,
                    )
                    for primary_key in update_options._matched_rows
                ]
                if identity_key in session.identity_map
            ]
        )

        values = update_options._resolved_keys_as_propnames
        attrib = set(k for k, v in values)
        for state in states:
            to_expire = attrib.intersection(state.dict)
            if to_expire:
                session._expire_state(state, to_expire)
        session._register_altered(states)


@CompileState.plugin_for("orm", "delete")
class BulkORMDelete(DeleteDMLState, BulkUDCompileState):
    @classmethod
    def create_for_statement(cls, statement, compiler, **kw):
        self = cls.__new__(cls)

        self.mapper = statement.table._annotations.get("parentmapper", None)

        DeleteDMLState.__init__(self, statement, compiler, **kw)

        return self

    @classmethod
    def _do_post_synchronize_evaluate(cls, session, update_options):

        session._remove_newly_deleted(
            [
                attributes.instance_state(obj)
                for obj in update_options._matched_objects
            ]
        )

    @classmethod
    def _do_post_synchronize_fetch(cls, session, update_options):
        target_mapper = update_options._subject_mapper

        for primary_key in update_options._matched_rows:
            # TODO: inline this and call remove_newly_deleted
            # once
            identity_key = target_mapper.identity_key_from_primary_key(
                list(primary_key),
                identity_token=update_options._refresh_identity_token,
            )
            if identity_key in session.identity_map:
                session._remove_newly_deleted(
                    [
                        attributes.instance_state(
                            session.identity_map[identity_key]
                        )
                    ]
                )
