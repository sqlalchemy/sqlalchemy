# orm/persistence.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""private module containing functions used to emit INSERT, UPDATE
and DELETE statements on behalf of a :class:`.Mapper` and its descending
mappers.

The functions here are called only by the unit of work functions
in unitofwork.py.

"""

import operator
from itertools import groupby, chain
from .. import sql, util, exc as sa_exc
from . import attributes, sync, exc as orm_exc, evaluator
from .base import state_str, _entity_descriptor
from ..sql import expression
from ..sql.base import _from_objects
from . import loading


def _bulk_insert(
        mapper, mappings, session_transaction, isstates, return_defaults,
        render_nulls):
    base_mapper = mapper.base_mapper

    cached_connections = _cached_connection_dict(base_mapper)

    if session_transaction.session.connection_callable:
        raise NotImplementedError(
            "connection_callable / per-instance sharding "
            "not supported in bulk_insert()")

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
            (None, state_dict, params, mapper,
                connection, value_params, has_all_pks, has_all_defaults)
            for
            state, state_dict, params, mp,
            conn, value_params, has_all_pks,
            has_all_defaults in _collect_insert_commands(table, (
                (None, mapping, mapper, connection)
                for mapping in mappings),
                bulk=True, return_defaults=return_defaults,
                render_nulls=render_nulls
            )
        )
        _emit_insert_statements(base_mapper, None,
                                cached_connections,
                                super_mapper, table, records,
                                bookkeeping=return_defaults)

    if return_defaults and isstates:
        identity_cls = mapper._identity_class
        identity_props = [p.key for p in mapper._identity_key_props]
        for state, dict_ in states:
            state.key = (
                identity_cls,
                tuple([dict_[key] for key in identity_props])
            )


def _bulk_update(mapper, mappings, session_transaction,
                 isstates, update_changed_only):
    base_mapper = mapper.base_mapper

    cached_connections = _cached_connection_dict(base_mapper)

    search_keys = mapper._primary_key_propkeys
    if mapper._version_id_prop:
        search_keys = {mapper._version_id_prop.key}.union(search_keys)

    def _changed_dict(mapper, state):
        return dict(
            (k, v)
            for k, v in state.dict.items() if k in state.committed_state or k
            in search_keys

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
            "not supported in bulk_update()")

    connection = session_transaction.connection(base_mapper)

    for table, super_mapper in base_mapper._sorted_tables.items():
        if not mapper.isa(super_mapper):
            continue

        records = _collect_update_commands(None, table, (
            (None, mapping, mapper, connection,
                (mapping[mapper._version_id_prop.key]
                    if mapper._version_id_prop else None))
            for mapping in mappings
        ), bulk=True)

        _emit_update_statements(base_mapper, None,
                                cached_connections,
                                super_mapper, table, records,
                                bookkeeping=False)


def save_obj(
        base_mapper, states, uowtransaction, single=False):
    """Issue ``INSERT`` and/or ``UPDATE`` statements for a list
    of objects.

    This is called within the context of a UOWTransaction during a
    flush operation, given a list of states to be flushed.  The
    base mapper in an inheritance hierarchy handles the inserts/
    updates for all descendant mappers.

    """

    # if batch=false, call _save_obj separately for each object
    if not single and not base_mapper.batch:
        for state in _sort_states(states):
            save_obj(base_mapper, [state], uowtransaction, single=True)
        return

    states_to_update = []
    states_to_insert = []
    cached_connections = _cached_connection_dict(base_mapper)

    for (state, dict_, mapper, connection,
            has_identity,
            row_switch, update_version_id) in _organize_states_for_save(
            base_mapper, states, uowtransaction
    ):
        if has_identity or row_switch:
            states_to_update.append(
                (state, dict_, mapper, connection, update_version_id)
            )
        else:
            states_to_insert.append(
                (state, dict_, mapper, connection)
            )

    for table, mapper in base_mapper._sorted_tables.items():
        if table not in mapper._pks_by_table:
            continue
        insert = _collect_insert_commands(table, states_to_insert)

        update = _collect_update_commands(
            uowtransaction, table, states_to_update)

        _emit_update_statements(base_mapper, uowtransaction,
                                cached_connections,
                                mapper, table, update)

        _emit_insert_statements(base_mapper, uowtransaction,
                                cached_connections,
                                mapper, table, insert)

    _finalize_insert_update_commands(
        base_mapper, uowtransaction,
        chain(
            (
                (state, state_dict, mapper, connection, False)
                for state, state_dict, mapper, connection in states_to_insert
            ),
            (
                (state, state_dict, mapper, connection, True)
                for state, state_dict, mapper, connection,
                update_version_id in states_to_update
            )
        )
    )


def post_update(base_mapper, states, uowtransaction, post_update_cols):
    """Issue UPDATE statements on behalf of a relationship() which
    specifies post_update.

    """
    cached_connections = _cached_connection_dict(base_mapper)

    states_to_update = list(_organize_states_for_post_update(
        base_mapper,
        states, uowtransaction))

    for table, mapper in base_mapper._sorted_tables.items():
        if table not in mapper._pks_by_table:
            continue

        update = (
            (
                state, state_dict, sub_mapper, connection,
                mapper._get_committed_state_attr_by_column(
                    state, state_dict, mapper.version_id_col
                ) if mapper.version_id_col is not None else None
            )
            for
            state, state_dict, sub_mapper, connection in states_to_update
            if table in sub_mapper._pks_by_table
        )

        update = _collect_post_update_commands(
            base_mapper, uowtransaction,
            table, update,
            post_update_cols
        )

        _emit_post_update_statements(base_mapper, uowtransaction,
                                     cached_connections,
                                     mapper, table, update)


def delete_obj(base_mapper, states, uowtransaction):
    """Issue ``DELETE`` statements for a list of objects.

    This is called within the context of a UOWTransaction during a
    flush operation.

    """

    cached_connections = _cached_connection_dict(base_mapper)

    states_to_delete = list(_organize_states_for_delete(
        base_mapper,
        states,
        uowtransaction))

    table_to_mapper = base_mapper._sorted_tables

    for table in reversed(list(table_to_mapper.keys())):
        mapper = table_to_mapper[table]
        if table not in mapper._pks_by_table:
            continue
        elif mapper.inherits and mapper.passive_deletes:
            continue

        delete = _collect_delete_commands(base_mapper, uowtransaction,
                                          table, states_to_delete)

        _emit_delete_statements(base_mapper, uowtransaction,
                                cached_connections, mapper, table, delete)

    for state, state_dict, mapper, connection, \
            update_version_id in states_to_delete:
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
            base_mapper, uowtransaction,
            states):

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
        if not has_identity and \
                instance_key in uowtransaction.session.identity_map:
            instance = \
                uowtransaction.session.identity_map[instance_key]
            existing = attributes.instance_state(instance)

            if not uowtransaction.was_already_deleted(existing):
                if not uowtransaction.is_deleted(existing):
                    raise orm_exc.FlushError(
                        "New instance %s with identity key %s conflicts "
                        "with persistent instance %s" %
                        (state_str(state), instance_key,
                         state_str(existing)))

                base_mapper._log_debug(
                    "detected row switch for identity %s.  "
                    "will update %s, remove %s from "
                    "transaction", instance_key,
                    state_str(state), state_str(existing))

                # remove the "delete" flag from the existing element
                uowtransaction.remove_state_actions(existing)
                row_switch = existing

        if (has_identity or row_switch) and mapper.version_id_col is not None:
            update_version_id = mapper._get_committed_state_attr_by_column(
                row_switch if row_switch else state,
                row_switch.dict if row_switch else dict_,
                mapper.version_id_col)

        yield (state, dict_, mapper, connection,
               has_identity, row_switch, update_version_id)


def _organize_states_for_post_update(base_mapper, states,
                                     uowtransaction):
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
            base_mapper, uowtransaction,
            states):

        mapper.dispatch.before_delete(mapper, connection, state)

        if mapper.version_id_col is not None:
            update_version_id = \
                mapper._get_committed_state_attr_by_column(
                    state, dict_,
                    mapper.version_id_col)
        else:
            update_version_id = None

        yield (
            state, dict_, mapper, connection, update_version_id)


def _collect_insert_commands(
        table, states_to_insert,
        bulk=False, return_defaults=False, render_nulls=False):
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
            elif not bulk and hasattr(value, '__clause_element__') or \
                    isinstance(value, sql.ClauseElement):
                value_params[col.key] = value.__clause_element__() \
                    if hasattr(value, '__clause_element__') else value
            else:
                params[col.key] = value

        if not bulk:
            # for all the columns that have no default and we don't have
            # a value and where "None" is not a special value, add
            # explicit None to the INSERT.   This is a legacy behavior
            # which might be worth removing, as it should not be necessary
            # and also produces confusion, given that "missing" and None
            # now have distinct meanings
            for colkey in mapper._insert_cols_as_none[table].\
                    difference(params).difference(value_params):
                params[colkey] = None

        if not bulk or return_defaults:
            has_all_pks = mapper._pk_keys_by_table[table].issubset(params)

            if mapper.base_mapper.eager_defaults:
                has_all_defaults = mapper._server_default_cols[table].\
                    issubset(params)
            else:
                has_all_defaults = True
        else:
            has_all_defaults = has_all_pks = True

        if mapper.version_id_generator is not False \
                and mapper.version_id_col is not None and \
                mapper.version_id_col in mapper._cols_by_table[table]:
            params[mapper.version_id_col.key] = \
                mapper.version_id_generator(None)

        yield (
            state, state_dict, params, mapper,
            connection, value_params, has_all_pks,
            has_all_defaults)


def _collect_update_commands(
        uowtransaction, table, states_to_update,
        bulk=False):
    """Identify sets of values to use in UPDATE statements for a
    list of states.

    This function works intricately with the history system
    to determine exactly what values should be updated
    as well as how the row should be matched within an UPDATE
    statement.  Includes some tricky scenarios where the primary
    key of an object might have been changed.

    """

    for state, state_dict, mapper, connection, \
            update_version_id in states_to_update:

        if table not in mapper._pks_by_table:
            continue

        pks = mapper._pks_by_table[table]

        value_params = {}

        propkey_to_col = mapper._propkey_to_col[table]

        if bulk:
            params = dict(
                (propkey_to_col[propkey].key, state_dict[propkey])
                for propkey in
                set(propkey_to_col).intersection(state_dict).difference(
                    mapper._pk_keys_by_table[table])
            )
            has_all_defaults = True
        else:
            params = {}
            for propkey in set(propkey_to_col).intersection(
                    state.committed_state):
                value = state_dict[propkey]
                col = propkey_to_col[propkey]

                if hasattr(value, '__clause_element__') or \
                        isinstance(value, sql.ClauseElement):
                    value_params[col] = value.__clause_element__() \
                        if hasattr(value, '__clause_element__') else value
                # guard against values that generate non-__nonzero__
                # objects for __eq__()
                elif state.manager[propkey].impl.is_equal(
                        value, state.committed_state[propkey]) is not True:
                    params[col.key] = value

            if mapper.base_mapper.eager_defaults:
                has_all_defaults = mapper._server_onupdate_default_cols[table].\
                    issubset(params)
            else:
                has_all_defaults = True

        if update_version_id is not None and \
                mapper.version_id_col in mapper._cols_by_table[table]:

            if not bulk and not (params or value_params):
                # HACK: check for history in other tables, in case the
                # history is only in a different table than the one
                # where the version_id_col is.  This logic was lost
                # from 0.9 -> 1.0.0 and restored in 1.0.6.
                for prop in mapper._columntoproperty.values():
                    history = (
                        state.manager[prop.key].impl.get_history(
                            state, state_dict,
                            attributes.PASSIVE_NO_INITIALIZE))
                    if history.added:
                        break
                else:
                    # no net change, break
                    continue

            col = mapper.version_id_col
            no_params = not params and not value_params
            params[col._label] = update_version_id

            if (bulk or col.key not in params) and \
                    mapper.version_id_generator is not False:
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
        if bulk:
            pk_params = dict(
                (propkey_to_col[propkey]._label, state_dict.get(propkey))
                for propkey in
                set(propkey_to_col).
                intersection(mapper._pk_attr_keys_by_table[table])
            )
        else:
            pk_params = {}
            for col in pks:
                propkey = mapper._columntoproperty[col].key

                history = state.manager[propkey].impl.get_history(
                    state, state_dict, attributes.PASSIVE_OFF)

                if history.added:
                    if not history.deleted or \
                            ("pk_cascaded", state, col) in \
                            uowtransaction.attributes:
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
                        "key value on column %s" % (table, col))

        if params or value_params:
            params.update(pk_params)
            yield (
                state, state_dict, params, mapper,
                connection, value_params, has_all_defaults, has_all_pks)


def _collect_post_update_commands(base_mapper, uowtransaction, table,
                                  states_to_update, post_update_cols):
    """Identify sets of values to use in UPDATE statements for a
    list of states within a post_update operation.

    """

    for state, state_dict, mapper, connection, \
            update_version_id in states_to_update:

        # assert table in mapper._pks_by_table

        pks = mapper._pks_by_table[table]
        params = {}
        hasdata = False

        for col in mapper._cols_by_table[table]:
            if col in pks:
                params[col._label] = \
                    mapper._get_state_attr_by_column(
                        state,
                        state_dict, col, passive=attributes.PASSIVE_OFF)

            elif col in post_update_cols or col.onupdate is not None:
                prop = mapper._columntoproperty[col]
                history = state.manager[prop.key].impl.get_history(
                    state, state_dict,
                    attributes.PASSIVE_NO_INITIALIZE)
                if history.added:
                    value = history.added[0]
                    params[col.key] = value
                    hasdata = True
        if hasdata:
            if update_version_id is not None and \
                    mapper.version_id_col in mapper._cols_by_table[table]:

                col = mapper.version_id_col
                params[col._label] = update_version_id

                if bool(state.key) and col.key not in params and \
                        mapper.version_id_generator is not False:
                    val = mapper.version_id_generator(update_version_id)
                    params[col.key] = val
            yield state, state_dict, mapper, connection, params


def _collect_delete_commands(base_mapper, uowtransaction, table,
                             states_to_delete):
    """Identify values to use in DELETE statements for a list of
    states to be deleted."""

    for state, state_dict, mapper, connection, \
            update_version_id in states_to_delete:

        if table not in mapper._pks_by_table:
            continue

        params = {}
        for col in mapper._pks_by_table[table]:
            params[col.key] = \
                value = \
                mapper._get_committed_state_attr_by_column(
                    state, state_dict, col)
            if value is None:
                raise orm_exc.FlushError(
                    "Can't delete from table %s "
                    "using NULL for primary "
                    "key value on column %s" % (table, col))

        if update_version_id is not None and \
                mapper.version_id_col in mapper._cols_by_table[table]:
            params[mapper.version_id_col.key] = update_version_id
        yield params, connection


def _emit_update_statements(base_mapper, uowtransaction,
                            cached_connections, mapper, table, update,
                            bookkeeping=True):
    """Emit UPDATE statements corresponding to value lists collected
    by _collect_update_commands()."""

    needs_version_id = mapper.version_id_col is not None and \
        mapper.version_id_col in mapper._cols_by_table[table]

    def update_stmt():
        clause = sql.and_()

        for col in mapper._pks_by_table[table]:
            clause.clauses.append(col == sql.bindparam(col._label,
                                                       type_=col.type))

        if needs_version_id:
            clause.clauses.append(
                mapper.version_id_col == sql.bindparam(
                    mapper.version_id_col._label,
                    type_=mapper.version_id_col.type))

        stmt = table.update(clause)
        return stmt

    cached_stmt = base_mapper._memo(('update', table), update_stmt)

    for (connection, paramkeys, hasvalue, has_all_defaults, has_all_pks), \
        records in groupby(
            update,
            lambda rec: (
                rec[4],  # connection
                set(rec[2]),  # set of parameter keys
                bool(rec[5]),  # whether or not we have "value" parameters
                rec[6],  # has_all_defaults
                rec[7]  # has all pks
            )
    ):
        rows = 0
        records = list(records)

        statement = cached_stmt
        return_defaults = False

        if not has_all_pks:
            statement = statement.return_defaults()
            return_defaults = True
        elif bookkeeping and not has_all_defaults and \
                mapper.base_mapper.eager_defaults:
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

        assert_multirow = assert_singlerow and \
            connection.dialect.supports_sane_multi_rowcount
        allow_multirow = has_all_defaults and not needs_version_id

        if hasvalue:
            for state, state_dict, params, mapper, \
                    connection, value_params, \
                    has_all_defaults, has_all_pks in records:
                c = connection.execute(
                    statement.values(value_params),
                    params)
                if bookkeeping:
                    _postfetch(
                        mapper,
                        uowtransaction,
                        table,
                        state,
                        state_dict,
                        c,
                        c.context.compiled_parameters[0],
                        value_params)
                rows += c.rowcount
                check_rowcount = assert_singlerow
        else:
            if not allow_multirow:
                check_rowcount = assert_singlerow
                for state, state_dict, params, mapper, \
                        connection, value_params, has_all_defaults, \
                        has_all_pks in records:
                    c = cached_connections[connection].\
                        execute(statement, params)

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
                            value_params)
                    rows += c.rowcount
            else:
                multiparams = [rec[2] for rec in records]

                check_rowcount = assert_multirow or (
                    assert_singlerow and
                    len(multiparams) == 1
                )

                c = cached_connections[connection].\
                    execute(statement, multiparams)

                rows += c.rowcount

                for state, state_dict, params, mapper, \
                        connection, value_params, \
                        has_all_defaults, has_all_pks in records:
                    if bookkeeping:
                        _postfetch(
                            mapper,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            c,
                            c.context.compiled_parameters[0],
                            value_params)

        if check_rowcount:
            if rows != len(records):
                raise orm_exc.StaleDataError(
                    "UPDATE statement on table '%s' expected to "
                    "update %d row(s); %d were matched." %
                    (table.description, len(records), rows))

        elif needs_version_id:
            util.warn("Dialect %s does not support updated rowcount "
                      "- versioning cannot be verified." %
                      c.dialect.dialect_description)


def _emit_insert_statements(base_mapper, uowtransaction,
                            cached_connections, mapper, table, insert,
                            bookkeeping=True):
    """Emit INSERT statements corresponding to value lists collected
    by _collect_insert_commands()."""

    cached_stmt = base_mapper._memo(('insert', table), table.insert)

    for (connection, pkeys, hasvalue, has_all_pks, has_all_defaults), \
        records in groupby(
            insert,
            lambda rec: (
                rec[4],  # connection
                set(rec[2]),  # parameter keys
                bool(rec[5]),  # whether we have "value" parameters
                rec[6],
                rec[7])):

        statement = cached_stmt

        if not bookkeeping or \
                (
                    has_all_defaults
                    or not base_mapper.eager_defaults
                    or not connection.dialect.implicit_returning
                ) and has_all_pks and not hasvalue:

            records = list(records)
            multiparams = [rec[2] for rec in records]

            c = cached_connections[connection].\
                execute(statement, multiparams)

            if bookkeeping:
                for (state, state_dict, params, mapper_rec,
                        conn, value_params, has_all_pks, has_all_defaults), \
                        last_inserted_params in \
                        zip(records, c.context.compiled_parameters):
                    if state:
                        _postfetch(
                            mapper_rec,
                            uowtransaction,
                            table,
                            state,
                            state_dict,
                            c,
                            last_inserted_params,
                            value_params)
                    else:
                        _postfetch_bulk_save(mapper_rec, state_dict, table)

        else:
            if not has_all_defaults and base_mapper.eager_defaults:
                statement = statement.return_defaults()
            elif mapper.version_id_col is not None:
                statement = statement.return_defaults(mapper.version_id_col)

            for state, state_dict, params, mapper_rec, \
                    connection, value_params, \
                    has_all_pks, has_all_defaults in records:

                if value_params:
                    result = connection.execute(
                        statement.values(value_params),
                        params)
                else:
                    result = cached_connections[connection].\
                        execute(statement, params)

                primary_key = result.context.inserted_primary_key

                if primary_key is not None:
                    # set primary key attributes
                    for pk, col in zip(primary_key,
                                       mapper._pks_by_table[table]):
                        prop = mapper_rec._columntoproperty[col]
                        if state_dict.get(prop.key) is None:
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
                            value_params)
                    else:
                        _postfetch_bulk_save(mapper_rec, state_dict, table)


def _emit_post_update_statements(base_mapper, uowtransaction,
                                 cached_connections, mapper, table, update):
    """Emit UPDATE statements corresponding to value lists collected
    by _collect_post_update_commands()."""

    needs_version_id = mapper.version_id_col is not None and \
        mapper.version_id_col in mapper._cols_by_table[table]

    def update_stmt():
        clause = sql.and_()

        for col in mapper._pks_by_table[table]:
            clause.clauses.append(col == sql.bindparam(col._label,
                                                       type_=col.type))

        if needs_version_id:
            clause.clauses.append(
                mapper.version_id_col == sql.bindparam(
                    mapper.version_id_col._label,
                    type_=mapper.version_id_col.type))

        stmt = table.update(clause)

        if mapper.version_id_col is not None:
            stmt = stmt.return_defaults(mapper.version_id_col)

        return stmt

    statement = base_mapper._memo(('post_update', table), update_stmt)

    # execute each UPDATE in the order according to the original
    # list of states to guarantee row access order, but
    # also group them into common (connection, cols) sets
    # to support executemany().
    for key, records in groupby(
        update, lambda rec: (
            rec[3],  # connection
            set(rec[4]),  # parameter keys
        )
    ):
        rows = 0

        records = list(records)
        connection = key[0]

        assert_singlerow = (
            connection.dialect.supports_sane_rowcount
            if mapper.version_id_col is None
            else connection.dialect.supports_sane_rowcount_returning
        )
        assert_multirow = assert_singlerow and \
            connection.dialect.supports_sane_multi_rowcount
        allow_multirow = not needs_version_id or assert_multirow


        if not allow_multirow:
            check_rowcount = assert_singlerow
            for state, state_dict, mapper_rec, \
                    connection, params in records:
                c = cached_connections[connection].\
                    execute(statement, params)
                _postfetch_post_update(
                    mapper_rec, uowtransaction, table, state, state_dict,
                    c, c.context.compiled_parameters[0])
                rows += c.rowcount
        else:
            multiparams = [
                params for
                state, state_dict, mapper_rec, conn, params in records]

            check_rowcount = assert_multirow or (
                assert_singlerow and
                len(multiparams) == 1
            )

            c = cached_connections[connection].\
                execute(statement, multiparams)

            rows += c.rowcount
            for state, state_dict, mapper_rec, \
                    connection, params in records:
                _postfetch_post_update(
                    mapper_rec, uowtransaction, table, state, state_dict,
                    c, c.context.compiled_parameters[0])

        if check_rowcount:
            if rows != len(records):
                raise orm_exc.StaleDataError(
                    "UPDATE statement on table '%s' expected to "
                    "update %d row(s); %d were matched." %
                    (table.description, len(records), rows))

        elif needs_version_id:
            util.warn("Dialect %s does not support updated rowcount "
                      "- versioning cannot be verified." %
                      c.dialect.dialect_description)


def _emit_delete_statements(base_mapper, uowtransaction, cached_connections,
                            mapper, table, delete):
    """Emit DELETE statements corresponding to value lists collected
    by _collect_delete_commands()."""

    need_version_id = mapper.version_id_col is not None and \
        mapper.version_id_col in mapper._cols_by_table[table]

    def delete_stmt():
        clause = sql.and_()
        for col in mapper._pks_by_table[table]:
            clause.clauses.append(
                col == sql.bindparam(col.key, type_=col.type))

        if need_version_id:
            clause.clauses.append(
                mapper.version_id_col ==
                sql.bindparam(
                    mapper.version_id_col.key,
                    type_=mapper.version_id_col.type
                )
            )

        return table.delete(clause)

    statement = base_mapper._memo(('delete', table), delete_stmt)
    for connection, recs in groupby(
        delete,
        lambda rec: rec[1]   # connection
    ):
        del_objects = [params for params, connection in recs]

        connection = cached_connections[connection]

        expected = len(del_objects)
        rows_matched = -1
        only_warn = False
        if connection.dialect.supports_sane_multi_rowcount:
            c = connection.execute(statement, del_objects)

            if not need_version_id:
                only_warn = True

            rows_matched = c.rowcount

        elif need_version_id:
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
                    "- versioning cannot be verified." %
                    connection.dialect.dialect_description,
                    stacklevel=12)
                connection.execute(statement, del_objects)
        else:
            c = connection.execute(statement, del_objects)

            if not need_version_id:
                only_warn = True

            rows_matched = c.rowcount

        if base_mapper.confirm_deleted_rows and \
                rows_matched > -1 and expected != rows_matched:
            if only_warn:
                util.warn(
                    "DELETE statement on table '%s' expected to "
                    "delete %d row(s); %d were matched.  Please set "
                    "confirm_deleted_rows=False within the mapper "
                    "configuration to prevent this warning." %
                    (table.description, expected, rows_matched)
                )
            else:
                raise orm_exc.StaleDataError(
                    "DELETE statement on table '%s' expected to "
                    "delete %d row(s); %d were matched.  Please set "
                    "confirm_deleted_rows=False within the mapper "
                    "configuration to prevent this warning." %
                    (table.description, expected, rows_matched)
                )


def _finalize_insert_update_commands(base_mapper, uowtransaction, states):
    """finalize state on states that have been inserted or updated,
    including calling after_insert/after_update events.

    """
    for state, state_dict, mapper, connection, has_identity in states:

        if mapper._readonly_props:
            readonly = state.unmodified_intersection(
                [
                    p.key for p in mapper._readonly_props
                    if (
                        p.expire_on_flush and
                        (not p.deferred or p.key in state.dict)
                    ) or (
                        not p.expire_on_flush and
                        not p.deferred and p.key not in state.dict
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
                    mapper._server_default_plus_onupdate_propkeys)
            )

        if mapper.version_id_col is not None and \
                mapper.version_id_generator is False:
            if mapper._version_id_prop.key in state.unloaded:
                toload_now.extend([mapper._version_id_prop.key])

        if toload_now:
            state.key = base_mapper._identity_key_from_state(state)
            loading.load_on_ident(
                uowtransaction.session.query(mapper),
                state.key, refresh_state=state,
                only_load_props=toload_now)

        # call after_XXX extensions
        if not has_identity:
            mapper.dispatch.after_insert(mapper, connection, state)
        else:
            mapper.dispatch.after_update(mapper, connection, state)

        if mapper.version_id_col is not None:
            if state_dict[mapper._version_id_prop.key] is None:
                raise orm_exc.FlushError(
                    "Instance does not contain a non-NULL version value")


def _postfetch_post_update(mapper, uowtransaction, table,
                           state, dict_, result, params):
    prefetch_cols = result.context.compiled.prefetch
    postfetch_cols = result.context.compiled.postfetch

    if mapper.version_id_col is not None and \
            mapper.version_id_col in mapper._cols_by_table[table]:
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
            state, uowtransaction, load_evt_attrs)

    if postfetch_cols:
        state._expire_attributes(state.dict,
                                 [mapper._columntoproperty[c].key
                                  for c in postfetch_cols if c in
                                  mapper._columntoproperty]
                                 )


def _postfetch(mapper, uowtransaction, table,
               state, dict_, result, params, value_params):
    """Expire attributes in need of newly persisted database state,
    after an INSERT or UPDATE statement has proceeded for that
    state."""

    prefetch_cols = result.context.compiled.prefetch
    postfetch_cols = result.context.compiled.postfetch
    returning_cols = result.context.compiled.returning

    if mapper.version_id_col is not None and \
            mapper.version_id_col in mapper._cols_by_table[table]:
        prefetch_cols = list(prefetch_cols) + [mapper.version_id_col]

    refresh_flush = bool(mapper.class_manager.dispatch.refresh_flush)
    if refresh_flush:
        load_evt_attrs = []

    if returning_cols:
        row = result.context.returned_defaults
        if row is not None:
            for col in returning_cols:
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
                    dict_[prop.key] = row[col]
                    if refresh_flush:
                        load_evt_attrs.append(prop.key)

    for c in prefetch_cols:
        if c.key in params and c in mapper._columntoproperty:
            dict_[mapper._columntoproperty[c].key] = params[c.key]
            if refresh_flush:
                load_evt_attrs.append(mapper._columntoproperty[c].key)

    if refresh_flush and load_evt_attrs:
        mapper.class_manager.dispatch.refresh_flush(
            state, uowtransaction, load_evt_attrs)

    if postfetch_cols:
        state._expire_attributes(state.dict,
                                 [mapper._columntoproperty[c].key
                                  for c in postfetch_cols if c in
                                  mapper._columntoproperty]
                                 )

    # synchronize newly inserted ids from one table to the next
    # TODO: this still goes a little too often.  would be nice to
    # have definitive list of "columns that changed" here
    for m, equated_pairs in mapper._table_to_equated[table]:
        sync.populate(state, m, state, m,
                      equated_pairs,
                      uowtransaction,
                      mapper.passive_updates)


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
        connection_callable = \
            uowtransaction.session.connection_callable
    else:
        connection = uowtransaction.transaction.connection(base_mapper)
        connection_callable = None

    for state in _sort_states(states):
        if connection_callable:
            connection = connection_callable(base_mapper, state.obj())

        mapper = state.manager.mapper

        yield state, state.dict, mapper, connection


def _cached_connection_dict(base_mapper):
    # dictionary of connection->connection_with_cache_options.
    return util.PopulateDict(
        lambda conn: conn.execution_options(
            compiled_cache=base_mapper._compiled_cache
        ))


def _sort_states(states):
    pending = set(states)
    persistent = set(s for s in pending if s.key is not None)
    pending.difference_update(persistent)
    return sorted(pending, key=operator.attrgetter("insert_order")) + \
        sorted(persistent, key=lambda q: q.key[1])


class BulkUD(object):
    """Handle bulk update and deletes via a :class:`.Query`."""

    def __init__(self, query):
        self.query = query.enable_eagerloads(False)
        self.mapper = self.query._bind_mapper()
        self._validate_query_state()

    def _validate_query_state(self):
        for attr, methname, notset, op in (
            ('_limit', 'limit()', None, operator.is_),
            ('_offset', 'offset()', None, operator.is_),
            ('_order_by', 'order_by()', False, operator.is_),
            ('_group_by', 'group_by()', False, operator.is_),
            ('_distinct', 'distinct()', False, operator.is_),
            (
                '_from_obj',
                'join(), outerjoin(), select_from(), or from_self()',
                (), operator.eq)
        ):
            if not op(getattr(self.query, attr), notset):
                raise sa_exc.InvalidRequestError(
                    "Can't call Query.update() or Query.delete() "
                    "when %s has been called" %
                    (methname, )
                )

    @property
    def session(self):
        return self.query.session

    @classmethod
    def _factory(cls, lookup, synchronize_session, *arg):
        try:
            klass = lookup[synchronize_session]
        except KeyError:
            raise sa_exc.ArgumentError(
                "Valid strategies for session synchronization "
                "are %s" % (", ".join(sorted(repr(x)
                                             for x in lookup))))
        else:
            return klass(*arg)

    def exec_(self):
        self._do_pre()
        self._do_pre_synchronize()
        self._do_exec()
        self._do_post_synchronize()
        self._do_post()

    def _execute_stmt(self, stmt):
        self.result = self.query.session.execute(
            stmt, params=self.query._params,
            mapper=self.mapper)
        self.rowcount = self.result.rowcount

    @util.dependencies("sqlalchemy.orm.query")
    def _do_pre(self, querylib):
        query = self.query
        self.context = querylib.QueryContext(query)

        if isinstance(query._entities[0], querylib._ColumnEntity):
            # check for special case of query(table)
            tables = set()
            for ent in query._entities:
                if not isinstance(ent, querylib._ColumnEntity):
                    tables.clear()
                    break
                else:
                    tables.update(_from_objects(ent.column))

            if len(tables) != 1:
                raise sa_exc.InvalidRequestError(
                    "This operation requires only one Table or "
                    "entity be specified as the target."
                )
            else:
                self.primary_table = tables.pop()

        else:
            self.primary_table = query._only_entity_zero(
                "This operation requires only one Table or "
                "entity be specified as the target."
            ).mapper.local_table

        session = query.session

        if query._autoflush:
            session._autoflush()

    def _do_pre_synchronize(self):
        pass

    def _do_post_synchronize(self):
        pass


class BulkEvaluate(BulkUD):
    """BulkUD which does the 'evaluate' method of session state resolution."""

    def _additional_evaluators(self, evaluator_compiler):
        pass

    def _do_pre_synchronize(self):
        query = self.query
        target_cls = query._mapper_zero().class_

        try:
            evaluator_compiler = evaluator.EvaluatorCompiler(target_cls)
            if query.whereclause is not None:
                eval_condition = evaluator_compiler.process(
                    query.whereclause)
            else:
                def eval_condition(obj):
                    return True

            self._additional_evaluators(evaluator_compiler)

        except evaluator.UnevaluatableError as err:
            raise sa_exc.InvalidRequestError(
                'Could not evaluate current criteria in Python: "%s". '
                'Specify \'fetch\' or False for the '
                'synchronize_session parameter.' % err)

        # TODO: detect when the where clause is a trivial primary key match
        self.matched_objects = [
            obj for (cls, pk, identity_token), obj in
            query.session.identity_map.items()
            if issubclass(cls, target_cls) and
            eval_condition(obj)]


class BulkFetch(BulkUD):
    """BulkUD which does the 'fetch' method of session state resolution."""

    def _do_pre_synchronize(self):
        query = self.query
        session = query.session
        context = query._compile_context()
        select_stmt = context.statement.with_only_columns(
            self.primary_table.primary_key)
        self.matched_rows = session.execute(
            select_stmt,
            mapper=self.mapper,
            params=query._params).fetchall()


class BulkUpdate(BulkUD):
    """BulkUD which handles UPDATEs."""

    def __init__(self, query, values, update_kwargs):
        super(BulkUpdate, self).__init__(query)
        self.values = values
        self.update_kwargs = update_kwargs

    @classmethod
    def factory(cls, query, synchronize_session, values, update_kwargs):
        return BulkUD._factory({
            "evaluate": BulkUpdateEvaluate,
            "fetch": BulkUpdateFetch,
            False: BulkUpdate
        }, synchronize_session, query, values, update_kwargs)

    @property
    def _resolved_values(self):
        values = []
        for k, v in (
                self.values.items() if hasattr(self.values, 'items')
                else self.values):
            if self.mapper:
                if isinstance(k, util.string_types):
                    desc = _entity_descriptor(self.mapper, k)
                    values.extend(desc._bulk_update_tuples(v))
                elif isinstance(k, attributes.QueryableAttribute):
                    values.extend(k._bulk_update_tuples(v))
                else:
                    values.append((k, v))
            else:
                values.append((k, v))
        return values

    @property
    def _resolved_values_keys_as_propnames(self):
        values = []
        for k, v in self._resolved_values:
            if isinstance(k, attributes.QueryableAttribute):
                values.append((k.key, v))
                continue
            elif hasattr(k, '__clause_element__'):
                k = k.__clause_element__()

            if self.mapper and isinstance(k, expression.ColumnElement):
                try:
                    attr = self.mapper._columntoproperty[k]
                except orm_exc.UnmappedColumnError:
                    pass
                else:
                    values.append((attr.key, v))
            else:
                raise sa_exc.InvalidRequestError(
                    "Invalid expression type: %r" % k)
        return values

    def _do_exec(self):
        values = self._resolved_values

        if not self.update_kwargs.get('preserve_parameter_order', False):
            values = dict(values)

        update_stmt = sql.update(self.primary_table,
                                 self.context.whereclause, values,
                                 **self.update_kwargs)

        self._execute_stmt(update_stmt)

    def _do_post(self):
        session = self.query.session
        session.dispatch.after_bulk_update(self)


class BulkDelete(BulkUD):
    """BulkUD which handles DELETEs."""

    def __init__(self, query):
        super(BulkDelete, self).__init__(query)

    @classmethod
    def factory(cls, query, synchronize_session):
        return BulkUD._factory({
            "evaluate": BulkDeleteEvaluate,
            "fetch": BulkDeleteFetch,
            False: BulkDelete
        }, synchronize_session, query)

    def _do_exec(self):
        delete_stmt = sql.delete(self.primary_table,
                                 self.context.whereclause)

        self._execute_stmt(delete_stmt)

    def _do_post(self):
        session = self.query.session
        session.dispatch.after_bulk_delete(self)


class BulkUpdateEvaluate(BulkEvaluate, BulkUpdate):
    """BulkUD which handles UPDATEs using the "evaluate"
    method of session resolution."""

    def _additional_evaluators(self, evaluator_compiler):
        self.value_evaluators = {}
        values = self._resolved_values_keys_as_propnames
        for key, value in values:
            self.value_evaluators[key] = evaluator_compiler.process(
                expression._literal_as_binds(value))

    def _do_post_synchronize(self):
        session = self.query.session
        states = set()
        evaluated_keys = list(self.value_evaluators.keys())
        for obj in self.matched_objects:
            state, dict_ = attributes.instance_state(obj),\
                attributes.instance_dict(obj)

            # only evaluate unmodified attributes
            to_evaluate = state.unmodified.intersection(
                evaluated_keys)
            for key in to_evaluate:
                dict_[key] = self.value_evaluators[key](obj)

            state.manager.dispatch.refresh(
                state, None, to_evaluate)

            state._commit(dict_, list(to_evaluate))

            # expire attributes with pending changes
            # (there was no autoflush, so they are overwritten)
            state._expire_attributes(dict_,
                                     set(evaluated_keys).
                                     difference(to_evaluate))
            states.add(state)
        session._register_altered(states)


class BulkDeleteEvaluate(BulkEvaluate, BulkDelete):
    """BulkUD which handles DELETEs using the "evaluate"
    method of session resolution."""

    def _do_post_synchronize(self):
        self.query.session._remove_newly_deleted(
            [attributes.instance_state(obj)
             for obj in self.matched_objects])


class BulkUpdateFetch(BulkFetch, BulkUpdate):
    """BulkUD which handles UPDATEs using the "fetch"
    method of session resolution."""

    def _do_post_synchronize(self):
        session = self.query.session
        target_mapper = self.query._mapper_zero()

        states = set([
            attributes.instance_state(session.identity_map[identity_key])
            for identity_key in [
                target_mapper.identity_key_from_primary_key(
                    list(primary_key))
                for primary_key in self.matched_rows
            ]
            if identity_key in session.identity_map
        ])

        values = self._resolved_values_keys_as_propnames
        attrib = set(k for k, v in values)
        for state in states:
            to_expire = attrib.intersection(state.dict)
            if to_expire:
                session._expire_state(state, to_expire)
        session._register_altered(states)


class BulkDeleteFetch(BulkFetch, BulkDelete):
    """BulkUD which handles DELETEs using the "fetch"
    method of session resolution."""

    def _do_post_synchronize(self):
        session = self.query.session
        target_mapper = self.query._mapper_zero()
        for primary_key in self.matched_rows:
            # TODO: inline this and call remove_newly_deleted
            # once
            identity_key = target_mapper.identity_key_from_primary_key(
                list(primary_key))
            if identity_key in session.identity_map:
                session._remove_newly_deleted(
                    [attributes.instance_state(
                        session.identity_map[identity_key]
                    )]
                )
