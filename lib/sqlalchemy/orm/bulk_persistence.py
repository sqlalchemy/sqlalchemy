# orm/bulk_persistence.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors


"""additional ORM persistence classes related to "bulk" operations,
specifically outside of the flush() process.

"""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterable
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from . import attributes
from . import evaluator
from . import exc as orm_exc
from . import persistence
from .base import NO_VALUE
from .context import AbstractORMCompileState
from .. import exc as sa_exc
from .. import sql
from .. import util
from ..engine import Dialect
from ..engine import result as _result
from ..sql import coercions
from ..sql import expression
from ..sql import roles
from ..sql import select
from ..sql import sqltypes
from ..sql.base import _entity_namespace_key
from ..sql.base import CompileState
from ..sql.base import Options
from ..sql.dml import DeleteDMLState
from ..sql.dml import InsertDMLState
from ..sql.dml import UpdateDMLState
from ..util import EMPTY_DICT
from ..util.typing import Literal

if TYPE_CHECKING:
    from .mapper import Mapper
    from .session import ORMExecuteState
    from .session import SessionTransaction
    from .state import InstanceState

_O = TypeVar("_O", bound=object)


_SynchronizeSessionArgument = Literal[False, "evaluate", "fetch"]


def _bulk_insert(
    mapper: Mapper[_O],
    mappings: Union[Iterable[InstanceState[_O]], Iterable[Dict[str, Any]]],
    session_transaction: SessionTransaction,
    isstates: bool,
    return_defaults: bool,
    render_nulls: bool,
) -> None:
    base_mapper = mapper.base_mapper

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
            ) in persistence._collect_insert_commands(
                table,
                ((None, mapping, mapper, connection) for mapping in mappings),
                bulk=True,
                return_defaults=return_defaults,
                render_nulls=render_nulls,
            )
        )
        persistence._emit_insert_statements(
            base_mapper,
            None,
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
    mapper: Mapper[Any],
    mappings: Union[Iterable[InstanceState[_O]], Iterable[Dict[str, Any]]],
    session_transaction: SessionTransaction,
    isstates: bool,
    update_changed_only: bool,
) -> None:
    base_mapper = mapper.base_mapper

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

        records = persistence._collect_update_commands(
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

        persistence._emit_update_statements(
            base_mapper,
            None,
            super_mapper,
            table,
            records,
            bookkeeping=False,
        )


class ORMDMLState(AbstractORMCompileState):
    @classmethod
    def get_entity_description(cls, statement):
        ext_info = statement.table._annotations["parententity"]
        mapper = ext_info.mapper
        if ext_info.is_aliased_class:
            _label_name = ext_info.name
        else:
            _label_name = mapper.class_.__name__

        return {
            "name": _label_name,
            "type": mapper.class_,
            "expr": ext_info.entity,
            "entity": ext_info.entity,
            "table": mapper.local_table,
        }

    @classmethod
    def get_returning_column_descriptions(cls, statement):
        def _ent_for_col(c):
            return c._annotations.get("parententity", None)

        def _attr_for_col(c, ent):
            if ent is None:
                return c
            proxy_key = c._annotations.get("proxy_key", None)
            if not proxy_key:
                return c
            else:
                return getattr(ent.entity, proxy_key, c)

        return [
            {
                "name": c.key,
                "type": c.type,
                "expr": _attr_for_col(c, ent),
                "aliased": ent.is_aliased_class,
                "entity": ent.entity,
            }
            for c, ent in [
                (c, _ent_for_col(c)) for c in statement._all_selected_columns
            ]
        ]


class BulkUDCompileState(ORMDMLState):
    class default_update_options(Options):
        _synchronize_session: _SynchronizeSessionArgument = "evaluate"
        _is_delete_using = False
        _is_update_from = False
        _autoflush = True
        _subject_mapper = None
        _resolved_values = EMPTY_DICT
        _resolved_keys_as_propnames = EMPTY_DICT
        _value_evaluators = EMPTY_DICT
        _matched_objects = None
        _matched_rows = None
        _refresh_identity_token = None

    @classmethod
    def can_use_returning(
        cls,
        dialect: Dialect,
        mapper: Mapper[Any],
        *,
        is_multitable: bool = False,
        is_update_from: bool = False,
        is_delete_using: bool = False,
    ) -> bool:
        raise NotImplementedError()

    @classmethod
    def orm_pre_session_exec(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        is_reentrant_invoke,
    ):
        if is_reentrant_invoke:
            return statement, execution_options

        (
            update_options,
            execution_options,
        ) = BulkUDCompileState.default_update_options.from_execution_options(
            "_sa_orm_update_options",
            {"synchronize_session", "is_delete_using", "is_update_from"},
            execution_options,
            statement._execution_options,
        )

        sync = update_options._synchronize_session
        if sync is not None:
            if sync not in ("evaluate", "fetch", False):
                raise sa_exc.ArgumentError(
                    "Valid strategies for session synchronization "
                    "are 'evaluate', 'fetch', False"
                )

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

        statement = statement._annotate(
            {
                "synchronize_session": update_options._synchronize_session,
                "is_delete_using": update_options._is_delete_using,
                "is_update_from": update_options._is_update_from,
            }
        )

        # this stage of the execution is called before the do_orm_execute event
        # hook.  meaning for an extension like horizontal sharding, this step
        # happens before the extension splits out into multiple backends and
        # runs only once.  if we do pre_sync_fetch, we execute a SELECT
        # statement, which the horizontal sharding extension splits amongst the
        # shards and combines the results together.

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

        return (
            statement,
            util.immutabledict(execution_options).union(
                {"_sa_orm_update_options": update_options}
            ),
        )

    @classmethod
    def orm_setup_cursor_result(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        result,
    ):

        # this stage of the execution is called after the
        # do_orm_execute event hook.  meaning for an extension like
        # horizontal sharding, this step happens *within* the horizontal
        # sharding event handler which calls session.execute() re-entrantly
        # and will occur for each backend individually.
        # the sharding extension then returns its own merged result from the
        # individual ones we return here.

        update_options = execution_options["_sa_orm_update_options"]
        if update_options._synchronize_session == "evaluate":
            cls._do_post_synchronize_evaluate(session, result, update_options)
        elif update_options._synchronize_session == "fetch":
            cls._do_post_synchronize_fetch(session, result, update_options)

        return result

    @classmethod
    def _adjust_for_extra_criteria(cls, global_attributes, ext_info):
        """Apply extra criteria filtering.

        For all distinct single-table-inheritance mappers represented in the
        table being updated or deleted, produce additional WHERE criteria such
        that only the appropriate subtypes are selected from the total results.

        Additionally, add WHERE criteria originating from LoaderCriteriaOptions
        collected from the statement.

        """

        return_crit = ()

        adapter = ext_info._adapter if ext_info.is_aliased_class else None

        if (
            "additional_entity_criteria",
            ext_info.mapper,
        ) in global_attributes:
            return_crit += tuple(
                ae._resolve_where_criteria(ext_info)
                for ae in global_attributes[
                    ("additional_entity_criteria", ext_info.mapper)
                ]
                if ae.include_aliases or ae.entity is ext_info
            )

        if ext_info.mapper._single_table_criterion is not None:
            return_crit += (ext_info.mapper._single_table_criterion,)

        if adapter:
            return_crit = tuple(adapter.traverse(crit) for crit in return_crit)

        return return_crit

    @classmethod
    def _interpret_returning_rows(cls, mapper, rows):
        """translate from local inherited table columns to base mapper
        primary key columns.

        Joined inheritance mappers always establish the primary key in terms of
        the base table.   When we UPDATE a sub-table, we can only get
        RETURNING for the sub-table's columns.

        Here, we create a lookup from the local sub table's primary key
        columns to the base table PK columns so that we can get identity
        key values from RETURNING that's against the joined inheritance
        sub-table.

        the complexity here is to support more than one level deep of
        inheritance, where we have to link columns to each other across
        the inheritance hierarchy.

        """

        if mapper.local_table is not mapper.base_mapper.local_table:
            return rows

        # this starts as a mapping of
        # local_pk_col: local_pk_col.
        # we will then iteratively rewrite the "value" of the dict with
        # each successive superclass column
        local_pk_to_base_pk = {pk: pk for pk in mapper.local_table.primary_key}

        for mp in mapper.iterate_to_root():
            if mp.inherits is None:
                break
            elif mp.local_table is mp.inherits.local_table:
                continue

            t_to_e = dict(mp._table_to_equated[mp.inherits.local_table])
            col_to_col = {sub_pk: super_pk for super_pk, sub_pk in t_to_e[mp]}
            for pk, super_ in local_pk_to_base_pk.items():
                local_pk_to_base_pk[pk] = col_to_col[super_]

        lookup = {
            local_pk_to_base_pk[lpk]: idx
            for idx, lpk in enumerate(mapper.local_table.primary_key)
        }
        primary_key_convert = [
            lookup[bpk] for bpk in mapper.base_mapper.primary_key
        ]

        return [tuple(row[idx] for idx in primary_key_convert) for row in rows]

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

        value_evaluators = resolved_keys_as_propnames = EMPTY_DICT

        try:
            evaluator_compiler = evaluator.EvaluatorCompiler(target_cls)
            crit = ()
            if statement._where_criteria:
                crit += statement._where_criteria

            global_attributes = {}
            for opt in statement._with_options:
                if opt._is_criteria_option:
                    opt.get_global_criteria(global_attributes)

            if global_attributes:
                crit += cls._adjust_for_extra_criteria(
                    global_attributes, mapper
                )

            if crit:
                eval_condition = evaluator_compiler.process(*crit)
            else:

                def eval_condition(obj):
                    return True

        except evaluator.UnevaluatableError as err:
            raise sa_exc.InvalidRequestError(
                'Could not evaluate current criteria in Python: "%s". '
                "Specify 'fetch' or False for the "
                "synchronize_session execution option." % err
            ) from err

        if statement.__visit_name__ == "lambda_element":
            # ._resolved is called on every LambdaElement in order to
            # generate the cache key, so this access does not add
            # additional expense
            effective_statement = statement._resolved
        else:
            effective_statement = statement

        if effective_statement.__visit_name__ == "update":
            resolved_values = cls._get_resolved_values(
                mapper, effective_statement
            )
            value_evaluators = {}
            resolved_keys_as_propnames = cls._resolved_keys_as_propnames(
                mapper, resolved_values
            )
            for key, value in resolved_keys_as_propnames:
                try:
                    _evaluator = evaluator_compiler.process(
                        coercions.expect(roles.ExpressionElementRole, value)
                    )
                except evaluator.UnevaluatableError:
                    pass
                else:
                    value_evaluators[key] = _evaluator

        # TODO: detect when the where clause is a trivial primary key match.
        matched_objects = [
            state.obj()
            for state in session.identity_map.all_states()
            if state.mapper.isa(mapper)
            and not state.expired
            and eval_condition(state.obj())
            and (
                update_options._refresh_identity_token is None
                # TODO: coverage for the case where horizontal sharding
                # invokes an update() or delete() given an explicit identity
                # token up front
                or state.identity_token
                == update_options._refresh_identity_token
            )
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
            return list(statement._ordered_values)
        elif statement._values:
            return list(statement._values.items())
        else:
            return []

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

        select_stmt = (
            select(*(mapper.primary_key + (mapper.select_identity_token,)))
            .select_from(mapper)
            .options(*statement._with_options)
        )
        select_stmt._where_criteria = statement._where_criteria

        def skip_for_returning(orm_context: ORMExecuteState) -> Any:
            bind = orm_context.session.get_bind(**orm_context.bind_arguments)
            if cls.can_use_returning(
                bind.dialect,
                mapper,
                is_update_from=update_options._is_update_from,
                is_delete_using=update_options._is_delete_using,
            ):
                return _result.null_result()
            else:
                return None

        result = session.execute(
            select_stmt,
            params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            _add_event=skip_for_returning,
        )
        matched_rows = result.fetchall()

        value_evaluators = EMPTY_DICT

        if statement.__visit_name__ == "lambda_element":
            # ._resolved is called on every LambdaElement in order to
            # generate the cache key, so this access does not add
            # additional expense
            effective_statement = statement._resolved
        else:
            effective_statement = statement

        if effective_statement.__visit_name__ == "update":
            target_cls = mapper.class_
            evaluator_compiler = evaluator.EvaluatorCompiler(target_cls)
            resolved_values = cls._get_resolved_values(
                mapper, effective_statement
            )
            resolved_keys_as_propnames = cls._resolved_keys_as_propnames(
                mapper, resolved_values
            )

            resolved_keys_as_propnames = cls._resolved_keys_as_propnames(
                mapper, resolved_values
            )
            value_evaluators = {}
            for key, value in resolved_keys_as_propnames:
                try:
                    _evaluator = evaluator_compiler.process(
                        coercions.expect(roles.ExpressionElementRole, value)
                    )
                except evaluator.UnevaluatableError:
                    pass
                else:
                    value_evaluators[key] = _evaluator

        else:
            resolved_keys_as_propnames = EMPTY_DICT

        return update_options + {
            "_value_evaluators": value_evaluators,
            "_matched_rows": matched_rows,
            "_resolved_keys_as_propnames": resolved_keys_as_propnames,
        }


@CompileState.plugin_for("orm", "insert")
class ORMInsert(ORMDMLState, InsertDMLState):
    @classmethod
    def orm_pre_session_exec(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        is_reentrant_invoke,
    ):
        bind_arguments["clause"] = statement
        try:
            plugin_subject = statement._propagate_attrs["plugin_subject"]
        except KeyError:
            assert False, "statement had 'orm' plugin but no plugin_subject"
        else:
            bind_arguments["mapper"] = plugin_subject.mapper

        return (
            statement,
            util.immutabledict(execution_options),
        )

    @classmethod
    def orm_setup_cursor_result(
        cls,
        session,
        statement,
        params,
        execution_options,
        bind_arguments,
        result,
    ):
        return result


@CompileState.plugin_for("orm", "update")
class BulkORMUpdate(BulkUDCompileState, UpdateDMLState):
    @classmethod
    def create_for_statement(cls, statement, compiler, **kw):

        self = cls.__new__(cls)

        ext_info = statement.table._annotations["parententity"]

        self.mapper = mapper = ext_info.mapper

        self.extra_criteria_entities = {}

        self._resolved_values = cls._get_resolved_values(mapper, statement)

        extra_criteria_attributes = {}

        for opt in statement._with_options:
            if opt._is_criteria_option:
                opt.get_global_criteria(extra_criteria_attributes)

        if statement._values:
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

        new_crit = cls._adjust_for_extra_criteria(
            extra_criteria_attributes, mapper
        )
        if new_crit:
            new_stmt = new_stmt.where(*new_crit)

        # if we are against a lambda statement we might not be the
        # topmost object that received per-execute annotations

        # do this first as we need to determine if there is
        # UPDATE..FROM

        UpdateDMLState.__init__(self, new_stmt, compiler, **kw)

        if compiler._annotations.get(
            "synchronize_session", None
        ) == "fetch" and self.can_use_returning(
            compiler.dialect, mapper, is_multitable=self.is_multitable
        ):
            if new_stmt._returning:
                raise sa_exc.InvalidRequestError(
                    "Can't use synchronize_session='fetch' "
                    "with explicit returning()"
                )
            self.statement = self.statement.returning(
                *mapper.local_table.primary_key
            )

        return self

    @classmethod
    def can_use_returning(
        cls,
        dialect: Dialect,
        mapper: Mapper[Any],
        *,
        is_multitable: bool = False,
        is_update_from: bool = False,
        is_delete_using: bool = False,
    ) -> bool:

        # normal answer for "should we use RETURNING" at all.
        normal_answer = (
            dialect.update_returning and mapper.local_table.implicit_returning
        )
        if not normal_answer:
            return False

        # these workarounds are currently hypothetical for UPDATE,
        # unlike DELETE where they impact MariaDB
        if is_update_from:
            return dialect.update_returning_multifrom

        elif is_multitable and not dialect.update_returning_multifrom:
            raise sa_exc.CompileError(
                f'Dialect "{dialect.name}" does not support RETURNING '
                "with UPDATE..FROM; for synchronize_session='fetch', "
                "please add the additional execution option "
                "'is_update_from=True' to the statement to indicate that "
                "a separate SELECT should be used for this backend."
            )

        return True

    @classmethod
    def _get_crud_kv_pairs(cls, statement, kv_iterator):
        plugin_subject = statement._propagate_attrs["plugin_subject"]

        core_get_crud_kv_pairs = UpdateDMLState._get_crud_kv_pairs

        if not plugin_subject or not plugin_subject.mapper:
            return core_get_crud_kv_pairs(statement, kv_iterator)

        mapper = plugin_subject.mapper

        values = []

        for k, v in kv_iterator:
            k = coercions.expect(roles.DMLColumnRole, k)

            if isinstance(k, str):
                desc = _entity_namespace_key(mapper, k, default=NO_VALUE)
                if desc is NO_VALUE:
                    values.append(
                        (
                            k,
                            coercions.expect(
                                roles.ExpressionElementRole,
                                v,
                                type_=sqltypes.NullType(),
                                is_crud=True,
                            ),
                        )
                    )
                else:
                    values.extend(
                        core_get_crud_kv_pairs(
                            statement, desc._bulk_update_tuples(v)
                        )
                    )
            elif "entity_namespace" in k._annotations:
                k_anno = k._annotations
                attr = _entity_namespace_key(
                    k_anno["entity_namespace"], k_anno["proxy_key"]
                )
                values.extend(
                    core_get_crud_kv_pairs(
                        statement, attr._bulk_update_tuples(v)
                    )
                )
            else:
                values.append(
                    (
                        k,
                        coercions.expect(
                            roles.ExpressionElementRole,
                            v,
                            type_=sqltypes.NullType(),
                            is_crud=True,
                        ),
                    )
                )
        return values

    @classmethod
    def _do_post_synchronize_evaluate(cls, session, result, update_options):

        states = set()
        evaluated_keys = list(update_options._value_evaluators.keys())
        values = update_options._resolved_keys_as_propnames
        attrib = set(k for k, v in values)
        for obj in update_options._matched_objects:

            state, dict_ = (
                attributes.instance_state(obj),
                attributes.instance_dict(obj),
            )

            # the evaluated states were gathered across all identity tokens.
            # however the post_sync events are called per identity token,
            # so filter.
            if (
                update_options._refresh_identity_token is not None
                and state.identity_token
                != update_options._refresh_identity_token
            ):
                continue

            # only evaluate unmodified attributes
            to_evaluate = state.unmodified.intersection(evaluated_keys)
            for key in to_evaluate:
                if key in dict_:
                    dict_[key] = update_options._value_evaluators[key](obj)

            state.manager.dispatch.refresh(state, None, to_evaluate)

            state._commit(dict_, list(to_evaluate))

            to_expire = attrib.intersection(dict_).difference(to_evaluate)
            if to_expire:
                state._expire_attributes(dict_, to_expire)

            states.add(state)
        session._register_altered(states)

    @classmethod
    def _do_post_synchronize_fetch(cls, session, result, update_options):
        target_mapper = update_options._subject_mapper

        states = set()
        evaluated_keys = list(update_options._value_evaluators.keys())

        if result.returns_rows:
            rows = cls._interpret_returning_rows(target_mapper, result.all())

            matched_rows = [
                tuple(row) + (update_options._refresh_identity_token,)
                for row in rows
            ]
        else:
            matched_rows = update_options._matched_rows

        objs = [
            session.identity_map[identity_key]
            for identity_key in [
                target_mapper.identity_key_from_primary_key(
                    list(primary_key),
                    identity_token=identity_token,
                )
                for primary_key, identity_token in [
                    (row[0:-1], row[-1]) for row in matched_rows
                ]
                if update_options._refresh_identity_token is None
                or identity_token == update_options._refresh_identity_token
            ]
            if identity_key in session.identity_map
        ]

        values = update_options._resolved_keys_as_propnames
        attrib = set(k for k, v in values)

        for obj in objs:
            state, dict_ = (
                attributes.instance_state(obj),
                attributes.instance_dict(obj),
            )

            to_evaluate = state.unmodified.intersection(evaluated_keys)
            for key in to_evaluate:
                if key in dict_:
                    dict_[key] = update_options._value_evaluators[key](obj)
            state.manager.dispatch.refresh(state, None, to_evaluate)

            state._commit(dict_, list(to_evaluate))

            to_expire = attrib.intersection(dict_).difference(to_evaluate)
            if to_expire:
                state._expire_attributes(dict_, to_expire)

            states.add(state)
        session._register_altered(states)


@CompileState.plugin_for("orm", "delete")
class BulkORMDelete(BulkUDCompileState, DeleteDMLState):
    @classmethod
    def create_for_statement(cls, statement, compiler, **kw):
        self = cls.__new__(cls)

        ext_info = statement.table._annotations["parententity"]
        self.mapper = mapper = ext_info.mapper

        self.extra_criteria_entities = {}

        extra_criteria_attributes = {}

        for opt in statement._with_options:
            if opt._is_criteria_option:
                opt.get_global_criteria(extra_criteria_attributes)

        new_crit = cls._adjust_for_extra_criteria(
            extra_criteria_attributes, mapper
        )
        if new_crit:
            statement = statement.where(*new_crit)

        # do this first as we need to determine if there is
        # DELETE..FROM
        DeleteDMLState.__init__(self, statement, compiler, **kw)

        if compiler._annotations.get(
            "synchronize_session", None
        ) == "fetch" and self.can_use_returning(
            compiler.dialect,
            mapper,
            is_multitable=self.is_multitable,
            is_delete_using=compiler._annotations.get(
                "is_delete_using", False
            ),
        ):
            self.statement = statement.returning(*statement.table.primary_key)

        return self

    @classmethod
    def can_use_returning(
        cls,
        dialect: Dialect,
        mapper: Mapper[Any],
        *,
        is_multitable: bool = False,
        is_update_from: bool = False,
        is_delete_using: bool = False,
    ) -> bool:

        # normal answer for "should we use RETURNING" at all.
        normal_answer = (
            dialect.delete_returning and mapper.local_table.implicit_returning
        )
        if not normal_answer:
            return False

        # now get into special workarounds because MariaDB supports
        # DELETE...RETURNING but not DELETE...USING...RETURNING.
        if is_delete_using:
            # is_delete_using hint was passed.   use
            # additional dialect feature (True for PG, False for MariaDB)
            return dialect.delete_returning_multifrom

        elif is_multitable and not dialect.delete_returning_multifrom:
            # is_delete_using hint was not passed, but we determined
            # at compile time that this is in fact a DELETE..USING.
            # it's too late to continue since we did not pre-SELECT.
            # raise that we need that hint up front.

            raise sa_exc.CompileError(
                f'Dialect "{dialect.name}" does not support RETURNING '
                "with DELETE..USING; for synchronize_session='fetch', "
                "please add the additional execution option "
                "'is_delete_using=True' to the statement to indicate that "
                "a separate SELECT should be used for this backend."
            )

        return True

    @classmethod
    def _do_post_synchronize_evaluate(cls, session, result, update_options):

        session._remove_newly_deleted(
            [
                attributes.instance_state(obj)
                for obj in update_options._matched_objects
            ]
        )

    @classmethod
    def _do_post_synchronize_fetch(cls, session, result, update_options):
        target_mapper = update_options._subject_mapper

        if result.returns_rows:
            rows = cls._interpret_returning_rows(target_mapper, result.all())

            matched_rows = [
                tuple(row) + (update_options._refresh_identity_token,)
                for row in rows
            ]
        else:
            matched_rows = update_options._matched_rows

        for row in matched_rows:
            primary_key = row[0:-1]
            identity_token = row[-1]

            # TODO: inline this and call remove_newly_deleted
            # once
            identity_key = target_mapper.identity_key_from_primary_key(
                list(primary_key),
                identity_token=identity_token,
            )
            if identity_key in session.identity_map:
                session._remove_newly_deleted(
                    [
                        attributes.instance_state(
                            session.identity_map[identity_key]
                        )
                    ]
                )
