from sqlalchemy import testing
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.interfaces import CoreExecuteOptionsParameter
from sqlalchemy.ext.asyncio.engine import AsyncConnection
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm._typing import OrmExecuteOptionsParameter
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.base import Executable
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.util.typing import is_fwd_ref

engine_execution_options = {
    "compiled_cache": "Optional[CompiledCacheType]",
    "logging_token": "str",
    "isolation_level": "IsolationLevel",
    "insertmanyvalues_page_size": "int",
    "schema_translate_map": "Optional[SchemaTranslateMapType]",
    "opt": "Any",
}
core_execution_options = {
    **engine_execution_options,
    "no_parameters": "bool",
    "stream_results": "bool",
    "max_row_buffer": "int",
    "yield_per": "int",
    "preserve_rowcount": "bool",
    "driver_column_names": "bool",
}

orm_dql_execution_options = {
    **core_execution_options,
    "populate_existing": "bool",
    "autoflush": "bool",
}

orm_dml_execution_options = {
    "synchronize_session": "SynchronizeSessionArgument",
    "dml_strategy": "DMLStrategyArgument",
    "is_delete_using": "bool",
    "is_update_from": "bool",
    "render_nulls": "bool",
}

orm_execution_options = {
    **orm_dql_execution_options,
    **orm_dml_execution_options,
}


class OverloadTest(fixtures.TestBase):
    # NOTE: get_overloads is python 3.11. typing_extensions implements it
    # but for it to work the typing_extensions overload needs to be use and
    # it can only be imported directly from typing_extensions in all modules
    # that use it otherwise flake8 (pyflakes actually) will flag it with F811
    __requires__ = ("python311",)

    @testing.combinations(
        (Engine, engine_execution_options),
        (Connection, core_execution_options),
        (AsyncEngine, engine_execution_options),
        (AsyncConnection, core_execution_options),
        (Query, orm_dql_execution_options),
        (Executable, orm_execution_options),
    )
    def test_methods(self, class_, expected):
        from typing import get_overloads

        overloads = get_overloads(getattr(class_, "execution_options"))
        eq_(len(overloads), 2)
        annotations = overloads[0].__annotations__.copy()
        annotations.pop("self", None)
        annotations.pop("return", None)
        eq_(annotations, expected)
        annotations = overloads[1].__annotations__.copy()
        annotations.pop("self", None)
        annotations.pop("return", None)
        eq_(annotations, {"opt": "Any"})

    @testing.combinations(
        (CoreExecuteOptionsParameter, core_execution_options),
        # note: this failed on python 3.14.0b1
        # due to https://github.com/python/cpython/issues/133701.
        # something to keep in mind in case it breaks again
        (OrmExecuteOptionsParameter, orm_execution_options),
    )
    def test_typed_dicts(self, typ, expected):
        # we currently expect these to be union types with first entry
        # is the typed dict

        typed_dict = typ.__args__[0]

        expected = dict(expected)
        expected.pop("opt")

        assert_annotations = {
            key: fwd_ref.__forward_arg__ if is_fwd_ref(fwd_ref) else fwd_ref
            for key, fwd_ref in typed_dict.__annotations__.items()
        }
        eq_(assert_annotations, expected)
