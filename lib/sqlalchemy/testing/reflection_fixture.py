import re

from . import config
from .fixtures import TablesTest
from .. import Integer
from .. import testing
from ..schema import Column
from ..schema import Computed
from ..schema import Table


class ComputedReflectionFixtureTest(TablesTest):
    run_inserts = run_deletes = None

    __backend__ = True
    __requires__ = ("computed_columns", "table_reflection")

    regexp = re.compile(r"[\[\]\(\)\s`]*")

    def normalize(self, text):
        return self.regexp.sub("", text)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "computed_default_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_col", Integer, Computed("normal + 42")),
            Column("with_default", Integer, server_default="42"),
        )

        t = Table(
            "computed_column_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_no_flag", Integer, Computed("normal + 42")),
        )

        t2 = Table(
            "computed_column_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_no_flag", Integer, Computed("normal / 42")),
            schema=config.test_schema,
        )
        if testing.requires.computed_columns_virtual.enabled:
            t.append_column(
                Column(
                    "computed_virtual",
                    Integer,
                    Computed("normal + 2", persisted=False),
                )
            )
            t2.append_column(
                Column(
                    "computed_virtual",
                    Integer,
                    Computed("normal / 2", persisted=False),
                )
            )
        if testing.requires.computed_columns_stored.enabled:
            t.append_column(
                Column(
                    "computed_stored",
                    Integer,
                    Computed("normal - 42", persisted=True),
                )
            )
            t2.append_column(
                Column(
                    "computed_stored",
                    Integer,
                    Computed("normal * 42", persisted=True),
                )
            )
