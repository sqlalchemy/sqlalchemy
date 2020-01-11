from . import config
from .assertions import eq_
from .assertions import is_
from .assertions import is_not_
from .assertions import is_true
from .fixtures import TablesTest
from .. import inspect
from .. import Integer
from ..schema import Column
from ..schema import Computed
from ..schema import MetaData
from ..schema import Table


class ComputedReflectionFixtureTest(TablesTest):
    run_inserts = run_deletes = None

    __backend__ = True
    __requires__ = ("computed_columns", "table_reflection")

    return_persisted = None
    default_persisted = None
    support_stored = None
    support_virtual = None

    def to_sqltext(self, column, op, value):
        assert False, "override in dialect"

    @classmethod
    def _col(cls, name, sqltext, persisted):
        if (
            persisted
            and cls.support_stored
            or not persisted
            and cls.support_virtual
        ):
            return [
                Column(name, Integer, Computed(sqltext, persisted=persisted))
            ]
        return []

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

        Table(
            "computed_column_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_no_flag", Integer, Computed("normal + 42")),
            *cls._col("computed_virtual", "normal + 2", False),
            *cls._col("computed_stored", "normal - 42", True),
        )

        Table(
            "computed_column_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_no_flag", Integer, Computed("normal / 42")),
            *cls._col("computed_virtual", "normal / 2", False),
            *cls._col("computed_stored", "normal * 42", True),
            schema=config.test_schema,
        )

    def test_computed_col_default_not_set(self):
        insp = inspect(config.db)

        cols = insp.get_columns("computed_column_table")
        for col in cols:
            if col["name"] == "with_default":
                is_true("42" in col["default"])
            elif not col["autoincrement"]:
                is_(col["default"], None)

    def test_get_column_returns_computed(self):
        insp = inspect(config.db)

        cols = insp.get_columns("computed_default_table")
        data = {c["name"]: c for c in cols}
        for key in ("id", "normal", "with_default"):
            is_true("computed" not in data[key])
        compData = data["computed_col"]
        is_true("computed" in compData)
        is_true("sqltext" in compData["computed"])
        actual = compData["computed"]["sqltext"]

        eq_(self.to_sqltext("normal", "+", "42"), actual)
        eq_("persisted" in compData["computed"], self.return_persisted)
        if self.return_persisted:
            eq_(compData["computed"]["persisted"], self.default_persisted)

    def check_column(self, data, column, sqltext, persisted):
        is_true("computed" in data[column])
        compData = data[column]["computed"]
        actual = compData["sqltext"]
        eq_(sqltext, actual)
        if self.return_persisted:
            is_true("persisted" in compData)
            is_(compData["persisted"], persisted)

    def test_get_column_returns_persisted(self):
        insp = inspect(config.db)

        cols = insp.get_columns("computed_column_table")
        data = {c["name"]: c for c in cols}

        self.check_column(
            data,
            "computed_no_flag",
            self.to_sqltext("normal", "+", "42"),
            self.default_persisted,
        )
        if self.support_virtual:
            self.check_column(
                data,
                "computed_virtual",
                self.to_sqltext("normal", "+", "2"),
                False,
            )
        if self.support_stored:
            self.check_column(
                data,
                "computed_stored",
                self.to_sqltext("normal", "-", "42"),
                True,
            )

    def test_get_column_returns_persisted_with_schama(self):
        insp = inspect(config.db)

        cols = insp.get_columns(
            "computed_column_table", schema=config.test_schema
        )
        data = {c["name"]: c for c in cols}

        self.check_column(
            data,
            "computed_no_flag",
            self.to_sqltext("normal", "/", "42"),
            self.default_persisted,
        )
        if self.support_virtual:
            self.check_column(
                data,
                "computed_virtual",
                self.to_sqltext("normal", "/", "2"),
                False,
            )
        if self.support_stored:
            self.check_column(
                data,
                "computed_stored",
                self.to_sqltext("normal", "*", "42"),
                True,
            )

    def check_table_column(self, table, name, text, persisted):
        is_true(name in table.columns)
        col = table.columns[name]
        is_not_(col.computed, None)
        is_true(isinstance(col.computed, Computed))

        eq_(str(col.computed.sqltext), text)
        if self.return_persisted:
            eq_(col.computed.persisted, persisted)
        else:
            is_(col.computed.persisted, None)

    def test_table_reflection(self):
        meta = MetaData()
        table = Table("computed_column_table", meta, autoload_with=config.db)

        self.check_table_column(
            table,
            "computed_no_flag",
            self.to_sqltext("normal", "+", "42"),
            self.default_persisted,
        )
        if self.support_virtual:
            self.check_table_column(
                table,
                "computed_virtual",
                self.to_sqltext("normal", "+", "2"),
                False,
            )
        if self.support_stored:
            self.check_table_column(
                table,
                "computed_stored",
                self.to_sqltext("normal", "-", "42"),
                True,
            )
