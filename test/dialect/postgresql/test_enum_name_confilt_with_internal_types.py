from sqlalchemy import Column
from sqlalchemy import Connection
from sqlalchemy import Enum
from sqlalchemy import inspect
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.testing import fixtures


class EnumBuiltinNameTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

    def _get_column_type_name(
        self,
        connection: Connection,
        table: str,
        column: str,
    ) -> str:
        cols = inspect(connection).get_columns(table)
        return [c for c in cols if c["name"] == column][0][
            "type"
        ].__class__.__name__

    def test_enum_with_builtin_name(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum("Red", "Green", "Blue", name="text")

        Table("palette", metadata, Column("primary_color", enum_type))

        metadata.create_all(connection)

        col_type_name = self._get_column_type_name(
            connection, "palette", "primary_color"
        )
        assert col_type_name == "ENUM"

    def test_enum_with_explicit_schema_and_builtin_name(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum(
            "Spring",
            "Summer",
            "Autumn",
            "Winter",
            name="text",
            schema="public",
        )

        Table("calendar", metadata, Column("season", enum_type))

        metadata.create_all(connection)

        col_type_name = self._get_column_type_name(
            connection, "calendar", "season"
        )
        assert col_type_name == "ENUM"

    def test_multiple_enums_with_builtin_names(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum1 = Enum("Circle", "Square", "Triangle", name="text")
        enum2 = Enum("Dog", "Cat", "Horse", name="char")

        Table(
            "sketchbook",
            metadata,
            Column("shape", enum1),
            Column("animal", enum2),
        )

        metadata.create_all(connection)

        cols = inspect(connection).get_columns("sketchbook")
        assert all(col["type"].__class__.__name__ == "ENUM" for col in cols)

    def test_enum_with_non_conflicting_name(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum("North", "South", "East", "West", name="compass")

        Table("map", metadata, Column("direction", enum_type))

        metadata.create_all(connection)

        col_type_name = self._get_column_type_name(
            connection, "map", "direction"
        )
        assert col_type_name == "ENUM"

    def test_enum_reuse_across_tables(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum("Monday", "Tuesday", "Friday", name="weekday")

        Table("work_schedule", metadata, Column("day", enum_type))
        Table("class_schedule", metadata, Column("day", enum_type))

        metadata.create_all(connection)

        for table, col in [
            ("work_schedule", "day"),
            ("class_schedule", "day"),
        ]:
            col_type_name = self._get_column_type_name(connection, table, col)
            assert col_type_name == "ENUM"

    def test_enum_case_sensitivity(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum("Case", "case", "CASE", name="cases")

        Table("notebook", metadata, Column("label", enum_type))

        metadata.create_all(connection)

        col_type_name = self._get_column_type_name(
            connection, "notebook", "label"
        )
        assert col_type_name == "ENUM"

    def test_enum_dropped_and_recreated(
        self,
        connection: Connection,
    ) -> None:
        metadata = MetaData()
        enum_type = Enum("Morning", "Evening", name="timeofday")

        Table("schedule", metadata, Column("slot", enum_type))

        metadata.create_all(connection)
        metadata.drop_all(connection)
        metadata.create_all(connection)

        col_type_name = self._get_column_type_name(
            connection, "schedule", "slot"
        )
        assert col_type_name == "ENUM"
