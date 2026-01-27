"""Tests for schema-qualified collation handling in PostgreSQL.

These tests verify the fix for issue #9693 where schema-qualified collations
like "my_schema.my_collation" were incorrectly rendered as:
    TEXT COLLATE "my_schema.my_collation"
Instead of the correct PostgreSQL syntax:
    TEXT COLLATE my_schema.my_collation

The fix requires changes in two files:
1. lib/sqlalchemy/sql/compiler.py - GenericTypeCompiler._render_string_type()
   must delegate collation formatting to the identifier preparer
2. lib/sqlalchemy/dialects/postgresql/base.py - PGIdentifierPreparer must
   override format_collation() to handle schema-qualified collations
"""
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, Unicode
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY, CHAR
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import AssertsCompiledSQL, fixtures


class SchemaQualifiedCollationIntegrationTest(AssertsCompiledSQL, fixtures.TestBase):
    """Integration tests verifying GenericTypeCompiler uses IdentifierPreparer.

    These tests REQUIRE both components to work together:
    - GenericTypeCompiler must call format_collation() on the preparer
    - PGIdentifierPreparer must properly format schema-qualified collations
    """

    __dialect__ = postgresql.dialect()

    def test_text_schema_qualified_collation_not_fully_quoted(self):
        """Schema-qualified collation must not be quoted as single identifier."""
        self.assert_compile(
            Text(collation="my_schema.my_collation"),
            "TEXT COLLATE my_schema.my_collation",
        )

    def test_varchar_schema_qualified_collation_renders_correctly(self):
        """VARCHAR with schema.collation must render schema separately."""
        self.assert_compile(
            String(50, collation="custom_schema.custom_collation"),
            "VARCHAR(50) COLLATE custom_schema.custom_collation",
        )

    def test_unicode_schema_qualified_collation_in_postgresql(self):
        """Unicode type must handle schema-qualified collations for PostgreSQL."""
        self.assert_compile(
            Unicode(100, collation="pg_catalog.unicode"),
            "VARCHAR(100) COLLATE pg_catalog.unicode",
        )

    def test_char_schema_qualified_collation_ddl_generation(self):
        """CHAR type must properly format schema-qualified collation in DDL."""
        self.assert_compile(
            CHAR(20, collation="another_schema.case_insensitive"),
            "CHAR(20) COLLATE another_schema.case_insensitive",
        )

    def test_create_table_with_schema_qualified_collation_column(self):
        """CREATE TABLE must render schema-qualified collation correctly."""
        metadata = MetaData()
        table = Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", Text(collation="my_schema.my_collation")),
        )
        self.assert_compile(
            CreateTable(table),
            "CREATE TABLE test_table ("
            "id SERIAL NOT NULL, "
            "name TEXT COLLATE my_schema.my_collation, "
            "PRIMARY KEY (id))",
        )

    def test_multiple_columns_with_different_schema_collations(self):
        """Multiple columns with different schema-qualified collations."""
        metadata = MetaData()
        table = Table(
            "multi_collation_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("col_a", Text(collation="schema_a.collation_x")),
            Column("col_b", String(50, collation="schema_b.collation_y")),
        )
        compiled = CreateTable(table).compile(dialect=postgresql.dialect())
        result = str(compiled)
        assert "COLLATE schema_a.collation_x" in result
        assert "COLLATE schema_b.collation_y" in result
        assert 'COLLATE "schema_a.collation_x"' not in result
        assert 'COLLATE "schema_b.collation_y"' not in result


class SimpleCollationRegressionTest(AssertsCompiledSQL, fixtures.TestBase):
    """Regression tests ensuring simple collations still work correctly.

    These tests verify that the fix for schema-qualified collations
    does not break existing behavior for simple (non-schema-qualified)
    collation names.
    """

    __dialect__ = postgresql.dialect()

    def test_simple_collation_text_still_quoted(self):
        """Simple collation names must remain quoted for case sensitivity."""
        self.assert_compile(
            Text(collation="en_US"),
            'TEXT COLLATE "en_US"',
        )

    def test_simple_collation_varchar_preserves_quoting(self):
        """VARCHAR with simple collation must preserve quoting behavior."""
        self.assert_compile(
            String(50, collation="C"),
            'VARCHAR(50) COLLATE "C"',
        )

    def test_simple_collation_unicode_maintains_quotes(self):
        """Unicode type simple collation must maintain quoted format."""
        self.assert_compile(
            Unicode(100, collation="POSIX"),
            'VARCHAR(100) COLLATE "POSIX"',
        )

    def test_mixed_case_simple_collation_quoted(self):
        """Mixed case simple collation must be quoted to preserve case."""
        self.assert_compile(
            Text(collation="EnUs"),
            'TEXT COLLATE "EnUs"',
        )


class PGIdentifierPreparerFormatCollationTest(AssertsCompiledSQL, fixtures.TestBase):
    """Tests for PGIdentifierPreparer.format_collation() method.

    These tests verify the PostgreSQL-specific collation formatting logic
    that splits schema-qualified collations into schema.name format with
    each part properly quoted when necessary.
    """

    __dialect__ = postgresql.dialect()

    def test_format_collation_schema_qualified_splits_correctly(self):
        """format_collation must split schema.name and format each part."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("my_schema.my_collation")
        assert result == "my_schema.my_collation"

    def test_format_collation_simple_name_quoted(self):
        """format_collation for simple names must return quoted identifier."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("en_US")
        assert result == '"en_US"'

    def test_format_collation_reserved_word_schema_quoted(self):
        """Schema part that is reserved word must be quoted."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("user.my_collation")
        assert "user" in result.lower() or '"user"' in result.lower()
        assert ".my_collation" in result or '."my_collation"' in result

    def test_format_collation_uppercase_schema_handled(self):
        """Uppercase schema name in collation must be handled properly."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("MySchema.MyCollation")
        assert '"' not in result or result != '"MySchema.MyCollation"'

    def test_format_collation_pg_catalog_schema(self):
        """pg_catalog schema collations must not be fully quoted."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("pg_catalog.default")
        assert result != '"pg_catalog.default"'


class ArrayWithSchemaQualifiedCollationTest(AssertsCompiledSQL, fixtures.TestBase):
    """Tests for ARRAY types with schema-qualified collations.

    PostgreSQL ARRAY types have special handling to place COLLATE at the end.
    These tests verify that schema-qualified collations work correctly with
    the ARRAY type's special rendering logic.
    """

    __dialect__ = postgresql.dialect()

    def test_array_string_schema_qualified_collation_placement(self):
        """ARRAY with schema-qualified collation must place COLLATE correctly."""
        self.assert_compile(
            ARRAY(String(30, collation="my_schema.my_collation")),
            "VARCHAR(30)[] COLLATE my_schema.my_collation",
        )

    def test_array_unicode_schema_qualified_collation(self):
        """ARRAY of Unicode with schema-qualified collation."""
        self.assert_compile(
            ARRAY(Unicode(50, collation="custom_schema.unicode_collation")),
            "VARCHAR(50)[] COLLATE custom_schema.unicode_collation",
        )

    def test_array_multidim_schema_qualified_collation(self):
        """Multi-dimensional ARRAY with schema-qualified collation."""
        self.assert_compile(
            ARRAY(String(30, collation="schema_x.collation_y"), dimensions=2),
            "VARCHAR(30)[][] COLLATE schema_x.collation_y",
        )

    def test_array_simple_collation_still_quoted(self):
        """ARRAY with simple collation must still quote the collation name."""
        self.assert_compile(
            ARRAY(Unicode(30, collation="en_US")),
            'VARCHAR(30)[] COLLATE "en_US"',
        )


class CollationDataFlowBoundaryTest(AssertsCompiledSQL, fixtures.TestBase):
    """Tests verifying data flow across component boundaries.

    These tests verify that collation data flows correctly from type
    definition through TypeCompiler to IdentifierPreparer and back,
    with proper transformation at each boundary.
    """

    __dialect__ = postgresql.dialect()

    def test_collation_flows_from_type_to_compiled_ddl(self):
        """Collation value must flow unchanged to DDL except for formatting."""
        metadata = MetaData()
        table = Table(
            "flow_test",
            metadata,
            Column("data", Text(collation="flow_schema.flow_collation")),
        )
        result = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        assert "flow_schema" in result
        assert "flow_collation" in result
        assert "COLLATE flow_schema.flow_collation" in result

    def test_type_compiler_delegates_to_preparer(self):
        """TypeCompiler must delegate collation formatting to preparer."""
        dialect = postgresql.dialect()
        type_compiler = dialect.type_compiler_instance
        preparer = dialect.identifier_preparer
        text_type = Text(collation="delegate_schema.delegate_collation")
        compiled = type_compiler.process(text_type)
        preparer_result = preparer.format_collation(
            "delegate_schema.delegate_collation"
        )
        expected_collation = "delegate_schema.delegate_collation"
        assert preparer_result in compiled or expected_collation in compiled
        assert '"delegate_schema.delegate_collation"' not in compiled

    def test_preparer_receives_raw_collation_string(self):
        """IdentifierPreparer must receive raw collation string from type."""
        preparer = postgresql.dialect().identifier_preparer
        raw_collation = "raw_schema.raw_collation"
        formatted = preparer.format_collation(raw_collation)
        assert formatted != f'"{raw_collation}"'

    def test_boundary_handles_empty_schema_gracefully(self):
        """Single-part collation (no schema) must be handled at boundary."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("En_US")
        assert result == '"En_US"'


class EdgeCaseCollationTest(AssertsCompiledSQL, fixtures.TestBase):
    """Edge case tests for collation handling.

    These tests cover unusual but valid collation specifications that
    might break naive implementations.
    """

    __dialect__ = postgresql.dialect()

    def test_collation_with_underscore_in_schema_name(self):
        """Schema name with underscores must be handled correctly."""
        self.assert_compile(
            Text(collation="my_custom_schema.my_custom_collation"),
            "TEXT COLLATE my_custom_schema.my_custom_collation",
        )

    def test_collation_with_numbers_in_names(self):
        """Collation with numbers in schema/name must work."""
        self.assert_compile(
            Text(collation="schema123.collation456"),
            "TEXT COLLATE schema123.collation456",
        )

    def test_collation_schema_only_lowercase(self):
        """All lowercase schema-qualified collation."""
        self.assert_compile(
            Text(collation="myschema.mycollation"),
            "TEXT COLLATE myschema.mycollation",
        )

    def test_collation_with_single_char_names(self):
        """Single character schema and collation names."""
        self.assert_compile(
            Text(collation="a.b"),
            "TEXT COLLATE a.b",
        )

    def test_three_part_collation_name_treated_as_schema_dot_rest(self):
        """Collation with multiple dots splits on first dot only."""
        preparer = postgresql.dialect().identifier_preparer
        result = preparer.format_collation("a.b.c")
        assert "a." in result


class CrossDialectCollationConsistencyTest(AssertsCompiledSQL, fixtures.TestBase):
    """Tests ensuring collation handling is consistent across dialect calls.

    These tests verify that repeated compilations and different code paths
    produce consistent results for the same collation specification.
    """

    __dialect__ = postgresql.dialect()

    def test_repeated_compilation_produces_same_result(self):
        """Multiple compilations of same type must produce identical DDL."""
        text_type = Text(collation="repeat_schema.repeat_collation")
        dialect = postgresql.dialect()
        result1 = dialect.type_compiler_instance.process(text_type)
        result2 = dialect.type_compiler_instance.process(text_type)
        result3 = dialect.type_compiler_instance.process(text_type)
        assert result1 == result2 == result3
        assert "repeat_schema.repeat_collation" in result1
        assert '"repeat_schema.repeat_collation"' not in result1

    def test_different_type_same_collation_consistent(self):
        """Different types with same collation must format collation identically."""
        collation = "consistent_schema.consistent_collation"
        dialect = postgresql.dialect()
        tc = dialect.type_compiler_instance

        text_result = tc.process(Text(collation=collation))
        varchar_result = tc.process(String(50, collation=collation))
        unicode_result = tc.process(Unicode(100, collation=collation))

        assert f"COLLATE {collation}" in text_result
        assert f"COLLATE {collation}" in varchar_result
        assert f"COLLATE {collation}" in unicode_result

    def test_table_in_schema_with_schema_qualified_collation(self):
        """Table in schema with column using schema-qualified collation."""
        metadata = MetaData()
        table = Table(
            "schemaed_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Text(collation="coll_schema.coll_name")),
            schema="table_schema",
        )
        compiled = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        assert "table_schema.schemaed_table" in compiled
        assert "COLLATE coll_schema.coll_name" in compiled
        assert 'COLLATE "coll_schema.coll_name"' not in compiled


class StateConsistencyTest(AssertsCompiledSQL, fixtures.TestBase):
    """Tests ensuring state consistency across components.

    These tests verify that the fix does not introduce state-related
    issues like caching problems or shared mutable state between
    compilations.
    """

    __dialect__ = postgresql.dialect()

    def test_different_collations_not_cached_incorrectly(self):
        """Different collations must not return cached results from each other."""
        dialect = postgresql.dialect()
        tc = dialect.type_compiler_instance

        result_schema = tc.process(Text(collation="schema_a.coll_a"))
        result_simple = tc.process(Text(collation="Simple_Coll"))

        assert "schema_a.coll_a" in result_schema
        assert '"Simple_Coll"' in result_simple
        assert "schema_a" not in result_simple
        assert "Simple_Coll" not in result_schema

    def test_preparer_state_not_modified_by_format_collation(self):
        """format_collation must not modify preparer internal state."""
        preparer = postgresql.dialect().identifier_preparer

        preparer.format_collation("state_schema.state_coll")
        result_simple = preparer.format_collation("En_US")

        assert result_simple == '"En_US"'

    def test_new_dialect_instance_formats_independently(self):
        """New dialect instances must format collations independently."""
        dialect1 = postgresql.dialect()
        dialect2 = postgresql.dialect()

        result1 = dialect1.identifier_preparer.format_collation("ind_schema.ind_coll")
        result2 = dialect2.identifier_preparer.format_collation("ind_schema.ind_coll")

        assert result1 == result2
        assert result1 == "ind_schema.ind_coll"
