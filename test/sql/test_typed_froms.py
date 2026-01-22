from typing import Annotated

import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import Double
from sqlalchemy import Integer
from sqlalchemy import Named
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import TypedColumns
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.testing import combinations
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import is_not
from sqlalchemy.testing import not_in


class TypedTableTest(fixtures.TestBase):
    """Test suite for typed table and TypedColumns classes."""

    def test_table_creation(self, metadata):
        """Test that typed_table creates an actual Table in metadata."""

        class name_does_not_matter(TypedColumns):
            id: Named[int]
            name: Named[str]

        test_table = Table("test_table", metadata, name_does_not_matter)

        is_instance_of(test_table, sa.Table)
        is_(type(test_table), sa.Table)
        eq_(test_table.name, "test_table")
        in_("test_table", metadata.tables)
        is_(metadata.tables["test_table"], test_table)

    def test_empty(self, metadata):
        """Test that typed_table creates an actual Table in metadata."""

        class empty_cols(TypedColumns):
            pass

        empty = Table("test_table", metadata, empty_cols)

        is_instance_of(empty, sa.Table)
        eq_(len(empty.c), 0)

    def test_simple_columns_with_objects(self, metadata):
        """Test table with explicit Column objects."""

        class users_cols(TypedColumns):
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        users = Table("users", metadata, users_cols, schema="my_schema")

        eq_(users.schema, "my_schema")
        eq_(len(users.c), 2)
        in_("id", users.c)
        in_("name", users.c)
        is_(users.c.id.primary_key, True)
        is_instance_of(users.c.name.type, String)
        eq_(users.c.name.type.length, 50)

    def test_columns_are_copied(self, metadata):
        """Test table with explicit Column objects."""

        class usersCols(TypedColumns):
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user = Table("users", metadata, usersCols, schema="my_schema")

        is_not(user.c.id, usersCols.id)
        is_not(user.c.name, usersCols.name)
        is_(usersCols.id.table, None)
        is_(usersCols.name.table, None)

    def test_columns_with_annotations_only(self, metadata):
        """Test table with type annotations only (no Column objects)."""

        class products_cols(TypedColumns):
            id: Column[int]
            name: Column[str]
            weight: Column[float]

        products = Table("products", metadata, products_cols)

        eq_(len(products.c), 3)
        in_("id", products.c)
        in_("name", products.c)
        in_("weight", products.c)
        is_instance_of(products.c.id.type, Integer)
        is_instance_of(products.c.name.type, String)
        is_instance_of(products.c.weight.type, Double)

    def test_columns_with_annotations_only_named(self, metadata):
        """Test table with type annotations only using Named."""

        class products_cols(TypedColumns):
            id: Named[int]
            name: Named[str]
            weight: Named[float]

        products = Table("products", metadata, products_cols)

        eq_(len(products.c), 3)
        in_("id", products.c)
        in_("name", products.c)
        in_("weight", products.c)
        is_instance_of(products.c.id.type, Integer)
        is_instance_of(products.c.name.type, String)
        is_instance_of(products.c.weight.type, Double)

    def test_mixed_columns_and_annotations(self, metadata):
        """Test table with mix of Column objects and annotations."""

        class items_cols(TypedColumns):
            id = Column(Integer, primary_key=True)
            name: Column[str]
            price: Column[float]

        items = Table("items", metadata, items_cols)

        eq_(len(items.c), 3)
        is_(items.c.id.primary_key, True)
        in_("name", items.c)
        in_("price", items.c)
        is_instance_of(items.c.id.type, Integer)
        is_instance_of(items.c.name.type, String)
        is_instance_of(items.c.price.type, Double)

    def test_annotation_completion(self, metadata):
        """Complete column information from annotation."""

        class items_cols(TypedColumns):
            id: Column[int | None] = Column(primary_key=True)
            name: Column[str] = Column(String(100))
            price: Column[float] = Column(nullable=True)

        items = Table("items", metadata, items_cols)

        eq_(len(items.c), 3)
        is_(items.c.id.primary_key, True)
        is_(items.c.id.nullable, False)
        in_("name", items.c)
        in_("price", items.c)
        is_instance_of(items.c.id.type, Integer)
        is_instance_of(items.c.name.type, String)
        eq_(items.c.name.type.length, 100)
        is_instance_of(items.c.price.type, Double)
        is_(items.c.price.nullable, True)

    def test_type_from_anno_ignored_when_provided(self, metadata):
        """Complete column information from annotation."""

        class items_cols(TypedColumns):
            id: Column[int | None] = Column(primary_key=True)
            name: Column[str] = Column(Double)
            price: Column[float] = Column(String)

        items = Table("items", metadata, items_cols)

        is_instance_of(items.c.name.type, Double)
        is_instance_of(items.c.price.type, String)

    def test_nullable_from_annotation(self, metadata):
        """Test nullable inference from Optional annotation."""

        class records_cols(TypedColumns):
            id: Column[int]
            description: Column[str | None]

        records = Table("records", metadata, records_cols)

        is_instance_of(records.c.id.type, Integer)
        is_(records.c.id.nullable, False)
        is_instance_of(records.c.description.type, String)
        is_(records.c.description.nullable, True)

    def test_nullable_from_annotation_ignored_when_set(self, metadata):
        """Test nullable inference is ignored if nullable is set"""

        class records_cols(TypedColumns):
            id: Column[int] = Column(nullable=True)
            description: Column[str | None] = Column(nullable=False)

        records = Table("records", metadata, records_cols)

        is_instance_of(records.c.id.type, Integer)
        is_(records.c.id.nullable, True)
        is_instance_of(records.c.description.type, String)
        is_(records.c.description.nullable, False)

    @combinations(True, False, argnames="define_cols")
    def test_use_same_typedcols_multiple_times(self, metadata, define_cols):
        if define_cols:

            class cols(TypedColumns):
                id = Column(Integer)
                name = Column(String)

        else:

            class cols(TypedColumns):
                id: Column[int]
                name: Column[str]

        t1 = Table("t1", metadata, cols)
        t2 = Table("t2", metadata, cols)
        is_not(t1.c.id, t2.c.id)
        is_not(t1.c.name, t2.c.name)

    def test_bad_anno_with_type_provided(self, metadata):
        """Test error when no type info is found."""

        class MyType:
            pass

        class ThisIsFine_cols(TypedColumns):
            id: Column[MyType] = Column(Double)
            name: Column = Column(String)

        ThisIsFine = Table("tbl", metadata, ThisIsFine_cols)

        is_instance_of(ThisIsFine.c.id.type, Double)
        is_instance_of(ThisIsFine.c.name.type, String)

    def test_inheritance_from_typed_columns(self, metadata):
        """Test column inheritance from parent TypedColumns."""

        class base_columns(TypedColumns):
            id: Column[int]

        class derived_cols(base_columns):
            name: Column[str]

        derived = Table("derived", metadata, derived_cols)

        eq_(len(derived.c), 2)
        in_("id", derived.c)
        in_("name", derived.c)
        eq_(derived.c.keys(), ["id", "name"])  # check order

    def test_many_mixin(self, metadata):
        class with_name(TypedColumns):
            name: Column[str]

        class with_age(TypedColumns):
            age: Column[int]

        class person_cols(with_age, with_name):
            id = Column(Integer, primary_key=True)

        person = Table("person", metadata, person_cols)

        eq_(person.c.keys(), ["name", "age", "id"])

    def test_shared_base_columns_different_tables(self, metadata):
        """Test that a base TypedColumns can be used in multiple tables
        with different instances."""

        class base_columns(TypedColumns):
            id: Column[int]

        class table1_cols(base_columns):
            name: Column[str]

        table1 = Table("table1", metadata, table1_cols)

        class table2_cols(base_columns):
            name: Column[str]

        table2 = Table("table2", metadata, table2_cols)

        in_("id", table1.c)
        in_("id", table2.c)
        is_not(table1.c.id, table2.c.id)

    def test_shared_column_with_pk_different_tables(self, metadata):
        """Test that base column instances with pk are independent in
        different tables."""

        class base_columns(TypedColumns):
            id = Column(Integer, primary_key=True)

        class table1_cols(base_columns):
            name: Column[str]

        table1 = Table("table1", metadata, table1_cols)

        class table2_cols(base_columns):
            other: Column[str]

        table2 = Table("table2", metadata, table2_cols)

        is_(table1.c.id.primary_key, True)
        is_(table2.c.id.primary_key, True)
        is_not(table1.c.id, table2.c.id)
        is_not(table1.c.id, base_columns.id)
        is_not(table2.c.id, base_columns.id)
        in_("name", table1.c)
        not_in("name", table2.c)
        in_("other", table2.c)
        not_in("other", table1.c)

    def test_override_column(self, metadata):
        """Test that a base TypedColumns can be used in multiple tables
        with different instances."""

        class base_columns(TypedColumns):
            id: Column[int]
            name: Column[str]
            theta: Column[float]

        class mid_columns(base_columns):
            name: Column[str | None]  # override to make nullable
            theta: Column[float] = Column(sa.Numeric(asdecimal=False))

        class table1_cols(mid_columns):
            id: Column[int] = Column(sa.BigInteger)

        table1 = Table("table1", metadata, table1_cols)

        eq_(len(table1.c), 3)
        is_instance_of(table1.c.id.type, sa.BigInteger)
        is_instance_of(table1.c.name.type, String)
        is_(table1.c.name.nullable, True)
        is_instance_of(table1.c.theta.type, sa.Numeric)
        is_(mid_columns.theta.table, None)

    def test_column_name_and_key_set(self, metadata):
        """Test that column name and key are properly set."""

        class t_cols(TypedColumns):
            user_id: Column[int]

        t = Table("t", metadata, t_cols)

        col = t.c.user_id
        eq_(col.name, "user_id")
        eq_(col.key, "user_id")

    def test_provide_different_col_name(self, metadata):
        class t_cols(TypedColumns):
            user_id: Column[int] = Column("uid")

        t = Table("t", metadata, t_cols)
        in_("user_id", t.c)
        not_in("uid", t.c)
        col = t.c.user_id
        eq_(col.name, "uid")
        eq_(col.key, "user_id")
        not_in("user_id", str(t.select()))

    def test_provide_different_key(self, metadata):
        # this doesn't make a lot of sense, but it's consistent with the orm
        class t_cols(TypedColumns):
            user_id: Column[int] = Column(key="uid")

        t = Table("t", metadata, t_cols)
        in_("uid", t.c)
        not_in("user_id", t.c)
        col = t.c.uid
        eq_(col.name, "user_id")
        eq_(col.key, "uid")
        not_in("uid", str(t.select()))

    def test_add_more_columns(self, metadata):

        class records_cols(TypedColumns):
            id: Column[int]
            description: Column[str | None]

        records = Table(
            "records",
            metadata,
            records_cols,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )

        eq_(records.c.keys(), ["id", "description", "x", "y", "z"])

    def test_add_constraints(self, metadata):

        class records_cols(TypedColumns):
            id: Column[int]
            description: Column[str | None]

        records = Table(
            "records",
            metadata,
            records_cols,
            Column("x", Integer),
            Column("y", Integer),
            sa.Index("foo", "id", "y"),
            sa.UniqueConstraint("description"),
        )

        eq_(records.c.keys(), ["id", "description", "x", "y"])
        is_(len(records.indexes), 1)
        eq_(list(records.indexes)[0].columns, [records.c.id, records.c.y])
        is_(len(records.constraints), 2)  # including pk
        (uq,) = [
            c
            for c in records.constraints
            if isinstance(c, sa.UniqueConstraint)
        ]
        eq_(uq.columns, [records.c.description])

    def test_init_no_col_no_typed_cols(self, metadata):
        tt = Table("a", metadata)
        eq_(len(tt.c), 0)

    def test_invalid_non_typed_columns(self, metadata):
        """Test that rejects non-TypedColumns subclasses."""
        with expect_raises_message(
            InvalidRequestError, "requires a TypedColumns subclass"
        ):

            class not_typed_columns:
                id = Column(Integer)

            Table("bad", metadata, not_typed_columns)

        with expect_raises_message(
            ArgumentError, "'SchemaItem' object, such as a 'Column'"
        ):
            Table("bad", metadata, 123)  # not a class at all

    def test_no_kw_args(self, metadata):
        """Test that rejects TypedColumns as kw args."""
        with expect_raises_message(
            TypeError,
            "The ``typed_columns_cls`` argument may be passed "
            "only positionally",
        ):

            class not_typed_columns(TypedColumns):
                id = Column(Integer)

            Table("bad", metadata, typed_columns_cls=not_typed_columns)

        with expect_raises_message(
            ArgumentError, "'SchemaItem' object, such as a 'Column'"
        ):
            Table("bad", metadata, 123)  # not a class at all

    def test_invalid_method_definition(self, metadata):
        """Test that TypedColumns rejects method definitions."""
        with expect_raises_message(
            InvalidRequestError, "may not define methods"
        ):

            class invalid(TypedColumns):
                id: Column[int]

                def some_method(self):
                    pass

    def test_cannot_interpret_annotation(self, metadata):
        with expect_raises_message(
            ArgumentError,
            "Could not interpret annotation this it not valid for "
            "attribute 'not_typed_columns.id'",
        ):

            class not_typed_columns(TypedColumns):
                id: "this it not valid"  # noqa

            Table("bad", metadata, not_typed_columns)

    def test_invalid_annotation_type(self, metadata):
        """Test error when annotation is not Column[...]."""

        with expect_raises_message(
            ArgumentError,
            "Annotation <class 'int'> for attribute 'bad_anno.id' is not "
            "of type Named/Column",
        ):

            class bad_anno(TypedColumns):
                id: int  # Missing Column[...]

            Table("bad_anno", metadata, bad_anno)

    def test_missing_generic_in_column(self, metadata):
        with expect_raises_message(
            ArgumentError,
            "No type information could be extracted from annotation "
            "<class 'sqlalchemy.sql.schema.Column'> for attribute "
            "'bad_anno.id'",
        ):

            class bad_anno(TypedColumns):
                id: Column  # missing generic

            Table("bad_anno", metadata, bad_anno)

    def test_missing_generic_in_named(self, metadata):
        with expect_raises_message(
            ArgumentError,
            "No type information could be extracted from annotation "
            "<class 'sqlalchemy.sql._annotated_cols.Named'> for attribute "
            "'bad_anno.id'",
        ):

            class bad_anno(TypedColumns):
                id: Named  # missing generic

            Table("bad_anno", metadata, bad_anno)

    def test_no_pep593(self, metadata):
        """Test nullable inference is ignored if nullable is set"""

        class records_cols(TypedColumns):
            id: Column[Annotated[int, "x"]]
            description: Column[str | None]

        with expect_raises_message(
            ArgumentError,
            "Could not find a SQL type for type typing.Annotated.+"
            " obtained from annotation .+ in attribute 'records_cols.id'",
        ):

            Table("records", metadata, records_cols)

    def test_no_pep593_columns(self, metadata):
        """Test nullable inference is ignored if nullable is set"""

        class records_cols(TypedColumns):
            id: Column[Annotated[int, Column(Integer, primary_key=True)]]
            description: Column[str | None]

        with expect_raises_message(
            ArgumentError,
            "Could not find a SQL type for type typing.Annotated.+"
            " obtained from annotation .+ in attribute 'records_cols.id'",
        ):

            Table("records", metadata, records_cols)

    def test_unknown_type(self, metadata):
        """Test error when no type info is found."""

        class MyType:
            pass

        with expect_raises_message(
            ArgumentError,
            "Could not find a SQL type for type .*MyType.+ obtained from "
            "annotation .* in attribute 'bad_anno.id'",
        ):

            class bad_anno(TypedColumns):
                id: Column[MyType]

            Table("bad_anno", metadata, bad_anno)

    def test_invalid_annotation_type_provided_column(self, metadata):
        """Test error when annotation is not Column[...]."""

        with expect_raises_message(
            ArgumentError,
            "Annotation <class 'int'> for attribute 'bad_anno.id' is not "
            "of type Named/Column",
        ):

            class bad_anno(TypedColumns):
                id: int = Column(Integer)

            Table("bad_anno", metadata, bad_anno)

    def test_missing_generic_in_column_provided_col(self, metadata):
        with expect_raises_message(
            ArgumentError,
            "Python typing annotation is required for attribute "
            r"'bad_anno.id' when primary argument\(s\) for Column construct "
            "are None or not present",
        ):

            class bad_anno(TypedColumns):
                id: Column = Column(nullable=False)

            Table("bad_anno", metadata, bad_anno)

    def test_unknown_type_provided_col(self, metadata):
        """Test error when no type info is found."""

        class MyType:
            pass

        with expect_raises_message(
            ArgumentError,
            "Python typing annotation is required for attribute "
            r"'bad_anno.id' when primary argument\(s\) for Column construct "
            "are None or not present",
        ):

            class bad_anno(TypedColumns):
                id: Column[MyType] = Column(nullable=False)

            Table("bad_anno", metadata, bad_anno)

    def test_invalid_attribute_value(self, metadata):
        """Test error when attribute is neither Column nor annotation."""
        with expect_raises_message(ArgumentError, "Expected a Column"):

            class bad_attr(TypedColumns):
                id = 42  # Invalid: not a Column

            Table("bad_attr", metadata, bad_attr)

    def test_cannot_instantiate_typed_columns(self):
        """Test that TypedColumns cannot be directly instantiated."""

        class TestTC(TypedColumns):
            pass

        with expect_raises_message(InvalidRequestError, "Cannot instantiate"):
            TestTC()

    def test_mix_column_duplicate(self, metadata):

        with expect_raises_message(
            DuplicateColumnError,
            "A column with name 'y' is already present in table 'records'",
        ):

            class records(TypedColumns):
                id: Column[int]
                y: Column[str | None]

            Table(
                "records",
                metadata,
                records,
                Column("x", Integer),
                Column("y", Integer),
                Column("z", Integer),
            )

    def test_simple_fk(self, metadata):
        t = sa.Table("t1", metadata, Column("id", Integer))

        class t2_cols(TypedColumns):
            id: Column[int]
            t1_id = Column(sa.ForeignKey("t1.id"))

        t2 = Table("t2", metadata, t2_cols)

        is_instance_of(t2.c.t1_id.type, Integer)
        eq_(len(t2.c.t1_id.foreign_keys), 1)
        is_(list(t2.c.t1_id.foreign_keys)[0].column, t.c.id)

    def test_simple_fk_many_times(self, metadata):
        t = sa.Table("t1", metadata, Column("id", Integer))

        class cols(TypedColumns):
            id: Column[int]
            t1_id = Column(sa.ForeignKey("t1.id"))

        t2 = Table("t2", metadata, cols)
        t3 = Table("t3", metadata, cols)
        t4 = Table("t4", metadata, cols)

        cc = set()
        fk = set()
        for tx in (t2, t3, t4):
            is_not(tx.c.t1_id, cols.t1_id)
            eq_(tx.c.t1_id.foreign_keys & cols.t1_id.foreign_keys, set())
            eq_(len(tx.c.t1_id.foreign_keys), 1)
            is_(list(tx.c.t1_id.foreign_keys)[0].column, t.c.id)
            cc.add(tx.c.t1_id)
            fk.update(tx.c.t1_id.foreign_keys)
        eq_(len(cc), 3)
        eq_(len(fk), 3)

    def test_fk_mixin(self, metadata):
        t = sa.Table("t1", metadata, Column("id", Integer))

        class tid(TypedColumns):
            t1_id = Column(sa.ForeignKey("t1.id"))

        class a_cols(tid):
            id: Column[int]

        a = Table("a", metadata, a_cols)

        class b_cols(tid):
            b: Column[int]

        b = Table("b", metadata, b_cols)

        for tx in (a, b):
            eq_(len(tx.c.t1_id.foreign_keys), 1)
            is_(list(tx.c.t1_id.foreign_keys)[0].column, t.c.id)

    def test_fk_non_tbl_bound(self, metadata):

        with expect_raises_message(
            InvalidRequestError,
            "Column 'a.t1_id' with foreign "
            "key to non-table-bound columns is not supported "
            "when using a TypedColumns. If possible use the "
            "qualified string name the column",
        ):

            class a(TypedColumns):
                id = Column(Integer)
                t1_id = Column(sa.ForeignKey(id))
                a: Column[int]

            Table("a", metadata, a)

    def test_fk_mixin_non_tbl_bound(self, metadata):
        class tid(TypedColumns):
            id = Column(Integer)
            t1_id = Column(sa.ForeignKey(id))

        with expect_raises_message(
            InvalidRequestError,
            "Column 'tid.t1_id' with foreign "
            "key to non-table-bound columns is not supported "
            "when using a TypedColumns. If possible use the "
            "qualified string name the column",
        ):

            class a(tid):
                a: Column[int]

            Table("a", metadata, a)

    def test_with_cols(self, metadata):
        class cols(TypedColumns):
            id = Column(Integer)
            x = Column(String)

        t = Table("a", metadata, cols)
        is_(t, t.with_cols(cols))

        class cols2(TypedColumns):
            name = Column(Integer)

        is_(t, t.with_cols(cols2))  # no runtime check is performed

        sq = t.select().subquery()
        is_(sq, sq.with_cols(cols))
        cte = t.select().cte()
        is_(cte, cte.with_cols(cols))
