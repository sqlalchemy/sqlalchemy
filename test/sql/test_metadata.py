from contextlib import contextmanager
import pickle

import sqlalchemy as tsa
from sqlalchemy import ARRAY
from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import BLANK_SCHEMA
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import ColumnDefault
from sqlalchemy import Computed
from sqlalchemy import desc
from sqlalchemy import Enum
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import insert_sentinel
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import TypeDecorator
from sqlalchemy import types as sqltypes
from sqlalchemy import Unicode
from sqlalchemy import UniqueConstraint
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.compiler import deregister
from sqlalchemy.schema import AddConstraint
from sqlalchemy.schema import CreateIndex
from sqlalchemy.schema import DefaultClause
from sqlalchemy.schema import DropIndex
from sqlalchemy.sql import naming
from sqlalchemy.sql import operators
from sqlalchemy.sql.base import _NONE_NAME
from sqlalchemy.sql.elements import literal_column
from sqlalchemy.sql.schema import _InsertSentinelColumnDefault
from sqlalchemy.sql.schema import RETAIN_SCHEMA
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import emits_warning
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import Variation
from sqlalchemy.testing.assertions import expect_warnings


class MetaDataTest(fixtures.TestBase, ComparesTables):
    def test_metadata_contains(self):
        metadata = MetaData()
        t1 = Table("t1", metadata, Column("x", Integer))
        t2 = Table("t2", metadata, Column("x", Integer), schema="foo")
        t3 = Table("t2", MetaData(), Column("x", Integer))
        t4 = Table("t1", MetaData(), Column("x", Integer), schema="foo")

        assert "t1" in metadata
        assert "foo.t2" in metadata
        assert "t2" not in metadata
        assert "foo.t1" not in metadata
        assert t1 in metadata
        assert t2 in metadata
        assert t3 not in metadata
        assert t4 not in metadata

    def test_uninitialized_column_copy(self):
        for col in [
            Column("foo", String(), nullable=False),
            Column("baz", String(), unique=True),
            Column(Integer(), primary_key=True),
            Column(
                "bar",
                Integer(),
                Sequence("foo_seq"),
                primary_key=True,
                key="bar",
                autoincrement="ignore_fk",
            ),
            Column(Integer(), ForeignKey("bat.blah"), doc="this is a col"),
            Column(
                "bar",
                Integer(),
                ForeignKey("bat.blah"),
                primary_key=True,
                comment="this is a comment",
                key="bar",
            ),
            Column("bar", Integer(), info={"foo": "bar"}),
        ]:
            c2 = col._copy()
            for attr in (
                "name",
                "type",
                "nullable",
                "primary_key",
                "key",
                "unique",
                "info",
                "doc",
                "autoincrement",
            ):
                eq_(getattr(col, attr), getattr(c2, attr))
            eq_(len(col.foreign_keys), len(c2.foreign_keys))
            if col.default:
                eq_(c2.default.name, "foo_seq")
            for a1, a2 in zip(col.foreign_keys, c2.foreign_keys):
                assert a1 is not a2
                eq_(a2._colspec, "bat.blah")

    def test_col_subclass_copy(self):
        class MyColumn(schema.Column):
            def __init__(self, *args, **kw):
                self.widget = kw.pop("widget", None)
                super().__init__(*args, **kw)

            def _copy(self, *arg, **kw):
                c = super()._copy(*arg, **kw)
                c.widget = self.widget
                return c

        c1 = MyColumn("foo", Integer, widget="x")
        c2 = c1._copy()
        assert isinstance(c2, MyColumn)
        eq_(c2.widget, "x")

    def test_uninitialized_column_copy_events(self):
        msgs = []

        def write(c, t):
            msgs.append("attach %s.%s" % (t.name, c.name))

        c1 = Column("foo", String())
        m = MetaData()
        for i in range(3):
            cx = c1._copy()
            # as of 0.7, these events no longer copy.  its expected
            # that listeners will be re-established from the
            # natural construction of things.
            cx._on_table_attach(write)
            Table("foo%d" % i, m, cx)
        eq_(msgs, ["attach foo0.foo", "attach foo1.foo", "attach foo2.foo"])

    def test_schema_collection_add(self):
        metadata = MetaData()

        Table("t1", metadata, Column("x", Integer), schema="foo")
        Table("t2", metadata, Column("x", Integer), schema="bar")
        Table("t3", metadata, Column("x", Integer))

        eq_(metadata._schemas, {"foo", "bar"})
        eq_(len(metadata.tables), 3)

    def test_schema_collection_remove(self):
        metadata = MetaData()

        t1 = Table("t1", metadata, Column("x", Integer), schema="foo")
        Table("t2", metadata, Column("x", Integer), schema="bar")
        t3 = Table("t3", metadata, Column("x", Integer), schema="bar")

        metadata.remove(t3)
        eq_(metadata._schemas, {"foo", "bar"})
        eq_(len(metadata.tables), 2)

        metadata.remove(t1)
        eq_(metadata._schemas, {"bar"})
        eq_(len(metadata.tables), 1)

    def test_schema_collection_remove_all(self):
        metadata = MetaData()

        Table("t1", metadata, Column("x", Integer), schema="foo")
        Table("t2", metadata, Column("x", Integer), schema="bar")

        metadata.clear()
        eq_(metadata._schemas, set())
        eq_(len(metadata.tables), 0)

    def test_metadata_tables_immutable(self):
        # this use case was added due to #1917.
        metadata = MetaData()

        Table("t1", metadata, Column("x", Integer))
        assert "t1" in metadata.tables

        assert_raises(TypeError, lambda: metadata.tables.pop("t1"))

    @testing.provide_metadata
    def test_dupe_tables(self):
        metadata = self.metadata
        Table(
            "table1",
            metadata,
            Column("col1", Integer, primary_key=True),
            Column("col2", String(20)),
        )

        metadata.create_all(testing.db)
        Table("table1", metadata, autoload_with=testing.db)

        def go():
            Table(
                "table1",
                metadata,
                Column("col1", Integer, primary_key=True),
                Column("col2", String(20)),
            )

        assert_raises_message(
            tsa.exc.InvalidRequestError,
            "Table 'table1' is already defined for this "
            "MetaData instance.  Specify 'extend_existing=True' "
            "to redefine options and columns on an existing "
            "Table object.",
            go,
        )

    def test_fk_copy(self):
        c1 = Column("foo", Integer)
        c2 = Column("bar", Integer)
        m = MetaData()
        t1 = Table("t", m, c1, c2)

        kw = dict(
            onupdate="X",
            ondelete="Y",
            use_alter=True,
            name="f1",
            deferrable="Z",
            initially="Q",
            link_to_name=True,
            comment="foo",
        )

        fk1 = ForeignKey(c1, **kw)
        fk2 = ForeignKeyConstraint((c1,), (c2,), **kw)

        t1.append_constraint(fk2)
        fk1c = fk1._copy()
        fk2c = fk2._copy()

        for k in kw:
            eq_(getattr(fk1c, k), kw[k])
            eq_(getattr(fk2c, k), kw[k])

    def test_check_constraint_copy(self):
        def r(x):
            return x

        c = CheckConstraint(
            "foo bar",
            name="name",
            initially=True,
            deferrable=True,
            comment="foo",
            _create_rule=r,
        )
        c2 = c._copy()
        eq_(c2.name, "name")
        eq_(str(c2.sqltext), "foo bar")
        eq_(c2.initially, True)
        eq_(c2.deferrable, True)
        eq_(c2.comment, "foo")
        assert c2._create_rule is r

    def test_col_replace_w_constraint(self):
        m = MetaData()
        a = Table("a", m, Column("id", Integer, primary_key=True))

        aid = Column("a_id", ForeignKey("a.id"))
        b = Table("b", m, aid)
        b.append_column(aid)

        assert b.c.a_id.references(a.c.id)
        eq_(len(b.constraints), 2)

    def test_fk_construct(self):
        c1 = Column("foo", Integer)
        c2 = Column("bar", Integer)
        m = MetaData()
        t1 = Table("t", m, c1, c2)
        fk1 = ForeignKeyConstraint(("foo",), ("bar",), table=t1)
        assert fk1 in t1.constraints

    def test_fk_constraint_col_collection_w_table(self):
        c1 = Column("foo", Integer)
        c2 = Column("bar", Integer)
        m = MetaData()
        t1 = Table("t", m, c1, c2)
        fk1 = ForeignKeyConstraint(("foo",), ("bar",), table=t1)
        eq_(dict(fk1.columns), {"foo": c1})

    def test_fk_constraint_col_collection_no_table(self):
        fk1 = ForeignKeyConstraint(("foo", "bat"), ("bar", "hoho"))
        eq_(dict(fk1.columns), {})
        eq_(fk1.column_keys, ["foo", "bat"])
        eq_(fk1._col_description, "foo, bat")
        eq_(fk1._elements, {"foo": fk1.elements[0], "bat": fk1.elements[1]})

    def test_fk_constraint_col_collection_no_table_real_cols(self):
        c1 = Column("foo", Integer)
        c2 = Column("bar", Integer)
        fk1 = ForeignKeyConstraint((c1,), (c2,))
        eq_(dict(fk1.columns), {})
        eq_(fk1.column_keys, ["foo"])
        eq_(fk1._col_description, "foo")
        eq_(fk1._elements, {"foo": fk1.elements[0]})

    def test_fk_constraint_col_collection_added_to_table(self):
        c1 = Column("foo", Integer)
        m = MetaData()
        fk1 = ForeignKeyConstraint(("foo",), ("bar",))
        Table("t", m, c1, fk1)
        eq_(dict(fk1.columns), {"foo": c1})
        eq_(fk1._elements, {"foo": fk1.elements[0]})

    def test_fk_constraint_col_collection_via_fk(self):
        fk = ForeignKey("bar")
        c1 = Column("foo", Integer, fk)
        m = MetaData()
        t1 = Table("t", m, c1)
        fk1 = fk.constraint
        eq_(fk1.column_keys, ["foo"])
        assert fk1 in t1.constraints
        eq_(fk1.column_keys, ["foo"])
        eq_(dict(fk1.columns), {"foo": c1})
        eq_(fk1._elements, {"foo": fk})

    def test_fk_no_such_parent_col_error(self):
        meta = MetaData()
        a = Table("a", meta, Column("a", Integer))
        Table("b", meta, Column("b", Integer))

        def go():
            a.append_constraint(ForeignKeyConstraint(["x"], ["b.b"]))

        assert_raises_message(
            exc.ArgumentError,
            "Can't create ForeignKeyConstraint on "
            "table 'a': no column named 'x' is present.",
            go,
        )

    def test_fk_given_non_col(self):
        not_a_col = bindparam("x")
        assert_raises_message(
            exc.ArgumentError,
            "String column name or Column object for DDL foreign "
            "key constraint expected, got .*Bind",
            ForeignKey,
            not_a_col,
        )

    def test_fk_given_non_col_clauseelem(self):
        class Foo:
            def __clause_element__(self):
                return bindparam("x")

        assert_raises_message(
            exc.ArgumentError,
            "String column name or Column object for DDL foreign "
            "key constraint expected, got .*Foo",
            ForeignKey,
            Foo(),
        )

    def test_fk_given_col_non_table(self):
        t = Table("t", MetaData(), Column("x", Integer))
        xa = t.alias().c.x
        assert_raises_message(
            exc.ArgumentError,
            "ForeignKey received Column not bound to a Table, got: .*Alias",
            ForeignKey,
            xa,
        )

    def test_fk_given_col_non_table_clauseelem(self):
        t = Table("t", MetaData(), Column("x", Integer))

        class Foo:
            def __clause_element__(self):
                return t.alias().c.x

        assert_raises_message(
            exc.ArgumentError,
            "ForeignKey received Column not bound to a Table, got: .*Alias",
            ForeignKey,
            Foo(),
        )

    def test_fk_no_such_target_col_error_upfront(self):
        meta = MetaData()
        a = Table("a", meta, Column("a", Integer))
        Table("b", meta, Column("b", Integer))

        a.append_constraint(ForeignKeyConstraint(["a"], ["b.x"]))

        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for ForeignKey 'b.x' on "
            "table 'a': table 'b' has no column named 'x'",
            getattr,
            list(a.foreign_keys)[0],
            "column",
        )

    def test_fk_no_such_target_col_error_delayed(self):
        meta = MetaData()
        a = Table("a", meta, Column("a", Integer))
        a.append_constraint(ForeignKeyConstraint(["a"], ["b.x"]))

        Table("b", meta, Column("b", Integer))

        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for ForeignKey 'b.x' on "
            "table 'a': table 'b' has no column named 'x'",
            getattr,
            list(a.foreign_keys)[0],
            "column",
        )

    def test_fk_mismatched_local_remote_cols(self):
        assert_raises_message(
            exc.ArgumentError,
            "ForeignKeyConstraint number of constrained columns must "
            "match the number of referenced columns.",
            ForeignKeyConstraint,
            ["a"],
            ["b.a", "b.b"],
        )

        assert_raises_message(
            exc.ArgumentError,
            "ForeignKeyConstraint number of constrained columns "
            "must match the number of referenced columns.",
            ForeignKeyConstraint,
            ["a", "b"],
            ["b.a"],
        )

        assert_raises_message(
            exc.ArgumentError,
            "ForeignKeyConstraint with duplicate source column "
            "references are not supported.",
            ForeignKeyConstraint,
            ["a", "a"],
            ["b.a", "b.b"],
        )

    def test_pickle_metadata_sequence_restated(self):
        m1 = MetaData()
        Table(
            "a",
            m1,
            Column("id", Integer, primary_key=True),
            Column("x", Integer, Sequence("x_seq")),
        )

        m2 = pickle.loads(pickle.dumps(m1))

        s2 = Sequence("x_seq")
        t2 = Table(
            "a",
            m2,
            Column("id", Integer, primary_key=True),
            Column("x", Integer, s2),
            extend_existing=True,
        )

        assert m2._sequences["x_seq"] is t2.c.x.default
        assert m2._sequences["x_seq"] is s2

    def test_sequence_restated_replaced(self):
        """Test restatement of Sequence replaces."""

        m1 = MetaData()
        s1 = Sequence("x_seq")
        t = Table("a", m1, Column("x", Integer, s1))
        assert m1._sequences["x_seq"] is s1

        s2 = Sequence("x_seq")
        Table("a", m1, Column("x", Integer, s2), extend_existing=True)
        assert t.c.x.default is s2
        assert m1._sequences["x_seq"] is s2

    def test_sequence_attach_to_table(self):
        m1 = MetaData()
        s1 = Sequence("s")
        Table("a", m1, Column("x", Integer, s1))
        assert s1.metadata is m1

    def test_sequence_attach_to_existing_table(self):
        m1 = MetaData()
        s1 = Sequence("s")
        t = Table("a", m1, Column("x", Integer))
        t.c.x._init_items(s1)
        assert s1.metadata is m1

    def test_pickle_metadata_sequence_implicit(self):
        m1 = MetaData()
        Table(
            "a",
            m1,
            Column("id", Integer, primary_key=True),
            Column("x", Integer, Sequence("x_seq")),
        )

        m2 = pickle.loads(pickle.dumps(m1))

        t2 = Table("a", m2, extend_existing=True)

        eq_(m2._sequences, {"x_seq": t2.c.x.default})

    def test_pickle_metadata_schema(self):
        m1 = MetaData()
        Table(
            "a",
            m1,
            Column("id", Integer, primary_key=True),
            Column("x", Integer, Sequence("x_seq")),
            schema="y",
        )

        m2 = pickle.loads(pickle.dumps(m1))

        Table("a", m2, schema="y", extend_existing=True)

        eq_(m2._schemas, m1._schemas)

    def test_metadata_schema_arg(self):
        m1 = MetaData(schema="sch1")
        m2 = MetaData(schema="sch1", quote_schema=True)
        m3 = MetaData(schema="sch1", quote_schema=False)
        m4 = MetaData()

        for (
            i,
            (
                name,
                metadata,
                schema_,
                quote_schema,
                exp_schema,
                exp_quote_schema,
            ),
        ) in enumerate(
            [
                ("t1", m1, None, None, "sch1", None),
                ("t2", m1, "sch2", None, "sch2", None),
                ("t3", m1, "sch2", True, "sch2", True),
                ("t4", m1, "sch1", None, "sch1", None),
                ("t5", m1, BLANK_SCHEMA, None, None, None),
                ("t1", m2, None, None, "sch1", True),
                ("t2", m2, "sch2", None, "sch2", None),
                ("t3", m2, "sch2", True, "sch2", True),
                ("t4", m2, "sch1", None, "sch1", None),
                ("t1", m3, None, None, "sch1", False),
                ("t2", m3, "sch2", None, "sch2", None),
                ("t3", m3, "sch2", True, "sch2", True),
                ("t4", m3, "sch1", None, "sch1", None),
                ("t1", m4, None, None, None, None),
                ("t2", m4, "sch2", None, "sch2", None),
                ("t3", m4, "sch2", True, "sch2", True),
                ("t4", m4, "sch1", None, "sch1", None),
                ("t5", m4, BLANK_SCHEMA, None, None, None),
            ]
        ):
            kw = {}
            if schema_ is not None:
                kw["schema"] = schema_
            if quote_schema is not None:
                kw["quote_schema"] = quote_schema
            t = Table(name, metadata, **kw)
            eq_(t.schema, exp_schema, "test %d, table schema" % i)
            eq_(
                t.schema.quote if t.schema is not None else None,
                exp_quote_schema,
                "test %d, table quote_schema" % i,
            )
            seq = Sequence(name, metadata=metadata, **kw)
            eq_(seq.schema, exp_schema, "test %d, seq schema" % i)
            eq_(
                seq.schema.quote if seq.schema is not None else None,
                exp_quote_schema,
                "test %d, seq quote_schema" % i,
            )

    def test_manual_dependencies(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer))
        b = Table("b", meta, Column("foo", Integer))
        c = Table("c", meta, Column("foo", Integer))
        d = Table("d", meta, Column("foo", Integer))
        e = Table("e", meta, Column("foo", Integer))

        e.add_is_dependent_on(c)
        a.add_is_dependent_on(b)
        b.add_is_dependent_on(d)
        e.add_is_dependent_on(b)
        c.add_is_dependent_on(a)
        eq_(meta.sorted_tables, [d, b, a, c, e])

    def test_sort_by_parameter_order(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer))
        b = Table("b", meta, Column("foo", Integer))
        c = Table("c", meta, Column("foo", Integer))
        d = Table("d", meta, Column("foo", Integer))
        e = Table("e", meta, Column("foo", Integer))

        e.add_is_dependent_on(c)
        a.add_is_dependent_on(b)
        eq_(meta.sorted_tables, [b, c, d, a, e])

    def test_fks_sort_by_parameter_order(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer, ForeignKey("b.foo")))
        b = Table("b", meta, Column("foo", Integer))
        c = Table("c", meta, Column("foo", Integer))
        d = Table("d", meta, Column("foo", Integer))
        e = Table("e", meta, Column("foo", Integer, ForeignKey("c.foo")))

        eq_(meta.sorted_tables, [b, c, d, a, e])

    def test_cycles_fks_warning_one(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer, ForeignKey("b.foo")))
        b = Table("b", meta, Column("foo", Integer, ForeignKey("d.foo")))
        c = Table("c", meta, Column("foo", Integer, ForeignKey("b.foo")))
        d = Table("d", meta, Column("foo", Integer, ForeignKey("c.foo")))
        e = Table("e", meta, Column("foo", Integer))

        with testing.expect_warnings(
            "Cannot correctly sort tables; there are unresolvable cycles "
            'between tables "b, c, d", which is usually caused by mutually '
            "dependent foreign key constraints.  "
            "Foreign key constraints involving these tables will not be "
            "considered"
        ):
            eq_(meta.sorted_tables, [b, c, d, e, a])

    def test_cycles_fks_warning_two(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer, ForeignKey("b.foo")))
        b = Table("b", meta, Column("foo", Integer, ForeignKey("a.foo")))
        c = Table("c", meta, Column("foo", Integer, ForeignKey("e.foo")))
        d = Table("d", meta, Column("foo", Integer))
        e = Table("e", meta, Column("foo", Integer, ForeignKey("d.foo")))

        with testing.expect_warnings(
            "Cannot correctly sort tables; there are unresolvable cycles "
            'between tables "a, b", which is usually caused by mutually '
            "dependent foreign key constraints.  "
            "Foreign key constraints involving these tables will not be "
            "considered"
        ):
            eq_(meta.sorted_tables, [a, b, d, e, c])

    def test_cycles_fks_fks_delivered_separately(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer, ForeignKey("b.foo")))
        b = Table("b", meta, Column("foo", Integer, ForeignKey("a.foo")))
        c = Table("c", meta, Column("foo", Integer, ForeignKey("e.foo")))
        d = Table("d", meta, Column("foo", Integer))
        e = Table("e", meta, Column("foo", Integer, ForeignKey("d.foo")))

        results = schema.sort_tables_and_constraints(
            sorted(meta.tables.values(), key=lambda t: t.key)
        )
        results[-1] = (None, set(results[-1][-1]))
        eq_(
            results,
            [
                (a, set()),
                (b, set()),
                (d, {fk.constraint for fk in d.foreign_keys}),
                (e, {fk.constraint for fk in e.foreign_keys}),
                (c, {fk.constraint for fk in c.foreign_keys}),
                (
                    None,
                    {fk.constraint for fk in a.foreign_keys}.union(
                        fk.constraint for fk in b.foreign_keys
                    ),
                ),
            ],
        )

    def test_cycles_fks_usealter(self):
        meta = MetaData()
        a = Table("a", meta, Column("foo", Integer, ForeignKey("b.foo")))
        b = Table(
            "b",
            meta,
            Column("foo", Integer, ForeignKey("d.foo", use_alter=True)),
        )
        c = Table("c", meta, Column("foo", Integer, ForeignKey("b.foo")))
        d = Table("d", meta, Column("foo", Integer, ForeignKey("c.foo")))
        e = Table("e", meta, Column("foo", Integer))

        eq_(meta.sorted_tables, [b, e, a, c, d])

    def test_nonexistent(self):
        assert_raises(
            tsa.exc.NoSuchTableError,
            Table,
            "fake_table",
            MetaData(),
            autoload_with=testing.db,
        )

    def test_assorted_repr(self):
        t1 = Table("foo", MetaData(), Column("x", Integer))
        i1 = Index("bar", t1.c.x)
        ck = schema.CheckConstraint("x > y", name="someconstraint")

        for const, exp in (
            (Sequence("my_seq"), "Sequence('my_seq')"),
            (Sequence("my_seq", start=5), "Sequence('my_seq', start=5)"),
            (Column("foo", Integer), "Column('foo', Integer(), table=None)"),
            (
                Column(
                    "foo",
                    Integer,
                    primary_key=True,
                    nullable=False,
                    onupdate=1,
                    default=42,
                    server_default="42",
                    comment="foo",
                ),
                "Column('foo', Integer(), table=None, primary_key=True, "
                f"nullable=False, onupdate={ColumnDefault(1)}, default="
                f"{ColumnDefault(42)}, server_default={DefaultClause('42')}, "
                "comment='foo')",
            ),
            (
                Column(
                    "foo",
                    Integer,
                    primary_key=True,
                    nullable=False,
                    onupdate=1,
                    insert_default=42,
                    server_default="42",
                    comment="foo",
                ),
                "Column('foo', Integer(), table=None, primary_key=True, "
                f"nullable=False, onupdate={ColumnDefault(1)}, default="
                f"{ColumnDefault(42)}, server_default={DefaultClause('42')}, "
                "comment='foo')",
            ),
            (
                Table("bar", MetaData(), Column("x", String)),
                "Table('bar', MetaData(), "
                "Column('x', String(), table=<bar>), schema=None)",
            ),
            (
                schema.DefaultGenerator(for_update=True),
                "DefaultGenerator(for_update=True)",
            ),
            (schema.Index("bar", "c"), "Index('bar', 'c')"),
            (i1, "Index('bar', Column('x', Integer(), table=<foo>))"),
            (schema.FetchedValue(), "FetchedValue()"),
            (
                ck,
                "CheckConstraint("
                "%s"
                ", name='someconstraint')" % repr(ck.sqltext),
            ),
            (
                ColumnDefault(("foo", "bar")),
                "ScalarElementColumnDefault(('foo', 'bar'))",
            ),
        ):
            eq_(repr(const), exp)

    @testing.variation("kind", ["engine", "conn", "something"])
    def test_metadata_bind(self, connection, kind):
        with expect_raises_message(
            exc.ArgumentError,
            "expected schema argument to be a string, got",
        ):
            if kind.engine:
                MetaData(connection.engine)
            elif kind.conn:
                MetaData(connection)
            else:
                MetaData(42)  # type: ignore


class ToMetaDataTest(fixtures.TestBase, AssertsCompiledSQL, ComparesTables):
    @testing.requires.check_constraints
    def test_copy(self):
        # TODO: modernize this test for 2.0

        from sqlalchemy.testing.schema import Table

        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, Sequence("foo_id_seq"), primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "foo",
                String(40),
                nullable=False,
                server_default="x",
                server_onupdate="q",
            ),
            Column(
                "bar", String(40), nullable=False, default="y", onupdate="z"
            ),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
            test_needs_fk=True,
        )

        table2 = Table(
            "othertable",
            meta,
            Column("id", Integer, Sequence("foo_seq"), primary_key=True),
            Column("myid", Integer, ForeignKey("mytable.myid")),
            test_needs_fk=True,
        )

        table3 = Table(
            "has_comments",
            meta,
            Column("foo", Integer, comment="some column"),
            comment="table comment",
        )

        def test_to_metadata():
            meta2 = MetaData()
            table_c = table.to_metadata(meta2)
            table2_c = table2.to_metadata(meta2)
            table3_c = table3.to_metadata(meta2)
            return (table_c, table2_c, table3_c)

        def test_pickle():
            meta.bind = testing.db
            meta2 = pickle.loads(pickle.dumps(meta))
            pickle.loads(pickle.dumps(meta2))
            return (
                meta2.tables["mytable"],
                meta2.tables["othertable"],
                meta2.tables["has_comments"],
            )

        def test_pickle_via_reflect():
            # this is the most common use case, pickling the results of a
            # database reflection
            meta2 = MetaData()
            t1 = Table("mytable", meta2, autoload_with=testing.db)
            Table("othertable", meta2, autoload_with=testing.db)
            Table("has_comments", meta2, autoload_with=testing.db)
            meta3 = pickle.loads(pickle.dumps(meta2))
            assert meta3.tables["mytable"] is not t1

            return (
                meta3.tables["mytable"],
                meta3.tables["othertable"],
                meta3.tables["has_comments"],
            )

        meta.create_all(testing.db)
        try:
            for test, has_constraints, reflect in (
                (test_to_metadata, True, False),
                (test_pickle, True, False),
                (test_pickle_via_reflect, False, True),
            ):
                table_c, table2_c, table3_c = test()
                self.assert_tables_equal(table, table_c)
                self.assert_tables_equal(table2, table2_c)
                assert table is not table_c
                assert table.primary_key is not table_c.primary_key
                assert (
                    list(table2_c.c.myid.foreign_keys)[0].column
                    is table_c.c.myid
                )
                assert (
                    list(table2_c.c.myid.foreign_keys)[0].column
                    is not table.c.myid
                )
                assert "x" in str(table_c.c.foo.server_default.arg)
                if not reflect:
                    assert isinstance(table_c.c.myid.default, Sequence)
                    assert str(table_c.c.foo.server_onupdate.arg) == "q"
                    assert str(table_c.c.bar.default.arg) == "y"
                    assert (
                        getattr(
                            table_c.c.bar.onupdate.arg,
                            "arg",
                            table_c.c.bar.onupdate.arg,
                        )
                        == "z"
                    )
                    assert isinstance(table2_c.c.id.default, Sequence)

                # constraints don't get reflected for any dialect right
                # now

                if has_constraints:
                    for c in table_c.c.description.constraints:
                        if isinstance(c, CheckConstraint):
                            break
                    else:
                        assert False
                    assert str(c.sqltext) == "description='hi'"
                    for c in table_c.constraints:
                        if isinstance(c, UniqueConstraint):
                            break
                    else:
                        assert False
                    assert c.columns.contains_column(table_c.c.name)
                    assert not c.columns.contains_column(table.c.name)

                if testing.requires.comment_reflection.enabled:
                    eq_(table3_c.comment, "table comment")
                    eq_(table3_c.c.foo.comment, "some column")

        finally:
            meta.drop_all(testing.db)

    def test_col_key_fk_parent(self):
        # test #2643
        m1 = MetaData()
        a = Table("a", m1, Column("x", Integer))
        b = Table("b", m1, Column("x", Integer, ForeignKey("a.x"), key="y"))
        assert b.c.y.references(a.c.x)

        m2 = MetaData()
        b2 = b.to_metadata(m2)
        a2 = a.to_metadata(m2)
        assert b2.c.y.references(a2.c.x)

    def test_fk_w_no_colname(self):
        """test a ForeignKey that refers to table name only.  the column
        name is assumed to be the same col name on parent table.

        this is a little used feature from long ago that nonetheless is
        still in the code.

        The feature was found to be not working but is repaired for
        SQLAlchemy 2.0.

        """
        m1 = MetaData()
        a = Table("a", m1, Column("x", Integer))
        b = Table("b", m1, Column("x", Integer, ForeignKey("a")))
        assert b.c.x.references(a.c.x)

        m2 = MetaData()
        b2 = b.to_metadata(m2)
        a2 = a.to_metadata(m2)
        assert b2.c.x.references(a2.c.x)

    def test_fk_w_no_colname_name_missing(self):
        """test a ForeignKey that refers to table name only.  the column
        name is assumed to be the same col name on parent table.

        this is a little used feature from long ago that nonetheless is
        still in the code.

        """
        m1 = MetaData()
        a = Table("a", m1, Column("x", Integer))
        b = Table("b", m1, Column("y", Integer, ForeignKey("a")))

        with expect_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for ForeignKey 'a' on "
            "table 'b': table 'a' has no column named 'y'",
        ):
            assert b.c.y.references(a.c.x)

    def test_column_collection_constraint_w_ad_hoc_columns(self):
        """Test ColumnCollectionConstraint that has columns that aren't
        part of the Table.

        """
        meta = MetaData()

        uq1 = UniqueConstraint(literal_column("some_name"))
        cc1 = CheckConstraint(literal_column("some_name") > 5)
        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            uq1,
            cc1,
        )

        self.assert_compile(
            schema.AddConstraint(uq1),
            "ALTER TABLE mytable ADD UNIQUE (some_name)",
            dialect="default",
        )
        self.assert_compile(
            schema.AddConstraint(cc1),
            "ALTER TABLE mytable ADD CHECK (some_name > 5)",
            dialect="default",
        )
        meta2 = MetaData()
        table2 = table.to_metadata(meta2)
        uq2 = [
            c for c in table2.constraints if isinstance(c, UniqueConstraint)
        ][0]
        cc2 = [
            c for c in table2.constraints if isinstance(c, CheckConstraint)
        ][0]
        self.assert_compile(
            schema.AddConstraint(uq2),
            "ALTER TABLE mytable ADD UNIQUE (some_name)",
            dialect="default",
        )
        self.assert_compile(
            schema.AddConstraint(cc2),
            "ALTER TABLE mytable ADD CHECK (some_name > 5)",
            dialect="default",
        )

    def test_change_schema(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
        )

        table2 = Table(
            "othertable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("myid", Integer, ForeignKey("mytable.myid")),
        )

        meta2 = MetaData()
        table_c = table.to_metadata(meta2, schema="someschema")
        table2_c = table2.to_metadata(meta2, schema="someschema")

        eq_(
            str(table_c.join(table2_c).onclause),
            str(table_c.c.myid == table2_c.c.myid),
        )
        eq_(
            str(table_c.join(table2_c).onclause),
            "someschema.mytable.myid = someschema.othertable.myid",
        )

    def test_retain_table_schema(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
            schema="myschema",
        )

        table2 = Table(
            "othertable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("myid", Integer, ForeignKey("myschema.mytable.myid")),
            schema="myschema",
        )

        meta2 = MetaData()
        table_c = table.to_metadata(meta2)
        table2_c = table2.to_metadata(meta2)

        eq_(
            str(table_c.join(table2_c).onclause),
            str(table_c.c.myid == table2_c.c.myid),
        )
        eq_(
            str(table_c.join(table2_c).onclause),
            "myschema.mytable.myid = myschema.othertable.myid",
        )

    def test_change_name_retain_metadata(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
            schema="myschema",
        )

        table2 = table.to_metadata(table.metadata, name="newtable")
        table3 = table.to_metadata(
            table.metadata, schema="newschema", name="newtable"
        )

        assert table.metadata is table2.metadata
        assert table.metadata is table3.metadata
        eq_(
            (table.name, table2.name, table3.name),
            ("mytable", "newtable", "newtable"),
        )
        eq_(
            (table.key, table2.key, table3.key),
            ("myschema.mytable", "myschema.newtable", "newschema.newtable"),
        )

    def test_change_name_change_metadata(self):
        meta = MetaData()
        meta2 = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
            schema="myschema",
        )

        table2 = table.to_metadata(meta2, name="newtable")

        assert table.metadata is not table2.metadata
        eq_((table.name, table2.name), ("mytable", "newtable"))
        eq_((table.key, table2.key), ("myschema.mytable", "myschema.newtable"))

    def test_change_name_selfref_fk_moves(self):
        meta = MetaData()

        referenced = Table(
            "ref", meta, Column("id", Integer, primary_key=True)
        )
        table = Table(
            "mytable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("parent_id", ForeignKey("mytable.id")),
            Column("ref_id", ForeignKey("ref.id")),
        )

        table2 = table.to_metadata(table.metadata, name="newtable")
        assert table.metadata is table2.metadata
        assert table2.c.ref_id.references(referenced.c.id)
        assert table2.c.parent_id.references(table2.c.id)

    def test_change_name_selfref_fk_moves_w_schema(self):
        meta = MetaData()

        referenced = Table(
            "ref", meta, Column("id", Integer, primary_key=True)
        )
        table = Table(
            "mytable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("parent_id", ForeignKey("mytable.id")),
            Column("ref_id", ForeignKey("ref.id")),
        )

        table2 = table.to_metadata(
            table.metadata, name="newtable", schema="newschema"
        )
        ref2 = referenced.to_metadata(table.metadata, schema="newschema")
        assert table.metadata is table2.metadata
        assert table2.c.ref_id.references(ref2.c.id)
        assert table2.c.parent_id.references(table2.c.id)

    def _assert_fk(self, t2, schema, expected, referred_schema_fn=None):
        m2 = MetaData()
        existing_schema = t2.schema
        if schema:
            t2c = t2.to_metadata(
                m2, schema=schema, referred_schema_fn=referred_schema_fn
            )
            eq_(t2c.schema, schema)
        else:
            t2c = t2.to_metadata(m2, referred_schema_fn=referred_schema_fn)
            eq_(t2c.schema, existing_schema)
        eq_(list(t2c.c.y.foreign_keys)[0]._get_colspec(), expected)

    def test_fk_has_schema_string_retain_schema(self):
        m = MetaData()

        t2 = Table("t2", m, Column("y", Integer, ForeignKey("q.t1.x")))
        self._assert_fk(t2, None, "q.t1.x")

        Table("t1", m, Column("x", Integer), schema="q")
        self._assert_fk(t2, None, "q.t1.x")

    def test_fk_has_schema_string_new_schema(self):
        m = MetaData()

        t2 = Table("t2", m, Column("y", Integer, ForeignKey("q.t1.x")))
        self._assert_fk(t2, "z", "q.t1.x")

        Table("t1", m, Column("x", Integer), schema="q")
        self._assert_fk(t2, "z", "q.t1.x")

    def test_fk_has_schema_col_retain_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="q")
        t2 = Table("t2", m, Column("y", Integer, ForeignKey(t1.c.x)))

        self._assert_fk(t2, "z", "q.t1.x")

    def test_fk_has_schema_col_new_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="q")
        t2 = Table("t2", m, Column("y", Integer, ForeignKey(t1.c.x)))

        self._assert_fk(t2, "z", "q.t1.x")

    def test_fk_and_referent_has_same_schema_string_retain_schema(self):
        m = MetaData()

        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey("q.t1.x")), schema="q"
        )

        self._assert_fk(t2, None, "q.t1.x")

        Table("t1", m, Column("x", Integer), schema="q")
        self._assert_fk(t2, None, "q.t1.x")

    def test_fk_and_referent_has_same_schema_string_new_schema(self):
        m = MetaData()

        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey("q.t1.x")), schema="q"
        )

        self._assert_fk(t2, "z", "z.t1.x")

        Table("t1", m, Column("x", Integer), schema="q")
        self._assert_fk(t2, "z", "z.t1.x")

    def test_fk_and_referent_has_same_schema_col_retain_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="q")
        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey(t1.c.x)), schema="q"
        )
        self._assert_fk(t2, None, "q.t1.x")

    def test_fk_and_referent_has_same_schema_col_new_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="q")
        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey(t1.c.x)), schema="q"
        )
        self._assert_fk(t2, "z", "z.t1.x")

    def test_fk_and_referent_has_diff_schema_string_retain_schema(self):
        m = MetaData()

        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey("p.t1.x")), schema="q"
        )

        self._assert_fk(t2, None, "p.t1.x")

        Table("t1", m, Column("x", Integer), schema="p")
        self._assert_fk(t2, None, "p.t1.x")

    def test_fk_and_referent_has_diff_schema_string_new_schema(self):
        m = MetaData()

        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey("p.t1.x")), schema="q"
        )

        self._assert_fk(t2, "z", "p.t1.x")

        Table("t1", m, Column("x", Integer), schema="p")
        self._assert_fk(t2, "z", "p.t1.x")

    def test_fk_and_referent_has_diff_schema_col_retain_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="p")
        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey(t1.c.x)), schema="q"
        )
        self._assert_fk(t2, None, "p.t1.x")

    def test_fk_and_referent_has_diff_schema_col_new_schema(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), schema="p")
        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey(t1.c.x)), schema="q"
        )
        self._assert_fk(t2, "z", "p.t1.x")

    def test_fk_custom_system(self):
        m = MetaData()
        t2 = Table(
            "t2", m, Column("y", Integer, ForeignKey("p.t1.x")), schema="q"
        )

        def ref_fn(table, to_schema, constraint, referred_schema):
            assert table is t2
            eq_(to_schema, "z")
            eq_(referred_schema, "p")
            return "h"

        self._assert_fk(t2, "z", "h.t1.x", referred_schema_fn=ref_fn)

    def test_fk_reset_to_none(self):
        m = MetaData()

        t2 = Table("t2", m, Column("y", Integer, ForeignKey("p.t1.x")))

        def ref_fn(table, to_schema, constraint, referred_schema):
            return BLANK_SCHEMA

        self._assert_fk(t2, None, "t1.x", referred_schema_fn=ref_fn)

    @testing.combinations(None, RETAIN_SCHEMA)
    def test_fk_test_non_return_for_referred_schema(self, sym):
        m = MetaData()

        t2 = Table("t2", m, Column("y", Integer, ForeignKey("p.t1.x")))

        def ref_fn(table, to_schema, constraint, referred_schema):
            return sym

        self._assert_fk(t2, None, "p.t1.x", referred_schema_fn=ref_fn)

    def test_fk_get_referent_is_always_a_column(self):
        """test the annotation on ForeignKey.get_referent() in that it does
        in fact return Column even if given a labeled expr in a subquery"""

        m = MetaData()
        a = Table("a", m, Column("id", Integer, primary_key=True))
        b = Table("b", m, Column("aid", Integer, ForeignKey("a.id")))

        stmt = select(a.c.id.label("somelabel")).subquery()

        referent = list(b.c.aid.foreign_keys)[0].get_referent(stmt)
        is_(referent, stmt.c.somelabel)
        assert isinstance(referent, Column)

    def test_copy_info(self):
        m = MetaData()
        fk = ForeignKey("t2.id")
        c = Column("c", Integer, fk)
        ck = CheckConstraint("c > 5")
        t = Table("t", m, c, ck)

        m.info["minfo"] = True
        fk.info["fkinfo"] = True
        c.info["cinfo"] = True
        ck.info["ckinfo"] = True
        t.info["tinfo"] = True
        t.primary_key.info["pkinfo"] = True
        fkc = [
            const
            for const in t.constraints
            if isinstance(const, ForeignKeyConstraint)
        ][0]
        fkc.info["fkcinfo"] = True

        m2 = MetaData()
        t2 = t.to_metadata(m2)

        m.info["minfo"] = False
        fk.info["fkinfo"] = False
        c.info["cinfo"] = False
        ck.info["ckinfo"] = False
        t.primary_key.info["pkinfo"] = False
        fkc.info["fkcinfo"] = False

        eq_(m2.info, {})
        eq_(t2.info, {"tinfo": True})
        eq_(t2.c.c.info, {"cinfo": True})
        eq_(list(t2.c.c.foreign_keys)[0].info, {"fkinfo": True})
        eq_(t2.primary_key.info, {"pkinfo": True})

        fkc2 = [
            const
            for const in t2.constraints
            if isinstance(const, ForeignKeyConstraint)
        ][0]
        eq_(fkc2.info, {"fkcinfo": True})

        ck2 = [
            const
            for const in t2.constraints
            if isinstance(const, CheckConstraint)
        ][0]
        eq_(ck2.info, {"ckinfo": True})

    def test_dialect_kwargs(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            mysql_engine="InnoDB",
        )

        meta2 = MetaData()
        table_c = table.to_metadata(meta2)

        eq_(table.kwargs, {"mysql_engine": "InnoDB"})

        eq_(table.kwargs, table_c.kwargs)

    def test_indexes(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("data1", Integer, index=True),
            Column("data2", Integer),
            Index("text", text("data1 + 1")),
        )
        Index("multi", table.c.data1, table.c.data2)
        Index("func", func.abs(table.c.data1))
        Index("multi-func", table.c.data1, func.abs(table.c.data2))

        meta2 = MetaData()
        table_c = table.to_metadata(meta2)

        def _get_key(i):
            return (
                [i.name, i.unique]
                + sorted(i.kwargs.items())
                + [str(col) for col in i.expressions]
            )

        eq_(
            sorted([_get_key(i) for i in table.indexes]),
            sorted([_get_key(i) for i in table_c.indexes]),
        )

    def test_indexes_with_col_redefine(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("data1", Integer),
            Column("data2", Integer),
            Index("text", text("data1 + 1")),
        )
        Index("multi", table.c.data1, table.c.data2)
        Index("func", func.abs(table.c.data1))
        Index("multi-func", table.c.data1, func.abs(table.c.data2))

        table = Table(
            "mytable",
            meta,
            Column("data1", Integer),
            Column("data2", Integer),
            extend_existing=True,
        )

        meta2 = MetaData()
        table_c = table.to_metadata(meta2)

        def _get_key(i):
            return (
                [i.name, i.unique]
                + sorted(i.kwargs.items())
                + [str(col) for col in i.expressions]
            )

        eq_(
            sorted([_get_key(i) for i in table.indexes]),
            sorted([_get_key(i) for i in table_c.indexes]),
        )

    @emits_warning("Table '.+' already exists within the given MetaData")
    def test_already_exists(self):
        meta1 = MetaData()
        table1 = Table(
            "mytable", meta1, Column("myid", Integer, primary_key=True)
        )
        meta2 = MetaData()
        table2 = Table(
            "mytable", meta2, Column("yourid", Integer, primary_key=True)
        )

        table_c = table1.to_metadata(meta2)
        table_d = table2.to_metadata(meta2)

        # d'oh!
        assert table_c is table_d

    def test_default_schema_metadata(self):
        meta = MetaData(schema="myschema")

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
        )

        table2 = Table(
            "othertable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("myid", Integer, ForeignKey("myschema.mytable.myid")),
        )

        meta2 = MetaData(schema="someschema")
        table_c = table.to_metadata(meta2, schema=None)
        table2_c = table2.to_metadata(meta2, schema=None)

        eq_(
            str(table_c.join(table2_c).onclause),
            str(table_c.c.myid == table2_c.c.myid),
        )
        eq_(
            str(table_c.join(table2_c).onclause),
            "someschema.mytable.myid = someschema.othertable.myid",
        )

    def test_strip_schema(self):
        meta = MetaData()

        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, primary_key=True),
            Column("name", String(40), nullable=True),
            Column(
                "description", String(30), CheckConstraint("description='hi'")
            ),
            UniqueConstraint("name"),
        )

        table2 = Table(
            "othertable",
            meta,
            Column("id", Integer, primary_key=True),
            Column("myid", Integer, ForeignKey("mytable.myid")),
        )

        meta2 = MetaData()
        table_c = table.to_metadata(meta2, schema=None)
        table2_c = table2.to_metadata(meta2, schema=None)

        eq_(
            str(table_c.join(table2_c).onclause),
            str(table_c.c.myid == table2_c.c.myid),
        )
        eq_(
            str(table_c.join(table2_c).onclause),
            "mytable.myid = othertable.myid",
        )

    def test_unique_true_flag(self):
        meta = MetaData()

        table = Table("mytable", meta, Column("x", Integer, unique=True))

        m2 = MetaData()

        t2 = table.to_metadata(m2)

        eq_(
            len(
                [
                    const
                    for const in t2.constraints
                    if isinstance(const, UniqueConstraint)
                ]
            ),
            1,
        )

    def test_index_true_flag(self):
        meta = MetaData()

        table = Table("mytable", meta, Column("x", Integer, index=True))

        m2 = MetaData()

        t2 = table.to_metadata(m2)

        eq_(len(t2.indexes), 1)


class InfoTest(fixtures.TestBase):
    def test_metadata_info(self):
        m1 = MetaData()
        eq_(m1.info, {})

        m1 = MetaData(info={"foo": "bar"})
        eq_(m1.info, {"foo": "bar"})

    def test_foreignkey_constraint_info(self):
        fkc = ForeignKeyConstraint(["a"], ["b"], name="bar")
        eq_(fkc.info, {})

        fkc = ForeignKeyConstraint(
            ["a"], ["b"], name="bar", info={"foo": "bar"}
        )
        eq_(fkc.info, {"foo": "bar"})

    def test_foreignkey_info(self):
        fkc = ForeignKey("a")
        eq_(fkc.info, {})

        fkc = ForeignKey("a", info={"foo": "bar"})
        eq_(fkc.info, {"foo": "bar"})

    def test_primarykey_constraint_info(self):
        pkc = PrimaryKeyConstraint("a", name="x")
        eq_(pkc.info, {})

        pkc = PrimaryKeyConstraint("a", name="x", info={"foo": "bar"})
        eq_(pkc.info, {"foo": "bar"})

    def test_unique_constraint_info(self):
        uc = UniqueConstraint("a", name="x")
        eq_(uc.info, {})

        uc = UniqueConstraint("a", name="x", info={"foo": "bar"})
        eq_(uc.info, {"foo": "bar"})

    def test_check_constraint_info(self):
        cc = CheckConstraint("foo=bar", name="x")
        eq_(cc.info, {})

        cc = CheckConstraint("foo=bar", name="x", info={"foo": "bar"})
        eq_(cc.info, {"foo": "bar"})

    def test_index_info(self):
        ix = Index("x", "a")
        eq_(ix.info, {})

        ix = Index("x", "a", info={"foo": "bar"})
        eq_(ix.info, {"foo": "bar"})

    def test_column_info(self):
        c = Column("x", Integer)
        eq_(c.info, {})

        c = Column("x", Integer, info={"foo": "bar"})
        eq_(c.info, {"foo": "bar"})

    def test_table_info(self):
        t = Table("x", MetaData())
        eq_(t.info, {})

        t = Table("x", MetaData(), info={"foo": "bar"})
        eq_(t.info, {"foo": "bar"})


class TableTest(fixtures.TestBase, AssertsCompiledSQL):
    @testing.requires.temporary_tables
    @testing.skip_if("mssql", "different col format")
    def test_prefixes(self):
        from sqlalchemy import Table

        table1 = Table(
            "temporary_table_1",
            MetaData(),
            Column("col1", Integer),
            prefixes=["TEMPORARY"],
        )

        self.assert_compile(
            schema.CreateTable(table1),
            "CREATE TEMPORARY TABLE temporary_table_1 (col1 INTEGER)",
        )

        table2 = Table(
            "temporary_table_2",
            MetaData(),
            Column("col1", Integer),
            prefixes=["VIRTUAL"],
        )
        self.assert_compile(
            schema.CreateTable(table2),
            "CREATE VIRTUAL TABLE temporary_table_2 (col1 INTEGER)",
        )

    @testing.combinations((None, []), ((), []), ([], []), (["foo"], ["foo"]))
    def test_prefixes_parameter_parsing(self, arg, expected):
        """test #6685"""
        table = Table("foo", MetaData(), Column("bar", Integer), prefixes=arg)
        eq_(table._prefixes, expected)

    def test_table_info(self):
        metadata = MetaData()
        t1 = Table("foo", metadata, info={"x": "y"})
        t2 = Table("bar", metadata, info={})
        t3 = Table("bat", metadata)
        assert t1.info == {"x": "y"}
        assert t2.info == {}
        assert t3.info == {}
        for t in (t1, t2, t3):
            t.info["bar"] = "zip"
            assert t.info["bar"] == "zip"

    def test_invalid_objects(self):
        assert_raises_message(
            tsa.exc.ArgumentError,
            "'SchemaItem' object, such as a 'Column' or a "
            "'Constraint' expected, got <.*ColumnClause at .*; q>",
            Table,
            "asdf",
            MetaData(),
            tsa.column("q", Integer),
        )

        assert_raises_message(
            tsa.exc.ArgumentError,
            r"'SchemaItem' object, such as a 'Column' or a "
            r"'Constraint' expected, got String\(\)",
            Table,
            "asdf",
            MetaData(),
            String(),
        )

        assert_raises_message(
            tsa.exc.ArgumentError,
            "'SchemaItem' object, such as a 'Column' or a "
            "'Constraint' expected, got 12",
            Table,
            "asdf",
            MetaData(),
            12,
        )

        assert_raises_message(
            tsa.exc.ArgumentError,
            "'SchemaItem' object, such as a 'Column' or a "
            "'Constraint' expected, got "
            r"\(Column\('q', Integer\(\), table=None\), "
            r"Column\('p', Integer\(\), table=None\)\)",
            Table,
            "asdf",
            MetaData(),
            (Column("q", Integer), Column("p", Integer)),
        )

    def test_reset_exported_passes(self):
        m = MetaData()

        t = Table("t", m, Column("foo", Integer))
        eq_(list(t.c), [t.c.foo])

        t._reset_exported()

        eq_(list(t.c), [t.c.foo])

    def test_foreign_key_constraints_collection(self):
        metadata = MetaData()
        t1 = Table("foo", metadata, Column("a", Integer))
        eq_(t1.foreign_key_constraints, set())

        fk1 = ForeignKey("q.id")
        fk2 = ForeignKey("j.id")
        fk3 = ForeignKeyConstraint(["b", "c"], ["r.x", "r.y"])

        t1.append_column(Column("b", Integer, fk1))
        eq_(t1.foreign_key_constraints, {fk1.constraint})

        t1.append_column(Column("c", Integer, fk2))
        eq_(t1.foreign_key_constraints, {fk1.constraint, fk2.constraint})

        t1.append_constraint(fk3)
        eq_(
            t1.foreign_key_constraints,
            {fk1.constraint, fk2.constraint, fk3},
        )

    def test_c_immutable(self):
        m = MetaData()
        t1 = Table("t", m, Column("x", Integer), Column("y", Integer))
        assert_raises(TypeError, t1.c.extend, [Column("z", Integer)])

        def assign():
            t1.c["z"] = Column("z", Integer)

        assert_raises(TypeError, assign)

        def assign2():
            t1.c.z = Column("z", Integer)

        assert_raises(TypeError, assign2)

    def test_c_mutate_after_unpickle(self):
        m = MetaData()

        y = Column("y", Integer)
        t1 = Table("t", m, Column("x", Integer), y)

        # note we are testing immutable column collection here
        t2 = pickle.loads(pickle.dumps(t1))
        z = Column("z", Integer)
        g = Column("g", Integer)
        t2.append_column(z)

        is_(t1.c.contains_column(y), True)
        is_(t2.c.contains_column(y), False)
        y2 = t2.c.y
        is_(t2.c.contains_column(y2), True)

        is_(t2.c.contains_column(z), True)
        is_(t2.c.contains_column(g), False)

    def test_table_ctor_duplicated_column_name(self):
        # when it will raise
        with testing.expect_raises_message(
            exc.ArgumentError,
            "A column with name 'col' is already present in table 't'",
        ):
            Table(
                "t",
                MetaData(),
                Column("a", Integer),
                Column("col", Integer),
                Column("col", String),
            )

    def test_append_column_existing_name(self):
        t = Table("t", MetaData(), Column("col", Integer))

        with testing.expect_raises_message(
            exc.DuplicateColumnError,
            r"A column with name 'col' is already present in table 't'. "
            r"Specify replace_existing=True to Table.append_column\(\) to "
            r"replace an existing column.",
        ):
            t.append_column(Column("col", String))

    def test_append_column_existing_key(self):
        t = Table("t", MetaData(), Column("col", Integer, key="c2"))

        with testing.expect_raises_message(
            exc.DuplicateColumnError,
            r"A column with key 'c2' is already present in table 't'. "
            r"Specify replace_existing=True to Table.append_column\(\) "
            r"to replace an existing column.",
        ):
            t.append_column(Column("col", String, key="c2"))

    @testing.variation("field", ["name", "key"])
    def test_append_column_replace_existing(self, field: Variation):
        if field.name:
            t = Table("t", MetaData(), Column("col", Integer))
            t.append_column(Column("col", String), replace_existing=True)
            is_true(isinstance(t.c.col.type, String))
        elif field.key:
            t = Table("t", MetaData(), Column("col", Integer, key="c2"))
            t.append_column(
                Column("col", String, key="c2"), replace_existing=True
            )
            is_true(isinstance(t.c.c2.type, String))
        else:
            field.fail()

    def test_autoincrement_replace(self):
        m = MetaData()

        t = Table("t", m, Column("id", Integer, primary_key=True))

        is_(t._autoincrement_column, t.c.id)

        t = Table(
            "t",
            m,
            Column("id", Integer, primary_key=True),
            extend_existing=True,
        )
        is_(t._autoincrement_column, t.c.id)

    def test_pk_args_standalone(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, primary_key=True),
            PrimaryKeyConstraint(mssql_clustered=True),
        )
        eq_(list(t.primary_key), [t.c.x])
        eq_(t.primary_key.dialect_kwargs, {"mssql_clustered": True})

    def test_pk_cols_sets_flags(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
            PrimaryKeyConstraint("x", "y"),
        )
        eq_(t.c.x.primary_key, True)
        eq_(t.c.y.primary_key, True)
        eq_(t.c.z.primary_key, False)

    def test_pk_col_mismatch_one(self):
        m = MetaData()

        with expect_warnings(
            "Table 't' specifies columns 'x' as primary_key=True, "
            "not matching locally specified columns 'q'"
        ):
            Table(
                "t",
                m,
                Column("x", Integer, primary_key=True),
                Column("q", Integer),
                PrimaryKeyConstraint("q"),
            )

    def test_pk_col_mismatch_two(self):
        m = MetaData()

        with expect_warnings(
            "Table 't' specifies columns 'a', 'b', 'c' as primary_key=True, "
            "not matching locally specified columns 'b', 'c'"
        ):
            Table(
                "t",
                m,
                Column("a", Integer, primary_key=True),
                Column("b", Integer, primary_key=True),
                Column("c", Integer, primary_key=True),
                PrimaryKeyConstraint("b", "c"),
            )

    @testing.emits_warning("Table 't'")
    def test_pk_col_mismatch_three(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, primary_key=True),
            Column("q", Integer),
            PrimaryKeyConstraint("q"),
        )
        eq_(list(t.primary_key), [t.c.q])

    @testing.emits_warning("Table 't'")
    def test_pk_col_mismatch_four(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("a", Integer, primary_key=True),
            Column("b", Integer, primary_key=True),
            Column("c", Integer, primary_key=True),
            PrimaryKeyConstraint("b", "c"),
        )
        eq_(list(t.primary_key), [t.c.b, t.c.c])

    def test_pk_always_flips_nullable(self):
        m = MetaData()

        t1 = Table("t1", m, Column("x", Integer), PrimaryKeyConstraint("x"))

        t2 = Table("t2", m, Column("x", Integer, primary_key=True))

        eq_(list(t1.primary_key), [t1.c.x])

        eq_(list(t2.primary_key), [t2.c.x])

        assert t1.c.x.primary_key
        assert t2.c.x.primary_key

        assert not t2.c.x.nullable
        assert not t1.c.x.nullable

    def test_pk_can_be_nullable(self):
        m = MetaData()

        t1 = Table(
            "t1",
            m,
            Column("x", Integer, nullable=True),
            PrimaryKeyConstraint("x"),
        )

        t2 = Table(
            "t2", m, Column("x", Integer, primary_key=True, nullable=True)
        )

        eq_(list(t1.primary_key), [t1.c.x])

        eq_(list(t2.primary_key), [t2.c.x])

        assert t1.c.x.primary_key
        assert t2.c.x.primary_key

        assert t2.c.x.nullable
        assert t1.c.x.nullable

    def test_must_exist(self):
        with testing.expect_raises_message(
            exc.InvalidRequestError, "Table 'foo' not defined"
        ):
            Table("foo", MetaData(), must_exist=True)

    @testing.combinations(
        ("comment", ("A", "B", "A")),
        ("implicit_returning", (True, False, True)),
        ("info", ({"A": 1}, {"A": 2}, {"A": 1})),
    )
    def test_extend_attributes(self, attrib, attrib_values):
        """
        ensure `extend_existing` is compatible with simple attributes
        """
        metadata = MetaData()
        for counter, _attrib_value in enumerate(attrib_values):
            _extend_existing = True if (counter > 0) else False
            _kwargs = {
                "extend_existing": _extend_existing,
                attrib: _attrib_value,
            }
            table_a = Table(
                "a",
                metadata,
                Column("foo", String, primary_key=True),
                **_kwargs,
            )
            eq_(getattr(table_a, attrib), _attrib_value)
            eq_(getattr(metadata.tables["a"], attrib), _attrib_value)


class PKAutoIncrementTest(fixtures.TestBase):
    def test_multi_integer_no_autoinc(self):
        pk = PrimaryKeyConstraint(Column("a", Integer), Column("b", Integer))
        t = Table("t", MetaData())
        t.append_constraint(pk)

        is_(pk._autoincrement_column, None)

    def test_multi_integer_multi_autoinc(self):
        pk = PrimaryKeyConstraint(
            Column("a", Integer, autoincrement=True),
            Column("b", Integer, autoincrement=True),
        )
        t = Table("t", MetaData())
        t.append_constraint(pk)

        assert_raises_message(
            exc.ArgumentError,
            "Only one Column may be marked",
            lambda: pk._autoincrement_column,
        )

    def test_single_integer_no_autoinc(self):
        pk = PrimaryKeyConstraint(Column("a", Integer))
        t = Table("t", MetaData())
        t.append_constraint(pk)

        is_(pk._autoincrement_column, pk.columns["a"])

    def test_single_string_no_autoinc(self):
        pk = PrimaryKeyConstraint(Column("a", String))
        t = Table("t", MetaData())
        t.append_constraint(pk)

        is_(pk._autoincrement_column, None)

    def test_single_string_illegal_autoinc(self):
        t = Table("t", MetaData(), Column("a", String, autoincrement=True))
        pk = PrimaryKeyConstraint(t.c.a)
        t.append_constraint(pk)

        assert_raises_message(
            exc.ArgumentError,
            "Column type VARCHAR on column 't.a'",
            lambda: pk._autoincrement_column,
        )

    def test_single_integer_default(self):
        t = Table(
            "t",
            MetaData(),
            Column("a", Integer, autoincrement=True, default=lambda: 1),
        )
        pk = PrimaryKeyConstraint(t.c.a)
        t.append_constraint(pk)

        is_(pk._autoincrement_column, t.c.a)

    def test_single_integer_server_default(self):
        # new as of 1.1; now that we have three states for autoincrement,
        # if the user puts autoincrement=True with a server_default, trust
        # them on it
        t = Table(
            "t",
            MetaData(),
            Column(
                "a", Integer, autoincrement=True, server_default=func.magic()
            ),
        )
        pk = PrimaryKeyConstraint(t.c.a)
        t.append_constraint(pk)

        is_(pk._autoincrement_column, t.c.a)

    def test_implicit_autoinc_but_fks(self):
        m = MetaData()
        Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table("t2", MetaData(), Column("a", Integer, ForeignKey("t1.id")))
        pk = PrimaryKeyConstraint(t2.c.a)
        t2.append_constraint(pk)
        is_(pk._autoincrement_column, None)

    def test_explicit_autoinc_but_fks(self):
        m = MetaData()
        Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            MetaData(),
            Column("a", Integer, ForeignKey("t1.id"), autoincrement=True),
        )
        pk = PrimaryKeyConstraint(t2.c.a)
        t2.append_constraint(pk)
        is_(pk._autoincrement_column, t2.c.a)

        t3 = Table(
            "t3",
            MetaData(),
            Column(
                "a", Integer, ForeignKey("t1.id"), autoincrement="ignore_fk"
            ),
        )
        pk = PrimaryKeyConstraint(t3.c.a)
        t3.append_constraint(pk)
        is_(pk._autoincrement_column, t3.c.a)

    def test_no_kw_args(self):
        with expect_raises_message(
            TypeError,
            r"Table\(\) takes at least two positional-only arguments",
            check_context=False,
        ):
            Table(name="foo", metadata=MetaData())
        with expect_raises_message(
            TypeError,
            r"Table\(\) takes at least two positional-only arguments",
            check_context=False,
        ):
            Table("foo", metadata=MetaData())


class SchemaTypeTest(fixtures.TestBase):
    __backend__ = True

    class TrackEvents:
        column = None
        table = None
        evt_targets = ()

        def _set_table(self, column, table):
            super()._set_table(column, table)
            self.column = column
            self.table = table

        def _on_table_create(self, target, bind, **kw):
            super()._on_table_create(target, bind, **kw)
            self.evt_targets += (target,)

        def _on_metadata_create(self, target, bind, **kw):
            super()._on_metadata_create(target, bind, **kw)
            self.evt_targets += (target,)

    # TODO: Enum and Boolean put TypeEngine first.  Changing that here
    # causes collection-mutate-while-iterated errors in the event system
    # since the hooks here call upon the adapted type.  Need to figure out
    # why Enum and Boolean don't have this problem.
    # NOTE: it's likely the need for the SchemaType.adapt() method,
    # which Enum / Boolean don't use (and crash if it comes first)
    class MyType(TrackEvents, sqltypes.SchemaType, sqltypes.TypeEngine):
        pass

    class WrapEnum(TrackEvents, Enum):
        pass

    class WrapBoolean(TrackEvents, Boolean):
        pass

    class MyTypeWImpl(MyType):
        def _gen_dialect_impl(self, dialect):
            return self.adapt(SchemaTypeTest.MyTypeImpl)

    class MyTypeImpl(MyTypeWImpl):
        pass

    class MyTypeDecAndSchema(TypeDecorator, sqltypes.SchemaType):
        impl = String()
        cache_ok = True

        evt_targets = ()

        def __init__(self):
            TypeDecorator.__init__(self)
            sqltypes.SchemaType.__init__(self)

        def _on_table_create(self, target, bind, **kw):
            self.evt_targets += (target,)

        def _on_metadata_create(self, target, bind, **kw):
            self.evt_targets += (target,)

    def test_before_parent_attach_plain(self):
        typ = self.MyType()
        self._test_before_parent_attach(typ)

    def test_before_parent_attach_typedec_enclosing_schematype(self):
        # additional test for [ticket:2919] as part of test for
        # [ticket:3832]
        # this also serves as the test for [ticket:6152]

        class MySchemaType(sqltypes.SchemaType):
            pass

        target_typ = MySchemaType()

        class MyType(TypeDecorator):
            impl = target_typ
            cache_ok = True

        typ = MyType()
        self._test_before_parent_attach(typ, target_typ)

    def test_before_parent_attach_array_enclosing_schematype(self):
        # test for [ticket:4141] which is the same idea as [ticket:3832]
        # for ARRAY

        typ = ARRAY(String)

        self._test_before_parent_attach(typ)

    def test_before_parent_attach_typedec_of_schematype(self):
        class MyType(TypeDecorator, sqltypes.SchemaType):
            impl = String
            cache_ok = True

        typ = MyType()
        self._test_before_parent_attach(typ)

    def test_before_parent_attach_schematype_of_typedec(self):
        class MyType(sqltypes.SchemaType, TypeDecorator):
            impl = String
            cache_ok = True

        typ = MyType()
        self._test_before_parent_attach(typ)

    def test_before_parent_attach_variant_array_schematype(self):
        target = Enum("one", "two", "three")
        typ = ARRAY(target).with_variant(String(), "other")
        self._test_before_parent_attach(typ, evt_target=target)

    def _test_before_parent_attach(self, typ, evt_target=None):
        canary = mock.Mock()

        if evt_target is None:
            evt_target = typ

        orig_set_parent = evt_target._set_parent
        orig_set_parent_w_dispatch = evt_target._set_parent_with_dispatch

        def _set_parent(parent, **kw):
            orig_set_parent(parent, **kw)
            canary._set_parent(parent)

        def _set_parent_w_dispatch(parent):
            orig_set_parent_w_dispatch(parent)
            canary._set_parent_with_dispatch(parent)

        with mock.patch.object(evt_target, "_set_parent", _set_parent):
            with mock.patch.object(
                evt_target, "_set_parent_with_dispatch", _set_parent_w_dispatch
            ):
                event.listen(evt_target, "before_parent_attach", canary.go)

                c = Column("q", typ)

        eq_(
            canary.mock_calls,
            [
                mock.call.go(evt_target, c),
                mock.call._set_parent(c),
                mock.call._set_parent_with_dispatch(c),
            ],
        )

    def test_independent_schema(self):
        m = MetaData()
        type_ = self.MyType(schema="q")
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "q")

    def test_inherit_schema_from_metadata(self):
        """test #6373"""
        m = MetaData(schema="q")
        type_ = self.MyType(metadata=m)
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "q")

    def test_inherit_schema_from_table_override_metadata(self):
        """test #6373"""
        m = MetaData(schema="q")
        type_ = self.MyType(metadata=m, inherit_schema=True)
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "z")

    def test_inherit_schema_from_metadata_override_explicit(self):
        """test #6373"""
        m = MetaData(schema="q")
        type_ = self.MyType(schema="e", metadata=m)
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "e")

    def test_inherit_schema(self):
        m = MetaData()
        type_ = self.MyType(schema="q", inherit_schema=True)
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "z")

    def test_independent_schema_enum(self):
        m = MetaData()
        type_ = sqltypes.Enum("a", schema="q")
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "q")

    def test_inherit_schema_enum(self):
        m = MetaData()
        type_ = sqltypes.Enum("a", "b", "c", schema="q", inherit_schema=True)
        t1 = Table("x", m, Column("y", type_), schema="z")
        eq_(t1.c.y.type.schema, "z")

    @testing.variation("assign_metadata", [True, False])
    def test_to_metadata_copy_type(self, assign_metadata):
        m1 = MetaData()

        if assign_metadata:
            type_ = self.MyType(metadata=m1)
        else:
            type_ = self.MyType()

        t1 = Table("x", m1, Column("y", type_))

        m2 = MetaData()
        t2 = t1.to_metadata(m2)

        if assign_metadata:
            # metadata was transferred
            # issue #11802
            is_(t2.c.y.type.metadata, m2)
        else:
            # metadata isn't set
            is_(t2.c.y.type.metadata, None)

        # our test type sets table, though
        is_(t2.c.y.type.table, t2)

    def test_to_metadata_copy_decorated(self):
        class MyDecorated(TypeDecorator):
            impl = self.MyType
            cache_ok = True

        m1 = MetaData()

        type_ = MyDecorated(schema="z")
        t1 = Table("x", m1, Column("y", type_))

        m2 = MetaData()
        t2 = t1.to_metadata(m2)
        eq_(t2.c.y.type.schema, "z")

    def test_to_metadata_independent_schema(self):
        m1 = MetaData()

        type_ = self.MyType()
        t1 = Table("x", m1, Column("y", type_))

        m2 = MetaData()
        t2 = t1.to_metadata(m2, schema="bar")

        eq_(t2.c.y.type.schema, None)

    @testing.combinations(
        ("name", "foobar", "name"),
        ("schema", "someschema", "schema"),
        ("inherit_schema", True, "inherit_schema"),
        ("metadata", MetaData(), "metadata"),
    )
    def test_copy_args(self, argname, value, attrname):
        kw = {argname: value}
        e1 = self.MyType(**kw)

        e1_copy = e1.copy()

        eq_(getattr(e1_copy, attrname), value)

    @testing.variation("already_has_a_schema", [True, False])
    def test_to_metadata_inherit_schema(self, already_has_a_schema):
        m1 = MetaData()

        if already_has_a_schema:
            type_ = self.MyType(schema="foo", inherit_schema=True)
            eq_(type_.schema, "foo")
        else:
            type_ = self.MyType(inherit_schema=True)

        t1 = Table("x", m1, Column("y", type_))
        # note that inherit_schema means the schema mutates to be that
        # of the table
        is_(type_.schema, None)

        m2 = MetaData()
        t2 = t1.to_metadata(m2, schema="bar")

        eq_(t1.c.y.type.schema, None)
        eq_(t2.c.y.type.schema, "bar")

    def test_to_metadata_independent_events(self):
        m1 = MetaData()

        type_ = self.MyType()
        t1 = Table("x", m1, Column("y", type_))

        m2 = MetaData()
        t2 = t1.to_metadata(m2)

        t1.dispatch.before_create(t1, testing.db)
        eq_(t1.c.y.type.evt_targets, (t1,))
        eq_(t2.c.y.type.evt_targets, ())

        t2.dispatch.before_create(t2, testing.db)
        t2.dispatch.before_create(t2, testing.db)
        eq_(t1.c.y.type.evt_targets, (t1,))
        eq_(t2.c.y.type.evt_targets, (t2, t2))

    def test_enum_column_copy_transfers_events(self):
        m = MetaData()

        type_ = self.WrapEnum("a", "b", "c", name="foo")
        y = Column("y", type_)
        y_copy = y._copy()
        t1 = Table("x", m, y_copy)

        is_true(y_copy.type._create_events)

        # for PostgreSQL, this will emit CREATE TYPE
        m.dispatch.before_create(t1, testing.db)
        try:
            eq_(t1.c.y.type.evt_targets, (t1,))
        finally:
            # do the drop so that PostgreSQL emits DROP TYPE
            m.dispatch.after_drop(t1, testing.db)

    def test_enum_nonnative_column_copy_transfers_events(self):
        m = MetaData()

        type_ = self.WrapEnum("a", "b", "c", name="foo", native_enum=False)
        y = Column("y", type_)
        y_copy = y._copy()
        t1 = Table("x", m, y_copy)

        is_true(y_copy.type._create_events)

        m.dispatch.before_create(t1, testing.db)
        eq_(t1.c.y.type.evt_targets, (t1,))

    def test_enum_nonnative_column_copy_transfers_constraintpref(self):
        m = MetaData()

        type_ = self.WrapEnum(
            "a",
            "b",
            "c",
            name="foo",
            native_enum=False,
            create_constraint=False,
        )
        y = Column("y", type_)
        y_copy = y._copy()
        Table("x", m, y_copy)

        is_false(y_copy.type.create_constraint)

    def test_boolean_column_copy_transfers_events(self):
        m = MetaData()

        type_ = self.WrapBoolean()
        y = Column("y", type_)
        y_copy = y._copy()
        Table("x", m, y_copy)

        is_true(y_copy.type._create_events)

    def test_boolean_nonnative_column_copy_transfers_constraintpref(self):
        m = MetaData()

        type_ = self.WrapBoolean(create_constraint=False)
        y = Column("y", type_)
        y_copy = y._copy()
        Table("x", m, y_copy)

        is_false(y_copy.type.create_constraint)

    def test_metadata_dispatch_no_new_impl(self):
        m1 = MetaData()
        typ = self.MyType(metadata=m1)
        m1.dispatch.before_create(m1, testing.db)
        eq_(typ.evt_targets, (m1,))

        dialect_impl = typ.dialect_impl(testing.db.dialect)
        eq_(dialect_impl.evt_targets, ())

    def test_metadata_dispatch_new_impl(self):
        m1 = MetaData()
        typ = self.MyTypeWImpl(metadata=m1)
        m1.dispatch.before_create(m1, testing.db)
        eq_(typ.evt_targets, (m1,))

        dialect_impl = typ.dialect_impl(testing.db.dialect)
        eq_(dialect_impl.evt_targets, (m1,))

    def test_table_dispatch_decorator_schematype(self):
        m1 = MetaData()
        typ = self.MyTypeDecAndSchema()
        t1 = Table("t1", m1, Column("x", typ))
        m1.dispatch.before_create(t1, testing.db)
        eq_(typ.evt_targets, (t1,))

    def test_table_dispatch_no_new_impl(self):
        m1 = MetaData()
        typ = self.MyType()
        t1 = Table("t1", m1, Column("x", typ))
        m1.dispatch.before_create(t1, testing.db)
        eq_(typ.evt_targets, (t1,))

        dialect_impl = typ.dialect_impl(testing.db.dialect)
        eq_(dialect_impl.evt_targets, ())

    def test_table_dispatch_new_impl(self):
        m1 = MetaData()
        typ = self.MyTypeWImpl()
        t1 = Table("t1", m1, Column("x", typ))
        m1.dispatch.before_create(t1, testing.db)
        eq_(typ.evt_targets, (t1,))

        dialect_impl = typ.dialect_impl(testing.db.dialect)
        eq_(dialect_impl.evt_targets, (t1,))

    def test_create_metadata_bound_no_crash(self):
        m1 = MetaData()
        self.MyType(metadata=m1)

        m1.create_all(testing.db)

    def test_boolean_constraint_type_doesnt_double(self):
        m1 = MetaData()

        t1 = Table("x", m1, Column("flag", Boolean(create_constraint=True)))
        eq_(
            len([c for c in t1.constraints if isinstance(c, CheckConstraint)]),
            1,
        )
        m2 = MetaData()
        t2 = t1.to_metadata(m2)

        eq_(
            len([c for c in t2.constraints if isinstance(c, CheckConstraint)]),
            1,
        )

    def test_enum_constraint_type_doesnt_double(self):
        m1 = MetaData()

        t1 = Table(
            "x",
            m1,
            Column("flag", Enum("a", "b", "c", create_constraint=True)),
        )
        eq_(
            len([c for c in t1.constraints if isinstance(c, CheckConstraint)]),
            1,
        )
        m2 = MetaData()
        t2 = t1.to_metadata(m2)

        eq_(
            len([c for c in t2.constraints if isinstance(c, CheckConstraint)]),
            1,
        )


class SchemaTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_default_schema_metadata_fk(self):
        m = MetaData(schema="foo")
        t1 = Table("t1", m, Column("x", Integer))
        t2 = Table("t2", m, Column("x", Integer, ForeignKey("t1.x")))
        assert t2.c.x.references(t1.c.x)

    def test_ad_hoc_schema_equiv_fk(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer), schema="foo")
        t2 = Table(
            "t2", m, Column("x", Integer, ForeignKey("t1.x")), schema="foo"
        )
        assert_raises(
            exc.NoReferencedTableError, lambda: t2.c.x.references(t1.c.x)
        )

    def test_default_schema_metadata_fk_alt_remote(self):
        m = MetaData(schema="foo")
        t1 = Table("t1", m, Column("x", Integer))
        t2 = Table(
            "t2", m, Column("x", Integer, ForeignKey("t1.x")), schema="bar"
        )
        assert t2.c.x.references(t1.c.x)

    def test_default_schema_metadata_fk_alt_local_raises(self):
        m = MetaData(schema="foo")
        t1 = Table("t1", m, Column("x", Integer), schema="bar")
        t2 = Table("t2", m, Column("x", Integer, ForeignKey("t1.x")))
        assert_raises(
            exc.NoReferencedTableError, lambda: t2.c.x.references(t1.c.x)
        )

    def test_default_schema_metadata_fk_alt_local(self):
        m = MetaData(schema="foo")
        t1 = Table("t1", m, Column("x", Integer), schema="bar")
        t2 = Table("t2", m, Column("x", Integer, ForeignKey("bar.t1.x")))
        assert t2.c.x.references(t1.c.x)

    @testing.combinations(
        (schema.CreateSchema("sa_schema"), "CREATE SCHEMA sa_schema"),
        (schema.DropSchema("sa_schema"), "DROP SCHEMA sa_schema"),
        # note we don't yet support lower-case table() or
        # lower-case column() for this
        # (
        #    schema.CreateTable(table("t", column("q", Integer))),
        #    "CREATE TABLE t (q INTEGER)",
        # ),
        (
            schema.CreateTable(Table("t", MetaData(), Column("q", Integer))),
            "CREATE TABLE t (q INTEGER)",
        ),
        (
            schema.DropTable(Table("t", MetaData(), Column("q", Integer))),
            "DROP TABLE t",
        ),
        (
            schema.CreateIndex(
                Index(
                    "foo",
                    "x",
                    _table=Table("t", MetaData(), Column("x", Integer)),
                )
            ),
            "CREATE INDEX foo ON t (x)",
        ),
        (
            schema.DropIndex(
                Index(
                    "foo",
                    "x",
                    _table=Table("t", MetaData(), Column("x", Integer)),
                )
            ),
            "DROP INDEX foo",
        ),
        (schema.CreateSequence(Sequence("my_seq")), "CREATE SEQUENCE my_seq"),
        (schema.DropSequence(Sequence("my_seq")), "DROP SEQUENCE my_seq"),
    )
    def test_stringify_schema_elements(self, element, expected):
        eq_ignore_whitespace(str(element), expected)

    def test_create_drop_schema(self):
        self.assert_compile(
            schema.CreateSchema("sa_schema"), "CREATE SCHEMA sa_schema"
        )
        self.assert_compile(
            schema.CreateSchema("sa_schema", if_not_exists=True),
            "CREATE SCHEMA IF NOT EXISTS sa_schema",
        )
        self.assert_compile(
            schema.DropSchema("sa_schema"), "DROP SCHEMA sa_schema"
        )
        self.assert_compile(
            schema.DropSchema("sa_schema", if_exists=True),
            "DROP SCHEMA IF EXISTS sa_schema",
        )
        self.assert_compile(
            schema.DropSchema("sa_schema", cascade=True),
            "DROP SCHEMA sa_schema CASCADE",
        )

    def test_iteration(self):
        metadata = MetaData()
        table1 = Table(
            "table1",
            metadata,
            Column("col1", Integer, primary_key=True),
            schema="someschema",
        )
        table2 = Table(
            "table2",
            metadata,
            Column("col1", Integer, primary_key=True),
            Column("col2", Integer, ForeignKey("someschema.table1.col1")),
            schema="someschema",
        )

        t1 = str(schema.CreateTable(table1).compile(bind=testing.db))
        t2 = str(schema.CreateTable(table2).compile(bind=testing.db))
        if testing.db.dialect.preparer(testing.db.dialect).omit_schema:
            assert t1.index("CREATE TABLE table1") > -1
            assert t2.index("CREATE TABLE table2") > -1
        else:
            assert t1.index("CREATE TABLE someschema.table1") > -1
            assert t2.index("CREATE TABLE someschema.table2") > -1


class UseExistingTest(testing.AssertsCompiledSQL, fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )

    @testing.fixture
    def existing_meta(self):
        meta2 = MetaData()
        Table("users", meta2, autoload_with=testing.db)
        return meta2

    @testing.fixture
    def empty_meta(self):
        return MetaData()

    @testing.variation(
        "scenario",
        [
            "inplace",
            "inplace_ee",
            "separate_ee_key_first",
            "separate_ee_key_second",
            "separate_ee_key_append_no_replace",
            "separate_ee_key_append_replace",
        ],
    )
    @testing.variation("both_have_keys", [True, False])
    def test_table_w_two_same_named_columns(
        self, empty_meta, scenario: Variation, both_have_keys: Variation
    ):
        if scenario.inplace:
            with expect_raises_message(
                exc.DuplicateColumnError,
                "A column with name 'b' is already present in table 'users'.",
            ):
                t1 = Table(
                    "users",
                    empty_meta,
                    Column("a", String),
                    Column("b", String, key="b1" if both_have_keys else None),
                    Column("b", String, key="b2"),
                )
            return
        elif scenario.inplace_ee:
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b1" if both_have_keys else None),
                Column("b", String, key="b2"),
                extend_existing=True,
            )
        elif scenario.separate_ee_key_first:
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b2"),
            )

            expected_warnings = (
                [
                    'Column with user-specified key "b2" is being '
                    'replaced with plain named column "b", key "b2" '
                    "is being removed."
                ]
                if not both_have_keys
                else []
            )
            with expect_warnings(*expected_warnings):
                t1 = Table(
                    "users",
                    empty_meta,
                    Column("a", String),
                    Column("b", String, key="b1" if both_have_keys else None),
                    extend_existing=True,
                )
        elif scenario.separate_ee_key_second:
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b1" if both_have_keys else None),
            )
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b2"),
                extend_existing=True,
            )
        elif scenario.separate_ee_key_append_no_replace:
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b1" if both_have_keys else None),
            )
            with expect_raises_message(
                exc.DuplicateColumnError,
                r"A column with name 'b' is already present in table 'users'. "
                r"Specify replace_existing=True to Table.append_column\(\) "
                r"to replace an existing column.",
            ):
                t1.append_column(Column("b", String, key="b2"))
            return
        elif scenario.separate_ee_key_append_replace:
            t1 = Table(
                "users",
                empty_meta,
                Column("a", String),
                Column("b", String, key="b1" if both_have_keys else None),
            )
            t1.append_column(
                Column("b", String, key="b2"), replace_existing=True
            )

        else:
            scenario.fail()

        if scenario.separate_ee_key_first:
            if both_have_keys:
                eq_(t1.c.keys(), ["a", "b1"])
            else:
                eq_(t1.c.keys(), ["a", "b"])
        else:
            eq_(t1.c.keys(), ["a", "b2"])
        self.assert_compile(select(t1), "SELECT users.a, users.b FROM users")

    def test_exception_no_flags(self, existing_meta):
        def go():
            Table(
                "users",
                existing_meta,
                Column("name", Unicode),
                autoload_with=testing.db,
            )

        assert_raises_message(
            exc.InvalidRequestError,
            "Table 'users' is already defined for this MetaData instance.",
            go,
        )

    def test_keep_plus_existing_raises(self, existing_meta):
        assert_raises(
            exc.ArgumentError,
            Table,
            "users",
            existing_meta,
            keep_existing=True,
            extend_existing=True,
        )

    def test_keep_existing_no_dupe_constraints(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("id", Integer),
            Column("name", Unicode),
            UniqueConstraint("name"),
            keep_existing=True,
        )
        assert "name" in users.c
        assert "id" in users.c
        eq_(len(users.constraints), 2)

        u2 = Table(
            "users",
            empty_meta,
            Column("id", Integer),
            Column("name", Unicode),
            UniqueConstraint("name"),
            keep_existing=True,
        )
        eq_(len(u2.constraints), 2)

    def test_extend_existing_dupes_constraints(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("id", Integer),
            Column("name", Unicode),
            UniqueConstraint("name"),
            extend_existing=True,
        )
        assert "name" in users.c
        assert "id" in users.c
        eq_(len(users.constraints), 2)

        u2 = Table(
            "users",
            empty_meta,
            Column("id", Integer),
            Column("name", Unicode),
            UniqueConstraint("name"),
            extend_existing=True,
        )
        # constraint got duped
        eq_(len(u2.constraints), 3)

    def test_autoload_replace_column(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("name", Unicode),
            autoload_with=testing.db,
        )
        assert isinstance(users.c.name.type, Unicode)

    def test_keep_existing_coltype(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("name", Unicode),
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert not isinstance(users.c.name.type, Unicode)

    def test_keep_existing_quote(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            quote=True,
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert not users.name.quote

    def test_keep_existing_add_column(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("foo", Integer),
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert "foo" not in users.c

    def test_keep_existing_coltype_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("name", Unicode),
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert isinstance(users.c.name.type, Unicode)

    @testing.skip_if(
        lambda: testing.db.dialect.requires_name_normalize,
        "test depends on lowercase as case insensitive",
    )
    def test_keep_existing_quote_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            quote=True,
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert users.name.quote

    def test_keep_existing_add_column_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("foo", Integer),
            autoload_with=testing.db,
            keep_existing=True,
        )
        assert "foo" in users.c

    def test_keep_existing_coltype_no_reflection(self, existing_meta):
        users = Table(
            "users", existing_meta, Column("name", Unicode), keep_existing=True
        )
        assert not isinstance(users.c.name.type, Unicode)

    def test_keep_existing_quote_no_reflection(self, existing_meta):
        users = Table("users", existing_meta, quote=True, keep_existing=True)
        assert not users.name.quote

    def test_keep_existing_add_column_no_reflection(self, existing_meta):
        users = Table(
            "users", existing_meta, Column("foo", Integer), keep_existing=True
        )
        assert "foo" not in users.c

    def test_extend_existing_coltype(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("name", Unicode),
            autoload_with=testing.db,
            extend_existing=True,
        )
        assert isinstance(users.c.name.type, Unicode)

    def test_extend_existing_quote(self, existing_meta):
        assert_raises_message(
            tsa.exc.ArgumentError,
            "Can't redefine 'quote' or 'quote_schema' arguments",
            Table,
            "users",
            existing_meta,
            quote=True,
            autoload_with=testing.db,
            extend_existing=True,
        )

    def test_extend_existing_add_column(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("foo", Integer),
            autoload_with=testing.db,
            extend_existing=True,
        )
        assert "foo" in users.c

    def test_extend_existing_coltype_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("name", Unicode),
            autoload_with=testing.db,
            extend_existing=True,
        )
        assert isinstance(users.c.name.type, Unicode)

    @testing.skip_if(
        lambda: testing.db.dialect.requires_name_normalize,
        "test depends on lowercase as case insensitive",
    )
    def test_extend_existing_quote_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            quote=True,
            autoload_with=testing.db,
            extend_existing=True,
        )
        assert users.name.quote

    def test_extend_existing_add_column_no_orig(self, empty_meta):
        users = Table(
            "users",
            empty_meta,
            Column("foo", Integer),
            autoload_with=testing.db,
            extend_existing=True,
        )
        assert "foo" in users.c

    def test_extend_existing_coltype_no_reflection(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("name", Unicode),
            extend_existing=True,
        )
        assert isinstance(users.c.name.type, Unicode)

    def test_extend_existing_quote_no_reflection(self, existing_meta):
        assert_raises_message(
            tsa.exc.ArgumentError,
            "Can't redefine 'quote' or 'quote_schema' arguments",
            Table,
            "users",
            existing_meta,
            quote=True,
            extend_existing=True,
        )

    def test_extend_existing_add_column_no_reflection(self, existing_meta):
        users = Table(
            "users",
            existing_meta,
            Column("foo", Integer),
            extend_existing=True,
        )
        assert "foo" in users.c


class ConstraintTest(fixtures.TestBase):
    def _single_fixture(self):
        m = MetaData()

        t1 = Table("t1", m, Column("a", Integer), Column("b", Integer))

        t2 = Table("t2", m, Column("a", Integer, ForeignKey("t1.a")))

        t3 = Table("t3", m, Column("a", Integer))
        return t1, t2, t3

    def _assert_index_col_x(self, t, i, columns=True):
        eq_(t.indexes, {i})
        if columns:
            eq_(list(i.columns), [t.c.x])
        else:
            eq_(list(i.columns), [])
        assert i.table is t

    def test_separate_decl_columns(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer))
        i = Index("i", t.c.x)
        self._assert_index_col_x(t, i)

    def test_separate_decl_columns_functional(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer))
        i = Index("i", func.foo(t.c.x))
        self._assert_index_col_x(t, i)

    def test_index_no_cols_private_table_arg(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer))
        i = Index("i", _table=t)
        is_(i.table, t)
        eq_(list(i.columns), [])

    def test_index_w_cols_private_table_arg(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer))
        i = Index("i", t.c.x, _table=t)
        is_(i.table, t)

        eq_(list(i.columns), [t.c.x])

    def test_inline_decl_columns(self):
        m = MetaData()
        c = Column("x", Integer)
        i = Index("i", c)
        t = Table("t", m, c, i)
        self._assert_index_col_x(t, i)

    def test_inline_decl_columns_functional(self):
        m = MetaData()
        c = Column("x", Integer)
        i = Index("i", func.foo(c))
        t = Table("t", m, c, i)
        self._assert_index_col_x(t, i)

    def test_inline_decl_string(self):
        m = MetaData()
        i = Index("i", "x")
        t = Table("t", m, Column("x", Integer), i)
        self._assert_index_col_x(t, i)

    def test_inline_decl_textonly(self):
        m = MetaData()
        i = Index("i", text("foobar(x)"))
        t = Table("t", m, Column("x", Integer), i)
        self._assert_index_col_x(t, i, columns=False)

    def test_separate_decl_textonly(self):
        m = MetaData()
        i = Index("i", text("foobar(x)"))
        t = Table("t", m, Column("x", Integer))
        t.append_constraint(i)
        self._assert_index_col_x(t, i, columns=False)

    def test_unnamed_column_exception(self):
        # this can occur in some declarative situations
        c = Column(Integer)
        idx = Index("q", c)
        m = MetaData()
        t = Table("t", m, Column("q"))
        assert_raises_message(
            exc.ArgumentError,
            "Can't add unnamed column to column collection",
            t.append_constraint,
            idx,
        )

    def test_non_attached_col_plus_string_expr(self):
        # another one that declarative can lead towards
        metadata = MetaData()

        t1 = Table("a", metadata, Column("id", Integer))

        c2 = Column("x", Integer)

        # if we do it here, no problem
        # t1.append_column(c2)

        idx = Index("foo", c2, desc("foo"))

        t1.append_column(c2)

        self._assert_index_col_x(t1, idx, columns=True)

    def test_column_associated_w_lowercase_table(self):
        from sqlalchemy import table

        c = Column("x", Integer)
        table("foo", c)
        idx = Index("q", c)
        is_(idx.table, None)  # lower-case-T table doesn't have indexes

    def test_clauseelement_extraction_one(self):
        t = Table("t", MetaData(), Column("x", Integer), Column("y", Integer))

        class MyThing:
            def __clause_element__(self):
                return t.c.x + 5

        idx = Index("foo", MyThing())
        self._assert_index_col_x(t, idx)

    def test_clauseelement_extraction_two(self):
        t = Table("t", MetaData(), Column("x", Integer), Column("y", Integer))

        class MyThing:
            def __clause_element__(self):
                return t.c.x + 5

        idx = Index("bar", MyThing(), t.c.y)

        eq_(set(t.indexes), {idx})

    def test_clauseelement_extraction_three(self):
        t = Table("t", MetaData(), Column("x", Integer), Column("y", Integer))

        expr1 = t.c.x + 5

        class MyThing:
            def __clause_element__(self):
                return expr1

        idx = Index("bar", MyThing(), t.c.y)

        is_true(idx.expressions[0].compare(expr1))
        is_(idx.expressions[1], t.c.y)

    def test_table_references(self):
        t1, t2, t3 = self._single_fixture()
        assert list(t2.c.a.foreign_keys)[0].references(t1)
        assert not list(t2.c.a.foreign_keys)[0].references(t3)

    def test_column_references(self):
        t1, t2, t3 = self._single_fixture()
        assert t2.c.a.references(t1.c.a)
        assert not t2.c.a.references(t3.c.a)
        assert not t2.c.a.references(t1.c.b)

    def test_column_references_derived(self):
        t1, t2, t3 = self._single_fixture()
        s1 = tsa.select(tsa.select(t1).alias()).subquery()
        assert t2.c.a.references(s1.c.a)
        assert not t2.c.a.references(s1.c.b)

    def test_copy_doesnt_reference(self):
        t1, t2, t3 = self._single_fixture()
        a2 = t2.c.a._copy()
        assert not a2.references(t1.c.a)
        assert not a2.references(t1.c.b)

    def test_derived_column_references(self):
        t1, t2, t3 = self._single_fixture()
        s1 = tsa.select(tsa.select(t2).alias()).subquery()
        assert s1.c.a.references(t1.c.a)
        assert not s1.c.a.references(t1.c.b)

    def test_referred_table_accessor(self):
        t1, t2, t3 = self._single_fixture()
        fkc = list(t2.foreign_key_constraints)[0]
        is_(fkc.referred_table, t1)

    def test_referred_table_accessor_not_available(self):
        t1 = Table("t", MetaData(), Column("x", ForeignKey("q.id")))
        fkc = list(t1.foreign_key_constraints)[0]
        assert_raises_message(
            exc.InvalidRequestError,
            "Foreign key associated with column 't.x' could not find "
            "table 'q' with which to generate a foreign key to target "
            "column 'id'",
            getattr,
            fkc,
            "referred_table",
        )

    def test_related_column_not_present_atfirst_ok(self):
        m = MetaData()
        base_table = Table("base", m, Column("id", Integer, primary_key=True))
        fk = ForeignKey("base.q")
        derived_table = Table(
            "derived", m, Column("id", None, fk, primary_key=True)
        )

        base_table.append_column(Column("q", Integer))
        assert fk.column is base_table.c.q
        assert isinstance(derived_table.c.id.type, Integer)

    def test_related_column_not_present_atfirst_ok_onname(self):
        m = MetaData()
        base_table = Table("base", m, Column("id", Integer, primary_key=True))
        fk = ForeignKey("base.q", link_to_name=True)
        derived_table = Table(
            "derived", m, Column("id", None, fk, primary_key=True)
        )

        base_table.append_column(Column("q", Integer, key="zz"))
        assert fk.column is base_table.c.zz
        assert isinstance(derived_table.c.id.type, Integer)

    def test_related_column_not_present_atfirst_ok_linktoname_conflict(self):
        m = MetaData()
        base_table = Table("base", m, Column("id", Integer, primary_key=True))
        fk = ForeignKey("base.q", link_to_name=True)
        derived_table = Table(
            "derived", m, Column("id", None, fk, primary_key=True)
        )

        base_table.append_column(Column("zz", Integer, key="q"))
        base_table.append_column(Column("q", Integer, key="zz"))
        assert fk.column is base_table.c.zz
        assert isinstance(derived_table.c.id.type, Integer)

    def test_invalid_composite_fk_check_strings(self):
        m = MetaData()

        assert_raises_message(
            exc.ArgumentError,
            r"ForeignKeyConstraint on t1\(x, y\) refers to "
            "multiple remote tables: t2 and t3",
            Table,
            "t1",
            m,
            Column("x", Integer),
            Column("y", Integer),
            ForeignKeyConstraint(["x", "y"], ["t2.x", "t3.y"]),
        )

    def test_invalid_composite_fk_check_columns(self):
        m = MetaData()

        t2 = Table("t2", m, Column("x", Integer))
        t3 = Table("t3", m, Column("y", Integer))

        assert_raises_message(
            exc.ArgumentError,
            r"ForeignKeyConstraint on t1\(x, y\) refers to "
            "multiple remote tables: t2 and t3",
            Table,
            "t1",
            m,
            Column("x", Integer),
            Column("y", Integer),
            ForeignKeyConstraint(["x", "y"], [t2.c.x, t3.c.y]),
        )

    def test_invalid_composite_fk_check_columns_notattached(self):
        m = MetaData()
        x = Column("x", Integer)
        y = Column("y", Integer)

        # no error is raised for this one right now.
        # which is a minor bug.
        Table(
            "t1",
            m,
            Column("x", Integer),
            Column("y", Integer),
            ForeignKeyConstraint(["x", "y"], [x, y]),
        )

        Table("t2", m, x)
        Table("t3", m, y)

    def test_constraint_copied_to_proxy_ok(self):
        m = MetaData()
        Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            m,
            Column("id", Integer, ForeignKey("t1.id"), primary_key=True),
        )

        s = tsa.select(t2).subquery()
        t2fk = list(t2.c.id.foreign_keys)[0]
        sfk = list(s.c.id.foreign_keys)[0]

        # the two FKs share the ForeignKeyConstraint
        is_(t2fk.constraint, sfk.constraint)

        # but the ForeignKeyConstraint isn't
        # aware of the select's FK
        eq_(t2fk.constraint.elements, [t2fk])

    def test_type_propagate_composite_fk_string(self):
        metadata = MetaData()
        Table(
            "a",
            metadata,
            Column("key1", Integer, primary_key=True),
            Column("key2", String(40), primary_key=True),
        )

        b = Table(
            "b",
            metadata,
            Column("a_key1", None),
            Column("a_key2", None),
            Column("id", Integer, primary_key=True),
            ForeignKeyConstraint(["a_key1", "a_key2"], ["a.key1", "a.key2"]),
        )

        assert isinstance(b.c.a_key1.type, Integer)
        assert isinstance(b.c.a_key2.type, String)

    def test_type_propagate_composite_fk_col(self):
        metadata = MetaData()
        a = Table(
            "a",
            metadata,
            Column("key1", Integer, primary_key=True),
            Column("key2", String(40), primary_key=True),
        )

        b = Table(
            "b",
            metadata,
            Column("a_key1", None),
            Column("a_key2", None),
            Column("id", Integer, primary_key=True),
            ForeignKeyConstraint(["a_key1", "a_key2"], [a.c.key1, a.c.key2]),
        )

        assert isinstance(b.c.a_key1.type, Integer)
        assert isinstance(b.c.a_key2.type, String)

    def test_type_propagate_standalone_fk_string(self):
        metadata = MetaData()
        Table("a", metadata, Column("key1", Integer, primary_key=True))

        b = Table("b", metadata, Column("a_key1", None, ForeignKey("a.key1")))

        assert isinstance(b.c.a_key1.type, Integer)

    def test_type_propagate_standalone_fk_col(self):
        metadata = MetaData()
        a = Table("a", metadata, Column("key1", Integer, primary_key=True))

        b = Table("b", metadata, Column("a_key1", None, ForeignKey(a.c.key1)))

        assert isinstance(b.c.a_key1.type, Integer)

    def test_type_propagate_chained_string_source_first(self):
        metadata = MetaData()
        Table("a", metadata, Column("key1", Integer, primary_key=True))

        b = Table("b", metadata, Column("a_key1", None, ForeignKey("a.key1")))

        c = Table(
            "c", metadata, Column("b_key1", None, ForeignKey("b.a_key1"))
        )

        assert isinstance(b.c.a_key1.type, Integer)
        assert isinstance(c.c.b_key1.type, Integer)

    def test_type_propagate_chained_string_source_last(self):
        metadata = MetaData()

        b = Table("b", metadata, Column("a_key1", None, ForeignKey("a.key1")))

        c = Table(
            "c", metadata, Column("b_key1", None, ForeignKey("b.a_key1"))
        )

        Table("a", metadata, Column("key1", Integer, primary_key=True))

        assert isinstance(b.c.a_key1.type, Integer)
        assert isinstance(c.c.b_key1.type, Integer)

    def test_type_propagate_chained_string_source_last_onname(self):
        metadata = MetaData()

        b = Table(
            "b",
            metadata,
            Column(
                "a_key1",
                None,
                ForeignKey("a.key1", link_to_name=True),
                key="ak1",
            ),
        )

        c = Table(
            "c",
            metadata,
            Column(
                "b_key1",
                None,
                ForeignKey("b.a_key1", link_to_name=True),
                key="bk1",
            ),
        )

        Table(
            "a", metadata, Column("key1", Integer, primary_key=True, key="ak1")
        )

        assert isinstance(b.c.ak1.type, Integer)
        assert isinstance(c.c.bk1.type, Integer)

    def test_type_propagate_chained_string_source_last_onname_conflict(self):
        metadata = MetaData()

        b = Table(
            "b",
            metadata,
            # b.c.key1 -> a.c.key1 -> String
            Column(
                "ak1",
                None,
                ForeignKey("a.key1", link_to_name=False),
                key="key1",
            ),
            # b.c.ak1 -> a.c.ak1 -> Integer
            Column(
                "a_key1",
                None,
                ForeignKey("a.key1", link_to_name=True),
                key="ak1",
            ),
        )

        c = Table(
            "c",
            metadata,
            # c.c.b_key1 -> b.c.ak1 -> Integer
            Column("b_key1", None, ForeignKey("b.ak1", link_to_name=False)),
            # c.c.b_ak1 -> b.c.ak1
            Column("b_ak1", None, ForeignKey("b.ak1", link_to_name=True)),
        )

        Table(
            "a",
            metadata,
            # a.c.key1
            Column("ak1", String, key="key1"),
            # a.c.ak1
            Column("key1", Integer, primary_key=True, key="ak1"),
        )

        assert isinstance(b.c.key1.type, String)
        assert isinstance(b.c.ak1.type, Integer)

        assert isinstance(c.c.b_ak1.type, String)
        assert isinstance(c.c.b_key1.type, Integer)

    def test_type_propagate_chained_col_orig_first(self):
        metadata = MetaData()
        a = Table("a", metadata, Column("key1", Integer, primary_key=True))

        b = Table("b", metadata, Column("a_key1", None, ForeignKey(a.c.key1)))

        c = Table(
            "c", metadata, Column("b_key1", None, ForeignKey(b.c.a_key1))
        )

        assert isinstance(b.c.a_key1.type, Integer)
        assert isinstance(c.c.b_key1.type, Integer)

    def test_column_accessor_col(self):
        c1 = Column("x", Integer)
        fk = ForeignKey(c1)
        is_(fk.column, c1)

    def test_column_accessor_clause_element(self):
        c1 = Column("x", Integer)

        class CThing:
            def __init__(self, c):
                self.c = c

            def __clause_element__(self):
                return self.c

        fk = ForeignKey(CThing(c1))
        is_(fk.column, c1)

    def test_column_accessor_string_no_parent(self):
        fk = ForeignKey("sometable.somecol")
        assert_raises_message(
            exc.InvalidRequestError,
            "this ForeignKey object does not yet have a parent "
            "Column associated with it.",
            getattr,
            fk,
            "column",
        )

    def test_column_accessor_string_no_parent_table(self):
        fk = ForeignKey("sometable.somecol")
        Column("x", fk)
        assert_raises_message(
            exc.InvalidRequestError,
            "this ForeignKey's parent column is not yet "
            "associated with a Table.",
            getattr,
            fk,
            "column",
        )

    def test_column_accessor_string_no_target_table(self):
        fk = ForeignKey("sometable.somecol")
        c1 = Column("x", fk)
        Table("t", MetaData(), c1)
        assert_raises_message(
            exc.NoReferencedTableError,
            "Foreign key associated with column 't.x' could not find "
            "table 'sometable' with which to generate a "
            "foreign key to target column 'somecol'",
            getattr,
            fk,
            "column",
        )

    def test_column_accessor_string_no_target_column(self):
        fk = ForeignKey("sometable.somecol")
        c1 = Column("x", fk)
        m = MetaData()
        Table("t", m, c1)
        Table("sometable", m, Column("notsomecol", Integer))
        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for ForeignKey "
            "'sometable.somecol' on table 't': "
            "table 'sometable' has no column named 'somecol'",
            getattr,
            fk,
            "column",
        )

    def test_remove_table_fk_bookkeeping(self):
        metadata = MetaData()
        fk = ForeignKey("t1.x")
        t2 = Table("t2", metadata, Column("y", Integer, fk))
        t3 = Table("t3", metadata, Column("y", Integer, ForeignKey("t1.x")))

        assert t2.key in metadata.tables
        assert ("t1", "x") in metadata._fk_memos

        metadata.remove(t2)

        # key is removed
        assert t2.key not in metadata.tables

        # the memo for the FK is still there
        assert ("t1", "x") in metadata._fk_memos

        # fk is not in the collection
        assert fk not in metadata._fk_memos[("t1", "x")]

        # make the referenced table
        t1 = Table("t1", metadata, Column("x", Integer))

        # t2 tells us exactly what's wrong
        assert_raises_message(
            exc.InvalidRequestError,
            "Table t2 is no longer associated with its parent MetaData",
            getattr,
            fk,
            "column",
        )

        # t3 is unaffected
        assert t3.c.y.references(t1.c.x)

        # remove twice OK
        metadata.remove(t2)

    def test_double_fk_usage_raises(self):
        f = ForeignKey("b.id")

        Column("x", Integer, f)
        assert_raises(exc.InvalidRequestError, Column, "y", Integer, f)

    def test_auto_append_constraint(self):
        m = MetaData()

        t = Table("tbl", m, Column("a", Integer), Column("b", Integer))

        t2 = Table("t2", m, Column("a", Integer), Column("b", Integer))

        for c in (
            UniqueConstraint(t.c.a),
            CheckConstraint(t.c.a > 5),
            ForeignKeyConstraint([t.c.a], [t2.c.a]),
            PrimaryKeyConstraint(t.c.a),
        ):
            assert c in t.constraints
            t.append_constraint(c)
            assert c in t.constraints

        c = Index("foo", t.c.a)
        assert c in t.indexes

    def test_auto_append_lowercase_table(self):
        t = table("t", column("a"))
        t2 = table("t2", column("a"))
        for c in (
            UniqueConstraint(t.c.a),
            CheckConstraint(t.c.a > 5),
            ForeignKeyConstraint([t.c.a], [t2.c.a]),
            PrimaryKeyConstraint(t.c.a),
            Index("foo", t.c.a),
        ):
            assert True

    def test_to_metadata_ok(self):
        m = MetaData()

        t = Table("tbl", m, Column("a", Integer), Column("b", Integer))

        t2 = Table("t2", m, Column("a", Integer), Column("b", Integer))

        UniqueConstraint(t.c.a)
        CheckConstraint(t.c.a > 5)
        ForeignKeyConstraint([t.c.a], [t2.c.a])
        PrimaryKeyConstraint(t.c.a)

        m2 = MetaData()

        t3 = t.to_metadata(m2)

        eq_(len(t3.constraints), 4)

        for c in t3.constraints:
            assert c.table is t3

    def test_ColumnCollectionConstraint_copy(self):
        m = MetaData()

        t = Table("tbl", m, Column("a", Integer), Column("b", Integer))
        t2 = Table("t2", m, Column("a", Integer), Column("b", Integer))

        kw = {
            "comment": "baz",
            "name": "ccc",
            "initially": "foo",
            "deferrable": "bar",
        }

        UniqueConstraint(t.c.a, **kw)
        CheckConstraint(t.c.a > 5, **kw)
        ForeignKeyConstraint([t.c.a], [t2.c.a], **kw)
        PrimaryKeyConstraint(t.c.a, **kw)

        m2 = MetaData()

        t3 = t.to_metadata(m2)

        eq_(len(t3.constraints), 4)

        for c in t3.constraints:
            assert c.table is t3
            for k, v in kw.items():
                eq_(getattr(c, k), v)

    def test_check_constraint_copy(self):
        m = MetaData()
        t = Table("tbl", m, Column("a", Integer), Column("b", Integer))
        ck = CheckConstraint(t.c.a > 5)
        ck2 = ck._copy()
        assert ck in t.constraints
        assert ck2 not in t.constraints

    def test_ambig_check_constraint_auto_append(self):
        m = MetaData()

        t = Table("tbl", m, Column("a", Integer), Column("b", Integer))

        t2 = Table("t2", m, Column("a", Integer), Column("b", Integer))
        c = CheckConstraint(t.c.a > t2.c.b)
        assert c not in t.constraints
        assert c not in t2.constraints

    def test_auto_append_ck_on_col_attach_one(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        ck = CheckConstraint(a > b)

        t = Table("tbl", m, a, b)
        assert ck in t.constraints

    def test_auto_append_ck_on_col_attach_two(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)
        ck = CheckConstraint(a > b + c)

        t = Table("tbl", m, a)
        assert ck not in t.constraints

        t.append_column(b)
        assert ck not in t.constraints

        t.append_column(c)
        assert ck in t.constraints

    def test_auto_append_ck_on_col_attach_three(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)
        ck = CheckConstraint(a > b + c)

        t = Table("tbl", m, a)
        assert ck not in t.constraints

        t.append_column(b)
        assert ck not in t.constraints

        t2 = Table("t2", m)
        t2.append_column(c)

        # two different tables, so CheckConstraint does nothing.
        assert ck not in t.constraints

    def test_auto_append_uq_on_col_attach_one(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        uq = UniqueConstraint(a, b)

        t = Table("tbl", m, a, b)
        assert uq in t.constraints

    def test_auto_append_uq_on_col_attach_two(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)
        uq = UniqueConstraint(a, b, c)

        t = Table("tbl", m, a)
        assert uq not in t.constraints

        t.append_column(b)
        assert uq not in t.constraints

        t.append_column(c)
        assert uq in t.constraints

    def test_auto_append_uq_on_col_attach_three(self):
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)
        uq = UniqueConstraint(a, b, c)

        t = Table("tbl", m, a)
        assert uq not in t.constraints

        t.append_column(b)
        assert uq not in t.constraints

        t2 = Table("t2", m)

        # two different tables, so UniqueConstraint raises
        assert_raises_message(
            exc.ArgumentError,
            r"Column\(s\) 't2\.c' are not part of table 'tbl'\.",
            t2.append_column,
            c,
        )

    def test_auto_append_uq_on_col_attach_four(self):
        """Test that a uniqueconstraint that names Column and string names
        won't autoattach using deferred column attachment.

        """
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)
        uq = UniqueConstraint(a, "b", "c")

        t = Table("tbl", m, a)
        assert uq not in t.constraints

        t.append_column(b)
        assert uq not in t.constraints

        t.append_column(c)

        # we don't track events for previously unknown columns
        # named 'c' to be attached
        assert uq not in t.constraints

        t.append_constraint(uq)

        assert uq in t.constraints

        eq_(
            [cn for cn in t.constraints if isinstance(cn, UniqueConstraint)],
            [uq],
        )

    def test_auto_append_uq_on_col_attach_five(self):
        """Test that a uniqueconstraint that names Column and string names
        *will* autoattach if the table has all those names up front.

        """
        m = MetaData()

        a = Column("a", Integer)
        b = Column("b", Integer)
        c = Column("c", Integer)

        t = Table("tbl", m, a, c, b)

        uq = UniqueConstraint(a, "b", "c")

        assert uq in t.constraints

        t.append_constraint(uq)

        assert uq in t.constraints

        eq_(
            [cn for cn in t.constraints if isinstance(cn, UniqueConstraint)],
            [uq],
        )

    def test_index_asserts_cols_standalone(self):
        metadata = MetaData()

        t1 = Table("t1", metadata, Column("x", Integer))
        t2 = Table("t2", metadata, Column("y", Integer))
        assert_raises_message(
            exc.ArgumentError,
            r"Column\(s\) 't2.y' are not part of table 't1'.",
            Index,
            "bar",
            t1.c.x,
            t2.c.y,
        )

    def test_index_asserts_cols_inline(self):
        metadata = MetaData()

        t1 = Table("t1", metadata, Column("x", Integer))
        assert_raises_message(
            exc.ArgumentError,
            "Index 'bar' is against table 't1', and "
            "cannot be associated with table 't2'.",
            Table,
            "t2",
            metadata,
            Column("y", Integer),
            Index("bar", t1.c.x),
        )

    def test_raise_index_nonexistent_name(self):
        m = MetaData()

        with expect_raises_message(
            exc.ConstraintColumnNotFoundError,
            "Can't create Index on table 't': no column named 'q' is present.",
        ):
            Table("t", m, Column("x", Integer), Index("foo", "q"))

    def test_raise_not_a_column(self):
        assert_raises(exc.ArgumentError, Index, "foo", 5)

    def test_raise_expr_no_column(self):
        idx = Index("foo", func.lower(5))

        assert_raises_message(
            exc.CompileError,
            "Index 'foo' is not associated with any table.",
            schema.CreateIndex(idx).compile,
            dialect=testing.db.dialect,
        )
        assert_raises_message(
            exc.CompileError,
            "Index 'foo' is not associated with any table.",
            schema.CreateIndex(idx).compile,
        )

    def test_no_warning_w_no_columns(self):
        idx = Index(name="foo")

        assert_raises_message(
            exc.CompileError,
            "Index 'foo' is not associated with any table.",
            schema.CreateIndex(idx).compile,
            dialect=testing.db.dialect,
        )
        assert_raises_message(
            exc.CompileError,
            "Index 'foo' is not associated with any table.",
            schema.CreateIndex(idx).compile,
        )

    def test_raise_clauseelement_not_a_column(self):
        m = MetaData()
        t2 = Table("t2", m, Column("x", Integer))

        class SomeClass:
            def __clause_element__(self):
                return t2

        assert_raises_message(
            exc.ArgumentError,
            r"String column name or column expression for DDL constraint "
            r"expected, got .*SomeClass",
            Index,
            "foo",
            SomeClass(),
        )

    @testing.fixture
    def no_pickle_annotated(self):
        class NoPickle:
            def __reduce__(self):
                raise NotImplementedError()

        class ClauseElement(operators.ColumnOperators):
            def __init__(self, col):
                self.col = col._annotate({"bar": NoPickle()})

            def __clause_element__(self):
                return self.col

            def operate(self, op, *other, **kwargs):
                return self.col.operate(op, *other, **kwargs)

        m = MetaData()
        t = Table("t", m, Column("q", Integer))
        return t, ClauseElement(t.c.q)

    def test_pickle_fk_annotated_col(self, no_pickle_annotated):
        t, q_col = no_pickle_annotated

        t2 = Table("t2", t.metadata, Column("p", ForeignKey(q_col)))
        assert t2.c.p.references(t.c.q)

        m2 = pickle.loads(pickle.dumps(t.metadata))

        m2_t, m2_t2 = m2.tables["t"], m2.tables["t2"]

        is_true(m2_t2.c.p.references(m2_t.c.q))

    def test_pickle_uq_annotated_col(self, no_pickle_annotated):
        t, q_col = no_pickle_annotated

        t.append_constraint(UniqueConstraint(q_col))

        m2 = pickle.loads(pickle.dumps(t.metadata))

        const = [
            c
            for c in m2.tables["t"].constraints
            if isinstance(c, UniqueConstraint)
        ][0]

        is_true(const.columns[0].compare(t.c.q))

    def test_pickle_idx_expr_annotated_col(self, no_pickle_annotated):
        t, q_col = no_pickle_annotated

        expr = q_col > 5
        t.append_constraint(Index("conditional_index", expr))

        m2 = pickle.loads(pickle.dumps(t.metadata))

        const = list(m2.tables["t"].indexes)[0]

        is_true(const.expressions[0].compare(expr))

    def test_pickle_ck_binary_annotated_col(self, no_pickle_annotated):
        t, q_col = no_pickle_annotated

        ck = CheckConstraint(q_col > 5)
        t.append_constraint(ck)

        m2 = pickle.loads(pickle.dumps(t.metadata))
        const = [
            c
            for c in m2.tables["t"].constraints
            if isinstance(c, CheckConstraint)
        ][0]
        is_true(const.sqltext.compare(ck.sqltext))


class ColumnDefinitionTest(AssertsCompiledSQL, fixtures.TestBase):
    """Test Column() construction."""

    __dialect__ = "default"

    def columns(self):
        return [
            Column(Integer),
            Column("b", Integer),
            Column(Integer),
            Column("d", Integer),
            Column(Integer, name="e"),
            Column(type_=Integer),
            Column(Integer()),
            Column("h", Integer()),
            Column(type_=Integer()),
        ]

    def test_basic(self):
        c = self.columns()

        for i, v in ((0, "a"), (2, "c"), (5, "f"), (6, "g"), (8, "i")):
            c[i].name = v
            c[i].key = v
        del i, v

        tbl = Table("table", MetaData(), *c)

        for i, col in enumerate(tbl.c):
            assert col.name == c[i].name

    def test_name_none(self):
        c = Column(Integer)
        assert_raises_message(
            exc.ArgumentError,
            "Column must be constructed with a non-blank name or assign a "
            "non-blank .name ",
            Table,
            "t",
            MetaData(),
            c,
        )

    def test_name_blank(self):
        c = Column("", Integer)
        assert_raises_message(
            exc.ArgumentError,
            "Column must be constructed with a non-blank name or assign a "
            "non-blank .name ",
            Table,
            "t",
            MetaData(),
            c,
        )

    def test_no_shared_column_schema(self):
        c = Column("x", Integer)
        Table("t", MetaData(), c)

        assert_raises_message(
            exc.ArgumentError,
            "Column object 'x' already assigned to Table 't'",
            Table,
            "q",
            MetaData(),
            c,
        )

    def test_no_shared_column_sql(self):
        c = column("x", Integer)
        table("t", c)

        assert_raises_message(
            exc.ArgumentError,
            "column object 'x' already assigned to table 't'",
            table,
            "q",
            c,
        )

    def test_incomplete_key(self):
        c = Column(Integer)
        assert c.name is None
        assert c.key is None

        c.name = "named"
        Table("t", MetaData(), c)

        assert c.name == "named"
        assert c.name == c.key

    def test_unique_index_flags_default_to_none(self):
        c = Column(Integer)
        eq_(c.unique, None)
        eq_(c.index, None)

        c = Column("c", Integer, index=True)
        eq_(c.unique, None)
        eq_(c.index, True)

        t = Table("t", MetaData(), c)
        eq_(list(t.indexes)[0].unique, False)

        c = Column(Integer, unique=True)
        eq_(c.unique, True)
        eq_(c.index, None)

        c = Column("c", Integer, index=True, unique=True)
        eq_(c.unique, True)
        eq_(c.index, True)

        t = Table("t", MetaData(), c)
        eq_(list(t.indexes)[0].unique, True)

    def test_bogus(self):
        assert_raises(exc.ArgumentError, Column, "foo", name="bar")
        assert_raises(
            exc.ArgumentError, Column, "foo", Integer, type_=Integer()
        )

    def test_custom_subclass_proxy(self):
        """test proxy generation of a Column subclass, can be compiled."""

        from sqlalchemy.schema import Column
        from sqlalchemy.ext.compiler import compiles
        from sqlalchemy.sql import select

        class MyColumn(Column):
            def _constructor(self, name, type_, **kw):
                kw["name"] = name
                return MyColumn(type_, **kw)

            def __init__(self, type_, **kw):
                Column.__init__(self, type_, **kw)

            def my_goofy_thing(self):
                return "hi"

        @compiles(MyColumn)
        def goofy(element, compiler, **kw):
            s = compiler.visit_column(element, **kw)
            return s + "-"

        id_ = MyColumn(Integer, primary_key=True)
        id_.name = "id"
        name = MyColumn(String)
        name.name = "name"
        t1 = Table("foo", MetaData(), id_, name)

        # goofy thing
        eq_(t1.c.name.my_goofy_thing(), "hi")

        # create proxy
        s = select(t1.select().alias())

        # proxy has goofy thing
        eq_(s.subquery().c.name.my_goofy_thing(), "hi")

        # compile works
        self.assert_compile(
            select(t1.select().alias()),
            "SELECT anon_1.id-, anon_1.name- FROM "
            "(SELECT foo.id- AS id, foo.name- AS name "
            "FROM foo) AS anon_1",
        )

    def test_custom_subclass_proxy_typeerror(self):
        from sqlalchemy.schema import Column
        from sqlalchemy.sql import select

        class MyColumn(Column):
            def __init__(self, type_, **kw):
                Column.__init__(self, type_, **kw)

        id_ = MyColumn(Integer, primary_key=True)
        id_.name = "id"
        name = MyColumn(String)
        name.name = "name"
        t1 = Table("foo", MetaData(), id_, name)
        assert_raises_message(
            TypeError,
            "Could not create a copy of this <class "
            "'test.sql.test_metadata..*MyColumn'> "
            "object.  Ensure the class includes a _constructor()",
            getattr,
            select(t1.select().alias()).subquery(),
            "c",
        )

    def test_custom_create(self):
        @compiles(schema.CreateColumn)
        def compile_(element, compiler, **kw):
            column = element.element

            if "special" not in column.info:
                return compiler.visit_create_column(element, **kw)

            text = "%s SPECIAL DIRECTIVE %s" % (
                column.name,
                compiler.type_compiler.process(column.type),
            )
            default = compiler.get_column_default_string(column)
            if default is not None:
                text += " DEFAULT " + default

            if not column.nullable:
                text += " NOT NULL"

            if column.constraints:
                text += " ".join(
                    compiler.process(const) for const in column.constraints
                )
            return text

        t = Table(
            "mytable",
            MetaData(),
            Column("x", Integer, info={"special": True}, primary_key=True),
            Column("y", String(50)),
            Column("z", String(20), info={"special": True}),
        )

        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE mytable (x SPECIAL DIRECTIVE INTEGER "
            "NOT NULL, y VARCHAR(50), "
            "z SPECIAL DIRECTIVE VARCHAR(20), PRIMARY KEY (x))",
        )

        deregister(schema.CreateColumn)

    @testing.combinations(("index",), ("unique",), argnames="paramname")
    @testing.combinations((True,), (False,), (None,), argnames="orig")
    @testing.combinations((True,), (False,), (None,), argnames="merging")
    def test_merge_index_unique(self, paramname, orig, merging):
        """test #11091"""
        source = Column(**{paramname: merging})

        target = Column(**{paramname: orig})

        source._merge(target)

        target_copy = target._copy()
        for col in (
            target,
            target_copy,
        ):
            result = getattr(col, paramname)
            if orig is None:
                is_(result, merging)
            else:
                is_(result, orig)

    @testing.combinations(
        ("default", lambda ctx: 10),
        ("default", func.foo()),
        ("identity_gen", Identity()),
        ("identity_gen", Sequence("some_seq")),
        ("identity_gen", Computed("side * side")),
        ("onupdate", lambda ctx: 10),
        ("onupdate", func.foo()),
        ("server_onupdate", func.foo()),
        ("server_default", func.foo()),
        ("nullable", True),
        ("nullable", False),
        ("index", True),
        ("unique", True),
        ("type", BigInteger()),
        ("type", Enum("one", "two", "three", create_constraint=True)),
        ("doc", "some doc"),
        ("comment", "some comment"),
        ("system", True),
        ("autoincrement", True),
        ("info", {"foo": "bar"}),
        argnames="paramname, value",
    )
    def test_merge_column(
        self,
        paramname,
        value,
    ):
        args = []
        params = {}
        if paramname == "type" or isinstance(
            value, (Computed, Sequence, Identity)
        ):
            args.append(value)
        else:
            params[paramname] = value

        source = Column(*args, **params)

        target = Column()

        source._merge(target)

        target_copy = target._copy()
        for col in (
            target,
            target_copy,
        ):
            if isinstance(value, (Computed, Identity)):
                default = col.server_default
                assert isinstance(default, type(value))
                is_(default.column, col)
            elif isinstance(value, Sequence):
                default = col.default

                # TODO: sequence mutated in place
                is_(default.column, target_copy)

                assert isinstance(default, type(value))

            elif paramname in (
                "default",
                "onupdate",
                "server_default",
                "server_onupdate",
            ):
                default = getattr(col, paramname)
                is_(default.arg, value)

                # TODO: _copy() seems to note that it isn't copying
                # server defaults or defaults outside of Computed, Identity,
                # so here it's getting mutated in place.   this is a bug
                is_(default.column, target_copy)

            elif paramname in ("info",):
                eq_(col.info, value)
            elif paramname == "type":
                assert type(col.type) is type(value)

                if isinstance(col.type, Enum):
                    col.name = "data"
                    t = Table("t", MetaData(), col)
                    assert CheckConstraint in [type(c) for c in t.constraints]
            else:
                is_(getattr(col, paramname), value)

    @testing.combinations(True, False, argnames="specify_identity")
    @testing.combinations(True, False, None, argnames="specify_nullable")
    def test_merge_column_identity(
        self,
        specify_identity,
        specify_nullable,
    ):
        if specify_identity:
            args = [Identity()]
        else:
            args = []

        if specify_nullable is not None:
            params = {"nullable": specify_nullable}
        else:
            params = {}

        source = Column(*args, **params)

        target = Column()

        source._merge(target)

        # test identity + _copy() for #8410
        for col in (
            target,
            target._copy(),
        ):
            if specify_nullable is True:
                is_(col.nullable, True)
            elif specify_identity:
                is_(col.nullable, False)
            elif specify_nullable is False:
                is_(col.nullable, False)
            else:
                is_(col.nullable, True)

    @testing.combinations(
        ("default", lambda ctx: 10, lambda ctx: 15),
        ("default", func.foo(), func.bar()),
        ("identity_gen", Identity(), Identity()),
        ("identity_gen", Sequence("some_seq"), Sequence("some_other_seq")),
        ("identity_gen", Computed("side * side"), Computed("top / top")),
        ("onupdate", lambda ctx: 10, lambda ctx: 15),
        ("onupdate", func.foo(), func.bar()),
        ("server_onupdate", func.foo(), func.bar()),
        ("server_default", func.foo(), func.bar()),
        ("nullable", True, False),
        ("nullable", False, True),
        ("type", BigInteger(), Numeric()),
        argnames="paramname, value, override_value",
    )
    def test_dont_merge_column(
        self,
        paramname,
        value,
        override_value,
    ):
        args = []
        params = {}
        override_args = []
        override_params = {}
        if paramname == "type" or isinstance(
            value, (Computed, Sequence, Identity)
        ):
            args.append(value)
            override_args.append(override_value)
        else:
            params[paramname] = value
            override_params[paramname] = override_value

        source = Column(*args, **params)

        target = Column(*override_args, **override_params)

        source._merge(target)

        if isinstance(value, Sequence):
            default = target.default
            assert default is override_value
        elif isinstance(value, (Computed, Identity)):
            default = target.server_default
            assert default is override_value
        elif paramname in (
            "default",
            "onupdate",
            "server_default",
            "server_onupdate",
        ):
            default = getattr(target, paramname)
            is_(default.arg, override_value)
            is_(default.column, target)
        elif paramname == "type":
            assert type(target.type) is type(override_value)
        else:
            is_(getattr(target, paramname), override_value)


class ColumnDefaultsTest(fixtures.TestBase):
    """test assignment of default fixures to columns"""

    def _fixture(self, *arg, **kw):
        return Column("x", Integer, *arg, **kw)

    def test_server_default_positional(self):
        target = schema.DefaultClause("y")
        c = self._fixture(target)
        assert c.server_default is target
        assert target.column is c

    def test_onupdate_default_not_server_default_one(self):
        target1 = schema.DefaultClause("y")
        target2 = schema.DefaultClause("z")

        c = self._fixture(server_default=target1, server_onupdate=target2)
        eq_(c.server_default.arg, "y")
        eq_(c.server_onupdate.arg, "z")

    def test_onupdate_default_not_server_default_two(self):
        target1 = schema.DefaultClause("y", for_update=True)
        target2 = schema.DefaultClause("z", for_update=True)

        c = self._fixture(server_default=target1, server_onupdate=target2)
        eq_(c.server_default.arg, "y")
        eq_(c.server_onupdate.arg, "z")

    def test_onupdate_default_not_server_default_three(self):
        target1 = schema.DefaultClause("y", for_update=False)
        target2 = schema.DefaultClause("z", for_update=True)

        c = self._fixture(target1, target2)
        eq_(c.server_default.arg, "y")
        eq_(c.server_onupdate.arg, "z")

    def test_onupdate_default_not_server_default_four(self):
        target1 = schema.DefaultClause("y", for_update=False)

        c = self._fixture(server_onupdate=target1)
        is_(c.server_default, None)
        eq_(c.server_onupdate.arg, "y")

    def test_server_default_keyword_as_schemaitem(self):
        target = schema.DefaultClause("y")
        c = self._fixture(server_default=target)
        assert c.server_default is target
        assert target.column is c

    def test_server_default_keyword_as_clause(self):
        target = "y"
        c = self._fixture(server_default=target)
        assert c.server_default.arg == target
        assert c.server_default.column is c

    def test_server_default_onupdate_positional(self):
        target = schema.DefaultClause("y", for_update=True)
        c = self._fixture(target)
        assert c.server_onupdate is target
        assert target.column is c

    def test_server_default_onupdate_keyword_as_schemaitem(self):
        target = schema.DefaultClause("y", for_update=True)
        c = self._fixture(server_onupdate=target)
        assert c.server_onupdate is target
        assert target.column is c

    def test_server_default_onupdate_keyword_as_clause(self):
        target = "y"
        c = self._fixture(server_onupdate=target)
        assert c.server_onupdate.arg == target
        assert c.server_onupdate.column is c

    def test_column_default_positional(self):
        target = schema.ColumnDefault("y")
        c = self._fixture(target)
        assert c.default is target
        assert target.column is c

    def test_column_default_keyword_as_schemaitem(self):
        target = schema.ColumnDefault("y")
        c = self._fixture(default=target)
        assert c.default is target
        assert target.column is c

    def test_column_default_keyword_as_clause(self):
        target = "y"
        c = self._fixture(default=target)
        assert c.default.arg == target
        assert c.default.column is c

    def test_column_default_onupdate_positional(self):
        target = schema.ColumnDefault("y", for_update=True)
        c = self._fixture(target)
        assert c.onupdate is target
        assert target.column is c

    def test_column_default_onupdate_keyword_as_schemaitem(self):
        target = schema.ColumnDefault("y", for_update=True)
        c = self._fixture(onupdate=target)
        assert c.onupdate is target
        assert target.column is c

    def test_column_default_onupdate_keyword_as_clause(self):
        target = "y"
        c = self._fixture(onupdate=target)
        assert c.onupdate.arg == target
        assert c.onupdate.column is c

    def test_column_insert_default(self):
        c = self._fixture(insert_default="y")
        assert c.default.arg == "y"

    def test_column_insert_default_predecende_on_default(self):
        c = self._fixture(insert_default="x", default="y")
        assert c.default.arg == "x"
        c = self._fixture(default="y", insert_default="x")
        assert c.default.arg == "x"


class ColumnOptionsTest(fixtures.TestBase):
    def test_default_generators(self):
        g1, g2 = Sequence("foo_id_seq"), ColumnDefault("f5")
        assert Column(String, default=g1).default is g1
        assert Column(String, onupdate=g1).onupdate is g1
        assert Column(String, default=g2).default is g2
        assert Column(String, onupdate=g2).onupdate is g2

    def _null_type_no_error(self, col):
        c_str = str(schema.CreateColumn(col).compile())
        assert "NULL" in c_str

    def _no_name_error(self, col):
        assert_raises_message(
            exc.ArgumentError,
            "Column must be constructed with a non-blank name or "
            "assign a non-blank .name",
            Table,
            "t",
            MetaData(),
            col,
        )

    def _no_error(self, col):
        m = MetaData()
        Table("bar", m, Column("id", Integer))
        t = Table("t", m, col)
        schema.CreateTable(t).compile()

    def test_argument_signatures(self):
        self._no_name_error(Column())
        self._null_type_no_error(Column("foo"))
        self._no_name_error(Column(default="foo"))

        self._no_name_error(Column(Sequence("a")))
        self._null_type_no_error(Column("foo", default="foo"))

        self._null_type_no_error(Column("foo", Sequence("a")))

        self._no_name_error(Column(ForeignKey("bar.id")))

        self._no_error(Column("foo", ForeignKey("bar.id")))

        self._no_name_error(Column(ForeignKey("bar.id"), default="foo"))

        self._no_name_error(Column(ForeignKey("bar.id"), Sequence("a")))
        self._no_error(Column("foo", ForeignKey("bar.id"), default="foo"))
        self._no_error(Column("foo", ForeignKey("bar.id"), Sequence("a")))

    def test_column_info(self):
        c1 = Column("foo", String, info={"x": "y"})
        c2 = Column("bar", String, info={})
        c3 = Column("bat", String)
        assert c1.info == {"x": "y"}
        assert c2.info == {}
        assert c3.info == {}

        for c in (c1, c2, c3):
            c.info["bar"] = "zip"
            assert c.info["bar"] == "zip"


class CatchAllEventsTest(fixtures.RemovesEvents, fixtures.TestBase):
    def test_all_events(self):
        canary = []

        def before_attach(obj, parent):
            canary.append(
                "%s->%s" % (obj.__class__.__name__, parent.__class__.__name__)
            )

        def after_attach(obj, parent):
            canary.append("%s->%s" % (obj.__class__.__name__, parent))

        self.event_listen(
            schema.SchemaItem, "before_parent_attach", before_attach
        )
        self.event_listen(
            schema.SchemaItem, "after_parent_attach", after_attach
        )

        m = MetaData()
        Table(
            "t1",
            m,
            Column("id", Integer, Sequence("foo_id"), primary_key=True),
            Column("bar", String, ForeignKey("t2.id")),
        )
        Table("t2", m, Column("id", Integer, primary_key=True))

        eq_(
            canary,
            [
                "Sequence->Column",
                "Sequence->id",
                "ForeignKey->Column",
                "ForeignKey->bar",
                "Table->MetaData",
                "PrimaryKeyConstraint->Table",
                "PrimaryKeyConstraint->t1",
                "Column->Table",
                "Column->t1",
                "Column->Table",
                "Column->t1",
                "ForeignKeyConstraint->Table",
                "ForeignKeyConstraint->t1",
                "Table->MetaData()",
                "Table->MetaData",
                "PrimaryKeyConstraint->Table",
                "PrimaryKeyConstraint->t2",
                "Column->Table",
                "Column->t2",
                "Table->MetaData()",
            ],
        )

    def test_events_per_constraint(self):
        canary = []

        def evt(target):
            def before_attach(obj, parent):
                canary.append(
                    "%s->%s" % (target.__name__, parent.__class__.__name__)
                )

            def after_attach(obj, parent):
                assert hasattr(obj, "name")  # so we can change it
                canary.append("%s->%s" % (target.__name__, parent))

            self.event_listen(target, "before_parent_attach", before_attach)
            self.event_listen(target, "after_parent_attach", after_attach)

        for target in [
            schema.ForeignKeyConstraint,
            schema.PrimaryKeyConstraint,
            schema.UniqueConstraint,
            schema.CheckConstraint,
            schema.Index,
        ]:
            evt(target)

        m = MetaData()
        Table(
            "t1",
            m,
            Column("id", Integer, Sequence("foo_id"), primary_key=True),
            Column("bar", String, ForeignKey("t2.id"), index=True),
            Column("bat", Integer, unique=True),
        )
        Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            Column("bar", Integer),
            Column("bat", Integer),
            CheckConstraint("bar>5"),
            UniqueConstraint("bar", "bat"),
            Index(None, "bar", "bat"),
        )
        eq_(
            canary,
            [
                "PrimaryKeyConstraint->Table",
                "PrimaryKeyConstraint->t1",
                "Index->Table",
                "Index->t1",
                "ForeignKeyConstraint->Table",
                "ForeignKeyConstraint->t1",
                "UniqueConstraint->Table",
                "UniqueConstraint->t1",
                "PrimaryKeyConstraint->Table",
                "PrimaryKeyConstraint->t2",
                "CheckConstraint->Table",
                "CheckConstraint->t2",
                "UniqueConstraint->Table",
                "UniqueConstraint->t2",
                "Index->Table",
                "Index->t2",
            ],
        )


class DialectKWArgTest(fixtures.TestBase):
    @contextmanager
    def _fixture(self):
        from sqlalchemy.engine.default import DefaultDialect

        class ParticipatingDialect(DefaultDialect):
            construct_arguments = [
                (schema.Index, {"x": 5, "y": False, "z_one": None}),
                (schema.ForeignKeyConstraint, {"foobar": False}),
            ]

        class ParticipatingDialect2(DefaultDialect):
            construct_arguments = [
                (schema.Index, {"x": 9, "y": True, "pp": "default"}),
                (schema.Table, {"*": None}),
            ]

        class NonParticipatingDialect(DefaultDialect):
            construct_arguments = None

        def load(dialect_name):
            if dialect_name == "participating":
                return ParticipatingDialect
            elif dialect_name == "participating2":
                return ParticipatingDialect2
            elif dialect_name == "nonparticipating":
                return NonParticipatingDialect
            else:
                raise exc.NoSuchModuleError("no dialect %r" % dialect_name)

        with mock.patch("sqlalchemy.dialects.registry.load", load):
            yield

    def teardown_test(self):
        Index._kw_registry.clear()

    def test_participating(self):
        with self._fixture():
            idx = Index("a", "b", "c", participating_y=True)
            eq_(
                idx.dialect_options,
                {"participating": {"x": 5, "y": True, "z_one": None}},
            )
            eq_(idx.dialect_kwargs, {"participating_y": True})

    def test_nonparticipating(self):
        with self._fixture():
            idx = Index(
                "a", "b", "c", nonparticipating_y=True, nonparticipating_q=5
            )
            eq_(
                idx.dialect_kwargs,
                {"nonparticipating_y": True, "nonparticipating_q": 5},
            )

    def test_bad_kwarg_raise(self):
        with self._fixture():
            assert_raises_message(
                TypeError,
                "Additional arguments should be named "
                "<dialectname>_<argument>, got 'foobar'",
                Index,
                "a",
                "b",
                "c",
                foobar=True,
            )

    def test_unknown_dialect_warning(self):
        with self._fixture():
            with testing.expect_warnings(
                "Can't validate argument 'unknown_y'; can't locate "
                "any SQLAlchemy dialect named 'unknown'",
            ):
                Index("a", "b", "c", unknown_y=True)

    def test_participating_bad_kw(self):
        with self._fixture():
            assert_raises_message(
                exc.ArgumentError,
                "Argument 'participating_q_p_x' is not accepted by dialect "
                "'participating' on behalf of "
                "<class 'sqlalchemy.sql.schema.Index'>",
                Index,
                "a",
                "b",
                "c",
                participating_q_p_x=8,
            )

    def test_participating_unknown_schema_item(self):
        with self._fixture():
            # the dialect doesn't include UniqueConstraint in
            # its registry at all.
            assert_raises_message(
                exc.ArgumentError,
                "Argument 'participating_q_p_x' is not accepted by dialect "
                "'participating' on behalf of "
                "<class 'sqlalchemy.sql.schema.UniqueConstraint'>",
                UniqueConstraint,
                "a",
                "b",
                participating_q_p_x=8,
            )

    @testing.emits_warning("Can't validate")
    def test_unknown_dialect_warning_still_populates(self):
        with self._fixture():
            idx = Index("a", "b", "c", unknown_y=True)
            eq_(idx.dialect_kwargs, {"unknown_y": True})  # still populates

    @testing.emits_warning("Can't validate")
    def test_unknown_dialect_warning_still_populates_multiple(self):
        with self._fixture():
            idx = Index(
                "a",
                "b",
                "c",
                unknown_y=True,
                unknown_z=5,
                otherunknown_foo="bar",
                participating_y=8,
            )
            eq_(
                idx.dialect_options,
                {
                    "unknown": {"y": True, "z": 5, "*": None},
                    "otherunknown": {"foo": "bar", "*": None},
                    "participating": {"x": 5, "y": 8, "z_one": None},
                },
            )
            eq_(
                idx.dialect_kwargs,
                {
                    "unknown_z": 5,
                    "participating_y": 8,
                    "unknown_y": True,
                    "otherunknown_foo": "bar",
                },
            )  # still populates

    def test_combined(self):
        with self._fixture():
            idx = Index(
                "a", "b", "c", participating_x=7, nonparticipating_y=True
            )

            eq_(
                idx.dialect_options,
                {
                    "participating": {"y": False, "x": 7, "z_one": None},
                    "nonparticipating": {"y": True, "*": None},
                },
            )
            eq_(
                idx.dialect_kwargs,
                {"participating_x": 7, "nonparticipating_y": True},
            )

    def test_multiple_participating(self):
        with self._fixture():
            idx = Index(
                "a",
                "b",
                "c",
                participating_x=7,
                participating2_x=15,
                participating2_y="lazy",
            )
            eq_(
                idx.dialect_options,
                {
                    "participating": {"x": 7, "y": False, "z_one": None},
                    "participating2": {"x": 15, "y": "lazy", "pp": "default"},
                },
            )
            eq_(
                idx.dialect_kwargs,
                {
                    "participating_x": 7,
                    "participating2_x": 15,
                    "participating2_y": "lazy",
                },
            )

    def test_foreign_key_propagate(self):
        with self._fixture():
            m = MetaData()
            fk = ForeignKey("t2.id", participating_foobar=True)
            t = Table("t", m, Column("id", Integer, fk))
            fkc = [
                c for c in t.constraints if isinstance(c, ForeignKeyConstraint)
            ][0]
            eq_(fkc.dialect_kwargs, {"participating_foobar": True})

    def test_foreign_key_propagate_exceptions_delayed(self):
        with self._fixture():
            m = MetaData()
            fk = ForeignKey("t2.id", participating_fake=True)
            c1 = Column("id", Integer, fk)
            assert_raises_message(
                exc.ArgumentError,
                "Argument 'participating_fake' is not accepted by "
                "dialect 'participating' on behalf of "
                "<class 'sqlalchemy.sql.schema.ForeignKeyConstraint'>",
                Table,
                "t",
                m,
                c1,
            )

    def test_wildcard(self):
        with self._fixture():
            m = MetaData()
            t = Table(
                "x",
                m,
                Column("x", Integer),
                participating2_xyz="foo",
                participating2_engine="InnoDB",
            )
            eq_(
                t.dialect_kwargs,
                {
                    "participating2_xyz": "foo",
                    "participating2_engine": "InnoDB",
                },
            )

    def test_uninit_wildcard(self):
        with self._fixture():
            m = MetaData()
            t = Table("x", m, Column("x", Integer))
            eq_(t.dialect_options["participating2"], {"*": None})
            eq_(t.dialect_kwargs, {})

    def test_not_contains_wildcard(self):
        with self._fixture():
            m = MetaData()
            t = Table("x", m, Column("x", Integer))
            assert "foobar" not in t.dialect_options["participating2"]

    def test_contains_wildcard(self):
        with self._fixture():
            m = MetaData()
            t = Table("x", m, Column("x", Integer), participating2_foobar=5)
            assert "foobar" in t.dialect_options["participating2"]

    def test_update(self):
        with self._fixture():
            idx = Index("a", "b", "c", participating_x=20)
            eq_(idx.dialect_kwargs, {"participating_x": 20})
            idx._validate_dialect_kwargs(
                {"participating_x": 25, "participating_z_one": "default"}
            )
            eq_(
                idx.dialect_options,
                {"participating": {"x": 25, "y": False, "z_one": "default"}},
            )
            eq_(
                idx.dialect_kwargs,
                {"participating_x": 25, "participating_z_one": "default"},
            )

            idx._validate_dialect_kwargs(
                {"participating_x": 25, "participating_z_one": "default"}
            )

            eq_(
                idx.dialect_options,
                {"participating": {"x": 25, "y": False, "z_one": "default"}},
            )
            eq_(
                idx.dialect_kwargs,
                {"participating_x": 25, "participating_z_one": "default"},
            )

            idx._validate_dialect_kwargs(
                {"participating_y": True, "participating2_y": "p2y"}
            )
            eq_(
                idx.dialect_options,
                {
                    "participating": {"x": 25, "y": True, "z_one": "default"},
                    "participating2": {"y": "p2y", "pp": "default", "x": 9},
                },
            )
            eq_(
                idx.dialect_kwargs,
                {
                    "participating_x": 25,
                    "participating_y": True,
                    "participating2_y": "p2y",
                    "participating_z_one": "default",
                },
            )

    def test_key_error_kwargs_no_dialect(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises(KeyError, idx.kwargs.__getitem__, "foo_bar")

    def test_key_error_kwargs_no_underscore(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises(KeyError, idx.kwargs.__getitem__, "foobar")

    def test_key_error_kwargs_no_argument(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises(
                KeyError, idx.kwargs.__getitem__, "participating_asdmfq34098"
            )

            assert_raises(
                KeyError,
                idx.kwargs.__getitem__,
                "nonparticipating_asdmfq34098",
            )

    def test_key_error_dialect_options(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises(
                KeyError,
                idx.dialect_options["participating"].__getitem__,
                "asdfaso890",
            )

            assert_raises(
                KeyError,
                idx.dialect_options["nonparticipating"].__getitem__,
                "asdfaso890",
            )

    def test_ad_hoc_participating_via_opt(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            idx.dialect_options["participating"]["foobar"] = 5

            eq_(idx.dialect_options["participating"]["foobar"], 5)
            eq_(idx.kwargs["participating_foobar"], 5)

    def test_ad_hoc_nonparticipating_via_opt(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            idx.dialect_options["nonparticipating"]["foobar"] = 5

            eq_(idx.dialect_options["nonparticipating"]["foobar"], 5)
            eq_(idx.kwargs["nonparticipating_foobar"], 5)

    def test_ad_hoc_participating_via_kwargs(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            idx.kwargs["participating_foobar"] = 5

            eq_(idx.dialect_options["participating"]["foobar"], 5)
            eq_(idx.kwargs["participating_foobar"], 5)

    def test_ad_hoc_nonparticipating_via_kwargs(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            idx.kwargs["nonparticipating_foobar"] = 5

            eq_(idx.dialect_options["nonparticipating"]["foobar"], 5)
            eq_(idx.kwargs["nonparticipating_foobar"], 5)

    def test_ad_hoc_via_kwargs_invalid_key(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises_message(
                exc.ArgumentError,
                "Keys must be of the form <dialectname>_<argname>",
                idx.kwargs.__setitem__,
                "foobar",
                5,
            )

    def test_ad_hoc_via_kwargs_invalid_dialect(self):
        with self._fixture():
            idx = Index("a", "b", "c")
            assert_raises_message(
                exc.ArgumentError,
                "no dialect 'nonexistent'",
                idx.kwargs.__setitem__,
                "nonexistent_foobar",
                5,
            )

    def test_add_new_arguments_participating(self):
        with self._fixture():
            Index.argument_for("participating", "xyzqpr", False)

            idx = Index("a", "b", "c", participating_xyzqpr=True)

            eq_(idx.kwargs["participating_xyzqpr"], True)

            idx = Index("a", "b", "c")
            eq_(idx.dialect_options["participating"]["xyzqpr"], False)

    def test_add_new_arguments_participating_no_existing(self):
        with self._fixture():
            PrimaryKeyConstraint.argument_for("participating", "xyzqpr", False)

            pk = PrimaryKeyConstraint("a", "b", "c", participating_xyzqpr=True)

            eq_(pk.kwargs["participating_xyzqpr"], True)

            pk = PrimaryKeyConstraint("a", "b", "c")
            eq_(pk.dialect_options["participating"]["xyzqpr"], False)

    def test_add_new_arguments_nonparticipating(self):
        with self._fixture():
            assert_raises_message(
                exc.ArgumentError,
                "Dialect 'nonparticipating' does have keyword-argument "
                "validation and defaults enabled configured",
                Index.argument_for,
                "nonparticipating",
                "xyzqpr",
                False,
            )

    def test_add_new_arguments_invalid_dialect(self):
        with self._fixture():
            assert_raises_message(
                exc.ArgumentError,
                "no dialect 'nonexistent'",
                Index.argument_for,
                "nonexistent",
                "foobar",
                5,
            )


class NamingConventionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _fixture(self, naming_convention, table_schema=None):
        m1 = MetaData(naming_convention=naming_convention)

        u1 = Table(
            "user",
            m1,
            Column("id", Integer, primary_key=True),
            Column("version", Integer, primary_key=True),
            Column("data", String(30)),
            Column("Data2", String(30), key="data2"),
            Column("Data3", String(30), key="data3"),
            schema=table_schema,
        )

        return u1

    def _colliding_name_fixture(self, naming_convention, id_flags):
        m1 = MetaData(naming_convention=naming_convention)

        t1 = Table(
            "foo",
            m1,
            Column("id", Integer, **id_flags),
            Column("foo_id", Integer),
        )
        return t1

    def test_colliding_col_label_from_index_flag(self):
        t1 = self._colliding_name_fixture(
            {"ix": "ix_%(column_0_label)s"}, {"index": True}
        )

        idx = list(t1.indexes)[0]

        # name is generated up front.  alembic really prefers this
        eq_(idx.name, "ix_foo_id")
        self.assert_compile(
            CreateIndex(idx), "CREATE INDEX ix_foo_id ON foo (id)"
        )

    def test_colliding_col_label_from_unique_flag(self):
        t1 = self._colliding_name_fixture(
            {"uq": "uq_%(column_0_label)s"}, {"unique": True}
        )

        const = [c for c in t1.constraints if isinstance(c, UniqueConstraint)]
        uq = const[0]

        # name is generated up front.  alembic really prefers this
        eq_(uq.name, "uq_foo_id")

        self.assert_compile(
            AddConstraint(uq),
            "ALTER TABLE foo ADD CONSTRAINT uq_foo_id UNIQUE (id)",
        )

    def test_colliding_col_label_from_index_obj(self):
        t1 = self._colliding_name_fixture({"ix": "ix_%(column_0_label)s"}, {})

        idx = Index(None, t1.c.id)
        is_(idx, list(t1.indexes)[0])
        eq_(idx.name, "ix_foo_id")
        self.assert_compile(
            CreateIndex(idx), "CREATE INDEX ix_foo_id ON foo (id)"
        )

    def test_colliding_col_label_from_unique_obj(self):
        t1 = self._colliding_name_fixture({"uq": "uq_%(column_0_label)s"}, {})
        uq = UniqueConstraint(t1.c.id)
        const = [c for c in t1.constraints if isinstance(c, UniqueConstraint)]
        is_(const[0], uq)
        eq_(const[0].name, "uq_foo_id")
        self.assert_compile(
            AddConstraint(const[0]),
            "ALTER TABLE foo ADD CONSTRAINT uq_foo_id UNIQUE (id)",
        )

    def test_colliding_col_label_from_index_flag_no_conv(self):
        t1 = self._colliding_name_fixture({"ck": "foo"}, {"index": True})

        idx = list(t1.indexes)[0]

        # this behavior needs to fail, as of #4911 since we are testing it,
        # ensure it raises a CompileError.  In #4289 we may want to revisit
        # this in some way, most likely specifically to Postgresql only.
        assert_raises_message(
            exc.CompileError,
            "CREATE INDEX requires that the index have a name",
            CreateIndex(idx).compile,
        )

        assert_raises_message(
            exc.CompileError,
            "DROP INDEX requires that the index have a name",
            DropIndex(idx).compile,
        )

    def test_colliding_col_label_from_unique_flag_no_conv(self):
        t1 = self._colliding_name_fixture({"ck": "foo"}, {"unique": True})

        const = [c for c in t1.constraints if isinstance(c, UniqueConstraint)]
        is_(const[0].name, None)

        self.assert_compile(
            AddConstraint(const[0]), "ALTER TABLE foo ADD UNIQUE (id)"
        )

    @testing.combinations(
        ("nopk",),
        ("column",),
        ("constraint",),
        ("explicit_name",),
        argnames="pktype",
    )
    @testing.combinations(
        ("pk_%(table_name)s", "pk_t1"),
        ("pk_%(column_0_name)s", "pk_x"),
        ("pk_%(column_0_N_name)s", "pk_x_y"),
        ("pk_%(column_0_N_label)s", "pk_t1_x_t1_y"),
        ("%(column_0_name)s", "x"),
        ("%(column_0N_name)s", "xy"),
        argnames="conv, expected_name",
    )
    def test_pk_conventions(self, conv, expected_name, pktype):
        m1 = MetaData(naming_convention={"pk": conv})

        if pktype == "column":
            t1 = Table(
                "t1",
                m1,
                Column("x", Integer, primary_key=True),
                Column("y", Integer, primary_key=True),
            )
        elif pktype == "constraint":
            t1 = Table(
                "t1",
                m1,
                Column("x", Integer),
                Column("y", Integer),
                PrimaryKeyConstraint("x", "y"),
            )
        elif pktype == "nopk":
            t1 = Table(
                "t1",
                m1,
                Column("x", Integer, nullable=False),
                Column("y", Integer, nullable=False),
            )
            expected_name = None
        elif pktype == "explicit_name":
            t1 = Table(
                "t1",
                m1,
                Column("x", Integer, primary_key=True),
                Column("y", Integer, primary_key=True),
                PrimaryKeyConstraint("x", "y", name="myname"),
            )
            expected_name = "myname"

        if expected_name:
            eq_(t1.primary_key.name, expected_name)

        if pktype == "nopk":
            self.assert_compile(
                schema.CreateTable(t1),
                "CREATE TABLE t1 (x INTEGER NOT NULL, y INTEGER NOT NULL)",
            )
        else:
            self.assert_compile(
                schema.CreateTable(t1),
                "CREATE TABLE t1 (x INTEGER NOT NULL, y INTEGER NOT NULL, "
                "CONSTRAINT %s PRIMARY KEY (x, y))" % expected_name,
            )

    def test_constraint_classes_for_keys(self):
        u1 = self._fixture(
            naming_convention={
                UniqueConstraint: "uq_%(table_name)s_%(column_0_name)s"
            }
        )
        uq = UniqueConstraint(u1.c.data)
        eq_(uq.name, "uq_user_data")

    def test_uq_name(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"}
        )
        uq = UniqueConstraint(u1.c.data)
        eq_(uq.name, "uq_user_data")

    def test_uq_conv_name(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"}
        )
        uq = UniqueConstraint(u1.c.data, name=naming.conv("myname"))
        self.assert_compile(
            schema.AddConstraint(uq),
            'ALTER TABLE "user" ADD CONSTRAINT myname UNIQUE (data)',
            dialect="default",
        )

    def test_uq_defer_name_convention(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"}
        )
        uq = UniqueConstraint(u1.c.data, name=naming._NONE_NAME)
        self.assert_compile(
            schema.AddConstraint(uq),
            'ALTER TABLE "user" ADD CONSTRAINT uq_user_data UNIQUE (data)',
            dialect="default",
        )

    def test_uq_key(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_key)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2)
        eq_(uq.name, "uq_user_data")

    def test_uq_label(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_label)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2)
        eq_(uq.name, "uq_user_user_data")

    def test_uq_allcols_underscore_name(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_N_name)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2, u1.c.data3)
        eq_(uq.name, "uq_user_data_Data2_Data3")

    def test_uq_allcols_merged_name(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0N_name)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2, u1.c.data3)
        eq_(uq.name, "uq_user_dataData2Data3")

    def test_uq_allcols_merged_key(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0N_key)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2, u1.c.data3)
        eq_(uq.name, "uq_user_datadata2data3")

    def test_uq_allcols_truncated_name(self):
        u1 = self._fixture(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0N_name)s"}
        )
        uq = UniqueConstraint(u1.c.data, u1.c.data2, u1.c.data3)

        dialect = default.DefaultDialect()
        self.assert_compile(
            schema.AddConstraint(uq),
            'ALTER TABLE "user" ADD '
            'CONSTRAINT "uq_user_dataData2Data3" '
            'UNIQUE (data, "Data2", "Data3")',
            dialect=dialect,
        )

        dialect.max_identifier_length = 15
        self.assert_compile(
            schema.AddConstraint(uq),
            'ALTER TABLE "user" ADD '
            'CONSTRAINT uq_user_2769 UNIQUE (data, "Data2", "Data3")',
            dialect=dialect,
        )

    def test_fk_allcols_underscore_name(self):
        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0_N_name)s_"
                "%(referred_table_name)s_%(referred_column_0_N_name)s"
            }
        )

        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("UserData", String(30), key="user_data"),
            Column("UserData2", String(30), key="user_data2"),
            Column("UserData3", String(30), key="user_data3"),
        )
        fk = ForeignKeyConstraint(
            ["user_data", "user_data2", "user_data3"],
            ["user.data", "user.data2", "user.data3"],
        )
        a1.append_constraint(fk)
        self.assert_compile(
            schema.AddConstraint(fk),
            "ALTER TABLE address ADD CONSTRAINT "
            '"fk_address_UserData_UserData2_UserData3_user_data_Data2_Data3" '
            'FOREIGN KEY("UserData", "UserData2", "UserData3") '
            'REFERENCES "user" (data, "Data2", "Data3")',
            dialect=default.DefaultDialect(),
        )

    def test_fk_allcols_merged_name(self):
        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0N_name)s_"
                "%(referred_table_name)s_%(referred_column_0N_name)s"
            }
        )

        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("UserData", String(30), key="user_data"),
            Column("UserData2", String(30), key="user_data2"),
            Column("UserData3", String(30), key="user_data3"),
        )
        fk = ForeignKeyConstraint(
            ["user_data", "user_data2", "user_data3"],
            ["user.data", "user.data2", "user.data3"],
        )
        a1.append_constraint(fk)
        self.assert_compile(
            schema.AddConstraint(fk),
            "ALTER TABLE address ADD CONSTRAINT "
            '"fk_address_UserDataUserData2UserData3_user_dataData2Data3" '
            'FOREIGN KEY("UserData", "UserData2", "UserData3") '
            'REFERENCES "user" (data, "Data2", "Data3")',
            dialect=default.DefaultDialect(),
        )

    def test_fk_allcols_truncated_name(self):
        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0N_name)s_"
                "%(referred_table_name)s_%(referred_column_0N_name)s"
            }
        )

        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("UserData", String(30), key="user_data"),
            Column("UserData2", String(30), key="user_data2"),
            Column("UserData3", String(30), key="user_data3"),
        )
        fk = ForeignKeyConstraint(
            ["user_data", "user_data2", "user_data3"],
            ["user.data", "user.data2", "user.data3"],
        )
        a1.append_constraint(fk)

        dialect = default.DefaultDialect()
        dialect.max_identifier_length = 15
        self.assert_compile(
            schema.AddConstraint(fk),
            "ALTER TABLE address ADD CONSTRAINT "
            "fk_addr_f9ff "
            'FOREIGN KEY("UserData", "UserData2", "UserData3") '
            'REFERENCES "user" (data, "Data2", "Data3")',
            dialect=dialect,
        )

    def test_ix_allcols_truncation(self):
        u1 = self._fixture(
            naming_convention={"ix": "ix_%(table_name)s_%(column_0N_name)s"}
        )
        ix = Index(None, u1.c.data, u1.c.data2, u1.c.data3)
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = 15
        self.assert_compile(
            schema.CreateIndex(ix),
            'CREATE INDEX ix_user_2de9 ON "user" (data, "Data2", "Data3")',
            dialect=dialect,
        )

    def test_ix_name(self):
        u1 = self._fixture(
            naming_convention={"ix": "ix_%(table_name)s_%(column_0_name)s"}
        )
        ix = Index(None, u1.c.data)
        eq_(ix.name, "ix_user_data")

    def test_ck_name_required(self):
        u1 = self._fixture(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        ck = CheckConstraint(u1.c.data == "x", name="mycheck")
        eq_(ck.name, "ck_user_mycheck")

        assert_raises_message(
            exc.InvalidRequestError,
            r"Naming convention including %\(constraint_name\)s token "
            "requires that constraint is explicitly named.",
            CheckConstraint,
            u1.c.data == "x",
        )

    def test_ck_name_deferred_required(self):
        u1 = self._fixture(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        ck = CheckConstraint(u1.c.data == "x", name=naming._NONE_NAME)

        assert_raises_message(
            exc.InvalidRequestError,
            r"Naming convention including %\(constraint_name\)s token "
            "requires that constraint is explicitly named.",
            schema.AddConstraint(ck).compile,
        )

    def test_column_attached_ck_name(self):
        m = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        ck = CheckConstraint("x > 5", name="x1")
        Table("t", m, Column("x", ck))
        eq_(ck.name, "ck_t_x1")

    def test_table_attached_ck_name(self):
        m = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        ck = CheckConstraint("x > 5", name="x1")
        Table("t", m, Column("x", Integer), ck)
        eq_(ck.name, "ck_t_x1")

    def test_uq_name_already_conv(self):
        m = MetaData(
            naming_convention={
                "uq": "uq_%(constraint_name)s_%(column_0_name)s"
            }
        )

        t = Table("mytable", m)
        uq = UniqueConstraint(name=naming.conv("my_special_key"))

        t.append_constraint(uq)
        eq_(uq.name, "my_special_key")

    def test_fk_name_schema(self):
        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0_name)s_"
                "%(referred_table_name)s_%(referred_column_0_name)s"
            },
            table_schema="foo",
        )
        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("user_version_id", Integer),
        )
        fk = ForeignKeyConstraint(
            ["user_id", "user_version_id"], ["foo.user.id", "foo.user.version"]
        )
        a1.append_constraint(fk)
        eq_(fk.name, "fk_address_user_id_user_id")

    def test_fk_attrs(self):
        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0_name)s_"
                "%(referred_table_name)s_%(referred_column_0_name)s"
            }
        )
        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("user_version_id", Integer),
        )
        fk = ForeignKeyConstraint(
            ["user_id", "user_version_id"], ["user.id", "user.version"]
        )
        a1.append_constraint(fk)
        eq_(fk.name, "fk_address_user_id_user_id")

    @testing.combinations(True, False, argnames="col_has_type")
    def test_fk_ref_local_referent_has_no_type(self, col_has_type):
        """test #7958"""

        metadata = MetaData(
            naming_convention={
                "fk": "fk_%(referred_column_0_name)s",
            }
        )
        Table("a", metadata, Column("id", Integer, primary_key=True))
        b = Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            (
                Column("aid", ForeignKey("a.id"))
                if not col_has_type
                else Column("aid", Integer, ForeignKey("a.id"))
            ),
        )
        fks = list(
            c for c in b.constraints if isinstance(c, ForeignKeyConstraint)
        )
        eq_(fks[0].name, "fk_id")

    def test_custom(self):
        def key_hash(const, table):
            return "HASH_%s" % table.name

        u1 = self._fixture(
            naming_convention={
                "fk": "fk_%(table_name)s_%(key_hash)s",
                "key_hash": key_hash,
            }
        )
        m1 = u1.metadata
        a1 = Table(
            "address",
            m1,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("user_version_id", Integer),
        )
        fk = ForeignKeyConstraint(
            ["user_id", "user_version_id"], ["user.id", "user.version"]
        )
        a1.append_constraint(fk)
        eq_(fk.name, "fk_address_HASH_address")

    def test_schematype_ck_name_boolean(self):
        m1 = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )

        u1 = Table(
            "user",
            m1,
            Column("x", Boolean(name="foo", create_constraint=True)),
        )

        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x BOOLEAN, "
            "CONSTRAINT ck_user_foo CHECK (x IN (0, 1))"
            ")",
        )

        # test no side effects from first compile
        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x BOOLEAN, "
            "CONSTRAINT ck_user_foo CHECK (x IN (0, 1))"
            ")",
        )

    def test_schematype_ck_name_boolean_not_on_name(self):
        m1 = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(column_0_name)s"}
        )

        u1 = Table("user", m1, Column("x", Boolean(create_constraint=True)))
        # constraint is not hit
        is_(
            [c for c in u1.constraints if isinstance(c, CheckConstraint)][
                0
            ].name,
            _NONE_NAME,
        )
        # but is hit at compile time
        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x BOOLEAN, "
            "CONSTRAINT ck_user_x CHECK (x IN (0, 1))"
            ")",
        )

    def test_schematype_ck_name_enum(self):
        m1 = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )

        u1 = Table(
            "user",
            m1,
            Column("x", Enum("a", "b", name="foo", create_constraint=True)),
        )

        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x VARCHAR(1), "
            "CONSTRAINT ck_user_foo CHECK (x IN ('a', 'b'))"
            ")",
        )

        # test no side effects from first compile
        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x VARCHAR(1), "
            "CONSTRAINT ck_user_foo CHECK (x IN ('a', 'b'))"
            ")",
        )

    def test_schematype_ck_name_propagate_conv(self):
        m1 = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )

        u1 = Table(
            "user",
            m1,
            Column(
                "x",
                Enum(
                    "a", "b", name=naming.conv("foo"), create_constraint=True
                ),
            ),
        )
        eq_(
            [c for c in u1.constraints if isinstance(c, CheckConstraint)][
                0
            ].name,
            "foo",
        )
        # but is hit at compile time
        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" ('
            "x VARCHAR(1), "
            "CONSTRAINT foo CHECK (x IN ('a', 'b'))"
            ")",
        )

    def test_schematype_ck_name_boolean_no_name(self):
        m1 = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )

        u1 = Table("user", m1, Column("x", Boolean(create_constraint=True)))
        # constraint gets special _defer_none_name
        is_(
            [c for c in u1.constraints if isinstance(c, CheckConstraint)][
                0
            ].name,
            _NONE_NAME,
        )
        # no issue with native boolean
        self.assert_compile(
            schema.CreateTable(u1),
            """CREATE TABLE "user" (x BOOLEAN)""",
            dialect="postgresql",
        )

        assert_raises_message(
            exc.InvalidRequestError,
            r"Naming convention including \%\(constraint_name\)s token "
            r"requires that constraint is explicitly named.",
            schema.CreateTable(u1).compile,
            dialect=default.DefaultDialect(),
        )

    def test_schematype_no_ck_name_boolean_no_name(self):
        m1 = MetaData()  # no naming convention

        u1 = Table("user", m1, Column("x", Boolean(create_constraint=True)))
        # constraint gets special _defer_none_name
        is_(
            [c for c in u1.constraints if isinstance(c, CheckConstraint)][
                0
            ].name,
            _NONE_NAME,
        )

        self.assert_compile(
            schema.CreateTable(u1),
            'CREATE TABLE "user" (x BOOLEAN, CHECK (x IN (0, 1)))',
        )

    def test_ck_constraint_redundant_event(self):
        u1 = self._fixture(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )

        ck1 = CheckConstraint(u1.c.version > 3, name="foo")
        u1.append_constraint(ck1)
        u1.append_constraint(ck1)
        u1.append_constraint(ck1)

        eq_(ck1.name, "ck_user_foo")

    def test_pickle_metadata(self):
        m = MetaData(naming_convention={"pk": "%(table_name)s_pk"})

        m2 = pickle.loads(pickle.dumps(m))

        eq_(m2.naming_convention, {"pk": "%(table_name)s_pk"})

        t2a = Table("t2", m, Column("id", Integer, primary_key=True))
        t2b = Table("t2", m2, Column("id", Integer, primary_key=True))

        eq_(t2a.primary_key.name, t2b.primary_key.name)
        eq_(t2b.primary_key.name, "t2_pk")

    def test_expression_index(self):
        m = MetaData(naming_convention={"ix": "ix_%(column_0_label)s"})
        t = Table("t", m, Column("q", Integer), Column("p", Integer))
        ix = Index(None, t.c.q + 5)
        t.append_constraint(ix)

        # huh.  pretty cool
        self.assert_compile(
            CreateIndex(ix), "CREATE INDEX ix_t_q ON t (q + 5)"
        )


class CopyDialectOptionsTest(fixtures.TestBase):
    @contextmanager
    def _fixture(self):
        from sqlalchemy.engine.default import DefaultDialect

        class CopyDialectOptionsTestDialect(DefaultDialect):
            construct_arguments = [
                (Table, {"some_table_arg": None}),
                (Column, {"some_column_arg": None}),
                (Index, {"some_index_arg": None}),
                (PrimaryKeyConstraint, {"some_pk_arg": None}),
                (UniqueConstraint, {"some_uq_arg": None}),
            ]

        def load(dialect_name):
            if dialect_name == "copydialectoptionstest":
                return CopyDialectOptionsTestDialect
            else:
                raise exc.NoSuchModuleError("no dialect %r" % dialect_name)

        with mock.patch("sqlalchemy.dialects.registry.load", load):
            yield

    @classmethod
    def check_dialect_options_(cls, t):
        eq_(
            t.dialect_kwargs["copydialectoptionstest_some_table_arg"],
            "a1",
        )
        eq_(
            t.c.foo.dialect_kwargs["copydialectoptionstest_some_column_arg"],
            "a2",
        )
        eq_(
            t.primary_key.dialect_kwargs["copydialectoptionstest_some_pk_arg"],
            "a3",
        )
        eq_(
            list(t.indexes)[0].dialect_kwargs[
                "copydialectoptionstest_some_index_arg"
            ],
            "a4",
        )
        eq_(
            list(c for c in t.constraints if isinstance(c, UniqueConstraint))[
                0
            ].dialect_kwargs["copydialectoptionstest_some_uq_arg"],
            "a5",
        )

    def test_dialect_options_are_copied(self):
        with self._fixture():
            t1 = Table(
                "t",
                MetaData(),
                Column(
                    "foo",
                    Integer,
                    copydialectoptionstest_some_column_arg="a2",
                ),
                Column("bar", Integer),
                PrimaryKeyConstraint(
                    "foo", copydialectoptionstest_some_pk_arg="a3"
                ),
                UniqueConstraint(
                    "bar", copydialectoptionstest_some_uq_arg="a5"
                ),
                copydialectoptionstest_some_table_arg="a1",
            )
            Index(
                "idx",
                t1.c.foo,
                copydialectoptionstest_some_index_arg="a4",
            )

            self.check_dialect_options_(t1)

            m2 = MetaData()
            t2 = t1.to_metadata(m2)  # make a copy
            self.check_dialect_options_(t2)


class SentinelColTest(fixtures.TestBase):
    def make_table_w_sentinel_col(self, *arg, **kw):
        return Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column(*arg, **kw),
        )

    def test_only_one_sentinel(self):
        with expect_raises_message(
            exc.ArgumentError,
            "a Table may have only one explicit sentinel column",
        ):
            Table(
                "t",
                MetaData(),
                Column("id", Integer, primary_key=True, insert_sentinel=True),
                Column("ASdf", String(50)),
                insert_sentinel("sentinel"),
            )

    def test_no_sentinel_default_on_notnull(self):
        with expect_raises_message(
            exc.ArgumentError,
            "The _InsertSentinelColumnDefault may only be applied to a "
            "Column that is nullable",
        ):
            self.make_table_w_sentinel_col(
                "sentinel",
                Integer,
                nullable=False,
                insert_sentinel=True,
                default=_InsertSentinelColumnDefault(),
            )

    def test_no_sentinel_default_on_non_sentinel(self):
        with expect_raises_message(
            exc.ArgumentError,
            "The _InsertSentinelColumnDefault may only be applied to a "
            "Column marked as insert_sentinel=True",
        ):
            self.make_table_w_sentinel_col(
                "sentinel",
                Integer,
                default=_InsertSentinelColumnDefault(),
            )
