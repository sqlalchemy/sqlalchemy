import sqlalchemy as sa
from sqlalchemy import Computed
from sqlalchemy import event
from sqlalchemy import Identity
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import mapper
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import assert_engine
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class TriggerDefaultsTest(fixtures.MappedTest):
    __requires__ = ("row_triggers",)

    @classmethod
    def define_tables(cls, metadata):
        dt = Table(
            "dt",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("col1", String(20)),
            Column(
                "col2", String(20), server_default=sa.schema.FetchedValue()
            ),
            Column(
                "col3", String(20), sa.schema.FetchedValue(for_update=True)
            ),
            Column(
                "col4",
                String(20),
                sa.schema.FetchedValue(),
                sa.schema.FetchedValue(for_update=True),
            ),
        )

        dialect_name = testing.db.dialect.name

        for ins in (
            sa.DDL(
                "CREATE TRIGGER dt_ins AFTER INSERT ON dt "
                "FOR EACH ROW BEGIN "
                "UPDATE dt SET col2='ins', col4='ins' "
                "WHERE dt.id = NEW.id; END"
            ).execute_if(dialect="sqlite"),
            sa.DDL(
                "CREATE TRIGGER dt_ins ON dt AFTER INSERT AS "
                "UPDATE dt SET col2='ins', col4='ins' "
                "WHERE dt.id IN (SELECT id FROM inserted);"
            ).execute_if(dialect="mssql"),
            sa.DDL(
                "CREATE TRIGGER dt_ins BEFORE INSERT "
                "ON dt "
                "FOR EACH ROW "
                "BEGIN "
                ":NEW.col2 := 'ins'; :NEW.col4 := 'ins'; END;"
            ).execute_if(dialect="oracle"),
            sa.DDL(
                "CREATE TRIGGER dt_ins BEFORE INSERT "
                "ON dt "
                "FOR EACH ROW "
                "EXECUTE PROCEDURE my_func_ins();"
            ).execute_if(dialect="postgresql"),
            sa.DDL(
                "CREATE TRIGGER dt_ins BEFORE INSERT ON dt "
                "FOR EACH ROW BEGIN "
                "SET NEW.col2='ins'; SET NEW.col4='ins'; END"
            ).execute_if(
                callable_=lambda ddl, target, bind, **kw: bind.engine.name
                not in ("oracle", "mssql", "sqlite", "postgresql")
            ),
        ):
            my_func_ins = sa.DDL(
                "CREATE OR REPLACE FUNCTION my_func_ins() "
                "RETURNS TRIGGER AS $$ "
                "BEGIN "
                "NEW.col2 := 'ins'; NEW.col4 := 'ins'; "
                "RETURN NEW; "
                "END; $$ LANGUAGE PLPGSQL"
            ).execute_if(dialect="postgresql")
            event.listen(dt, "after_create", my_func_ins)

            event.listen(dt, "after_create", ins)
        if dialect_name == "postgresql":
            event.listen(
                dt, "before_drop", sa.DDL("DROP TRIGGER dt_ins ON dt")
            )
        else:
            event.listen(dt, "before_drop", sa.DDL("DROP TRIGGER dt_ins"))

        for up in (
            sa.DDL(
                "CREATE TRIGGER dt_up AFTER UPDATE ON dt "
                "FOR EACH ROW BEGIN "
                "UPDATE dt SET col3='up', col4='up' "
                "WHERE dt.id = OLD.id; END"
            ).execute_if(dialect="sqlite"),
            sa.DDL(
                "CREATE TRIGGER dt_up ON dt AFTER UPDATE AS "
                "UPDATE dt SET col3='up', col4='up' "
                "WHERE dt.id IN (SELECT id FROM deleted);"
            ).execute_if(dialect="mssql"),
            sa.DDL(
                "CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                "FOR EACH ROW BEGIN "
                ":NEW.col3 := 'up'; :NEW.col4 := 'up'; END;"
            ).execute_if(dialect="oracle"),
            sa.DDL(
                "CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                "FOR EACH ROW "
                "EXECUTE PROCEDURE my_func_up();"
            ).execute_if(dialect="postgresql"),
            sa.DDL(
                "CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                "FOR EACH ROW BEGIN "
                "SET NEW.col3='up'; SET NEW.col4='up'; END"
            ).execute_if(
                callable_=lambda ddl, target, bind, **kw: bind.engine.name
                not in ("oracle", "mssql", "sqlite", "postgresql")
            ),
        ):
            my_func_up = sa.DDL(
                "CREATE OR REPLACE FUNCTION my_func_up() "
                "RETURNS TRIGGER AS $$ "
                "BEGIN "
                "NEW.col3 := 'up'; NEW.col4 := 'up'; "
                "RETURN NEW; "
                "END; $$ LANGUAGE PLPGSQL"
            ).execute_if(dialect="postgresql")
            event.listen(dt, "after_create", my_func_up)

            event.listen(dt, "after_create", up)

        if dialect_name == "postgresql":
            event.listen(dt, "before_drop", sa.DDL("DROP TRIGGER dt_up ON dt"))
        else:
            event.listen(dt, "before_drop", sa.DDL("DROP TRIGGER dt_up"))

    @classmethod
    def setup_classes(cls):
        class Default(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Default, dt = cls.classes.Default, cls.tables.dt

        mapper(Default, dt)

    def test_insert(self):
        Default = self.classes.Default

        d1 = Default(id=1)

        eq_(d1.col1, None)
        eq_(d1.col2, None)
        eq_(d1.col3, None)
        eq_(d1.col4, None)

        session = fixture_session()
        session.add(d1)
        session.flush()

        eq_(d1.col1, None)
        eq_(d1.col2, "ins")
        eq_(d1.col3, None)
        # don't care which trigger fired
        assert d1.col4 in ("ins", "up")

    def test_update(self):
        Default = self.classes.Default

        d1 = Default(id=1)

        session = fixture_session()
        session.add(d1)
        session.flush()
        d1.col1 = "set"
        session.flush()

        eq_(d1.col1, "set")
        eq_(d1.col2, "ins")
        eq_(d1.col3, "up")
        eq_(d1.col4, "up")


class ExcludedDefaultsTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "dt",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("col1", String(20), default="hello"),
        )

    def test_exclude(self):
        dt = self.tables.dt

        class Foo(fixtures.BasicEntity):
            pass

        mapper(Foo, dt, exclude_properties=("col1",))

        f1 = Foo()
        sess = fixture_session()
        sess.add(f1)
        sess.flush()
        eq_(sess.connection().execute(dt.select()).fetchall(), [(1, "hello")])


class ComputedDefaultsOnUpdateTest(fixtures.MappedTest):
    """test that computed columns are recognized as server
    oninsert/onupdate defaults."""

    __backend__ = True
    __requires__ = ("computed_columns",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, Computed("foo + 42")),
        )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

        class ThingNoEager(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing

        mapper(Thing, cls.tables.test, eager_defaults=True)

        ThingNoEager = cls.classes.ThingNoEager
        mapper(ThingNoEager, cls.tables.test, eager_defaults=False)

    @testing.combinations(("eager", True), ("noneager", False), id_="ia")
    def test_insert_computed(self, eager):
        if eager:
            Thing = self.classes.Thing
        else:
            Thing = self.classes.ThingNoEager

        s = fixture_session()

        t1, t2 = (Thing(id=1, foo=5), Thing(id=2, foo=10))

        s.add_all([t1, t2])

        with assert_engine(testing.db) as asserter:
            s.flush()
            eq_(t1.bar, 5 + 42)
            eq_(t2.bar, 10 + 42)

        asserter.assert_(
            Conditional(
                eager and testing.db.dialect.implicit_returning,
                [
                    Conditional(
                        testing.db.dialect.insert_executemany_returning,
                        [
                            CompiledSQL(
                                "INSERT INTO test (id, foo) "
                                "VALUES (%(id)s, %(foo)s) "
                                "RETURNING test.bar",
                                [{"foo": 5, "id": 1}, {"foo": 10, "id": 2}],
                                dialect="postgresql",
                            ),
                        ],
                        [
                            CompiledSQL(
                                "INSERT INTO test (id, foo) "
                                "VALUES (%(id)s, %(foo)s) "
                                "RETURNING test.bar",
                                [{"foo": 5, "id": 1}],
                                dialect="postgresql",
                            ),
                            CompiledSQL(
                                "INSERT INTO test (id, foo) "
                                "VALUES (%(id)s, %(foo)s) "
                                "RETURNING test.bar",
                                [{"foo": 10, "id": 2}],
                                dialect="postgresql",
                            ),
                        ],
                    )
                ],
                [
                    CompiledSQL(
                        "INSERT INTO test (id, foo) VALUES (:id, :foo)",
                        [{"foo": 5, "id": 1}, {"foo": 10, "id": 2}],
                    ),
                    CompiledSQL(
                        "SELECT test.bar AS test_bar FROM test "
                        "WHERE test.id = :pk_1",
                        [{"pk_1": 1}],
                    ),
                    CompiledSQL(
                        "SELECT test.bar AS test_bar FROM test "
                        "WHERE test.id = :pk_1",
                        [{"pk_1": 2}],
                    ),
                ],
            )
        )

    @testing.combinations(
        (
            "eagerload",
            True,
            testing.requires.computed_columns_on_update_returning,
        ),
        (
            "noneagerload",
            False,
        ),
        id_="ia",
    )
    def test_update_computed(self, eager):
        if eager:
            Thing = self.classes.Thing
        else:
            Thing = self.classes.ThingNoEager

        s = fixture_session()

        t1, t2 = (Thing(id=1, foo=1), Thing(id=2, foo=2))

        s.add_all([t1, t2])
        s.flush()

        t1.foo = 5
        t2.foo = 6

        with assert_engine(testing.db) as asserter:
            s.flush()
            eq_(t1.bar, 5 + 42)
            eq_(t2.bar, 6 + 42)

        if eager and testing.db.dialect.implicit_returning:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE test SET foo=%(foo)s "
                    "WHERE test.id = %(test_id)s "
                    "RETURNING test.bar",
                    [{"foo": 5, "test_id": 1}],
                    dialect="postgresql",
                ),
                CompiledSQL(
                    "UPDATE test SET foo=%(foo)s "
                    "WHERE test.id = %(test_id)s "
                    "RETURNING test.bar",
                    [{"foo": 6, "test_id": 2}],
                    dialect="postgresql",
                ),
            )
        elif eager:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE test SET foo=:foo WHERE test.id = :test_id",
                    [{"foo": 5, "test_id": 1}],
                ),
                CompiledSQL(
                    "UPDATE test SET foo=:foo WHERE test.id = :test_id",
                    [{"foo": 6, "test_id": 2}],
                ),
                CompiledSQL(
                    "SELECT test.bar AS test_bar FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 1}],
                ),
                CompiledSQL(
                    "SELECT test.bar AS test_bar FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 2}],
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE test SET foo=:foo WHERE test.id = :test_id",
                    [{"foo": 5, "test_id": 1}, {"foo": 6, "test_id": 2}],
                ),
                CompiledSQL(
                    "SELECT test.bar AS test_bar FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 1}],
                ),
                CompiledSQL(
                    "SELECT test.bar AS test_bar FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 2}],
                ),
            )


class IdentityDefaultsOnUpdateTest(fixtures.MappedTest):
    """test that computed columns are recognized as server
    oninsert/onupdate defaults."""

    __backend__ = True
    __requires__ = ("identity_columns",)
    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, Identity(), primary_key=True),
            Column("foo", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing

        mapper(Thing, cls.tables.test)

    def test_insert_identity(self):
        Thing = self.classes.Thing

        s = fixture_session()

        t1, t2 = (Thing(foo=5), Thing(foo=10))

        s.add_all([t1, t2])

        with assert_engine(testing.db) as asserter:
            s.flush()
            eq_(t1.id, 1)
            eq_(t2.id, 2)

        asserter.assert_(
            Conditional(
                testing.db.dialect.implicit_returning,
                [
                    Conditional(
                        testing.db.dialect.insert_executemany_returning,
                        [
                            CompiledSQL(
                                "INSERT INTO test (foo) VALUES (%(foo)s) "
                                "RETURNING test.id",
                                [{"foo": 5}, {"foo": 10}],
                                dialect="postgresql",
                            ),
                        ],
                        [
                            CompiledSQL(
                                "INSERT INTO test (foo) VALUES (%(foo)s) "
                                "RETURNING test.id",
                                [{"foo": 5}],
                                dialect="postgresql",
                            ),
                            CompiledSQL(
                                "INSERT INTO test (foo) VALUES (%(foo)s) "
                                "RETURNING test.id",
                                [{"foo": 10}],
                                dialect="postgresql",
                            ),
                        ],
                    )
                ],
                [
                    CompiledSQL(
                        "INSERT INTO test (foo) VALUES (:foo)",
                        [{"foo": 5}],
                    ),
                    CompiledSQL(
                        "INSERT INTO test (foo) VALUES (:foo)",
                        [{"foo": 10}],
                    ),
                ],
            )
        )
