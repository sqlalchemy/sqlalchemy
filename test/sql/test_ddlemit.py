from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import schema
from sqlalchemy import Sequence
from sqlalchemy import Table
from sqlalchemy.sql.ddl import SchemaDropper
from sqlalchemy.sql.ddl import SchemaGenerator
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.mock import Mock


class EmitDDLTest(fixtures.TestBase):
    def _mock_connection(self, item_exists):
        def has_item(connection, name, schema):
            return item_exists(name)

        def has_index(connection, tablename, idxname, schema):
            return item_exists(idxname)

        return Mock(
            dialect=Mock(
                supports_sequences=True,
                has_table=Mock(side_effect=has_item),
                has_sequence=Mock(side_effect=has_item),
                has_index=Mock(side_effect=has_index),
                supports_comments=True,
                inline_comments=False,
            ),
            _schema_translate_map=None,
        )

    def _mock_create_fixture(
        self, checkfirst, tables, item_exists=lambda item: False
    ):
        connection = self._mock_connection(item_exists)

        return SchemaGenerator(
            connection.dialect,
            connection,
            checkfirst=checkfirst,
            tables=tables,
        )

    def _mock_drop_fixture(
        self, checkfirst, tables, item_exists=lambda item: True
    ):
        connection = self._mock_connection(item_exists)

        return SchemaDropper(
            connection.dialect,
            connection,
            checkfirst=checkfirst,
            tables=tables,
        )

    def _table_fixture(self):
        m = MetaData()

        return (m,) + tuple(
            Table("t%d" % i, m, Column("x", Integer)) for i in range(1, 6)
        )

    def _use_alter_fixture_one(self):
        m = MetaData()

        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )
        return m, t1, t2

    def _fk_fixture_one(self):
        m = MetaData()

        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )
        t2 = Table("t2", m, Column("id", Integer, primary_key=True))
        return m, t1, t2

    def _table_index_fixture(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer), Column("y", Integer))
        i1 = Index("my_idx", t1.c.x, t1.c.y)
        return m, t1, i1

    def _table_seq_fixture(self):
        m = MetaData()

        s1 = Sequence("s1")
        s2 = Sequence("s2")
        t1 = Table("t1", m, Column("x", Integer, s1, primary_key=True))
        t2 = Table("t2", m, Column("x", Integer, s2, primary_key=True))

        return m, t1, t2, s1, s2

    def _table_comment_fixture(self):
        m = MetaData()

        c1 = Column("id", Integer, comment="c1")

        t1 = Table("t1", m, c1, comment="t1")

        return m, t1, c1

    def test_comment(self):
        m, t1, c1 = self._table_comment_fixture()

        generator = self._mock_create_fixture(
            False, [t1], item_exists=lambda t: t not in ("t1",)
        )

        self._assert_create_comment([t1, t1, c1], generator, m)

    def test_create_seq_checkfirst(self):
        m, t1, t2, s1, s2 = self._table_seq_fixture()
        generator = self._mock_create_fixture(
            True, [t1, t2], item_exists=lambda t: t not in ("t1", "s1")
        )

        self._assert_create([t1, s1], generator, m)

    def test_drop_seq_checkfirst(self):
        m, t1, t2, s1, s2 = self._table_seq_fixture()
        generator = self._mock_drop_fixture(
            True, [t1, t2], item_exists=lambda t: t in ("t1", "s1")
        )

        self._assert_drop([t1, s1], generator, m)

    def test_create_table_index_checkfirst(self):
        """create table that doesn't exist should not require a check
        on the index"""

        m, t1, i1 = self._table_index_fixture()

        def exists(name):
            if name == "my_idx":
                raise NotImplementedError()
            else:
                return False

        generator = self._mock_create_fixture(True, [t1], item_exists=exists)
        self._assert_create([t1, i1], generator, t1)

    def test_create_table_exists_index_checkfirst(self):
        """for the moment, if the table *does* exist, we are not checking
        for the index.  this can possibly be changed."""

        m, t1, i1 = self._table_index_fixture()

        def exists(name):
            if name == "my_idx":
                raise NotImplementedError()
            else:
                return True

        generator = self._mock_create_fixture(True, [t1], item_exists=exists)
        # nothing is created
        self._assert_create([], generator, t1)

    def test_drop_table_index_checkfirst(self):
        m, t1, i1 = self._table_index_fixture()

        def exists(name):
            if name == "my_idx":
                raise NotImplementedError()
            else:
                return True

        generator = self._mock_drop_fixture(True, [t1], item_exists=exists)
        self._assert_drop_tables([t1], generator, t1)

    def test_create_index_checkfirst_exists(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_create_fixture(
            True, [i1], item_exists=lambda idx: True
        )
        self._assert_create_index([], generator, i1)

    def test_create_index_checkfirst_doesnt_exist(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_create_fixture(
            True, [i1], item_exists=lambda idx: False
        )
        self._assert_create_index([i1], generator, i1)

    def test_create_index_nocheck_exists(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_create_fixture(
            False, [i1], item_exists=lambda idx: True
        )
        self._assert_create_index([i1], generator, i1)

    def test_create_index_nocheck_doesnt_exist(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_create_fixture(
            False, [i1], item_exists=lambda idx: False
        )
        self._assert_create_index([i1], generator, i1)

    def test_drop_index_checkfirst_exists(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_drop_fixture(
            True, [i1], item_exists=lambda idx: True
        )
        self._assert_drop_index([i1], generator, i1)

    def test_drop_index_checkfirst_doesnt_exist(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_drop_fixture(
            True, [i1], item_exists=lambda idx: False
        )
        self._assert_drop_index([], generator, i1)

    def test_drop_index_nocheck_exists(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_drop_fixture(
            False, [i1], item_exists=lambda idx: True
        )
        self._assert_drop_index([i1], generator, i1)

    def test_drop_index_nocheck_doesnt_exist(self):
        m, t1, i1 = self._table_index_fixture()
        generator = self._mock_drop_fixture(
            False, [i1], item_exists=lambda idx: False
        )
        self._assert_drop_index([i1], generator, i1)

    def test_create_collection_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(
            True, [t2, t3, t4], item_exists=lambda t: t not in ("t2", "t4")
        )

        self._assert_create_tables([t2, t4], generator, m)

    def test_drop_collection_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(
            True, [t2, t3, t4], item_exists=lambda t: t in ("t2", "t4")
        )

        self._assert_drop_tables([t2, t4], generator, m)

    def test_create_collection_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(
            False, [t2, t3, t4], item_exists=lambda t: t not in ("t2", "t4")
        )

        self._assert_create_tables([t2, t3, t4], generator, m)

    def test_create_empty_collection(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(
            True, [], item_exists=lambda t: t not in ("t2", "t4")
        )

        self._assert_create_tables([], generator, m)

    def test_drop_empty_collection(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(
            True, [], item_exists=lambda t: t in ("t2", "t4")
        )

        self._assert_drop_tables([], generator, m)

    def test_drop_collection_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(
            False, [t2, t3, t4], item_exists=lambda t: t in ("t2", "t4")
        )

        self._assert_drop_tables([t2, t3, t4], generator, m)

    def test_create_metadata_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(
            True, None, item_exists=lambda t: t not in ("t2", "t4")
        )

        self._assert_create_tables([t2, t4], generator, m)

    def test_drop_metadata_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(
            True, None, item_exists=lambda t: t in ("t2", "t4")
        )

        self._assert_drop_tables([t2, t4], generator, m)

    def test_create_metadata_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(
            False, None, item_exists=lambda t: t not in ("t2", "t4")
        )

        self._assert_create_tables([t1, t2, t3, t4, t5], generator, m)

    def test_drop_metadata_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(
            False, None, item_exists=lambda t: t in ("t2", "t4")
        )

        self._assert_drop_tables([t1, t2, t3, t4, t5], generator, m)

    def test_create_metadata_auto_alter_fk(self):
        m, t1, t2 = self._use_alter_fixture_one()
        generator = self._mock_create_fixture(False, [t1, t2])
        self._assert_create_w_alter(
            [t1, t2]
            + list(t1.foreign_key_constraints)
            + list(t2.foreign_key_constraints),
            generator,
            m,
        )

    def test_create_metadata_inline_fk(self):
        m, t1, t2 = self._fk_fixture_one()
        generator = self._mock_create_fixture(False, [t1, t2])
        self._assert_create_w_alter(
            [t1, t2]
            + list(t1.foreign_key_constraints)
            + list(t2.foreign_key_constraints),
            generator,
            m,
        )

    def _assert_create_tables(self, elements, generator, argument):
        self._assert_ddl(schema.CreateTable, elements, generator, argument)

    def _assert_drop_tables(self, elements, generator, argument):
        self._assert_ddl(schema.DropTable, elements, generator, argument)

    def _assert_create(self, elements, generator, argument):
        self._assert_ddl(
            (schema.CreateTable, schema.CreateSequence, schema.CreateIndex),
            elements,
            generator,
            argument,
        )

    def _assert_drop(self, elements, generator, argument):
        self._assert_ddl(
            (schema.DropTable, schema.DropSequence),
            elements,
            generator,
            argument,
        )

    def _assert_create_w_alter(self, elements, generator, argument):
        self._assert_ddl(
            (schema.CreateTable, schema.CreateSequence, schema.AddConstraint),
            elements,
            generator,
            argument,
        )

    def _assert_drop_w_alter(self, elements, generator, argument):
        self._assert_ddl(
            (schema.DropTable, schema.DropSequence, schema.DropConstraint),
            elements,
            generator,
            argument,
        )

    def _assert_create_comment(self, elements, generator, argument):
        self._assert_ddl(
            (
                schema.CreateTable,
                schema.SetTableComment,
                schema.SetColumnComment,
            ),
            elements,
            generator,
            argument,
        )

    def _assert_create_index(self, elements, generator, argument):
        self._assert_ddl((schema.CreateIndex,), elements, generator, argument)

    def _assert_drop_index(self, elements, generator, argument):
        self._assert_ddl((schema.DropIndex,), elements, generator, argument)

    def _assert_ddl(self, ddl_cls, elements, generator, argument):
        generator.traverse_single(argument)
        for call_ in generator.connection.execute.mock_calls:
            c = call_[1][0]
            assert isinstance(c, ddl_cls)
            assert c.element in elements, (
                "element %r was not expected" % c.element
            )
            elements.remove(c.element)
            if getattr(c, "include_foreign_key_constraints", None) is not None:
                elements[:] = [
                    e
                    for e in elements
                    if e not in set(c.include_foreign_key_constraints)
                ]
        assert not elements, "elements remain in list: %r" % elements
