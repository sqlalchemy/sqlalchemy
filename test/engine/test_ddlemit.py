from sqlalchemy.testing import fixtures
from sqlalchemy.engine.ddl import SchemaGenerator, SchemaDropper
from sqlalchemy.engine import default
from sqlalchemy import MetaData, Table, Column, Integer, Sequence
from sqlalchemy import schema

class EmitDDLTest(fixtures.TestBase):
    def _mock_connection(self, item_exists):
        _canary = []

        class MockDialect(default.DefaultDialect):
            supports_sequences = True

            def has_table(self, connection, name, schema):
                return item_exists(name)

            def has_sequence(self, connection, name, schema):
                return item_exists(name)

        class MockConnection(object):
            dialect = MockDialect()
            canary = _canary

            def execute(self, item):
                _canary.append(item)

        return MockConnection()

    def _mock_create_fixture(self, checkfirst, tables,
                    item_exists=lambda item: False):
        connection = self._mock_connection(item_exists)

        return SchemaGenerator(connection.dialect, connection,
                    checkfirst=checkfirst,
                    tables=tables)

    def _mock_drop_fixture(self, checkfirst, tables,
                    item_exists=lambda item: True):
        connection = self._mock_connection(item_exists)

        return SchemaDropper(connection.dialect, connection,
                    checkfirst=checkfirst,
                    tables=tables)

    def _table_fixture(self):
        m = MetaData()

        return (m, ) + tuple(
                            Table('t%d' % i, m, Column('x', Integer))
                            for i in xrange(1, 6)
                        )

    def _table_seq_fixture(self):
        m = MetaData()

        s1 = Sequence('s1')
        s2 = Sequence('s2')
        t1 = Table('t1', m, Column("x", Integer, s1, primary_key=True))
        t2 = Table('t2', m, Column("x", Integer, s2, primary_key=True))

        return m, t1, t2, s1, s2


    def test_create_seq_checkfirst(self):
        m, t1, t2, s1, s2 = self._table_seq_fixture()
        generator = self._mock_create_fixture(True, [t1, t2],
                        item_exists=lambda t: t not in ("t1", "s1")
                        )

        self._assert_create([t1, s1], generator, m)


    def test_drop_seq_checkfirst(self):
        m, t1, t2, s1, s2 = self._table_seq_fixture()
        generator = self._mock_drop_fixture(True, [t1, t2],
                        item_exists=lambda t: t in ("t1", "s1")
                        )

        self._assert_drop([t1, s1], generator, m)

    def test_create_collection_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(True, [t2, t3, t4],
                        item_exists=lambda t: t not in ("t2", "t4")
                        )

        self._assert_create_tables([t2, t4], generator, m)

    def test_drop_collection_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(True, [t2, t3, t4],
                        item_exists=lambda t: t in ("t2", "t4")
                        )

        self._assert_drop_tables([t2, t4], generator, m)

    def test_create_collection_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(False, [t2, t3, t4],
                        item_exists=lambda t: t not in ("t2", "t4")
                        )

        self._assert_create_tables([t2, t3, t4], generator, m)

    def test_create_empty_collection(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(True, [],
                        item_exists=lambda t: t not in ("t2", "t4")
                        )

        self._assert_create_tables([], generator, m)

    def test_drop_empty_collection(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(True, [],
                        item_exists=lambda t: t in ("t2", "t4")
                        )

        self._assert_drop_tables([], generator, m)

    def test_drop_collection_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(False, [t2, t3, t4],
                        item_exists=lambda t: t in ("t2", "t4")
                        )

        self._assert_drop_tables([t2, t3, t4], generator, m)

    def test_create_metadata_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(True, None,
                        item_exists=lambda t: t not in ("t2", "t4")
                        )

        self._assert_create_tables([t2, t4], generator, m)

    def test_drop_metadata_checkfirst(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(True, None,
                        item_exists=lambda t: t in ("t2", "t4")
                        )

        self._assert_drop_tables([t2, t4], generator, m)

    def test_create_metadata_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_create_fixture(False, None,
                        item_exists=lambda t: t not in ("t2", "t4")
                        )

        self._assert_create_tables([t1, t2, t3, t4, t5], generator, m)

    def test_drop_metadata_nocheck(self):
        m, t1, t2, t3, t4, t5 = self._table_fixture()
        generator = self._mock_drop_fixture(False, None,
                        item_exists=lambda t: t in ("t2", "t4")
                        )

        self._assert_drop_tables([t1, t2, t3, t4, t5], generator, m)

    def _assert_create_tables(self, elements, generator, argument):
        self._assert_ddl(schema.CreateTable, elements, generator, argument)

    def _assert_drop_tables(self, elements, generator, argument):
        self._assert_ddl(schema.DropTable, elements, generator, argument)

    def _assert_create(self, elements, generator, argument):
        self._assert_ddl(
                (schema.CreateTable, schema.CreateSequence),
                elements, generator, argument)

    def _assert_drop(self, elements, generator, argument):
        self._assert_ddl(
                (schema.DropTable, schema.DropSequence),
                elements, generator, argument)

    def _assert_ddl(self, ddl_cls, elements, generator, argument):
        generator.traverse_single(argument)
        for c in generator.connection.canary:
            assert isinstance(c, ddl_cls)
            assert c.element in elements, "element %r was not expected"\
                             % c.element
            elements.remove(c.element)
        assert not elements, "elements remain in list: %r" % elements
