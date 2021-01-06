"""Generic mapping to Select statements"""
import sqlalchemy as sa
from sqlalchemy import column
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


# TODO: more tests mapping to selects


class SelectableNoFromsTest(fixtures.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "common",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", Integer),
            Column("extra", String(45)),
        )

    @classmethod
    def setup_classes(cls):
        class Subset(cls.Comparable):
            pass

    def test_no_tables(self):
        Subset = self.classes.Subset

        selectable = select(column("x"), column("y"), column("z")).alias()
        mapper(Subset, selectable, primary_key=[selectable.c.x])

        self.assert_compile(
            fixture_session().query(Subset),
            "SELECT anon_1.x AS anon_1_x, anon_1.y AS anon_1_y, "
            "anon_1.z AS anon_1_z FROM (SELECT x, y, z) AS anon_1",
            use_default_dialect=True,
        )

    def test_no_table_needs_pl(self):
        Subset = self.classes.Subset

        selectable = select(column("x"), column("y"), column("z")).alias()
        assert_raises_message(
            sa.exc.ArgumentError,
            "could not assemble any primary key columns",
            mapper,
            Subset,
            selectable,
        )

    def test_no_selects(self):
        Subset, common = self.classes.Subset, self.tables.common

        subset_select = select(common.c.id, common.c.data)
        assert_raises(sa.exc.ArgumentError, mapper, Subset, subset_select)

    def test_basic(self):
        Subset, common = self.classes.Subset, self.tables.common

        subset_select = select(common.c.id, common.c.data).alias()
        mapper(Subset, subset_select)
        sess = Session(bind=testing.db)
        sess.add(Subset(data=1))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Subset).all(), [Subset(data=1)])
        eq_(sess.query(Subset).filter(Subset.data == 1).one(), Subset(data=1))
        eq_(sess.query(Subset).filter(Subset.data != 1).first(), None)

        subset_select = sa.orm.class_mapper(Subset).persist_selectable
        eq_(
            sess.query(Subset).filter(subset_select.c.data == 1).one(),
            Subset(data=1),
        )
