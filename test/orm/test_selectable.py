"""Generic mapping to Select statements"""
from sqlalchemy.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import String, Integer, select
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, Session
from sqlalchemy.testing import eq_, AssertsCompiledSQL
from sqlalchemy.testing import fixtures



# TODO: more tests mapping to selects

class SelectableNoFromsTest(fixtures.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        Table('common', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', Integer),
              Column('extra', String(45)))

    @classmethod
    def setup_classes(cls):
        class Subset(cls.Comparable):
            pass

    def test_no_tables(self):
        Subset = self.classes.Subset


        selectable = select(["x", "y", "z"]).alias()
        mapper(Subset, selectable, primary_key=[selectable.c.x])

        self.assert_compile(
            Session().query(Subset),
            "SELECT anon_1.x, anon_1.y, anon_1.z FROM (SELECT x, y, z) AS anon_1",
            use_default_dialect=True
        )

    def test_no_table_needs_pl(self):
        Subset = self.classes.Subset


        selectable = select(["x", "y", "z"]).alias()
        assert_raises_message(
            sa.exc.ArgumentError,
            "could not assemble any primary key columns",
            mapper, Subset, selectable
        )

    def test_no_selects(self):
        Subset, common = self.classes.Subset, self.tables.common

        subset_select = select([common.c.id, common.c.data])
        assert_raises(sa.exc.InvalidRequestError, mapper, Subset, subset_select)

    def test_basic(self):
        Subset, common = self.classes.Subset, self.tables.common

        subset_select = select([common.c.id, common.c.data]).alias()
        subset_mapper = mapper(Subset, subset_select)
        sess = Session(bind=testing.db)
        sess.add(Subset(data=1))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Subset).all(), [Subset(data=1)])
        eq_(sess.query(Subset).filter(Subset.data==1).one(), Subset(data=1))
        eq_(sess.query(Subset).filter(Subset.data!=1).first(), None)

        subset_select = sa.orm.class_mapper(Subset).mapped_table
        eq_(sess.query(Subset).filter(subset_select.c.data==1).one(),
            Subset(data=1))


