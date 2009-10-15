"""Generic mapping to Select statements"""
from sqlalchemy.test.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import String, Integer, select
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import mapper, create_session
from sqlalchemy.test.testing import eq_
from test.orm import _base


# TODO: more tests mapping to selects

class SelectableNoFromsTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('common', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', Integer),
              Column('extra', String(45)))

    @classmethod
    def setup_classes(cls):
        class Subset(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_no_tables(self):

        selectable = select(["x", "y", "z"]).alias()
        assert_raises_message(sa.exc.InvalidRequestError,
                                 "Could not find any Table objects",
                                 mapper, Subset, selectable)

    @testing.resolve_artifact_names
    def test_no_selects(self):
        subset_select = select([common.c.id, common.c.data])
        assert_raises(sa.exc.InvalidRequestError, mapper, Subset, subset_select)
        
    @testing.resolve_artifact_names
    def test_basic(self):
        subset_select = select([common.c.id, common.c.data]).alias()
        subset_mapper = mapper(Subset, subset_select)

        sess = create_session(bind=testing.db)
        sess.add(Subset(data=1))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Subset).all(), [Subset(data=1)])
        eq_(sess.query(Subset).filter(Subset.data==1).one(), Subset(data=1))
        eq_(sess.query(Subset).filter(Subset.data!=1).first(), None)

        subset_select = sa.orm.class_mapper(Subset).mapped_table
        eq_(sess.query(Subset).filter(subset_select.c.data==1).one(),
            Subset(data=1))


