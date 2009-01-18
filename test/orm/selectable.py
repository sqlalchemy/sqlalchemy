"""Generic mapping to Select statements"""
import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import Table, Column, String, Integer, select
from testlib.sa.orm import mapper, create_session
from testlib.testing import eq_
from orm import _base


# TODO: more tests mapping to selects

class SelectableNoFromsTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('common', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', Integer),
              Column('extra', String(45)))

    def setup_classes(self):
        class Subset(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_no_tables(self):

        selectable = select(["x", "y", "z"])
        self.assertRaisesMessage(sa.exc.InvalidRequestError,
                                 "Could not find any Table objects",
                                 mapper, Subset, selectable)

    @testing.emits_warning('.*creating an Alias.*')
    @testing.resolve_artifact_names
    def test_basic(self):
        subset_select = select([common.c.id, common.c.data])
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


if __name__ == '__main__':
    testenv.main()
