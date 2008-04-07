"""all tests involving generic mapping to Select statements"""

import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *
from query import QueryTest

class SelectableNoFromsTest(ORMTest):
    def define_tables(self, metadata):
        global common_table
        common_table = Table('common', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', Integer),
            Column('extra', String(45)),
        )

    def test_no_tables(self):
        class Subset(object):
            pass
        selectable = select(["x", "y", "z"])
        self.assertRaisesMessage(exceptions.InvalidRequestError, "Could not find any Table objects", mapper, Subset, selectable)

    @testing.emits_warning('.*creating an Alias.*')
    def test_basic(self):
        class Subset(Base):
            pass

        subset_select = select([common_table.c.id, common_table.c.data])
        subset_mapper = mapper(Subset, subset_select)

        sess = create_session(bind=testing.db)
        l = Subset()
        l.data = 1
        sess.save(l)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(Subset).all(), [Subset(data=1)])
        self.assertEquals(sess.query(Subset).filter(Subset.data==1).one(), Subset(data=1))
        self.assertEquals(sess.query(Subset).filter(Subset.data!=1).first(), None)
        
        subset_select = class_mapper(Subset).mapped_table
        self.assertEquals(sess.query(Subset).filter(subset_select.c.data==1).one(), Subset(data=1))

        
    # TODO: more tests mapping to selects

if __name__ == '__main__':
    testenv.main()
