"""all tests involving generic mapping to Select statements"""

import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from fixtures import *
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
        selectable = select(["x", "y", "z"]).alias('foo')
        try:
            mapper(Subset, selectable)
            compile_mappers()
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Could not find any Table objects in mapped table 'SELECT x, y, z'", str(e)
            
    def test_basic(self):
        class Subset(Base):
            pass

        subset_select = select([common_table.c.id, common_table.c.data]).alias('subset')
        subset_mapper = mapper(Subset, subset_select)

        sess = create_session(bind=testbase.db)
        l = Subset()
        l.data = 1
        sess.save(l)
        sess.flush()
        sess.clear()

        assert [Subset(data=1)] == sess.query(Subset).all()

    # TODO: more tests mapping to selects
    
if __name__ == '__main__':
    testbase.main()