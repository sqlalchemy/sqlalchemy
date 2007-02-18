from sqlalchemy import *
from sqlalchemy.orm import mapperlib, session, unitofwork, attributes
Mapper = mapperlib.Mapper
import gc
import testbase
import tables

class A(object):pass
class B(object):pass

class MapperCleanoutTest(testbase.AssertMixin):
    """test that clear_mappers() removes everything related to the class.
    
    does not include classes that use the assignmapper extension."""
    def setUp(self):
        global engine
        engine = testbase.db
    
    def test_mapper_cleanup(self):
        for x in range(0, 5):
            self.do_test()
            gc.collect()
            for o in gc.get_objects():
                if isinstance(o, Mapper):
                    # the classes in the 'tables' package have assign_mapper called on them
                    # which is particularly sticky
                    # if getattr(tables, o.class_.__name__, None) is o.class_:
                    #    continue
                    # well really we are just testing our own classes here
                    if (o.class_ not in [A,B]):
                        continue
                    assert False
        assert True
        
    def do_test(self):
        metadata = BoundMetaData(engine)

        table1 = Table("mytable", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30))
            )

        table2 = Table("mytable2", metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(30)),
            Column('col3', Integer, ForeignKey("mytable.col1"))
            )
    
        metadata.create_all()


        m1 = mapper(A, table1, properties={
            "bs":relation(B)
        })
        m2 = mapper(B, table2)

        m3 = mapper(A, table1, non_primary=True)
        
        sess = create_session()
        a1 = A()
        a2 = A()
        a3 = A()
        a1.bs.append(B())
        a1.bs.append(B())
        a3.bs.append(B())
        for x in [a1,a2,a3]:
            sess.save(x)
        sess.flush()
        sess.clear()

        alist = sess.query(A).select()
        for a in alist:
            print "A", a, "BS", [b for b in a.bs]
    
        metadata.drop_all()
        clear_mappers()
    
if __name__ == '__main__':
    testbase.main()
