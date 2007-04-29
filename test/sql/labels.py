import testbase

from sqlalchemy import *

# TODO: either create a mock dialect with named paramstyle and a short identifier length,
# or find a way to just use sqlite dialect and make those changes

class LabelTypeTest(testbase.PersistTest):
    def test_type(self):
        m = MetaData()
        t = Table('sometable', m, 
            Column('col1', Integer),
            Column('col2', Float))
        assert isinstance(t.c.col1.label('hi').type, Integer)
        assert isinstance(select([t.c.col2], scalar=True).label('lala').type, Float)

class LongLabelsTest(testbase.PersistTest):
    def setUpAll(self):
        global metadata, table1
        metadata = MetaData(engine=testbase.db)
        table1 = Table("some_large_named_table", metadata,
            Column("this_is_the_primarykey_column", Integer, Sequence("this_is_some_large_seq"), primary_key=True),
            Column("this_is_the_data_column", String(30))
            )
        metadata.create_all()
    def tearDown(self):
        table1.delete().execute()
        
    def tearDownAll(self):
        metadata.drop_all()
        
    def test_result(self):
        table1.insert().execute(**{"this_is_the_primarykey_column":1, "this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_primarykey_column":2, "this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_primarykey_column":3, "this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_primarykey_column":4, "this_is_the_data_column":"data4"})

        r = table1.select(use_labels=True, order_by=[table1.c.this_is_the_primarykey_column]).execute()
        result = []
        for row in r:
            result.append((row[table1.c.this_is_the_primarykey_column], row[table1.c.this_is_the_data_column]))
        assert result == [
            (1, "data1"),
            (2, "data2"),
            (3, "data3"),
            (4, "data4"),
        ], repr(result)
    
    def test_colbinds(self):
        table1.insert().execute(**{"this_is_the_primarykey_column":1, "this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_primarykey_column":2, "this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_primarykey_column":3, "this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_primarykey_column":4, "this_is_the_data_column":"data4"})

        r = table1.select(table1.c.this_is_the_primarykey_column == 4).execute()
        assert r.fetchall() == [(4, "data4")]

        r = table1.select(or_(
            table1.c.this_is_the_primarykey_column == 4,
            table1.c.this_is_the_primarykey_column == 2
        )).execute()
        assert r.fetchall() == [(2, "data2"), (4, "data4")]
    
    def test_insert_no_pk(self):
        table1.insert().execute(**{"this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_data_column":"data4"})
        
    def test_subquery(self):
        q = table1.select(table1.c.this_is_the_primarykey_column == 4, use_labels=True)
        x = select([q])
        print str(x)
        
if __name__ == '__main__':
    testbase.main()
