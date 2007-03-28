import testbase

from sqlalchemy import *

class LongLabelsTest(testbase.PersistTest):
    def setUpAll(self):
        global metadata, table1
        metadata = MetaData(engine=testbase.db)
        table1 = Table("some_large_named_table", metadata,
            Column("this_is_the_primary_key_column", Integer, primary_key=True),
            Column("this_is_the_data_column", String(30))
            )
        metadata.create_all()
        table1.insert().execute(**{"this_is_the_primary_key_column":1, "this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_primary_key_column":2, "this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_primary_key_column":3, "this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_primary_key_column":4, "this_is_the_data_column":"data4"})
    def tearDownAll(self):
        metadata.drop_all()
        
    def test_result(self):
        r = table1.select(use_labels=True).execute()
        result = []
        for row in r:
            result.append((row[table1.c.this_is_the_primary_key_column], row[table1.c.this_is_the_data_column]))
        assert result == [
            (1, "data1"),
            (2, "data2"),
            (3, "data3"),
            (4, "data4"),
        ]
    
    def test_colbinds(self):
        r = table1.select(table1.c.this_is_the_primary_key_column == 4).execute()
        assert r.fetchall() == [(4, "data4")]

        r = table1.select(or_(
            table1.c.this_is_the_primary_key_column == 4,
            table1.c.this_is_the_primary_key_column == 2
        )).execute()
        assert r.fetchall() == [(2, "data2"), (4, "data4")]
        
if __name__ == '__main__':
    testbase.main()