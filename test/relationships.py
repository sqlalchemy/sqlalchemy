"""Test complex relationships"""

import testbase
import unittest, sys, datetime

db = testbase.db
#db.echo_uow=True

from sqlalchemy import *


class RelationTest(testbase.PersistTest):
    """this is essentially an extension of the "dependency.py" topological sort test.  
    in this test, a table is dependent on two other tables that are otherwise unrelated to each other.
    the dependency sort must insure that this childmost table is below both parent tables in the outcome
    (a bug existed where this was not always the case).
    while the straight topological sort tests should expose this, since the sorting can be different due
    to subtle differences in program execution, this test case was exposing the bug whereas the simpler tests
    were not."""
    def setUpAll(self):
        global tbl_a
        global tbl_b
        global tbl_c
        global tbl_d
        tbl_a = Table("tbl_a", db,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        tbl_b = Table("tbl_b", db,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        tbl_c = Table("tbl_c", db,
            Column("id", Integer, primary_key=True),
            Column("tbl_a_id", Integer, ForeignKey("tbl_a.id"), nullable=False),
            Column("name", String),
        )
        tbl_d = Table("tbl_d", db,
            Column("id", Integer, primary_key=True),
            Column("tbl_c_id", Integer, ForeignKey("tbl_c.id"), nullable=False),
            Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
            Column("name", String),
        )
    def setUp(self):
        tbl_a.create()
        tbl_b.create()
        tbl_c.create()
        tbl_d.create()

        objectstore.clear()
        clear_mappers()

        class A(object):
            pass
        class B(object):
            pass
        class C(object):
            pass
        class D(object):
            pass

        D.mapper = mapper(D, tbl_d)
        C.mapper = mapper(C, tbl_c, properties=dict(
            d_rows=relation(D, private=True, backref="c_row"),
        ))
        B.mapper = mapper(B, tbl_b)
        A.mapper = mapper(A, tbl_a, properties=dict(
            c_rows=relation(C, private=True, backref="a_row"),
        ))
        D.mapper.add_property("b_row", relation(B))

        global a
        global c
        a = A(); a.name = "a1"
        b = B(); b.name = "b1"
        c = C(); c.name = "c1"; c.a_row = a
        # we must have more than one d row or it won't fail
        d = D(); d.name = "d1"; d.b_row = b; d.c_row = c
        d = D(); d.name = "d2"; d.b_row = b; d.c_row = c
        d = D(); d.name = "d3"; d.b_row = b; d.c_row = c

    def tearDown(self):
        tbl_d.drop()
        tbl_c.drop()
        tbl_b.drop()
        tbl_a.drop()

    def tearDownAll(self):
        testbase.db.tables.clear()
    
    def testDeleteRootTable(self):
        session = objectstore.get_session()
        session.commit()
        session.delete(a) # works as expected
        session.commit()

    def testDeleteMiddleTable(self):
        session = objectstore.get_session()
        session.commit()
        session.delete(c) # fails
        session.commit()
        
        
if __name__ == "__main__":
    testbase.main()        
