import testbase
from sqlalchemy import *
from testlib import *

class TraversalTest(AssertMixin):
    """test ClauseVisitor's traversal, particularly its ability to copy and modify
    a ClauseElement in place."""
    
    def setUpAll(self):
        global A, B
        
        # establish two ficticious ClauseElements.
        # define deep equality semantics as well as deep identity semantics.
        class A(ClauseElement):
            def __init__(self, expr):
                self.expr = expr

            def is_other(self, other):
                return other is self
            
            def __eq__(self, other):
                return other.expr == self.expr
            
            def __ne__(self, other):
                return other.expr != self.expr
                
            def __str__(self):
                return "A(%s)" % repr(self.expr)
                
        class B(ClauseElement):
            def __init__(self, *items):
                self.items = items

            def is_other(self, other):
                if other is not self:
                    return False
                for i1, i2 in zip(self.items, other.items):
                    if i1 is not i2:
                        return False
                return True

            def __eq__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return False
                return True
            
            def __ne__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return True
                return False
            
            def _copy_internals(self):    
                self.items = [i._clone() for i in self.items]

            def get_children(self, **kwargs):
                return self.items
            
            def __str__(self):
                return "B(%s)" % repr([str(i) for i in self.items])
    
    def test_test_classes(self):
        a1 = A("expr1")
        struct = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(a1, A("expr2"), B(A("expr1b"), A("expr2bmodified")), A("expr3"))

        assert a1.is_other(a1)
        assert struct.is_other(struct)
        assert struct == struct2
        assert struct != struct3
        assert not struct.is_other(struct2)
        assert not struct.is_other(struct3)
        
    def test_clone(self):    
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        
        class Vis(ClauseVisitor):
            def visit_a(self, a):
                pass
            def visit_b(self, b):
                pass
                
        vis = Vis()
        s2 = vis.traverse(struct, clone=True)
        assert struct == s2
        assert not struct.is_other(s2)

    def test_no_clone(self):    
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))

        class Vis(ClauseVisitor):
            def visit_a(self, a):
                pass
            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct, clone=False)
        assert struct == s2
        assert struct.is_other(s2)
        
    def test_change_in_place(self):
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(A("expr1"), A("expr2modified"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(A("expr1"), A("expr2"), B(A("expr1b"), A("expr2bmodified")), A("expr3"))

        class Vis(ClauseVisitor):
            def visit_a(self, a):
                if a.expr == "expr2":
                    a.expr = "expr2modified"
            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct, clone=True)
        assert struct != s2
        assert not struct.is_other(s2)
        assert struct2 == s2

        class Vis2(ClauseVisitor):
            def visit_a(self, a):
                if a.expr == "expr2b":
                    a.expr = "expr2bmodified"
            def visit_b(self, b):
                pass

        vis2 = Vis2()
        s3 = vis2.traverse(struct, clone=True)
        assert struct != s3
        assert struct3 == s3

class ClauseTest(SQLCompileTest):
    """test copy-in-place behavior of various ClauseElements."""
    
    def setUpAll(self):
        global t1, t2
        t1 = table("table1", 
            column("col1"),
            column("col2"),
            column("col3"),
            )
        t2 = table("table2", 
            column("col1"),
            column("col2"),
            column("col3"),
            )
            
    def test_binary(self):
        clause = t1.c.col2 == t2.c.col2
        assert str(clause) == ClauseVisitor().traverse(clause, clone=True)
    
    def test_join(self):
        clause = t1.join(t2, t1.c.col2==t2.c.col2)
        c1 = str(clause)
        assert str(clause) == str(ClauseVisitor().traverse(clause, clone=True))
    
        class Vis(ClauseVisitor):
            def visit_binary(self, binary):
                binary.right = t2.c.col3
                
        clause2 = Vis().traverse(clause, clone=True)
        assert c1 == str(clause)
        assert str(clause2) == str(t1.join(t2, t1.c.col2==t2.c.col3))
    
    def test_select(self):
        s2 = select([t1])
        s2_assert = str(s2)
        s3_assert = str(select([t1], t1.c.col2==7))
        class Vis(ClauseVisitor):
            def visit_select(self, select):
                select.append_whereclause(t1.c.col2==7)
        s3 = Vis().traverse(s2, clone=True)
        assert str(s3) == s3_assert
        assert str(s2) == s2_assert
        print str(s2)
        print str(s3)
        Vis().traverse(s2)
        assert str(s2) == s3_assert

        print "------------------"
        
        s4_assert = str(select([t1], and_(t1.c.col2==7, t1.c.col3==9)))
        class Vis(ClauseVisitor):
            def visit_select(self, select):
                select.append_whereclause(t1.c.col3==9)
        s4 = Vis().traverse(s3, clone=True)
        print str(s3)
        print str(s4)
        assert str(s4) == s4_assert
        assert str(s3) == s3_assert
        
        print "------------------"
        s5_assert = str(select([t1], and_(t1.c.col2==7, t1.c.col1==9)))
        class Vis(ClauseVisitor):
            def visit_binary(self, binary):
                if binary.left is t1.c.col3:
                    binary.left = t1.c.col1
                    binary.right = bindparam("table1_col1")
        s5 = Vis().traverse(s4, clone=True)
        print str(s4)
        print str(s5)
        assert str(s5) == s5_assert
        assert str(s4) == s4_assert
    
    def test_correlated_select(self):
        s = select(['*'], t1.c.col1==t2.c.col1, from_obj=[t1, t2]).correlate(t2)
        class Vis(ClauseVisitor):
            def visit_select(self, select):
                select.append_whereclause(t1.c.col2==7)
                
        self.assert_compile(Vis().traverse(s, clone=True), "SELECT * FROM table1 WHERE table1.col1 = table2.col1 AND table1.col2 = :table1_col2")

    def test_clause_adapter(self):
        from sqlalchemy import sql_util
        
        t1alias = t1.alias('t1alias')
        
        vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label('foo'), clone=True)
        assert ff._get_from_objects() == [t1alias]
        
        self.assert_compile(vis.traverse(select(['*'], from_obj=[t1]), clone=True), "SELECT * FROM table1 AS t1alias")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2), clone=True), "SELECT * FROM table1 AS t1alias, table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]), clone=True), "SELECT * FROM table1 AS t1alias, table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t1), clone=True), "SELECT * FROM table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t2), clone=True), "SELECT * FROM table1 AS t1alias WHERE t1alias.col1 = table2.col2")
        
        ff = vis.traverse(func.count(t1.c.col1).label('foo'), clone=True)
        self.assert_compile(ff, "count(t1alias.col1) AS foo")
        assert ff._get_from_objects() == [t1alias]
        
# TODO:
#        self.assert_compile(vis.traverse(select([func.count(t1.c.col1).label('foo')]), clone=True), "SELECT count(t1alias.col1) AS foo FROM table1 AS t1alias")
        
        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2), clone=True), "SELECT * FROM table1 AS t1alias, table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]), clone=True), "SELECT * FROM table1 AS t1alias, table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t1), clone=True), "SELECT * FROM table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t2), clone=True), "SELECT * FROM table1 AS t1alias WHERE t1alias.col1 = t2alias.col2")
        
        
        
class SelectTest(SQLCompileTest):
    """tests the generative capability of Select"""

    def setUpAll(self):
        global t1, t2
        t1 = table("table1", 
            column("col1"),
            column("col2"),
            column("col3"),
            )
        t2 = table("table2", 
            column("col1"),
            column("col2"),
            column("col3"),
            )
    
    def test_select(self):
        self.assert_compile(t1.select().where(t1.c.col1==5).order_by(t1.c.col3), "SELECT table1.col1, table1.col2, table1.col3 FROM table1 WHERE table1.col1 = :table1_col1 ORDER BY table1.col3")
    
        self.assert_compile(t1.select().select_from(select([t2], t2.c.col1==t1.c.col1)).order_by(t1.c.col3), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, (SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2 WHERE table2.col1 = table1.col1) ORDER BY table1.col3")
        
        s = select([t2], t2.c.col1==t1.c.col1, correlate=False)
        s = s.correlate(t1).order_by(t2.c.col3)
        self.assert_compile(t1.select().select_from(s).order_by(t1.c.col3), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, (SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2 WHERE table2.col1 = table1.col1 ORDER BY table2.col3) ORDER BY table1.col3")


if __name__ == '__main__':
    testbase.main()        
