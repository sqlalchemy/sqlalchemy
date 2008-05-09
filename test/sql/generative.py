import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.sql import table, column, ClauseElement
from sqlalchemy.sql.expression import  _clone
from testlib import *
from sqlalchemy.sql.visitors import *
from sqlalchemy import util
from sqlalchemy.sql import util as sql_util


class TraversalTest(TestBase, AssertsExecutionResults):
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

            def _copy_internals(self, clone=_clone):
                self.items = [clone(i) for i in self.items]

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


class ClauseTest(TestBase, AssertsCompiledSQL):
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

    def test_binary_anon_label_quirk(self):
        t = table('t1', column('col1'))


        f = t.c.col1 * 5
        self.assert_compile(select([f]), "SELECT t1.col1 * :col1_1 AS anon_1 FROM t1")

        f.anon_label

        a = t.alias()
        f = sql_util.ClauseAdapter(a).traverse(f)

        self.assert_compile(select([f]), "SELECT t1_1.col1 * :col1_1 AS anon_1 FROM t1 AS t1_1")
        
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
    
    def test_text(self):
        clause = text("select * from table where foo=:bar", bindparams=[bindparam('bar')])
        c1 = str(clause)
        class Vis(ClauseVisitor):
            def visit_textclause(self, text):
                text.text = text.text + " SOME MODIFIER=:lala"
                text.bindparams['lala'] = bindparam('lala')

        clause2 = Vis().traverse(clause, clone=True)
        assert c1 == str(clause)
        assert str(clause2) == c1 + " SOME MODIFIER=:lala"
        assert clause.bindparams.keys() == ['bar']
        assert util.Set(clause2.bindparams.keys()) == util.Set(['bar', 'lala'])

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
                    binary.right = bindparam("col1", unique=True)
        s5 = Vis().traverse(s4, clone=True)
        print str(s4)
        print str(s5)
        assert str(s5) == s5_assert
        assert str(s4) == s4_assert
    
    def test_union(self):
        u = union(t1.select(), t2.select())
        u2 = ClauseVisitor().traverse(u, clone=True)
        assert str(u) == str(u2)
        assert [str(c) for c in u2.c] == [str(c) for c in u.c]

        u = union(t1.select(), t2.select())
        cols = [str(c) for c in u.c]
        u2 = ClauseVisitor().traverse(u, clone=True)
        assert str(u) == str(u2)
        assert [str(c) for c in u2.c] == cols
        
        s1 = select([t1], t1.c.col1 == bindparam('id_param'))
        s2 = select([t2])
        u = union(s1, s2)
        
        u2 = u.params(id_param=7)
        u3 = u.params(id_param=10)
        assert str(u) == str(u2) == str(u3)
        assert u2.compile().params == {'id_param':7}
        assert u3.compile().params == {'id_param':10}
        
    def test_binds(self):
        """test that unique bindparams change their name upon clone() to prevent conflicts"""

        s = select([t1], t1.c.col1==bindparam(None, unique=True)).alias()
        s2 = ClauseVisitor().traverse(s, clone=True).alias()
        s3 = select([s], s.c.col2==s2.c.col2)

        self.assert_compile(s3, "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, "\
        "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :param_1) AS anon_1, "\
        "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1 WHERE table1.col1 = :param_2) AS anon_2 "\
        "WHERE anon_1.col2 = anon_2.col2")

        s = select([t1], t1.c.col1==4).alias()
        s2 = ClauseVisitor().traverse(s, clone=True).alias()
        s3 = select([s], s.c.col2==s2.c.col2)
        self.assert_compile(s3, "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, "\
        "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :col1_1) AS anon_1, "\
        "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1 WHERE table1.col1 = :col1_2) AS anon_2 "\
        "WHERE anon_1.col2 = anon_2.col2")

    @testing.emits_warning('.*replaced by another column with the same key')
    def test_alias(self):
        subq = t2.select().alias('subq')
        s = select([t1.c.col1, subq.c.col1], from_obj=[t1, subq, t1.join(subq, t1.c.col1==subq.c.col2)])
        orig = str(s)
        s2 = ClauseVisitor().traverse(s, clone=True)
        assert orig == str(s) == str(s2)

        s4 = ClauseVisitor().traverse(s2, clone=True)
        assert orig == str(s) == str(s2) == str(s4)

        s3 = sql_util.ClauseAdapter(table('foo')).traverse(s, clone=True)
        assert orig == str(s) == str(s3)

        s4 = sql_util.ClauseAdapter(table('foo')).traverse(s3, clone=True)
        assert orig == str(s) == str(s3) == str(s4)

    def test_correlated_select(self):
        s = select(['*'], t1.c.col1==t2.c.col1, from_obj=[t1, t2]).correlate(t2)
        class Vis(ClauseVisitor):
            def visit_select(self, select):
                select.append_whereclause(t1.c.col2==7)

        self.assert_compile(Vis().traverse(s, clone=True), "SELECT * FROM table1 WHERE table1.col1 = table2.col1 AND table1.col2 = :col2_1")

class ClauseAdapterTest(TestBase, AssertsCompiledSQL):
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

    def test_correlation_on_clone(self):
        t1alias = t1.alias('t1alias')
        t2alias = t2.alias('t2alias')
        vis = sql_util.ClauseAdapter(t1alias)

        s = select(['*'], from_obj=[t1alias, t2alias]).as_scalar()
        assert t2alias in s._froms
        assert t1alias in s._froms

        self.assert_compile(select(['*'], t2alias.c.col1==s), "SELECT * FROM table2 AS t2alias WHERE t2alias.col1 = (SELECT * FROM table1 AS t1alias)")
        s = vis.traverse(s, clone=True)
        assert t2alias not in s._froms  # not present because it's been cloned
        assert t1alias in s._froms # present because the adapter placed it there
        # correlate list on "s" needs to take into account the full _cloned_set for each element in _froms when correlating
        self.assert_compile(select(['*'], t2alias.c.col1==s), "SELECT * FROM table2 AS t2alias WHERE t2alias.col1 = (SELECT * FROM table1 AS t1alias)")

        s = select(['*'], from_obj=[t1alias, t2alias]).correlate(t2alias).as_scalar()
        self.assert_compile(select(['*'], t2alias.c.col1==s), "SELECT * FROM table2 AS t2alias WHERE t2alias.col1 = (SELECT * FROM table1 AS t1alias)")
        s = vis.traverse(s, clone=True)
        self.assert_compile(select(['*'], t2alias.c.col1==s), "SELECT * FROM table2 AS t2alias WHERE t2alias.col1 = (SELECT * FROM table1 AS t1alias)")
        s = ClauseVisitor().traverse(s, clone=True)
        self.assert_compile(select(['*'], t2alias.c.col1==s), "SELECT * FROM table2 AS t2alias WHERE t2alias.col1 = (SELECT * FROM table1 AS t1alias)")
        
        s = select(['*']).where(t1.c.col1==t2.c.col1).as_scalar()
        self.assert_compile(select([t1.c.col1, s]), "SELECT table1.col1, (SELECT * FROM table2 WHERE table1.col1 = table2.col1) AS anon_1 FROM table1")
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s, clone=True)
        self.assert_compile(select([t1alias.c.col1, s]), "SELECT t1alias.col1, (SELECT * FROM table2 WHERE t1alias.col1 = table2.col1) AS anon_1 FROM table1 AS t1alias")
        s = ClauseVisitor().traverse(s, clone=True)
        self.assert_compile(select([t1alias.c.col1, s]), "SELECT t1alias.col1, (SELECT * FROM table2 WHERE t1alias.col1 = table2.col1) AS anon_1 FROM table1 AS t1alias")

        s = select(['*']).where(t1.c.col1==t2.c.col1).correlate(t1).as_scalar()
        self.assert_compile(select([t1.c.col1, s]), "SELECT table1.col1, (SELECT * FROM table2 WHERE table1.col1 = table2.col1) AS anon_1 FROM table1")
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s, clone=True)
        self.assert_compile(select([t1alias.c.col1, s]), "SELECT t1alias.col1, (SELECT * FROM table2 WHERE t1alias.col1 = table2.col1) AS anon_1 FROM table1 AS t1alias")
        s = ClauseVisitor().traverse(s, clone=True)
        self.assert_compile(select([t1alias.c.col1, s]), "SELECT t1alias.col1, (SELECT * FROM table2 WHERE t1alias.col1 = table2.col1) AS anon_1 FROM table1 AS t1alias")
        
        
    def test_table_to_alias(self):

        t1alias = t1.alias('t1alias')

        vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label('foo'), clone=True)
        assert ff._get_from_objects() == [t1alias]

        self.assert_compile(vis.traverse(select(['*'], from_obj=[t1]), clone=True), "SELECT * FROM table1 AS t1alias")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2), clone=True), "SELECT * FROM table1 AS t1alias, table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]), clone=True), "SELECT * FROM table1 AS t1alias, table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t1), clone=True), "SELECT * FROM table2 WHERE t1alias.col1 = table2.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t2), clone=True), "SELECT * FROM table1 AS t1alias WHERE t1alias.col1 = table2.col2")


        s = select(['*'], from_obj=[t1]).alias('foo')
        self.assert_compile(s.select(), "SELECT foo.* FROM (SELECT * FROM table1) AS foo")
        self.assert_compile(vis.traverse(s.select(), clone=True), "SELECT foo.* FROM (SELECT * FROM table1 AS t1alias) AS foo")
        self.assert_compile(s.select(), "SELECT foo.* FROM (SELECT * FROM table1) AS foo")

        ff = vis.traverse(func.count(t1.c.col1).label('foo'), clone=True)
        self.assert_compile(ff, "count(t1alias.col1) AS foo")
        assert ff._get_from_objects() == [t1alias]

# TODO:
    #    self.assert_compile(vis.traverse(select([func.count(t1.c.col1).label('foo')]), clone=True), "SELECT count(t1alias.col1) AS foo FROM table1 AS t1alias")

        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2), clone=True), "SELECT * FROM table1 AS t1alias, table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]), clone=True), "SELECT * FROM table1 AS t1alias, table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t1), clone=True), "SELECT * FROM table2 AS t2alias WHERE t1alias.col1 = t2alias.col2")
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1==t2.c.col2, from_obj=[t1, t2]).correlate(t2), clone=True), "SELECT * FROM table1 AS t1alias WHERE t1alias.col1 = t2alias.col2")

    def test_include_exclude(self):
        m = MetaData()
        a=Table( 'a',m,
          Column( 'id',    Integer, primary_key=True),
          Column( 'xxx_id', Integer, ForeignKey( 'a.id', name='adf',use_alter=True ) )
        )

        e = (a.c.id == a.c.xxx_id)
        assert str(e) == "a.id = a.xxx_id"
        b = a.alias()

        e = sql_util.ClauseAdapter( b, include= set([ a.c.id ]),
          equivalents= { a.c.id: set([ a.c.id]) }
        ).traverse( e)

        assert str(e) == "a_1.id = a.xxx_id"

    def test_join_to_alias(self):
        metadata = MetaData()
        a = Table('a', metadata,
            Column('id', Integer, primary_key=True))
        b = Table('b', metadata,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            )
        c = Table('c', metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', Integer, ForeignKey('b.id')),
            )

        d = Table('d', metadata,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            )

        j1 = a.outerjoin(b)
        j2 = select([j1], use_labels=True)

        j3 = c.join(j2, j2.c.b_id==c.c.bid)

        j4 = j3.outerjoin(d)
        self.assert_compile(j4,  "c JOIN (SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid FROM a LEFT OUTER JOIN b ON a.id = b.aid) "
                                 "ON b_id = c.bid"
                                 " LEFT OUTER JOIN d ON a_id = d.aid")
        j5 = j3.alias('foo')
        j6 = sql_util.ClauseAdapter(j5).copy_and_process([j4])[0]

        # this statement takes c join(a join b), wraps it inside an aliased "select * from c join(a join b) AS foo".
        # the outermost right side "left outer join d" stays the same, except "d" joins against foo.a_id instead
        # of plain "a_id"
        self.assert_compile(j6, "(SELECT c.id AS c_id, c.bid AS c_bid, a_id AS a_id, b_id AS b_id, b_aid AS b_aid FROM "
                                "c JOIN (SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid FROM a LEFT OUTER JOIN b ON a.id = b.aid) "
                                "ON b_id = c.bid) AS foo"
                                " LEFT OUTER JOIN d ON foo.a_id = d.aid")

    def test_derived_from(self):
        assert select([t1]).is_derived_from(t1)
        assert not select([t2]).is_derived_from(t1)
        assert not t1.is_derived_from(select([t1]))
        assert t1.alias().is_derived_from(t1)


        s1 = select([t1, t2]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()
        assert s2.is_derived_from(s1)
        s2 = s2._clone()
        assert s2.is_derived_from(s1)

    def test_aliasedselect_to_aliasedselect(self):
        # original issue from ticket #904
        s1 = select([t1]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()

        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(s1),
            "SELECT foo.col1, foo.col2, foo.col3 FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1) AS foo  LIMIT 5 OFFSET 10")

        j = s1.outerjoin(t2, s1.c.col1==t2.c.col1)
        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(j).select(),
            "SELECT anon_1.col1, anon_1.col2, anon_1.col3, table2.col1, table2.col2, table2.col3 FROM "\
            "(SELECT foo.col1 AS col1, foo.col2 AS col2, foo.col3 AS col3 FROM "\
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1) AS foo  LIMIT 5 OFFSET 10) AS anon_1 "\
            "LEFT OUTER JOIN table2 ON anon_1.col1 = table2.col1")

        talias = t1.alias('bar')
        j = s1.outerjoin(talias, s1.c.col1==talias.c.col1)
        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(j).select(),
            "SELECT anon_1.col1, anon_1.col2, anon_1.col3, bar.col1, bar.col2, bar.col3 FROM "\
            "(SELECT foo.col1 AS col1, foo.col2 AS col2, foo.col3 AS col3 FROM "\
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1) AS foo  LIMIT 5 OFFSET 10) AS anon_1 "\
            "LEFT OUTER JOIN table1 AS bar ON anon_1.col1 = bar.col1")
    
    def test_recursive(self):
        metadata = MetaData()
        a = Table('a', metadata,
            Column('id', Integer, primary_key=True))
        b = Table('b', metadata,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            )
        c = Table('c', metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', Integer, ForeignKey('b.id')),
            )

        d = Table('d', metadata,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            )

        u = union(
            a.join(b).select().apply_labels(),
            a.join(d).select().apply_labels()
        ).alias()    
        
        self.assert_compile(
            sql_util.ClauseAdapter(u).traverse(select([c.c.bid]).where(c.c.bid==u.c.b_aid)),
            "SELECT c.bid "\
            "FROM c, (SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid "\
            "FROM a JOIN b ON a.id = b.aid UNION SELECT a.id AS a_id, d.id AS d_id, d.aid AS d_aid "\
            "FROM a JOIN d ON a.id = d.aid) AS anon_1 "\
            "WHERE c.bid = anon_1.b_aid"
        )

class SelectTest(TestBase, AssertsCompiledSQL):
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
        self.assert_compile(t1.select().where(t1.c.col1==5).order_by(t1.c.col3),
        "SELECT table1.col1, table1.col2, table1.col3 FROM table1 WHERE table1.col1 = :col1_1 ORDER BY table1.col3")

        self.assert_compile(t1.select().select_from(select([t2], t2.c.col1==t1.c.col1)).order_by(t1.c.col3),
            "SELECT table1.col1, table1.col2, table1.col3 FROM table1, (SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 "\
            "FROM table2 WHERE table2.col1 = table1.col1) ORDER BY table1.col3")

        s = select([t2], t2.c.col1==t1.c.col1, correlate=False)
        s = s.correlate(t1).order_by(t2.c.col3)
        self.assert_compile(t1.select().select_from(s).order_by(t1.c.col3),
            "SELECT table1.col1, table1.col2, table1.col3 FROM table1, (SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 "\
            "FROM table2 WHERE table2.col1 = table1.col1 ORDER BY table2.col3) ORDER BY table1.col3")

    def test_columns(self):
        s = t1.select()
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")
        select_copy = s.column('yyy')
        self.assert_compile(select_copy, "SELECT table1.col1, table1.col2, table1.col3, yyy FROM table1")
        assert s.columns is not select_copy.columns
        assert s._columns is not select_copy._columns
        assert s._raw_columns is not select_copy._raw_columns
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")

    def test_froms(self):
        s = t1.select()
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")
        select_copy = s.select_from(t2)
        self.assert_compile(select_copy, "SELECT table1.col1, table1.col2, table1.col3 FROM table1, table2")
        assert s._froms is not select_copy._froms
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")

    def test_correlation(self):
        s = select([t2], t1.c.col1==t2.c.col1)
        self.assert_compile(s, "SELECT table2.col1, table2.col2, table2.col3 FROM table2, table1 WHERE table1.col1 = table2.col1")
        s2 = select([t1], t1.c.col2==s.c.col2)
        self.assert_compile(s2, "SELECT table1.col1, table1.col2, table1.col3 FROM table1, "
                "(SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2 "
                "WHERE table1.col1 = table2.col1) WHERE table1.col2 = col2")

        s3 = s.correlate(None)
        self.assert_compile(select([t1], t1.c.col2==s3.c.col2), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, "
                "(SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2, table1 "
                "WHERE table1.col1 = table2.col1) WHERE table1.col2 = col2")
        self.assert_compile(select([t1], t1.c.col2==s.c.col2), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, "
                "(SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2 "
                "WHERE table1.col1 = table2.col1) WHERE table1.col2 = col2")
        s4 = s3.correlate(t1)
        self.assert_compile(select([t1], t1.c.col2==s4.c.col2), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, "
                "(SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2 "
                "WHERE table1.col1 = table2.col1) WHERE table1.col2 = col2")
        self.assert_compile(select([t1], t1.c.col2==s3.c.col2), "SELECT table1.col1, table1.col2, table1.col3 FROM table1, "
                "(SELECT table2.col1 AS col1, table2.col2 AS col2, table2.col3 AS col3 FROM table2, table1 "
                "WHERE table1.col1 = table2.col1) WHERE table1.col2 = col2")

    def test_prefixes(self):
        s = t1.select()
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")
        select_copy = s.prefix_with("FOOBER")
        self.assert_compile(select_copy, "SELECT FOOBER table1.col1, table1.col2, table1.col3 FROM table1")
        self.assert_compile(s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1")


class InsertTest(TestBase, AssertsCompiledSQL):
    """Tests the generative capability of Insert"""

    # fixme: consolidate converage from elsewhere here and expand

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

    def test_prefixes(self):
        i = t1.insert()
        self.assert_compile(i,
                            "INSERT INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        gen = i.prefix_with("foober")
        self.assert_compile(gen,
                            "INSERT foober INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        self.assert_compile(i,
                            "INSERT INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        i2 = t1.insert(prefixes=['squiznart'])
        self.assert_compile(i2,
                            "INSERT squiznart INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        gen2 = i2.prefix_with("quux")
        self.assert_compile(gen2,
                            "INSERT squiznart quux INTO "
                            "table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

if __name__ == '__main__':
    testenv.main()
