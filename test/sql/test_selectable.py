"""Test various algorithmic properties of selectables."""

from sqlalchemy.testing import eq_, assert_raises, \
    assert_raises_message, is_
from sqlalchemy import *
from sqlalchemy.testing import fixtures, AssertsCompiledSQL, \
    AssertsExecutionResults
from sqlalchemy.sql import elements
from sqlalchemy import testing
from sqlalchemy.sql import util as sql_util, visitors, expression
from sqlalchemy import exc
from sqlalchemy.sql import table, column, null
from sqlalchemy import util
from sqlalchemy.schema import Column, Table, MetaData

metadata = MetaData()
table1 = Table('table1', metadata,
               Column('col1', Integer, primary_key=True),
               Column('col2', String(20)),
               Column('col3', Integer),
               Column('colx', Integer),

               )

table2 = Table('table2', metadata,
               Column('col1', Integer, primary_key=True),
               Column('col2', Integer, ForeignKey('table1.col1')),
               Column('col3', String(20)),
               Column('coly', Integer),
               )

keyed = Table('keyed', metadata,
              Column('x', Integer, key='colx'),
              Column('y', Integer, key='coly'),
              Column('z', Integer),
              )


class SelectableTest(
        fixtures.TestBase,
        AssertsExecutionResults,
        AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_indirect_correspondence_on_labels(self):
        # this test depends upon 'distance' to
        # get the right result

        # same column three times

        s = select([table1.c.col1.label('c2'), table1.c.col1,
                    table1.c.col1.label('c1')])

        # this tests the same thing as
        # test_direct_correspondence_on_labels below -
        # that the presence of label() affects the 'distance'
        assert s.corresponding_column(table1.c.col1) is s.c.col1

        assert s.corresponding_column(s.c.col1) is s.c.col1
        assert s.corresponding_column(s.c.c1) is s.c.c1

    def test_labeled_subquery_twice(self):
        scalar_select = select([table1.c.col1]).label('foo')

        s1 = select([scalar_select])
        s2 = select([scalar_select, scalar_select])

        eq_(
            s1.c.foo.proxy_set,
            set([s1.c.foo, scalar_select, scalar_select.element])
        )
        eq_(
            s2.c.foo.proxy_set,
            set([s2.c.foo, scalar_select, scalar_select.element])
        )

        assert s1.corresponding_column(scalar_select) is s1.c.foo
        assert s2.corresponding_column(scalar_select) is s2.c.foo

    def test_label_grouped_still_corresponds(self):
        label = select([table1.c.col1]).label('foo')
        label2 = label.self_group()

        s1 = select([label])
        s2 = select([label2])
        assert s1.corresponding_column(label) is s1.c.foo
        assert s2.corresponding_column(label) is s2.c.foo

    def test_direct_correspondence_on_labels(self):
        # this test depends on labels being part
        # of the proxy set to get the right result

        l1, l2 = table1.c.col1.label('foo'), table1.c.col1.label('bar')
        sel = select([l1, l2])

        sel2 = sel.alias()
        assert sel2.corresponding_column(l1) is sel2.c.foo
        assert sel2.corresponding_column(l2) is sel2.c.bar

        sel2 = select([table1.c.col1.label('foo'), table1.c.col2.label('bar')])

        sel3 = sel.union(sel2).alias()
        assert sel3.corresponding_column(l1) is sel3.c.foo
        assert sel3.corresponding_column(l2) is sel3.c.bar

    def test_keyed_gen(self):
        s = select([keyed])
        eq_(s.c.colx.key, 'colx')

        eq_(s.c.colx.name, 'x')

        assert s.corresponding_column(keyed.c.colx) is s.c.colx
        assert s.corresponding_column(keyed.c.coly) is s.c.coly
        assert s.corresponding_column(keyed.c.z) is s.c.z

        sel2 = s.alias()
        assert sel2.corresponding_column(keyed.c.colx) is sel2.c.colx
        assert sel2.corresponding_column(keyed.c.coly) is sel2.c.coly
        assert sel2.corresponding_column(keyed.c.z) is sel2.c.z

    def test_keyed_label_gen(self):
        s = select([keyed]).apply_labels()

        assert s.corresponding_column(keyed.c.colx) is s.c.keyed_colx
        assert s.corresponding_column(keyed.c.coly) is s.c.keyed_coly
        assert s.corresponding_column(keyed.c.z) is s.c.keyed_z

        sel2 = s.alias()
        assert sel2.corresponding_column(keyed.c.colx) is sel2.c.keyed_colx
        assert sel2.corresponding_column(keyed.c.coly) is sel2.c.keyed_coly
        assert sel2.corresponding_column(keyed.c.z) is sel2.c.keyed_z

    def test_keyed_c_collection_upper(self):
        c = Column('foo', Integer, key='bar')
        t = Table('t', MetaData(), c)
        is_(t.c.bar, c)

    def test_keyed_c_collection_lower(self):
        c = column('foo')
        c.key = 'bar'
        t = table('t', c)
        is_(t.c.bar, c)

    def test_clone_c_proxy_key_upper(self):
        c = Column('foo', Integer, key='bar')
        t = Table('t', MetaData(), c)
        s = select([t])._clone()
        assert c in s.c.bar.proxy_set

    def test_clone_c_proxy_key_lower(self):
        c = column('foo')
        c.key = 'bar'
        t = table('t', c)
        s = select([t])._clone()
        assert c in s.c.bar.proxy_set

    def test_no_error_on_unsupported_expr_key(self):
        from sqlalchemy.sql.expression import BinaryExpression

        def myop(x, y):
            pass

        t = table('t', column('x'), column('y'))

        expr = BinaryExpression(t.c.x, t.c.y, myop)

        s = select([t, expr])
        eq_(
            s.c.keys(),
            ['x', 'y', expr.anon_label]
        )

    def test_cloned_intersection(self):
        t1 = table('t1', column('x'))
        t2 = table('t2', column('x'))

        s1 = t1.select()
        s2 = t2.select()
        s3 = t1.select()

        s1c1 = s1._clone()
        s1c2 = s1._clone()
        s2c1 = s2._clone()
        s3c1 = s3._clone()

        eq_(
            expression._cloned_intersection(
                [s1c1, s3c1], [s2c1, s1c2]
            ),
            set([s1c1])
        )

    def test_cloned_difference(self):
        t1 = table('t1', column('x'))
        t2 = table('t2', column('x'))

        s1 = t1.select()
        s2 = t2.select()
        s3 = t1.select()

        s1c1 = s1._clone()
        s1c2 = s1._clone()
        s2c1 = s2._clone()
        s2c2 = s2._clone()
        s3c1 = s3._clone()

        eq_(
            expression._cloned_difference(
                [s1c1, s2c1, s3c1], [s2c1, s1c2]
            ),
            set([s3c1])
        )

    def test_distance_on_aliases(self):
        a1 = table1.alias('a1')
        for s in (select([a1, table1], use_labels=True),
                  select([table1, a1], use_labels=True)):
            assert s.corresponding_column(table1.c.col1) \
                is s.c.table1_col1
            assert s.corresponding_column(a1.c.col1) is s.c.a1_col1

    def test_join_against_self(self):
        jj = select([table1.c.col1.label('bar_col1')])
        jjj = join(table1, jj, table1.c.col1 == jj.c.bar_col1)

        # test column directly against itself

        assert jjj.corresponding_column(jjj.c.table1_col1) \
            is jjj.c.table1_col1
        assert jjj.corresponding_column(jj.c.bar_col1) is jjj.c.bar_col1

        # test alias of the join

        j2 = jjj.alias('foo')
        assert j2.corresponding_column(table1.c.col1) \
            is j2.c.table1_col1

    def test_clone_append_column(self):
        sel = select([literal_column('1').label('a')])
        eq_(list(sel.c.keys()), ['a'])
        cloned = visitors.ReplacingCloningVisitor().traverse(sel)
        cloned.append_column(literal_column('2').label('b'))
        cloned.append_column(func.foo())
        eq_(list(cloned.c.keys()), ['a', 'b', 'foo()'])

    def test_append_column_after_replace_selectable(self):
        basesel = select([literal_column('1').label('a')])
        tojoin = select([
            literal_column('1').label('a'),
            literal_column('2').label('b')
        ])
        basefrom = basesel.alias('basefrom')
        joinfrom = tojoin.alias('joinfrom')
        sel = select([basefrom.c.a])
        replaced = sel.replace_selectable(
            basefrom,
            basefrom.join(joinfrom, basefrom.c.a == joinfrom.c.a)
        )
        self.assert_compile(
            replaced,
            "SELECT basefrom.a FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a"
        )
        replaced.append_column(joinfrom.c.b)
        self.assert_compile(
            replaced,
            "SELECT basefrom.a, joinfrom.b FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a"
        )

    def test_against_cloned_non_table(self):
        # test that corresponding column digs across
        # clone boundaries with anonymous labeled elements
        col = func.count().label('foo')
        sel = select([col])

        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)
        assert sel2.corresponding_column(col) is sel2.c.foo

        sel3 = visitors.ReplacingCloningVisitor().traverse(sel2)
        assert sel3.corresponding_column(col) is sel3.c.foo

    def test_with_only_generative(self):
        s1 = table1.select().as_scalar()
        self.assert_compile(
            s1.with_only_columns([s1]),
            "SELECT (SELECT table1.col1, table1.col2, "
            "table1.col3, table1.colx FROM table1) AS anon_1"
        )

    def test_type_coerce_preserve_subq(self):
        class MyType(TypeDecorator):
            impl = Integer

        stmt = select([type_coerce(column('x'), MyType).label('foo')])
        stmt2 = stmt.select()
        assert isinstance(stmt._raw_columns[0].type, MyType)
        assert isinstance(stmt.c.foo.type, MyType)
        assert isinstance(stmt2.c.foo.type, MyType)

    def test_select_on_table(self):
        sel = select([table1, table2], use_labels=True)

        assert sel.corresponding_column(table1.c.col1) \
            is sel.c.table1_col1
        assert sel.corresponding_column(
            table1.c.col1,
            require_embedded=True) is sel.c.table1_col1
        assert table1.corresponding_column(sel.c.table1_col1) \
            is table1.c.col1
        assert table1.corresponding_column(sel.c.table1_col1,
                                           require_embedded=True) is None

    def test_join_against_join(self):
        j = outerjoin(table1, table2, table1.c.col1 == table2.c.col2)
        jj = select([table1.c.col1.label('bar_col1')],
                    from_obj=[j]).alias('foo')
        jjj = join(table1, jj, table1.c.col1 == jj.c.bar_col1)
        assert jjj.corresponding_column(jjj.c.table1_col1) \
            is jjj.c.table1_col1
        j2 = jjj.alias('foo')
        assert j2.corresponding_column(jjj.c.table1_col1) \
            is j2.c.table1_col1
        assert jjj.corresponding_column(jj.c.bar_col1) is jj.c.bar_col1

    def test_table_alias(self):
        a = table1.alias('a')

        j = join(a, table2)

        criterion = a.c.col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_union(self):

        # tests that we can correspond a column in a Select statement
        # with a certain Table, against a column in a Union where one of
        # its underlying Selects matches to that same Table

        u = select([table1.c.col1,
                    table1.c.col2,
                    table1.c.col3,
                    table1.c.colx,
                    null().label('coly')]).union(select([table2.c.col1,
                                                         table2.c.col2,
                                                         table2.c.col3,
                                                         null().label('colx'),
                                                         table2.c.coly]))
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)

        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2

    def test_union_precedence(self):
        # conflicting column correspondence should be resolved based on
        # the order of the select()s in the union

        s1 = select([table1.c.col1, table1.c.col2])
        s2 = select([table1.c.col2, table1.c.col1])
        s3 = select([table1.c.col3, table1.c.colx])
        s4 = select([table1.c.colx, table1.c.col3])

        u1 = union(s1, s2)
        assert u1.corresponding_column(table1.c.col1) is u1.c.col1
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2

        u1 = union(s1, s2, s3, s4)
        assert u1.corresponding_column(table1.c.col1) is u1.c.col1
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2
        assert u1.corresponding_column(table1.c.colx) is u1.c.col2
        assert u1.corresponding_column(table1.c.col3) is u1.c.col1

    def test_singular_union(self):
        u = union(select([table1.c.col1, table1.c.col2, table1.c.col3]), select(
            [table1.c.col1, table1.c.col2, table1.c.col3]))
        u = union(select([table1.c.col1, table1.c.col2, table1.c.col3]))
        assert u.c.col1 is not None
        assert u.c.col2 is not None
        assert u.c.col3 is not None

    def test_alias_union(self):

        # same as testunion, except its an alias of the union

        u = select([table1.c.col1,
                    table1.c.col2,
                    table1.c.col3,
                    table1.c.colx,
                    null().label('coly')]).union(select([table2.c.col1,
                                                         table2.c.col2,
                                                         table2.c.col3,
                                                         null().label('colx'),
                                                         table2.c.coly])).alias('analias')
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
        assert s2.corresponding_column(u.c.coly) is s2.c.table2_coly

    def test_union_of_alias(self):
        s1 = select([table1.c.col1, table1.c.col2])
        s2 = select([table1.c.col1, table1.c.col2]).alias()

        u1 = union(s1, s2)
        assert u1.corresponding_column(s1.c.col1) is u1.c.col1
        assert u1.corresponding_column(s2.c.col1) is u1.c.col1

        u2 = union(s2, s1)
        assert u2.corresponding_column(s1.c.col1) is u2.c.col1
        assert u2.corresponding_column(s2.c.col1) is u2.c.col1

    def test_union_of_text(self):
        s1 = select([table1.c.col1, table1.c.col2])
        s2 = text("select col1, col2 from foo").columns(
            column('col1'), column('col2'))

        u1 = union(s1, s2)
        assert u1.corresponding_column(s1.c.col1) is u1.c.col1
        assert u1.corresponding_column(s2.c.col1) is u1.c.col1

        u2 = union(s2, s1)
        assert u2.corresponding_column(s1.c.col1) is u2.c.col1
        assert u2.corresponding_column(s2.c.col1) is u2.c.col1

    @testing.emits_warning("Column 'col1'")
    def test_union_dupe_keys(self):
        s1 = select([table1.c.col1, table1.c.col2, table2.c.col1])
        s2 = select([table2.c.col1, table2.c.col2, table2.c.col3])
        u1 = union(s1, s2)

        assert u1.corresponding_column(
            s1.c._all_columns[0]) is u1.c._all_columns[0]
        assert u1.corresponding_column(s2.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(s1.c.col2) is u1.c.col2
        assert u1.corresponding_column(s2.c.col2) is u1.c.col2

        assert u1.corresponding_column(s2.c.col3) is u1.c._all_columns[2]

        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[2]
        assert u1.corresponding_column(table2.c.col3) is u1.c._all_columns[2]

    @testing.emits_warning("Column 'col1'")
    def test_union_alias_dupe_keys(self):
        s1 = select([table1.c.col1, table1.c.col2, table2.c.col1]).alias()
        s2 = select([table2.c.col1, table2.c.col2, table2.c.col3])
        u1 = union(s1, s2)

        assert u1.corresponding_column(
            s1.c._all_columns[0]) is u1.c._all_columns[0]
        assert u1.corresponding_column(s2.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(s1.c.col2) is u1.c.col2
        assert u1.corresponding_column(s2.c.col2) is u1.c.col2

        assert u1.corresponding_column(s2.c.col3) is u1.c._all_columns[2]

        # this differs from the non-alias test because table2.c.col1 is
        # more directly at s2.c.col1 than it is s1.c.col1.
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(table2.c.col3) is u1.c._all_columns[2]

    @testing.emits_warning("Column 'col1'")
    def test_union_alias_dupe_keys_grouped(self):
        s1 = select([table1.c.col1, table1.c.col2, table2.c.col1]).\
            limit(1).alias()
        s2 = select([table2.c.col1, table2.c.col2, table2.c.col3]).limit(1)
        u1 = union(s1, s2)

        assert u1.corresponding_column(
            s1.c._all_columns[0]) is u1.c._all_columns[0]
        assert u1.corresponding_column(s2.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(s1.c.col2) is u1.c.col2
        assert u1.corresponding_column(s2.c.col2) is u1.c.col2

        assert u1.corresponding_column(s2.c.col3) is u1.c._all_columns[2]

        # this differs from the non-alias test because table2.c.col1 is
        # more directly at s2.c.col1 than it is s1.c.col1.
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(table2.c.col3) is u1.c._all_columns[2]

    def test_select_union(self):

        # like testaliasunion, but off a Select off the union.

        u = select([table1.c.col1,
                    table1.c.col2,
                    table1.c.col3,
                    table1.c.colx,
                    null().label('coly')]).union(select([table2.c.col1,
                                                         table2.c.col2,
                                                         table2.c.col3,
                                                         null().label('colx'),
                                                         table2.c.coly])).alias('analias')
        s = select([u])
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert s.corresponding_column(s1.c.table1_col2) is s.c.col2
        assert s.corresponding_column(s2.c.table2_col2) is s.c.col2

    def test_union_against_join(self):

        # same as testunion, except its an alias of the union

        u = select([table1.c.col1,
                    table1.c.col2,
                    table1.c.col3,
                    table1.c.colx,
                    null().label('coly')]).union(select([table2.c.col1,
                                                         table2.c.col2,
                                                         table2.c.col3,
                                                         null().label('colx'),
                                                         table2.c.coly])).alias('analias')
        j1 = table1.join(table2)
        assert u.corresponding_column(j1.c.table1_colx) is u.c.colx
        assert j1.corresponding_column(u.c.colx) is j1.c.table1_colx

    def test_join(self):
        a = join(table1, table2)
        print(str(a.select(use_labels=True)))
        b = table2.alias('b')
        j = join(a, b)
        print(str(j))
        criterion = a.c.table1_col1 == b.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_select_alias(self):
        a = table1.select().alias('a')
        j = join(a, table2)

        criterion = a.c.col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_select_labels(self):
        a = table1.select(use_labels=True)
        j = join(a, table2)

        criterion = a.c.table1_col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_scalar_cloned_comparator(self):
        sel = select([table1.c.col1]).as_scalar()
        expr = sel == table1.c.col1

        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)

        expr2 = sel2 == table1.c.col1
        is_(expr2.left, sel2)

    def test_column_labels(self):
        a = select([table1.c.col1.label('acol1'),
                    table1.c.col2.label('acol2'),
                    table1.c.col3.label('acol3')])
        j = join(a, table2)
        criterion = a.c.acol1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_labeled_select_correspoinding(self):
        l1 = select([func.max(table1.c.col1)]).label('foo')

        s = select([l1])
        eq_(s.corresponding_column(l1), s.c.foo)

        s = select([table1.c.col1, l1])
        eq_(s.corresponding_column(l1), s.c.foo)

    def test_select_alias_labels(self):
        a = table2.select(use_labels=True).alias('a')
        j = join(a, table1)

        criterion = table1.c.col1 == a.c.table2_col2
        self.assert_(criterion.compare(j.onclause))

    def test_table_joined_to_select_of_table(self):
        metadata = MetaData()
        a = Table('a', metadata,
                  Column('id', Integer, primary_key=True))

        j2 = select([a.c.id.label('aid')]).alias('bar')

        j3 = a.join(j2, j2.c.aid == a.c.id)

        j4 = select([j3]).alias('foo')
        assert j4.corresponding_column(j2.c.aid) is j4.c.aid
        assert j4.corresponding_column(a.c.id) is j4.c.id

    def test_two_metadata_join_raises(self):
        m = MetaData()
        m2 = MetaData()

        t1 = Table('t1', m, Column('id', Integer), Column('id2', Integer))
        t2 = Table('t2', m, Column('id', Integer, ForeignKey('t1.id')))
        t3 = Table('t3', m2, Column('id', Integer, ForeignKey('t1.id2')))

        s = select([t2, t3], use_labels=True)

        assert_raises(exc.NoReferencedTableError, s.join, t1)

    def test_multi_label_chain_naming_col(self):
        # See [ticket:2167] for this one.
        l1 = table1.c.col1.label('a')
        l2 = select([l1]).label('b')
        s = select([l2])
        assert s.c.b is not None
        self.assert_compile(
            s.select(),
            "SELECT b FROM (SELECT (SELECT table1.col1 AS a FROM table1) AS b)"
        )

        s2 = select([s.label('c')])
        self.assert_compile(
            s2.select(),
            "SELECT c FROM (SELECT (SELECT (SELECT table1.col1 AS a FROM table1) AS b) AS c)"
        )

    def test_self_referential_select_raises(self):
        t = table('t', column('x'))

        s = select([t])

        s.append_whereclause(s.c.x > 5)
        assert_raises_message(
            exc.InvalidRequestError,
            r"select\(\) construct refers to itself as a FROM",
            s.compile
        )

    def test_unusual_column_elements_text(self):
        """test that .c excludes text()."""

        s = select([table1.c.col1, text("foo")])
        eq_(
            list(s.c),
            [s.c.col1]
        )

    def test_unusual_column_elements_clauselist(self):
        """Test that raw ClauseList is expanded into .c."""

        from sqlalchemy.sql.expression import ClauseList
        s = select([table1.c.col1, ClauseList(table1.c.col2, table1.c.col3)])
        eq_(
            list(s.c),
            [s.c.col1, s.c.col2, s.c.col3]
        )

    def test_unusual_column_elements_boolean_clauselist(self):
        """test that BooleanClauseList is placed as single element in .c."""

        c2 = and_(table1.c.col2 == 5, table1.c.col3 == 4)
        s = select([table1.c.col1, c2])
        eq_(
            list(s.c),
            [s.c.col1, s.corresponding_column(c2)]
        )

    def test_from_list_deferred_constructor(self):
        c1 = Column('c1', Integer)
        c2 = Column('c2', Integer)

        s = select([c1])

        t = Table('t', MetaData(), c1, c2)

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        self.assert_compile(select([c1]),
                            "SELECT t.c1 FROM t")
        self.assert_compile(select([c2]),
                            "SELECT t.c2 FROM t")

    def test_from_list_deferred_whereclause(self):
        c1 = Column('c1', Integer)
        c2 = Column('c2', Integer)

        s = select([c1]).where(c1 == 5)

        t = Table('t', MetaData(), c1, c2)

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        self.assert_compile(select([c1]),
                            "SELECT t.c1 FROM t")
        self.assert_compile(select([c2]),
                            "SELECT t.c2 FROM t")

    def test_from_list_deferred_fromlist(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer))

        c1 = Column('c1', Integer)
        s = select([c1]).where(c1 == 5).select_from(t1)

        t2 = Table('t2', MetaData(), c1)

        eq_(c1._from_objects, [t2])

        self.assert_compile(select([c1]),
                            "SELECT t2.c1 FROM t2")

    def test_from_list_deferred_cloning(self):
        c1 = Column('c1', Integer)
        c2 = Column('c2', Integer)

        s = select([c1])
        s2 = select([c2])
        s3 = sql_util.ClauseAdapter(s).traverse(s2)

        Table('t', MetaData(), c1, c2)

        self.assert_compile(
            s3,
            "SELECT t.c2 FROM t"
        )

    def test_from_list_with_columns(self):
        table1 = table('t1', column('a'))
        table2 = table('t2', column('b'))
        s1 = select([table1.c.a, table2.c.b])
        self.assert_compile(s1,
                            "SELECT t1.a, t2.b FROM t1, t2"
                            )
        s2 = s1.with_only_columns([table2.c.b])
        self.assert_compile(s2,
                            "SELECT t2.b FROM t2"
                            )

        s3 = sql_util.ClauseAdapter(table1).traverse(s1)
        self.assert_compile(s3,
                            "SELECT t1.a, t2.b FROM t1, t2"
                            )
        s4 = s3.with_only_columns([table2.c.b])
        self.assert_compile(s4,
                            "SELECT t2.b FROM t2"
                            )

    def test_from_list_warning_against_existing(self):
        c1 = Column('c1', Integer)
        s = select([c1])

        # force a compile.
        self.assert_compile(
            s,
            "SELECT c1"
        )

        Table('t', MetaData(), c1)

        self.assert_compile(
            s,
            "SELECT t.c1 FROM t"
        )

    def test_from_list_recovers_after_warning(self):
        c1 = Column('c1', Integer)
        c2 = Column('c2', Integer)

        s = select([c1])

        # force a compile.
        eq_(str(s), "SELECT c1")

        @testing.emits_warning()
        def go():
            return Table('t', MetaData(), c1, c2)
        t = go()

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        # 's' has been baked.  Can't afford
        # not caching select._froms.
        # hopefully the warning will clue the user
        self.assert_compile(s, "SELECT t.c1 FROM t")
        self.assert_compile(select([c1]), "SELECT t.c1 FROM t")
        self.assert_compile(select([c2]), "SELECT t.c2 FROM t")

    def test_label_gen_resets_on_table(self):
        c1 = Column('c1', Integer)
        eq_(c1._label, "c1")
        Table('t1', MetaData(), c1)
        eq_(c1._label, "t1_c1")


class RefreshForNewColTest(fixtures.TestBase):

    def test_join_uninit(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        j = a.join(b, a.c.x == b.c.y)

        q = column('q')
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_join_init(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        j = a.join(b, a.c.x == b.c.y)
        j.c
        q = column('q')
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_join_samename_init(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        j = a.join(b, a.c.x == b.c.y)
        j.c
        q = column('x')
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_x is q

    def test_select_samename_init(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        s = select([a, b]).apply_labels()
        s.c
        q = column('x')
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.c.b_x.proxy_set

    def test_aliased_select_samename_uninit(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        s = select([a, b]).apply_labels().alias()
        q = column('x')
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.c.b_x.proxy_set

    def test_aliased_select_samename_init(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        s = select([a, b]).apply_labels().alias()
        s.c
        q = column('x')
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.c.b_x.proxy_set

    def test_aliased_select_irrelevant(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        c = table('c', column('z'))
        s = select([a, b]).apply_labels().alias()
        s.c
        q = column('x')
        c.append_column(q)
        s._refresh_for_new_column(q)
        assert 'c_x' not in s.c

    def test_aliased_select_no_cols_clause(self):
        a = table('a', column('x'))
        s = select([a.c.x]).apply_labels().alias()
        s.c
        q = column('q')
        a.append_column(q)
        s._refresh_for_new_column(q)
        assert 'a_q' not in s.c

    def test_union_uninit(self):
        a = table('a', column('x'))
        s1 = select([a])
        s2 = select([a])
        s3 = s1.union(s2)
        q = column('q')
        a.append_column(q)
        s3._refresh_for_new_column(q)
        assert a.c.q in s3.c.q.proxy_set

    def test_union_init_raises(self):
        a = table('a', column('x'))
        s1 = select([a])
        s2 = select([a])
        s3 = s1.union(s2)
        s3.c
        q = column('q')
        a.append_column(q)
        assert_raises_message(
            NotImplementedError,
            "CompoundSelect constructs don't support addition of "
            "columns to underlying selectables",
            s3._refresh_for_new_column, q
        )

    def test_nested_join_uninit(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        c = table('c', column('z'))
        j = a.join(b, a.c.x == b.c.y).join(c, b.c.y == c.c.z)

        q = column('q')
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_nested_join_init(self):
        a = table('a', column('x'))
        b = table('b', column('y'))
        c = table('c', column('z'))
        j = a.join(b, a.c.x == b.c.y).join(c, b.c.y == c.c.z)

        j.c
        q = column('q')
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_fk_table(self):
        m = MetaData()
        fk = ForeignKey('x.id')
        Table('x', m, Column('id', Integer))
        a = Table('a', m, Column('x', Integer, fk))
        a.c

        q = Column('q', Integer)
        a.append_column(q)
        a._refresh_for_new_column(q)
        eq_(a.foreign_keys, set([fk]))

        fk2 = ForeignKey('g.id')
        p = Column('p', Integer, fk2)
        a.append_column(p)
        a._refresh_for_new_column(p)
        eq_(a.foreign_keys, set([fk, fk2]))

    def test_fk_join(self):
        m = MetaData()
        fk = ForeignKey('x.id')
        Table('x', m, Column('id', Integer))
        a = Table('a', m, Column('x', Integer, fk))
        b = Table('b', m, Column('y', Integer))
        j = a.join(b, a.c.x == b.c.y)
        j.c

        q = Column('q', Integer)
        b.append_column(q)
        j._refresh_for_new_column(q)
        eq_(j.foreign_keys, set([fk]))

        fk2 = ForeignKey('g.id')
        p = Column('p', Integer, fk2)
        b.append_column(p)
        j._refresh_for_new_column(p)
        eq_(j.foreign_keys, set([fk, fk2]))


class AnonLabelTest(fixtures.TestBase):

    """Test behaviors fixed by [ticket:2168]."""

    def test_anon_labels_named_column(self):
        c1 = column('x')

        assert c1.label(None) is not c1
        eq_(str(select([c1.label(None)])), "SELECT x AS x_1")

    def test_anon_labels_literal_column(self):
        c1 = literal_column('x')
        assert c1.label(None) is not c1
        eq_(str(select([c1.label(None)])), "SELECT x AS x_1")

    def test_anon_labels_func(self):
        c1 = func.count('*')
        assert c1.label(None) is not c1

        eq_(str(select([c1])), "SELECT count(:count_2) AS count_1")
        c2 = select([c1]).compile()

        eq_(str(select([c1.label(None)])), "SELECT count(:count_2) AS count_1")

    def test_named_labels_named_column(self):
        c1 = column('x')
        eq_(str(select([c1.label('y')])), "SELECT x AS y")

    def test_named_labels_literal_column(self):
        c1 = literal_column('x')
        eq_(str(select([c1.label('y')])), "SELECT x AS y")


class JoinAliasingTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_flat_ok_on_non_join(self):
        a = table('a', column('a'))
        s = a.select()
        self.assert_compile(
            s.alias(flat=True).select(),
            "SELECT anon_1.a FROM (SELECT a.a AS a FROM a) AS anon_1"
        )

    def test_join_alias(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b).alias(),
            "SELECT a.a AS a_a, b.b AS b_b FROM a JOIN b ON a.a = b.b"
        )

    def test_join_standalone_alias(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        self.assert_compile(
            alias(a.join(b, a.c.a == b.c.b)),
            "SELECT a.a AS a_a, b.b AS b_b FROM a JOIN b ON a.a = b.b"
        )

    def test_join_alias_flat(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b).alias(flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b"
        )

    def test_join_standalone_alias_flat(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        self.assert_compile(
            alias(a.join(b, a.c.a == b.c.b), flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b"
        )

    def test_composed_join_alias_flat(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        c = table('c', column('c'))
        d = table('d', column('d'))

        j1 = a.join(b, a.c.a == b.c.b)
        j2 = c.join(d, c.c.c == d.c.d)
        self.assert_compile(
            j1.join(j2, b.c.b == c.c.c).alias(flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b JOIN "
            "(c AS c_1 JOIN d AS d_1 ON c_1.c = d_1.d) ON b_1.b = c_1.c"
        )

    def test_composed_join_alias(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        c = table('c', column('c'))
        d = table('d', column('d'))

        j1 = a.join(b, a.c.a == b.c.b)
        j2 = c.join(d, c.c.c == d.c.d)
        self.assert_compile(
            select([j1.join(j2, b.c.b == c.c.c).alias()]),
            "SELECT anon_1.a_a, anon_1.b_b, anon_1.c_c, anon_1.d_d "
            "FROM (SELECT a.a AS a_a, b.b AS b_b, c.c AS c_c, d.d AS d_d "
            "FROM a JOIN b ON a.a = b.b "
            "JOIN (c JOIN d ON c.c = d.d) ON b.b = c.c) AS anon_1"
        )


class JoinConditionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_join_condition(self):
        m = MetaData()
        t1 = Table('t1', m, Column('id', Integer))
        t2 = Table('t2', m,
                   Column('id', Integer),
                   Column('t1id', ForeignKey('t1.id')))
        t3 = Table('t3', m,
                   Column('id', Integer),
                   Column('t1id', ForeignKey('t1.id')),
                   Column('t2id', ForeignKey('t2.id')))
        t4 = Table('t4', m, Column('id', Integer),
                   Column('t2id', ForeignKey('t2.id')))
        t5 = Table('t5', m,
                   Column('t1id1', ForeignKey('t1.id')),
                   Column('t1id2', ForeignKey('t1.id')),
                   )

        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)

        for (left, right, a_subset, expected) in [
            (t1, t2, None, t1.c.id == t2.c.t1id),
            (t1t2, t3, t2, t1t2.c.t2_id == t3.c.t2id),
            (t2t3, t1, t3, t1.c.id == t3.c.t1id),
            (t2t3, t4, None, t2t3.c.t2_id == t4.c.t2id),
            (t2t3, t4, t3, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, None, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t1, t2t3.c.t2_id == t4.c.t2id),
            (t1t2, t2t3, t2, t1t2.c.t2_id == t2t3.c.t3_t2id),
        ]:
            assert expected.compare(
                sql_util.join_condition(
                    left,
                    right,
                    a_subset=a_subset))

        # these are ambiguous, or have no joins
        for left, right, a_subset in [
            (t1t2, t3, None),
            (t2t3, t1, None),
            (t1, t4, None),
            (t1t2, t2t3, None),
            (t5, t1, None),
            (t5.select(use_labels=True), t1, None)
        ]:
            assert_raises(
                exc.ArgumentError,
                sql_util.join_condition,
                left, right, a_subset=a_subset
            )

        als = t2t3.alias()
        # test join's behavior, including natural
        for left, right, expected in [
            (t1, t2, t1.c.id == t2.c.t1id),
            (t1t2, t3, t1t2.c.t2_id == t3.c.t2id),
            (t2t3, t1, t1.c.id == t3.c.t1id),
            (t2t3, t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3, t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id == t4.c.t2id),
            (t1t2, als, t1t2.c.t2_id == als.c.t3_t2id)
        ]:
            assert expected.compare(
                left.join(right).onclause
            )

        # these are right-nested joins
        j = t1t2.join(t2t3)
        assert j.onclause.compare(t2.c.id == t3.c.t2id)
        self.assert_compile(
            j, "t1 JOIN t2 ON t1.id = t2.t1id JOIN "
            "(t2 JOIN t3 ON t2.id = t3.t2id) ON t2.id = t3.t2id")

        st2t3 = t2t3.select(use_labels=True)
        j = t1t2.join(st2t3)
        assert j.onclause.compare(t2.c.id == st2t3.c.t3_t2id)
        self.assert_compile(
            j, "t1 JOIN t2 ON t1.id = t2.t1id JOIN "
            "(SELECT t2.id AS t2_id, t2.t1id AS t2_t1id, "
            "t3.id AS t3_id, t3.t1id AS t3_t1id, t3.t2id AS t3_t2id "
            "FROM t2 JOIN t3 ON t2.id = t3.t2id) ON t2.id = t3_t2id")

    def test_join_multiple_equiv_fks(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True)
                   )
        t2 = Table(
            't2',
            m,
            Column(
                't1id',
                Integer,
                ForeignKey('t1.id'),
                ForeignKey('t1.id')))

        assert sql_util.join_condition(t1, t2).compare(t1.c.id == t2.c.t1id)

    def test_join_cond_no_such_unrelated_table(self):
        m = MetaData()
        # bounding the "good" column with two "bad" ones is so to
        # try to get coverage to get the "continue" statements
        # in the loop...
        t1 = Table('t1', m,
                   Column('y', Integer, ForeignKey('t22.id')),
                   Column('x', Integer, ForeignKey('t2.id')),
                   Column('q', Integer, ForeignKey('t22.id')),
                   )
        t2 = Table('t2', m, Column('id', Integer))
        assert sql_util.join_condition(t1, t2).compare(t1.c.x == t2.c.id)
        assert sql_util.join_condition(t2, t1).compare(t1.c.x == t2.c.id)

    def test_join_cond_no_such_unrelated_column(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, ForeignKey('t2.id')),
                   Column('y', Integer, ForeignKey('t3.q')))
        t2 = Table('t2', m, Column('id', Integer))
        Table('t3', m, Column('id', Integer))
        assert sql_util.join_condition(t1, t2).compare(t1.c.x == t2.c.id)
        assert sql_util.join_condition(t2, t1).compare(t1.c.x == t2.c.id)

    def test_join_cond_no_such_related_table(self):
        m1 = MetaData()
        m2 = MetaData()
        t1 = Table('t1', m1, Column('x', Integer, ForeignKey('t2.id')))
        t2 = Table('t2', m2, Column('id', Integer))
        assert_raises_message(
            exc.NoReferencedTableError,
            "Foreign key associated with column 't1.x' could not find "
            "table 't2' with which to generate a foreign key to "
            "target column 'id'",
            sql_util.join_condition, t1, t2
        )

        assert_raises_message(
            exc.NoReferencedTableError,
            "Foreign key associated with column 't1.x' could not find "
            "table 't2' with which to generate a foreign key to "
            "target column 'id'",
            sql_util.join_condition, t2, t1
        )

    def test_join_cond_no_such_related_column(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, ForeignKey('t2.q')))
        t2 = Table('t2', m, Column('id', Integer))
        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for "
            "ForeignKey 't2.q' on table 't1': "
            "table 't2' has no column named 'q'",
            sql_util.join_condition, t1, t2
        )

        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for "
            "ForeignKey 't2.q' on table 't1': "
            "table 't2' has no column named 'q'",
            sql_util.join_condition, t2, t1
        )


class PrimaryKeyTest(fixtures.TestBase, AssertsExecutionResults):

    def test_join_pk_collapse_implicit(self):
        """test that redundant columns in a join get 'collapsed' into a
        minimal primary key, which is the root column along a chain of
        foreign key relationships."""

        meta = MetaData()
        a = Table('a', meta, Column('id', Integer, primary_key=True))
        b = Table('b', meta, Column('id', Integer, ForeignKey('a.id'),
                                    primary_key=True))
        c = Table('c', meta, Column('id', Integer, ForeignKey('b.id'),
                                    primary_key=True))
        d = Table('d', meta, Column('id', Integer, ForeignKey('c.id'),
                                    primary_key=True))
        assert c.c.id.references(b.c.id)
        assert not d.c.id.references(a.c.id)
        assert list(a.join(b).primary_key) == [a.c.id]
        assert list(b.join(c).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).primary_key) == [a.c.id]
        assert list(b.join(c).join(d).primary_key) == [b.c.id]
        assert list(d.join(c).join(b).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).join(d).primary_key) == [a.c.id]

    def test_join_pk_collapse_explicit(self):
        """test that redundant columns in a join get 'collapsed' into a
        minimal primary key, which is the root column along a chain of
        explicit join conditions."""

        meta = MetaData()
        a = Table('a', meta, Column('id', Integer, primary_key=True),
                  Column('x', Integer))
        b = Table('b', meta, Column('id', Integer, ForeignKey('a.id'),
                                    primary_key=True), Column('x', Integer))
        c = Table('c', meta, Column('id', Integer, ForeignKey('b.id'),
                                    primary_key=True), Column('x', Integer))
        d = Table('d', meta, Column('id', Integer, ForeignKey('c.id'),
                                    primary_key=True), Column('x', Integer))
        print(list(a.join(b, a.c.x == b.c.id).primary_key))
        assert list(a.join(b, a.c.x == b.c.id).primary_key) == [a.c.id]
        assert list(b.join(c, b.c.x == c.c.id).primary_key) == [b.c.id]
        assert list(a.join(b).join(c, c.c.id == b.c.x).primary_key) \
            == [a.c.id]
        assert list(b.join(c, c.c.x == b.c.id).join(d).primary_key) \
            == [b.c.id]
        assert list(b.join(c, c.c.id == b.c.x).join(d).primary_key) \
            == [b.c.id]
        assert list(
            d.join(
                b,
                d.c.id == b.c.id).join(
                c,
                b.c.id == c.c.x).primary_key) == [
            b.c.id]
        assert list(a.join(b).join(c, c.c.id
                                   == b.c.x).join(d).primary_key) == [a.c.id]
        assert list(a.join(b, and_(a.c.id == b.c.id, a.c.x
                                   == b.c.id)).primary_key) == [a.c.id]

    def test_init_doesnt_blowitaway(self):
        meta = MetaData()
        a = Table('a', meta,
                  Column('id', Integer, primary_key=True),
                  Column('x', Integer))
        b = Table('b', meta,
                  Column('id', Integer, ForeignKey('a.id'), primary_key=True),
                  Column('x', Integer))

        j = a.join(b)
        assert list(j.primary_key) == [a.c.id]

        j.foreign_keys
        assert list(j.primary_key) == [a.c.id]

    def test_non_column_clause(self):
        meta = MetaData()
        a = Table('a', meta,
                  Column('id', Integer, primary_key=True),
                  Column('x', Integer))
        b = Table('b', meta,
                  Column('id', Integer, ForeignKey('a.id'), primary_key=True),
                  Column('x', Integer, primary_key=True))

        j = a.join(b, and_(a.c.id == b.c.id, b.c.x == 5))
        assert str(j) == "a JOIN b ON a.id = b.id AND b.x = :x_1", str(j)
        assert list(j.primary_key) == [a.c.id, b.c.x]

    def test_onclause_direction(self):
        metadata = MetaData()

        employee = Table('Employee', metadata,
                         Column('name', String(100)),
                         Column('id', Integer, primary_key=True),
                         )

        engineer = Table('Engineer', metadata,
                         Column('id', Integer,
                                ForeignKey('Employee.id'), primary_key=True))

        eq_(util.column_set(employee.join(engineer, employee.c.id
                                          == engineer.c.id).primary_key),
            util.column_set([employee.c.id]))
        eq_(util.column_set(employee.join(engineer, engineer.c.id
                                          == employee.c.id).primary_key),
            util.column_set([employee.c.id]))


class ReduceTest(fixtures.TestBase, AssertsExecutionResults):

    def test_reduce(self):
        meta = MetaData()
        t1 = Table('t1', meta,
                   Column('t1id', Integer, primary_key=True),
                   Column('t1data', String(30)))
        t2 = Table(
            't2',
            meta,
            Column(
                't2id',
                Integer,
                ForeignKey('t1.t1id'),
                primary_key=True),
            Column(
                't2data',
                String(30)))
        t3 = Table(
            't3',
            meta,
            Column(
                't3id',
                Integer,
                ForeignKey('t2.t2id'),
                primary_key=True),
            Column(
                't3data',
                String(30)))

        eq_(util.column_set(sql_util.reduce_columns([
            t1.c.t1id,
            t1.c.t1data,
            t2.c.t2id,
            t2.c.t2data,
            t3.c.t3id,
            t3.c.t3data,
        ])), util.column_set([t1.c.t1id, t1.c.t1data, t2.c.t2data,
                              t3.c.t3data]))

    def test_reduce_selectable(self):
        metadata = MetaData()
        engineers = Table('engineers', metadata,
                          Column('engineer_id', Integer, primary_key=True),
                          Column('engineer_name', String(50)))
        managers = Table('managers', metadata,
                         Column('manager_id', Integer, primary_key=True),
                         Column('manager_name', String(50)))
        s = select([engineers,
                    managers]).where(engineers.c.engineer_name
                                     == managers.c.manager_name)
        eq_(util.column_set(sql_util.reduce_columns(list(s.c), s)),
            util.column_set([s.c.engineer_id, s.c.engineer_name,
                             s.c.manager_id]))

    def test_reduce_generation(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, primary_key=True),
                   Column('y', Integer))
        t2 = Table('t2', m, Column('z', Integer, ForeignKey('t1.x')),
                   Column('q', Integer))
        s1 = select([t1, t2])
        s2 = s1.reduce_columns(only_synonyms=False)
        eq_(
            set(s2.inner_columns),
            set([t1.c.x, t1.c.y, t2.c.q])
        )

        s2 = s1.reduce_columns()
        eq_(
            set(s2.inner_columns),
            set([t1.c.x, t1.c.y, t2.c.z, t2.c.q])
        )

    def test_reduce_only_synonym_fk(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, primary_key=True),
                   Column('y', Integer))
        t2 = Table('t2', m, Column('x', Integer, ForeignKey('t1.x')),
                   Column('q', Integer, ForeignKey('t1.y')))
        s1 = select([t1, t2])
        s1 = s1.reduce_columns(only_synonyms=True)
        eq_(
            set(s1.c),
            set([s1.c.x, s1.c.y, s1.c.q])
        )

    def test_reduce_only_synonym_lineage(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, primary_key=True),
                   Column('y', Integer),
                   Column('z', Integer)
                   )
        # test that the first appearance in the columns clause
        # wins - t1 is first, t1.c.x wins
        s1 = select([t1])
        s2 = select([t1, s1]).where(t1.c.x == s1.c.x).where(s1.c.y == t1.c.z)
        eq_(
            set(s2.reduce_columns().inner_columns),
            set([t1.c.x, t1.c.y, t1.c.z, s1.c.y, s1.c.z])
        )

        # reverse order, s1.c.x wins
        s1 = select([t1])
        s2 = select([s1, t1]).where(t1.c.x == s1.c.x).where(s1.c.y == t1.c.z)
        eq_(
            set(s2.reduce_columns().inner_columns),
            set([s1.c.x, t1.c.y, t1.c.z, s1.c.y, s1.c.z])
        )

    def test_reduce_aliased_join(self):
        metadata = MetaData()
        people = Table(
            'people', metadata, Column(
                'person_id', Integer, Sequence(
                    'person_id_seq', optional=True), primary_key=True), Column(
                'name', String(50)), Column(
                    'type', String(30)))
        engineers = Table(
            'engineers',
            metadata,
            Column('person_id', Integer, ForeignKey('people.person_id'
                                                    ), primary_key=True),
            Column('status', String(30)),
            Column('engineer_name', String(50)),
            Column('primary_language', String(50)),
        )
        managers = Table(
            'managers', metadata, Column(
                'person_id', Integer, ForeignKey('people.person_id'), primary_key=True), Column(
                'status', String(30)), Column(
                'manager_name', String(50)))
        pjoin = \
            people.outerjoin(engineers).outerjoin(managers).\
            select(use_labels=True).alias('pjoin'
                                          )
        eq_(util.column_set(sql_util.reduce_columns([pjoin.c.people_person_id,
                                                     pjoin.c.engineers_person_id,
                                                     pjoin.c.managers_person_id])),
            util.column_set([pjoin.c.people_person_id]))

    def test_reduce_aliased_union(self):
        metadata = MetaData()

        item_table = Table(
            'item',
            metadata,
            Column(
                'id',
                Integer,
                ForeignKey('base_item.id'),
                primary_key=True),
            Column(
                'dummy',
                Integer,
                default=0))
        base_item_table = Table(
            'base_item', metadata, Column(
                'id', Integer, primary_key=True), Column(
                'child_name', String(255), default=None))
        from sqlalchemy.orm.util import polymorphic_union
        item_join = polymorphic_union({
            'BaseItem':
            base_item_table.select(
                base_item_table.c.child_name
                == 'BaseItem'),
                'Item': base_item_table.join(item_table)},
            None, 'item_join')
        eq_(util.column_set(sql_util.reduce_columns([item_join.c.id,
                                                     item_join.c.dummy,
                                                     item_join.c.child_name])),
            util.column_set([item_join.c.id,
                             item_join.c.dummy,
                             item_join.c.child_name]))

    def test_reduce_aliased_union_2(self):
        metadata = MetaData()
        page_table = Table('page', metadata, Column('id', Integer,
                                                    primary_key=True))
        magazine_page_table = Table('magazine_page', metadata,
                                    Column('page_id', Integer,
                                           ForeignKey('page.id'),
                                           primary_key=True))
        classified_page_table = Table(
            'classified_page',
            metadata,
            Column(
                'magazine_page_id',
                Integer,
                ForeignKey('magazine_page.page_id'),
                primary_key=True))

        # this is essentially the union formed by the ORM's
        # polymorphic_union function. we define two versions with
        # different ordering of selects.
        #
        # the first selectable has the "real" column
        # classified_page.magazine_page_id

        pjoin = union(
            select([
                page_table.c.id,
                magazine_page_table.c.page_id,
                classified_page_table.c.magazine_page_id
            ]).
            select_from(
                page_table.join(magazine_page_table).
                join(classified_page_table)),

            select([
                page_table.c.id,
                magazine_page_table.c.page_id,
                cast(null(), Integer).label('magazine_page_id')
            ]).
            select_from(page_table.join(magazine_page_table))
        ).alias('pjoin')
        eq_(util.column_set(sql_util.reduce_columns(
            [pjoin.c.id, pjoin.c.page_id, pjoin.c.magazine_page_id])), util.column_set([pjoin.c.id]))

        # the first selectable has a CAST, which is a placeholder for
        # classified_page.magazine_page_id in the second selectable.
        # reduce_columns needs to take into account all foreign keys
        # derived from pjoin.c.magazine_page_id. the UNION construct
        # currently makes the external column look like that of the
        # first selectable only.

        pjoin = union(select([
            page_table.c.id,
            magazine_page_table.c.page_id,
            cast(null(), Integer).label('magazine_page_id')
        ]).
            select_from(page_table.join(magazine_page_table)),

            select([
                page_table.c.id,
                magazine_page_table.c.page_id,
                classified_page_table.c.magazine_page_id
            ]).
            select_from(page_table.join(magazine_page_table).
                        join(classified_page_table))
        ).alias('pjoin')
        eq_(util.column_set(sql_util.reduce_columns(
            [pjoin.c.id, pjoin.c.page_id, pjoin.c.magazine_page_id])), util.column_set([pjoin.c.id]))


class DerivedTest(fixtures.TestBase, AssertsExecutionResults):

    def test_table(self):
        meta = MetaData()

        t1 = Table('t1', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))
        t2 = Table('t2', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))

        assert t1.is_derived_from(t1)
        assert not t2.is_derived_from(t1)

    def test_alias(self):
        meta = MetaData()
        t1 = Table('t1', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))
        t2 = Table('t2', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))

        assert t1.alias().is_derived_from(t1)
        assert not t2.alias().is_derived_from(t1)
        assert not t1.is_derived_from(t1.alias())
        assert not t1.is_derived_from(t2.alias())

    def test_select(self):
        meta = MetaData()

        t1 = Table('t1', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))
        t2 = Table('t2', meta, Column('c1', Integer, primary_key=True),
                   Column('c2', String(30)))

        assert t1.select().is_derived_from(t1)
        assert not t2.select().is_derived_from(t1)

        assert select([t1, t2]).is_derived_from(t1)

        assert t1.select().alias('foo').is_derived_from(t1)
        assert select([t1, t2]).alias('foo').is_derived_from(t1)
        assert not t2.select().alias('foo').is_derived_from(t1)


class AnnotationsTest(fixtures.TestBase):

    def test_hashing(self):
        t = table('t', column('x'))

        a = t.alias()
        s = t.select()
        s2 = a.select()

        for obj in [
            t,
            t.c.x,
            a,
            s,
            s2,
            t.c.x > 1,
            (t.c.x > 1).label(None)
        ]:
            annot = obj._annotate({})
            eq_(set([obj]), set([annot]))

    def test_compare(self):
        t = table('t', column('x'), column('y'))
        x_a = t.c.x._annotate({})
        assert t.c.x.compare(x_a)
        assert x_a.compare(t.c.x)
        assert not x_a.compare(t.c.y)
        assert not t.c.y.compare(x_a)
        assert (t.c.x == 5).compare(x_a == 5)
        assert not (t.c.y == 5).compare(x_a == 5)

        s = select([t])
        x_p = s.c.x
        assert not x_a.compare(x_p)
        assert not t.c.x.compare(x_p)
        x_p_a = x_p._annotate({})
        assert x_p_a.compare(x_p)
        assert x_p.compare(x_p_a)
        assert not x_p_a.compare(x_a)

    def test_late_name_add(self):
        from sqlalchemy.schema import Column
        c1 = Column(Integer)
        c1_a = c1._annotate({"foo": "bar"})
        c1.name = 'somename'
        eq_(c1_a.name, 'somename')

    def test_late_table_add(self):
        c1 = Column("foo", Integer)
        c1_a = c1._annotate({"foo": "bar"})
        t = Table('t', MetaData(), c1)
        is_(c1_a.table, t)

    def test_basic_attrs(self):
        t = Table('t', MetaData(),
                  Column('x', Integer, info={'q': 'p'}),
                  Column('y', Integer, key='q'))
        x_a = t.c.x._annotate({})
        y_a = t.c.q._annotate({})
        t.c.x.info['z'] = 'h'

        eq_(y_a.key, 'q')
        is_(x_a.table, t)
        eq_(x_a.info, {'q': 'p', 'z': 'h'})
        eq_(t.c.x.anon_label, x_a.anon_label)

    def test_custom_constructions(self):
        from sqlalchemy.schema import Column

        class MyColumn(Column):

            def __init__(self):
                Column.__init__(self, 'foo', Integer)
            _constructor = Column

        t1 = Table('t1', MetaData(), MyColumn())
        s1 = t1.select()
        assert isinstance(t1.c.foo, MyColumn)
        assert isinstance(s1.c.foo, Column)

        annot_1 = t1.c.foo._annotate({})
        s2 = select([annot_1])
        assert isinstance(s2.c.foo, Column)
        annot_2 = s1._annotate({})
        assert isinstance(annot_2.c.foo, Column)

    def test_custom_construction_correct_anno_subclass(self):
        # [ticket:2918]
        from sqlalchemy.schema import Column
        from sqlalchemy.sql.elements import AnnotatedColumnElement

        class MyColumn(Column):
            pass

        assert isinstance(
            MyColumn('x', Integer)._annotate({"foo": "bar"}),
            AnnotatedColumnElement)

    def test_custom_construction_correct_anno_expr(self):
        # [ticket:2918]
        from sqlalchemy.schema import Column

        class MyColumn(Column):
            pass

        col = MyColumn('x', Integer)
        binary_1 = col == 5
        col_anno = MyColumn('x', Integer)._annotate({"foo": "bar"})
        binary_2 = col_anno == 5
        eq_(binary_2.left._annotations, {"foo": "bar"})

    def test_annotated_corresponding_column(self):
        table1 = table('table1', column("col1"))

        s1 = select([table1.c.col1])
        t1 = s1._annotate({})
        t2 = s1

        # t1 needs to share the same _make_proxy() columns as t2, even
        # though it's annotated.  otherwise paths will diverge once they
        # are corresponded against "inner" below.

        assert t1.c is t2.c
        assert t1.c.col1 is t2.c.col1

        inner = select([s1])

        assert inner.corresponding_column(
            t2.c.col1,
            require_embedded=False) is inner.corresponding_column(
            t2.c.col1,
            require_embedded=True) is inner.c.col1
        assert inner.corresponding_column(
            t1.c.col1,
            require_embedded=False) is inner.corresponding_column(
            t1.c.col1,
            require_embedded=True) is inner.c.col1

    def test_annotated_visit(self):
        table1 = table('table1', column("col1"), column("col2"))

        bin = table1.c.col1 == bindparam('foo', value=None)
        assert str(bin) == "table1.col1 = :foo"

        def visit_binary(b):
            b.right = table1.c.col2

        b2 = visitors.cloned_traverse(bin, {}, {'binary': visit_binary})
        assert str(b2) == "table1.col1 = table1.col2"

        b3 = visitors.cloned_traverse(bin._annotate({}), {}, {'binary':
                                                              visit_binary})
        assert str(b3) == 'table1.col1 = table1.col2'

        def visit_binary(b):
            b.left = bindparam('bar')

        b4 = visitors.cloned_traverse(b2, {}, {'binary': visit_binary})
        assert str(b4) == ":bar = table1.col2"

        b5 = visitors.cloned_traverse(b3, {}, {'binary': visit_binary})
        assert str(b5) == ":bar = table1.col2"

    def test_label_accessors(self):
        t1 = table('t1', column('c1'))
        l1 = t1.c.c1.label(None)
        is_(l1._order_by_label_element, l1)
        l1a = l1._annotate({"foo": "bar"})
        is_(l1a._order_by_label_element, l1a)

    def test_annotate_aliased(self):
        t1 = table('t1', column('c1'))
        s = select([(t1.c.c1 + 3).label('bat')])
        a = s.alias()
        a = sql_util._deep_annotate(a, {'foo': 'bar'})
        eq_(a._annotations['foo'], 'bar')
        eq_(a.element._annotations['foo'], 'bar')

    def test_annotate_expressions(self):
        table1 = table('table1', column('col1'), column('col2'))
        for expr, expected in [(table1.c.col1, 'table1.col1'),
                               (table1.c.col1 == 5,
                                'table1.col1 = :col1_1'),
                               (table1.c.col1.in_([2, 3, 4]),
                                'table1.col1 IN (:col1_1, :col1_2, '
                                ':col1_3)')]:
            eq_(str(expr), expected)
            eq_(str(expr._annotate({})), expected)
            eq_(str(sql_util._deep_annotate(expr, {})), expected)
            eq_(str(sql_util._deep_annotate(
                expr, {}, exclude=[table1.c.col1])), expected)

    def test_deannotate(self):
        table1 = table('table1', column("col1"), column("col2"))

        bin = table1.c.col1 == bindparam('foo', value=None)

        b2 = sql_util._deep_annotate(bin, {'_orm_adapt': True})
        b3 = sql_util._deep_deannotate(b2)
        b4 = sql_util._deep_deannotate(bin)

        for elem in (b2._annotations, b2.left._annotations):
            assert '_orm_adapt' in elem

        for elem in b3._annotations, b3.left._annotations, \
                b4._annotations, b4.left._annotations:
            assert elem == {}

        assert b2.left is not bin.left
        assert b3.left is not b2.left is not bin.left
        assert b4.left is bin.left  # since column is immutable
        # deannotate copies the element
        assert bin.right is not b2.right is not b3.right is not b4.right

    def test_annotate_unique_traversal(self):
        """test that items are copied only once during
        annotate, deannotate traversal

        #2453 - however note this was modified by
        #1401, and it's likely that re49563072578
        is helping us with the str() comparison
        case now, as deannotate is making
        clones again in some cases.
        """
        table1 = table('table1', column('x'))
        table2 = table('table2', column('y'))
        a1 = table1.alias()
        s = select([a1.c.x]).select_from(
            a1.join(table2, a1.c.x == table2.c.y)
        )
        for sel in (
            sql_util._deep_deannotate(s),
            visitors.cloned_traverse(s, {}, {}),
            visitors.replacement_traverse(s, {}, lambda x: None)
        ):
            # the columns clause isn't changed at all
            assert sel._raw_columns[0].table is a1
            assert sel._froms[0] is sel._froms[1].left

            eq_(str(s), str(sel))

        # when we are modifying annotations sets only
        # partially, each element is copied unconditionally
        # when encountered.
        for sel in (
            sql_util._deep_deannotate(s, {"foo": "bar"}),
            sql_util._deep_annotate(s, {'foo': 'bar'}),
        ):
            assert sel._froms[0] is not sel._froms[1].left

            # but things still work out due to
            # re49563072578
            eq_(str(s), str(sel))

    def test_annotate_varied_annot_same_col(self):
        """test two instances of the same column with different annotations
        preserving them when deep_annotate is run on them.

        """
        t1 = table('table1', column("col1"), column("col2"))
        s = select([t1.c.col1._annotate({"foo": "bar"})])
        s2 = select([t1.c.col1._annotate({"bat": "hoho"})])
        s3 = s.union(s2)
        sel = sql_util._deep_annotate(s3, {"new": "thing"})

        eq_(
            sel.selects[0]._raw_columns[0]._annotations,
            {"foo": "bar", "new": "thing"}
        )

        eq_(
            sel.selects[1]._raw_columns[0]._annotations,
            {"bat": "hoho", "new": "thing"}
        )

    def test_deannotate_2(self):
        table1 = table('table1', column("col1"), column("col2"))
        j = table1.c.col1._annotate({"remote": True}) == \
            table1.c.col2._annotate({"local": True})
        j2 = sql_util._deep_deannotate(j)
        eq_(
            j.left._annotations, {"remote": True}
        )
        eq_(
            j2.left._annotations, {}
        )

    def test_deannotate_3(self):
        table1 = table('table1', column("col1"), column("col2"),
                       column("col3"), column("col4"))
        j = and_(
            table1.c.col1._annotate({"remote": True}) ==
            table1.c.col2._annotate({"local": True}),
            table1.c.col3._annotate({"remote": True}) ==
            table1.c.col4._annotate({"local": True})
        )
        j2 = sql_util._deep_deannotate(j)
        eq_(
            j.clauses[0].left._annotations, {"remote": True}
        )
        eq_(
            j2.clauses[0].left._annotations, {}
        )

    def test_annotate_fromlist_preservation(self):
        """test the FROM list in select still works
        even when multiple annotate runs have created
        copies of the same selectable

        #2453, continued

        """
        table1 = table('table1', column('x'))
        table2 = table('table2', column('y'))
        a1 = table1.alias()
        s = select([a1.c.x]).select_from(
            a1.join(table2, a1.c.x == table2.c.y)
        )

        assert_s = select([select([s])])
        for fn in (
            sql_util._deep_deannotate,
            lambda s: sql_util._deep_annotate(s, {'foo': 'bar'}),
            lambda s: visitors.cloned_traverse(s, {}, {}),
            lambda s: visitors.replacement_traverse(s, {}, lambda x: None)
        ):

            sel = fn(select([fn(select([fn(s)]))]))
            eq_(str(assert_s), str(sel))

    def test_bind_unique_test(self):
        table('t', column('a'), column('b'))

        b = bindparam("bind", value="x", unique=True)

        # the annotation of "b" should render the
        # same.  The "unique" test in compiler should
        # also pass, [ticket:2425]
        eq_(str(or_(b, b._annotate({"foo": "bar"}))),
            ":bind_1 OR :bind_1")

    def test_comparators_cleaned_out_construction(self):
        c = column('a')

        comp1 = c.comparator

        c1 = c._annotate({"foo": "bar"})
        comp2 = c1.comparator
        assert comp1 is not comp2

    def test_comparators_cleaned_out_reannotate(self):
        c = column('a')

        c1 = c._annotate({"foo": "bar"})
        comp1 = c1.comparator

        c2 = c1._annotate({"bat": "hoho"})
        comp2 = c2.comparator

        assert comp1 is not comp2

    def test_comparator_cleanout_integration(self):
        c = column('a')

        c1 = c._annotate({"foo": "bar"})
        comp1 = c1.comparator

        c2 = c1._annotate({"bat": "hoho"})
        comp2 = c2.comparator

        assert (c2 == 5).left._annotations == {"foo": "bar", "bat": "hoho"}


class ReprTest(fixtures.TestBase):
    def test_ensure_repr_elements(self):
        for obj in [
            elements.Cast(1, 2),
            elements.TypeClause(String()),
            elements.ColumnClause('x'),
            elements.BindParameter('q'),
            elements.Null(),
            elements.True_(),
            elements.False_(),
            elements.ClauseList(),
            elements.BooleanClauseList.and_(),
            elements.Tuple(),
            elements.Case([]),
            elements.Extract('foo', column('x')),
            elements.UnaryExpression(column('x')),
            elements.Grouping(column('x')),
            elements.Over(func.foo()),
            elements.Label('q', column('x')),
        ]:
            repr(obj)


class WithLabelsTest(fixtures.TestBase):

    def _assert_labels_warning(self, s):
        assert_raises_message(
            exc.SAWarning,
            r"replaced by Column.*, which has the same key",
            lambda: s.c
        )

    def _assert_result_keys(self, s, keys):
        compiled = s.compile()
        eq_(set(compiled._create_result_map()), set(keys))

    def _assert_subq_result_keys(self, s, keys):
        compiled = s.select().compile()
        eq_(set(compiled._create_result_map()), set(keys))

    def _names_overlap(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer))
        t2 = Table('t2', m, Column('x', Integer))
        return select([t1, t2])

    def test_names_overlap_nolabel(self):
        sel = self._names_overlap()
        self._assert_labels_warning(sel)
        self._assert_result_keys(sel, ['x'])

    def test_names_overlap_label(self):
        sel = self._names_overlap().apply_labels()
        eq_(
            list(sel.c.keys()),
            ['t1_x', 't2_x']
        )
        self._assert_result_keys(sel, ['t1_x', 't2_x'])

    def _names_overlap_keys_dont(self):
        m = MetaData()
        t1 = Table('t1', m, Column('x', Integer, key='a'))
        t2 = Table('t2', m, Column('x', Integer, key='b'))
        return select([t1, t2])

    def test_names_overlap_keys_dont_nolabel(self):
        sel = self._names_overlap_keys_dont()
        eq_(
            list(sel.c.keys()),
            ['a', 'b']
        )
        self._assert_result_keys(sel, ['x'])

    def test_names_overlap_keys_dont_label(self):
        sel = self._names_overlap_keys_dont().apply_labels()
        eq_(
            list(sel.c.keys()),
            ['t1_a', 't2_b']
        )
        self._assert_result_keys(sel, ['t1_x', 't2_x'])

    def _labels_overlap(self):
        m = MetaData()
        t1 = Table('t', m, Column('x_id', Integer))
        t2 = Table('t_x', m, Column('id', Integer))
        return select([t1, t2])

    def test_labels_overlap_nolabel(self):
        sel = self._labels_overlap()
        eq_(
            list(sel.c.keys()),
            ['x_id', 'id']
        )
        self._assert_result_keys(sel, ['x_id', 'id'])

    def test_labels_overlap_label(self):
        sel = self._labels_overlap().apply_labels()
        t2 = sel.froms[1]
        eq_(
            list(sel.c.keys()),
            ['t_x_id', t2.c.id.anon_label]
        )
        self._assert_result_keys(sel, ['t_x_id', 'id_1'])
        self._assert_subq_result_keys(sel, ['t_x_id', 'id_1'])

    def _labels_overlap_keylabels_dont(self):
        m = MetaData()
        t1 = Table('t', m, Column('x_id', Integer, key='a'))
        t2 = Table('t_x', m, Column('id', Integer, key='b'))
        return select([t1, t2])

    def test_labels_overlap_keylabels_dont_nolabel(self):
        sel = self._labels_overlap_keylabels_dont()
        eq_(list(sel.c.keys()), ['a', 'b'])
        self._assert_result_keys(sel, ['x_id', 'id'])

    def test_labels_overlap_keylabels_dont_label(self):
        sel = self._labels_overlap_keylabels_dont().apply_labels()
        eq_(list(sel.c.keys()), ['t_a', 't_x_b'])
        self._assert_result_keys(sel, ['t_x_id', 'id_1'])

    def _keylabels_overlap_labels_dont(self):
        m = MetaData()
        t1 = Table('t', m, Column('a', Integer, key='x_id'))
        t2 = Table('t_x', m, Column('b', Integer, key='id'))
        return select([t1, t2])

    def test_keylabels_overlap_labels_dont_nolabel(self):
        sel = self._keylabels_overlap_labels_dont()
        eq_(list(sel.c.keys()), ['x_id', 'id'])
        self._assert_result_keys(sel, ['a', 'b'])

    def test_keylabels_overlap_labels_dont_label(self):
        sel = self._keylabels_overlap_labels_dont().apply_labels()
        t2 = sel.froms[1]
        eq_(list(sel.c.keys()), ['t_x_id', t2.c.id.anon_label])
        self._assert_result_keys(sel, ['t_a', 't_x_b'])
        self._assert_subq_result_keys(sel, ['t_a', 't_x_b'])

    def _keylabels_overlap_labels_overlap(self):
        m = MetaData()
        t1 = Table('t', m, Column('x_id', Integer, key='x_a'))
        t2 = Table('t_x', m, Column('id', Integer, key='a'))
        return select([t1, t2])

    def test_keylabels_overlap_labels_overlap_nolabel(self):
        sel = self._keylabels_overlap_labels_overlap()
        eq_(list(sel.c.keys()), ['x_a', 'a'])
        self._assert_result_keys(sel, ['x_id', 'id'])
        self._assert_subq_result_keys(sel, ['x_id', 'id'])

    def test_keylabels_overlap_labels_overlap_label(self):
        sel = self._keylabels_overlap_labels_overlap().apply_labels()
        t2 = sel.froms[1]
        eq_(list(sel.c.keys()), ['t_x_a', t2.c.a.anon_label])
        self._assert_result_keys(sel, ['t_x_id', 'id_1'])
        self._assert_subq_result_keys(sel, ['t_x_id', 'id_1'])

    def _keys_overlap_names_dont(self):
        m = MetaData()
        t1 = Table('t1', m, Column('a', Integer, key='x'))
        t2 = Table('t2', m, Column('b', Integer, key='x'))
        return select([t1, t2])

    def test_keys_overlap_names_dont_nolabel(self):
        sel = self._keys_overlap_names_dont()
        self._assert_labels_warning(sel)
        self._assert_result_keys(sel, ['a', 'b'])

    def test_keys_overlap_names_dont_label(self):
        sel = self._keys_overlap_names_dont().apply_labels()
        eq_(
            list(sel.c.keys()),
            ['t1_x', 't2_x']
        )
        self._assert_result_keys(sel, ['t1_a', 't2_b'])


class ResultMapTest(fixtures.TestBase):

    def _fixture(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer), Column('y', Integer))
        return t

    def _mapping(self, stmt):
        compiled = stmt.compile()
        return dict(
            (elem, key)
            for key, elements in compiled._create_result_map().items()
            for elem in elements[1]
        )

    def test_select_label_alt_name(self):
        t = self._fixture()
        l1, l2 = t.c.x.label('a'), t.c.y.label('b')
        s = select([l1, l2])
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_alias_label_alt_name(self):
        t = self._fixture()
        l1, l2 = t.c.x.label('a'), t.c.y.label('b')
        s = select([l1, l2]).alias()
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_alias_column(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y
        s = select([x, y]).alias()
        mapping = self._mapping(s)

        assert t.c.x in mapping

    def test_select_alias_column_apply_labels(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y
        s = select([x, y]).apply_labels().alias()
        mapping = self._mapping(s)
        assert t.c.x in mapping

    def test_select_table_alias_column(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y

        ta = t.alias()
        s = select([ta.c.x, ta.c.y])
        mapping = self._mapping(s)
        assert x not in mapping

    def test_select_label_alt_name_table_alias_column(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y

        ta = t.alias()
        l1, l2 = ta.c.x.label('a'), ta.c.y.label('b')

        s = select([l1, l2])
        mapping = self._mapping(s)
        assert x not in mapping
        assert l1 in mapping
        assert ta.c.x not in mapping

    def test_column_subquery_exists(self):
        t = self._fixture()
        s = exists().where(t.c.x == 5).select()
        mapping = self._mapping(s)
        assert t.c.x not in mapping
        eq_(
            [type(entry[-1]) for entry in s.compile()._result_columns],
            [Boolean]
        )

    def test_plain_exists(self):
        expr = exists([1])
        eq_(type(expr.type), Boolean)
        eq_(
            [type(entry[-1]) for
             entry in select([expr]).compile()._result_columns],
            [Boolean]
        )

    def test_plain_exists_negate(self):
        expr = ~exists([1])
        eq_(type(expr.type), Boolean)
        eq_(
            [type(entry[-1]) for
             entry in select([expr]).compile()._result_columns],
            [Boolean]
        )

    def test_plain_exists_double_negate(self):
        expr = ~(~exists([1]))
        eq_(type(expr.type), Boolean)
        eq_(
            [type(entry[-1]) for
             entry in select([expr]).compile()._result_columns],
            [Boolean]
        )

    def test_column_subquery_plain(self):
        t = self._fixture()
        s1 = select([t.c.x]).where(t.c.x > 5).as_scalar()
        s2 = select([s1])
        mapping = self._mapping(s2)
        assert t.c.x not in mapping
        assert s1 in mapping
        eq_(
            [type(entry[-1]) for entry in s2.compile()._result_columns],
            [Integer]
        )

    def test_unary_boolean(self):

        s1 = select([not_(True)], use_labels=True)
        eq_(
            [type(entry[-1]) for entry in s1.compile()._result_columns],
            [Boolean]
        )

class ForUpdateTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _assert_legacy(self, leg, read=False, nowait=False):
        t = table('t', column('c'))
        s1 = select([t], for_update=leg)

        if leg is False:
            assert s1._for_update_arg is None
            assert s1.for_update is None
        else:
            eq_(
                s1._for_update_arg.read, read
            )
            eq_(
                s1._for_update_arg.nowait, nowait
            )
            eq_(s1.for_update, leg)

    def test_false_legacy(self):
        self._assert_legacy(False)

    def test_plain_true_legacy(self):
        self._assert_legacy(True)

    def test_read_legacy(self):
        self._assert_legacy("read", read=True)

    def test_nowait_legacy(self):
        self._assert_legacy("nowait", nowait=True)

    def test_read_nowait_legacy(self):
        self._assert_legacy("read_nowait", read=True, nowait=True)

    def test_legacy_setter(self):
        t = table('t', column('c'))
        s = select([t])
        s.for_update = 'nowait'
        eq_(s._for_update_arg.nowait, True)

    def test_basic_clone(self):
        t = table('t', column('c'))
        s = select([t]).with_for_update(read=True, of=t.c.c)
        s2 = visitors.ReplacingCloningVisitor().traverse(s)
        assert s2._for_update_arg is not s._for_update_arg
        eq_(s2._for_update_arg.read, True)
        eq_(s2._for_update_arg.of, [t.c.c])
        self.assert_compile(s2,
                            "SELECT t.c FROM t FOR SHARE OF t",
                            dialect="postgresql")

    def test_adapt(self):
        t = table('t', column('c'))
        s = select([t]).with_for_update(read=True, of=t.c.c)
        a = t.alias()
        s2 = sql_util.ClauseAdapter(a).traverse(s)
        eq_(s2._for_update_arg.of, [a.c.c])
        self.assert_compile(s2,
                            "SELECT t_1.c FROM t AS t_1 FOR SHARE OF t_1",
                            dialect="postgresql")
