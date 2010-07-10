"""Test various algorithmic properties of selectables."""

from sqlalchemy.test.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy import *
from sqlalchemy.test import *
from sqlalchemy.sql import util as sql_util, visitors
from sqlalchemy import exc
from sqlalchemy.sql import table, column, null
from sqlalchemy import util

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


class SelectableTest(TestBase, AssertsExecutionResults):

    def test_distance_on_labels(self):

        # same column three times

        s = select([table1.c.col1.label('c2'), table1.c.col1,
                   table1.c.col1.label('c1')])

        # didnt do this yet...col.label().make_proxy() has same
        # "distance" as col.make_proxy() so far assert
        # s.corresponding_column(table1.c.col1) is s.c.col1

        assert s.corresponding_column(s.c.col1) is s.c.col1
        assert s.corresponding_column(s.c.c1) is s.c.c1

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

        # test column directly agaisnt itself

        assert jjj.corresponding_column(jjj.c.table1_col1) \
            is jjj.c.table1_col1
        assert jjj.corresponding_column(jj.c.bar_col1) is jjj.c.bar_col1

        # test alias of the join

        j2 = jjj.alias('foo')
        assert j2.corresponding_column(table1.c.col1) \
            is j2.c.table1_col1
    
    def test_against_cloned_non_table(self):
        # test that corresponding column digs across
        # clone boundaries with anonymous labeled elements
        col = func.count().label('foo')
        sel = select([col])
        
        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)
        assert sel2.corresponding_column(col) is sel2.c.foo

        sel3 = visitors.ReplacingCloningVisitor().traverse(sel2)
        assert sel3.corresponding_column(col) is sel3.c.foo

        
    def test_select_on_table(self):
        sel = select([table1, table2], use_labels=True)

        assert sel.corresponding_column(table1.c.col1) \
            is sel.c.table1_col1
        assert sel.corresponding_column(table1.c.col1,
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

        u = select([table1.c.col1, table1.c.col2, table1.c.col3,
                   table1.c.colx, null().label('coly'
                   )]).union(select([table2.c.col1, table2.c.col2,
                             table2.c.col3, null().label('colx'),
                             table2.c.coly]))
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        c = u.corresponding_column(s1.c.table1_col2)
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
        u = union(select([table1.c.col1, table1.c.col2,
                  table1.c.col3]), select([table1.c.col1,
                  table1.c.col2, table1.c.col3]))
        u = union(select([table1.c.col1, table1.c.col2, table1.c.col3]))
        assert u.c.col1 is not None
        assert u.c.col2 is not None
        assert u.c.col3 is not None

    def test_alias_union(self):

        # same as testunion, except its an alias of the union

        u = select([table1.c.col1, table1.c.col2, table1.c.col3,
                   table1.c.colx, null().label('coly'
                   )]).union(select([table2.c.col1, table2.c.col2,
                             table2.c.col3, null().label('colx'),
                             table2.c.coly])).alias('analias')
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
        assert s2.corresponding_column(u.c.coly) is s2.c.table2_coly

    def test_select_union(self):

        # like testaliasunion, but off a Select off the union.

        u = select([table1.c.col1, table1.c.col2, table1.c.col3,
                   table1.c.colx, null().label('coly'
                   )]).union(select([table2.c.col1, table2.c.col2,
                             table2.c.col3, null().label('colx'),
                             table2.c.coly])).alias('analias')
        s = select([u])
        s1 = table1.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert s.corresponding_column(s1.c.table1_col2) is s.c.col2
        assert s.corresponding_column(s2.c.table2_col2) is s.c.col2

    def test_union_against_join(self):

        # same as testunion, except its an alias of the union

        u = select([table1.c.col1, table1.c.col2, table1.c.col3,
                   table1.c.colx, null().label('coly'
                   )]).union(select([table2.c.col1, table2.c.col2,
                             table2.c.col3, null().label('colx'),
                             table2.c.coly])).alias('analias')
        j1 = table1.join(table2)
        assert u.corresponding_column(j1.c.table1_colx) is u.c.colx
        assert j1.corresponding_column(u.c.colx) is j1.c.table1_colx

    def test_join(self):
        a = join(table1, table2)
        print str(a.select(use_labels=True))
        b = table2.alias('b')
        j = join(a, b)
        print str(j)
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
        b = Table('b', metadata,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('a.id')),
            )

        j1 = a.outerjoin(b)
        j2 = select([a.c.id.label('aid')]).alias('bar')

        j3 = a.join(j2, j2.c.aid==a.c.id)

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
    

    def test_join_condition(self):
        m = MetaData()
        t1 = Table('t1', m, Column('id', Integer))
        t2 = Table('t2', m, Column('id', Integer), Column('t1id',
                   ForeignKey('t1.id')))
        t3 = Table('t3', m, Column('id', Integer), Column('t1id',
                   ForeignKey('t1.id')), Column('t2id',
                   ForeignKey('t2.id')))
        t4 = Table('t4', m, Column('id', Integer), Column('t2id',
                   ForeignKey('t2.id')))
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
            assert expected.compare(sql_util.join_condition(left,
                                    right, a_subset=a_subset))
        
        # these are ambiguous, or have no joins
        for left, right, a_subset in [
            (t1t2, t3, None),
            (t2t3, t1, None),
            (t1, t4, None),
            (t1t2, t2t3, None),
        ]:
            assert_raises(
                exc.ArgumentError,
                sql_util.join_condition,
                left, right, a_subset=a_subset
            )
        
        als = t2t3.alias()
        # test join's behavior, including natural
        for left, right, expected in [
            (t1, t2, t1.c.id==t2.c.t1id),
            (t1t2, t3, t1t2.c.t2_id==t3.c.t2id),
            (t2t3, t1, t1.c.id==t3.c.t1id),
            (t2t3, t4, t2t3.c.t2_id==t4.c.t2id),
            (t2t3, t4, t2t3.c.t2_id==t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id==t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id==t4.c.t2id),
            (t1t2, als, t1t2.c.t2_id==als.c.t3_t2id)
        ]:
            assert expected.compare(
                left.join(right).onclause
            )



        # TODO: this raises due to right side being "grouped", and no
        # longer has FKs.  Did we want to make _FromGrouping friendlier
        # ?

        assert_raises_message(exc.ArgumentError,
                              "Perhaps you meant to convert the right "
                              "side to a subquery using alias\(\)\?",
                              t1t2.join, t2t3)
        assert_raises_message(exc.ArgumentError,
                              "Perhaps you meant to convert the right "
                              "side to a subquery using alias\(\)\?",
                              t1t2.join, t2t3.select(use_labels=True))
        

class PrimaryKeyTest(TestBase, AssertsExecutionResults):

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
        print list(a.join(b, a.c.x == b.c.id).primary_key)
        assert list(a.join(b, a.c.x == b.c.id).primary_key) == [a.c.id]
        assert list(b.join(c, b.c.x == c.c.id).primary_key) == [b.c.id]
        assert list(a.join(b).join(c, c.c.id == b.c.x).primary_key) \
            == [a.c.id]
        assert list(b.join(c, c.c.x == b.c.id).join(d).primary_key) \
            == [b.c.id]
        assert list(b.join(c, c.c.id == b.c.x).join(d).primary_key) \
            == [b.c.id]
        assert list(d.join(b, d.c.id == b.c.id).join(c, b.c.id
                    == c.c.x).primary_key) == [b.c.id]
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

        j = a.join(b, and_(a.c.id==b.c.id, b.c.x==5))
        assert str(j) == "a JOIN b ON a.id = b.id AND b.x = :x_1", str(j)
        assert list(j.primary_key) == [a.c.id, b.c.x]

    def test_onclause_direction(self):
        metadata = MetaData()

        employee = Table( 'Employee', metadata,
            Column('name', String(100)),
            Column('id', Integer, primary_key= True),
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


class ReduceTest(TestBase, AssertsExecutionResults):
    def test_reduce(self):
        meta = MetaData()
        t1 = Table('t1', meta,
            Column('t1id', Integer, primary_key=True),
            Column('t1data', String(30)))
        t2 = Table('t2', meta,
            Column('t2id', Integer, ForeignKey('t1.t1id'), primary_key=True),
            Column('t2data', String(30)))
        t3 = Table('t3', meta,
            Column('t3id', Integer, ForeignKey('t2.t2id'), primary_key=True),
            Column('t3data', String(30)))
        
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
        engineers = Table('engineers', metadata, Column('engineer_id',
                          Integer, primary_key=True),
                          Column('engineer_name', String(50)))
        managers = Table('managers', metadata, Column('manager_id',
                         Integer, primary_key=True),
                         Column('manager_name', String(50)))
        s = select([engineers,
                   managers]).where(engineers.c.engineer_name
                                    == managers.c.manager_name)
        eq_(util.column_set(sql_util.reduce_columns(list(s.c), s)),
            util.column_set([s.c.engineer_id, s.c.engineer_name,
            s.c.manager_id]))
       

    def test_reduce_aliased_join(self):
        metadata = MetaData()
        people = Table('people', metadata, Column('person_id', Integer,
                       Sequence('person_id_seq', optional=True),
                       primary_key=True), Column('name', String(50)),
                       Column('type', String(30)))
        engineers = Table(
            'engineers',
            metadata,
            Column('person_id', Integer, ForeignKey('people.person_id'
                   ), primary_key=True),
            Column('status', String(30)),
            Column('engineer_name', String(50)),
            Column('primary_language', String(50)),
            )
        managers = Table('managers', metadata, Column('person_id',
                         Integer, ForeignKey('people.person_id'),
                         primary_key=True), Column('status',
                         String(30)), Column('manager_name',
                         String(50)))
        pjoin = \
            people.outerjoin(engineers).outerjoin(managers).\
            select(use_labels=True).alias('pjoin'
                )
        eq_(util.column_set(sql_util.reduce_columns([pjoin.c.people_person_id,
            pjoin.c.engineers_person_id, pjoin.c.managers_person_id])),
            util.column_set([pjoin.c.people_person_id]))
        
    def test_reduce_aliased_union(self):
        metadata = MetaData()

        item_table = Table('item', metadata, Column('id', Integer,
                           ForeignKey('base_item.id'),
                           primary_key=True), Column('dummy', Integer,
                           default=0))
        base_item_table = Table('base_item', metadata, Column('id',
                                Integer, primary_key=True),
                                Column('child_name', String(255),
                                default=None))
        from sqlalchemy.orm.util import polymorphic_union
        item_join = polymorphic_union({
                'BaseItem':
                    base_item_table.select(
                            base_item_table.c.child_name
                            == 'BaseItem'), 
                'Item': base_item_table.join(item_table)}, 
                None, 'item_join')
        eq_(util.column_set(sql_util.reduce_columns([item_join.c.id,
            item_join.c.dummy, item_join.c.child_name])),
            util.column_set([item_join.c.id, item_join.c.dummy,
            item_join.c.child_name]))
    

    def test_reduce_aliased_union_2(self):
        metadata = MetaData()
        page_table = Table('page', metadata, Column('id', Integer,
                           primary_key=True))
        magazine_page_table = Table('magazine_page', metadata,
                                    Column('page_id', Integer,
                                    ForeignKey('page.id'),
                                    primary_key=True))
        classified_page_table = Table('classified_page', metadata,
                Column('magazine_page_id', Integer,
                ForeignKey('magazine_page.page_id'), primary_key=True))

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
        eq_(util.column_set(sql_util.reduce_columns([pjoin.c.id,
            pjoin.c.page_id, pjoin.c.magazine_page_id])),
            util.column_set([pjoin.c.id]))

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
        eq_(util.column_set(sql_util.reduce_columns([pjoin.c.id,
            pjoin.c.page_id, pjoin.c.magazine_page_id])),
            util.column_set([pjoin.c.id]))

class DerivedTest(TestBase, AssertsExecutionResults):
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

class AnnotationsTest(TestBase):
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

        assert inner.corresponding_column(t2.c.col1,
                require_embedded=False) \
            is inner.corresponding_column(t2.c.col1,
                require_embedded=True) is inner.c.col1
        assert inner.corresponding_column(t1.c.col1,
                require_embedded=False) \
            is inner.corresponding_column(t1.c.col1,
                require_embedded=True) is inner.c.col1

    def test_annotated_visit(self):
        table1 = table('table1', column("col1"), column("col2"))
        
        bin = table1.c.col1 == bindparam('foo', value=None)
        assert str(bin) == "table1.col1 = :foo"
        def visit_binary(b):
            b.right = table1.c.col2
            
        b2 = visitors.cloned_traverse(bin, {}, {'binary':visit_binary})
        assert str(b2) == "table1.col1 = table1.col2"


        b3 = visitors.cloned_traverse(bin._annotate({}), {}, {'binary'
                : visit_binary})
        assert str(b3) == 'table1.col1 = table1.col2'

        def visit_binary(b):
            b.left = bindparam('bar')
        
        b4 = visitors.cloned_traverse(b2, {}, {'binary':visit_binary})
        assert str(b4) == ":bar = table1.col2"

        b5 = visitors.cloned_traverse(b3, {}, {'binary':visit_binary})
        assert str(b5) == ":bar = table1.col2"
    

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
            eq_(str(sql_util._deep_annotate(expr, {},
                exclude=[table1.c.col1])), expected)
        
    def test_deannotate(self):
        table1 = table('table1', column("col1"), column("col2"))
        
        bin = table1.c.col1 == bindparam('foo', value=None)

        b2 = sql_util._deep_annotate(bin, {'_orm_adapt':True})
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
        assert b4.right is not bin.right is not b2.right is not b3.right
        
