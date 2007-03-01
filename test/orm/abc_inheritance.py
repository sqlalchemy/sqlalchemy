from sqlalchemy import *
from sqlalchemy.orm.sync import ONETOMANY, MANYTOONE
import testbase

def produce_test(parent, child, direction, fkeyinline):
    class ABCTest(testbase.ORMTest):
        def define_tables(self, meta):
            global ta, tb, tc
            ta = ["a", meta]
            ta.append(Column('id', Integer, primary_key=True)), 
            ta.append(Column('data', String(30)))
            if "a"== parent and direction == MANYTOONE:
                ta.append(Column('child_id', Integer, ForeignKey("%s.id" % child, use_alter=True, name="foo")))
            elif "a" == child and direction == ONETOMANY:
                ta.append(Column('parent_id', Integer, ForeignKey("%s.id" % parent, use_alter=True, name="foo")))
            ta = Table(*ta)
    
            tb = ["b", meta]
            if fkeyinline:
                tb.append(Column('id', Integer, ForeignKey("a.id"), primary_key=True, ))
            else:
                tb.append(Column('id', Integer, primary_key=True))
                tb.append(Column('a_id', Integer, ForeignKey("a.id"), primary_key=True))
        
            tb.append(Column('data', String(30)))
    
            if "b"== parent and direction == MANYTOONE:
                tb.append(Column('child_id', Integer, ForeignKey("%s.id" % child, use_alter=True, name="foo")))
            elif "b" == child and direction == ONETOMANY:
                tb.append(Column('parent_id', Integer, ForeignKey("%s.id" % parent, use_alter=True, name="foo")))
            tb = Table(*tb)
    
            tc = ["c", meta]
            if fkeyinline:
                tc.append(Column('id', Integer, ForeignKey("b.id"), primary_key=True, ))
            else:
                tc.append(Column('id', Integer, primary_key=True))
                tc.append(Column('b_id', Integer, ForeignKey("b.id"), primary_key=True))
        
            tc.append(Column('data', String(30)))
    
            if "c"== parent and direction == MANYTOONE:
                tc.append(Column('child_id', Integer, ForeignKey("%s.id" % child, use_alter=True, name="foo")))
            elif "c" == child and direction == ONETOMANY:
                tc.append(Column('parent_id', Integer, ForeignKey("%s.id" % parent, use_alter=True, name="foo")))
            tc = Table(*tc)
        
        # TODO: get finicky postgres to work
        @testbase.supported('sqlite')
        def test_basic(self):
            parent_table = {"a":ta, "b":tb, "c": tc}[parent]
            child_table = {"a":ta, "b":tb, "c": tc}[child]

            if direction == MANYTOONE:
                foreign_keys = [parent_table.c.child_id]
            elif direction == ONETOMANY:
                foreign_keys = [child_table.c.parent_id]

            if fkeyinline:
                atob = ta.c.id==tb.c.id
                btoc = tc.c.id==tb.c.id
            else:
                atob = ta.c.id==tb.c.a_id
                btoc = tc.c.b_id==tb.c.id

            if direction == ONETOMANY:
                relationjoin = parent_table.c.id==child_table.c.parent_id
            elif direction == MANYTOONE:
                relationjoin = parent_table.c.child_id==child_table.c.id

            abcjoin = polymorphic_union(
                {"a":ta.select(tb.c.id==None, from_obj=[ta.outerjoin(tb, onclause=atob)]),
                "b":ta.join(tb, onclause=atob).outerjoin(tc, onclause=btoc).select(tc.c.id==None),
                "c":tc.join(tb, onclause=btoc).join(ta, onclause=atob)
                },"type", "abcjoin"
            )

            bcjoin = polymorphic_union(
            {
            "b":ta.join(tb, onclause=atob).outerjoin(tc, onclause=btoc).select(tc.c.id==None),
            "c":tc.join(tb, onclause=btoc).join(ta, onclause=atob)
            },"type", "bcjoin"
            )

            class A(object):pass
            class B(A):pass
            class C(B):pass

            mapper(A, ta, polymorphic_on=abcjoin.c.type, select_table=abcjoin, polymorphic_identity="a")
            mapper(B, tb, polymorphic_on=bcjoin.c.type, select_table=bcjoin, polymorphic_identity="b", inherits=A, inherit_condition=atob)
            mapper(C, tc, polymorphic_identity="c", inherits=B, inherit_condition=btoc)

            parent_mapper = class_mapper({ta:A, tb:B, tc:C}[parent_table])
            child_mapper = class_mapper({ta:A, tb:B, tc:C}[child_table])

            parent_class = parent_mapper.class_
            child_class = child_mapper.class_

            parent_mapper.add_property("collection", relation(child_mapper, primaryjoin=relationjoin, foreign_keys=foreign_keys, uselist=True))

            sess = create_session()

            parent_obj = parent_class()
            child_obj = child_class()
            somea = A()
            someb = B()
            somec = C()
            print "APPENDING", parent.__class__.__name__ , "TO", child.__class__.__name__
            sess.save(parent_obj)
            parent_obj.collection.append(child_obj)
            if direction == ONETOMANY:
                child2 = child_class()
                parent_obj.collection.append(child2)
                sess.save(child2)
            elif direction == MANYTOONE:
                parent2 = parent_class()
                parent2.collection.append(child_obj)
                sess.save(parent2)
            sess.save(somea)
            sess.save(someb)
            sess.save(somec)
            sess.flush()
            sess.clear()

            result = sess.query(parent_class).get(parent_obj.id)
            assert result.id == parent_obj.id
            assert result.collection[0].id == child_obj.id
            if direction == ONETOMANY:
                assert result.collection[1].id == child2.id
            elif direction == MANYTOONE:
                result2 = sess.query(parent_class).get(parent2.id)
                assert result2.id == parent2.id
                assert result2.collection[0].id == child_obj.id
    ABCTest.__name__ = "Test%s%s%d%sTest" % (parent, child, direction, fkeyinline)
    return ABCTest

for parent in ["a", "b", "c"]:
    for child in ["a", "b", "c"]:
        if parent == child:
            continue
        for fkeyinline in [True]: #, False]:
            for direction in [ONETOMANY, MANYTOONE]:
                testclass = produce_test(parent, child, direction, fkeyinline)
                exec("%s = testclass" % testclass.__name__)


if __name__ == "__main__":
    testbase.main()
