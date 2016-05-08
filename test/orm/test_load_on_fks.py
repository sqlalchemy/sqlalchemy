from sqlalchemy import *
from sqlalchemy.orm import *

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.testing import eq_, AssertsExecutionResults, assert_raises
from sqlalchemy import testing
from sqlalchemy.testing import fixtures
from sqlalchemy.orm.attributes import instance_state
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.testing.schema import Table, Column

engine = testing.db


class FlushOnPendingTest(AssertsExecutionResults, fixtures.TestBase):
    def setUp(self):
        global Parent, Child, Base
        Base= declarative_base()

        class Parent(Base):
            __tablename__ = 'parent'

            id= Column(Integer, primary_key=True, test_needs_autoincrement=True)
            name = Column(String(50), nullable=False)
            children = relationship("Child", load_on_pending=True)

        class Child(Base):
            __tablename__ = 'child'
            id= Column(Integer, primary_key=True, test_needs_autoincrement=True)
            parent_id = Column(Integer, ForeignKey('parent.id'))

        Base.metadata.create_all(engine)

    def tearDown(self):
        Base.metadata.drop_all(engine)

    def test_annoying_autoflush_one(self):
        sess = Session(engine)

        p1 = Parent()
        sess.add(p1)
        p1.children = []

    def test_annoying_autoflush_two(self):
        sess = Session(engine)

        p1 = Parent()
        sess.add(p1)
        assert p1.children == []

    def test_dont_load_if_no_keys(self):
        sess = Session(engine)

        p1 = Parent()
        sess.add(p1)

        def go():
            assert p1.children == []
        self.assert_sql_count(testing.db, go, 0)

class LoadOnFKsTest(AssertsExecutionResults, fixtures.TestBase):

    def setUp(self):
        global Parent, Child, Base
        Base= declarative_base()

        class Parent(Base):
            __tablename__ = 'parent'
            __table_args__ = {'mysql_engine':'InnoDB'}

            id= Column(Integer, primary_key=True, test_needs_autoincrement=True)

        class Child(Base):
            __tablename__ = 'child'
            __table_args__ = {'mysql_engine':'InnoDB'}

            id= Column(Integer, primary_key=True, test_needs_autoincrement=True)
            parent_id = Column(Integer, ForeignKey('parent.id'))

            parent = relationship(Parent, backref=backref("children"))

        Base.metadata.create_all(engine)

        global sess, p1, p2, c1, c2
        sess = Session(bind=engine)

        p1 = Parent()
        p2 = Parent()
        c1, c2 = Child(), Child()
        c1.parent = p1
        sess.add_all([p1, p2])
        assert c1 in sess

        sess.commit()

    def tearDown(self):
        sess.rollback()
        Base.metadata.drop_all(engine)

    def test_load_on_pending_allows_backref_event(self):
        Child.parent.property.load_on_pending = True
        sess.autoflush = False
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None.
        # change as of [ticket:3708]
        assert c3 in p1.children

    def test_enable_rel_loading_allows_backref_event(self):
        sess.autoflush = False
        c3 = Child()
        sess.enable_relationship_loading(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None
        # change as of [ticket:3708]
        assert c3 in p1.children

    def test_m2o_history_on_persistent_allows_backref_event(self):
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        assert c3 in p1.children

    def test_load_on_persistent_allows_backref_event(self):
        Child.parent.property.load_on_pending = True
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        assert c3 in p1.children

    def test_enable_rel_loading_on_persistent_allows_backref_event(self):
        c3 = Child()
        sess.enable_relationship_loading(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None
        # change as of [ticket:3708]
        assert c3 in p1.children

    def test_no_load_on_pending_allows_backref_event(self):
        # users who stick with the program and don't use
        # 'load_on_pending' get expected behavior

        sess.autoflush = False
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        c3.parent = p1

        assert c3 in p1.children

    def test_autoflush_on_pending(self):
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        # pendings don't autoflush
        assert c3.parent is None

    def test_autoflush_on_pending(self):
        Child.parent.property.load_on_pending = True
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        # ...unless the flag is on
        assert c3.parent is p1

    def test_collection_load_from_pending_populated(self):
        Parent.children.property.load_on_pending = True
        p2 = Parent(id=p1.id)
        sess.add(p2)
        # load should emit since PK is populated
        def go():
            assert p2.children
        self.assert_sql_count(testing.db, go, 1)

    def test_collection_load_from_pending_no_sql(self):
        Parent.children.property.load_on_pending = True
        p2 = Parent(id=None)
        sess.add(p2)
        # load should not emit since "None" is the bound
        # param list
        def go():
            assert not p2.children
        self.assert_sql_count(testing.db, go, 0)

    def test_load_on_pending_with_set(self):
        Child.parent.property.load_on_pending = True

        p1.children

        c3 = Child()
        sess.add(c3)

        c3.parent_id = p1.id

        def go():
            c3.parent = p1
        self.assert_sql_count(testing.db, go, 0)

    def test_backref_doesnt_double(self):
        Child.parent.property.load_on_pending = True
        sess.autoflush = False
        p1.children
        c3 = Child()
        sess.add(c3)
        c3.parent = p1
        c3.parent = p1
        c3.parent = p1
        c3.parent = p1
        assert len(p1.children)== 2

    def test_m2o_lazy_loader_on_persistent(self):
        """Compare the behaviors from the lazyloader using
        the "committed" state in all cases, vs. the lazyloader
        using the "current" state in all cases except during flush.

        """

        for loadfk in (True, False):
            for loadrel in (True, False):
                for autoflush in (True, False):
                    for manualflush in (True, False):
                        for fake_autoexpire in (True, False):
                            sess.autoflush = autoflush

                            if loadfk:
                                c1.parent_id
                            if loadrel:
                                c1.parent

                            c1.parent_id = p2.id

                            if manualflush:
                                sess.flush()

                            # fake_autoexpire refers to the eventual
                            # auto-expire of 'parent' when c1.parent_id
                            # is altered.
                            if fake_autoexpire:
                                sess.expire(c1, ['parent'])

                            # old 0.6 behavior
                            #if manualflush and (not loadrel or fake_autoexpire):
                            #    # a flush occurs, we get p2
                            #    assert c1.parent is p2
                            #elif not loadrel and not loadfk:
                            #    # problematically - we get None since committed state
                            #    # is empty when c1.parent_id was mutated, since we want
                            #    # to save on selects.  this is
                            #    # why the patch goes in in 0.6 - this is mostly a bug.
                            #    assert c1.parent is None
                            #else:
                            #    # if things were loaded, autoflush doesn't even
                            #    # happen.
                            #    assert c1.parent is p1

                            # new behavior
                            if loadrel and not fake_autoexpire:
                                assert c1.parent is p1
                            else:
                                assert c1.parent is p2

                            sess.rollback()

    def test_m2o_lazy_loader_on_pending(self):
        for loadonpending in (False, True):
            for autoflush in (False, True):
                for manualflush in (False, True):
                    Child.parent.property.load_on_pending = loadonpending
                    sess.autoflush = autoflush
                    c2 = Child()
                    sess.add(c2)
                    c2.parent_id = p2.id

                    if manualflush:
                       sess.flush()

                    if loadonpending or manualflush:
                        assert c2.parent is p2
                    else:
                        assert c2.parent is None

                    sess.rollback()

    def test_m2o_lazy_loader_on_transient(self):
        for loadonpending in (False, True):
            for attach in (False, True):
                for autoflush in (False, True):
                    for manualflush in (False, True):
                        for enable_relationship_rel in (False, True):
                            Child.parent.property.load_on_pending = loadonpending
                            sess.autoflush = autoflush
                            c2 = Child()

                            if attach:
                                state = instance_state(c2)
                                state.session_id = sess.hash_key

                            if enable_relationship_rel:
                                sess.enable_relationship_loading(c2)

                            c2.parent_id = p2.id

                            if manualflush:
                               sess.flush()

                            if (loadonpending and attach) or enable_relationship_rel:
                                assert c2.parent is p2
                            else:
                                assert c2.parent is None

                            sess.rollback()
