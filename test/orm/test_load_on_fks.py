from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import backref
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.attributes import instance_state
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column


class FlushOnPendingTest(AssertsExecutionResults, fixtures.TestBase):
    def setup_test(self):
        global Parent, Child, Base
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50), nullable=False)
            children = relationship("Child", load_on_pending=True)

        class Child(Base):
            __tablename__ = "child"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            parent_id = Column(Integer, ForeignKey("parent.id"))

        Base.metadata.create_all(testing.db)

    def teardown_test(self):
        Base.metadata.drop_all(testing.db)

    def test_annoying_autoflush_one(self):
        sess = fixture_session()

        p1 = Parent()
        sess.add(p1)
        p1.children = []

    def test_annoying_autoflush_two(self):
        sess = fixture_session()

        p1 = Parent()
        sess.add(p1)
        assert p1.children == []

    def test_dont_load_if_no_keys(self):
        sess = fixture_session()

        p1 = Parent()
        sess.add(p1)

        def go():
            assert p1.children == []

        self.assert_sql_count(testing.db, go, 0)


class LoadOnFKsTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base):
            __tablename__ = "parent"
            __table_args__ = {"mysql_engine": "InnoDB"}

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class Child(Base):
            __tablename__ = "child"
            __table_args__ = {"mysql_engine": "InnoDB"}

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            parent_id = Column(Integer, ForeignKey("parent.id"))

            parent = relationship(Parent, backref=backref("children"))

    @testing.fixture
    def parent_fixture(self, connection):
        Parent, Child = self.classes("Parent", "Child")

        sess = fixture_session(bind=connection)
        p1 = Parent()
        p2 = Parent()
        c1, c2 = Child(), Child()
        c1.parent = p1
        sess.add_all([p1, p2])
        assert c1 in sess

        sess.flush()

        Child.parent.property.load_on_pending = False

        sess.expire_all()

        yield sess, p1, p2, c1, c2

        sess.close()

    def test_m2o_history_on_persistent_allows_backref_event(
        self, parent_fixture
    ):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        assert c3 in p1.children

    def test_load_on_persistent_allows_backref_event(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Child.parent.property.load_on_pending = True
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id
        c3.parent = p1

        assert c3 in p1.children

    def test_load_on_pending_allows_backref_event(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        sess.autoflush = False

        Child.parent.property.load_on_pending = True
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None.
        # change as of [ticket:3708]
        assert c3 in p1.children

    def test_no_load_on_pending_allows_backref_event(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        # users who stick with the program and don't use
        # 'load_on_pending' get expected behavior

        sess.autoflush = False
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        c3.parent = p1

        assert c3 in p1.children

    def test_autoflush_on_pending(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        # ensure p1.id is not expired
        p1.id

        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        # pendings don't autoflush
        assert c3.parent is None

    def test_autoflush_load_on_pending_on_pending(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        # ensure p1.id is not expired
        p1.id

        Child.parent.property.load_on_pending = True
        c3 = Child()
        sess.add(c3)
        c3.parent_id = p1.id

        # ...unless the flag is on
        assert c3.parent is p1

    def test_collection_load_from_pending_populated(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Parent.children.property.load_on_pending = True
        p2 = Parent(id=p1.id)
        sess.add(p2)
        # load should emit since PK is populated

        def go():
            assert p2.children

        self.assert_sql_count(testing.db, go, 1)

    def test_collection_load_from_pending_no_sql(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Parent.children.property.load_on_pending = True
        p2 = Parent(id=None)
        sess.add(p2)
        # load should not emit since "None" is the bound
        # param list

        def go():
            assert not p2.children

        self.assert_sql_count(testing.db, go, 0)

    def test_load_on_pending_with_set(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Child.parent.property.load_on_pending = True

        p1.children

        c3 = Child()
        sess.add(c3)

        c3.parent_id = p1.id

        def go():
            c3.parent = p1

        self.assert_sql_count(testing.db, go, 0)

    def test_backref_doesnt_double(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Child.parent.property.load_on_pending = True
        sess.autoflush = False
        p1.children
        c3 = Child()
        sess.add(c3)
        c3.parent = p1
        c3.parent = p1
        c3.parent = p1
        c3.parent = p1
        assert len(p1.children) == 2

    @testing.combinations(True, False, argnames="loadfk")
    @testing.combinations(True, False, argnames="loadrel")
    @testing.combinations(True, False, argnames="autoflush")
    @testing.combinations(True, False, argnames="manualflush")
    @testing.combinations(True, False, argnames="fake_autoexpire")
    def test_m2o_lazy_loader_on_persistent(
        self,
        parent_fixture,
        loadfk,
        loadrel,
        autoflush,
        manualflush,
        fake_autoexpire,
    ):
        """Compare the behaviors from the lazyloader using
        the "committed" state in all cases, vs. the lazyloader
        using the "current" state in all cases except during flush.

        """

        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

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
            sess.expire(c1, ["parent"])

        # old 0.6 behavior
        # if manualflush and (not loadrel or
        #                     fake_autoexpire):
        #    # a flush occurs, we get p2
        #    assert c1.parent is p2
        # elif not loadrel and not loadfk:
        #    # problematically - we get None since
        #    # committed state
        #    # is empty when c1.parent_id was mutated,
        #    # since we want
        #    # to save on selects.  this is
        #    # why the patch goes in in 0.6 - this is
        #    # mostly a bug.
        #    assert c1.parent is None
        # else:
        #    # if things were loaded, autoflush doesn't
        #    # even happen.
        #    assert c1.parent is p1

        # new behavior
        if loadrel and not fake_autoexpire:
            assert c1.parent is p1
        else:
            assert c1.parent is p2

    @testing.combinations(True, False, argnames="loadonpending")
    @testing.combinations(True, False, argnames="autoflush")
    @testing.combinations(True, False, argnames="manualflush")
    def test_m2o_lazy_loader_on_pending(
        self, parent_fixture, loadonpending, autoflush, manualflush
    ):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        Child.parent.property.load_on_pending = loadonpending
        sess.autoflush = autoflush

        # ensure p2.id not expired
        p2.id

        c2 = Child()
        sess.add(c2)
        c2.parent_id = p2.id

        if manualflush:
            sess.flush()

        if loadonpending or manualflush:
            assert c2.parent is p2
        else:
            assert c2.parent is None

    @testing.combinations(True, False, argnames="loadonpending")
    @testing.combinations(True, False, argnames="attach")
    @testing.combinations(True, False, argnames="autoflush")
    @testing.combinations(True, False, argnames="manualflush")
    @testing.combinations(True, False, argnames="enable_relationship_rel")
    def test_m2o_lazy_loader_on_transient(
        self,
        parent_fixture,
        loadonpending,
        attach,
        autoflush,
        manualflush,
        enable_relationship_rel,
    ):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

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
