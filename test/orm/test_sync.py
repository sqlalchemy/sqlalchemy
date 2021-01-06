from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sync
from sqlalchemy.orm import unitofwork
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class AssertsUOW(object):
    def _get_test_uow(self, session):
        uow = unitofwork.UOWTransaction(session)
        deleted = set(session._deleted)
        new = set(session._new)
        dirty = set(session._dirty_states).difference(deleted)
        for s in new.union(dirty):
            uow.register_object(s)
        for d in deleted:
            uow.register_object(d, isdelete=True)
        return uow


class SyncTest(
    fixtures.MappedTest, testing.AssertsExecutionResults, AssertsUOW
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
        )
        Table(
            "t2",
            metadata,
            Column("id", Integer, ForeignKey("t1.id"), primary_key=True),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.A, cls.tables.t1)
        mapper(cls.classes.B, cls.tables.t2)

    def _fixture(self):
        A, B = self.classes.A, self.classes.B
        session = fixture_session()
        uowcommit = self._get_test_uow(session)
        a_mapper = class_mapper(A)
        b_mapper = class_mapper(B)
        self.a1 = a1 = A()
        self.b1 = b1 = B()
        uowcommit = self._get_test_uow(session)
        return (
            uowcommit,
            attributes.instance_state(a1),
            attributes.instance_state(b1),
            a_mapper,
            b_mapper,
        )

    def test_populate(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        a1.obj().id = 7
        assert "id" not in b1.obj().__dict__
        sync.populate(a1, a_mapper, b1, b_mapper, pairs, uowcommit, False)
        eq_(b1.obj().id, 7)
        eq_(b1.obj().__dict__["id"], 7)
        assert ("pk_cascaded", b1, b_mapper.c.id) not in uowcommit.attributes

    def test_populate_flag_cascaded(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        a1.obj().id = 7
        assert "id" not in b1.obj().__dict__
        sync.populate(a1, a_mapper, b1, b_mapper, pairs, uowcommit, True)
        eq_(b1.obj().id, 7)
        eq_(b1.obj().__dict__["id"], 7)
        eq_(uowcommit.attributes[("pk_cascaded", b1, b_mapper.c.id)], True)

    def test_populate_unmapped_source(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(b_mapper.c.id, b_mapper.c.id)]
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            "Can't execute sync rule for source column 't2.id'; "
            r"mapper 'mapped class A->t1' does not map this column.",
            sync.populate,
            a1,
            a_mapper,
            b1,
            b_mapper,
            pairs,
            uowcommit,
            False,
        )

    def test_populate_unmapped_dest(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, a_mapper.c.id)]
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            r"Can't execute sync rule for destination "
            r"column 't1.id'; "
            r"mapper 'mapped class B->t2' does not map this column.",
            sync.populate,
            a1,
            a_mapper,
            b1,
            b_mapper,
            pairs,
            uowcommit,
            False,
        )

    def test_clear(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, b_mapper.c.t1id)]
        b1.obj().t1id = 8
        eq_(b1.obj().__dict__["t1id"], 8)
        sync.clear(b1, b_mapper, pairs)
        eq_(b1.obj().__dict__["t1id"], None)

    def test_clear_pk(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        b1.obj().id = 8
        eq_(b1.obj().__dict__["id"], 8)
        assert_raises_message(
            AssertionError,
            "Dependency rule tried to blank-out primary key "
            "column 't2.id' on instance '<B",
            sync.clear,
            b1,
            b_mapper,
            pairs,
        )

    def test_clear_unmapped(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(a_mapper.c.id, a_mapper.c.foo)]
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            "Can't execute sync rule for destination "
            r"column 't1.foo'; mapper 'mapped class B->t2' does not "
            "map this column.",
            sync.clear,
            b1,
            b_mapper,
            pairs,
        )

    def test_update(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().id = 10
        a1._commit_all(a1.dict)
        a1.obj().id = 12
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        dest = {}
        sync.update(a1, a_mapper, dest, "old_", pairs)
        eq_(dest, {"id": 12, "old_id": 10})

    def test_update_unmapped(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(b_mapper.c.id, b_mapper.c.id)]
        dest = {}
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            "Can't execute sync rule for source column 't2.id'; "
            r"mapper 'mapped class A->t1' does not map this column.",
            sync.update,
            a1,
            a_mapper,
            dest,
            "old_",
            pairs,
        )

    def test_populate_dict(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().id = 10
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        dest = {}
        sync.populate_dict(a1, a_mapper, dest, pairs)
        eq_(dest, {"id": 10})

    def test_populate_dict_unmapped(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().id = 10
        pairs = [(b_mapper.c.id, b_mapper.c.id)]
        dest = {}
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            "Can't execute sync rule for source column 't2.id'; "
            r"mapper 'mapped class A->t1' does not map this column.",
            sync.populate_dict,
            a1,
            a_mapper,
            dest,
            pairs,
        )

    def test_source_modified_unmodified(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().id = 10
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        eq_(sync.source_modified(uowcommit, a1, a_mapper, pairs), False)

    def test_source_modified_no_pairs(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        eq_(sync.source_modified(uowcommit, a1, a_mapper, []), False)

    def test_source_modified_modified(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().id = 10
        a1._commit_all(a1.dict)
        a1.obj().id = 12
        pairs = [(a_mapper.c.id, b_mapper.c.id)]
        eq_(sync.source_modified(uowcommit, a1, a_mapper, pairs), True)

    def test_source_modified_composite(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().foo = 10
        a1._commit_all(a1.dict)
        a1.obj().foo = 12
        pairs = [
            (a_mapper.c.id, b_mapper.c.id),
            (a_mapper.c.foo, b_mapper.c.id),
        ]
        eq_(sync.source_modified(uowcommit, a1, a_mapper, pairs), True)

    def test_source_modified_composite_unmodified(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        a1.obj().foo = 10
        a1._commit_all(a1.dict)
        pairs = [
            (a_mapper.c.id, b_mapper.c.id),
            (a_mapper.c.foo, b_mapper.c.id),
        ]
        eq_(sync.source_modified(uowcommit, a1, a_mapper, pairs), False)

    def test_source_modified_no_unmapped(self):
        uowcommit, a1, b1, a_mapper, b_mapper = self._fixture()
        pairs = [(b_mapper.c.id, b_mapper.c.id)]
        assert_raises_message(
            orm_exc.UnmappedColumnError,
            "Can't execute sync rule for source column 't2.id'; "
            r"mapper 'mapped class A->t1' does not map this column.",
            sync.source_modified,
            uowcommit,
            a1,
            a_mapper,
            pairs,
        )
