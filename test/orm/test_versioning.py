import datetime
import sqlalchemy as sa
from sqlalchemy.testing import engines, config
from sqlalchemy import testing
from sqlalchemy.testing.mock import patch
from sqlalchemy import (
    Integer, String, Date, ForeignKey, orm, exc, select, TypeDecorator)
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import (
    mapper, relationship, Session, create_session, sessionmaker,
    exc as orm_exc)
from sqlalchemy.testing import (
    eq_, assert_raises, assert_raises_message, fixtures)
from sqlalchemy.testing.assertsql import CompiledSQL
import uuid
from sqlalchemy import util


def make_uuid():
    return uuid.uuid4().hex


class VersioningTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('version_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('version_id', Integer, nullable=False),
              Column('value', String(40), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        s1 = Session()
        return s1

    @engines.close_open_connections
    def test_notsane_warning(self):
        Foo = self.classes.Foo

        save = testing.db.dialect.supports_sane_rowcount
        testing.db.dialect.supports_sane_rowcount = False
        try:
            s1 = self._fixture()
            f1 = Foo(value='f1')
            f2 = Foo(value='f2')
            s1.add_all((f1, f2))
            s1.commit()

            f1.value = 'f1rev2'
            assert_raises(sa.exc.SAWarning, s1.commit)
        finally:
            testing.db.dialect.supports_sane_rowcount = save

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support (update|delete)d rowcount')
    def test_basic(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        f2 = Foo(value='f2')
        s1.add_all((f1, f2))
        s1.commit()

        f1.value = 'f1rev2'
        s1.commit()

        s2 = create_session(autocommit=False)
        f1_s = s2.query(Foo).get(f1.id)
        f1_s.value = 'f1rev3'
        s2.commit()

        f1.value = 'f1rev3mine'

        # Only dialects with a sane rowcount can detect the
        # StaleDataError
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(
                sa.orm.exc.StaleDataError,
                r"UPDATE statement on table 'version_table' expected "
                r"to update 1 row\(s\); 0 were matched.", s1.commit),
            s1.rollback()
        else:
            s1.commit()

        # new in 0.5 !  don't need to close the session
        f1 = s1.query(Foo).get(f1.id)
        f2 = s1.query(Foo).get(f2.id)

        f1_s.value = 'f1rev4'
        s2.commit()

        s1.delete(f1)
        s1.delete(f2)

        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(
                sa.orm.exc.StaleDataError,
                r"DELETE statement on table 'version_table' expected "
                r"to delete 2 row\(s\); 1 were matched.",
                s1.commit)
        else:
            s1.commit()

    def test_multiple_updates(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        f2 = Foo(value='f2')
        s1.add_all((f1, f2))
        s1.commit()

        f1.value = 'f1rev2'
        f2.value = 'f2rev2'
        s1.commit()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, 'f1rev2', 2), (f2.id, 'f2rev2', 2)]
        )

    def test_bulk_insert(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        s1.bulk_insert_mappings(
            Foo,
            [{"id": 1, "value": "f1"}, {"id": 2, "value": "f2"}]
        )
        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(1, 'f1', 1), (2, 'f2', 1)]
        )

    def test_bulk_update(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        f2 = Foo(value='f2')
        s1.add_all((f1, f2))
        s1.commit()

        s1.bulk_update_mappings(
            Foo,
            [
                {"id": f1.id, "value": "f1rev2", "version_id": 1},
                {"id": f2.id, "value": "f2rev2", "version_id": 1},

            ]
        )
        s1.commit()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, 'f1rev2', 2), (f2.id, 'f2rev2', 2)]
        )

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support (update|delete)d rowcount')
    def test_bump_version(self):
        """test that version number can be bumped.

        Ensures that the UPDATE or DELETE is against the
        last committed version of version_id_col, not the modified
        state.

        """

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        s1.add(f1)
        s1.commit()
        eq_(f1.version_id, 1)
        f1.version_id = 2
        s1.commit()
        eq_(f1.version_id, 2)

        # skip an id, test that history
        # is honored
        f1.version_id = 4
        f1.value = "something new"
        s1.commit()
        eq_(f1.version_id, 4)

        f1.version_id = 5
        s1.delete(f1)
        s1.commit()
        eq_(s1.query(Foo).count(), 0)

    @testing.emits_warning(r'.*does not support updated rowcount')
    @engines.close_open_connections
    def test_versioncheck(self):
        """query.with_lockmode performs a 'version check' on an already loaded
        instance"""

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1s1 = Foo(value='f1 value')
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).get(f1s1.id)
        f1s2.value = 'f1 new value'
        s2.commit()

        # load, version is wrong
        assert_raises_message(
            sa.orm.exc.StaleDataError,
            r"Instance .* has version id '\d+' which does not "
            r"match database-loaded version id '\d+'",
            s1.query(Foo).with_lockmode('read').get, f1s1.id
        )

        # reload it - this expires the old version first
        s1.refresh(f1s1, lockmode='read')

        # now assert version OK
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

        # assert brand new load is OK too
        s1.close()
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

    def test_versioncheck_not_versioned(self):
        """ensure the versioncheck logic skips if there isn't a
        version_id_col actually configured"""

        Foo = self.classes.Foo
        version_table = self.tables.version_table

        mapper(Foo, version_table)
        s1 = Session()
        f1s1 = Foo(value='f1 value', version_id=1)
        s1.add(f1s1)
        s1.commit()
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

    @testing.emits_warning(r'.*does not support updated rowcount')
    @engines.close_open_connections
    @testing.requires.update_nowait
    def test_versioncheck_for_update(self):
        """query.with_lockmode performs a 'version check' on an already loaded
        instance"""

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1s1 = Foo(value='f1 value')
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).get(f1s1.id)
        s2.refresh(f1s2, lockmode='update')
        f1s2.value = 'f1 new value'

        assert_raises(
            exc.DBAPIError,
            s1.refresh, f1s1, lockmode='update_nowait'
        )
        s1.rollback()

        s2.commit()
        s1.refresh(f1s1, lockmode='update_nowait')
        assert f1s1.version_id == f1s2.version_id

    def test_update_multi_missing_broken_multi_rowcount(self):
        @util.memoized_property
        def rowcount(self):
            if len(self.context.compiled_parameters) > 1:
                return -1
            else:
                return self.context.rowcount

        with patch.object(
                config.db.dialect, "supports_sane_multi_rowcount", False):
            with patch(
                    "sqlalchemy.engine.result.ResultProxy.rowcount",
                    rowcount):

                Foo = self.classes.Foo
                s1 = self._fixture()
                f1s1 = Foo(value='f1 value')
                s1.add(f1s1)
                s1.commit()

                f1s1.value = 'f2 value'
                s1.flush()
                eq_(f1s1.version_id, 2)

    @testing.emits_warning(r'.*does not support updated rowcount')
    @engines.close_open_connections
    def test_noversioncheck(self):
        """test query.with_lockmode works when the mapper has no version id
        col"""

        Foo, version_table = self.classes.Foo, self.tables.version_table

        s1 = create_session(autocommit=False)
        mapper(Foo, version_table)
        f1s1 = Foo(value="foo", version_id=0)
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).with_lockmode('read').get(f1s1.id)
        assert f1s2.id == f1s1.id
        assert f1s2.value == f1s1.value

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_merge_no_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        s1.add(f1)
        s1.commit()

        f1.value = 'f2'
        s1.commit()

        f2 = Foo(id=f1.id, value='f3')
        f3 = s1.merge(f2)
        assert f3 is f1
        s1.commit()
        eq_(f3.version_id, 3)

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_merge_correct_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        s1.add(f1)
        s1.commit()

        f1.value = 'f2'
        s1.commit()

        f2 = Foo(id=f1.id, value='f3', version_id=2)
        f3 = s1.merge(f2)
        assert f3 is f1
        s1.commit()
        eq_(f3.version_id, 3)

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_merge_incorrect_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        s1.add(f1)
        s1.commit()

        f1.value = 'f2'
        s1.commit()

        f2 = Foo(id=f1.id, value='f3', version_id=1)
        assert_raises_message(
            orm_exc.StaleDataError,
            "Version id '1' on merged state "
            "<Foo at .*?> does not match existing version '2'. "
            "Leave the version attribute unset when "
            "merging to update the most recent version.",
            s1.merge, f2
        )

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_merge_incorrect_version_not_in_session(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        s1.add(f1)
        s1.commit()

        f1.value = 'f2'
        s1.commit()

        f2 = Foo(id=f1.id, value='f3', version_id=1)
        s1.close()

        assert_raises_message(
            orm_exc.StaleDataError,
            "Version id '1' on merged state "
            "<Foo at .*?> does not match existing version '2'. "
            "Leave the version attribute unset when "
            "merging to update the most recent version.",
            s1.merge, f2
        )


class NoBumpOnRelationshipTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('version_id', Integer)
        )
        Table(
            'b', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('a_id', ForeignKey('a.id'))
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def _run_test(self, auto_version_counter=True):
        A, B = self.classes('A', 'B')
        s = Session()
        if auto_version_counter:
            a1 = A()
        else:
            a1 = A(version_id=1)
        s.add(a1)
        s.commit()
        eq_(a1.version_id, 1)

        b1 = B()
        b1.a = a1
        s.add(b1)
        s.commit()

        eq_(a1.version_id, 1)

    def test_plain_counter(self):
        A, B = self.classes('A', 'B')
        a, b = self.tables('a', 'b')

        mapper(
            A, a, properties={
                'bs': relationship(B, backref='a')
            },
            version_id_col=a.c.version_id,
        )
        mapper(B, b)

        self._run_test()

    def test_functional_counter(self):
        A, B = self.classes('A', 'B')
        a, b = self.tables('a', 'b')

        mapper(
            A, a, properties={
                'bs': relationship(B, backref='a')
            },
            version_id_col=a.c.version_id,
            version_id_generator=lambda num: (num or 0) + 1
        )
        mapper(B, b)

        self._run_test()

    def test_no_counter(self):
        A, B = self.classes('A', 'B')
        a, b = self.tables('a', 'b')

        mapper(
            A, a, properties={
                'bs': relationship(B, backref='a')
            },
            version_id_col=a.c.version_id,
            version_id_generator=False
        )
        mapper(B, b)

        self._run_test(False)


class ColumnTypeTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        class SpecialType(TypeDecorator):
            impl = Date

            def process_bind_param(self, value, dialect):
                assert isinstance(value, datetime.date)
                return value

        Table('version_table', metadata,
              Column('id', SpecialType, primary_key=True),
              Column('version_id', Integer, nullable=False),
              Column('value', String(40), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        s1 = Session()
        return s1

    @engines.close_open_connections
    def test_update(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(id=datetime.date.today(), value='f1')
        s1.add(f1)
        s1.commit()

        f1.value = 'f1rev2'
        s1.commit()


class RowSwitchTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'p', metadata,
            Column('id', String(10), primary_key=True),
            Column('version_id', Integer, default=1, nullable=False),
            Column('data', String(50))
        )
        Table(
            'c', metadata,
            Column('id', String(10), ForeignKey('p.id'), primary_key=True),
            Column('version_id', Integer, default=1, nullable=False),
            Column('data', String(50))
        )

    @classmethod
    def setup_classes(cls):

        class P(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        p, c, C, P = cls.tables.p, cls.tables.c, cls.classes.C, cls.classes.P

        mapper(
            P, p, version_id_col=p.c.version_id, properties={
                'c': relationship(
                    C, uselist=False, cascade='all, delete-orphan')})
        mapper(C, c, version_id_col=c.c.version_id)

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_row_switch(self):
        P = self.classes.P

        session = sessionmaker()()
        session.add(P(id='P1', data='P version 1'))
        session.commit()
        session.close()

        p = session.query(P).first()
        session.delete(p)
        session.add(P(id='P1', data="really a row-switch"))
        session.commit()

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_child_row_switch(self):
        P, C = self.classes.P, self.classes.C

        assert P.c.property.strategy.use_get

        session = sessionmaker()()
        session.add(P(id='P1', data='P version 1'))
        session.commit()
        session.close()

        p = session.query(P).first()
        p.c = C(data='child version 1')
        session.commit()

        p = session.query(P).first()
        p.c = C(data='child row-switch')
        session.commit()


class AlternateGeneratorTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'p', metadata,
            Column('id', String(10), primary_key=True),
            Column('version_id', String(32), nullable=False),
            Column('data', String(50))
        )
        Table(
            'c', metadata,
            Column('id', String(10), ForeignKey('p.id'), primary_key=True),
            Column('version_id', String(32), nullable=False),
            Column('data', String(50))
        )

    @classmethod
    def setup_classes(cls):

        class P(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        p, c, C, P = cls.tables.p, cls.tables.c, cls.classes.C, cls.classes.P

        mapper(
            P, p, version_id_col=p.c.version_id,
            version_id_generator=lambda x: make_uuid(),
            properties={
                'c': relationship(
                    C, uselist=False, cascade='all, delete-orphan')
            })
        mapper(
            C, c, version_id_col=c.c.version_id,
            version_id_generator=lambda x: make_uuid(),
        )

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support updated rowcount')
    def test_row_switch(self):
        P = self.classes.P

        session = sessionmaker()()
        session.add(P(id='P1', data='P version 1'))
        session.commit()
        session.close()

        p = session.query(P).first()
        session.delete(p)
        session.add(P(id='P1', data="really a row-switch"))
        session.commit()

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support (update|delete)d rowcount')
    def test_child_row_switch_one(self):
        P, C = self.classes.P, self.classes.C

        assert P.c.property.strategy.use_get

        session = sessionmaker()()
        session.add(P(id='P1', data='P version 1'))
        session.commit()
        session.close()

        p = session.query(P).first()
        p.c = C(data='child version 1')
        session.commit()

        p = session.query(P).first()
        p.c = C(data='child row-switch')
        session.commit()

    @testing.emits_warning_on(
        '+zxjdbc', r'.*does not support (update|delete)d rowcount')
    def test_child_row_switch_two(self):
        P = self.classes.P

        Session = sessionmaker()

        # TODO: not sure this test is
        # testing exactly what its looking for

        sess1 = Session()
        sess1.add(P(id='P1', data='P version 1'))
        sess1.commit()
        sess1.close()

        p1 = sess1.query(P).first()

        sess2 = Session()
        p2 = sess2.query(P).first()

        sess1.delete(p1)
        sess1.commit()

        # this can be removed and it still passes
        sess1.add(P(id='P1', data='P version 2'))
        sess1.commit()

        p2.data = 'P overwritten by concurrent tx'
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(
                orm.exc.StaleDataError,
                r"UPDATE statement on table 'p' expected to update "
                r"1 row\(s\); 0 were matched.",
                sess2.commit
            )
        else:
            sess2.commit


class PlainInheritanceTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'base', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('version_id', Integer, nullable=True),
            Column('data', String(50))
        )
        Table(
            'sub', metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
            Column('sub_data', String(50))
        )

    @classmethod
    def setup_classes(cls):

        class Base(cls.Basic):
            pass

        class Sub(Base):
            pass

    def test_update_child_table_only(self):
        Base, sub, base, Sub = (
            self.classes.Base, self.tables.sub, self.tables.base,
            self.classes.Sub)

        mapper(Base, base, version_id_col=base.c.version_id)
        mapper(Sub, sub, inherits=Base)

        s = Session()
        s1 = Sub(data='b', sub_data='s')
        s.add(s1)
        s.commit()

        s1.sub_data = 's2'
        s.commit()

        eq_(s1.version_id, 2)


class InheritanceTwoVersionIdsTest(fixtures.MappedTest):
    """Test versioning where both parent/child table have a
    versioning column.

    """
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'base', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('version_id', Integer, nullable=True),
            Column('data', String(50))
        )
        Table(
            'sub', metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
            Column('version_id', Integer, nullable=False),
            Column('sub_data', String(50))
        )

    @classmethod
    def setup_classes(cls):

        class Base(cls.Basic):
            pass

        class Sub(Base):
            pass

    def test_base_both(self):
        Base, sub, base, Sub = (
            self.classes.Base, self.tables.sub, self.tables.base,
            self.classes.Sub)

        mapper(Base, base, version_id_col=base.c.version_id)
        mapper(Sub, sub, inherits=Base)

        session = Session()
        b1 = Base(data='b1')
        session.add(b1)
        session.commit()
        eq_(b1.version_id, 1)
        # base is populated
        eq_(select([base.c.version_id]).scalar(), 1)

    def test_sub_both(self):
        Base, sub, base, Sub = (
            self.classes.Base, self.tables.sub, self.tables.base,
            self.classes.Sub)

        mapper(Base, base, version_id_col=base.c.version_id)
        mapper(Sub, sub, inherits=Base)

        session = Session()
        s1 = Sub(data='s1', sub_data='s1')
        session.add(s1)
        session.commit()

        # table is populated
        eq_(select([sub.c.version_id]).scalar(), 1)

        # base is populated
        eq_(select([base.c.version_id]).scalar(), 1)

    def test_sub_only(self):
        Base, sub, base, Sub = (
            self.classes.Base, self.tables.sub, self.tables.base,
            self.classes.Sub)

        mapper(Base, base)
        mapper(Sub, sub, inherits=Base, version_id_col=sub.c.version_id)

        session = Session()
        s1 = Sub(data='s1', sub_data='s1')
        session.add(s1)
        session.commit()

        # table is populated
        eq_(select([sub.c.version_id]).scalar(), 1)

        # base is not
        eq_(select([base.c.version_id]).scalar(), None)

    def test_mismatch_version_col_warning(self):
        Base, sub, base, Sub = (
            self.classes.Base, self.tables.sub, self.tables.base,
            self.classes.Sub)

        mapper(Base, base, version_id_col=base.c.version_id)

        assert_raises_message(
            exc.SAWarning,
            "Inheriting version_id_col 'version_id' does not "
            "match inherited version_id_col 'version_id' and will not "
            "automatically populate the inherited versioning column. "
            "version_id_col should only be specified on "
            "the base-most mapper that includes versioning.",
            mapper, Sub, sub, inherits=Base,
            version_id_col=sub.c.version_id)


class ServerVersioningTest(fixtures.MappedTest):
    run_define_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.sql import ColumnElement
        from sqlalchemy.ext.compiler import compiles
        import itertools

        counter = itertools.count(1)

        class IncDefault(ColumnElement):
            pass

        @compiles(IncDefault)
        def compile(element, compiler, **kw):
            # cache the counter value on the statement
            # itself so the assertsql system gets the same
            # value when it compiles the statement a second time
            stmt = compiler.statement
            if hasattr(stmt, "_counter"):
                return stmt._counter
            else:
                stmt._counter = str(next(counter))
                return stmt._counter

        Table(
            'version_table', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'version_id', Integer, nullable=False,
                default=IncDefault(), onupdate=IncDefault()),
            Column('value', String(40), nullable=False))

    @classmethod
    def setup_classes(cls):

        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def _fixture(self, expire_on_commit=True, eager_defaults=False):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        mapper(
            Foo, version_table, version_id_col=version_table.c.version_id,
            version_id_generator=False,
            eager_defaults=eager_defaults
        )

        s1 = Session(expire_on_commit=expire_on_commit)
        return s1

    def test_insert_col(self):
        self._test_insert_col()

    def test_insert_col_eager_defaults(self):
        self._test_insert_col(eager_defaults=True)

    def _test_insert_col(self, **kw):
        sess = self._fixture(**kw)

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)

        statements = [
            # note that the assertsql tests the rule against
            # "default" - on a "returning" backend, the statement
            # includes "RETURNING"
            CompiledSQL(
                "INSERT INTO version_table (version_id, value) "
                "VALUES (1, :value)",
                lambda ctx: [{'value': 'f1'}]
            )
        ]
        if not testing.db.dialect.implicit_returning:
            # DBs without implicit returning, we must immediately
            # SELECT for the new version id
            statements.append(
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :param_1",
                    lambda ctx: [{"param_1": 1}]
                )
            )
        self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_update_col(self):
        self._test_update_col()

    def test_update_col_eager_defaults(self):
        self._test_update_col(eager_defaults=True)

    def _test_update_col(self, **kw):
        sess = self._fixture(**kw)

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)
        sess.flush()

        f1.value = 'f2'

        statements = [
            # note that the assertsql tests the rule against
            # "default" - on a "returning" backend, the statement
            # includes "RETURNING"
            CompiledSQL(
                "UPDATE version_table SET version_id=2, value=:value "
                "WHERE version_table.id = :version_table_id AND "
                "version_table.version_id = :version_table_version_id",
                lambda ctx: [
                    {
                        "version_table_id": 1,
                        "version_table_version_id": 1, "value": "f2"}]
            )
        ]
        if not testing.db.dialect.implicit_returning:
            # DBs without implicit returning, we must immediately
            # SELECT for the new version id
            statements.append(
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :param_1",
                    lambda ctx: [{"param_1": 1}]
                )
            )
        self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_sql_expr_bump(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)
        sess.flush()

        eq_(f1.version_id, 1)

        f1.id = self.classes.Foo.id + 0

        sess.flush()

        eq_(f1.version_id, 2)

    @testing.requires.returning
    def test_sql_expr_w_mods_bump(self):
        sess = self._fixture()

        f1 = self.classes.Foo(id=2, value='f1')
        sess.add(f1)
        sess.flush()

        eq_(f1.version_id, 1)

        f1.id = self.classes.Foo.id + 3

        sess.flush()

        eq_(f1.id, 5)
        eq_(f1.version_id, 2)

    def test_multi_update(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value='f1')
        f2 = self.classes.Foo(value='f2')
        f3 = self.classes.Foo(value='f3')
        sess.add_all([f1, f2, f3])
        sess.flush()

        f1.value = 'f1a'
        f2.value = 'f2a'
        f3.value = 'f3a'

        statements = [
            # note that the assertsql tests the rule against
            # "default" - on a "returning" backend, the statement
            # includes "RETURNING"
            CompiledSQL(
                "UPDATE version_table SET version_id=2, value=:value "
                "WHERE version_table.id = :version_table_id AND "
                "version_table.version_id = :version_table_version_id",
                lambda ctx: [
                    {
                        "version_table_id": 1,
                        "version_table_version_id": 1, "value": "f1a"}]
            ),
            CompiledSQL(
                "UPDATE version_table SET version_id=2, value=:value "
                "WHERE version_table.id = :version_table_id AND "
                "version_table.version_id = :version_table_version_id",
                lambda ctx: [
                    {
                        "version_table_id": 2,
                        "version_table_version_id": 1, "value": "f2a"}]
            ),
            CompiledSQL(
                "UPDATE version_table SET version_id=2, value=:value "
                "WHERE version_table.id = :version_table_id AND "
                "version_table.version_id = :version_table_version_id",
                lambda ctx: [
                    {
                        "version_table_id": 3,
                        "version_table_version_id": 1, "value": "f3a"}]
            )
        ]
        if not testing.db.dialect.implicit_returning:
            # DBs without implicit returning, we must immediately
            # SELECT for the new version id
            statements.extend([
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :param_1",
                    lambda ctx: [{"param_1": 1}]
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :param_1",
                    lambda ctx: [{"param_1": 2}]
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :param_1",
                    lambda ctx: [{"param_1": 3}]
                )
            ])
        self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_delete_col(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)
        sess.flush()

        sess.delete(f1)

        statements = [
            # note that the assertsql tests the rule against
            # "default" - on a "returning" backend, the statement
            # includes "RETURNING"
            CompiledSQL(
                "DELETE FROM version_table "
                "WHERE version_table.id = :id AND "
                "version_table.version_id = :version_id",
                lambda ctx: [{"id": 1, "version_id": 1}]
            )
        ]
        self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_concurrent_mod_err_expire_on_commit(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)
        sess.commit()

        f1.value

        s2 = Session()
        f2 = s2.query(self.classes.Foo).first()
        f2.value = 'f2'
        s2.commit()

        f1.value = 'f3'

        assert_raises_message(
            orm.exc.StaleDataError,
            r"UPDATE statement on table 'version_table' expected to "
            r"update 1 row\(s\); 0 were matched.",
            sess.commit
        )

    def test_concurrent_mod_err_noexpire_on_commit(self):
        sess = self._fixture(expire_on_commit=False)

        f1 = self.classes.Foo(value='f1')
        sess.add(f1)
        sess.commit()

        # here, we're not expired overall, so no load occurs and we
        # stay without a version id, unless we've emitted
        # a SELECT for it within the flush.
        f1.value

        s2 = Session(expire_on_commit=False)
        f2 = s2.query(self.classes.Foo).first()
        f2.value = 'f2'
        s2.commit()

        f1.value = 'f3'

        assert_raises_message(
            orm.exc.StaleDataError,
            r"UPDATE statement on table 'version_table' expected to "
            r"update 1 row\(s\); 0 were matched.",
            sess.commit
        )


class ManualVersionTest(fixtures.MappedTest):
    run_define_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a", metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('data', String(30)),
            Column('vid', Integer)
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(
            cls.classes.A, cls.tables.a, version_id_col=cls.tables.a.c.vid,
            version_id_generator=False)

    def test_insert(self):
        sess = Session()
        a1 = self.classes.A()

        a1.vid = 1
        sess.add(a1)
        sess.commit()

        eq_(a1.vid, 1)

    def test_update(self):
        sess = Session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = 'd1'
        sess.add(a1)
        sess.commit()

        a1.vid = 2
        a1.data = 'd2'

        sess.commit()

        eq_(a1.vid, 2)

    def test_update_concurrent_check(self):
        sess = Session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = 'd1'
        sess.add(a1)
        sess.commit()

        a1.vid = 2
        sess.execute(self.tables.a.update().values(vid=3))
        a1.data = 'd2'
        assert_raises(
            orm_exc.StaleDataError,
            sess.commit
        )

    def test_update_version_conditional(self):
        sess = Session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = 'd1'
        sess.add(a1)
        sess.commit()

        # change the data and UPDATE without
        # incrementing version id
        a1.data = 'd2'
        sess.commit()

        eq_(a1.vid, 1)

        a1.data = 'd3'
        a1.vid = 2
        sess.commit()

        eq_(a1.vid, 2)
