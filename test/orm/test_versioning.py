import sqlalchemy as sa
from test.lib import engines, testing
from sqlalchemy import Integer, String, ForeignKey, literal_column, \
    orm, exc, select
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, Session, \
    create_session, column_property, sessionmaker,\
    exc as orm_exc
from test.lib.testing import eq_, ne_, assert_raises, assert_raises_message
from test.lib import fixtures
from test.orm import _fixtures
from test.lib import fixtures


_uuids = [
    '1fc614acbb904742a2990f86af6ded95',
    '23e253786f4d491b9f9d6189dc33de9b',
    'fc44910db37e43fd93e9ec8165b885cf',
    '0187a1832b4249e6b48911821d86de58',
    '778af6ea2fb74a009d8d2f5abe5dc29a',
    '51a6ce031aff47e4b5f2895c4161f120',
    '7434097cd319401fb9f15fa443ccbbbb',
    '9bc548a8128e4a85ac18060bc3f4b7d3',
    '59548715e3c440b7bcb96417d06f7930',
    'd7647c7004734de196885ca2bd73adf8',
    '70cef121d3ff48d39906b6d1ac77f41a',
    'ee37a8a6430c466aa322b8a215a0dd70',
    '782a5f04b4364a53a6fce762f48921c1',
    'bef510f2420f4476a7629013ead237f5',
    ]

def make_uuid():
    """generate uuids even on Python 2.4 which has no 'uuid'"""
    return _uuids.pop(0)

class VersioningTest(fixtures.MappedTest):

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

        mapper(Foo, version_table, 
                version_id_col=version_table.c.version_id)
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

            f1.value='f1rev2'
            assert_raises(sa.exc.SAWarning, s1.commit)
        finally:
            testing.db.dialect.supports_sane_rowcount = save

    @testing.emits_warning_on('+zxjdbc', r'.*does not support (update|delete)d rowcount')
    def test_basic(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value='f1')
        f2 = Foo(value='f2')
        s1.add_all((f1, f2))
        s1.commit()

        f1.value='f1rev2'
        s1.commit()

        s2 = create_session(autocommit=False)
        f1_s = s2.query(Foo).get(f1.id)
        f1_s.value='f1rev3'
        s2.commit()

        f1.value='f1rev3mine'

        # Only dialects with a sane rowcount can detect the
        # StaleDataError
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(sa.orm.exc.StaleDataError, 
            r"UPDATE statement on table 'version_table' expected "
            r"to update 1 row\(s\); 0 were matched.",
            s1.commit),
            s1.rollback()
        else:
            s1.commit()

        # new in 0.5 !  dont need to close the session
        f1 = s1.query(Foo).get(f1.id)
        f2 = s1.query(Foo).get(f2.id)

        f1_s.value='f1rev4'
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support (update|delete)d rowcount')
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
        """query.with_lockmode performs a 'version check' on an already loaded instance"""

        Foo = self.classes.Foo


        s1 = self._fixture()
        f1s1 = Foo(value='f1 value')
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).get(f1s1.id)
        f1s2.value='f1 new value'
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


    @testing.emits_warning(r'.*does not support updated rowcount')
    @engines.close_open_connections
    @testing.requires.update_nowait
    def test_versioncheck_for_update(self):
        """query.with_lockmode performs a 'version check' on an already loaded instance"""

        Foo = self.classes.Foo


        s1 = self._fixture()
        f1s1 = Foo(value='f1 value')
        s1.add(f1s1)
        s1.commit()

        s2 = create_session(autocommit=False)
        f1s2 = s2.query(Foo).get(f1s1.id)
        s2.refresh(f1s2, lockmode='update')
        f1s2.value='f1 new value'

        assert_raises(
            exc.DBAPIError,
            s1.refresh, f1s1, lockmode='update_nowait'
        )
        s1.rollback()

        s2.commit()
        s1.refresh(f1s1, lockmode='update_nowait')
        assert f1s1.version_id == f1s2.version_id

    @testing.emits_warning(r'.*does not support updated rowcount')
    @engines.close_open_connections
    def test_noversioncheck(self):
        """test query.with_lockmode works when the mapper has no version id col"""

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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

class RowSwitchTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('p', metadata,
            Column('id', String(10), primary_key=True),
            Column('version_id', Integer, default=1, nullable=False),
            Column('data', String(50))
        )
        Table('c', metadata,
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
        p, c, C, P = (cls.tables.p,
                                cls.tables.c,
                                cls.classes.C,
                                cls.classes.P)

        mapper(P, p, version_id_col=p.c.version_id, 
            properties={
            'c':relationship(C, uselist=False, cascade='all, delete-orphan')
        })
        mapper(C, c, version_id_col=c.c.version_id)

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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
    @classmethod
    def define_tables(cls, metadata):
        Table('p', metadata,
            Column('id', String(10), primary_key=True),
            Column('version_id', String(32), nullable=False),
            Column('data', String(50))
        )
        Table('c', metadata,
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
        p, c, C, P = (cls.tables.p,
                                cls.tables.c,
                                cls.classes.C,
                                cls.classes.P)

        mapper(P, p, version_id_col=p.c.version_id, 
            version_id_generator=lambda x:make_uuid(),
            properties={
            'c':relationship(C, uselist=False, cascade='all, delete-orphan')
        })
        mapper(C, c, version_id_col=c.c.version_id,
                    version_id_generator=lambda x:make_uuid(),
        )

    @testing.emits_warning_on('+zxjdbc', r'.*does not support updated rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support (update|delete)d rowcount')
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

    @testing.emits_warning_on('+zxjdbc', r'.*does not support (update|delete)d rowcount')
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

class InheritanceTwoVersionIdsTest(fixtures.MappedTest):
    """Test versioning where both parent/child table have a
    versioning column.

    """
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('version_id', Integer, nullable=True),
            Column('data', String(50))
        )
        Table('sub', metadata,
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
        Base, sub, base, Sub = (self.classes.Base,
                                self.tables.sub,
                                self.tables.base,
                                self.classes.Sub)

        mapper(Base, base, 
                version_id_col=base.c.version_id)
        mapper(Sub, sub, inherits=Base)

        session = Session()
        b1 = Base(data='b1')
        session.add(b1)
        session.commit()
        eq_(b1.version_id, 1)
        # base is populated
        eq_(select([base.c.version_id]).scalar(), 1)

    def test_sub_both(self):
        Base, sub, base, Sub = (self.classes.Base,
                                self.tables.sub,
                                self.tables.base,
                                self.classes.Sub)

        mapper(Base, base, 
                version_id_col=base.c.version_id)
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
        Base, sub, base, Sub = (self.classes.Base,
                                self.tables.sub,
                                self.tables.base,
                                self.classes.Sub)

        mapper(Base, base)
        mapper(Sub, sub, inherits=Base, 
                version_id_col=sub.c.version_id)

        session = Session()
        s1 = Sub(data='s1', sub_data='s1')
        session.add(s1)
        session.commit()

        # table is populated
        eq_(select([sub.c.version_id]).scalar(), 1)

        # base is not
        eq_(select([base.c.version_id]).scalar(), None)

    def test_mismatch_version_col_warning(self):
        Base, sub, base, Sub = (self.classes.Base,
                                self.tables.sub,
                                self.tables.base,
                                self.classes.Sub)

        mapper(Base, base, 
                version_id_col=base.c.version_id)

        assert_raises_message(
            exc.SAWarning,
            "Inheriting version_id_col 'version_id' does not "
            "match inherited version_id_col 'version_id' and will not "
            "automatically populate the inherited versioning column. "
            "version_id_col should only be specified on "
            "the base-most mapper that includes versioning.",
            mapper,
            Sub, sub, inherits=Base, 
                version_id_col=sub.c.version_id)
