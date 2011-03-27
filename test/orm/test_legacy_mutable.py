"""Test the interaction of :class:`.MutableType` as well as the 
``mutable=True`` flag with the ORM.

For new mutablity functionality, see test.ext.test_mutable.

"""
from test.lib.testing import eq_
import operator
from sqlalchemy.orm import mapper as orm_mapper

import sqlalchemy as sa
from sqlalchemy import Integer, String, ForeignKey
from test.lib import testing, pickleable
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, create_session, Session, attributes
from test.lib.testing import eq_, ne_
from test.lib.util import gc_collect
from test.lib import fixtures
from test.orm import _fixtures

class MutableTypesTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', sa.PickleType(mutable=True)),
            Column('val', sa.Unicode(30)))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mutable_t, Foo = cls.tables.mutable_t, cls.classes.Foo

        mapper(Foo, mutable_t)

    def test_modified_status(self):
        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4,5))

        session = Session()
        session.add(f1)
        session.commit()

        f2 = session.query(Foo).first()
        assert 'data' in sa.orm.attributes.instance_state(f2).unmodified
        eq_(f2.data, f1.data)

        f2.data.y = 19
        assert f2 in session.dirty
        assert 'data' not in sa.orm.attributes.instance_state(f2).unmodified

    def test_mutations_persisted(self):
        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4,5))

        session = Session()
        session.add(f1)
        session.commit()
        f1.data
        session.close()

        f2 = session.query(Foo).first()
        f2.data.y = 19
        session.commit()
        f2.data
        session.close()

        f3 = session.query(Foo).first()
        ne_(f3.data,f1.data)
        eq_(f3.data, pickleable.Bar(4, 19))

    def test_no_unnecessary_update(self):
        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4,5), val = u'hi')

        session = Session()
        session.add(f1)
        session.commit()

        self.sql_count_(0, session.commit)

        f1.val = u'someothervalue'
        self.assert_sql(testing.db, session.commit, [
            ("UPDATE mutable_t SET val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'someothervalue'})])

        f1.val = u'hi'
        f1.data.x = 9
        self.assert_sql(testing.db, session.commit, [
            ("UPDATE mutable_t SET data=:data, val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'hi', 'data':f1.data})])

    def test_mutated_state_resurrected(self):
        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4,5), val = u'hi')

        session = Session()
        session.add(f1)
        session.commit()

        f1.data.y = 19
        del f1

        gc_collect()
        assert len(session.identity_map) == 1

        session.commit()

        assert session.query(Foo).one().data == pickleable.Bar(4, 19)

    def test_mutated_plus_scalar_state_change_resurrected(self):
        """test that a non-mutable attribute event subsequent to
        a mutable event prevents the object from falling into
        resurrected state.

         """

        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4, 5), val=u'some val')
        session = Session()
        session.add(f1)
        session.commit()
        f1.data.x = 10
        f1.data.y = 15
        f1.val=u'some new val'

        assert sa.orm.attributes.instance_state(f1)._strong_obj is not None

        del f1
        session.commit()
        eq_(
            session.query(Foo.val).all(),
            [('some new val', )]
        )

    def test_non_mutated_state_not_resurrected(self):
        Foo = self.classes.Foo

        f1 = Foo(data = pickleable.Bar(4,5))

        session = Session()
        session.add(f1)
        session.commit()

        session = Session()
        f1 = session.query(Foo).first()
        del f1
        gc_collect()

        assert len(session.identity_map) == 0
        f1 = session.query(Foo).first()
        assert not attributes.instance_state(f1).modified

    def test_scalar_no_net_change_no_update(self):
        """Test that a no-net-change on a scalar attribute event
        doesn't cause an UPDATE for a mutable state.

         """

        Foo = self.classes.Foo


        f1 = Foo(val=u'hi')

        session = Session()
        session.add(f1)
        session.commit()
        session.close()

        f1 = session.query(Foo).first()
        f1.val = u'hi'
        self.sql_count_(0, session.commit)

    def test_expire_attribute_set(self):
        """test no SELECT emitted when assigning to an expired
        mutable attribute.

        """

        Foo = self.classes.Foo


        f1 = Foo(data = pickleable.Bar(4, 5), val=u'some val')
        session = Session()
        session.add(f1)
        session.commit()

        assert 'data' not in f1.__dict__
        def go():
            f1.data = pickleable.Bar(10, 15)
        self.sql_count_(0, go)
        session.commit()

        eq_(f1.data.x, 10)

    def test_expire_mutate(self):
        """test mutations are detected on an expired mutable
        attribute."""

        Foo = self.classes.Foo


        f1 = Foo(data = pickleable.Bar(4, 5), val=u'some val')
        session = Session()
        session.add(f1)
        session.commit()

        assert 'data' not in f1.__dict__
        def go():
            f1.data.x = 10
        self.sql_count_(1, go)
        session.commit()

        eq_(f1.data.x, 10)

    def test_deferred_attribute_set(self):
        """test no SELECT emitted when assigning to a deferred
        mutable attribute.

        """

        mutable_t, Foo = self.tables.mutable_t, self.classes.Foo

        sa.orm.clear_mappers()
        mapper(Foo, mutable_t, properties={
            'data':sa.orm.deferred(mutable_t.c.data)
        })

        f1 = Foo(data = pickleable.Bar(4, 5), val=u'some val')
        session = Session()
        session.add(f1)
        session.commit()

        session.close()

        f1 = session.query(Foo).first()
        def go():
            f1.data = pickleable.Bar(10, 15)
        self.sql_count_(0, go)
        session.commit()

        eq_(f1.data.x, 10)

    def test_deferred_mutate(self):
        """test mutations are detected on a deferred mutable
        attribute."""

        mutable_t, Foo = self.tables.mutable_t, self.classes.Foo


        sa.orm.clear_mappers()
        mapper(Foo, mutable_t, properties={
            'data':sa.orm.deferred(mutable_t.c.data)
        })

        f1 = Foo(data = pickleable.Bar(4, 5), val=u'some val')
        session = Session()
        session.add(f1)
        session.commit()

        session.close()

        f1 = session.query(Foo).first()
        def go():
            f1.data.x = 10
        self.sql_count_(1, go)
        session.commit()

        def go():
            eq_(f1.data.x, 10)
        self.sql_count_(1, go)


class PickledDictsTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', 
                sa.PickleType(comparator=operator.eq, mutable=True)))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mutable_t, Foo = cls.tables.mutable_t, cls.classes.Foo

        mapper(Foo, mutable_t)

    def test_dicts(self):
        """Dictionaries may not pickle the same way twice."""

        Foo = self.classes.Foo


        f1 = Foo()
        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        self.sql_count_(0, session.commit)

        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        self.sql_count_(0, session.commit)

        f1.data[0]['personne']['VenSoir']= False
        self.sql_count_(1, session.commit)

        session.expunge_all()
        f = session.query(Foo).get(f1.id)
        eq_(f.data,
            [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': False,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ])
