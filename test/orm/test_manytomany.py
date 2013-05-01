from sqlalchemy.testing import assert_raises, \
    assert_raises_message, eq_
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import mapper, relationship, Session,  \
    exc as orm_exc, sessionmaker, backref
from sqlalchemy.testing import fixtures


class M2MTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('place', metadata,
            Column('place_id', Integer, test_needs_autoincrement=True,
                   primary_key=True),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        Table('transition', metadata,
            Column('transition_id', Integer,
                   test_needs_autoincrement=True, primary_key=True),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        Table('place_thingy', metadata,
            Column('thingy_id', Integer, test_needs_autoincrement=True,
                   primary_key=True),
            Column('place_id', Integer, ForeignKey('place.place_id'),
                   nullable=False),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        # association table #1
        Table('place_input', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')),
                   test_needs_acid=True,
                   )

        # association table #2
        Table('place_output', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')),
                   test_needs_acid=True,
                   )

        Table('place_place', metadata,
              Column('pl1_id', Integer, ForeignKey('place.place_id')),
              Column('pl2_id', Integer, ForeignKey('place.place_id')),
              test_needs_acid=True,
              )

    @classmethod
    def setup_classes(cls):
        class Place(cls.Basic):
            def __init__(self, name):
                self.name = name

        class PlaceThingy(cls.Basic):
            def __init__(self, name):
                self.name = name

        class Transition(cls.Basic):
            def __init__(self, name):
                self.name = name

    def test_overlapping_attribute_error(self):
        place, Transition, place_input, Place, transition = (self.tables.place,
                                self.classes.Transition,
                                self.tables.place_input,
                                self.classes.Place,
                                self.tables.transition)

        mapper(Place, place, properties={
            'transitions': relationship(Transition,
                                secondary=place_input, backref='places')
        })
        mapper(Transition, transition, properties={
            'places': relationship(Place,
                                secondary=place_input, backref='transitions')
        })
        assert_raises_message(sa.exc.ArgumentError,
                        "property of that name exists",
                         sa.orm.configure_mappers)

    def test_self_referential_roundtrip(self):

        place, Place, place_place = (self.tables.place,
                                self.classes.Place,
                                self.tables.place_place)

        mapper(Place, place, properties={
            'places': relationship(
                        Place,
                        secondary=place_place,
                        primaryjoin=place.c.place_id == place_place.c.pl1_id,
                        secondaryjoin=place.c.place_id == place_place.c.pl2_id,
                        order_by=place_place.c.pl2_id
                )
        })

        sess = Session()
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')
        p4 = Place('place4')
        p5 = Place('place5')
        p6 = Place('place6')
        p7 = Place('place7')
        sess.add_all((p1, p2, p3, p4, p5, p6, p7))
        p1.places.append(p2)
        p1.places.append(p3)
        p5.places.append(p6)
        p6.places.append(p1)
        p7.places.append(p1)
        p1.places.append(p5)
        p4.places.append(p3)
        p3.places.append(p4)
        sess.commit()

        eq_(p1.places, [p2, p3, p5])
        eq_(p5.places, [p6])
        eq_(p7.places, [p1])
        eq_(p6.places, [p1])
        eq_(p4.places, [p3])
        eq_(p3.places, [p4])
        eq_(p2.places, [])

    def test_self_referential_bidirectional_mutation(self):
        place, Place, place_place = (self.tables.place,
                                self.classes.Place,
                                self.tables.place_place)

        mapper(Place, place, properties={
            'child_places': relationship(
                        Place,
                        secondary=place_place,
                        primaryjoin=place.c.place_id == place_place.c.pl1_id,
                        secondaryjoin=place.c.place_id == place_place.c.pl2_id,
                        order_by=place_place.c.pl2_id,
                        backref='parent_places'
                )
        })

        sess = Session()
        p1 = Place('place1')
        p2 = Place('place2')
        p2.parent_places = [p1]
        sess.add_all([p1, p2])
        p1.parent_places.append(p2)
        sess.commit()

        assert p1 in p2.parent_places
        assert p2 in p1.parent_places


    def test_joinedload_on_double(self):
        """test that a mapper can have two eager relationships to the same table, via
        two different association tables.  aliases are required."""

        place_input, transition, Transition, PlaceThingy, \
                            place, place_thingy, Place, \
                            place_output = (self.tables.place_input,
                                self.tables.transition,
                                self.classes.Transition,
                                self.classes.PlaceThingy,
                                self.tables.place,
                                self.tables.place_thingy,
                                self.classes.Place,
                                self.tables.place_output)


        mapper(PlaceThingy, place_thingy)
        mapper(Place, place, properties={
            'thingies': relationship(PlaceThingy, lazy='joined')
        })

        mapper(Transition, transition, properties=dict(
            inputs=relationship(Place, place_output, lazy='joined'),
            outputs=relationship(Place, place_input, lazy='joined'),
            )
        )

        tran = Transition('transition1')
        tran.inputs.append(Place('place1'))
        tran.outputs.append(Place('place2'))
        tran.outputs.append(Place('place3'))
        sess = Session()
        sess.add(tran)
        sess.commit()

        r = sess.query(Transition).all()
        self.assert_unordered_result(r, Transition,
            {'name': 'transition1',
            'inputs': (Place, [{'name': 'place1'}]),
            'outputs': (Place, [{'name': 'place2'}, {'name': 'place3'}])
            })

    def test_bidirectional(self):
        place_input, transition, Transition, Place, place, place_output = (
                                self.tables.place_input,
                                self.tables.transition,
                                self.classes.Transition,
                                self.classes.Place,
                                self.tables.place,
                                self.tables.place_output)

        mapper(Place, place)
        mapper(Transition, transition, properties=dict(
            inputs=relationship(Place, place_output,
                                backref=backref('inputs',
                                    order_by=transition.c.transition_id),
                                order_by=Place.place_id),
            outputs=relationship(Place, place_input,
                                backref=backref('outputs',
                                    order_by=transition.c.transition_id),
                                order_by=Place.place_id),
            )
        )

        t1 = Transition('transition1')
        t2 = Transition('transition2')
        t3 = Transition('transition3')
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')

        sess = Session()
        sess.add_all([p3, p1, t1, t2, p2, t3])

        t1.inputs.append(p1)
        t1.inputs.append(p2)
        t1.outputs.append(p3)
        t2.inputs.append(p1)
        p2.inputs.append(t2)
        p3.inputs.append(t2)
        p1.outputs.append(t1)
        sess.commit()

        self.assert_result([t1],
                    Transition, {'outputs':
                            (Place, [{'name': 'place3'}, {'name': 'place1'}])})
        self.assert_result([p2],
                        Place, {'inputs':
                                (Transition, [{'name': 'transition1'},
                                                {'name': 'transition2'}])})

    @testing.requires.sane_multi_rowcount
    def test_stale_conditions(self):
        Place, Transition, place_input, place, transition = (
                                self.classes.Place,
                                self.classes.Transition,
                                self.tables.place_input,
                                self.tables.place,
                                self.tables.transition)

        mapper(Place, place, properties={
            'transitions': relationship(Transition, secondary=place_input,
                                            passive_updates=False)
        })
        mapper(Transition, transition)

        p1 = Place('place1')
        t1 = Transition('t1')
        p1.transitions.append(t1)
        sess = sessionmaker()()
        sess.add_all([p1, t1])
        sess.commit()

        p1.place_id
        p1.transitions

        sess.execute("delete from place_input", mapper=Place)
        p1.place_id = 7

        assert_raises_message(
            orm_exc.StaleDataError,
            r"UPDATE statement on table 'place_input' expected to "
            r"update 1 row\(s\); Only 0 were matched.",
            sess.commit
        )
        sess.rollback()

        p1.place_id
        p1.transitions
        sess.execute("delete from place_input", mapper=Place)
        p1.transitions.remove(t1)
        assert_raises_message(
            orm_exc.StaleDataError,
            r"DELETE statement on table 'place_input' expected to "
            r"delete 1 row\(s\); Only 0 were matched.",
            sess.commit
        )


class AssortedPersistenceTests(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("left", metadata,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('data', String(30))
            )

        Table("right", metadata,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('data', String(30)),
            )

        Table('secondary', metadata,
            Column('left_id', Integer, ForeignKey('left.id'),
                                        primary_key=True),
            Column('right_id', Integer, ForeignKey('right.id'),
                                        primary_key=True),
            )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass
        class B(cls.Comparable):
            pass

    def _standard_bidirectional_fixture(self):
        left, secondary, right = self.tables.left, \
                    self.tables.secondary, self.tables.right
        A, B = self.classes.A, self.classes.B
        mapper(A, left, properties={
            'bs': relationship(B, secondary=secondary,
                            backref='as', order_by=right.c.id)
        })
        mapper(B, right)

    def _bidirectional_onescalar_fixture(self):
        left, secondary, right = self.tables.left, \
                    self.tables.secondary, self.tables.right
        A, B = self.classes.A, self.classes.B
        mapper(A, left, properties={
            'bs': relationship(B, secondary=secondary,
                            backref=backref('a', uselist=False),
                            order_by=right.c.id)
        })
        mapper(B, right)

    def test_session_delete(self):
        self._standard_bidirectional_fixture()
        A, B = self.classes.A, self.classes.B
        secondary = self.tables.secondary

        sess = Session()
        sess.add_all([
            A(data='a1', bs=[B(data='b1')]),
            A(data='a2', bs=[B(data='b2')])
        ])
        sess.commit()

        a1 = sess.query(A).filter_by(data='a1').one()
        sess.delete(a1)
        sess.flush()
        eq_(sess.query(secondary).count(), 1)

        a2 = sess.query(A).filter_by(data='a2').one()
        sess.delete(a2)
        sess.flush()
        eq_(sess.query(secondary).count(), 0)

    def test_remove_scalar(self):
        # test setting a uselist=False to None
        self._bidirectional_onescalar_fixture()
        A, B = self.classes.A, self.classes.B
        secondary = self.tables.secondary

        sess = Session()
        sess.add_all([
            A(data='a1', bs=[B(data='b1'), B(data='b2')]),
        ])
        sess.commit()

        a1 = sess.query(A).filter_by(data='a1').one()
        b2 = sess.query(B).filter_by(data='b2').one()
        assert b2.a is a1

        b2.a = None
        sess.commit()

        eq_(a1.bs, [B(data='b1')])
        eq_(b2.a, None)
        eq_(sess.query(secondary).count(), 1)
