from test.lib.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from test.lib import testing
from sqlalchemy import MetaData, Integer, String, ForeignKey, func, \
    util, select
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, backref, \
    class_mapper,  \
    validates, aliased
from sqlalchemy.orm import attributes, \
    composite, relationship, \
    Session
from test.lib.testing import eq_
from test.orm import _base, _fixtures


class PointTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('graphs', metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('name', String(30)))

        Table('edges', metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('graph_id', Integer, 
                                ForeignKey('graphs.id'), 
                                nullable=False),
            Column('x1', Integer),
            Column('y1', Integer),
            Column('x2', Integer),
            Column('y2', Integer),
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Point(_base.BasicEntity):
            def __init__(self, x, y):
                self.x = x
                self.y = y
            def __composite_values__(self):
                return [self.x, self.y]
            __hash__ = None
            def __eq__(self, other):
                return isinstance(other, Point) and \
                        other.x == self.x and \
                        other.y == self.y
            def __ne__(self, other):
                return not isinstance(other, Point) or \
                        not self.__eq__(other)

        class Graph(_base.BasicEntity):
            pass
        class Edge(_base.BasicEntity):
            def __init__(self, *args):
                if args:
                    self.start, self.end = args

        mapper(Graph, graphs, properties={
            'edges':relationship(Edge)
        })
        mapper(Edge, edges, properties={
            'start':sa.orm.composite(Point, edges.c.x1, edges.c.y1),
            'end': sa.orm.composite(Point, edges.c.x2, edges.c.y2)
        })

    @testing.resolve_artifact_names
    def _fixture(self):
        sess = Session()
        g = Graph(id=1, edges=[
            Edge(Point(3, 4), Point(5, 6)),
            Edge(Point(14, 5), Point(2, 7))
        ])
        sess.add(g)
        sess.commit()
        return sess

    @testing.resolve_artifact_names
    def test_round_trip(self):

        sess = self._fixture()

        g1 = sess.query(Graph).first()
        sess.close()

        g = sess.query(Graph).get(g1.id)
        eq_(
            [(e.start, e.end) for e in g.edges],
            [
                (Point(3, 4), Point(5, 6)),
                (Point(14, 5), Point(2, 7)),
            ]
        )

    @testing.resolve_artifact_names
    def test_detect_change(self):
        sess = self._fixture()

        g = sess.query(Graph).first()
        g.edges[1].end = Point(18, 4)
        sess.commit()

        e = sess.query(Edge).get(g.edges[1].id)
        eq_(e.end, Point(18, 4))

    @testing.resolve_artifact_names
    def test_eager_load(self):
        sess = self._fixture()

        g = sess.query(Graph).first()
        sess.close()

        def go():
            g2 = sess.query(Graph).\
                  options(sa.orm.joinedload('edges')).\
                  get(g.id)

            eq_(
                [(e.start, e.end) for e in g2.edges],
                [
                    (Point(3, 4), Point(5, 6)),
                    (Point(14, 5), Point(2, 7)),
                ]
            )
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_comparator(self):
        sess = self._fixture()

        g = sess.query(Graph).first()

        assert sess.query(Edge).\
                    filter(Edge.start==Point(3, 4)).one() is \
                    g.edges[0]

        assert sess.query(Edge).\
                    filter(Edge.start!=Point(3, 4)).first() is \
                    g.edges[1]

        eq_(
            sess.query(Edge).filter(Edge.start==None).all(), 
            []
        )

    @testing.resolve_artifact_names
    def test_query_cols(self):
        sess = self._fixture()

        eq_(
            sess.query(Edge.start, Edge.end).all(), 
            [(3, 4, 5, 6), (14, 5, 2, 7)]
        )

    @testing.resolve_artifact_names
    def test_delete(self):
        sess = self._fixture()
        g = sess.query(Graph).first()

        e = g.edges[1]
        del e.end
        sess.flush()
        eq_(
            sess.query(Edge.start, Edge.end).all(), 
            [(3, 4, 5, 6), (14, 5, None, None)]
        )

    @testing.resolve_artifact_names
    def test_save_null(self):
        """test saving a null composite value

        See google groups thread for more context:
        http://groups.google.com/group/sqlalchemy/browse_thread/thread/0c6580a1761b2c29

        """
        sess = Session()
        g = Graph(id=1)
        e = Edge(None, None)
        g.edges.append(e)

        sess.add(g)
        sess.commit()

        g2 = sess.query(Graph).get(1)
        assert g2.edges[-1].start.x is None
        assert g2.edges[-1].start.y is None

    @testing.resolve_artifact_names
    def test_expire(self):
        sess = self._fixture()
        g = sess.query(Graph).first()
        e = g.edges[0]
        sess.expire(e)
        assert 'start' not in e.__dict__
        assert e.start == Point(3, 4)

    @testing.resolve_artifact_names
    def test_default_value(self):
        e = Edge()
        eq_(e.start, None)

class PrimaryKeyTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('graphs', metadata,
            Column('id', Integer, primary_key=True, 
                        test_needs_autoincrement=True),
            Column('version_id', Integer, primary_key=True, 
                                            nullable=True),
            Column('name', String(30)))

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Version(_base.BasicEntity):
            def __init__(self, id, version):
                self.id = id
                self.version = version
            def __composite_values__(self):
                return (self.id, self.version)
            __hash__ = None
            def __eq__(self, other):
                return isinstance(other, Version) and \
                                other.id == self.id and \
                                other.version == self.version
            def __ne__(self, other):
                return not self.__eq__(other)

        class Graph(_base.BasicEntity):
            def __init__(self, version):
                self.version = version

        mapper(Graph, graphs, properties={
            'version':sa.orm.composite(Version, graphs.c.id,
                                       graphs.c.version_id)})


    @testing.resolve_artifact_names
    def _fixture(self):
        sess = Session()
        g = Graph(Version(1, 1))
        sess.add(g)
        sess.commit()
        return sess

    @testing.resolve_artifact_names
    def test_get_by_col(self):

        sess = self._fixture()
        g = sess.query(Graph).first()

        g2 = sess.query(Graph).get([g.id, g.version_id])
        eq_(g.version, g2.version)

    @testing.resolve_artifact_names
    def test_get_by_composite(self):
        sess = self._fixture()
        g = sess.query(Graph).first()

        g2 = sess.query(Graph).get(Version(g.id, g.version_id))
        eq_(g.version, g2.version)

    @testing.fails_on('mssql', 'Cannot update identity columns.')
    @testing.resolve_artifact_names
    def test_pk_mutation(self):
        sess = self._fixture()

        g = sess.query(Graph).first()

        g.version = Version(2, 1)
        sess.commit()
        g2 = sess.query(Graph).get(Version(2, 1))
        eq_(g.version, g2.version)

    @testing.fails_on_everything_except("sqlite")
    @testing.resolve_artifact_names
    def test_null_pk(self):
        sess = Session()

        # test pk with one column NULL
        # only sqlite can really handle this
        g = Graph(Version(2, None))
        sess.add(g)
        sess.commit()
        g2 = sess.query(Graph).filter_by(version=Version(2, None)).one()
        eq_(g.version, g2.version)

class DefaultsTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foobars', metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('x1', Integer, default=2),
            Column('x2', Integer),
            Column('x3', Integer, default=15),
            Column('x4', Integer)
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Foobar(_base.BasicEntity):
            pass

        class FBComposite(_base.BasicEntity):
            def __init__(self, x1, x2, x3, x4):
                self.x1 = x1
                self.x2 = x2
                self.x3 = x3
                self.x4 = x4
            def __composite_values__(self):
                return self.x1, self.x2, self.x3, self.x4
            __hash__ = None
            def __eq__(self, other):
                return other.x1 == self.x1 and \
                        other.x2 == self.x2 and \
                        other.x3 == self.x3 and \
                        other.x4 == self.x4
            def __ne__(self, other):
                return not self.__eq__(other)

        mapper(Foobar, foobars, properties=dict(
            foob=sa.orm.composite(FBComposite, 
                                foobars.c.x1, 
                                foobars.c.x2, 
                                foobars.c.x3, 
                                foobars.c.x4)
        ))

    @testing.resolve_artifact_names
    def test_attributes_with_defaults(self):

        sess = Session()
        f1 = Foobar()
        f1.foob = FBComposite(None, 5, None, None)
        sess.add(f1)
        sess.flush()

        assert f1.foob == FBComposite(2, 5, 15, None)

        f2 = Foobar()
        sess.add(f2)
        sess.flush()
        assert f2.foob == FBComposite(2, None, 15, None)

    @testing.resolve_artifact_names
    def test_set_composite_values(self):
        sess = Session()
        f1 = Foobar()
        f1.foob = FBComposite(None, 5, None, None)
        sess.add(f1)
        sess.flush()

        assert f1.foob == FBComposite(2, 5, 15, None)

class MappedSelectTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('descriptions', metadata,
            Column('id', Integer, primary_key=True, 
                            test_needs_autoincrement=True),
            Column('d1', String(20)),
            Column('d2', String(20)),
        )

        Table('values', metadata,
            Column('id', Integer, primary_key=True, 
                            test_needs_autoincrement=True),
            Column('description_id', Integer, 
                            ForeignKey('descriptions.id'),
                            nullable=False),
            Column('v1', String(20)),
            Column('v2', String(20)),
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Descriptions(_base.BasicEntity):
            pass

        class Values(_base.BasicEntity):
            pass

        class CustomValues(_base.BasicEntity, list):
            def __init__(self, *args):
                self.extend(args)

            def __composite_values__(self):
                return self

        desc_values = select(
            [values, descriptions.c.d1, descriptions.c.d2],
            descriptions.c.id == values.c.description_id
        ).alias('descriptions_values') 

        mapper(Descriptions, descriptions, properties={
            'values': relationship(Values, lazy='dynamic'),
            'custom_descriptions': composite(
                                CustomValues,
                                        descriptions.c.d1,
                                        descriptions.c.d2),

        })

        mapper(Values, desc_values, properties={
            'custom_values': composite(CustomValues, 
                                            desc_values.c.v1,
                                            desc_values.c.v2),

        })

    @testing.resolve_artifact_names
    def test_set_composite_attrs_via_selectable(self):
        session = Session()
        d = Descriptions(
            custom_descriptions = CustomValues('Color', 'Number'),
            values =[
                Values(custom_values = CustomValues('Red', '5')),
                Values(custom_values=CustomValues('Blue', '1'))
            ]
        )

        session.add(d)
        session.commit()
        eq_(
            testing.db.execute(descriptions.select()).fetchall(),
            [(1, u'Color', u'Number')]
        )
        eq_(
            testing.db.execute(values.select()).fetchall(),
            [(1, 1, u'Red', u'5'), (2, 1, u'Blue', u'1')]
        )

class ManyToOneTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('a', 
            metadata,
            Column('id', Integer, primary_key=True, 
                            test_needs_autoincrement=True),
            Column('b1', String(20)),
            Column('b2_id', Integer, ForeignKey('b.id'))
        )

        Table('b', metadata,
            Column('id', Integer, primary_key=True, 
                            test_needs_autoincrement=True),
            Column('data', String(20))
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class A(_base.ComparableEntity):
            pass
        class B(_base.ComparableEntity):
            pass

        class C(_base.BasicEntity):
            def __init__(self, b1, b2):
                self.b1, self.b2 = b1, b2

            def __composite_values__(self):
                return self.b1, self.b2

            def __eq__(self, other):
                return isinstance(other, C) and \
                    other.b1 == self.b1 and \
                    other.b2 == self.b2


        mapper(A, a, properties={
            'b2':relationship(B),
            'c':composite(C, 'b1', 'b2')
        })
        mapper(B, b)

    @testing.resolve_artifact_names
    def test_persist(self):
        sess = Session()
        sess.add(A(c=C('b1', B(data='b2'))))
        sess.commit()

        a1 = sess.query(A).one()
        eq_(a1.c, C('b1', B(data='b2')))

    @testing.resolve_artifact_names
    def test_query(self):
        sess = Session()
        b1, b2 = B(data='b1'), B(data='b2')
        a1 = A(c=C('a1b1', b1))
        a2 = A(c=C('a2b1', b2))
        sess.add_all([a1, a2])
        sess.commit()

        eq_(
            sess.query(A).filter(A.c==C('a2b1', b2)).one(),
            a2
        )

class ConfigurationTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('edge', metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('x1', Integer),
            Column('y1', Integer),
            Column('x2', Integer),
            Column('y2', Integer),
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class Point(_base.BasicEntity):
            def __init__(self, x, y):
                self.x = x
                self.y = y
            def __composite_values__(self):
                return [self.x, self.y]
            def __eq__(self, other):
                return isinstance(other, Point) and \
                        other.x == self.x and \
                        other.y == self.y
            def __ne__(self, other):
                return not isinstance(other, Point) or \
                    not self.__eq__(other)

        class Edge(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def _test_roundtrip(self):
        e1 = Edge(start=Point(3, 4), end=Point(5, 6))
        sess = Session()
        sess.add(e1)
        sess.commit()

        eq_(
            sess.query(Edge).one(),
            Edge(start=Point(3, 4), end=Point(5, 6))
        )

    @testing.resolve_artifact_names
    def test_columns(self):
        mapper(Edge, edge, properties={
            'start':sa.orm.composite(Point, edge.c.x1, edge.c.y1),
            'end': sa.orm.composite(Point, edge.c.x2, edge.c.y2)
        })

        self._test_roundtrip()

    @testing.resolve_artifact_names
    def test_attributes(self):
        m = mapper(Edge, edge)
        m.add_property('start', sa.orm.composite(Point, Edge.x1, Edge.y1))
        m.add_property('end', sa.orm.composite(Point, Edge.x2, Edge.y2))

        self._test_roundtrip()

    @testing.resolve_artifact_names
    def test_strings(self):
        m = mapper(Edge, edge)
        m.add_property('start', sa.orm.composite(Point, 'x1', 'y1'))
        m.add_property('end', sa.orm.composite(Point, 'x2', 'y2'))

        self._test_roundtrip()