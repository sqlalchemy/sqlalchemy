from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql import expression
from sqlalchemy import event

# Python datatypes

class GisElement(object):
    """Represents a geometry value."""

    @property
    def wkt(self):
        return func.AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.AsBinary(literal(self, Geometry))

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__, id(self), self.desc)

class PersistentGisElement(GisElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc

class TextualGisElement(GisElement, expression.Function):
    """Represents a Geometry value as expressed within application code; i.e. in wkt format.

    Extends expression.Function so that the value is interpreted as 
    GeomFromText(value) in a SQL expression context.

    """

    def __init__(self, desc, srid=-1):
        assert isinstance(desc, basestring)
        self.desc = desc
        expression.Function.__init__(self, "GeomFromText", desc, srid)


# SQL datatypes.

class Geometry(TypeEngine):
    """Base PostGIS Geometry column type.

    Converts bind/result values to/from a PersistentGisElement.

    """

    name = 'GEOMETRY'

    def __init__(self, dimension=None, srid=-1):
        self.dimension = dimension
        self.srid = srid

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.desc
            else:
                return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return PersistentGisElement(value)
            else:
                return value
        return process

# other datatypes can be added as needed, which 
# currently only affect DDL statements.

class Point(Geometry):
    name = 'POINT'

class Curve(Geometry):
    name = 'CURVE'

class LineString(Curve):
    name = 'LINESTRING'

# ... etc.


# DDL integration

class GISDDL(object):
    """A DDL extension which integrates SQLAlchemy table create/drop 
    methods with PostGis' AddGeometryColumn/DropGeometryColumn functions.

    Usage::

        sometable = Table('sometable', metadata, ...)

        GISDDL(sometable)

        sometable.create()

    """

    def __init__(self, table):
        event.listen(table, "before_create", self.before_create)
        event.listen(table, "after_create", self.after_create)
        event.listen(table, "before_drop", self.before_drop)
        event.listen(table, "after_drop", self.before_drop)
        self._stack = []

    def before_create(self, target, connection, **kw):
        self("before-create", target, connection)

    def after_create(self, target, connection, **kw):
        self("after-create", target, connection)

    def before_drop(self, target, connection, **kw):
        self("before-drop", target, connection)

    def after_drop(self, target, connection, **kw):
        self("after-drop", target, connection)

    def __call__(self, event, table, bind):
        if event in ('before-create', 'before-drop'):
            regular_cols = [c for c in table.c if not isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            self._stack.append(table.c)
            table.columns = expression.ColumnCollection(*regular_cols)

            if event == 'before-drop':
                for c in gis_cols:
                    bind.execute(select([func.DropGeometryColumn('public', table.name, c.name)], autocommit=True))

        elif event == 'after-create':
            table.columns = self._stack.pop()

            for c in table.c:
                if isinstance(c.type, Geometry):
                    bind.execute(select([func.AddGeometryColumn(table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
        elif event == 'after-drop':
            table.columns = self._stack.pop()

# ORM integration

def _to_postgis(value):
    """Interpret a value as a GIS-compatible construct."""

    if hasattr(value, '__clause_element__'):
        return value.__clause_element__()
    elif isinstance(value, (expression.ClauseElement, GisElement)):
        return value
    elif isinstance(value, basestring):
        return TextualGisElement(value)
    elif value is None:
        return None
    else:
        raise Exception("Invalid type")


class GisAttribute(AttributeExtension):
    """Intercepts 'set' events on a mapped instance attribute and 
    converts the incoming value to a GIS expression.

    """

    def set(self, state, value, oldvalue, initiator):
        return _to_postgis(value)

class GisComparator(ColumnProperty.ColumnComparator):
    """Intercepts standard Column operators on mapped class attributes
    and overrides their behavior.

    """

    # override the __eq__() operator
    def __eq__(self, other):
        return self.__clause_element__().op('~=')(_to_postgis(other))

    # add a custom operator
    def intersects(self, other):
        return self.__clause_element__().op('&&')(_to_postgis(other))

    # any number of GIS operators can be overridden/added here
    # using the techniques above.


def GISColumn(*args, **kw):
    """Define a declarative column property with GIS behavior.

    This just produces orm.column_property() with the appropriate
    extension and comparator_factory arguments.  The given arguments
    are passed through to Column.  The declarative module extracts
    the Column for inclusion in the mapped table.

    """
    return column_property(
                Column(*args, **kw), 
                extension=GisAttribute(), 
                comparator_factory=GisComparator
            )

# illustrate usage
if __name__ == '__main__':
    from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        func, literal, select)
    from sqlalchemy.orm import sessionmaker, column_property
    from sqlalchemy.ext.declarative import declarative_base

    engine = create_engine('postgresql://scott:tiger@localhost/test', echo=True)
    metadata = MetaData(engine)
    Base = declarative_base(metadata=metadata)

    class Road(Base):
        __tablename__ = 'roads'

        road_id = Column(Integer, primary_key=True)
        road_name = Column(String)
        road_geom = GISColumn(Geometry(2))

    # enable the DDL extension, which allows CREATE/DROP operations
    # to work correctly.  This is not needed if working with externally
    # defined tables.
    GISDDL(Road.__table__)

    metadata.drop_all()
    metadata.create_all()

    session = sessionmaker(bind=engine)()

    # Add objects.  We can use strings...
    session.add_all([
        Road(road_name='Jeff Rd', road_geom='LINESTRING(191232 243118,191108 243242)'),
        Road(road_name='Geordie Rd', road_geom='LINESTRING(189141 244158,189265 244817)'),
        Road(road_name='Paul St', road_geom='LINESTRING(192783 228138,192612 229814)'),
        Road(road_name='Graeme Ave', road_geom='LINESTRING(189412 252431,189631 259122)'),
        Road(road_name='Phil Tce', road_geom='LINESTRING(190131 224148,190871 228134)'),
    ])

    # or use an explicit TextualGisElement (similar to saying func.GeomFromText())
    r = Road(road_name='Dave Cres', road_geom=TextualGisElement('LINESTRING(198231 263418,198213 268322)', -1))
    session.add(r)

    # pre flush, the TextualGisElement represents the string we sent.
    assert str(r.road_geom) == 'LINESTRING(198231 263418,198213 268322)'
    assert session.scalar(r.road_geom.wkt) == 'LINESTRING(198231 263418,198213 268322)'

    session.commit()

    # after flush and/or commit, all the TextualGisElements become PersistentGisElements.
    assert str(r.road_geom) == "01020000000200000000000000B832084100000000E813104100000000283208410000000088601041"

    r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()

    # illustrate the overridden __eq__() operator.

    # strings come in as TextualGisElements
    r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(189412 252431,189631 259122)').one()

    # PersistentGisElements work directly
    r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()

    assert r1 is r2 is r3

    # illustrate the "intersects" operator
    print session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()

    # illustrate usage of the "wkt" accessor. this requires a DB
    # execution to call the AsText() function so we keep this explicit.
    assert session.scalar(r1.road_geom.wkt) == 'LINESTRING(189412 252431,189631 259122)'

    session.rollback()

    metadata.drop_all()
