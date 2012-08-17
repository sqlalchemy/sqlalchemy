from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import expression
from sqlalchemy import event, Table

# Python datatypes

class GisElement(object):
    """Represents a geometry value."""

    @property
    def wkt(self):
        return func.ST_AsText(literal(self, Geometry))

    @property
    def wkb(self):
        return func.ST_AsBinary(literal(self, Geometry))

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__,
                                    id(self), self.desc)

class PersistentGisElement(GisElement):
    """Represents a Geometry value as loaded from the database."""

    def __init__(self, desc):
        self.desc = desc

class TextualGisElement(GisElement, expression.Function):
    """Represents a Geometry value as expressed within application code;
    i.e. in wkt format.

    Extends expression.Function so that the value is interpreted as
    GeomFromText(value) in a SQL expression context.

    """

    def __init__(self, desc, srid=-1):
        assert isinstance(desc, basestring)
        self.desc = desc
        expression.Function.__init__(self, "ST_GeomFromText", desc, srid)


# SQL datatypes.

class Geometry(UserDefinedType):
    """Base PostGIS Geometry column type.

    Converts bind/result values to/from a PersistentGisElement.

    """

    name = "GEOMETRY"

    def __init__(self, dimension=None, srid=-1):
        self.dimension = dimension
        self.srid = srid

    class comparator_factory(UserDefinedType.Comparator):
        """Define custom operations for geometry types."""

        # override the __eq__() operator
        def __eq__(self, other):
            return self.op('~=')(_to_postgis(other))

        # add a custom operator
        def intersects(self, other):
            return self.op('&&')(_to_postgis(other))

        # any number of GIS operators can be overridden/added here
        # using the techniques above.

    def get_col_spec(self):
        return self.name

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

# other datatypes can be added as needed.

class Point(Geometry):
    name = 'POINT'

class Curve(Geometry):
    name = 'CURVE'

class LineString(Curve):
    name = 'LINESTRING'

# ... etc.


# DDL integration
# Postgis historically has required AddGeometryColumn/DropGeometryColumn
# and other management methods in order to create Postgis columns.  Newer
# versions don't appear to require these special steps anymore.  However,
# here we illustrate how to set up these features in any case.

def setup_ddl_events():
    @event.listens_for(Table, "before_create")
    def before_create(target, connection, **kw):
        dispatch("before-create", target, connection)

    @event.listens_for(Table, "after_create")
    def after_create(target, connection, **kw):
        dispatch("after-create", target, connection)

    @event.listens_for(Table, "before_drop")
    def before_drop(target, connection, **kw):
        dispatch("before-drop", target, connection)

    @event.listens_for(Table, "after_drop")
    def after_drop(target, connection, **kw):
        dispatch("after-drop", target, connection)

    def dispatch(event, table, bind):
        if event in ('before-create', 'before-drop'):
            regular_cols = [c for c in table.c if not
                                    isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            table.info["_saved_columns"] = table.c

            # temporarily patch a set of columns not including the
            # Geometry columns
            table.columns = expression.ColumnCollection(*regular_cols)

            if event == 'before-drop':
                for c in gis_cols:
                    bind.execute(
                            select([
                                func.DropGeometryColumn(
                                    'public', table.name, c.name)],
                                    autocommit=True)
                            )

        elif event == 'after-create':
            table.columns = table.info.pop('_saved_columns')
            for c in table.c:
                if isinstance(c.type, Geometry):
                    bind.execute(
                            select([
                                    func.AddGeometryColumn(
                                        table.name, c.name,
                                        c.type.srid,
                                        c.type.name,
                                        c.type.dimension)],
                                autocommit=True)
                        )
        elif event == 'after-drop':
            table.columns = table.info.pop('_saved_columns')
setup_ddl_events()

# ORM integration

def _to_postgis(value):
    """Interpret a value as a GIS-compatible construct.


    TODO.  I'd like to make this unnecessary also,
    and see if the Geometry type can do the coersion.
    This would require [ticket:1534].

    """

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

# without importing "orm", the "attribute_instrument"
# event isn't even set up.
from sqlalchemy import orm

@event.listens_for(type, "attribute_instrument")
def attribute_instrument(cls, key, inst):
    type_ = getattr(inst, "type", None)
    if isinstance(type_, Geometry):
        @event.listens_for(inst, "set", retval=True)
        def set_value(state, value, oldvalue, initiator):
            return _to_postgis(value)


# illustrate usage
if __name__ == '__main__':
    from sqlalchemy import (create_engine, MetaData, Column, Integer, String,
        func, literal, select)
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    engine = create_engine('postgresql://scott:tiger@localhost/test', echo=True)
    metadata = MetaData(engine)
    Base = declarative_base(metadata=metadata)

    class Road(Base):
        __tablename__ = 'roads'

        road_id = Column(Integer, primary_key=True)
        road_name = Column(String)
        road_geom = Column(Geometry(2))


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
