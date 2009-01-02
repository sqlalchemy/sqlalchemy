"""A naive example illustrating techniques to help 
embed PostGIS functionality.

The techniques here could be used a capable developer 
as the basis for a comprehensive PostGIS SQLAlchemy extension.
Please note this is an entirely incomplete proof of concept
only, and PostGIS support is *not* a supported feature 
of SQLAlchemy.

Includes:

 * a DDL extension which allows CREATE/DROP to work in 
   conjunction with AddGeometryColumn/DropGeometryColumn
   
 * a Geometry type, as well as a few subtypes, which
   convert result row values to a GIS-aware object,
   and also integrates with the DDL extension.

 * a GIS-aware object which stores a raw geometry value
   and provides a factory for functions such as AsText().
   
 * an ORM comparator which can override standard column
   methods on mapped objects to produce GIS operators.
   
 * an attribute event listener that intercepts strings
   and converts to GeomFromText().
   
 * a standalone operator example.

The implementation is limited to only public, well known
and simple to use extension points, with the exception
of one temporary monkeypatch in the DDL extension.  
Future SQLAlchemy expansion points may allow more seamless
integration of some features.
 
"""

from sqlalchemy.orm.interfaces import AttributeExtension
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql import expression

class Geometry(TypeEngine):
    """Base PostGIS Geometry column type"""
    
    name = 'GEOMETRY'
    
    def __init__(self, dimension, srid=-1):
        self.dimension = dimension
        self.srid = srid

    def result_processor(self, dialect):
        def process(value):
            if value is not None:
                return gis_element(value)
            else:
                return value
        return process

class Point(Geometry):
    name = 'POINT'
    
class Curve(Geometry):
    name = 'CURVE'
    
class LineString(Curve):
    name = 'LINESTRING'

# ... add other types as needed


class GISDDL(object):
    def __init__(self, table):
        for event in ('before-create', 'after-create', 'before-drop', 'after-drop'):
            table.ddl_listeners[event].append(self)
        self._stack = []
        
    def __call__(self, event, table, bind):
        if event in ('before-create', 'before-drop'):
            regular_cols = [c for c in table.c if not isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            self._stack.append(table.c)
            table._columns = expression.ColumnCollection(*regular_cols)
            
            if event == 'before-drop':
                for c in gis_cols:
                    bind.execute(select([func.DropGeometryColumn('public', table.name, c.name)], autocommit=True))
                
        elif event == 'after-create':
            table._columns = self._stack.pop()
            
            for c in table.c:
                if isinstance(c.type, Geometry):
                    bind.execute(select([func.AddGeometryColumn(table.name, c.name, c.type.srid, c.type.name, c.type.dimension)], autocommit=True))
        elif event == 'after-drop':
            table._columns = self._stack.pop()

def _to_postgis(value):
    if hasattr(value, '__clause_element__'):
        return value.__clause_element__()
    elif isinstance(value, expression.ClauseElement):
        return value
    elif isinstance(value, basestring):
        return func.GeomFromText(value, -1)
    elif isinstance(value, gis_element):
        return value.desc
    elif value is None:
        return None
    else:
        raise Exception("Invalid type")
        

class GisAttribute(AttributeExtension):
    """Intercepts 'set' events on a mapped instance and 
    converts the incoming value to a GIS expression.
    
    """
    
    def set(self, state, value, oldvalue, initiator):
        return _to_postgis(value)
            
class GisComparator(ColumnProperty.ColumnComparator):
    """Intercepts standard Column operators on mapped class attributes
    and overrides their behavior.
    
    
    """
    
    def __eq__(self, other):
        return self.__clause_element__().op('~=')(_to_postgis(other))

    def intersects(self, other):
        return self.__clause_element__().op('&&')(_to_postgis(other))
    
class gis_element(object):
    """Represents a geometry value.
    
    This is just the raw string returned by PostGIS, 
    plus some helper functions.
    
    """
    
    def __init__(self, desc):
        self.desc = desc
    
    @property
    def wkt(self):
        return func.AsText(self.desc)

    @property
    def wkb(self):
        return func.AsBinary(self.desc)

        
def GISColumn(*args, **kw):
    """Define a declarative column property with GIS behavior."""
    
    return column_property(
                Column(*args, **kw), 
                extension=GisAttribute(), 
                comparator_factory=GisComparator
            )
    
if __name__ == '__main__':
    from sqlalchemy import *
    from sqlalchemy.orm import *
    from sqlalchemy.ext.declarative import declarative_base

    engine = create_engine('postgres://scott:tiger@localhost/gistest', echo=True)
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
    
    # Add objects using strings for the geometry objects; the attribute extension
    # converts them to GeomFromText
    session.add_all([
        Road(road_name='Jeff Rd', road_geom='LINESTRING(191232 243118,191108 243242)'),
        Road(road_name='Geordie Rd', road_geom='LINESTRING(189141 244158,189265 244817)'),
        Road(road_name='Paul St', road_geom='LINESTRING(192783 228138,192612 229814)'),
        Road(road_name='Graeme Ave', road_geom='LINESTRING(189412 252431,189631 259122)'),
        Road(road_name='Phil Tce', road_geom='LINESTRING(190131 224148,190871 228134)'),
    ])
    
    # GeomFromText can be called directly here as well.
    session.add(
        Road(road_name='Dave Cres', road_geom=func.GeomFromText('LINESTRING(198231 263418,198213 268322)', -1)),
    )
    
    session.commit()
    
    r1 = session.query(Road).filter(Road.road_name=='Graeme Ave').one()

    # illustrate the overridden __eq__() operator
    r2 = session.query(Road).filter(Road.road_geom == 'LINESTRING(189412 252431,189631 259122)').one()
    r3 = session.query(Road).filter(Road.road_geom == r1.road_geom).one()
    assert r1 is r2 is r3

    # illustrate the "intersects" operator
    print session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()

    # illustrate usage of the "wkt" accessor. this requires a DB
    # execution to call the AsText() function so we keep this explicit.
    assert session.scalar(r1.road_geom.wkt) == 'LINESTRING(189412 252431,189631 259122)'
    
    session.rollback()
    
    metadata.drop_all()
