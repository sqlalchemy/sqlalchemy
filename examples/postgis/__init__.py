"""A naive example illustrating techniques to help
embed PostGIS functionality.

This example was originally developed in the hopes that it would be
extrapolated into a comprehensive PostGIS integration layer.  We are
pleased to announce that this has come to fruition as `GeoAlchemy
<http://www.geoalchemy.org/>`_.

The example illustrates:

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
and simple to use extension points.

E.g.::

    print session.query(Road).filter(Road.road_geom.intersects(r1.road_geom)).all()

.. autosource::

"""

