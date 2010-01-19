"""
Illustrates three strategies for persisting and querying XML documents as represented by 
ElementTree in a relational database.   The techniques do not apply any mappings to the ElementTree objects directly, so are compatible with the native cElementTree as well as lxml, and can be adapted to suit any kind of DOM representation system.   Querying along xpath-like strings is illustrated as well.

In order of complexity:

* ``pickle.py`` - Quick and dirty, serialize the whole DOM into a BLOB column.  While the example
  is very brief, it has very limited functionality.
* ``adjacency_list.py`` - Each DOM node is stored in an individual table row, with attributes
  represented in a separate table.  The nodes are associated in a hierarchy using an adjacency list
  structure.  A query function is introduced which can search for nodes along any path with a given
  structure of attributes, basically a (very narrow) subset of xpath.
* ``optimized_al.py`` - Uses the same strategy as ``adjacency_list.py``, but adds a
  :class:`~sqlalchemy.orm.interfaces.MapperExtension` which optimizes how the hierarchical structure  
  is loaded, such that the full set of DOM nodes are loaded within a single table result set, and
  are organized hierarchically as they are received during a load.

E.g.::
    
    # parse an XML file and persist in the database
    doc = ElementTree.parse("test.xml")
    session.add(Document(file, doc))
    session.commit()
    
    # locate documents with a certain path/attribute structure 
    for document in find_document('/somefile/header/field2[@attr=foo]'):
        # dump the XML
        print document
    
"""