"""
Illustrates three strategies for persisting and querying XML
documents as represented by ElementTree in a relational
database. The techniques do not apply any mappings to the
ElementTree objects directly, so are compatible with the
native cElementTree as well as lxml, and can be adapted to
suit any kind of DOM representation system. Querying along
xpath-like strings is illustrated as well.

E.g.::

    # parse an XML file and persist in the database
    doc = ElementTree.parse("test.xml")
    session.add(Document(file, doc))
    session.commit()

    # locate documents with a certain path/attribute structure
    for document in find_document('/somefile/header/field2[@attr=foo]'):
        # dump the XML
        print(document)

.. autosource::
    :files: pickle_type.py, adjacency_list.py, optimized_al.py

"""
