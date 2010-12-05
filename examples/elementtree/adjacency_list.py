"""illustrates an explicit way to persist an XML document expressed using ElementTree.

This example explicitly marshals/unmarshals the ElementTree document into
mapped entities which have their own tables.  Compare to pickle.py which
uses pickle to accomplish the same task.  Note that the usage of both
styles of persistence are identical, as is the structure of the main Document class.
"""

################################# PART I - Imports/Coniguration ####################################
from sqlalchemy import (MetaData, Table, Column, Integer, String, ForeignKey,
    Unicode, and_, create_engine)
from sqlalchemy.orm import mapper, relationship, Session, lazyload

import sys, os, StringIO, re

from xml.etree import ElementTree

e = create_engine('sqlite://')
meta = MetaData()

################################# PART II - Table Metadata #########################################

# stores a top level record of an XML document.
documents = Table('documents', meta,
    Column('document_id', Integer, primary_key=True),
    Column('filename', String(30), unique=True),
    Column('element_id', Integer, ForeignKey('elements.element_id'))
)

# stores XML nodes in an adjacency list model.  This corresponds to
# Element and SubElement objects.
elements = Table('elements', meta,
    Column('element_id', Integer, primary_key=True),
    Column('parent_id', Integer, ForeignKey('elements.element_id')),
    Column('tag', Unicode(30), nullable=False),
    Column('text', Unicode),
    Column('tail', Unicode)
    )

# stores attributes.  This corresponds to the dictionary of attributes
# stored by an Element or SubElement.
attributes = Table('attributes', meta,
    Column('element_id', Integer, ForeignKey('elements.element_id'), primary_key=True),
    Column('name', Unicode(100), nullable=False, primary_key=True),
    Column('value', Unicode(255)))

meta.create_all(e)

#################################### PART III - Model #############################################

# our document class.  contains a string name,
# and the ElementTree root element.
class Document(object):
    def __init__(self, name, element):
        self.filename = name
        self.element = element

    def __str__(self):
        buf = StringIO.StringIO()
        self.element.write(buf)
        return buf.getvalue()

#################################### PART IV - Persistence Mapping #################################

# Node class.  a non-public class which will represent
# the DB-persisted Element/SubElement object.  We cannot create mappers for
# ElementTree elements directly because they are at the very least not new-style
# classes, and also may be backed by native implementations.
# so here we construct an adapter.
class _Node(object):
    pass

# Attribute class.  also internal, this will represent the key/value attributes stored for
# a particular Node.
class _Attribute(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

# setup mappers.  Document will eagerly load a list of _Node objects.
mapper(Document, documents, properties={
    '_root':relationship(_Node, lazy='joined', cascade="all")
})

mapper(_Node, elements, properties={
    'children':relationship(_Node, cascade="all"),
    # eagerly load attributes
    'attributes':relationship(_Attribute, lazy='joined', cascade="all, delete-orphan"),
})

mapper(_Attribute, attributes)

# define marshalling functions that convert from _Node/_Attribute to/from ElementTree objects.
# this will set the ElementTree element as "document._element", and append the root _Node
# object to the "_root" mapped collection.
class ElementTreeMarshal(object):
    def __get__(self, document, owner):
        if document is None:
            return self

        if hasattr(document, '_element'):
            return document._element

        def traverse(node, parent=None):
            if parent is not None:
                elem = ElementTree.SubElement(parent, node.tag)
            else:
                elem = ElementTree.Element(node.tag)
            elem.text = node.text
            elem.tail = node.tail
            for attr in node.attributes:
                elem.attrib[attr.name] = attr.value
            for child in node.children:
                traverse(child, parent=elem)
            return elem

        document._element = ElementTree.ElementTree(traverse(document._root))
        return document._element

    def __set__(self, document, element):
        def traverse(node):
            n = _Node()
            n.tag = unicode(node.tag)
            n.text = unicode(node.text)
            n.tail = unicode(node.tail)
            n.children = [traverse(n2) for n2 in node]
            n.attributes = [_Attribute(unicode(k), unicode(v)) for k, v in node.attrib.iteritems()]
            return n

        document._root = traverse(element.getroot())
        document._element = element

    def __delete__(self, document):
        del document._element
        document._root = []

# override Document's "element" attribute with the marshaller.
Document.element = ElementTreeMarshal()

########################################### PART V - Basic Persistence Example #####################

line = "\n--------------------------------------------------------"

# save to DB
session = Session(e)

# get ElementTree documents
for file in ('test.xml', 'test2.xml', 'test3.xml'):
    filename = os.path.join(os.path.dirname(__file__), file)
    doc = ElementTree.parse(filename)
    session.add(Document(file, doc))

print "\nSaving three documents...", line
session.commit()
print "Done."

print "\nFull text of document 'text.xml':", line
document = session.query(Document).filter_by(filename="test.xml").first()

print document

############################################ PART VI - Searching for Paths #########################

# manually search for a document which contains "/somefile/header/field1:hi"
d = session.query(Document).join('_root', aliased=True).filter(_Node.tag==u'somefile').\
    join('children', aliased=True, from_joinpoint=True).filter(_Node.tag==u'header').\
    join('children', aliased=True, from_joinpoint=True).filter(
            and_(_Node.tag==u'field1', _Node.text==u'hi')).one()
print d

# generalize the above approach into an extremely impoverished xpath function:
def find_document(path, compareto):
    j = documents
    prev_elements = None
    query = session.query(Document)
    attribute = '_root'
    for i, match in enumerate(re.finditer(r'/([\w_]+)(?:\[@([\w_]+)(?:=(.*))?\])?', path)):
        (token, attrname, attrvalue) = match.group(1, 2, 3)
        query = query.join(attribute, aliased=True, from_joinpoint=True).filter(_Node.tag==token)
        attribute = 'children'
        if attrname:
            if attrvalue:
                query = query.join('attributes', aliased=True, from_joinpoint=True).filter(
                        and_(_Attribute.name==attrname, _Attribute.value==attrvalue))
            else:
                query = query.join('attributes', aliased=True, from_joinpoint=True).filter(
                        _Attribute.name==attrname)
    return query.options(lazyload('_root')).filter(_Node.text==compareto).all()

for path, compareto in (
        (u'/somefile/header/field1', u'hi'),
        (u'/somefile/field1', u'hi'),
        (u'/somefile/header/field2', u'there'),
        (u'/somefile/header/field2[@attr=foo]', u'there')
    ):
    print "\nDocuments containing '%s=%s':" % (path, compareto), line
    print [d.filename for d in find_document(path, compareto)]

