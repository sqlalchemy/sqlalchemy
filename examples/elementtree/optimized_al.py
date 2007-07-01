"""This script duplicates adjacency_list.py, but optimizes the loading
of XML nodes to be based on a "flattened" datamodel. Any number of XML documents, 
each of arbitrary complexity, can be loaded in their entirety via a single query 
which joins on only three tables.

"""

################################# PART I - Imports/Coniguration ###########################################
from sqlalchemy import *
from sqlalchemy.orm import *

import sys, os, StringIO, re

import logging
logging.basicConfig()

# uncomment to show SQL statements
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# uncomment to show SQL statements and result sets
#logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


from elementtree import ElementTree
from elementtree.ElementTree import Element, SubElement

meta = MetaData()
meta.engine = 'sqlite://'

################################# PART II - Table Metadata ###########################################
    
# stores a top level record of an XML document.  
documents = Table('documents', meta,
    Column('document_id', Integer, primary_key=True),
    Column('filename', String(30), unique=True),
)

# stores XML nodes in an adjacency list model.  This corresponds to 
# Element and SubElement objects.
elements = Table('elements', meta,
    Column('element_id', Integer, primary_key=True),
    Column('parent_id', Integer, ForeignKey('elements.element_id')),
    Column('document_id', Integer, ForeignKey('documents.document_id')),
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

meta.create_all()

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

#################################### PART IV - Persistence Mapping ###################################

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

# HierarchicalLoader class.  overrides mapper append() to produce a hierarchical
# structure as rows are received, allowing us to query the full list of 
# adjacency-list rows in one query
class HierarchicalLoader(MapperExtension):
    def append_result(self, mapper, selectcontext, row, instance, result, **flags):
        if instance.parent_id is None:
            result.append(instance)
        else:
            if flags['isnew'] or selectcontext.populate_existing:
                parentnode = selectcontext.identity_map[mapper.identity_key(instance.parent_id)]
                parentnode.children.append(instance)
        return False

# setup mappers.  Document will eagerly load a list of _Node objects.
mapper(Document, documents, properties={
    '_root':relation(_Node, lazy=False, cascade="all")
})

# the _Node objects change the way they load so that a list of _Nodes will organize
# themselves hierarchically using the HierarchicalLoader.  this depends on the ordering of
# nodes being hierarchical as well; relation() always applies at least ROWID/primary key
# ordering to rows which will suffice.
mapper(_Node, elements, properties={
    'children':relation(_Node, lazy=None, cascade="all"),  # doesnt load; loading is handled by the relation to the Document
    'attributes':relation(_Attribute, lazy=False, cascade="all, delete-orphan"), # eagerly load attributes
    'document':relation(Document, lazy=None) # allow backwards attachment of _Node to Document.
}, extension=HierarchicalLoader())

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

        document._element = ElementTree.ElementTree(traverse(document._root[0]))
        return document._element
    
    def __set__(self, document, element):
        def traverse(node):
            n = _Node()
            n.tag = node.tag
            n.text = node.text
            n.tail = node.tail
            n.document = document
            n.children = [traverse(n2) for n2 in node]
            n.attributes = [_Attribute(k, v) for k, v in node.attrib.iteritems()]
            return n

        document._root.append(traverse(element.getroot()))
        document._element = element
    
    def __delete__(self, document):
        del document._element
        document._root = []

# override Document's "element" attribute with the marshaller.
Document.element = ElementTreeMarshal()

########################################### PART V - Basic Persistence Example ############################

line = "\n--------------------------------------------------------"

# save to DB
session = create_session()

# get ElementTree documents
for file in ('test.xml', 'test2.xml', 'test3.xml'):
    filename = os.path.join(os.path.dirname(sys.argv[0]), file)
    doc = ElementTree.parse(filename)
    session.save(Document(file, doc))

print "\nSaving three documents...", line
session.flush()
print "Done."

# clear session (to illustrate a full load), restore
session.clear()

print "\nFull text of document 'text.xml':", line
document = session.query(Document).filter_by(filename="test.xml").first()

print document

############################################ PART VI - Searching for Paths #######################################

# manually search for a document which contains "/somefile/header/field1:hi"
print "\nManual search for /somefile/header/field1=='hi':", line
n1 = elements.alias('n1')
n2 = elements.alias('n2')
n3 = elements.alias('n3')
j = documents.join(n1).join(n2, n1.c.element_id==n2.c.parent_id).join(n3, n2.c.element_id==n3.c.parent_id)
d = session.query(Document).select_from(j).filter(n1.c.tag=='somefile').filter(n2.c.tag=='header').filter(and_(n3.c.tag=='field1', n3.c.text=='hi')).one()
print d

# generalize the above approach into an extremely impoverished xpath function:
def find_document(path, compareto):
    j = documents
    prev_elements = None
    query = session.query(Document)
    for i, match in enumerate(re.finditer(r'/([\w_]+)(?:\[@([\w_]+)(?:=(.*))?\])?', path)):
        (token, attrname, attrvalue) = match.group(1, 2, 3)
        a = elements.alias("n%d" % i)
        query = query.filter(a.c.tag==token)
        if attrname:
            attr_alias = attributes.alias('a%d' % i)
            if attrvalue:
                query = query.filter(and_(a.c.element_id==attr_alias.c.element_id, attr_alias.c.name==attrname, attr_alias.c.value==attrvalue))
            else:
                query = query.filter(and_(a.c.element_id==attr_alias.c.element_id, attr_alias.c.name==attrname))
        if prev_elements is not None:
            j = j.join(a, prev_elements.c.element_id==a.c.parent_id)
        else:
            j = j.join(a)
        prev_elements = a
    return query.options(lazyload('_root')).select_from(j).filter(prev_elements.c.text==compareto).all()

for path, compareto in (
        ('/somefile/header/field1', 'hi'),
        ('/somefile/field1', 'hi'),
        ('/somefile/header/field2', 'there'),
        ('/somefile/header/field2[@attr=foo]', 'there')
    ):
    print "\nDocuments containing '%s=%s':" % (path, compareto), line
    print [d.filename for d in find_document(path, compareto)]

