"""Uses the same strategy as
  ``adjacency_list.py``, but associates each   DOM row with its owning
  document row, so that a full document of DOM nodes can be loaded
  using O(1) queries - the construction of the "hierarchy" is performed
  after the load in a non-recursive fashion and is more
  efficient.

"""

# PART I - Imports/Configuration

from __future__ import print_function

import os
import re
from xml.etree import ElementTree

from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Unicode
from sqlalchemy.orm import aliased
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


e = create_engine("sqlite://")
meta = MetaData()

# PART II - Table Metadata

# stores a top level record of an XML document.
documents = Table(
    "documents",
    meta,
    Column("document_id", Integer, primary_key=True),
    Column("filename", String(30), unique=True),
)

# stores XML nodes in an adjacency list model.  This corresponds to
# Element and SubElement objects.
elements = Table(
    "elements",
    meta,
    Column("element_id", Integer, primary_key=True),
    Column("parent_id", Integer, ForeignKey("elements.element_id")),
    Column("document_id", Integer, ForeignKey("documents.document_id")),
    Column("tag", Unicode(30), nullable=False),
    Column("text", Unicode),
    Column("tail", Unicode),
)

# stores attributes.  This corresponds to the dictionary of attributes
# stored by an Element or SubElement.
attributes = Table(
    "attributes",
    meta,
    Column(
        "element_id",
        Integer,
        ForeignKey("elements.element_id"),
        primary_key=True,
    ),
    Column("name", Unicode(100), nullable=False, primary_key=True),
    Column("value", Unicode(255)),
)

meta.create_all(e)

# PART III - Model

# our document class.  contains a string name,
# and the ElementTree root element.


class Document(object):
    def __init__(self, name, element):
        self.filename = name
        self.element = element


# PART IV - Persistence Mapping

# Node class.  a non-public class which will represent the DB-persisted
# Element/SubElement object.  We cannot create mappers for ElementTree elements
# directly because they are at the very least not new-style classes, and also
# may be backed by native implementations. so here we construct an adapter.


class _Node(object):
    pass


# Attribute class.  also internal, this will represent the key/value attributes
# stored for a particular Node.


class _Attribute(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value


# setup mappers.  Document will eagerly load a list of _Node objects.
# they will be ordered in primary key/insert order, so that we can reconstruct
# an ElementTree structure from the list.
mapper(
    Document,
    documents,
    properties={
        "_nodes": relationship(
            _Node, lazy="joined", cascade="all, delete-orphan"
        )
    },
)

# the _Node objects change the way they load so that a list of _Nodes will
# organize themselves hierarchically using the ElementTreeMarshal.  this
# depends on the ordering of nodes being hierarchical as well; relationship()
# always applies at least ROWID/primary key ordering to rows which will
# suffice.
mapper(
    _Node,
    elements,
    properties={
        "children": relationship(
            _Node, lazy=None
        ),  # doesnt load; used only for the save relationship
        "attributes": relationship(
            _Attribute, lazy="joined", cascade="all, delete-orphan"
        ),  # eagerly load attributes
    },
)

mapper(_Attribute, attributes)

# define marshalling functions that convert from _Node/_Attribute to/from
# ElementTree objects. this will set the ElementTree element as
# "document._element", and append the root _Node object to the "_nodes" mapped
# collection.


class ElementTreeMarshal(object):
    def __get__(self, document, owner):
        if document is None:
            return self

        if hasattr(document, "_element"):
            return document._element

        nodes = {}
        root = None
        for node in document._nodes:
            if node.parent_id is not None:
                parent = nodes[node.parent_id]
                elem = ElementTree.SubElement(parent, node.tag)
                nodes[node.element_id] = elem
            else:
                parent = None
                elem = root = ElementTree.Element(node.tag)
                nodes[node.element_id] = root
            for attr in node.attributes:
                elem.attrib[attr.name] = attr.value
            elem.text = node.text
            elem.tail = node.tail

        document._element = ElementTree.ElementTree(root)
        return document._element

    def __set__(self, document, element):
        def traverse(node):
            n = _Node()
            n.tag = str(node.tag)
            n.text = str(node.text)
            n.tail = str(node.tail)
            document._nodes.append(n)
            n.children = [traverse(n2) for n2 in node]
            n.attributes = [
                _Attribute(str(k), str(v)) for k, v in node.attrib.items()
            ]
            return n

        traverse(element.getroot())
        document._element = element

    def __delete__(self, document):
        del document._element
        document._nodes = []


# override Document's "element" attribute with the marshaller.
Document.element = ElementTreeMarshal()

# PART V - Basic Persistence Example

line = "\n--------------------------------------------------------"

# save to DB
session = Session(e)

# get ElementTree documents
for file in ("test.xml", "test2.xml", "test3.xml"):
    filename = os.path.join(os.path.dirname(__file__), file)
    doc = ElementTree.parse(filename)
    session.add(Document(file, doc))

print("\nSaving three documents...", line)
session.commit()
print("Done.")

print("\nFull text of document 'text.xml':", line)
document = session.query(Document).filter_by(filename="test.xml").first()

ElementTree.dump(document.element)

# PART VI - Searching for Paths

# manually search for a document which contains "/somefile/header/field1:hi"
print("\nManual search for /somefile/header/field1=='hi':", line)

root = aliased(_Node)
child_node = aliased(_Node)
grandchild_node = aliased(_Node)

d = (
    session.query(Document)
    .join(Document._nodes.of_type(root))
    .filter(and_(root.parent_id.is_(None), root.tag == "somefile"))
    .join(root.children.of_type(child_node))
    .filter(child_node.tag == "header")
    .join(child_node.children.of_type(grandchild_node))
    .filter(
        and_(grandchild_node.tag == "field1", grandchild_node.text == "hi")
    )
    .one()
)
ElementTree.dump(d.element)

# generalize the above approach into an extremely impoverished xpath function:


def find_document(path, compareto):
    query = session.query(Document)

    for i, match in enumerate(
        re.finditer(r"/([\w_]+)(?:\[@([\w_]+)(?:=(.*))?\])?", path)
    ):
        (token, attrname, attrvalue) = match.group(1, 2, 3)

        if not i:
            parent = Document
            target_node = aliased(_Node)

            query = query.join(parent._nodes.of_type(target_node)).filter(
                target_node.parent_id.is_(None)
            )
        else:
            parent = target_node
            target_node = aliased(_Node)

            query = query.join(parent.children.of_type(target_node))

        query = query.filter(target_node.tag == token)
        if attrname:
            attribute_entity = aliased(_Attribute)
            query = query.join(
                target_node.attributes.of_type(attribute_entity)
            )
            if attrvalue:
                query = query.filter(
                    and_(
                        attribute_entity.name == attrname,
                        attribute_entity.value == attrvalue,
                    )
                )
            else:
                query = query.filter(attribute_entity.name == attrname)
    return (
        query.options(lazyload(Document._nodes))
        .filter(target_node.text == compareto)
        .all()
    )


for path, compareto in (
    ("/somefile/header/field1", "hi"),
    ("/somefile/field1", "hi"),
    ("/somefile/header/field2", "there"),
    ("/somefile/header/field2[@attr=foo]", "there"),
):
    print("\nDocuments containing '%s=%s':" % (path, compareto), line)
    print([d.filename for d in find_document(path, compareto)])
