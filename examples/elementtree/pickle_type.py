"""
illustrates a quick and dirty way to persist an XML document expressed using
ElementTree and pickle.

This is a trivial example using PickleType to marshal/unmarshal the ElementTree
document into a binary column.  Compare to explicit.py which stores the
individual components of the ElementTree structure in distinct rows using two
additional mapped entities.  Note that the usage of both styles of persistence
are identical, as is the structure of the main Document class.

"""

import os
from xml.etree import ElementTree

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import PickleType
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import registry
from sqlalchemy.orm import Session


e = create_engine("sqlite://")
mapper_registry = registry()


# setup a comparator for the PickleType since it's a mutable
# element.


def are_elements_equal(x, y):
    return x == y


# stores a top level record of an XML document.
# the "element" column will store the ElementTree document as a BLOB.
documents = Table(
    "documents",
    mapper_registry.metadata,
    Column("document_id", Integer, primary_key=True),
    Column("filename", String(30), unique=True),
    Column("element", PickleType(comparator=are_elements_equal)),
)

mapper_registry.metadata.create_all(e)

# our document class.  contains a string name,
# and the ElementTree root element.


class Document(object):
    def __init__(self, name, element):
        self.filename = name
        self.element = element


# setup mapper.
mapper_registry.map_imperatively(Document, documents)

# time to test !

# get ElementTree document
filename = os.path.join(os.path.dirname(__file__), "test.xml")
doc = ElementTree.parse(filename)

# save to DB
session = Session(e)
session.add(Document("test.xml", doc))
session.commit()

# restore
document = session.query(Document).filter_by(filename="test.xml").first()

# print
ElementTree.dump(document.element)
