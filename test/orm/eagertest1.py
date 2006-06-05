from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
import datetime

class EagerTest(AssertMixin):
    def setUpAll(self):
        global designType, design, part, inheritedPart
        designType = Table('design_types', testbase.metadata, 
        	Column('design_type_id', Integer, primary_key=True),
        	)

        design =Table('design', testbase.metadata, 
        	Column('design_id', Integer, primary_key=True),
        	Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))

        part = Table('parts', testbase.metadata, 
        	Column('part_id', Integer, primary_key=True),
        	Column('design_id', Integer, ForeignKey('design.design_id')),
        	Column('design_type_id', Integer, ForeignKey('design_types.design_type_id')))

        inheritedPart = Table('inherited_part', testbase.metadata,
        	Column('ip_id', Integer, primary_key=True),
        	Column('part_id', Integer, ForeignKey('parts.part_id')),
        	Column('design_id', Integer, ForeignKey('design.design_id')),
        	)

        testbase.metadata.create_all()
    def tearDownAll(self):
        testbase.metadata.drop_all()
        testbase.metadata.clear()
    def testone(self):
        class Part(object):pass
        class Design(object):pass
        class DesignType(object):pass
        class InheritedPart(object):pass
	
        mapper(Part, part)

        mapper(InheritedPart, inheritedPart, properties=dict(
        	part=relation(Part, lazy=False)
        ))

        mapper(Design, design, properties=dict(
        	parts=relation(Part, private=True, backref="design"),
        	inheritedParts=relation(InheritedPart, private=True, backref="design"),
        ))

        mapper(DesignType, designType, properties=dict(
        #	designs=relation(Design, private=True, backref="type"),
        ))

        class_mapper(Design).add_property("type", relation(DesignType, lazy=False, backref="designs"))
        class_mapper(Part).add_property("design", relation(Design, lazy=False, backref="parts"))
        #Part.mapper.add_property("designType", relation(DesignType))

        d = Design()
        sess = create_session()
        sess.save(d)
        sess.flush()
        sess.clear()
        x = sess.query(Design).get(1)
        x.inheritedParts

if __name__ == "__main__":    
    testbase.main()


