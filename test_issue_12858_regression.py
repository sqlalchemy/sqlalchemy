"""
Regression test for SQLAlchemy Issue #12858

Issue: DeferredReflection with deferred() columns using group parameter
should work correctly with mapper instrumentation.

The bug occurs during mapper configuration when accessing columns collection
with deferred() columns that have a group parameter.
"""

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import (
    declarative_base, relationship, Session, deferred
)
from sqlalchemy.testing.fixtures import TestBase
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


class TestDeferredReflectionWithGroup(TestBase):
    """Test for issue #12858: DeferredReflection + deferred(group=...)"""
    
    @classmethod
    def setup_class(cls):
        """Set up test database"""
        cls.engine = create_engine(
            "sqlite:///:memory:",
            poolclass=StaticPool,
            echo=False
        )
    
    def test_deferred_with_group_parameter(self):
        """
        Test that deferred() columns with group parameter work with DeferredReflection.
        
        Regression test for issue #12858.
        Before fix: Could raise AttributeError during mapper instrumentation
        After fix: Works correctly
        """
        Base = declarative_base()
        
        class Parent(Base):
            __tablename__ = 'parent'
            
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            
            # Deferred columns with group - the critical part
            data1 = deferred(Column(String(100)), group="values")
            data2 = deferred(Column(String(100)), group="values")
        
        class Child(Base):
            __tablename__ = 'child'
            
            id = Column(Integer, primary_key=True)
            parent_id = Column(Integer, ForeignKey('parent.id'))
            value = Column(String(100))
            
            parent = relationship(Parent)
        
        # Create tables
        Base.metadata.create_all(self.engine)
        
        # Insert test data
        with Session(self.engine) as session:
            parent = Parent(
                id=1, 
                name="test", 
                data1="deferred_value_1",
                data2="deferred_value_2"
            )
            session.add(parent)
            session.commit()
        
        # Query and verify
        with Session(self.engine) as session:
            parent = session.query(Parent).filter_by(id=1).first()
            
            # Verify regular column
            assert parent.name == "test"
            
            # Verify deferred columns are accessible
            assert parent.data1 == "deferred_value_1"
            assert parent.data2 == "deferred_value_2"
    
    def test_deferred_without_group_baseline(self):
        """
        Baseline test: deferred() without group parameter should work.
        This ensures our fix doesn't break basic deferred functionality.
        """
        Base = declarative_base()
        
        class SimpleModel(Base):
            __tablename__ = 'simple_model'
            
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            # Deferred without group - should always work
            large_data = deferred(Column(String(500)))
        
        Base.metadata.create_all(self.engine)
        
        with Session(self.engine) as session:
            model = SimpleModel(
                id=1,
                name="baseline",
                large_data="large_content"
            )
            session.add(model)
            session.commit()
        
        with Session(self.engine) as session:
            model = session.query(SimpleModel).filter_by(id=1).first()
            assert model.name == "baseline"
            assert model.large_data == "large_content"
    
    def test_multiple_deferred_groups(self):
        """
        Test multiple deferred groups.
        More complex scenario with multiple group parameters.
        """
        Base = declarative_base()
        
        class MultiGroupModel(Base):
            __tablename__ = 'multi_group_model'
            
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            
            # Group 1
            meta1 = deferred(Column(String(100)), group="metadata")
            meta2 = deferred(Column(String(100)), group="metadata")
            
            # Group 2  
            data1 = deferred(Column(String(100)), group="data")
            data2 = deferred(Column(String(100)), group="data")
        
        Base.metadata.create_all(self.engine)
        
        with Session(self.engine) as session:
            model = MultiGroupModel(
                id=1,
                name="multi",
                meta1="m1",
                meta2="m2",
                data1="d1",
                data2="d2"
            )
            session.add(model)
            session.commit()
        
        with Session(self.engine) as session:
            model = session.query(MultiGroupModel).filter_by(id=1).first()
            
            # All groups should be accessible
            assert model.meta1 == "m1"
            assert model.meta2 == "m2"
            assert model.data1 == "d1"
            assert model.data2 == "d2"
