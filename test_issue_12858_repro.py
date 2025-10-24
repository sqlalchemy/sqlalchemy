"""
SQLAlchemy Issue #12858 - Reproduction Test

Issue: DeferredReflection + deferred() columns with group parameter
causes AttributeError during mapper instrumentation

Root Cause: 
During mapper's configure_instrumentation phase, the mapper tries to 
access columns collection (via _configure_property), but with DeferredReflection
and group parameter in deferred(), the columns might not be fully available yet.

The issue occurs in the sequence:
1. mapper._configure_class_instrumentation() is called
2. Which triggers _configure_property() for each property
3. For deferred() columns with group parameter, it needs access to columns
4. But columns collection is not fully initialized during deferred reflection

Snippet from issue #12858:
https://github.com/sqlalchemy/sqlalchemy/issues/12858
"""

from sqlalchemy import Column, Integer, String, create_engine, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, Session, deferred

Base = declarative_base()


class DeferredReflection(object):
    """Mixin for deferred table reflection."""
    
    @classmethod
    def __declare_last__(cls):
        """Called after all mapper configurations."""
        pass


class Parent(Base, DeferredReflection):
    __tablename__ = 'parent'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    
    # Deferred columns with group parameter - THIS CAUSES THE BUG
    data1 = deferred(Column(String(100)), group="values")
    data2 = deferred(Column(String(100)), group="values")


class Child(Base, DeferredReflection):
    __tablename__ = 'child'
    
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('parent.id'))
    value = Column(String(100))
    
    parent = relationship(Parent)


def test_deferred_reflection_with_group():
    """
    Test that reproduces the issue.
    
    Before fix:
        AttributeError: ... (during mapper configuration)
    
    After fix:
        Works correctly - deferred columns with group are available
    """
    print("Testing: DeferredReflection + deferred(group='values')")
    
    # Create in-memory SQLite database
    engine = create_engine('sqlite:///:memory:', echo=False)
    
    # Create tables
    Base.metadata.create_all(engine)
    
    # This should NOT raise AttributeError
    try:
        session = Session(engine)
        
        # Try to create and query
        parent = Parent(id=1, name="test", data1="value1", data2="value2")
        session.add(parent)
        session.commit()
        
        # Query with deferred columns
        loaded = session.query(Parent).filter_by(id=1).first()
        print(f"✓ Parent loaded: {loaded.name}")
        print(f"✓ Deferred data1: {loaded.data1}")
        print(f"✓ Deferred data2: {loaded.data2}")
        
        session.close()
        return True
        
    except AttributeError as e:
        print(f"✗ AttributeError (BUG): {e}")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"✗ Other Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_deferred_without_group():
    """
    Test baseline - deferred() without group parameter.
    This should always work.
    """
    print("\nTesting: deferred() without group parameter (baseline)")
    
    class ParentNoGroup(Base):
        __tablename__ = 'parent_no_group'
        
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        data1 = deferred(Column(String(100)))  # No group
    
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    
    try:
        session = Session(engine)
        parent = ParentNoGroup(id=1, name="test", data1="value1")
        session.add(parent)
        session.commit()
        
        session.query(ParentNoGroup).filter_by(id=1).first()
        print("✓ Baseline test passed")
        session.close()
        return True
    except Exception as e:
        print(f"✗ Baseline failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("SQLALCHEMY ISSUE #12858 - REPRODUCTION TEST")
    print("=" * 70)
    print()
    
    # Test baseline first
    baseline_ok = test_deferred_without_group()
    
    # Test the bug case
    bug_ok = test_deferred_reflection_with_group()
    
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Baseline (no group): {'✓ PASS' if baseline_ok else '✗ FAIL'}")
    print(f"Bug case (with group): {'✓ PASS (FIXED!)' if bug_ok else '✗ FAIL (BUG CONFIRMED)'}")
    print()
    
    exit(0 if bug_ok else 1)
