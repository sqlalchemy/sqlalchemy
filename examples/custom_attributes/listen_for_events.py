"""
Illustrates how to use AttributeExtension to listen for change events 
across the board.

"""

from sqlalchemy.orm.interfaces import AttributeExtension, \
    InstrumentationManager

class InstallListeners(InstrumentationManager):
    def post_configure_attribute(self, class_, key, inst):
        """Add an event listener to an InstrumentedAttribute."""
        
        inst.impl.extensions.insert(0, AttributeListener(key))
        
class AttributeListener(AttributeExtension):
    """Generic event listener.  
    
    Propagates attribute change events to a 
    "receive_change_event()" method on the target
    instance.
    
    """
    def __init__(self, key):
        self.key = key
    
    def append(self, state, value, initiator):
        self._report(state, value, None, "appended")
        return value

    def remove(self, state, value, initiator):
        self._report(state, value, None, "removed")

    def set(self, state, value, oldvalue, initiator):
        self._report(state, value, oldvalue, "set")
        return value
    
    def _report(self, state, value, oldvalue, verb):
        state.obj().receive_change_event(verb, self.key, value, oldvalue)

if __name__ == '__main__':

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.declarative import declarative_base

    class Base(object):
        __sa_instrumentation_manager__ = InstallListeners
        
        def receive_change_event(self, verb, key, value, oldvalue):
            s = "Value '%s' %s on attribute '%s', " % (value, verb, key)
            if oldvalue:
                s += "which replaced the value '%s', " % oldvalue
            s += "on object %s" % self
            print s
            
    Base = declarative_base(cls=Base)

    class MyMappedClass(Base):
        __tablename__ = "mytable"
    
        id = Column(Integer, primary_key=True)
        data = Column(String(50))
        related_id = Column(Integer, ForeignKey("related.id"))
        related = relationship("Related", backref="mapped")

        def __str__(self):
            return "MyMappedClass(data=%r)" % self.data
            
    class Related(Base):
        __tablename__ = "related"

        id = Column(Integer, primary_key=True)
        data = Column(String(50))

        def __str__(self):
            return "Related(data=%r)" % self.data
    
    # classes are instrumented.  Demonstrate the events !
    
    m1 = MyMappedClass(data='m1', related=Related(data='r1'))
    m1.data = 'm1mod'
    m1.related.mapped.append(MyMappedClass(data='m2'))
    del m1.data
    
    
