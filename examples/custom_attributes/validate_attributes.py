"""
Illustrates how to use AttributeExtension to create attribute validators.

"""

from sqlalchemy.orm.interfaces import AttributeExtension, InstrumentationManager

class InstallValidators(InstrumentationManager):
    """Searches a class for methods with a '_validates' attribute and assembles Validators."""
    
    def __init__(self, cls):
        self.validators = {}
        for k in dir(cls):
            item = getattr(cls, k)
            if hasattr(item, '_validates'):
                self.validators[item._validates] = item
                
    def instrument_attribute(self, class_, key, inst):
        """Add an event listener to an InstrumentedAttribute."""
        
        if key in self.validators:
            inst.impl.extensions.insert(0, Validator(key, self.validators[key]))
        return super(InstallValidators, self).instrument_attribute(class_, key, inst)
        
class Validator(AttributeExtension):
    """Validates an attribute, given the key and a validation function."""
    
    def __init__(self, key, validator):
        self.key = key
        self.validator = validator
    
    def append(self, state, value, initiator):
        return self.validator(state.obj(), value)

    def set(self, state, value, oldvalue, initiator):
        return self.validator(state.obj(), value)

def validates(key):
    """Mark a method as validating a named attribute."""
    
    def wrap(fn):
        fn._validates = key
        return fn
    return wrap

if __name__ == '__main__':

    from sqlalchemy import *
    from sqlalchemy.orm import *
    from sqlalchemy.ext.declarative import declarative_base
    import datetime
    
    Base = declarative_base(engine=create_engine('sqlite://', echo=True))
    Base.__sa_instrumentation_manager__ = InstallValidators

    class MyMappedClass(Base):
        __tablename__ = "mytable"
    
        id = Column(Integer, primary_key=True)
        date = Column(Date)
        related_id = Column(Integer, ForeignKey("related.id"))
        related = relation("Related", backref="mapped")

        @validates('date')
        def check_date(self, value):
            if isinstance(value, str):
                m, d, y = [int(x) for x in value.split('/')]
                return datetime.date(y, m, d)
            else:
                assert isinstance(value, datetime.date)
                return value
        
        @validates('related')
        def check_related(self, value):
            assert value.data == 'r1'
            return value
            
        def __str__(self):
            return "MyMappedClass(date=%r)" % self.date
            
    class Related(Base):
        __tablename__ = "related"

        id = Column(Integer, primary_key=True)
        data = Column(String(50))

        def __str__(self):
            return "Related(data=%r)" % self.data
    
    Base.metadata.create_all()
    session = sessionmaker()()
    
    r1 = Related(data='r1')
    r2 = Related(data='r2')
    m1 = MyMappedClass(date='5/2/2005', related=r1)
    m2 = MyMappedClass(date=datetime.date(2008, 10, 15))
    r1.mapped.append(m2)

    try:
        m1.date = "this is not a date"
    except:
        pass
    assert m1.date == datetime.date(2005, 5, 2)
    
    try:
        m2.related = r2
    except:
        pass
    assert m2.related is r1
    
    session.add(m1)
    session.commit()
    assert session.query(MyMappedClass.date).order_by(MyMappedClass.date).all() == [
        (datetime.date(2005, 5, 2),),
        (datetime.date(2008, 10, 15),)
    ]
    