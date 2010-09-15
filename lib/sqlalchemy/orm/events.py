"""ORM event interfaces.

"""
from sqlalchemy import event

class ClassEvents(event.Events):
    @classmethod
    def accept_with(cls, target):
        from sqlalchemy.orm.instrumentation import ClassManager
        
        if isinstance(target, ClassManager):
            return [target]
        elif isinstance(target, type):
            manager = manager_of_class(target)
            if manager:
                return [manager]
        return []
        
    # TODO: change these to accept "target" - 
    # the target is the state or the instance, depending
    # on if the listener was registered with "raw=True" -
    # do the same thing for all the other events here (Mapper, Session, Attributes).  
    # Not sending raw=True means the listen()  method of the
    # Events subclass will wrap incoming listeners to marshall each
    # "target" argument into "instance".  The name "target" can be
    # used consistently to make it simple.
    #
    # this way end users don't have to deal with InstanceState and
    # the internals can have faster performance.
    
    def on_init(self, state, instance, args, kwargs):
        """"""
        
    def on_init_failure(self, state, instance, args, kwargs):
        """"""
    
    def on_load(self, instance):
        """"""
    
    def on_resurrect(self, state, instance):
        """"""

class MapperEvents(event.Events):
    """"""
    
class SessionEvents(event.Events):
    """"""
    
class AttributeEvents(event.Events):
    """"""