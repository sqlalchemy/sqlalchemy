from sqlalchemy             import objectstore, create_engine, assign_mapper, relation, mapper
from sqlalchemy             import and_, or_
from sqlalchemy             import Table, Column
from sqlalchemy.ext.proxy   import ProxyEngine

import inspect


#
# the "proxy" to the database engine... this can be swapped out at runtime
#
engine = ProxyEngine()



#
# declarative column declaration - this is so that we can infer the colname
#
class column(object):
    def __init__(self, coltype, colname=None, foreign_key=None, primary_key=False):
        self.coltype     = coltype
        self.colname     = colname
        self.foreign_key = foreign_key
        self.primary_key = primary_key



#
# declarative relationship declaration
#
class relationship(object):
    def __init__(self, classname, colname=None, backref=None, private=False, lazy=True, uselist=True):
        self.classname = classname
        self.colname   = colname
        self.backref   = backref
        self.private   = private
        self.lazy      = lazy
        self.uselist   = uselist


class one_to_many(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False, lazy=True):
        relationship.__init__(self, classname, colname, backref, private, lazy, uselist=True)


class one_to_one(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False, lazy=True):
        relationship.__init__(self, classname, colname, backref, private, lazy, uselist=False)



# 
# SQLAlchemy metaclass and superclass that can be used to do SQLAlchemy 
# mapping in a declarative way, along with a function to process the 
# relationships between dependent objects as they come in, without blowing
# up if the classes aren't specified in a proper order
# 

__deferred_classes__  = []
def process_relationships(klass, was_deferred=False):
    defer = False
    for propname, reldesc in klass.relations.items():
        if not reldesc.classname in ActiveMapperMeta.classes:
            if not was_deferred: __deferred_classes__.append(klass)
            defer = True
    
    if not defer:
        relations = {}
        for propname, reldesc in klass.relations.items():
            relclass = ActiveMapperMeta.classes[reldesc.classname]
            relations[propname] = relation(relclass, 
                                           backref=reldesc.backref, 
                                           private=reldesc.private, 
                                           lazy=reldesc.lazy, 
                                           uselist=reldesc.uselist)
        assign_mapper(klass, klass.table, properties=relations)
        if was_deferred: __deferred_classes__.remove(klass)
    
    if not was_deferred:
        for deferred_class in __deferred_classes__:
            process_relationships(deferred_class, was_deferred=True)



class ActiveMapperMeta(type):
    classes = {}
    
    def __init__(cls, clsname, bases, dict):
        table_name = clsname.lower()
        columns    = []
        relations  = {}
        
        if 'mapping' in dict:
            members = inspect.getmembers(dict.get('mapping'))
            for name, value in members:
                if name == '__table__':
                    table_name = value
                    continue
                
                if name.startswith('__'): continue
                
                if isinstance(value, column):
                    if value.foreign_key:
                        col = Column(value.colname or name, 
                                     value.coltype,
                                     value.foreign_key, 
                                     primary_key=value.primary_key)
                    else:
                        col = Column(value.colname or name,
                                     value.coltype,
                                     primary_key=value.primary_key)
                    columns.append(col)
                    continue
                
                if isinstance(value, relationship):
                    relations[name] = value
            
            cls.table = Table(table_name, engine, *columns)
            assign_mapper(cls, cls.table)
            cls.relations = relations
            ActiveMapperMeta.classes[clsname] = cls
            
            process_relationships(cls)
        
        super(ActiveMapperMeta, cls).__init__(clsname, bases, dict)


class ActiveMapper(object):
    __metaclass__ = ActiveMapperMeta
    
    def set(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


#
# a utility function to create all tables for all ActiveMapper classes
#

def create_tables():
    for klass in ActiveMapperMeta.classes.values():
        klass.table.create()