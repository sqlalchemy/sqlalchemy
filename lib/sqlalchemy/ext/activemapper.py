from sqlalchemy             import create_session, relation, mapper, join, DynamicMetaData, class_mapper, util
from sqlalchemy             import and_, or_
from sqlalchemy             import Table, Column, ForeignKey
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy import backref as create_backref

import inspect
import sys
import sets

#
# the "proxy" to the database engine... this can be swapped out at runtime
#
metadata = DynamicMetaData("activemapper")

#
# thread local SessionContext
#
class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

objectstore = Objectstore(create_session)


#
# declarative column declaration - this is so that we can infer the colname
#
class column(object):
    def __init__(self, coltype, colname=None, foreign_key=None,
                 primary_key=False, *args, **kwargs):
        if isinstance( foreign_key, basestring ):
            foreign_key= ForeignKey( foreign_key )
        self.coltype     = coltype
        self.colname     = colname
        self.foreign_key = foreign_key
        self.primary_key = primary_key
        self.kwargs      = kwargs
        self.args        = args

#
# declarative relationship declaration
#
class relationship(object):
    def __init__(self, classname, colname=None, backref=None, private=False,
                 lazy=True, uselist=True, secondary=None):
        self.classname = classname
        self.colname   = colname
        self.backref   = backref
        self.private   = private
        self.lazy      = lazy
        self.uselist   = uselist
        self.secondary = secondary

class one_to_many(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False, lazy=True):
        relationship.__init__(self, classname, colname, backref, private, lazy, uselist=True)


class one_to_one(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False, lazy=True):
        relationship.__init__(self, classname, colname, create_backref(backref, uselist=False), private, lazy, uselist=False)

class many_to_many(relationship):
    def __init__(self, classname, secondary, backref=None, lazy=True):
        relationship.__init__(self, classname, None, backref, False, lazy,
                              uselist=True, secondary=secondary)


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
            relations[propname] = relation(relclass.mapper,
                                           secondary=reldesc.secondary,
                                           backref=reldesc.backref, 
                                           private=reldesc.private, 
                                           lazy=reldesc.lazy, 
                                           uselist=reldesc.uselist)
        class_mapper(klass).add_properties(relations)
        #assign_mapper(objectstore, klass, klass.table, properties=relations,
        #              inherits=getattr(klass, "_base_mapper", None))
        if was_deferred: __deferred_classes__.remove(klass)
    
    if not was_deferred:
        for deferred_class in __deferred_classes__:
            process_relationships(deferred_class, was_deferred=True)



class ActiveMapperMeta(type):
    classes = {}
    metadatas = util.Set()
    def __init__(cls, clsname, bases, dict):
        table_name = clsname.lower()
        columns    = []
        relations  = {}
        _metadata    = getattr( sys.modules[cls.__module__], "__metadata__", metadata )
        
        if 'mapping' in dict:
            members = inspect.getmembers(dict.get('mapping'))
            for name, value in members:
                if name == '__table__':
                    table_name = value
                    continue
                
                if '__metadata__' == name:
                    _metadata= value
                    continue
                    
                if name.startswith('__'): continue
                
                if isinstance(value, column):
                    if value.foreign_key:
                        col = Column(value.colname or name, 
                                     value.coltype,
                                     value.foreign_key, 
                                     primary_key=value.primary_key,
                                     *value.args, **value.kwargs)
                    else:
                        col = Column(value.colname or name,
                                     value.coltype,
                                     primary_key=value.primary_key,
                                     *value.args, **value.kwargs)
                    columns.append(col)
                    continue
                
                if isinstance(value, relationship):
                    relations[name] = value
            assert _metadata is not None, "No MetaData specified"
            ActiveMapperMeta.metadatas.add(_metadata)
            cls.table = Table(table_name, _metadata, *columns)
            # check for inheritence
            if hasattr( bases[0], "mapping" ):
                cls._base_mapper= bases[0].mapper
                assign_mapper(objectstore, cls, cls.table, inherits=cls._base_mapper)
            else:
                assign_mapper(objectstore, cls, cls.table)
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
    for metadata in ActiveMapperMeta.metadatas:
        metadata.create_all()
def drop_tables():
    for metadata in ActiveMapperMeta.metadatas:
        metadata.drop_all()

