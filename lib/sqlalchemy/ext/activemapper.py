from sqlalchemy             import create_session, relation, mapper, \
                                   join, DynamicMetaData, class_mapper, \
                                   util, Integer
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
class Objectstore(object):

    def __init__(self, *args, **kwargs):
        self._context = SessionContext(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._context.current, name)

objectstore = Objectstore(create_session)


#
# declarative column declaration - this is so that we can infer the colname
#
class column(object):
    def __init__(self, coltype, colname=None, foreign_key=None,
                 primary_key=False, *args, **kwargs):
        if isinstance(foreign_key, basestring): 
            foreign_key = ForeignKey(foreign_key)
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
                 lazy=True, uselist=True, secondary=None, order_by=False):
        self.classname = classname
        self.colname   = colname
        self.backref   = backref
        self.private   = private
        self.lazy      = lazy
        self.uselist   = uselist
        self.secondary = secondary
        self.order_by  = order_by

class one_to_many(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False,
                 lazy=True, order_by=False):
        relationship.__init__(self, classname, colname, backref, private, 
                              lazy, uselist=True, order_by=order_by)

class one_to_one(relationship):
    def __init__(self, classname, colname=None, backref=None, private=False,
                 lazy=True, order_by=False):
        if backref is not None:
            backref = create_backref(backref, uselist=False)
        relationship.__init__(self, classname, colname, backref, private, 
                              lazy, uselist=False, order_by=order_by)

class many_to_many(relationship):
    def __init__(self, classname, secondary, backref=None, lazy=True,
                 order_by=False):
        relationship.__init__(self, classname, None, backref, False, lazy,
                              uselist=True, secondary=secondary,
                              order_by=order_by)


# 
# SQLAlchemy metaclass and superclass that can be used to do SQLAlchemy 
# mapping in a declarative way, along with a function to process the 
# relationships between dependent objects as they come in, without blowing
# up if the classes aren't specified in a proper order
# 

__deferred_classes__ = set()
__processed_classes__ = set()
def process_relationships(klass, was_deferred=False):
    # first, we loop through all of the relationships defined on the
    # class, and make sure that the related class already has been
    # completely processed and defer processing if it has not
    defer = False
    for propname, reldesc in klass.relations.items():
        found = False
        for other_klass in __processed_classes__:
            if reldesc.classname == other_klass.__name__:
                found = True
                break
        
        if not found:
            if not was_deferred: __deferred_classes__.add(klass)
            defer = True
            break
    
    # next, we loop through all the columns looking for foreign keys
    # and make sure that we can find the related tables (they do not 
    # have to be processed yet, just defined), and we defer if we are 
    # not able to find any of the related tables
    for col in klass.columns:
        if col.foreign_key is not None:
            found = False
            for other_klass in ActiveMapperMeta.classes.values():
                table_name = col.foreign_key._colspec.rsplit('.', 1)[0]
                if other_klass.table.fullname.lower() == table_name.lower():
                    found = True
                        
            if not found:
                if not was_deferred: __deferred_classes__.add(klass)
                defer = True
                break
    
    # if we are able to find all related and referred to tables, then
    # we can go ahead and assign the relationships to the class
    if not defer:
        relations = {}
        for propname, reldesc in klass.relations.items():
            relclass = ActiveMapperMeta.classes[reldesc.classname]
            if isinstance(reldesc.order_by, str):
                reldesc.order_by = [ reldesc.order_by ]
            if isinstance(reldesc.order_by, list):
                for itemno in range(len(reldesc.order_by)):
                    if isinstance(reldesc.order_by[itemno], str):
                        reldesc.order_by[itemno] = \
                            getattr(relclass.c, reldesc.order_by[itemno])
            relations[propname] = relation(relclass.mapper,
                                           secondary=reldesc.secondary,
                                           backref=reldesc.backref, 
                                           private=reldesc.private, 
                                           lazy=reldesc.lazy, 
                                           uselist=reldesc.uselist,
                                           order_by=reldesc.order_by)
        
        class_mapper(klass).add_properties(relations)
        if klass in __deferred_classes__: 
            __deferred_classes__.remove(klass)
        __processed_classes__.add(klass)
    
    # finally, loop through the deferred classes and attempt to process
    # relationships for them
    if not was_deferred:
        # loop through the list of deferred classes, processing the
        # relationships, until we can make no more progress
        last_count = len(__deferred_classes__) + 1
        while last_count > len(__deferred_classes__):
            last_count = len(__deferred_classes__)
            deferred = __deferred_classes__.copy()
            for deferred_class in deferred:
                if deferred_class == klass: continue
                process_relationships(deferred_class, was_deferred=True)


class ActiveMapperMeta(type):
    classes = {}
    metadatas = util.Set()
    def __init__(cls, clsname, bases, dict):
        table_name = clsname.lower()
        columns    = []
        relations  = {}
        _metadata  = getattr(sys.modules[cls.__module__], 
                             "__metadata__", metadata)
        
        if 'mapping' in dict:
            found_pk = False
            
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
                    if value.primary_key == True: found_pk = True
                        
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
            
            if not found_pk:
                col = Column('id', Integer, primary_key=True)
                cls.mapping.id = col
                columns.append(col)
            
            assert _metadata is not None, "No MetaData specified"
            
            ActiveMapperMeta.metadatas.add(_metadata)
            cls.table = Table(table_name, _metadata, *columns)
            cls.columns = columns
            
            # check for inheritence
            if hasattr(bases[0], "mapping"):
                cls._base_mapper= bases[0].mapper
                assign_mapper(objectstore._context, cls, cls.table, 
                              inherits=cls._base_mapper)
            else:
                assign_mapper(objectstore._context, cls, cls.table)
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
