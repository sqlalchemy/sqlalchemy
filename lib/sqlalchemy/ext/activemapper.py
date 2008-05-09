from sqlalchemy             import ThreadLocalMetaData, util, Integer
from sqlalchemy             import Table, Column, ForeignKey
from sqlalchemy.orm         import class_mapper, relation, scoped_session
from sqlalchemy.orm         import sessionmaker
                                   
from sqlalchemy.orm import backref as create_backref

import inspect
import sys

#
# the "proxy" to the database engine... this can be swapped out at runtime
#
metadata = ThreadLocalMetaData()
Objectstore = scoped_session
objectstore = scoped_session(sessionmaker(autoflush=True, transactional=False))

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
                 lazy=True, uselist=True, secondary=None, order_by=False, viewonly=False):
        self.classname = classname
        self.colname   = colname
        self.backref   = backref
        self.private   = private
        self.lazy      = lazy
        self.uselist   = uselist
        self.secondary = secondary
        self.order_by  = order_by
        self.viewonly  = viewonly
    
    def process(self, klass, propname, relations):
        relclass = ActiveMapperMeta.classes[self.classname]
        
        if isinstance(self.order_by, str):
            self.order_by = [ self.order_by ]
        
        if isinstance(self.order_by, list):
            for itemno in range(len(self.order_by)):
                if isinstance(self.order_by[itemno], str):
                    self.order_by[itemno] = \
                        getattr(relclass.c, self.order_by[itemno])
        
        backref = self.create_backref(klass)
        relations[propname] = relation(relclass.mapper,
                                       secondary=self.secondary,
                                       backref=backref, 
                                       private=self.private, 
                                       lazy=self.lazy, 
                                       uselist=self.uselist,
                                       order_by=self.order_by, 
                                       viewonly=self.viewonly)
    
    def create_backref(self, klass):
        if self.backref is None:
            return None
        
        relclass = ActiveMapperMeta.classes[self.classname]
        
        if klass.__name__ == self.classname:
            class_mapper(relclass).compile()
            br_fkey = relclass.c[self.colname]
        else:
            br_fkey = None
        
        return create_backref(self.backref, remote_side=br_fkey)


class one_to_many(relationship):
    def __init__(self, *args, **kwargs):
        kwargs['uselist'] = True
        relationship.__init__(self, *args, **kwargs)

class one_to_one(relationship):
    def __init__(self, *args, **kwargs):
        kwargs['uselist'] = False
        relationship.__init__(self, *args, **kwargs)
    
    def create_backref(self, klass):
        if self.backref is None:
            return None
        
        relclass = ActiveMapperMeta.classes[self.classname]
        
        if klass.__name__ == self.classname:
            br_fkey = getattr(relclass.c, self.colname)
        else:
            br_fkey = None
        
        return create_backref(self.backref, foreignkey=br_fkey, uselist=False)


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

__deferred_classes__ = {}
__processed_classes__ = {}
def process_relationships(klass, was_deferred=False):
    # first, we loop through all of the relationships defined on the
    # class, and make sure that the related class already has been
    # completely processed and defer processing if it has not
    defer = False
    for propname, reldesc in klass.relations.items():
        found = (reldesc.classname == klass.__name__ or reldesc.classname in __processed_classes__)
        if not found:
            defer = True
            break
    
    # next, we loop through all the columns looking for foreign keys
    # and make sure that we can find the related tables (they do not 
    # have to be processed yet, just defined), and we defer if we are 
    # not able to find any of the related tables
    if not defer:
        for col in klass.columns:
            if col.foreign_keys:
                found = False
                cn = col.foreign_keys[0]._colspec
                table_name = cn[:cn.rindex('.')]
                for other_klass in ActiveMapperMeta.classes.values():
                    if other_klass.table.fullname.lower() == table_name.lower():
                        found = True
                        
                if not found:
                    defer = True
                    break

    if defer and not was_deferred:
        __deferred_classes__[klass.__name__] = klass
        
    # if we are able to find all related and referred to tables, then
    # we can go ahead and assign the relationships to the class
    if not defer:
        relations = {}
        for propname, reldesc in klass.relations.items():
            reldesc.process(klass, propname, relations)
        
        class_mapper(klass).add_properties(relations)
        if klass.__name__ in __deferred_classes__: 
            del __deferred_classes__[klass.__name__]
        __processed_classes__[klass.__name__] = klass
    
    # finally, loop through the deferred classes and attempt to process
    # relationships for them
    if not was_deferred:
        # loop through the list of deferred classes, processing the
        # relationships, until we can make no more progress
        last_count = len(__deferred_classes__) + 1
        while last_count > len(__deferred_classes__):
            last_count = len(__deferred_classes__)
            deferred = __deferred_classes__.copy()
            for deferred_class in deferred.values():
                process_relationships(deferred_class, was_deferred=True)


class ActiveMapperMeta(type):
    classes = {}
    metadatas = util.Set()
    def __init__(cls, clsname, bases, dict):
        table_name = clsname.lower()
        columns    = []
        relations  = {}
        autoload   = False
        _metadata  = getattr(sys.modules[cls.__module__], 
                             "__metadata__", metadata)
        version_id_col = None
        version_id_col_object = None
        table_opts = {}

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
                
                if '__autoload__' == name:
                    autoload = True
                    continue
                
                if '__version_id_col__' == name:
                    version_id_col = value
                
                if '__table_opts__' == name:
                    table_opts = value

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
            
            if not found_pk and not autoload:
                col = Column('id', Integer, primary_key=True)
                cls.mapping.id = col
                columns.append(col)
            
            assert _metadata is not None, "No MetaData specified"
            
            ActiveMapperMeta.metadatas.add(_metadata)
            
            if not autoload:
                cls.table = Table(table_name, _metadata, *columns, **table_opts)
                cls.columns = columns
            else:
                cls.table = Table(table_name, _metadata, autoload=True, **table_opts)
                cls.columns = cls.table._columns
            
            if version_id_col is not None:
                version_id_col_object = getattr(cls.table.c, version_id_col, None)
                assert(version_id_col_object is not None, "version_id_col (%s) does not exist." % version_id_col)

            # check for inheritence
            if hasattr(bases[0], "mapping"):
                cls._base_mapper= bases[0].mapper
                cls.mapper = objectstore.mapper(cls, cls.table, 
                              inherits=cls._base_mapper, version_id_col=version_id_col_object)
            else:
                cls.mapper = objectstore.mapper(cls, cls.table, version_id_col=version_id_col_object)
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
