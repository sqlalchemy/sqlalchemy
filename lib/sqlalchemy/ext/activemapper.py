from sqlalchemy             import objectstore, create_engine, assign_mapper, relation, mapper, join
from sqlalchemy             import and_, or_
from sqlalchemy             import Table, Column, ForeignKey
from sqlalchemy.ext.proxy   import ProxyEngine

import inspect
import sys

#
# the "proxy" to the database engine... this can be swapped out at runtime
#
engine = ProxyEngine()



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
        relationship.__init__(self, classname, colname, backref, private, lazy, uselist=False)

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
__processed_classes__ = []

def check_relationships(klass):
    #Check the class for foreign_keys recursively. If some foreign table is not found, the processing of the table
    #must be defered.
    for keyname in klass.table._foreign_keys:
        xtable = keyname._colspec[:keyname._colspec.find('.')]
        tablefound = False
        for xclass in ActiveMapperMeta.classes:
            if ActiveMapperMeta.classes[xclass].table.from_name == xtable:
                tablefound = True
                break
        if tablefound==False:
            #The refered table has not yet been created.
            return False

    return True


def process_relationships(klass):
    defer = False
    for propname, reldesc in klass.relations.items():
        #We require that every related table has been processed first
        if not reldesc.classname in __processed_classes__:
            if not klass._classname in __deferred_classes__: __deferred_classes__.append(klass._classname)
            defer = True


    #Check every column item to see if it points to an existing table
    #if it does not, defer...
    if not defer:
            if not check_relationships(klass):
                if not klass._classname in __deferred_classes__: __deferred_classes__.append(klass._classname)
                defer = True

    if not defer:
        relations = {}
        __processed_classes__.append(klass._classname)
        for propname, reldesc in klass.relations.items():
            relclass = ActiveMapperMeta.classes[reldesc.classname]
            relations[propname] = relation(relclass.mapper,
                                           secondary=reldesc.secondary,
                                           backref=reldesc.backref, 
                                           private=reldesc.private, 
                                           lazy=reldesc.lazy, 
                                           uselist=reldesc.uselist)
        if len(relations)>0:
            assign_mapper(klass, klass.table, properties=relations)

        if klass._classname in __deferred_classes__: __deferred_classes__.remove(klass._classname)

        for deferred_class in __deferred_classes__:
            process_relationships(ActiveMapperMeta.classes[deferred_class])




class ActiveMapperMeta(type):
    classes = {}

    def __init__(cls, clsname, bases, dict):
        table_name = clsname.lower()
        columns    = []
        relations  = {}
        _engine    = getattr( sys.modules[cls.__module__], "__engine__", engine )

        if 'mapping' in dict:
            members = inspect.getmembers(dict.get('mapping'))
            for name, value in members:
                if name == '__table__':
                    table_name = value
                    continue

                if '__engine__' == name:
                    _engine= value
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
            assert _engine is not None, "No engine specified"
            cls.table = Table(table_name, _engine, *columns)
            # check for inheritence
            if hasattr( bases[0], "mapping" ):
                cls._base_mapper= bases[0].mapper
                assign_mapper(cls, cls.table, inherits=cls._base_mapper)
            else:
                assign_mapper(cls, cls.table)
            cls.relations = relations
            cls._classname = clsname
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