from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import mapper, class_mapper, attributes, object_mapper
from sqlalchemy.orm.exc import UnmappedClassError, UnmappedColumnError
from sqlalchemy import Table, Column, ForeignKeyConstraint, Integer
from sqlalchemy.orm.interfaces import SessionExtension

def col_references_table(col, table):
    for fk in col.foreign_keys:
        if fk.references(table):
            return True
    return False

def _history_mapper(local_mapper):
    cls = local_mapper.class_

    # SLIGHT SQLA HACK #1 - set the "active_history" flag
    # on on column-mapped attributes so that the old version
    # of the info is always loaded (currently sets it on all attributes)
    for prop in local_mapper.iterate_properties:
        getattr(local_mapper.class_, prop.key).impl.active_history = True
    
    super_mapper = local_mapper.inherits
    super_history_mapper = getattr(cls, '__history_mapper__', None)
    
    polymorphic_on = None
    super_fks = []
    if not super_mapper or local_mapper.local_table is not super_mapper.local_table:
        cols = []
        for column in local_mapper.local_table.c:
            if column.name == 'version':
                continue
                
            col = column.copy()

            if super_mapper and col_references_table(column, super_mapper.local_table):
                super_fks.append((col.key, list(super_history_mapper.base_mapper.local_table.primary_key)[0]))

            cols.append(col)
            
            if column is local_mapper.polymorphic_on:
                polymorphic_on = col
        
        if super_mapper:
            super_fks.append(('version', super_history_mapper.base_mapper.local_table.c.version))
            cols.append(Column('version', Integer, primary_key=True))
        else:
            cols.append(Column('version', Integer, primary_key=True))
        
        if super_fks:
            cols.append(ForeignKeyConstraint(*zip(*super_fks)))

        table = Table(local_mapper.local_table.name + '_history', local_mapper.local_table.metadata,
           *cols
        )
    else:
        # single table inheritance.  take any additional columns that may have
        # been added and add them to the history table.
        for column in local_mapper.local_table.c:
            if column.key not in super_history_mapper.local_table.c:
                col = column.copy()
                super_history_mapper.local_table.append_column(col)
        table = None
        
    if super_history_mapper:
        bases = (super_history_mapper.class_,)
    else:
        bases = local_mapper.base_mapper.class_.__bases__
    versioned_cls = type.__new__(type, "%sHistory" % cls.__name__, bases, {})
    
    m = mapper(
            versioned_cls, 
            table, 
            inherits=super_history_mapper, 
            polymorphic_on=polymorphic_on,
            polymorphic_identity=local_mapper.polymorphic_identity
            )
    cls.__history_mapper__ = m
    
    if not super_history_mapper:
        cls.version = Column('version', Integer, default=1, nullable=False)
    
    
class VersionedMeta(DeclarativeMeta):
    def __init__(cls, classname, bases, dict_):
        DeclarativeMeta.__init__(cls, classname, bases, dict_)

        try:
            mapper = class_mapper(cls)
            _history_mapper(mapper)
        except UnmappedClassError:
            pass


def versioned_objects(iter):
    for obj in iter:
        if hasattr(obj, '__history_mapper__'):
            yield obj

def create_version(obj, session, deleted = False):
    obj_mapper = object_mapper(obj)
    history_mapper = obj.__history_mapper__
    history_cls = history_mapper.class_
    
    obj_state = attributes.instance_state(obj)
    
    attr = {}

    obj_changed = False
    
    for om, hm in zip(obj_mapper.iterate_to_root(), history_mapper.iterate_to_root()):
        if hm.single:
            continue
    
        for hist_col in hm.local_table.c:
            if hist_col.key == 'version':
                continue
                
            obj_col = om.local_table.c[hist_col.key]

            # SLIGHT SQLA HACK #3 - get the value of the
            # attribute based on the MapperProperty related to the
            # mapped column.  this will allow usage of MapperProperties
            # that have a different keyname than that of the mapped column.
            try:
                prop = obj_mapper._get_col_to_prop(obj_col)
            except UnmappedColumnError:
                # in the case of single table inheritance, there may be 
                # columns on the mapped table intended for the subclass only.
                # the "unmapped" status of the subclass column on the 
                # base class is a feature of the declarative module as of sqla 0.5.2.
                continue
                
            # expired object attributes and also deferred cols might not be in the
            # dict.  force it to load no matter what by using getattr().
            if prop.key not in obj_state.dict:
                getattr(obj, prop.key)

            a, u, d = attributes.get_history(obj, prop.key)

            if d:
                attr[hist_col.key] = d[0]
                obj_changed = True
            elif u:
                attr[hist_col.key] = u[0]
            else:
                raise Exception("TODO: what makes us arrive here ?")
                
    if not obj_changed and not deleted:            
        return

    attr['version'] = obj.version
    hist = history_cls()
    for key, value in attr.iteritems():
        setattr(hist, key, value)
    session.add(hist)
    obj.version += 1
    
class VersionedListener(SessionExtension):
    def before_flush(self, session, flush_context, instances):
        for obj in versioned_objects(session.dirty):
            create_version(obj, session)
        for obj in versioned_objects(session.deleted):
            create_version(obj, session, deleted = True)
            
