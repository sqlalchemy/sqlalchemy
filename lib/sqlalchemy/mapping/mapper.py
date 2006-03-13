# mapper/mapper.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import sync
from sqlalchemy.exceptions import *
import objectstore
import sys
import weakref

# a dictionary mapping classes to their primary mappers
mapper_registry = weakref.WeakKeyDictionary()

class Mapper(object):
    """Persists object instances to and from schema.Table objects via the sql package.
    Instances of this class should be constructed through this package's mapper() or
    relation() function."""
    def __init__(self, 
                class_, 
                table, 
                primarytable = None, 
                properties = None, 
                primary_key = None, 
                is_primary = False, 
                inherits = None, 
                inherit_condition = None, 
                extension = None,
                order_by = False,
                allow_column_override = False,
                **kwargs):

        if primarytable is not None:
            sys.stderr.write("'primarytable' argument to mapper is deprecated\n")
            
        if extension is None:
            self.extension = MapperExtension()
        else:
            self.extension = extension                
        self.class_ = class_
        self.is_primary = is_primary
        self.order_by = order_by
        self._options = {}
        
        if not issubclass(class_, object):
            raise ArgumentError("Class '%s' is not a new-style class" % class_.__name__)
            
        if isinstance(table, sql.Select):
            # some db's, noteably postgres, dont want to select from a select
            # without an alias.  also if we make our own alias internally, then
            # the configured properties on the mapper are not matched against the alias 
            # we make, theres workarounds but it starts to get really crazy (its crazy enough
            # the SQL that gets generated) so just require an alias
            raise ArgumentError("Mapping against a Select object requires that it has a name.  Use an alias to give it a name, i.e. s = select(...).alias('myselect')")
        else:
            self.table = table

        if inherits is not None:
            self.primarytable = inherits.primarytable
            # inherit_condition is optional since the join can figure it out
            self.table = sql.join(inherits.table, table, inherit_condition)
            self._synchronizer = sync.ClauseSynchronizer(self, self, sync.ONETOMANY)
            self._synchronizer.compile(self.table.onclause, inherits.tables, TableFinder(table))
            self.inherits = inherits
            self.noninherited_table = table
        else:
            self.primarytable = self.table
            self.noninherited_table = self.table
            self._synchronizer = None
            self.inherits = None
            
        # locate all tables contained within the "table" passed in, which
        # may be a join or other construct
        self.tables = TableFinder(self.table)

        # determine primary key columns, either passed in, or get them from our set of tables
        self.pks_by_table = {}
        if primary_key is not None:
            for k in primary_key:
                self.pks_by_table.setdefault(k.table, util.HashSet(ordered=True)).append(k)
                if k.table != self.table:
                    # associate pk cols from subtables to the "main" table
                    self.pks_by_table.setdefault(self.table, util.HashSet(ordered=True)).append(k)
        else:
            for t in self.tables + [self.table]:
                try:
                    l = self.pks_by_table[t]
                except KeyError:
                    l = self.pks_by_table.setdefault(t, util.HashSet(ordered=True))
                if not len(t.primary_key):
                    raise ArgumentError("Table " + t.name + " has no primary key columns. Specify primary_key argument to mapper.")
                for k in t.primary_key:
                    l.append(k)

        # make table columns addressable via the mapper
        self.columns = util.OrderedProperties()
        self.c = self.columns
        
        # object attribute names mapped to MapperProperty objects
        self.props = {}
        
        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as 
        # populating multiple object attributes
        self.columntoproperty = {}
        
        # load custom properties 
        if properties is not None:
            for key, prop in properties.iteritems():
                if sql.is_column(prop):
                    try:
                        prop = self.table._get_col_by_original(prop)
                    except KeyError:
                        raise ArgumentError("Column '%s' is not represented in mapper's table" % prop._label)
                    self.columns[key] = prop
                    prop = ColumnProperty(prop)
                elif isinstance(prop, list) and sql.is_column(prop[0]):
                    try:
                        prop = [self.table._get_col_by_original(p) for p in prop]
                    except KeyError, e:
                        raise ArgumentError("Column '%s' is not represented in mapper's table" % e.args[0])
                    self.columns[key] = prop[0]
                    prop = ColumnProperty(*prop)
                self.props[key] = prop
                if isinstance(prop, ColumnProperty):
                    for col in prop.columns:
                        proplist = self.columntoproperty.setdefault(col.original, [])
                        proplist.append(prop)

        # load properties from the main table object,
        # not overriding those set up in the 'properties' argument
        for column in self.table.columns:
            if not self.columns.has_key(column.key):
                self.columns[column.key] = column

            if self.columntoproperty.has_key(column.original):
                continue
                
            prop = self.props.get(column.key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.props[column.key] = prop
            elif isinstance(prop, ColumnProperty):
                # the order which columns are appended to a ColumnProperty is significant, as the 
                # column at index 0 determines which result column is used to populate the object
                # attribute, in the case of mapping against a join with column names repeated
                # (and particularly in an inheritance relationship)
                prop.columns.insert(0, column)
                #prop.columns.append(column)
            else:
                if not allow_column_override:
                    raise ArgumentError("WARNING: column '%s' not being added due to property '%s'.  Specify 'allow_column_override=True' to mapper() to ignore this condition." % (column.key, repr(prop)))
                else:
                    continue
        
            # its a ColumnProperty - match the ultimate table columns
            # back to the property
            proplist = self.columntoproperty.setdefault(column.original, [])
            proplist.append(prop)

        self._get_clause = sql.and_()
        for primary_key in self.pks_by_table[self.table]:
            self._get_clause.clauses.append(primary_key == sql.bindparam("pk_"+primary_key.key))

        if not mapper_registry.has_key(self.class_) or self.is_primary or (inherits is not None and inherits._is_primary_mapper()):
            objectstore.global_attributes.reset_class_managed(self.class_)
            self._init_class()
            self.identitytable = self.primarytable
        else:
            self.identitytable = mapper_registry[self.class_].primarytable
                
        if inherits is not None:
            for key, prop in inherits.props.iteritems():
                if not self.props.has_key(key):
                    self.props[key] = prop.copy()
                    self.props[key].parent = self
                    self.props[key].key = None  # force re-init
        l = [(key, prop) for key, prop in self.props.iteritems()]
        for key, prop in l:
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)

        # this prints a summary of the object attributes and how they
        # will be mapped to table columns
        #print "mapper %s, columntoproperty:" % (self.class_.__name__)
        #for key, value in self.columntoproperty.iteritems():
        #    print key.table.name, key.key, [(v.key, v) for v in value]
            
    engines = property(lambda s: [t.engine for t in s.tables])

    def add_property(self, key, prop):
        """adds an additional property to this mapper.  this is the same as if it were 
        specified within the 'properties' argument to the constructor.  if the named
        property already exists, this will replace it.  Useful for
        circular relationships, or overriding the parameters of auto-generated properties
        such as backreferences."""
        if sql.is_column(prop):
            self.columns[key] = prop
            prop = ColumnProperty(prop)
        self.props[key] = prop
        if isinstance(prop, ColumnProperty):
            for col in prop.columns:
                proplist = self.columntoproperty.setdefault(col.original, [])
                proplist.append(prop)
        prop.init(key, self)
        
    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + self.primarytable.name
    
    def _is_primary_mapper(self):
        return mapper_registry.get(self.class_, None) is self

    def _primary_mapper(self):
        return mapper_registry[self.class_]
        
    def _init_class(self):
        """sets up our classes' overridden __init__ method, this mappers hash key as its
        '_mapper' property, and our columns as its 'c' property.  if the class already had a
        mapper, the old __init__ method is kept the same."""
        if not self.class_.__dict__.has_key('_mapper'):
            oldinit = self.class_.__init__
            def init(self, *args, **kwargs):
                nohist = kwargs.pop('_mapper_nohistory', False)
                session = kwargs.pop('_sa_session', objectstore.get_session())
                if not nohist:
                    # register new with the correct session, before the object's 
                    # constructor is called, since further assignments within the
                    # constructor would otherwise bind it to whatever get_session() is.
                    session.register_new(self)
                if oldinit is not None:
                    oldinit(self, *args, **kwargs)
            # override oldinit, insuring that its not already one of our
            # own modified inits
            if oldinit is None or not hasattr(oldinit, '_sa_mapper_init'):
                init._sa_mapper_init = True
                self.class_.__init__ = init
        mapper_registry[self.class_] = self
        self.class_.c = self.c
        
    def set_property(self, key, prop):
        self.props[key] = prop
        prop.init(key, self)
    
    def instances(self, cursor, *mappers, **kwargs):
        limit = kwargs.get('limit', None)
        offset = kwargs.get('offset', None)
        populate_existing = kwargs.get('populate_existing', False)
        
        result = util.HistoryArraySet()
        if mappers:
            otherresults = []
            for m in mappers:
                otherresults.append(util.HistoryArraySet())
                
        imap = {}
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            self._instance(row, imap, result, populate_existing=populate_existing)
            i = 0
            for m in mappers:
                m._instance(row, imap, otherresults[i])
                i+=1
                
        # store new stuff in the identity map
        for value in imap.values():
            objectstore.get_session().register_clean(value)

        if mappers:
            result.extend(otherresults)
        return result
            
    def get(self, *ident):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The *ident argument is a 
        list of primary key columns in the order of the table def's primary key columns."""
        key = objectstore.get_id_key(ident, self.class_)
        #print "key: " + repr(key) + " ident: " + repr(ident)
        return self._get(key, ident)
        
    def _get(self, key, ident=None, reload=False):
        if not reload:
            try:
                return objectstore.get_session()._get(key)
            except KeyError:
                pass
            
        if ident is None:
            ident = key[1]
        i = 0
        params = {}
        for primary_key in self.pks_by_table[self.table]:
            params["pk_"+primary_key.key] = ident[i]
            i += 1
        try:
            statement = self._compile(self._get_clause)
            return self._select_statement(statement, params=params, populate_existing=reload)[0]
        except IndexError:
            return None

        
    def identity_key(self, *primary_key):
        """returns the instance key for the given identity value.  this is a global tracking object used by the objectstore, and is usually available off a mapped object as instance._instance_key."""
        return objectstore.get_id_key(tuple(primary_key), self.class_)
    
    def instance_key(self, instance):
        """returns the instance key for the given instance.  this is a global tracking object used by the objectstore, and is usually available off a mapped object as instance._instance_key."""
        return self.identity_key(*self.identity(instance))

    def identity(self, instance):
        """returns the identity (list of primary key values) for the given instance.  The list of values can be fed directly into the get() method as mapper.get(*key)."""
        return [self._getattrbycolumn(instance, column) for column in self.pks_by_table[self.table]]
        
    def compile(self, whereclause = None, **options):
        """works like select, except returns the SQL statement object without 
        compiling or executing it"""
        return self._compile(whereclause, **options)

    def copy(self):
        mapper = Mapper.__new__(Mapper)
        mapper.__dict__.update(self.__dict__)
        mapper.props = self.props.copy()
        return mapper
    
    def using(self, session):
        """returns a proxying object to this mapper, which will execute methods on the mapper
        within the context of the given session.  The session is placed as the "current" session
        via the push_session/pop_session methods in the objectstore module."""
        mapper = self
        class Proxy(object):
            def __getattr__(self, key):
                def callit(*args, **kwargs):
                    objectstore.push_session(session)
                    try:
                        return getattr(mapper, key)(*args, **kwargs)
                    finally:
                        objectstore.pop_session()
                return callit
        return Proxy()

    def options(self, *options):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload(), lazyload(), and noload()"""

        optkey = repr([hash_key(o) for o in options])
        try:
            return self._options[optkey]
        except KeyError:
            mapper = self.copy()
            for option in options:
                option.process(mapper)
            self._options[optkey] = mapper
            return mapper

    def get_by(self, *args, **params):
        """returns a single object instance based on the given key/value criterion. 
        this is either the first value in the result list, or None if the list is 
        empty.
        
        the keys are mapped to property or column names mapped by this mapper's Table, and the values
        are coerced into a WHERE clause separated by AND operators.  If the local property/column
        names dont contain the key, a search will be performed against this mapper's immediate
        list of relations as well, forming the appropriate join conditions if a matching property
        is located.
        
        e.g.   u = usermapper.get_by(user_name = 'fred')
        """
        x = self.select_whereclause(self._by_clause(*args, **params), limit=1)
        if x:
            return x[0]
        else:
            return None
            
    def select_by(self, *args, **params):
        """returns an array of object instances based on the given clauses and key/value criterion. 
        
        *args is a list of zero or more ClauseElements which will be connected by AND operators.
        **params is a set of zero or more key/value parameters which are converted into ClauseElements.
        the keys are mapped to property or column names mapped by this mapper's Table, and the values
        are coerced into a WHERE clause separated by AND operators.  If the local property/column
        names dont contain the key, a search will be performed against this mapper's immediate
        list of relations as well, forming the appropriate join conditions if a matching property
        is located.
        
        e.g.   result = usermapper.select_by(user_name = 'fred')
        """
        return self.select_whereclause(self._by_clause(*args, **params))
    
    def selectfirst_by(self, *args, **params):
        """works like select_by(), but only returns the first result by itself, or None if no 
        objects returned.  Synonymous with get_by()"""
        return self.get_by(*args, **params)

    def selectone_by(self, *args, **params):
        """works like selectfirst_by(), but throws an error if not exactly one result was returned."""
        ret = self.select_by(*args, **params)
        if len(ret) == 1:
            return ret[0]
        raise InvalidRequestError('Multiple rows returned for selectone_by')

    def count_by(self, *args, **params):
        """returns the count of instances based on the given clauses and key/value criterion.
        The criterion is constructed in the same way as the select_by() method."""
        return self.count(self._by_clause(*args, **params))
        
    def _by_clause(self, *args, **params):
        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg
        for key, value in params.iteritems():
            if value is False:
                continue
            c = self._get_criterion(key, value)
            if c is None:
                raise InvalidRequestError("Cant find criterion for property '"+ key + "'")
            if clause is None:
                clause = c
            else:                
                clause &= c
        return clause

    def _get_criterion(self, key, value):
        """used by select_by to match a key/value pair against
        local properties, column names, or a matching property in this mapper's
        list of relations."""
        if self.props.has_key(key):
            return self.props[key].columns[0] == value
        elif self.table.c.has_key(key):
            return self.table.c[key] == value
        else:
            for prop in self.props.values():
                c = prop.get_criterion(key, value)
                if c is not None:
                    return c
            else:
                return None

    def __getattr__(self, key):
        if (key.startswith('select_by_')):
            key = key[10:]
            def foo(arg):
                return self.select_by(**{key:arg})
            return foo
        elif (key.startswith('get_by_')):
            key = key[7:]
            def foo(arg):
                return self.get_by(**{key:arg})
            return foo
        else:
            raise AttributeError(key)
        
    def selectfirst(self, *args, **params):
        """works like select(), but only returns the first result by itself, or None if no 
        objects returned."""
        params['limit'] = 1
        ret = self.select(*args, **params)
        if ret:
            return ret[0]
        else:
            return None
            
    def selectone(self, *args, **params):
        """works like selectfirst(), but throws an error if not exactly one result was returned."""
        ret = self.select(*args, **params)
        if len(ret) == 1:
            return ret[0]
        raise InvalidRequestError('Multiple rows returned for selectone')
            
    def select(self, arg = None, **kwargs):
        """selects instances of the object from the database.  
        
        arg can be any ClauseElement, which will form the criterion with which to
        load the objects.
        
        For more advanced usage, arg can also be a Select statement object, which
        will be executed and its resulting rowset used to build new object instances.  
        in this case, the developer must insure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""
        if arg is not None and isinstance(arg, sql.Selectable):
            return self.select_statement(arg, **kwargs)
        else:
            return self.select_whereclause(arg, **kwargs)

    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        statement = self._compile(whereclause, **kwargs)
        return self._select_statement(statement, params=params)

    def count(self, whereclause=None, params=None, **kwargs):
        s = self.table.count(whereclause)
        if params is not None:
            return s.scalar(**params)
        else:
            return s.scalar()

    def select_statement(self, statement, **params):
        return self._select_statement(statement, params=params)

    def select_text(self, text, **params):
        t = sql.text(text, engine=self.primarytable.engine)
        return self.instances(t.execute(**params))

    def _select_statement(self, statement, params=None, **kwargs):
        statement.use_labels = True
        if params is None:
            params = {}
        return self.instances(statement.execute(**params), **kwargs)

    def _getpropbycolumn(self, column):
        try:
            prop = self.columntoproperty[column.original]
        except KeyError:
            try:
                prop = self.props[column.key]
                raise InvalidRequestError("Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop)))
            except KeyError:
                raise InvalidRequestError("No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self)))
        return prop[0]
        
    def _getattrbycolumn(self, obj, column):
        prop = self._getpropbycolumn(column)
        return prop.getattr(obj)

    def _setattrbycolumn(self, obj, column, value):
        self.columntoproperty[column.original][0].setattr(obj, value)

        
    def save_obj(self, objects, uow, postupdate=False):
        """called by a UnitOfWork object to save objects, which involves either an INSERT or
        an UPDATE statement for each table used by this mapper, for each element of the
        list."""
          
        for table in self.tables:
            #print "SAVE_OBJ table ", table.name
            # looping through our set of tables, which are all "real" tables, as opposed
            # to our main table which might be a select statement or something non-writeable
            
            # the loop structure is tables on the outer loop, objects on the inner loop.
            # this allows us to bundle inserts/updates on the same table together...although currently
            # they are separate execs via execute(), not executemany()
            
            if not self._has_pks(table):
                # if we dont have a full set of primary keys for this table, we cant really
                # do any CRUD with it, so skip.  this occurs if we are mapping against a query
                # that joins on other tables so its not really an error condition.
                continue

            # two lists to store parameters for each table/object pair located
            insert = []
            update = []
            
            # we have our own idea of the primary key columns 
            # for this table, in the case that the user
            # specified custom primary key cols.
            for obj in objects:
                #print "SAVE_OBJ we are Mapper(" + str(id(self)) + ") obj: " +  obj.__class__.__name__ + repr(id(obj))
                params = {}

                # 'postupdate' means a PropertyLoader is telling us, "yes I know you 
                # already inserted/updated this row but I need you to UPDATE one more 
                # time"
                isinsert = not postupdate and not hasattr(obj, "_instance_key")
                if isinsert:
                    self.extension.before_insert(self, obj)
                else:
                    self.extension.before_update(self, obj)
                hasdata = False
                for col in table.columns:
                    if self.pks_by_table[table].contains(col):
                        # column is a primary key ?
                        if not isinsert:
                            # doing an UPDATE?  put primary key values as "WHERE" parameters
                            # matching the bindparam we are creating below, i.e. "<tablename>_<colname>"
                            params[col.table.name + "_" + col.key] = self._getattrbycolumn(obj, col)
                        else:
                            # doing an INSERT, primary key col ? 
                            # if the primary key values are not populated,
                            # leave them out of the INSERT altogether, since PostGres doesn't want
                            # them to be present for SERIAL to take effect.  A SQLEngine that uses
                            # explicit sequences will put them back in if they are needed
                            value = self._getattrbycolumn(obj, col)
                            if value is not None:
                                params[col.key] = value
                    else:
                        # column is not a primary key ?
                        if not isinsert:
                            # doing an UPDATE ? get the history for the attribute, with "passive"
                            # so as not to trigger any deferred loads.  if there is a new
                            # value, add it to the bind parameters
                            prop = self._getpropbycolumn(col)
                            history = prop.get_history(obj, passive=True)
                            if history:
                                a = history.added_items()
                                if len(a):
                                    params[col.key] = a[0]
                                    hasdata = True
                        else:
                            # doing an INSERT, non primary key col ? 
                            # add the attribute's value to the 
                            # bind parameters, unless its None and the column has a 
                            # default.  if its None and theres no default, we still might
                            # not want to put it in the col list but SQLIte doesnt seem to like that
                            # if theres no columns at all
                            value = self._getattrbycolumn(obj, col)
                            if col.default is None or value is not None:
                                params[col.key] = value

                if not isinsert:
                    if hasdata:
                        # if none of the attributes changed, dont even
                        # add the row to be updated.
                        update.append((obj, params))
                else:
                    insert.append((obj, params))
            if len(update):
                clause = sql.and_()
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.table.name + "_" + col.key))
                statement = table.update(clause)
                rows = 0
                for rec in update:
                    (obj, params) = rec
                    c = statement.execute(params)
                    self._postfetch(table, obj, table.engine.last_updated_params())
                    self.extension.after_update(self, obj)
                    rows += c.cursor.rowcount
                if table.engine.supports_sane_rowcount() and rows != len(update):
                    raise CommitError("ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (rows, len(update)))
            if len(insert):
                statement = table.insert()
                for rec in insert:
                    (obj, params) = rec
                    statement.execute(**params)
                    primary_key = table.engine.last_inserted_ids()
                    if primary_key is not None:
                        i = 0
                        for col in self.pks_by_table[table]:
                            #print "col: " + table.name + "." + col.key + " val: " + repr(self._getattrbycolumn(obj, col))
                            if self._getattrbycolumn(obj, col) is None:
                                self._setattrbycolumn(obj, col, primary_key[i])
                            i+=1
                    self._postfetch(table, obj, table.engine.last_inserted_params())
                    if self._synchronizer is not None:
                        self._synchronizer.execute(obj, obj)
                    self.extension.after_insert(self, obj)

    def _postfetch(self, table, obj, params):
        """after an INSERT or UPDATE, asks the engine if PassiveDefaults fired off on the database side
        which need to be post-fetched, *or* if pre-exec defaults like ColumnDefaults were fired off
        and should be populated into the instance. this is only for non-primary key columns."""
        if table.engine.lastrow_has_defaults():
            clause = sql.and_()
            for p in self.pks_by_table[table]:
                clause.clauses.append(p == self._getattrbycolumn(obj, p))
            row = table.select(clause).execute().fetchone()
            for c in table.c:
                if self._getattrbycolumn(obj, c) is None:
                    self._setattrbycolumn(obj, c, row[c])
        else:
            for c in table.c:
                if c.primary_key or not params.has_key(c.name):
                    continue
                if self._getattrbycolumn(obj, c) != params.get_original(c.name):
                    self._setattrbycolumn(obj, c, params.get_original(c.name))

    def delete_obj(self, objects, uow):
        """called by a UnitOfWork object to delete objects, which involves a
        DELETE statement for each table used by this mapper, for each object in the list."""
        for table in util.reversed(self.tables):
            if not self._has_pks(table):
                continue
            delete = []
            for obj in objects:
                params = {}
                if not hasattr(obj, "_instance_key"):
                    continue
                else:
                    delete.append(params)
                for col in self.pks_by_table[table]:
                    params[col.key] = self._getattrbycolumn(obj, col)
                self.extension.before_delete(self, obj)
            if len(delete):
                clause = sql.and_()
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key))
                statement = table.delete(clause)
                c = statement.execute(*delete)
                if table.engine.supports_sane_rowcount() and c.rowcount != len(delete):
                    raise CommitError("ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (c.cursor.rowcount, len(delete)))

    def _has_pks(self, table):
        try:
            for k in self.pks_by_table[table]:
                if not self.columntoproperty.has_key(k.original):
                    return False
            else:
                return True
        except KeyError:
            return False
            
    def register_dependencies(self, uowcommit, *args, **kwargs):
        """called by an instance of objectstore.UOWTransaction to register 
        which mappers are dependent on which, as well as DependencyProcessor 
        objects which will process lists of objects in between saves and deletes."""
        for prop in self.props.values():
            prop.register_dependencies(uowcommit, *args, **kwargs)
        if self.inherits is not None:
            uowcommit.register_dependency(self.inherits, self)
            
    def register_deleted(self, obj, uow):
        for prop in self.props.values():
            prop.register_deleted(obj, uow)
    
    def _should_nest(self, **kwargs):
        """returns True if the given statement options indicate that we should "nest" the
        generated query as a subquery inside of a larger eager-loading query.  this is used
        with keywords like distinct, limit and offset and the mapper defines eager loads."""
        return (
            getattr(self, '_has_eager', False)
            and (kwargs.has_key('limit') or kwargs.has_key('offset') or kwargs.get('distinct', False))
        )
        
    def _compile(self, whereclause = None, **kwargs):
        order_by = kwargs.pop('order_by', False)
        if order_by is False:
            order_by = self.order_by
        if order_by is False:
            if self.table.default_order_by() is not None:
                order_by = self.table.default_order_by()

        if self._should_nest(**kwargs):
            s2 = sql.select(self.table.primary_key, whereclause, use_labels=True, from_obj=[self.table], **kwargs)
#            raise "ok first thing", str(s2)
            if not kwargs.get('distinct', False) and order_by:
                s2.order_by(*util.to_list(order_by))
            s3 = s2.alias('rowcount')
            crit = []
            for i in range(0, len(self.table.primary_key)):
                crit.append(s3.primary_key[i] == self.table.primary_key[i])
            statement = sql.select([], sql.and_(*crit), from_obj=[self.table], use_labels=True)
 #           raise "OK statement", str(statement)
            if order_by:
                statement.order_by(*util.to_list(order_by))
        else:
            statement = sql.select([], whereclause, from_obj=[self.table], use_labels=True, **kwargs)
            if order_by:
                statement.order_by(*util.to_list(order_by))
            # for a DISTINCT query, you need the columns explicitly specified in order
            # to use it in "order_by".  insure they are in the column criterion (particularly oid).
            # TODO: this should be done at the SQL level not the mapper level
            if kwargs.get('distinct', False) and order_by:
                statement.append_column(*util.to_list(order_by))
        # plugin point
        
            
        # give all the attached properties a chance to modify the query
        for key, value in self.props.iteritems():
            value.setup(key, statement, **kwargs) 
        return statement
        
    def _identity_key(self, row):
        return objectstore.get_row_key(row, self.class_, self.pks_by_table[self.table])

    def _instance(self, row, imap, result = None, populate_existing = False):
        """pulls an object instance from the given row and appends it to the given result
        list. if the instance already exists in the given identity map, its not added.  in
        either case, executes all the property loaders on the instance to also process extra
        information in the row."""

        # look in main identity map.  if its there, we dont do anything to it,
        # including modifying any of its related items lists, as its already
        # been exposed to being modified by the application.
        identitykey = self._identity_key(row)
        if objectstore.get_session().has_key(identitykey):
            instance = objectstore.get_session()._get(identitykey)

            isnew = False
            if populate_existing:
                if not imap.has_key(identitykey):
                    imap[identitykey] = instance
                for prop in self.props.values():
                    prop.execute(instance, row, identitykey, imap, True)

            if self.extension.append_result(self, row, imap, result, instance, isnew, populate_existing=populate_existing):
                if result is not None:
                    result.append_nohistory(instance)

            return instance
                    
        # look in result-local identitymap for it.
        exists = imap.has_key(identitykey)      
        if not exists:
            # check if primary key cols in the result are None - this indicates 
            # an instance of the object is not present in the row
            for col in self.pks_by_table[self.table]:
                if row[col] is None:
                    return None
            # plugin point
            instance = self.extension.create_instance(self, row, imap, self.class_)
            if instance is None:
                instance = self.class_(_mapper_nohistory=True)
            imap[identitykey] = instance
            isnew = True
        else:
            instance = imap[identitykey]
            isnew = False

        # plugin point
        
        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        for prop in self.props.values():
            prop.execute(instance, row, identitykey, imap, isnew)

        if self.extension.append_result(self, row, imap, result, instance, isnew, populate_existing=populate_existing):
            if result is not None:
                result.append_nohistory(instance)

        return instance

class MapperProperty(object):
    """an element attached to a Mapper that describes and assists in the loading and saving 
    of an attribute on an object instance."""
    def execute(self, instance, row, identitykey, imap, isnew):
        """called when the mapper receives a row.  instance is the parent instance
        corresponding to the row. """
        raise NotImplementedError()
    def copy(self):
        raise NotImplementedError()
    def get_criterion(self, key, value):
        """Returns a WHERE clause suitable for this MapperProperty corresponding to the 
        given key/value pair, where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by PropertyLoaders.
            
        this is called by a mappers select_by method to formulate a set of key/value pairs into 
        a WHERE criterion that spans multiple tables if needed."""
        return None
    def hash_key(self):
        """describes this property and its instantiated arguments in such a way
        as to uniquely identify the concept this MapperProperty represents,within 
        a process."""
        raise NotImplementedError()
    def setup(self, key, statement, **options):
        """called when a statement is being constructed.  """
        return self
    def init(self, key, parent):
        """called when the MapperProperty is first attached to a new parent Mapper."""
        self.key = key
        self.parent = parent
        self.do_init(key, parent)
    def do_init(self, key, parent):
        """template method for subclasses"""
        pass
    def register_deleted(self, object, uow):
        """called when the instance is being deleted"""
        pass
    def register_dependencies(self, *args, **kwargs):
        pass
    def is_primary(self):
        """a return value of True indicates we are the primary MapperProperty for this loader's
        attribute on our mapper's class.  It means we can set the object's attribute behavior
        at the class level.  otherwise we have to set attribute behavior on a per-instance level."""
        return self.parent._is_primary_mapper()

class MapperOption(object):
    """describes a modification to a Mapper in the context of making a copy
    of it.  This is used to assist in the prototype pattern used by mapper.options()."""
    def process(self, mapper):
        raise NotImplementedError()
    def hash_key(self):
        return repr(self)

class ExtensionOption(MapperOption):
    """adds a new MapperExtension to a mapper's chain of extensions"""
    def __init__(self, ext):
        self.ext = ext
    def process(self, mapper):
        self.ext.next = mapper.extension
        mapper.extension = self.ext

class MapperExtension(object):
    def __init__(self):
        self.next = None
    def create_instance(self, mapper, row, imap, class_):
        """called when a new object instance is about to be created from a row.  
        the method can choose to create the instance itself, or it can return 
        None to indicate normal object creation should take place.
        
        mapper - the mapper doing the operation
        
        row - the result row from the database
        
        imap - a dictionary that is storing the running set of objects collected from the
        current result set
        
        class_ - the class we are mapping.
        """
        if self.next is None:
            return None
        else:
            return self.next.create_instance(mapper, row, imap, class_)
    def append_result(self, mapper, row, imap, result, instance, isnew, populate_existing=False):
        """called when an object instance is being appended to a result list.
        
        If it returns True, it is assumed that this method handled the appending itself.

        mapper - the mapper doing the operation
        
        row - the result row from the database
        
        imap - a dictionary that is storing the running set of objects collected from the
        current result set
        
        result - an instance of util.HistoryArraySet(), which may be an attribute on an
        object if this is a related object load (lazy or eager).  use result.append_nohistory(value)
        to append objects to this list.
        
        instance - the object instance to be appended to the result
        
        isnew - indicates if this is the first time we have seen this object instance in the current result
        set.  if you are selecting from a join, such as an eager load, you might see the same object instance
        many times in the same result set.
        
        populate_existing - usually False, indicates if object instances that were already in the main 
        identity map, i.e. were loaded by a previous select(), get their attributes overwritten
        """
        if self.next is None:
            return True
        else:
            return self.next.append_result(mapper, row, imap, result, instance, isnew, populate_existing)
    def before_insert(self, mapper, instance):
        """called before an object instance is INSERTed into its table.
        
        this is a good place to set up primary key values and such that arent handled otherwise."""
        if self.next is not None:
            self.next.before_insert(mapper, instance)
    def before_update(self, mapper, instance):
        """called before an object instnace is UPDATED"""
        if self.next is not None:
            self.next.before_update(mapper, instance)
    def after_update(self, mapper, instance):
        """called after an object instnace is UPDATED"""
        if self.next is not None:
            self.next.after_update(mapper, instance)
    def after_insert(self, mapper, instance):
        """called after an object instance has been INSERTed"""
        if self.next is not None:
            self.next.after_insert(mapper, instance)
    def before_delete(self, mapper, instance):
        """called before an object instance is DELETEed"""
        if self.next is not None:
            self.next.before_delete(mapper, instance)

class TableFinder(sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self, table, check_columns=False):
        self.tables = []
        self.check_columns = check_columns
        table.accept_visitor(self)
    def visit_table(self, table):
        self.tables.append(table)
    def __len__(self):
        return len(self.tables)
    def __getitem__(self, i):
        return self.tables[i]
    def __iter__(self):
        return iter(self.tables)
    def __contains__(self, obj):
        return obj in self.tables
    def __add__(self, obj):
        return self.tables + obj
    def visit_column(self, column):
        if self.check_columns:
            column.table.accept_visitor(self)
        
def hash_key(obj):
    if obj is None:
        return 'None'
    elif isinstance(obj, list):
        return repr([hash_key(o) for o in obj])
    elif hasattr(obj, 'hash_key'):
        return obj.hash_key()
    else:
        return repr(obj)
        
def object_mapper(object):
    """given an object, returns the primary Mapper associated with the object
    or the object's class."""
    return class_mapper(object.__class__)

def class_mapper(class_):
    """given a class, returns the primary Mapper associated with the class."""
    try:
        return mapper_registry[class_]
    except (KeyError, AttributeError):
        raise InvalidRequestError("Class '%s' has no mapper associated with it" % class_.__name__)
