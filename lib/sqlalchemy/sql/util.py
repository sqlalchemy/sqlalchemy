from sqlalchemy import util, schema, topological
from sqlalchemy.sql import expression, visitors

"""Utility functions that build upon SQL and Schema constructs."""

class ClauseParameters(object):
    """Represent a dictionary/iterator of bind parameter key names/values.

    Tracks the original [sqlalchemy.sql#_BindParamClause] objects as well as the
    keys/position of each parameter, and can return parameters as a
    dictionary or a list.  Will process parameter values according to
    the ``TypeEngine`` objects present in the ``_BindParamClause`` instances.
    """

    __slots__ = 'dialect', '_binds', 'positional'

    def __init__(self, dialect, positional=None):
        self.dialect = dialect
        self._binds = {}
        if positional is None:
            self.positional = []
        else:
            self.positional = positional

    def get_parameter(self, key):
        return self._binds[key]

    def set_parameter(self, bindparam, value, name):
        self._binds[name] = [bindparam, name, value]
        
    def get_original(self, key):
        return self._binds[key][2]

    def get_type(self, key):
        return self._binds[key][0].type

    def get_processors(self):
        """return a dictionary of bind 'processing' functions"""
        return dict([
            (key, value) for key, value in 
            [(
                key,
                self._binds[key][0].bind_processor(self.dialect)
            ) for key in self._binds]
            if value is not None
        ])
    
    def get_processed(self, key, processors):
        if key in processors:
            return processors[key](self._binds[key][2])
        else:
            return self._binds[key][2]
            
    def keys(self):
        return self._binds.keys()

    def __iter__(self):
        return iter(self.keys())
        
    def __getitem__(self, key):
        (bind, name, value) = self._binds[key]
        processor = bind.bind_processor(self.dialect)
        if processor is not None:
            return processor(value)
        else:
            return value
 
    def __contains__(self, key):
        return key in self._binds
    
    def set_value(self, key, value):
        self._binds[key][2] = value
            
    def get_original_dict(self):
        return dict([(name, value) for (b, name, value) in self._binds.values()])

    def get_raw_list(self, processors):
        binds, res = self._binds, []
        for key in self.positional:
            if key in processors:
                res.append(processors[key](binds[key][2]))
            else:
                res.append(binds[key][2])
        return res

    def get_raw_dict(self, processors, encode_keys=False):
        binds, res = self._binds, {}
        if encode_keys:
            encoding = self.dialect.encoding
            for key in self.keys():
                if key in processors:
                    res[key.encode(encoding)] = processors[key](binds[key][2])
                else:
                    res[key.encode(encoding)] = binds[key][2]
        else:
            for key in self.keys():
                if key in processors:
                    res[key] = processors[key](binds[key][2])
                else:
                    res[key] = binds[key][2]
        return res
        
    def __repr__(self):
        return self.__class__.__name__ + ":" + repr(self.get_original_dict())



class TableCollection(object):
    def __init__(self, tables=None):
        self.tables = tables or []

    def __len__(self):
        return len(self.tables)

    def __getitem__(self, i):
        return self.tables[i]

    def __iter__(self):
        return iter(self.tables)

    def __contains__(self, obj):
        return obj in self.tables

    def __add__(self, obj):
        return self.tables + list(obj)

    def add(self, table):
        self.tables.append(table)
        if hasattr(self, '_sorted'):
            del self._sorted

    def sort(self, reverse=False):
        try:
            sorted = self._sorted
        except AttributeError, e:
            self._sorted = self._do_sort()
            sorted = self._sorted
        if reverse:
            x = sorted[:]
            x.reverse()
            return x
        else:
            return sorted

    def _do_sort(self):
        tuples = []
        class TVisitor(schema.SchemaVisitor):
            def visit_foreign_key(_self, fkey):
                if fkey.use_alter:
                    return
                parent_table = fkey.column.table
                if parent_table in self:
                    child_table = fkey.parent.table
                    tuples.append( ( parent_table, child_table ) )
        vis = TVisitor()
        for table in self.tables:
            vis.traverse(table)
        sorter = topological.QueueDependencySorter( tuples, self.tables )
        head =  sorter.sort()
        sequence = []
        def to_sequence( node, seq=sequence):
            seq.append( node.item )
            for child in node.children:
                to_sequence( child )
        if head is not None:
            to_sequence( head )
        return sequence


class TableFinder(TableCollection, visitors.NoColumnVisitor):
    """locate all Tables within a clause."""

    def __init__(self, clause, check_columns=False, include_aliases=False):
        TableCollection.__init__(self)
        self.check_columns = check_columns
        self.include_aliases = include_aliases
        for clause in util.to_list(clause):
            self.traverse(clause)

    def visit_alias(self, alias):
        if self.include_aliases:
            self.tables.append(alias)
            
    def visit_table(self, table):
        self.tables.append(table)

    def visit_column(self, column):
        if self.check_columns:
            self.tables.append(column.table)

class ColumnFinder(visitors.ClauseVisitor):
    def __init__(self):
        self.columns = util.Set()

    def visit_column(self, c):
        self.columns.add(c)

    def __iter__(self):
        return iter(self.columns)

class ColumnsInClause(visitors.ClauseVisitor):
    """Given a selectable, visit clauses and determine if any columns
    from the clause are in the selectable.
    """

    def __init__(self, selectable):
        self.selectable = selectable
        self.result = False

    def visit_column(self, column):
        if self.selectable.c.get(column.key) is column:
            self.result = True

class AbstractClauseProcessor(visitors.NoColumnVisitor):
    """Traverse a clause and attempt to convert the contents of container elements
    to a converted element.

    The conversion operation is defined by subclasses.
    """

    def convert_element(self, elem):
        """Define the *conversion* method for this ``AbstractClauseProcessor``."""

        raise NotImplementedError()

    def copy_and_process(self, list_):
        """Copy the container elements in the given list to a new list and
        process the new list.
        """

        list_ = list(list_)
        self.process_list(list_)
        return list_

    def process_list(self, list_):
        """Process all elements of the given list in-place."""

        for i in range(0, len(list_)):
            elem = self.convert_element(list_[i])
            if elem is not None:
                list_[i] = elem
            else:
                list_[i] = self.traverse(list_[i], clone=True)
    
    def visit_grouping(self, grouping):
        elem = self.convert_element(grouping.elem)
        if elem is not None:
            grouping.elem = elem
            
    def visit_clauselist(self, clist):
        for i in range(0, len(clist.clauses)):
            n = self.convert_element(clist.clauses[i])
            if n is not None:
                clist.clauses[i] = n
    
    def visit_unary(self, unary):
        elem = self.convert_element(unary.element)
        if elem is not None:
            unary.element = elem
            
    def visit_binary(self, binary):
        elem = self.convert_element(binary.left)
        if elem is not None:
            binary.left = elem
        elem = self.convert_element(binary.right)
        if elem is not None:
            binary.right = elem
    
    def visit_join(self, join):
        elem = self.convert_element(join.left)
        if elem is not None:
            join.left = elem
        elem = self.convert_element(join.right)
        if elem is not None:
            join.right = elem
        join._init_primary_key()
            
    def visit_select(self, select):
        fr = util.OrderedSet()
        for elem in select._froms:
            n = self.convert_element(elem)
            if n is not None:
                fr.add((elem, n))
        select._recorrelate_froms(fr)

        col = []
        for elem in select._raw_columns:
            n = self.convert_element(elem)
            if n is None:
                col.append(elem)
            else:
                col.append(n)
        select._raw_columns = col
    
class ClauseAdapter(AbstractClauseProcessor):
    """Given a clause (like as in a WHERE criterion), locate columns
    which are embedded within a given selectable, and changes those
    columns to be that of the selectable.

    E.g.::

      table1 = Table('sometable', metadata,
          Column('col1', Integer),
          Column('col2', Integer)
          )
      table2 = Table('someothertable', metadata,
          Column('col1', Integer),
          Column('col2', Integer)
          )

      condition = table1.c.col1 == table2.c.col1

    and make an alias of table1::

      s = table1.alias('foo')

    calling ``ClauseAdapter(s).traverse(condition)`` converts
    condition to read::

      s.c.col1 == table2.c.col1
    """

    def __init__(self, selectable, include=None, exclude=None, equivalents=None):
        self.selectable = selectable
        self.include = include
        self.exclude = exclude
        self.equivalents = equivalents

    def convert_element(self, col):
        if isinstance(col, expression.FromClause):
            if self.selectable.is_derived_from(col):
                return self.selectable
        if not isinstance(col, expression.ColumnElement):
            return None
        if self.include is not None:
            if col not in self.include:
                return None
        if self.exclude is not None:
            if col in self.exclude:
                return None
        newcol = self.selectable.corresponding_column(col, raiseerr=False, require_embedded=True, keys_ok=False)
        if newcol is None and self.equivalents is not None and col in self.equivalents:
            for equiv in self.equivalents[col]:
                newcol = self.selectable.corresponding_column(equiv, raiseerr=False, require_embedded=True, keys_ok=False)
                if newcol:
                    return newcol
        #if newcol is None:
        #    self.traverse(col)
        #    return col
        return newcol


