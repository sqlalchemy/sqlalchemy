from sqlalchemy import exceptions, schema, topological, util
from sqlalchemy.sql import expression, operators, visitors
from itertools import chain

"""Utility functions that build upon SQL and Schema constructs."""

def sort_tables(tables, reverse=False):
    tuples = []
    class TVisitor(schema.SchemaVisitor):
        def visit_foreign_key(_self, fkey):
            if fkey.use_alter:
                return
            parent_table = fkey.column.table
            if parent_table in tables:
                child_table = fkey.parent.table
                tuples.append( ( parent_table, child_table ) )
    vis = TVisitor()
    for table in tables:
        vis.traverse(table)
    sequence = topological.sort(tuples, tables)
    if reverse:
        return util.reversed(sequence)
    else:
        return sequence

def find_tables(clause, check_columns=False, include_aliases=False):
    tables = []
    kwargs = {}
    if include_aliases:
        def visit_alias(alias):
            tables.append(alias)
        kwargs['visit_alias']  = visit_alias

    if check_columns:
        def visit_column(column):
            tables.append(column.table)
        kwargs['visit_column'] = visit_column

    def visit_table(table):
        tables.append(table)
    kwargs['visit_table'] = visit_table

    visitors.traverse(clause, traverse_options= {'column_collections':False}, **kwargs)
    return tables

def find_columns(clause):
    cols = util.Set()
    def visit_column(col):
        cols.add(col)
    visitors.traverse(clause, visit_column=visit_column)
    return cols


def reduce_columns(columns, *clauses):
    """given a list of columns, return a 'reduced' set based on natural equivalents.

    the set is reduced to the smallest list of columns which have no natural
    equivalent present in the list.  A "natural equivalent" means that two columns
    will ultimately represent the same value because they are related by a foreign key.

    \*clauses is an optional list of join clauses which will be traversed
    to further identify columns that are "equivalent".

    This function is primarily used to determine the most minimal "primary key"
    from a selectable, by reducing the set of primary key columns present
    in the the selectable to just those that are not repeated.

    """

    columns = util.OrderedSet(columns)

    omit = util.Set()
    for col in columns:
        for fk in col.foreign_keys:
            for c in columns:
                if c is col:
                    continue
                if fk.column.shares_lineage(c):
                    omit.add(col)
                    break

    if clauses:
        def visit_binary(binary):
            if binary.operator == operators.eq:
                cols = util.Set(chain(*[c.proxy_set for c in columns.difference(omit)]))
                if binary.left in cols and binary.right in cols:
                    for c in columns:
                        if c.shares_lineage(binary.right):
                            omit.add(c)
                            break
        for clause in clauses:
            visitors.traverse(clause, visit_binary=visit_binary)

    return expression.ColumnSet(columns.difference(omit))

def row_adapter(from_, to, equivalent_columns=None):
    """create a row adapter between two selectables.

    The returned adapter is a class that can be instantiated repeatedly for any number
    of rows; this is an inexpensive process.  However, the creation of the row
    adapter class itself *is* fairly expensive so caching should be used to prevent
    repeated calls to this function.
    """

    map = {}
    for c in to.c:
        corr = from_.corresponding_column(c)
        if corr:
            map[c] = corr
        elif equivalent_columns:
            if c in equivalent_columns:
                for c2 in equivalent_columns[c]:
                    corr = from_.corresponding_column(c2)
                    if corr:
                        map[c] = corr
                        break

    class AliasedRow(object):
        def __init__(self, row):
            self.row = row
        def __contains__(self, key):
            if key in map:
                return map[key] in self.row
            else:
                return key in self.row
        def has_key(self, key):
            return key in self
        def __getitem__(self, key):
            if key in map:
                key = map[key]
            return self.row[key]
        def keys(self):
            return map.keys()
    AliasedRow.map = map
    return AliasedRow

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

class ClauseAdapter(visitors.ClauseVisitor):
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

    __traverse_options__ = {'column_collections':False}

    def __init__(self, selectable, include=None, exclude=None, equivalents=None):
        self.__traverse_options__ = self.__traverse_options__.copy()
        self.__traverse_options__['stop_on'] = [selectable]
        self.selectable = selectable
        self.include = include
        self.exclude = exclude
        self.equivalents = equivalents
    
    def traverse(self, obj, clone=True):
        if not clone:
            raise exceptions.ArgumentError("ClauseAdapter 'clone' argument must be True")
        return visitors.ClauseVisitor.traverse(self, obj, clone=True)
        
    def copy_and_chain(self, adapter):
        """create a copy of this adapter and chain to the given adapter.

        currently this adapter must be unchained to start, raises
        an exception if it's already chained.

        Does not modify the given adapter.
        """

        if adapter is None:
            return self

        if hasattr(self, '_next'):
            raise NotImplementedError("Can't chain_to on an already chained ClauseAdapter (yet)")

        ca = ClauseAdapter(self.selectable, self.include, self.exclude, self.equivalents)
        ca._next = adapter
        return ca

    def before_clone(self, col):
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
        newcol = self.selectable.corresponding_column(col, require_embedded=True)
        if newcol is None and self.equivalents is not None and col in self.equivalents:
            for equiv in self.equivalents[col]:
                newcol = self.selectable.corresponding_column(equiv, require_embedded=True)
                if newcol:
                    return newcol
        return newcol
