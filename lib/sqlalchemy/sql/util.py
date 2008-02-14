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

class AbstractClauseProcessor(object):
    """Traverse and copy a ClauseElement, replacing selected elements based on rules.

    This class implements its own visit-and-copy strategy but maintains the
    same public interface as visitors.ClauseVisitor.
    """

    __traverse_options__ = {'column_collections':False}

    def __init__(self, stop_on=None):
        self.stop_on = stop_on

    def convert_element(self, elem):
        """Define the *conversion* method for this ``AbstractClauseProcessor``."""

        raise NotImplementedError()

    def chain(self, visitor):
        # chaining AbstractClauseProcessor and other ClauseVisitor
        # objects separately.  All the ACP objects are chained on
        # their convert_element() method whereas regular visitors
        # chain on their visit_XXX methods.
        if isinstance(visitor, AbstractClauseProcessor):
            attr = '_next_acp'
        else:
            attr = '_next'

        tail = self
        while getattr(tail, attr, None) is not None:
            tail = getattr(tail, attr)
        setattr(tail, attr, visitor)
        return self

    def copy_and_process(self, list_):
        """Copy the given list to a new list, with each element traversed individually."""

        list_ = list(list_)
        stop_on = util.Set(self.stop_on or [])
        cloned = {}
        for i in range(0, len(list_)):
            list_[i] = self._traverse(list_[i], stop_on, cloned, _clone_toplevel=True)
        return list_

    def _convert_element(self, elem, stop_on, cloned):
        v = self
        while v is not None:
            newelem = v.convert_element(elem)
            if newelem:
                stop_on.add(newelem)
                return newelem
            v = getattr(v, '_next_acp', None)

        if elem not in cloned:
            # the full traversal will only make a clone of a particular element
            # once.
            cloned[elem] = elem._clone()
        return cloned[elem]

    def traverse(self, elem, clone=True):
        if not clone:
            raise exceptions.ArgumentError("AbstractClauseProcessor 'clone' argument must be True")

        return self._traverse(elem, util.Set(self.stop_on or []), {}, _clone_toplevel=True)

    def _traverse(self, elem, stop_on, cloned, _clone_toplevel=False):
        if elem in stop_on:
            return elem

        if _clone_toplevel:
            elem = self._convert_element(elem, stop_on, cloned)
            if elem in stop_on:
                return elem

        def clone(element):
            return self._convert_element(element, stop_on, cloned)
        elem._copy_internals(clone=clone)

        v = getattr(self, '_next', None)
        while v is not None:
            meth = getattr(v, "visit_%s" % elem.__visit_name__, None)
            if meth:
                meth(elem)
            v = getattr(v, '_next', None)

        for e in elem.get_children(**self.__traverse_options__):
            if e not in stop_on:
                self._traverse(e, stop_on, cloned)
        return elem

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
        AbstractClauseProcessor.__init__(self, [selectable])
        self.selectable = selectable
        self.include = include
        self.exclude = exclude
        self.equivalents = equivalents

    def copy_and_chain(self, adapter):
        """create a copy of this adapter and chain to the given adapter.

        currently this adapter must be unchained to start, raises
        an exception if it's already chained.

        Does not modify the given adapter.
        """

        if adapter is None:
            return self

        if hasattr(self, '_next_acp') or hasattr(self, '_next'):
            raise NotImplementedError("Can't chain_to on an already chained ClauseAdapter (yet)")

        ca = ClauseAdapter(self.selectable, self.include, self.exclude, self.equivalents)
        ca._next_acp = adapter
        return ca

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
        newcol = self.selectable.corresponding_column(col, require_embedded=True)
        if newcol is None and self.equivalents is not None and col in self.equivalents:
            for equiv in self.equivalents[col]:
                newcol = self.selectable.corresponding_column(equiv, require_embedded=True)
                if newcol:
                    return newcol
        return newcol
