from sqlalchemy import util, schema, topological
from sqlalchemy.sql import expression, visitors

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


