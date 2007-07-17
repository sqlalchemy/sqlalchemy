from sqlalchemy import sql, util, schema, topological

"""Utility functions that build upon SQL and Schema constructs."""

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
        head = sorter.sort()
        sequence = []
        def to_sequence( node, seq=sequence):
            seq.append( node.item )
            for child in node.children:
                to_sequence( child )
        if head is not None:
            to_sequence( head )
        return sequence


class TableFinder(TableCollection, sql.NoColumnVisitor):
    """locate all Tables within a clause."""

    def __init__(self, table, check_columns=False, include_aliases=False):
        TableCollection.__init__(self)
        self.check_columns = check_columns
        self.include_aliases = include_aliases
        if table is not None:
            self.traverse(table)

    def visit_alias(self, alias):
        if self.include_aliases:
            self.tables.append(alias)
            
    def visit_table(self, table):
        self.tables.append(table)

    def visit_column(self, column):
        if self.check_columns:
            self.traverse(column.table)

class ColumnFinder(sql.ClauseVisitor):
    def __init__(self):
        self.columns = util.Set()

    def visit_column(self, c):
        self.columns.add(c)

    def __iter__(self):
        return iter(self.columns)

class ColumnsInClause(sql.ClauseVisitor):
    """Given a selectable, visit clauses and determine if any columns
    from the clause are in the selectable.
    """

    def __init__(self, selectable):
        self.selectable = selectable
        self.result = False

    def visit_column(self, column):
        if self.selectable.c.get(column.key) is column:
            self.result = True

class AbstractClauseProcessor(sql.NoColumnVisitor):
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

        list_ = [o.copy_container() for o in list_]
        self.process_list(list_)
        return list_

    def process_list(self, list_):
        """Process all elements of the given list in-place."""

        for i in range(0, len(list_)):
            elem = self.convert_element(list_[i])
            if elem is not None:
                list_[i] = elem
            else:
                self.traverse(list_[i])
    
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

    # TODO: visit_select().  
    
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
        if not isinstance(col, sql.ColumnElement):
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
        return newcol
