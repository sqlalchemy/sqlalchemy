import sqlalchemy.sql as sql
import sqlalchemy.schema as schema

"""utility functions that build upon SQL and Schema constructs"""


class TableCollection(object):
    def __init__(self):
        self.tables = []
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
        import sqlalchemy.orm.topological
        tuples = []
        class TVisitor(schema.SchemaVisitor):
            def visit_foreign_key(self, fkey):
                parent_table = fkey.column.table
                child_table = fkey.parent.table
                tuples.append( ( parent_table, child_table ) )
        vis = TVisitor()        
        for table in self.tables:
            table.accept_schema_visitor(vis)
        sorter = sqlalchemy.orm.topological.QueueDependencySorter( tuples, self.tables )
        head =  sorter.sort()
        sequence = []
        def to_sequence( node, seq=sequence):
            seq.append( node.item )
            for child in node.children:
                to_sequence( child )
        if head is not None:
            to_sequence( head )
        return sequence
        

class TableFinder(TableCollection, sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self, table, check_columns=False):
        TableCollection.__init__(self)
        self.check_columns = check_columns
        if table is not None:
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
        return self.tables + list(obj)
    def visit_column(self, column):
        if self.check_columns:
            column.table.accept_visitor(self)

