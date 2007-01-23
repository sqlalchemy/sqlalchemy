from sqlalchemy import sql, util, schema, topological

"""utility functions that build upon SQL and Schema constructs"""


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
            table.accept_schema_visitor(vis)
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
        

class TableFinder(TableCollection, sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self, table, check_columns=False):
        TableCollection.__init__(self)
        self.check_columns = check_columns
        if table is not None:
            table.accept_visitor(self)
    def visit_table(self, table):
        self.tables.append(table)
    def visit_column(self, column):
        if self.check_columns:
            column.table.accept_visitor(self)

class ColumnFinder(sql.ClauseVisitor):
    def __init__(self):
        self.columns = util.Set()
    def visit_column(self, c):
        self.columns.add(c)
    def __iter__(self):
        return iter(self.columns)
            
class Aliasizer(sql.ClauseVisitor):
    """converts a table instance within an expression to be an alias of that table."""
    def __init__(self, *tables, **kwargs):
        self.tables = {}
        self.aliases = kwargs.get('aliases', {})
        for t in tables:
            self.tables[t] = t
            if not self.aliases.has_key(t):
                self.aliases[t] = sql.alias(t)
            if isinstance(t, sql.Join):
                for t2 in t.columns:
                    self.tables[t2.table] = t2
                    self.aliases[t2.table] = self.aliases[t]
        self.binary = None
    def get_alias(self, table):
        return self.aliases[table]
    def visit_compound(self, compound):
        self.visit_clauselist(compound)
    def visit_clauselist(self, clist):
        for i in range(0, len(clist.clauses)):
            if isinstance(clist.clauses[i], schema.Column) and self.tables.has_key(clist.clauses[i].table):
                orig = clist.clauses[i]
                clist.clauses[i] = self.get_alias(clist.clauses[i].table).corresponding_column(clist.clauses[i])
    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and self.tables.has_key(binary.left.table):
            binary.left = self.get_alias(binary.left.table).corresponding_column(binary.left)
        if isinstance(binary.right, schema.Column) and self.tables.has_key(binary.right.table):
            binary.right = self.get_alias(binary.right.table).corresponding_column(binary.right)


class ClauseAdapter(sql.ClauseVisitor):
    """given a clause (like as in a WHERE criterion), locates columns which 'correspond' to a given selectable, 
    and changes those columns to be that of the selectable.
    
        such as:
        
        table1 = Table('sometable', metadata, 
            Column('col1', Integer),
            Column('col2', Integer)
            )
        table2 = Table('someothertable', metadata, 
            Column('col1', Integer),
            Column('col2', Integer)
            )
    
        condition = table1.c.col1 == table2.c.col1
        
        and make an alias of table1:
        
        s = table1.alias('foo')
        
        calling condition.accept_visitor(ClauseAdapter(s)) converts condition to read:
        
        s.c.col1 == table2.c.col1
    
    """
    def __init__(self, selectable):
        self.selectable = selectable
    def visit_binary(self, binary):
        if isinstance(binary.left, sql.ColumnElement):
            col = self.selectable.corresponding_column(binary.left, raiseerr=False, keys_ok=False)
            if col is not None:
                binary.left = col
        if isinstance(binary.right, sql.ColumnElement):
            col = self.selectable.corresponding_column(binary.right, raiseerr=False, keys_ok=False)
            if col is not None:
                binary.right = col

class ColumnsInClause(sql.ClauseVisitor):
    """given a selectable, visits clauses and determines if any columns from the clause are in the selectable"""
    def __init__(self, selectable):
        self.selectable = selectable
        self.result = False
    def visit_column(self, column):
        if self.selectable.c.get(column.key) is column:
            self.result = True
