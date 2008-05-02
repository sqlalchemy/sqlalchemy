from sqlalchemy import exceptions, schema, topological, util, sql
from sqlalchemy.sql import expression, operators, visitors
from itertools import chain

"""Utility functions that build upon SQL and Schema constructs."""

def sort_tables(tables, reverse=False):
    """sort a collection of Table objects in order of their foreign-key dependency."""
    
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
    """locate Table objects within the given expression."""
    
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
    """locate Column objects within the given expression."""
    
    cols = util.Set()
    def visit_column(col):
        cols.add(col)
    visitors.traverse(clause, visit_column=visit_column)
    return cols

def join_condition(a, b, ignore_nonexistent_tables=False):
    """create a join condition between two tables.
    
    ignore_nonexistent_tables=True allows a join condition to be
    determined between two tables which may contain references to
    other not-yet-defined tables.  In general the NoSuchTableError
    raised is only required if the user is trying to join selectables
    across multiple MetaData objects (which is an extremely rare use 
    case).
    
    """
    crit = []
    constraints = util.Set()
    for fk in b.foreign_keys:
        try:
            col = fk.get_referent(a)
        except exceptions.NoReferencedTableError:
            if ignore_nonexistent_tables:
                continue
            else:
                raise
                
        if col:
            crit.append(col == fk.parent)
            constraints.add(fk.constraint)

    if a is not b:
        for fk in a.foreign_keys:
            try:
                col = fk.get_referent(b)
            except exceptions.NoReferencedTableError:
                if ignore_nonexistent_tables:
                    continue
                else:
                    raise
            
            if col:
                crit.append(col == fk.parent)
                constraints.add(fk.constraint)

    if len(crit) == 0:
        raise exceptions.ArgumentError(
            "Can't find any foreign key relationships "
            "between '%s' and '%s'" % (a.description, b.description))
    elif len(constraints) > 1:
        raise exceptions.ArgumentError(
            "Can't determine join between '%s' and '%s'; "
            "tables have more than one foreign key "
            "constraint relationship between them. "
            "Please specify the 'onclause' of this "
            "join explicitly." % (a.description, b.description))
    elif len(crit) == 1:
        return (crit[0])
    else:
        return sql.and_(*crit)
    
    
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

def criterion_as_pairs(expression, consider_as_foreign_keys=None, consider_as_referenced_keys=None, any_operator=False):
    """traverse an expression and locate binary criterion pairs."""
    
    if consider_as_foreign_keys and consider_as_referenced_keys:
        raise exceptions.ArgumentError("Can only specify one of 'consider_as_foreign_keys' or 'consider_as_referenced_keys'")
        
    def visit_binary(binary):
        if not any_operator and binary.operator != operators.eq:
            return
        if not isinstance(binary.left, sql.ColumnElement) or not isinstance(binary.right, sql.ColumnElement):
            return

        if consider_as_foreign_keys:
            if binary.left in consider_as_foreign_keys:
                pairs.append((binary.right, binary.left))
            elif binary.right in consider_as_foreign_keys:
                pairs.append((binary.left, binary.right))
        elif consider_as_referenced_keys:
            if binary.left in consider_as_referenced_keys:
                pairs.append((binary.left, binary.right))
            elif binary.right in consider_as_referenced_keys:
                pairs.append((binary.right, binary.left))
        else:
            if isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column):
                if binary.left.references(binary.right):
                    pairs.append((binary.right, binary.left))
                elif binary.right.references(binary.left):
                    pairs.append((binary.left, binary.right))
    pairs = []
    visitors.traverse(expression, visit_binary=visit_binary)
    return pairs

def folded_equivalents(join, equivs=None):
    """Returns the column list of the given Join with all equivalently-named,
    equated columns folded into one column, where 'equated' means they are
    equated to each other in the ON clause of this join.

    This function is used by Join.select(fold_equivalents=True).
    
    TODO: deprecate ?
    """

    if equivs is None:
        equivs = util.Set()
    def visit_binary(binary):
        if binary.operator == operators.eq and binary.left.name == binary.right.name:
            equivs.add(binary.right)
            equivs.add(binary.left)
    visitors.traverse(join.onclause, visit_binary=visit_binary)
    collist = []
    if isinstance(join.left, expression.Join):
        left = folded_equivalents(join.left, equivs)
    else:
        left = list(join.left.columns)
    if isinstance(join.right, expression.Join):
        right = folded_equivalents(join.right, equivs)
    else:
        right = list(join.right.columns)
    used = util.Set()
    for c in left + right:
        if c in equivs:
            if c.name not in used:
                collist.append(c)
                used.add(c.name)
        else:
            collist.append(c)
    return collist

class AliasedRow(object):
    
    def __init__(self, row, map):
        # AliasedRow objects don't nest, so un-nest
        # if another AliasedRow was passed
        if isinstance(row, AliasedRow):
            self.row = row.row
        else:
            self.row = row
        self.map = map
        
    def __contains__(self, key):
        return self.map[key] in self.row

    def has_key(self, key):
        return key in self

    def __getitem__(self, key):
        return self.row[self.map[key]]

    def keys(self):
        return self.row.keys()

def row_adapter(from_, equivalent_columns=None):
    """create a row adapter callable against a selectable."""
    
    if equivalent_columns is None:
        equivalent_columns = {}

    def locate_col(col):
        c = from_.corresponding_column(col)
        if c:
            return c
        elif col in equivalent_columns:
            for c2 in equivalent_columns[col]:
                corr = from_.corresponding_column(c2)
                if corr:
                    return corr
        return col
        
    map = util.PopulateDict(locate_col)
    
    def adapt(row):
        return AliasedRow(row, map)
    return adapt

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
