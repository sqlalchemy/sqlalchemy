from sqlalchemy import exc, schema, topological, util, sql
from sqlalchemy.sql import expression, operators, visitors
from itertools import chain

"""Utility functions that build upon SQL and Schema constructs."""

def sort_tables(tables, reverse=False):
    """sort a collection of Table objects in order of their foreign-key dependency."""
    
    tuples = []
    def visit_foreign_key(fkey):
        if fkey.use_alter:
            return
        parent_table = fkey.column.table
        if parent_table in tables:
            child_table = fkey.parent.table
            tuples.append( ( parent_table, child_table ) )

    for table in tables:
        visitors.traverse(table, {'schema_visitor':True}, {'foreign_key':visit_foreign_key})    
    sequence = topological.sort(tuples, tables)
    if reverse:
        return util.reversed(sequence)
    else:
        return sequence

def search(clause, target):
    if not clause:
        return False
    for elem in visitors.iterate(clause, {'column_collections':False}):
        if elem is target:
            return True
    else:
        return False

def find_tables(clause, check_columns=False, include_aliases=False, include_joins=False, include_selects=False):
    """locate Table objects within the given expression."""
    
    tables = []
    _visitors = {}
    
    def visit_something(elem):
        tables.append(elem)
        
    if include_selects:
        _visitors['select'] = _visitors['compound_select'] = visit_something
    
    if include_joins:
        _visitors['join'] = visit_something
        
    if include_aliases:
        _visitors['alias']  = visit_something

    if check_columns:
        def visit_column(column):
            tables.append(column.table)
        _visitors['column'] = visit_column

    _visitors['table'] = visit_something

    visitors.traverse(clause, {'column_collections':False}, _visitors)
    return tables

def find_columns(clause):
    """locate Column objects within the given expression."""
    
    cols = util.Set()
    def visit_column(col):
        cols.add(col)
    visitors.traverse(clause, {}, {'column':visit_column})
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
        except exc.NoReferencedTableError:
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
            except exc.NoReferencedTableError:
                if ignore_nonexistent_tables:
                    continue
                else:
                    raise

            if col:
                crit.append(col == fk.parent)
                constraints.add(fk.constraint)

    if len(crit) == 0:
        raise exc.ArgumentError(
            "Can't find any foreign key relationships "
            "between '%s' and '%s'" % (a.description, b.description))
    elif len(constraints) > 1:
        raise exc.ArgumentError(
            "Can't determine join between '%s' and '%s'; "
            "tables have more than one foreign key "
            "constraint relationship between them. "
            "Please specify the 'onclause' of this "
            "join explicitly." % (a.description, b.description))
    elif len(crit) == 1:
        return (crit[0])
    else:
        return sql.and_(*crit)

class Annotated(object):
    """clones a ClauseElement and applies an 'annotations' dictionary.
    
    Unlike regular clones, this clone also mimics __hash__() and 
    __cmp__() of the original element so that it takes its place
    in hashed collections.
    
    A reference to the original element is maintained, for the important
    reason of keeping its hash value current.  When GC'ed, the 
    hash value may be reused, causing conflicts.

    """
    def __new__(cls, *args):
        if not args:
            return object.__new__(cls)
        else:
            element, values = args
            return object.__new__(
                type.__new__(type, "Annotated%s" % element.__class__.__name__, (Annotated, element.__class__), {}) 
            )

    def __init__(self, element, values):
        # force FromClause to generate their internal 
        # collections into __dict__
        if isinstance(element, expression.FromClause):
            element.c
        
        self.__dict__ = element.__dict__.copy()
        self.__element = element
        self._annotations = values

    def _annotate(self, values):
        _values = self._annotations.copy()
        _values.update(values)
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__ = self.__dict__.copy()
        clone._annotations = _values
        return clone
        
    def __hash__(self):
        return hash(self.__element)

    def __cmp__(self, other):
        return cmp(hash(self.__element), hash(other))

def splice_joins(left, right, stop_on=None):
    if left is None:
        return right
        
    stack = [(right, None)]

    adapter = ClauseAdapter(left)
    ret = None
    while stack:
        (right, prevright) = stack.pop()
        if isinstance(right, expression.Join) and right is not stop_on:
            right = right._clone()
            right._reset_exported()
            right.onclause = adapter.traverse(right.onclause)
            stack.append((right.left, right))
        else:
            right = adapter.traverse(right)
        if prevright:
            prevright.left = right
        if not ret:
            ret = right

    return ret
    
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
            visitors.traverse(clause, {}, {'binary':visit_binary})

    return expression.ColumnSet(columns.difference(omit))

def criterion_as_pairs(expression, consider_as_foreign_keys=None, consider_as_referenced_keys=None, any_operator=False):
    """traverse an expression and locate binary criterion pairs."""
    
    if consider_as_foreign_keys and consider_as_referenced_keys:
        raise exc.ArgumentError("Can only specify one of 'consider_as_foreign_keys' or 'consider_as_referenced_keys'")
        
    def visit_binary(binary):
        if not any_operator and binary.operator is not operators.eq:
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
    visitors.traverse(expression, {}, {'binary':visit_binary})
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
    visitors.traverse(join.onclause, {}, {'binary':visit_binary})
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


class ClauseAdapter(visitors.ReplacingCloningVisitor):
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
    def __init__(self, selectable, equivalents=None, include=None, exclude=None):
        self.__traverse_options__ = {'column_collections':False, 'stop_on':[selectable]}
        self.selectable = selectable
        self.include = include
        self.exclude = exclude
        self.equivalents = equivalents or {}
        
    def _corresponding_column(self, col, require_embedded):
        newcol = self.selectable.corresponding_column(col, require_embedded=require_embedded)

        if not newcol and col in self.equivalents:
            for equiv in self.equivalents[col]:
                newcol = self.selectable.corresponding_column(equiv, require_embedded=require_embedded)
                if newcol:
                    return newcol
        return newcol

    def replace(self, col):
        if isinstance(col, expression.FromClause):
            if self.selectable.is_derived_from(col):
                return self.selectable

        if not isinstance(col, expression.ColumnElement):
            return None

        if self.include and col not in self.include:
            return None
        elif self.exclude and col in self.exclude:
            return None

        return self._corresponding_column(col, True)

class ColumnAdapter(ClauseAdapter):

    def __init__(self, selectable, equivalents=None, chain_to=None, include=None, exclude=None):
        ClauseAdapter.__init__(self, selectable, equivalents, include, exclude)
        if chain_to:
            self.chain(chain_to)
        self.columns = util.PopulateDict(self._locate_col)

    def wrap(self, adapter):
        ac = self.__class__.__new__(self.__class__)
        ac.__dict__ = self.__dict__.copy()
        ac._locate_col = ac._wrap(ac._locate_col, adapter._locate_col)
        ac.adapt_clause = ac._wrap(ac.adapt_clause, adapter.adapt_clause)
        ac.adapt_list = ac._wrap(ac.adapt_list, adapter.adapt_list)
        ac.columns = util.PopulateDict(ac._locate_col)
        return ac

    adapt_clause = ClauseAdapter.traverse
    adapt_list = ClauseAdapter.copy_and_process

    def _wrap(self, local, wrapped):
        def locate(col):
            col = local(col)
            return wrapped(col)
        return locate

    def _locate_col(self, col):
        c = self._corresponding_column(col, False)
        if not c:
            c = self.adapt_clause(col)
            
            # anonymize labels in case they have a hardcoded name
            if isinstance(c, expression._Label):
                c = c.label(None)
        return c    

    def adapted_row(self, row):
        return AliasedRow(row, self.columns)
    
