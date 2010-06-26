from sqlalchemy import exc, schema, topological, util, sql, types as sqltypes
from sqlalchemy.sql import expression, operators, visitors
from itertools import chain

"""Utility functions that build upon SQL and Schema constructs."""

def sort_tables(tables):
    """sort a collection of Table objects in order of their foreign-key dependency."""
    
    tables = list(tables)
    tuples = []
    def visit_foreign_key(fkey):
        if fkey.use_alter:
            return
        parent_table = fkey.column.table
        if parent_table in tables:
            child_table = fkey.parent.table
            if parent_table is not child_table:
                tuples.append((parent_table, child_table))

    for table in tables:
        visitors.traverse(table, 
                            {'schema_visitor':True}, 
                            {'foreign_key':visit_foreign_key})

        tuples.extend(
            [parent, table] for parent in table._extra_dependencies
        )
                            
    return list(topological.sort(tuples, tables))

def find_join_source(clauses, join_to):
    """Given a list of FROM clauses and a selectable, 
    return the first index and element from the list of 
    clauses which can be joined against the selectable.  returns 
    None, None if no match is found.
    
    e.g.::
    
        clause1 = table1.join(table2)
        clause2 = table4.join(table5)
        
        join_to = table2.join(table3)
        
        find_join_source([clause1, clause2], join_to) == clause1
    
    """
    
    selectables = list(expression._from_objects(join_to))
    for i, f in enumerate(clauses):
        for s in selectables:
            if f.is_derived_from(s):
                return i, f
    else:
        return None, None

def find_tables(clause, check_columns=False, 
                include_aliases=False, include_joins=False, 
                include_selects=False, include_crud=False):
    """locate Table objects within the given expression."""
    
    tables = []
    _visitors = {}
    
    if include_selects:
        _visitors['select'] = _visitors['compound_select'] = tables.append
    
    if include_joins:
        _visitors['join'] = tables.append
        
    if include_aliases:
        _visitors['alias']  = tables.append
    
    if include_crud:
        _visitors['insert'] = _visitors['update'] = \
                    _visitors['delete'] = lambda ent: tables.append(ent.table)
        
    if check_columns:
        def visit_column(column):
            tables.append(column.table)
        _visitors['column'] = visit_column

    _visitors['table'] = tables.append

    visitors.traverse(clause, {'column_collections':False}, _visitors)
    return tables

def find_columns(clause):
    """locate Column objects within the given expression."""
    
    cols = util.column_set()
    visitors.traverse(clause, {}, {'column':cols.add})
    return cols

def _quote_ddl_expr(element):
    if isinstance(element, basestring):
        element = element.replace("'", "''")
        return "'%s'" % element
    else:
        return repr(element)
    
def expression_as_ddl(clause):
    """Given a SQL expression, convert for usage in DDL, such as 
     CREATE INDEX and CHECK CONSTRAINT.
     
     Converts bind params into quoted literals, column identifiers
     into detached column constructs so that the parent table
     identifier is not included.
    
    """
    def repl(element):
        if isinstance(element, expression._BindParamClause):
            return expression.literal_column(_quote_ddl_expr(element.value))
        elif isinstance(element, expression.ColumnClause) and \
                element.table is not None:
            return expression.column(element.name)
        else:
            return None
        
    return visitors.replacement_traverse(clause, {}, repl)
    
def adapt_criterion_to_null(crit, nulls):
    """given criterion containing bind params, convert selected elements to IS NULL."""

    def visit_binary(binary):
        if isinstance(binary.left, expression._BindParamClause) and binary.left.key in nulls:
            # reverse order if the NULL is on the left side
            binary.left = binary.right
            binary.right = expression.null()
            binary.operator = operators.is_
            binary.negate = operators.isnot
        elif isinstance(binary.right, expression._BindParamClause) and binary.right.key in nulls:
            binary.right = expression.null()
            binary.operator = operators.is_
            binary.negate = operators.isnot

    return visitors.cloned_traverse(crit, {}, {'binary':visit_binary})
    
    
def join_condition(a, b, ignore_nonexistent_tables=False, a_subset=None):
    """create a join condition between two tables or selectables.
    
    e.g.::
    
        join_condition(tablea, tableb)
        
    would produce an expression along the lines of::
    
        tablea.c.id==tableb.c.tablea_id
    
    The join is determined based on the foreign key relationships
    between the two selectables.   If there are multiple ways
    to join, or no way to join, an error is raised.
    
    :param ignore_nonexistent_tables: This flag will cause the
    function to silently skip over foreign key resolution errors
    due to nonexistent tables - the assumption is that these
    tables have not yet been defined within an initialization process
    and are not significant to the operation.

    :param a_subset: An optional expression that is a sub-component
    of ``a``.  An attempt will be made to join to just this sub-component
    first before looking at the full ``a`` construct, and if found
    will be successful even if there are other ways to join to ``a``.
    This allows the "right side" of a join to be passed thereby
    providing a "natural join".
    
    """
    crit = []
    constraints = set()
    
    for left in (a_subset, a):
        if left is None:
            continue
        for fk in b.foreign_keys:
            try:
                col = fk.get_referent(left)
            except exc.NoReferencedTableError:
                if ignore_nonexistent_tables:
                    continue
                else:
                    raise
                
            if col is not None:
                crit.append(col == fk.parent)
                constraints.add(fk.constraint)
        if left is not b:
            for fk in left.foreign_keys:
                try:
                    col = fk.get_referent(b)
                except exc.NoReferencedTableError:
                    if ignore_nonexistent_tables:
                        continue
                    else:
                        raise

                if col is not None:
                    crit.append(col == fk.parent)
                    constraints.add(fk.constraint)
        if crit:
            break
            
    if len(crit) == 0:
        if isinstance(b, expression._FromGrouping):
            hint = " Perhaps you meant to convert the right side to a subquery using alias()?"
        else:
            hint = ""
        raise exc.ArgumentError(
            "Can't find any foreign key relationships "
            "between '%s' and '%s'.%s" % (a.description, b.description, hint))
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
            # clone constructor
            return object.__new__(cls)
        else:
            element, values = args
            # pull appropriate subclass from registry of annotated
            # classes
            try:
                cls = annotated_classes[element.__class__]
            except KeyError:
                cls = annotated_classes[element.__class__] = type.__new__(type, 
                        "Annotated%s" % element.__class__.__name__, 
                        (Annotated, element.__class__), {})
            return object.__new__(cls)

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
    
    def _deannotate(self):
        return self.__element
    
    def _compiler_dispatch(self, visitor, **kw):
        return self.__element.__class__._compiler_dispatch(self, visitor, **kw)
        
    @property
    def _constructor(self):
        return self.__element._constructor
        
    def _clone(self):
        clone = self.__element._clone()
        if clone is self.__element:
            # detect immutable, don't change anything
            return self
        else:
            # update the clone with any changes that have occured
            # to this object's __dict__.
            clone.__dict__.update(self.__dict__)
            return Annotated(clone, self._annotations)
    
    def __hash__(self):
        return hash(self.__element)

    def __cmp__(self, other):
        return cmp(hash(self.__element), hash(other))

# hard-generate Annotated subclasses.  this technique
# is used instead of on-the-fly types (i.e. type.__new__())
# so that the resulting objects are pickleable.
annotated_classes = {}

for cls in expression.__dict__.values() + [schema.Column, schema.Table]:
    if isinstance(cls, type) and issubclass(cls, expression.ClauseElement):
        exec "class Annotated%s(Annotated, cls):\n" \
             "    pass" % (cls.__name__, ) in locals()
        exec "annotated_classes[cls] = Annotated%s" % (cls.__name__)

def _deep_annotate(element, annotations, exclude=None):
    """Deep copy the given ClauseElement, annotating each element with the given annotations dictionary.

    Elements within the exclude collection will be cloned but not annotated.

    """
    def clone(elem):
        # check if element is present in the exclude list.
        # take into account proxying relationships.
        if exclude and \
                    hasattr(elem, 'proxy_set') and \
                    elem.proxy_set.intersection(exclude):
            elem = elem._clone()
        elif annotations != elem._annotations:
            elem = elem._annotate(annotations.copy())
        elem._copy_internals(clone=clone)
        return elem

    if element is not None:
        element = clone(element)
    return element

def _deep_deannotate(element):
    """Deep copy the given element, removing all annotations."""

    def clone(elem):
        elem = elem._deannotate()
        elem._copy_internals(clone=clone)
        return elem

    if element is not None:
        element = clone(element)
    return element


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
        if prevright is not None:
            prevright.left = right
        if ret is None:
            ret = right

    return ret
    
def reduce_columns(columns, *clauses, **kw):
    """given a list of columns, return a 'reduced' set based on natural equivalents.

    the set is reduced to the smallest list of columns which have no natural
    equivalent present in the list.  A "natural equivalent" means that two columns
    will ultimately represent the same value because they are related by a foreign key.

    \*clauses is an optional list of join clauses which will be traversed
    to further identify columns that are "equivalent".

    \**kw may specify 'ignore_nonexistent_tables' to ignore foreign keys
    whose tables are not yet configured.
    
    This function is primarily used to determine the most minimal "primary key"
    from a selectable, by reducing the set of primary key columns present
    in the the selectable to just those that are not repeated.

    """
    ignore_nonexistent_tables = kw.pop('ignore_nonexistent_tables', False)
    
    columns = util.ordered_column_set(columns)

    omit = util.column_set()
    for col in columns:
        for fk in chain(*[c.foreign_keys for c in col.proxy_set]):
            for c in columns:
                if c is col:
                    continue
                try:
                    fk_col = fk.column
                except exc.NoReferencedTableError:
                    if ignore_nonexistent_tables:
                        continue
                    else:
                        raise
                if fk_col.shares_lineage(c):
                    omit.add(col)
                    break

    if clauses:
        def visit_binary(binary):
            if binary.operator == operators.eq:
                cols = util.column_set(chain(*[c.proxy_set for c in columns.difference(omit)]))
                if binary.left in cols and binary.right in cols:
                    for c in columns:
                        if c.shares_lineage(binary.right):
                            omit.add(c)
                            break
        for clause in clauses:
            visitors.traverse(clause, {}, {'binary':visit_binary})

    return expression.ColumnSet(columns.difference(omit))

def criterion_as_pairs(expression, consider_as_foreign_keys=None, 
                        consider_as_referenced_keys=None, any_operator=False):
    """traverse an expression and locate binary criterion pairs."""
    
    if consider_as_foreign_keys and consider_as_referenced_keys:
        raise exc.ArgumentError("Can only specify one of "
                                "'consider_as_foreign_keys' or "
                                "'consider_as_referenced_keys'")
        
    def visit_binary(binary):
        if not any_operator and binary.operator is not operators.eq:
            return
        if not isinstance(binary.left, sql.ColumnElement) or \
                    not isinstance(binary.right, sql.ColumnElement):
            return

        if consider_as_foreign_keys:
            if binary.left in consider_as_foreign_keys and \
                        (binary.right is binary.left or 
                        binary.right not in consider_as_foreign_keys):
                pairs.append((binary.right, binary.left))
            elif binary.right in consider_as_foreign_keys and \
                        (binary.left is binary.right or 
                        binary.left not in consider_as_foreign_keys):
                pairs.append((binary.left, binary.right))
        elif consider_as_referenced_keys:
            if binary.left in consider_as_referenced_keys and \
                        (binary.right is binary.left or 
                        binary.right not in consider_as_referenced_keys):
                pairs.append((binary.left, binary.right))
            elif binary.right in consider_as_referenced_keys and \
                        (binary.left is binary.right or 
                        binary.left not in consider_as_referenced_keys):
                pairs.append((binary.right, binary.left))
        else:
            if isinstance(binary.left, schema.Column) and \
                        isinstance(binary.right, schema.Column):
                if binary.left.references(binary.right):
                    pairs.append((binary.right, binary.left))
                elif binary.right.references(binary.left):
                    pairs.append((binary.left, binary.right))
    pairs = []
    visitors.traverse(expression, {}, {'binary':visit_binary})
    return pairs

def folded_equivalents(join, equivs=None):
    """Return a list of uniquely named columns.
    
    The column list of the given Join will be narrowed 
    down to a list of all equivalently-named,
    equated columns folded into one column, where 'equated' means they are
    equated to each other in the ON clause of this join.

    This function is used by Join.select(fold_equivalents=True).
    
    Deprecated.   This function is used for a certain kind of 
    "polymorphic_union" which is designed to achieve joined
    table inheritance where the base table has no "discriminator"
    column; [ticket:1131] will provide a better way to 
    achieve this.

    """
    if equivs is None:
        equivs = set()
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
    used = set()
    for c in left + right:
        if c in equivs:
            if c.name not in used:
                collist.append(c)
                used.add(c.name)
        else:
            collist.append(c)
    return collist

class AliasedRow(object):
    """Wrap a RowProxy with a translation map.
    
    This object allows a set of keys to be translated
    to those present in a RowProxy.
    
    """
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
    """Clones and modifies clauses based on column correspondence.
    
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

    make an alias of table1::

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
        self.equivalents = util.column_dict(equivalents or {})
        
    def _corresponding_column(self, col, require_embedded, _seen=util.EMPTY_SET):
        newcol = self.selectable.corresponding_column(col, require_embedded=require_embedded)

        if newcol is None and col in self.equivalents and col not in _seen:
            for equiv in self.equivalents[col]:
                newcol = self._corresponding_column(equiv, require_embedded=require_embedded, _seen=_seen.union([col]))
                if newcol is not None:
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
    """Extends ClauseAdapter with extra utility functions.
    
    Provides the ability to "wrap" this ClauseAdapter 
    around another, a columns dictionary which returns
    adapted elements given an original, and an 
    adapted_row() factory.
    
    """
    def __init__(self, selectable, equivalents=None, 
                        chain_to=None, include=None, 
                        exclude=None, adapt_required=False):
        ClauseAdapter.__init__(self, selectable, equivalents, include, exclude)
        if chain_to:
            self.chain(chain_to)
        self.columns = util.populate_column_dict(self._locate_col)
        self.adapt_required = adapt_required

    def wrap(self, adapter):
        ac = self.__class__.__new__(self.__class__)
        ac.__dict__ = self.__dict__.copy()
        ac._locate_col = ac._wrap(ac._locate_col, adapter._locate_col)
        ac.adapt_clause = ac._wrap(ac.adapt_clause, adapter.adapt_clause)
        ac.adapt_list = ac._wrap(ac.adapt_list, adapter.adapt_list)
        ac.columns = util.populate_column_dict(ac._locate_col)
        return ac

    adapt_clause = ClauseAdapter.traverse
    adapt_list = ClauseAdapter.copy_and_process

    def _wrap(self, local, wrapped):
        def locate(col):
            col = local(col)
            return wrapped(col)
        return locate

    def _locate_col(self, col):
        c = self._corresponding_column(col, True)
        if c is None:
            c = self.adapt_clause(col)
            
            # anonymize labels in case they have a hardcoded name
            if isinstance(c, expression._Label):
                c = c.label(None)
                
        # adapt_required indicates that if we got the same column
        # back which we put in (i.e. it passed through), 
        # it's not correct.  this is used by eagerloading which
        # knows that all columns and expressions need to be adapted
        # to a result row, and a "passthrough" is definitely targeting
        # the wrong column.
        if self.adapt_required and c is col:
            return None
            
        return c    

    def adapted_row(self, row):
        return AliasedRow(row, self.columns)
    
    def __getstate__(self):
        d = self.__dict__.copy()
        del d['columns']
        return d
        
    def __setstate__(self, state):
        self.__dict__.update(state)
        self.columns = util.PopulateDict(self._locate_col)
