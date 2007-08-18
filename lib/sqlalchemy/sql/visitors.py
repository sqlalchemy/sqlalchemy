class ClauseVisitor(object):
    """A class that knows how to traverse and visit
    ``ClauseElements``.
    
    Calls visit_XXX() methods dynamically generated for each particualr
    ``ClauseElement`` subclass encountered.  Traversal of a
    hierarchy of ``ClauseElements`` is achieved via the
    ``traverse()`` method, which is passed the lead
    ``ClauseElement``.
    
    By default, ``ClauseVisitor`` traverses all elements
    fully.  Options can be specified at the class level via the 
    ``__traverse_options__`` dictionary which will be passed
    to the ``get_children()`` method of each ``ClauseElement``;
    these options can indicate modifications to the set of 
    elements returned, such as to not return column collections
    (column_collections=False) or to return Schema-level items
    (schema_visitor=True).
    
    ``ClauseVisitor`` also supports a simultaneous copy-and-traverse
    operation, which will produce a copy of a given ``ClauseElement``
    structure while at the same time allowing ``ClauseVisitor`` subclasses
    to modify the new structure in-place.
    
    """
    __traverse_options__ = {}
    
    def traverse_single(self, obj, **kwargs):
        meth = getattr(self, "visit_%s" % obj.__visit_name__, None)
        if meth:
            return meth(obj, **kwargs)

    def iterate(self, obj, stop_on=None):
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            if stop_on is None or t not in stop_on:
                yield t
                traversal.insert(0, t)
                for c in t.get_children(**self.__traverse_options__):
                    stack.append(c)
        
    def traverse(self, obj, stop_on=None, clone=False):
        if clone:
            obj = obj._clone()
            
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            if stop_on is None or t not in stop_on:
                traversal.insert(0, t)
                if clone:
                    t._copy_internals()
                for c in t.get_children(**self.__traverse_options__):
                    stack.append(c)
        for target in traversal:
            v = self
            while v is not None:
                meth = getattr(v, "visit_%s" % target.__visit_name__, None)
                if meth:
                    meth(target)
                v = getattr(v, '_next', None)
        return obj

    def chain(self, visitor):
        """'chain' an additional ClauseVisitor onto this ClauseVisitor.
        
        the chained visitor will receive all visit events after this one."""
        tail = self
        while getattr(tail, '_next', None) is not None:
            tail = tail._next
        tail._next = visitor
        return self

class NoColumnVisitor(ClauseVisitor):
    """a ClauseVisitor that will not traverse the exported Column 
    collections on Table, Alias, Select, and CompoundSelect objects
    (i.e. their 'columns' or 'c' attribute).
    
    this is useful because most traversals don't need those columns, or
    in the case of DefaultCompiler it traverses them explicitly; so
    skipping their traversal here greatly cuts down on method call overhead.
    """
    
    __traverse_options__ = {'column_collections':False}
