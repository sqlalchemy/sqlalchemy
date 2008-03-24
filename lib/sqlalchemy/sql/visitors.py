from sqlalchemy import util

class ClauseVisitor(object):
    """Traverses and visits ``ClauseElement`` structures.
    
    Calls visit_XXX() methods for each particular
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
        """visit a single element, without traversing its child elements."""
        
        for v in self._iterate_visitors:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            if meth:
                return meth(obj, **kwargs)
    
    traverse_chained = traverse_single
        
    def iterate(self, obj):
        """traverse the given expression structure, and return an iterator of all elements."""
        
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            yield t
            traversal.insert(0, t)
            for c in t.get_children(**self.__traverse_options__):
                stack.append(c)
    
    def traverse(self, obj, clone=False):
        """traverse the given expression structure.
        
        Returns the structure given, or a copy of the structure if
        clone=True.
        
        When the copy operation takes place, the before_clone() method
        will receive each element before it is copied.  If the method
        returns a non-None value, the return value is taken as the 
        "copied" element and traversal will not descend further.  
        
        The visit_XXX() methods receive the element *after* it's been
        copied.  To compare an element to another regardless of
        one element being a cloned copy of the original, the 
        '_cloned_set' attribute of ClauseElement can be used for the compare, 
        i.e.::
        
            original in copied._cloned_set
            
        
        """
        if clone:
            return self._cloned_traversal(obj)
        else:
            return self._non_cloned_traversal(obj)

    def copy_and_process(self, list_):
        """Apply cloned traversal to the given list of elements, and return the new list."""

        return [self._cloned_traversal(x) for x in list_]

    def before_clone(self, elem):
        """receive pre-copied elements during a cloning traversal.
        
        If the method returns a new element, the element is used 
        instead of creating a simple copy of the element.  Traversal 
        will halt on the newly returned element if it is re-encountered.
        """
        return None
    
    def _clone_element(self, elem, stop_on, cloned):
        for v in self._iterate_visitors:
            newelem = v.before_clone(elem)
            if newelem:
                stop_on.add(newelem)
                return newelem

        if elem not in cloned:
            # the full traversal will only make a clone of a particular element
            # once.
            cloned[elem] = elem._clone()
        return cloned[elem]
            
    def _cloned_traversal(self, obj):
        """a recursive traversal which creates copies of elements, returning the new structure."""
        
        stop_on = self.__traverse_options__.get('stop_on', [])
        return self._cloned_traversal_impl(obj, util.Set(stop_on), {}, _clone_toplevel=True)
        
    def _cloned_traversal_impl(self, elem, stop_on, cloned, _clone_toplevel=False):
        if elem in stop_on:
            return elem

        if _clone_toplevel:
            elem = self._clone_element(elem, stop_on, cloned)
            if elem in stop_on:
                return elem

        def clone(element):
            return self._clone_element(element, stop_on, cloned)
        elem._copy_internals(clone=clone)

        for v in self._iterate_visitors:
            meth = getattr(v, "visit_%s" % elem.__visit_name__, None)
            if meth:
                meth(elem)

        for e in elem.get_children(**self.__traverse_options__):
            if e not in stop_on:
                self._cloned_traversal_impl(e, stop_on, cloned)
        return elem
        
    def _non_cloned_traversal(self, obj):
        """a non-recursive, non-cloning traversal."""
        
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            traversal.insert(0, t)
            for c in t.get_children(**self.__traverse_options__):
                stack.append(c)
        for target in traversal:
            for v in self._iterate_visitors:
                meth = getattr(v, "visit_%s" % target.__visit_name__, None)
                if meth:
                    meth(target)
        return obj

    def _iterate_visitors(self):
        """iterate through this visitor and each 'chained' visitor."""
        
        v = self
        while v is not None:
            yield v
            v = getattr(v, '_next', None)
    _iterate_visitors = property(_iterate_visitors)

    def chain(self, visitor):
        """'chain' an additional ClauseVisitor onto this ClauseVisitor.
        
        the chained visitor will receive all visit events after this one.
        """
        tail = self
        while getattr(tail, '_next', None) is not None:
            tail = tail._next
        tail._next = visitor
        return self

class NoColumnVisitor(ClauseVisitor):
    """ClauseVisitor with 'column_collections' set to False; will not
    traverse the front-facing Column collections on Table, Alias, Select, 
    and CompoundSelect objects.
    
    """
    
    __traverse_options__ = {'column_collections':False}


def traverse(clause, **kwargs):
    """traverse the given clause, applying visit functions passed in as keyword arguments."""
    
    clone = kwargs.pop('clone', False)
    class Vis(ClauseVisitor):
        __traverse_options__ = kwargs.pop('traverse_options', {})
    vis = Vis()
    for key in kwargs:
        if key.startswith('visit_'):
            setattr(vis, key, kwargs[key])
    return vis.traverse(clause, clone=clone)

