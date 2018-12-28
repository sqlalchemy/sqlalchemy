.. change::
   :tags: bug, orm

   The "remove" event for collections is now called before the item is removed
   in the case of the ``collection.remove()`` method, as is consistent with the
   behavior for most other forms of collection item removal (such as
   ``__delitem__``, replacement under ``__setitem__``).  For ``pop()`` methods,
   the remove event still fires after the operation.
