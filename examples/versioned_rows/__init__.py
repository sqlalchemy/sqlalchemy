"""
Several examples that illustrate the technique of intercepting changes
that would be first interpreted as an UPDATE on a row, and instead turning
it into an INSERT of a new row, leaving the previous row intact as
a historical version.

Compare to the :ref:`examples_versioned_history` example which writes a
history row to a separate history table.

.. autosource::

"""
