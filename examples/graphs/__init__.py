"""An example of persistence for a directed graph structure.   The
graph is stored as a collection of edges, each referencing both a
"lower" and an "upper" node in a table of nodes.  Basic persistence
and querying for lower- and upper- neighbors are illustrated::

    n2 = Node(2)
    n5 = Node(5)
    n2.add_neighbor(n5)
    print(n2.higher_neighbors())

.. autosource::

"""
