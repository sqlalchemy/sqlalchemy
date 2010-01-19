"""Large collection example.

Illustrates the options to use with :func:`~sqlalchemy.orm.relation()` when the list of related objects is very large, including:

* "dynamic" relations which query slices of data as accessed
* how to use ON DELETE CASCADE in conjunction with ``passive_deletes=True`` to greatly improve the performance of related collection deletion.

"""
