"""
Recipes which illustrate augmentation of ORM SELECT behavior as used by
:meth:`_orm.Session.execute` with :term:`2.0 style` use of
:func:`_sql.select`, as well as the :term:`1.x style` :class:`_orm.Query`
object.

Examples include demonstrations of the :func:`_orm.with_loader_criteria`
option as well as the :meth:`_orm.SessionEvents.do_orm_execute` hook.

As of SQLAlchemy 1.4, the :class:`_orm.Query` construct is unified
with the :class:`_expression.Select` construct, so that these two objects
are mostly the same.


.. autosource::

"""  # noqa
