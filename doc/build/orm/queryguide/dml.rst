.. highlight:: pycon+sql
.. |prev| replace:: :doc:`inheritance`
.. |next| replace:: :doc:`columns`

.. include:: queryguide_nav_include.rst

.. doctest-include _dml_setup.rst

.. _orm_expression_update_delete:

ORM-Enabled INSERT, UPDATE, and DELETE statements
=================================================

.. admonition:: About this Document

    This section makes use of ORM mappings first illustrated in the
    :ref:`unified_tutorial`, shown in the section
    :ref:`tutorial_declaring_mapped_classes`, as well as inheritance
    mappings shown in the section :ref:`inheritance_toplevel`.

    :doc:`View the ORM setup for this page <_dml_setup>`.

The :meth:`_orm.Session.execute` method, in addition to handling ORM-enabled
:class:`_sql.Select` objects, can also accommodate ORM-enabled
:class:`_sql.Insert`, :class:`_sql.Update` and :class:`_sql.Delete` objects,
in various ways which are each used to INSERT, UPDATE, or DELETE
many database rows at once.  There is also dialect-specific support
for ORM-enabled "upserts", which are INSERT statements that automatically
make use of UPDATE for rows that already exist.

The following table summarizes the calling forms that are discussed in this
document:

=====================================================   ==========================================   ========================================================================     ========================================================= ============================================================================
ORM Use Case                                            DML Construct Used                           Data is passed using ...                                                     Supports RETURNING?                                       Supports Multi-Table Mappings?
=====================================================   ==========================================   ========================================================================     ========================================================= ============================================================================
:ref:`orm_queryguide_bulk_insert`                       :func:`_dml.insert`                          List of dictionaries to :paramref:`_orm.Session.execute.params`              :ref:`yes <orm_queryguide_bulk_insert_returning>`         :ref:`yes <orm_queryguide_insert_joined_table_inheritance>`
:ref:`orm_queryguide_bulk_insert_w_sql`                 :func:`_dml.insert`                          :paramref:`_orm.Session.execute.params` with :meth:`_dml.Insert.values`      :ref:`yes <orm_queryguide_bulk_insert_w_sql>`             :ref:`yes <orm_queryguide_insert_joined_table_inheritance>`
:ref:`orm_queryguide_insert_values`                     :func:`_dml.insert`                          List of dictionaries to :meth:`_dml.Insert.values`                           :ref:`yes <orm_queryguide_insert_values>`                 no
:ref:`orm_queryguide_upsert`                            :func:`_dml.insert`                          List of dictionaries to :meth:`_dml.Insert.values`                           :ref:`yes <orm_queryguide_upsert_returning>`              no
:ref:`orm_queryguide_bulk_update`                       :func:`_dml.update`                          List of dictionaries to :paramref:`_orm.Session.execute.params`              no                                                        :ref:`yes <orm_queryguide_bulk_update_joined_inh>`
:ref:`orm_queryguide_update_delete_where`               :func:`_dml.update`, :func:`_dml.delete`     keywords to :meth:`_dml.Update.values`                                       :ref:`yes <orm_queryguide_update_delete_where_returning>` :ref:`partial, with manual steps <orm_queryguide_update_delete_joined_inh>`
=====================================================   ==========================================   ========================================================================     ========================================================= ============================================================================



.. _orm_queryguide_bulk_insert:

ORM Bulk INSERT Statements
--------------------------

A :func:`_dml.insert` construct can be constructed in terms of an ORM class
and passed to the :meth:`_orm.Session.execute` method.   A list of parameter
dictionaries sent to the :paramref:`_orm.Session.execute.params` parameter, separate
from the :class:`_dml.Insert` object itself, will invoke **bulk INSERT mode**
for the statement, which essentially means the operation will optimize
as much as possible for many rows::

    >>> from sqlalchemy import insert
    >>> session.execute(
    ...     insert(User),
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ],
    ... )
    {execsql}INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] [('spongebob', 'Spongebob Squarepants'), ('sandy', 'Sandy Cheeks'), ('patrick', 'Patrick Star'),
    ('squidward', 'Squidward Tentacles'), ('ehkrabs', 'Eugene H. Krabs')]
    {stop}<...>

The parameter dictionaries contain key/value pairs which may correspond to ORM
mapped attributes that line up with mapped :class:`._schema.Column`
or :func:`_orm.mapped_column` declarations, as well as with
:ref:`composite <mapper_composite>` declarations.   The keys should match
the **ORM mapped attribute name** and **not** the actual database column name,
if these two names happen to be different.

.. versionchanged:: 2.0  Passing an :class:`_dml.Insert` construct to the
   :meth:`_orm.Session.execute` method now invokes a "bulk insert", which
   makes use of the same functionality as the legacy
   :meth:`_orm.Session.bulk_insert_mappings` method.  This is a behavior change
   compared to the 1.x series where the :class:`_dml.Insert` would be interpreted
   in a Core-centric way, using column names for value keys; ORM attribute
   keys are now accepted.   Core-style functionality is available by passing
   the execution option ``{"dml_strategy": "raw"}`` to the
   :paramref:`_orm.Session.execution_options` parameter of
   :meth:`_orm.Session.execute`.

.. _orm_queryguide_bulk_insert_returning:

Getting new objects with RETURNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

  >>> session.rollback()
  ROLLBACK...
  >>> session.connection()
  BEGIN (implicit)...

The bulk ORM insert feature supports INSERT..RETURNING for selected
backends, which can return a :class:`.Result` object that may yield individual
columns back as well as fully constructed ORM objects corresponding
to the newly generated records.    INSERT..RETURNING requires the use of a backend that
supports SQL RETURNING syntax as well as support for :term:`executemany`
with RETURNING; this feature is available with all
:ref:`SQLAlchemy-included <included_dialects>` backends
with the exception of MySQL (MariaDB is included).

As an example, we can run the same statement as before, adding use of the
:meth:`.UpdateBase.returning` method, passing the full ``User`` entity
as what we'd like to return.  :meth:`_orm.Session.scalars` is used to allow
iteration of ``User`` objects::

    >>> users = session.scalars(
    ...     insert(User).returning(User),
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ],
    ... )
    {execsql}INSERT INTO user_account (name, fullname)
    VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
    RETURNING id, name, fullname, species
    [...] ('spongebob', 'Spongebob Squarepants', 'sandy', 'Sandy Cheeks',
    'patrick', 'Patrick Star', 'squidward', 'Squidward Tentacles',
    'ehkrabs', 'Eugene H. Krabs')
    {stop}>>> print(users.all())
    [User(name='spongebob', fullname='Spongebob Squarepants'),
     User(name='sandy', fullname='Sandy Cheeks'),
     User(name='patrick', fullname='Patrick Star'),
     User(name='squidward', fullname='Squidward Tentacles'),
     User(name='ehkrabs', fullname='Eugene H. Krabs')]

In the above example, the rendered SQL takes on the form used by the
:ref:`insertmanyvalues <engine_insertmanyvalues>` feature as requested by the
SQLite backend, where individual parameter dictionaries are inlined into a
single INSERT statement so that RETURNING may be used.

.. versionchanged:: 2.0  The ORM :class:`.Session` now interprets RETURNING
   clauses from :class:`_dml.Insert`, :class:`_dml.Update`, and
   even :class:`_dml.Delete` constructs in an ORM context, meaning a mixture
   of column expressions and ORM mapped entities may be passed to the
   :meth:`_dml.Insert.returning` method which will then be delivered
   in the way that ORM results are delivered from constructs such as
   :class:`_sql.Select`, including that mapped entities will be delivered
   in the result as ORM mapped objects.  Limited support for ORM loader
   options such as :func:`_orm.load_only` and :func:`_orm.selectinload`
   is also present.

.. _orm_queryguide_bulk_insert_returning_ordered:

Correlating RETURNING records with input data order
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using bulk INSERT with RETURNING, it's important to note that most
database backends provide no formal guarantee of the order in which the
records from RETURNING are returned, including that there is no guarantee that
their order will correspond to that of the input records.  For applications
that need to ensure RETURNING records can be correlated with input data,
the additional parameter :paramref:`_dml.Insert.returning.sort_by_parameter_order`
may be specified, which depending on backend may use special INSERT forms
that maintain a token which is used to reorder the returned rows appropriately,
or in some cases, such as in the example below using the SQLite backend,
the operation will INSERT one row at a time::

    >>> data = [
    ...     {"name": "pearl", "fullname": "Pearl Krabs"},
    ...     {"name": "plankton", "fullname": "Plankton"},
    ...     {"name": "gary", "fullname": "Gary"},
    ... ]
    >>> user_ids = session.scalars(
    ...     insert(User).returning(User.id, sort_by_parameter_order=True), data
    ... )
    {execsql}INSERT INTO user_account (name, fullname) VALUES (?, ?) RETURNING id
    [... (insertmanyvalues) 1/3 (ordered; batch not supported)] ('pearl', 'Pearl Krabs')
    INSERT INTO user_account (name, fullname) VALUES (?, ?) RETURNING id
    [insertmanyvalues 2/3 (ordered; batch not supported)] ('plankton', 'Plankton')
    INSERT INTO user_account (name, fullname) VALUES (?, ?) RETURNING id
    [insertmanyvalues 3/3 (ordered; batch not supported)] ('gary', 'Gary')
    {stop}>>> for user_id, input_record in zip(user_ids, data):
    ...     input_record["id"] = user_id
    >>> print(data)
    [{'name': 'pearl', 'fullname': 'Pearl Krabs', 'id': 6},
    {'name': 'plankton', 'fullname': 'Plankton', 'id': 7},
    {'name': 'gary', 'fullname': 'Gary', 'id': 8}]

.. versionadded:: 2.0.10 Added :paramref:`_dml.Insert.returning.sort_by_parameter_order`
   which is implemented within the :term:`insertmanyvalues` architecture.

.. seealso::

    :ref:`engine_insertmanyvalues_returning_order` - background on approaches
    taken to guarantee correspondence between input data and result rows
    without significant loss of performance


.. _orm_queryguide_insert_heterogeneous_params:

Using Heterogenous Parameter Dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

  >>> session.rollback()
  ROLLBACK...
  >>> session.connection()
  BEGIN (implicit)...

The ORM bulk insert feature supports lists of parameter dictionaries that are
"heterogenous", which basically means "individual dictionaries can have different
keys".   When this condition is detected,
the ORM will break up the parameter dictionaries into groups corresponding
to each set of keys and batch accordingly into separate INSERT statements::

    >>> users = session.scalars(
    ...     insert(User).returning(User),
    ...     [
    ...         {
    ...             "name": "spongebob",
    ...             "fullname": "Spongebob Squarepants",
    ...             "species": "Sea Sponge",
    ...         },
    ...         {"name": "sandy", "fullname": "Sandy Cheeks", "species": "Squirrel"},
    ...         {"name": "patrick", "species": "Starfish"},
    ...         {
    ...             "name": "squidward",
    ...             "fullname": "Squidward Tentacles",
    ...             "species": "Squid",
    ...         },
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs", "species": "Crab"},
    ...     ],
    ... )
    {execsql}INSERT INTO user_account (name, fullname, species)
    VALUES (?, ?, ?), (?, ?, ?) RETURNING id, name, fullname, species
    [... (insertmanyvalues) 1/1 (unordered)] ('spongebob', 'Spongebob Squarepants', 'Sea Sponge',
    'sandy', 'Sandy Cheeks', 'Squirrel')
    INSERT INTO user_account (name, species)
    VALUES (?, ?) RETURNING id, name, fullname, species
    [...] ('patrick', 'Starfish')
    INSERT INTO user_account (name, fullname, species)
    VALUES (?, ?, ?), (?, ?, ?) RETURNING id, name, fullname, species
    [... (insertmanyvalues) 1/1 (unordered)] ('squidward', 'Squidward Tentacles',
    'Squid', 'ehkrabs', 'Eugene H. Krabs', 'Crab')



In the above example, the five parameter dictionaries passed translated into
three INSERT statements, grouped along the specific sets of keys
in each dictionary while still maintaining row order, i.e.
``("name", "fullname", "species")``, ``("name", "species")``, ``("name","fullname", "species")``.

.. _orm_queryguide_insert_joined_table_inheritance:

Bulk INSERT for Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK
    >>> session.connection()
    BEGIN...

ORM bulk insert builds upon the internal system that is used by the
traditional :term:`unit of work` system in order to emit INSERT statements.  This means
that for an ORM entity that is mapped to multiple tables, typically one which
is mapped using :ref:`joined table inheritance <joined_inheritance>`, the
bulk INSERT operation will emit an INSERT statement for each table represented
by the mapping, correctly transferring server-generated primary key values
to the table rows that depend upon them.  The RETURNING feature is also supported
here, where the ORM will receive :class:`.Result` objects for each INSERT
statement executed, and will then "horizontally splice" them together so that
the returned rows include values for all columns inserted::

    >>> managers = session.scalars(
    ...     insert(Manager).returning(Manager),
    ...     [
    ...         {"name": "sandy", "manager_name": "Sandy Cheeks"},
    ...         {"name": "ehkrabs", "manager_name": "Eugene H. Krabs"},
    ...     ],
    ... )
    {execsql}INSERT INTO employee (name, type) VALUES (?, ?) RETURNING id, name, type
    [... (insertmanyvalues) 1/2 (ordered; batch not supported)] ('sandy', 'manager')
    INSERT INTO employee (name, type) VALUES (?, ?) RETURNING id, name, type
    [insertmanyvalues 2/2 (ordered; batch not supported)] ('ehkrabs', 'manager')
    INSERT INTO manager (id, manager_name) VALUES (?, ?), (?, ?) RETURNING id, manager_name, id AS id__1
    [... (insertmanyvalues) 1/1 (ordered)] (1, 'Sandy Cheeks', 2, 'Eugene H. Krabs')

.. tip:: Bulk INSERT of joined inheritance mappings requires that the ORM
   make use of the :paramref:`_dml.Insert.returning.sort_by_parameter_order`
   parameter internally, so that it can correlate primary key values from
   RETURNING rows from the base table into the parameter sets being used
   to INSERT into the "sub" table, which is why the SQLite backend
   illustrated above transparently degrades to using non-batched statements.
   Background on this feature is at
   :ref:`engine_insertmanyvalues_returning_order`.


.. _orm_queryguide_bulk_insert_w_sql:

ORM Bulk Insert with SQL Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ORM bulk insert feature supports the addition of a fixed set of
parameters which may include SQL expressions to be applied to every target row.
To achieve this, combine the use of the :meth:`_dml.Insert.values` method,
passing a dictionary of parameters that will be applied to all rows,
with the usual bulk calling form by including a list of parameter dictionaries
that contain individual row values when invoking :meth:`_orm.Session.execute`.

As an example, given an ORM mapping that includes a "timestamp" column:

.. sourcecode:: python

    import datetime


    class LogRecord(Base):
        __tablename__ = "log_record"
        id: Mapped[int] = mapped_column(primary_key=True)
        message: Mapped[str]
        code: Mapped[str]
        timestamp: Mapped[datetime.datetime]

If we wanted to INSERT a series of ``LogRecord`` elements, each with a unique
``message`` field, however we would like to apply the SQL function ``now()``
to all rows, we can pass ``timestamp`` within :meth:`_dml.Insert.values`
and then pass the additional records using "bulk" mode::

    >>> from sqlalchemy import func
    >>> log_record_result = session.scalars(
    ...     insert(LogRecord).values(code="SQLA", timestamp=func.now()).returning(LogRecord),
    ...     [
    ...         {"message": "log message #1"},
    ...         {"message": "log message #2"},
    ...         {"message": "log message #3"},
    ...         {"message": "log message #4"},
    ...     ],
    ... )
    {execsql}INSERT INTO log_record (message, code, timestamp)
    VALUES (?, ?, CURRENT_TIMESTAMP), (?, ?, CURRENT_TIMESTAMP),
    (?, ?, CURRENT_TIMESTAMP), (?, ?, CURRENT_TIMESTAMP)
    RETURNING id, message, code, timestamp
    [... (insertmanyvalues) 1/1 (unordered)] ('log message #1', 'SQLA', 'log message #2',
    'SQLA', 'log message #3', 'SQLA', 'log message #4', 'SQLA')


    {stop}>>> print(log_record_result.all())
    [LogRecord('log message #1', 'SQLA', datetime.datetime(...)),
     LogRecord('log message #2', 'SQLA', datetime.datetime(...)),
     LogRecord('log message #3', 'SQLA', datetime.datetime(...)),
     LogRecord('log message #4', 'SQLA', datetime.datetime(...))]


.. _orm_queryguide_insert_values:

ORM Bulk Insert with Per Row SQL Expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK
    >>> session.execute(
    ...     insert(User),
    ...     [
    ...         {
    ...             "name": "spongebob",
    ...             "fullname": "Spongebob Squarepants",
    ...             "species": "Sea Sponge",
    ...         },
    ...         {"name": "sandy", "fullname": "Sandy Cheeks", "species": "Squirrel"},
    ...         {"name": "patrick", "species": "Starfish"},
    ...         {
    ...             "name": "squidward",
    ...             "fullname": "Squidward Tentacles",
    ...             "species": "Squid",
    ...         },
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs", "species": "Crab"},
    ...     ],
    ... )
    BEGIN...

The :meth:`_dml.Insert.values` method itself accommodates a list of parameter
dictionaries directly. When using the :class:`_dml.Insert` construct in this
way, without passing any list of parameter dictionaries to the
:paramref:`_orm.Session.execute.params` parameter, bulk ORM insert mode is not
used, and instead the INSERT statement is rendered exactly as given and invoked
exactly once. This mode of operation may be useful both for the case of passing
SQL expressions on a per-row basis, and is also used when using "upsert"
statements with the ORM, documented later in this chapter at
:ref:`orm_queryguide_upsert`.

A contrived example of an INSERT that embeds per-row SQL expressions,
and also demonstrates :meth:`_dml.Insert.returning` in this form, is below::


  >>> from sqlalchemy import select
  >>> address_result = session.scalars(
  ...     insert(Address)
  ...     .values(
  ...         [
  ...             {
  ...                 "user_id": select(User.id).where(User.name == "sandy"),
  ...                 "email_address": "sandy@company.com",
  ...             },
  ...             {
  ...                 "user_id": select(User.id).where(User.name == "spongebob"),
  ...                 "email_address": "spongebob@company.com",
  ...             },
  ...             {
  ...                 "user_id": select(User.id).where(User.name == "patrick"),
  ...                 "email_address": "patrick@company.com",
  ...             },
  ...         ]
  ...     )
  ...     .returning(Address),
  ... )
  {execsql}INSERT INTO address (user_id, email_address) VALUES
  ((SELECT user_account.id
  FROM user_account
  WHERE user_account.name = ?), ?), ((SELECT user_account.id
  FROM user_account
  WHERE user_account.name = ?), ?), ((SELECT user_account.id
  FROM user_account
  WHERE user_account.name = ?), ?) RETURNING id, user_id, email_address
  [...] ('sandy', 'sandy@company.com', 'spongebob', 'spongebob@company.com',
  'patrick', 'patrick@company.com')
  {stop}>>> print(address_result.all())
  [Address(email_address='sandy@company.com'),
   Address(email_address='spongebob@company.com'),
   Address(email_address='patrick@company.com')]

Because bulk ORM insert mode is not used above, the following features
are not present:

* :ref:`Joined table inheritance <orm_queryguide_insert_joined_table_inheritance>`
  or other multi-table mappings are not supported, since that would require multiple
  INSERT statements.

* :ref:`Heterogenous parameter sets <orm_queryguide_insert_heterogeneous_params>`
  are not supported - each element in the VALUES set must have the same
  columns.

* Core-level scale optimizations such as the batching provided by
  :ref:`insertmanyvalues <engine_insertmanyvalues>` are not available; statements
  will need to ensure the total number of parameters does not exceed limits
  imposed by the backing database.

For the above reasons, it is generally not recommended to use multiple
parameter sets with :meth:`_dml.Insert.values` with ORM INSERT statements
unless there is a clear rationale, which is either that "upsert" is being used
or there is a need to embed per-row SQL expressions in each parameter set.

.. seealso::

    :ref:`orm_queryguide_upsert`


.. _orm_queryguide_legacy_bulk_insert:

Legacy Session Bulk INSERT Methods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`_orm.Session` includes legacy methods for performing
"bulk" INSERT and UPDATE statements.  These methods share implementations
with the SQLAlchemy 2.0 versions of these features, described
at :ref:`orm_queryguide_bulk_insert` and :ref:`orm_queryguide_bulk_update`,
however lack many features, namely RETURNING support as well as support
for session-synchronization.

Code which makes use of :meth:`.Session.bulk_insert_mappings` for example
can port code as follows, starting with this mappings example::

    session.bulk_insert_mappings(User, [{"name": "u1"}, {"name": "u2"}, {"name": "u3"}])

The above is expressed using the new API as::

    from sqlalchemy import insert

    session.execute(insert(User), [{"name": "u1"}, {"name": "u2"}, {"name": "u3"}])

.. seealso::

    :ref:`orm_queryguide_legacy_bulk_update`


.. _orm_queryguide_upsert:

ORM "upsert" Statements
~~~~~~~~~~~~~~~~~~~~~~~

Selected backends with SQLAlchemy may include dialect-specific :class:`_dml.Insert`
constructs which additionally have the ability to perform "upserts", or INSERTs
where an existing row in the parameter set is turned into an approximation of
an UPDATE statement instead. By "existing row" , this may mean rows
which share the same primary key value, or may refer to other indexed
columns within the row that are considered to be unique; this is dependent
on the capabilities of the backend in use.

The dialects included with SQLAlchemy that include dialect-specific "upsert"
API features are:

* SQLite - using :class:`_sqlite.Insert` documented at :ref:`sqlite_on_conflict_insert`
* PostgreSQL - using :class:`_postgresql.Insert` documented at :ref:`postgresql_insert_on_conflict`
* MySQL/MariaDB - using :class:`_mysql.Insert` documented at :ref:`mysql_insert_on_duplicate_key_update`

Users should review the above sections for background on proper construction
of these objects; in particular, the "upsert" method typically needs to
refer back to the original statement, so the statement is usually constructed
in two separate steps.

Third party backends such as those mentioned at :ref:`external_toplevel` may
also feature similar constructs.

While SQLAlchemy does not yet have a backend-agnostic upsert construct, the above
:class:`_dml.Insert` variants are nonetheless ORM compatible in that they may be used
in the same way as the :class:`_dml.Insert` construct itself as documented at
:ref:`orm_queryguide_insert_values`, that is, by embedding the desired rows
to INSERT within the :meth:`_dml.Insert.values` method.   In the example
below, the SQLite :func:`_sqlite.insert` function is used to generate
an :class:`_sqlite.Insert` construct that includes "ON CONFLICT DO UPDATE"
support.   The statement is then passed to :meth:`_orm.Session.execute` where
it proceeds normally, with the additional characteristic that the
parameter dictionaries passed to :meth:`_dml.Insert.values` are interpreted
as ORM mapped attribute keys, rather than column names:

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK
    >>> session.execute(
    ...     insert(User).values(
    ...         [
    ...             dict(name="sandy"),
    ...             dict(name="spongebob", fullname="Spongebob Squarepants"),
    ...         ]
    ...     )
    ... )
    BEGIN...

::

    >>> from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
    >>> stmt = sqlite_upsert(User).values(
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ]
    ... )
    >>> stmt = stmt.on_conflict_do_update(
    ...     index_elements=[User.name], set_=dict(fullname=stmt.excluded.fullname)
    ... )
    >>> session.execute(stmt)
    {execsql}INSERT INTO user_account (name, fullname)
    VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
    ON CONFLICT (name) DO UPDATE SET fullname = excluded.fullname
    [...] ('spongebob', 'Spongebob Squarepants', 'sandy', 'Sandy Cheeks',
    'patrick', 'Patrick Star', 'squidward', 'Squidward Tentacles',
    'ehkrabs', 'Eugene H. Krabs')
    {stop}<...>

.. _orm_queryguide_upsert_returning:

Using RETURNING with upsert statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From the SQLAlchemy ORM's point of view, upsert statements look like regular
:class:`_dml.Insert` constructs, which includes that :meth:`_dml.Insert.returning`
works with upsert statements in the same way as was demonstrated at
:ref:`orm_queryguide_insert_values`, so that any column expression or
relevant ORM entity class may be passed.  Continuing from the
example in the previous section::

    >>> result = session.scalars(
    ...     stmt.returning(User), execution_options={"populate_existing": True}
    ... )
    {execsql}INSERT INTO user_account (name, fullname)
    VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
    ON CONFLICT (name) DO UPDATE SET fullname = excluded.fullname
    RETURNING id, name, fullname, species
    [...] ('spongebob', 'Spongebob Squarepants', 'sandy', 'Sandy Cheeks',
    'patrick', 'Patrick Star', 'squidward', 'Squidward Tentacles',
    'ehkrabs', 'Eugene H. Krabs')
    {stop}>>> print(result.all())
    [User(name='spongebob', fullname='Spongebob Squarepants'),
      User(name='sandy', fullname='Sandy Cheeks'),
      User(name='patrick', fullname='Patrick Star'),
      User(name='squidward', fullname='Squidward Tentacles'),
      User(name='ehkrabs', fullname='Eugene H. Krabs')]

The example above uses RETURNING to return ORM objects for each row inserted or
upserted by the statement. The example also adds use of the
:ref:`orm_queryguide_populate_existing` execution option. This option indicates
that ``User`` objects which are already present
in the :class:`_orm.Session` for rows that already exist should be
**refreshed** with the data from the new row. For a pure :class:`_dml.Insert`
statement, this option is not significant, because every row produced is a
brand new primary key identity. However when the :class:`_dml.Insert` also
includes "upsert" options, it may also be yielding results from rows that
already exist and therefore may already have a primary key identity represented
in the :class:`_orm.Session` object's :term:`identity map`.

.. seealso::

    :ref:`orm_queryguide_populate_existing`


.. _orm_queryguide_bulk_update:

ORM Bulk UPDATE by Primary Key
------------------------------

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK
    >>> session.execute(
    ...     insert(User),
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ],
    ... )
    BEGIN ...
    >>> session.commit()
    COMMIT...
    >>> session.connection()
    BEGIN ...

The :class:`_dml.Update` construct may be used with
:meth:`_orm.Session.execute` in a similar way as the :class:`_dml.Insert`
statement is used as described at :ref:`orm_queryguide_bulk_insert`, passing a
list of many parameter dictionaries, each dictionary representing an individual
row that corresponds to a single primary key value. This use should not be
confused with a more common way to use :class:`_dml.Update` statements with the
ORM, using an explicit WHERE clause, which is documented at
:ref:`orm_queryguide_update_delete_where`.

For the "bulk" version of UPDATE, a :func:`_dml.update` construct is made in
terms of an ORM class and passed to the :meth:`_orm.Session.execute` method;
the resulting :class:`_dml.Update` object should have **no values and typically
no WHERE criteria**, that is, the :meth:`_dml.Update.values` method is not
used, and the :meth:`_dml.Update.where` is **usually** not used, but may be
used in the unusual case that additional filtering criteria would be added.

Passing the :class:`_dml.Update` construct along with a list of parameter
dictionaries which each include a full primary key value will invoke **bulk
UPDATE by primary key mode** for the statement, generating the appropriate
WHERE criteria to match each row by primary key, and using :term:`executemany`
to run each parameter set against the UPDATE statement::

    >>> from sqlalchemy import update
    >>> session.execute(
    ...     update(User),
    ...     [
    ...         {"id": 1, "fullname": "Spongebob Squarepants"},
    ...         {"id": 3, "fullname": "Patrick Star"},
    ...         {"id": 5, "fullname": "Eugene H. Krabs"},
    ...     ],
    ... )
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.id = ?
    [...] [('Spongebob Squarepants', 1), ('Patrick Star', 3), ('Eugene H. Krabs', 5)]
    {stop}<...>

Note that each parameter dictionary **must include a full primary key for
each record**, else an error is raised.

Like the bulk INSERT feature, heterogeneous parameter lists are supported here
as well, where the parameters will be grouped into sub-batches of UPDATE
runs.

.. versionchanged:: 2.0.11  Additional WHERE criteria can be combined with
   :ref:`orm_queryguide_bulk_update` by using the :meth:`_dml.Update.where`
   method to add additional criteria.  However this criteria is always in
   addition to the WHERE criteria that's already made present which includes
   primary key values.

The RETURNING feature is not available when using the "bulk UPDATE by primary
key" feature; the list of multiple parameter dictionaries necessarily makes use
of DBAPI :term:`executemany`, which in its usual form does not typically
support result rows.


.. versionchanged:: 2.0  Passing an :class:`_dml.Update` construct to the
   :meth:`_orm.Session.execute` method along with a list of parameter
   dictionaries now invokes a "bulk update", which makes use of the same
   functionality as the legacy :meth:`_orm.Session.bulk_update_mappings`
   method.  This is a behavior change compared to the 1.x series where the
   :class:`_dml.Update` would only be supported with explicit WHERE criteria
   and inline VALUES.

.. _orm_queryguide_bulk_update_disabling:

Disabling Bulk ORM Update by Primary Key for an UPDATE statement with multiple parameter sets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ORM Bulk Update by Primary Key feature, which runs an UPDATE statement
per record which includes WHERE criteria for each primary key value, is
automatically used when:

1. the UPDATE statement given is against an ORM entity
2. the :class:`_orm.Session` is used to execute the statement, and not a
   Core :class:`_engine.Connection`
3. The parameters passed are a **list of dictionaries**.

In order to invoke an UPDATE statement without using "ORM Bulk Update by Primary Key",
invoke the statement against the :class:`_engine.Connection` directly using
the :meth:`_orm.Session.connection` method to acquire the current
:class:`_engine.Connection` for the transaction::


    >>> from sqlalchemy import bindparam
    >>> session.connection().execute(
    ...     update(User).where(User.name == bindparam("u_name")),
    ...     [
    ...         {"u_name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"u_name": "patrick", "fullname": "Patrick Star"},
    ...     ],
    ... )
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.name = ?
    [...] [('Spongebob Squarepants', 'spongebob'), ('Patrick Star', 'patrick')]
    {stop}<...>

.. seealso::

    :ref:`error_bupq`

.. _orm_queryguide_bulk_update_joined_inh:

Bulk UPDATE by Primary Key for Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

    >>> session.execute(
    ...     insert(Manager).returning(Manager),
    ...     [
    ...         {"name": "sandy", "manager_name": "Sandy Cheeks"},
    ...         {"name": "ehkrabs", "manager_name": "Eugene H. Krabs"},
    ...     ],
    ... )
    INSERT...
    >>> session.commit()
    COMMIT...
    >>> session.connection()
    BEGIN (implicit)...

ORM bulk update has similar behavior to ORM bulk insert when using mappings
with joined table inheritance; as described at
:ref:`orm_queryguide_insert_joined_table_inheritance`, the bulk UPDATE
operation will emit an UPDATE statement for each table represented in the
mapping, for which the given parameters include values to be updated
(non-affected tables are skipped).

Example::

    >>> session.execute(
    ...     update(Manager),
    ...     [
    ...         {
    ...             "id": 1,
    ...             "name": "scheeks",
    ...             "manager_name": "Sandy Cheeks, President",
    ...         },
    ...         {
    ...             "id": 2,
    ...             "name": "eugene",
    ...             "manager_name": "Eugene H. Krabs, VP Marketing",
    ...         },
    ...     ],
    ... )
    {execsql}UPDATE employee SET name=? WHERE employee.id = ?
    [...] [('scheeks', 1), ('eugene', 2)]
    UPDATE manager SET manager_name=? WHERE manager.id = ?
    [...] [('Sandy Cheeks, President', 1), ('Eugene H. Krabs, VP Marketing', 2)]
    {stop}<...>

.. _orm_queryguide_legacy_bulk_update:

Legacy Session Bulk UPDATE Methods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As discussed at :ref:`orm_queryguide_legacy_bulk_insert`, the
:meth:`_orm.Session.bulk_update_mappings` method of :class:`_orm.Session` is
the legacy form of bulk update, which the ORM makes use of internally when
interpreting a :func:`_sql.update` statement with primary key parameters given;
however, when using the legacy version, features such as support for
session-synchronization are not included.

The example below::

    session.bulk_update_mappings(
        User,
        [
            {"id": 1, "name": "scheeks", "manager_name": "Sandy Cheeks, President"},
            {"id": 2, "name": "eugene", "manager_name": "Eugene H. Krabs, VP Marketing"},
        ],
    )

Is expressed using the new API as::

    from sqlalchemy import update

    session.execute(
        update(User),
        [
            {"id": 1, "name": "scheeks", "manager_name": "Sandy Cheeks, President"},
            {"id": 2, "name": "eugene", "manager_name": "Eugene H. Krabs, VP Marketing"},
        ],
    )

.. seealso::

    :ref:`orm_queryguide_legacy_bulk_insert`



.. _orm_queryguide_update_delete_where:

ORM UPDATE and DELETE with Custom WHERE Criteria
------------------------------------------------

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK...
    >>> session.connection()
    BEGIN (implicit)...

The :class:`_dml.Update` and :class:`_dml.Delete` constructs, when constructed
with custom WHERE criteria (that is, using the :meth:`_dml.Update.where` and
:meth:`_dml.Delete.where` methods), may be invoked in an ORM context
by passing them to :meth:`_orm.Session.execute`, without using
the :paramref:`_orm.Session.execute.params` parameter. For :class:`_dml.Update`,
the values to be updated should be passed using :meth:`_dml.Update.values`.

This mode of use differs
from the feature described previously at :ref:`orm_queryguide_bulk_update`
in that the ORM uses the given WHERE clause as is, rather than fixing the
WHERE clause to be by primary key.   This means that the single UPDATE or
DELETE statement can affect many rows at once.

As an example, below an UPDATE is emitted that affects the "fullname"
field of multiple rows
::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(User)
    ...     .where(User.name.in_(["squidward", "sandy"]))
    ...     .values(fullname="Name starts with S")
    ... )
    >>> session.execute(stmt)
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.name IN (?, ?)
    [...] ('Name starts with S', 'squidward', 'sandy')
    {stop}<...>


For a DELETE, an example of deleting rows based on criteria::

    >>> from sqlalchemy import delete
    >>> stmt = delete(User).where(User.name.in_(["squidward", "sandy"]))
    >>> session.execute(stmt)
    {execsql}DELETE FROM user_account WHERE user_account.name IN (?, ?)
    [...] ('squidward', 'sandy')
    {stop}<...>

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK...
    >>> session.connection()
    BEGIN (implicit)...

.. _orm_queryguide_update_delete_sync:


Selecting a Synchronization Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When making use of :func:`_dml.update` or :func:`_dml.delete` in conjunction
with ORM-enabled execution using :meth:`_orm.Session.execute`, additional
ORM-specific functionality is present which will **synchronize** the state
being changed by the statement with that of the objects that are currently
present within the :term:`identity map` of the :class:`_orm.Session`.
By "synchronize" we mean that UPDATEd attributes will be refreshed with the
new value, or at the very least :term:`expired` so that they will re-populate
with their new value on next access, and DELETEd objects will be
moved into the :term:`deleted` state.

This synchronization is controllable as the "synchronization strategy",
which is passed as an string ORM execution option, typically by using the
:paramref:`_orm.Session.execute.execution_options` dictionary::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(User).where(User.name == "squidward").values(fullname="Squidward Tentacles")
    ... )
    >>> session.execute(stmt, execution_options={"synchronize_session": False})
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.name = ?
    [...] ('Squidward Tentacles', 'squidward')
    {stop}<...>

The execution option may also be bundled with the statement itself using the
:meth:`_sql.Executable.execution_options` method::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(User)
    ...     .where(User.name == "squidward")
    ...     .values(fullname="Squidward Tentacles")
    ...     .execution_options(synchronize_session=False)
    ... )
    >>> session.execute(stmt)
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.name = ?
    [...] ('Squidward Tentacles', 'squidward')
    {stop}<...>

The following values for ``synchronize_session`` are supported:

* ``'auto'`` - this is the default.   The ``'fetch'`` strategy will be used on
  backends that support RETURNING, which includes all SQLAlchemy-native drivers
  except for MySQL.   If RETURNING is not supported, the ``'evaluate'``
  strategy will be used instead.

* ``'fetch'`` - Retrieves the primary key identity of affected rows by either
  performing a SELECT before the UPDATE or DELETE, or by using RETURNING if the
  database supports it, so that in-memory objects which are affected by the
  operation can be refreshed with new values (updates) or expunged from the
  :class:`_orm.Session` (deletes). This synchronization strategy may be used
  even if the given :func:`_dml.update` or :func:`_dml.delete`
  construct explicitly specifies entities or columns using
  :meth:`_dml.UpdateBase.returning`.

  .. versionchanged:: 2.0 Explicit :meth:`_dml.UpdateBase.returning` may be
     combined with the ``'fetch'`` synchronization strategy when using
     ORM-enabled UPDATE and DELETE with WHERE criteria.  The actual statement
     will contain the union of columns between that which the ``'fetch'``
     strategy requires and those which were requested.

* ``'evaluate'`` - This indicates to evaluate the WHERE
  criteria given in the UPDATE or DELETE statement in Python, to locate
  matching objects within the :class:`_orm.Session`. This approach does not add
  any SQL round trips to the operation, and in the absence of RETURNING
  support, may be more efficient. For UPDATE or DELETE statements with complex
  criteria, the ``'evaluate'`` strategy may not be able to evaluate the
  expression in Python and will raise an error. If this occurs, use the
  ``'fetch'`` strategy for the operation instead.

  .. tip::

    If a SQL expression makes use of custom operators using the
    :meth:`_sql.Operators.op` or :class:`_sql.custom_op` feature, the
    :paramref:`_sql.Operators.op.python_impl` parameter may be used to indicate
    a Python function that will be used by the ``"evaluate"`` synchronization
    strategy.

    .. versionadded:: 2.0

  .. warning::

    The ``"evaluate"`` strategy should be avoided if an UPDATE operation is
    to run on a :class:`_orm.Session` that has many objects which have
    been expired, because it will necessarily need to refresh objects in order
    to test them against the given WHERE criteria, which will emit a SELECT
    for each one.   In this case, and particularly if the backend supports
    RETURNING, the ``"fetch"`` strategy should be preferred.

* ``False`` - don't synchronize the session. This option may be useful
  for backends that don't support RETURNING where the ``"evaluate"`` strategy
  is not able to be used.  In this case, the state of objects in the
  :class:`_orm.Session` is unchanged and will not automatically correspond
  to the UPDATE or DELETE statement that was emitted, if such objects
  that would normally correspond to the rows matched are present.


.. _orm_queryguide_update_delete_where_returning:

Using RETURNING with UPDATE/DELETE and Custom WHERE Criteria
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :meth:`.UpdateBase.returning` method is fully compatible with
ORM-enabled UPDATE and DELETE with WHERE criteria.   Full ORM objects
and/or columns may be indicated for RETURNING::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(User)
    ...     .where(User.name == "squidward")
    ...     .values(fullname="Squidward Tentacles")
    ...     .returning(User)
    ... )
    >>> result = session.scalars(stmt)
    {execsql}UPDATE user_account SET fullname=? WHERE user_account.name = ?
    RETURNING id, name, fullname, species
    [...] ('Squidward Tentacles', 'squidward')
    {stop}>>> print(result.all())
    [User(name='squidward', fullname='Squidward Tentacles')]

The support for RETURNING is also compatible with the ``fetch`` synchronization
strategy, which also uses RETURNING.  The ORM will organize the columns in
RETURNING appropriately so that the synchronization proceeds as well as that
the returned :class:`.Result` will contain the requested entities and SQL
columns in their requested order.

.. versionadded:: 2.0  :meth:`.UpdateBase.returning` may be used for
   ORM enabled UPDATE and DELETE while still retaining full compatibility
   with the ``fetch`` synchronization strategy.

.. _orm_queryguide_update_delete_joined_inh:

UPDATE/DELETE with Custom WHERE Criteria for Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

    >>> session.rollback()
    ROLLBACK...
    >>> session.connection()
    BEGIN (implicit)...

The UPDATE/DELETE with WHERE criteria feature, unlike the
:ref:`orm_queryguide_bulk_update`, only emits a single UPDATE or DELETE
statement per call to :meth:`_orm.Session.execute`. This means that when
running an :func:`_dml.update` or :func:`_dml.delete` statement against a
multi-table mapping, such as a subclass in a joined-table inheritance mapping,
the statement must conform to the backend's current capabilities, which may
include that the backend does not support an UPDATE or DELETE statement that
refers to multiple tables, or may have only limited support for this. This
means that for mappings such as joined inheritance subclasses, the ORM version
of the UPDATE/DELETE with WHERE criteria feature can only be used to a limited
extent or not at all, depending on specifics.

The most straightforward way to emit a multi-row UPDATE statement
for a joined-table subclass is to refer to the sub-table alone.
This means the :func:`_dml.Update` construct should only refer to attributes
that are local to the subclass table, as in the example below::


    >>> stmt = (
    ...     update(Manager)
    ...     .where(Manager.id == 1)
    ...     .values(manager_name="Sandy Cheeks, President")
    ... )
    >>> session.execute(stmt)
    {execsql}UPDATE manager SET manager_name=? WHERE manager.id = ?
    [...] ('Sandy Cheeks, President', 1)
    <...>

With the above form, a rudimentary way to refer to the base table in order
to locate rows which will work on any SQL backend is so use a subquery::

    >>> stmt = (
    ...     update(Manager)
    ...     .where(
    ...         Manager.id
    ...         == select(Employee.id).where(Employee.name == "sandy").scalar_subquery()
    ...     )
    ...     .values(manager_name="Sandy Cheeks, President")
    ... )
    >>> session.execute(stmt)
    {execsql}UPDATE manager SET manager_name=? WHERE manager.id = (SELECT employee.id
    FROM employee
    WHERE employee.name = ?) RETURNING id
    [...] ('Sandy Cheeks, President', 'sandy')
    {stop}<...>

For backends that support UPDATE...FROM, the subquery may be stated instead
as additional plain WHERE criteria, however the criteria between the two
tables must be stated explicitly in some way::

    >>> stmt = (
    ...     update(Manager)
    ...     .where(Manager.id == Employee.id, Employee.name == "sandy")
    ...     .values(manager_name="Sandy Cheeks, President")
    ... )
    >>> session.execute(stmt)
    {execsql}UPDATE manager SET manager_name=? FROM employee
    WHERE manager.id = employee.id AND employee.name = ?
    [...] ('Sandy Cheeks, President', 'sandy')
    {stop}<...>


For a DELETE, it's expected that rows in both the base table and the sub-table
would be DELETEd at the same time.   To DELETE many rows of joined inheritance
objects **without** using cascading foreign keys, emit DELETE for each
table individually::

    >>> from sqlalchemy import delete
    >>> session.execute(delete(Manager).where(Manager.id == 1))
    {execsql}DELETE FROM manager WHERE manager.id = ?
    [...] (1,)
    {stop}<...>
    >>> session.execute(delete(Employee).where(Employee.id == 1))
    {execsql}DELETE FROM employee WHERE employee.id = ?
    [...] (1,)
    {stop}<...>

Overall, normal :term:`unit of work` processes should be **preferred** for
updating and deleting rows for joined inheritance and other multi-table
mappings, unless there is a performance rationale for using custom WHERE
criteria.


Legacy Query Methods
~~~~~~~~~~~~~~~~~~~~

The ORM enabled UPDATE/DELETE with WHERE feature was originally part of the
now-legacy :class:`.Query` object, in the :meth:`_orm.Query.update`
and :meth:`_orm.Query.delete` methods.  These methods remain available
and provide a subset of the same functionality as that described at
:ref:`orm_queryguide_update_delete_where`.  The primary difference is that
the legacy methods don't provide for explicit RETURNING support.

.. seealso::

    :meth:`_orm.Query.update`

    :meth:`_orm.Query.delete`

..  Setup code, not for display

    >>> session.close()
    ROLLBACK...
    >>> conn.close()
